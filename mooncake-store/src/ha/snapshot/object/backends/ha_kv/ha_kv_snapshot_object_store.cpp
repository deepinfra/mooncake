#include "ha/snapshot/object/backends/ha_kv/ha_kv_snapshot_object_store.h"

#include <cstdio>

namespace mooncake {

namespace {

constexpr char kManifestRoot[] = "/mooncake_snapshot_objects/m/";
constexpr char kChunkRoot[] = "/mooncake_snapshot_objects/c/";
constexpr char kNotFound[] = "Object not found: ";

std::string ManifestKey(const std::string& key) { return kManifestRoot + key; }

std::string ChunkPrefix(const std::string& key) {
    return kChunkRoot + key + '\x01';
}

std::string ChunkKey(const std::string& key, size_t index) {
    char suffix[16];
    std::snprintf(suffix, sizeof(suffix), "%08zu", index);
    return ChunkPrefix(key) + suffix;
}

// The end of the bytewise range of keys starting with `prefix`.
std::string PrefixEnd(const std::string& prefix) { return prefix + '\xff'; }

struct Manifest {
    uint64_t size{0};
    uint64_t chunks{0};
};

std::string EncodeManifest(const Manifest& manifest) {
    return std::to_string(manifest.size) + ":" +
           std::to_string(manifest.chunks);
}

bool DecodeManifest(const std::string& text, Manifest& manifest) {
    const size_t colon = text.find(':');
    if (colon == std::string::npos) {
        return false;
    }
    try {
        manifest.size = std::stoull(text.substr(0, colon));
        manifest.chunks = std::stoull(text.substr(colon + 1));
    } catch (...) {
        return false;
    }
    return true;
}

std::string Failed(const char* what, const std::string& key, ErrorCode err) {
    return std::string(what) + " " + key + ": " + toString(err);
}

}  // namespace

HaKvSnapshotObjectStore::HaKvSnapshotObjectStore(
    std::shared_ptr<HaKvBackend> backend, size_t chunk_bytes)
    : backend_(std::move(backend)), chunk_bytes_(chunk_bytes) {}

tl::expected<void, std::string> HaKvSnapshotObjectStore::Upload(
    const std::string& key, std::string_view bytes) {
    const std::string chunks = ChunkPrefix(key);
    // The manifest goes last: an object is either whole or absent.
    const std::string manifest_key = ManifestKey(key);
    if (auto err = backend_->DeleteRange(manifest_key,
                                         manifest_key + std::string(1, '\0'));
        err != ErrorCode::OK) {
        return tl::make_unexpected(Failed("clear manifest", key, err));
    }
    if (auto err = backend_->DeleteRange(chunks, PrefixEnd(chunks));
        err != ErrorCode::OK) {
        return tl::make_unexpected(Failed("clear chunks", key, err));
    }
    Manifest manifest{.size = bytes.size(), .chunks = 0};
    for (size_t offset = 0; offset < bytes.size() || manifest.chunks == 0;
         offset += chunk_bytes_) {
        const auto piece = bytes.substr(
            offset, std::min(chunk_bytes_, bytes.size() - offset));
        if (auto err = backend_->Put(ChunkKey(key, manifest.chunks), piece);
            err != ErrorCode::OK) {
            return tl::make_unexpected(Failed("put chunk", key, err));
        }
        ++manifest.chunks;
        if (bytes.empty()) {
            break;
        }
    }
    if (auto err = backend_->Put(ManifestKey(key), EncodeManifest(manifest));
        err != ErrorCode::OK) {
        return tl::make_unexpected(Failed("put manifest", key, err));
    }
    return {};
}

tl::expected<void, std::string> HaKvSnapshotObjectStore::Download(
    const std::string& key, std::string& bytes) {
    std::string text;
    const ErrorCode err = backend_->Get(ManifestKey(key), text);
    if (err == ErrorCode::ETCD_KEY_NOT_EXIST) {
        return tl::make_unexpected(kNotFound + key);
    }
    Manifest manifest;
    if (err != ErrorCode::OK || !DecodeManifest(text, manifest)) {
        return tl::make_unexpected(Failed("get manifest", key, err));
    }
    bytes.clear();
    bytes.reserve(manifest.size);
    for (uint64_t index = 0; index < manifest.chunks; ++index) {
        std::string piece;
        if (auto chunk_err = backend_->Get(ChunkKey(key, index), piece);
            chunk_err != ErrorCode::OK) {
            return tl::make_unexpected(Failed("get chunk", key, chunk_err));
        }
        bytes += piece;
    }
    if (bytes.size() != manifest.size) {
        return tl::make_unexpected("Object size mismatch: " + key);
    }
    return {};
}

tl::expected<void, std::string> HaKvSnapshotObjectStore::UploadBuffer(
    const std::string& key, const std::vector<uint8_t>& buffer) {
    return Upload(key,
                  std::string_view(reinterpret_cast<const char*>(buffer.data()),
                                   buffer.size()));
}

tl::expected<void, std::string> HaKvSnapshotObjectStore::DownloadBuffer(
    const std::string& key, std::vector<uint8_t>& buffer) {
    std::string bytes;
    auto result = Download(key, bytes);
    if (!result) {
        return result;
    }
    buffer.assign(bytes.begin(), bytes.end());
    return {};
}

tl::expected<void, std::string> HaKvSnapshotObjectStore::UploadString(
    const std::string& key, const std::string& data) {
    return Upload(key, data);
}

tl::expected<void, std::string> HaKvSnapshotObjectStore::DownloadString(
    const std::string& key, std::string& data) {
    return Download(key, data);
}

tl::expected<void, std::string>
HaKvSnapshotObjectStore::DeleteObjectsWithPrefix(const std::string& prefix) {
    const std::string manifests = ManifestKey(prefix);
    const std::string chunks = kChunkRoot + prefix;
    if (auto err = backend_->DeleteRange(manifests, PrefixEnd(manifests));
        err != ErrorCode::OK) {
        return tl::make_unexpected(Failed("delete manifests", prefix, err));
    }
    if (auto err = backend_->DeleteRange(chunks, PrefixEnd(chunks));
        err != ErrorCode::OK) {
        return tl::make_unexpected(Failed("delete chunks", prefix, err));
    }
    return {};
}

tl::expected<void, std::string> HaKvSnapshotObjectStore::ListObjectsWithPrefix(
    const std::string& prefix, std::vector<std::string>& object_keys) {
    const std::string begin = ManifestKey(prefix);
    std::vector<KvPair> kvs;
    if (auto err = backend_->Range(begin, PrefixEnd(begin), 0, kvs);
        err != ErrorCode::OK) {
        return tl::make_unexpected(Failed("list", prefix, err));
    }
    object_keys.clear();
    const size_t root = std::string(kManifestRoot).size();
    for (const auto& kv : kvs) {
        object_keys.push_back(kv.key.substr(root));
    }
    return {};
}

tl::expected<SnapshotObjectInspection, std::string>
HaKvSnapshotObjectStore::InspectObject(const std::string& key) {
    std::string text;
    const ErrorCode err = backend_->Get(ManifestKey(key), text);
    if (err == ErrorCode::ETCD_KEY_NOT_EXIST) {
        return tl::make_unexpected(kNotFound + key);
    }
    Manifest manifest;
    if (err != ErrorCode::OK || !DecodeManifest(text, manifest)) {
        return tl::make_unexpected(Failed("inspect", key, err));
    }
    return SnapshotObjectInspection{.stored_size = manifest.size,
                                    .crc32c = std::nullopt};
}

bool HaKvSnapshotObjectStore::IsNotFoundError(const std::string& error) const {
    return error.starts_with(kNotFound);
}

std::string HaKvSnapshotObjectStore::GetConnectionInfo() const {
    return "HaKvSnapshotObjectStore";
}

}  // namespace mooncake
