#pragma once

#include <memory>
#include <string>
#include <vector>

#include "ha/kv/ha_kv_backend.h"
#include "ha/snapshot/object/snapshot_object_store.h"

namespace mooncake {

// Snapshot objects kept in the HA KV store itself, in chunks, so a
// replicated store carries them to the other node with everything else and a
// new follower can be rebuilt from the leader alone.
class HaKvSnapshotObjectStore final : public SnapshotObjectStore {
   public:
    explicit HaKvSnapshotObjectStore(std::shared_ptr<HaKvBackend> backend,
                                     size_t chunk_bytes = 4u << 20);

    tl::expected<void, std::string> UploadBuffer(
        const std::string& key, const std::vector<uint8_t>& buffer) override;
    tl::expected<void, std::string> DownloadBuffer(
        const std::string& key, std::vector<uint8_t>& buffer) override;
    tl::expected<void, std::string> UploadString(
        const std::string& key, const std::string& data) override;
    tl::expected<void, std::string> DownloadString(const std::string& key,
                                                   std::string& data) override;
    tl::expected<void, std::string> DeleteObjectsWithPrefix(
        const std::string& prefix) override;
    tl::expected<void, std::string> ListObjectsWithPrefix(
        const std::string& prefix,
        std::vector<std::string>& object_keys) override;
    tl::expected<SnapshotObjectInspection, std::string> InspectObject(
        const std::string& key) override;
    bool IsNotFoundError(const std::string& error) const override;
    std::string GetConnectionInfo() const override;

   private:
    tl::expected<void, std::string> Upload(const std::string& key,
                                           std::string_view bytes);
    tl::expected<void, std::string> Download(const std::string& key,
                                             std::string& bytes);

    std::shared_ptr<HaKvBackend> backend_;
    size_t chunk_bytes_;
};

}  // namespace mooncake
