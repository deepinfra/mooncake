// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tent/common/cumem_host_pool.h"

#include <glog/logging.h>

#include <dirent.h>

#include <fstream>
#include <sstream>

#ifdef USE_CUDA
#include <cuda.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <map>
#include <type_traits>
#endif

namespace mooncake {
namespace tent {

#ifdef USE_CUDA
namespace {

constexpr uint32_t kMagic = 0x5543434D;  // "MCCU"
constexpr uint32_t kVersion = 1;
constexpr uint32_t kMaxParts = 8192;
// Fds per SCM_RIGHTS message (the kernel's SCM_MAX_FD is 253).
constexpr uint32_t kFdsPerMessage = 250;
constexpr char kSocketPrefix[] = "mooncake_cumem_";

struct WireHeader {
    uint32_t magic;
    uint32_t version;
    uint64_t base;
    uint64_t length;
    uint32_t count;
    uint32_t reserved;
};

struct WirePart {
    uint64_t offset;
    uint64_t size;
};

// The driver API, resolved at runtime so CPU-only processes still load.
struct Driver {
    bool ok = false;
    decltype(&cuInit) init;
    decltype(&cuDeviceGet) device_get;
    decltype(&cuDevicePrimaryCtxRetain) ctx_retain;
    decltype(&cuDevicePrimaryCtxRelease) ctx_release;
    decltype(&cuCtxGetCurrent) ctx_get;
    decltype(&cuCtxSetCurrent) ctx_set;
    decltype(&cuMemGetAllocationGranularity) granularity;
    decltype(&cuMemCreate) create;
    decltype(&cuMemRelease) release;
    decltype(&cuMemAddressReserve) reserve;
    decltype(&cuMemAddressFree) address_free;
    decltype(&cuMemMap) map;
    decltype(&cuMemUnmap) unmap;
    decltype(&cuMemSetAccess) set_access;
    decltype(&cuMemExportToShareableHandle) export_handle;
    decltype(&cuMemImportFromShareableHandle) import_handle;
    decltype(&cuPointerGetAttribute) pointer_attr;
    decltype(&cuGetErrorString) error_string;
};

const Driver& driver() {
    static Driver d = [] {
        Driver d{};
        void* lib = dlopen("libcuda.so.1", RTLD_NOW | RTLD_GLOBAL);
        if (!lib) return d;
        bool all = true;
        auto sym = [&](auto& fn, const char* name) {
            fn = reinterpret_cast<std::remove_reference_t<decltype(fn)>>(
                dlsym(lib, name));
            if (!fn) all = false;
        };
        sym(d.init, "cuInit");
        sym(d.device_get, "cuDeviceGet");
        sym(d.ctx_retain, "cuDevicePrimaryCtxRetain");
        sym(d.ctx_release, "cuDevicePrimaryCtxRelease_v2");
        sym(d.ctx_get, "cuCtxGetCurrent");
        sym(d.ctx_set, "cuCtxSetCurrent");
        sym(d.granularity, "cuMemGetAllocationGranularity");
        sym(d.create, "cuMemCreate");
        sym(d.release, "cuMemRelease");
        sym(d.reserve, "cuMemAddressReserve");
        sym(d.address_free, "cuMemAddressFree");
        sym(d.map, "cuMemMap");
        sym(d.unmap, "cuMemUnmap");
        sym(d.set_access, "cuMemSetAccess");
        sym(d.export_handle, "cuMemExportToShareableHandle");
        sym(d.import_handle, "cuMemImportFromShareableHandle");
        sym(d.pointer_attr, "cuPointerGetAttribute");
        sym(d.error_string, "cuGetErrorString");
        d.ok = all && d.init(0) == CUDA_SUCCESS;
        return d;
    }();
    return d;
}

Status cuFailure(const char* what, CUresult rc) {
    const char* msg = nullptr;
    if (driver().error_string) driver().error_string(rc, &msg);
    return Status::InternalError(std::string(what) + ": " +
                                 (msg ? msg : std::to_string(rc)) + LOC_MARK);
}

#define CU_CHECK(call, what)                            \
    do {                                                \
        CUresult _rc = (call);                          \
        if (_rc != CUDA_SUCCESS) return cuFailure(what, _rc); \
    } while (0)

// Makes `device`'s primary context current for one scope, restoring the
// caller's context afterwards.
class ScopedPrimaryContext {
   public:
    explicit ScopedPrimaryContext(int device) {
        const auto& d = driver();
        d.ctx_get(&saved_);
        CUcontext ctx = nullptr;
        if (d.device_get(&dev_, device) == CUDA_SUCCESS &&
            d.ctx_retain(&ctx, dev_) == CUDA_SUCCESS) {
            retained_ = true;
            ok_ = d.ctx_set(ctx) == CUDA_SUCCESS;
        }
    }
    ~ScopedPrimaryContext() {
        driver().ctx_set(saved_);
        if (retained_) driver().ctx_release(dev_);
    }
    bool ok() const { return ok_; }

   private:
    CUcontext saved_ = nullptr;
    CUdevice dev_ = 0;
    bool retained_ = false;
    bool ok_ = false;
};

struct ServedPool {
    uint64_t end;
    std::string socket_name;
    std::vector<CumemHostPool::NumaRange> ranges;
};
std::mutex g_registry_mutex;
// Pools served by this process, by base address.
std::map<uint64_t, ServedPool> g_registry;

size_t alignUp(size_t value, size_t alignment) {
    return (value + alignment - 1) / alignment * alignment;
}

// Bytes of a field of /sys/devices/system/node/node<N>/meminfo.
uint64_t nodeMeminfo(int node, const std::string& field) {
    std::ifstream in("/sys/devices/system/node/node" + std::to_string(node) +
                     "/meminfo");
    std::string line;
    while (std::getline(in, line)) {
        std::istringstream words(line);
        std::string node_word, id, key;
        uint64_t kb = 0;
        if (words >> node_word >> id >> key >> kb && key == field + ":")
            return kb << 10;
    }
    return 0;
}

// The driver takes only free 2 MiB blocks of the node and neither reclaims
// page cache nor compacts for them. Faulting in transparent huge pages bound
// to the node does both, and freeing them right after leaves that many free
// blocks for the next allocation. It runs in a child at the highest OOM
// score, so a node with less reclaimable memory than it looks loses the child
// to the OOM killer, not a neighbor. Callers make no other driver call
// meanwhile (fork).
bool reclaimOnNode(int node, size_t bytes) {
    const uint64_t file = nodeMeminfo(node, "Active(file)") +
                          nodeMeminfo(node, "Inactive(file)");
    if (bytes > file / 10 * 9) return false;
    const pid_t pid = fork();
    if (pid == 0) {
        int fd = open("/proc/self/oom_score_adj", O_WRONLY);
        if (fd >= 0) {
            (void)!write(fd, "1000", 4);
            close(fd);
        }
        constexpr size_t kHuge = 2ull << 20;
        void* raw = mmap(nullptr, bytes + kHuge, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        if (raw == MAP_FAILED) _exit(1);
        void* p = reinterpret_cast<void*>(
            alignUp(reinterpret_cast<uintptr_t>(raw), kHuge));
        bytes = bytes / kHuge * kHuge;
        unsigned long mask[16] = {};
        mask[node / 64] = 1UL << (node % 64);
        constexpr int kMpolBind = 2;
        constexpr int kMadvPopulateWrite = 23;
        if (syscall(SYS_mbind, p, bytes, kMpolBind, mask, 1024, 0) != 0)
            _exit(1);
        madvise(p, bytes, MADV_HUGEPAGE);
        if (madvise(p, bytes, kMadvPopulateWrite) != 0) {
            for (size_t off = 0; off < bytes; off += 4096)
                static_cast<volatile char*>(p)[off] = 1;
        }
        _exit(0);
    }
    if (pid < 0) return false;
    int status = 0;
    while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {
    }
    return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

CUmemAllocationProp hostNumaProp(int numa_node) {
    CUmemAllocationProp prop{};
    prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
    prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;
    prop.location.type = CU_MEM_LOCATION_TYPE_HOST_NUMA;
    prop.location.id = numa_node;
    return prop;
}

}  // namespace

Status CumemHostPool::Create(size_t length, const std::vector<int>& numa_nodes,
                             size_t piece_bytes,
                             std::unique_ptr<CumemHostPool>& out) {
    const auto& d = driver();
    if (!d.ok) return Status::NotImplemented("CUDA driver unavailable" LOC_MARK);
    if (numa_nodes.empty() || length == 0)
        return Status::InvalidArgument("empty CumemHostPool" LOC_MARK);

    size_t gran = 0;
    auto prop0 = hostNumaProp(numa_nodes.front());
    CU_CHECK(d.granularity(&gran, &prop0, CU_MEM_ALLOC_GRANULARITY_RECOMMENDED),
             "cuMemGetAllocationGranularity");
    const size_t per_node = alignUp(length / numa_nodes.size(), gran);
    const size_t max_pieces = kMaxParts / numa_nodes.size();
    piece_bytes = alignUp(
        std::max({piece_bytes, gran, (per_node + max_pieces - 1) / max_pieces}),
        gran);

    // Allocation zeroes the pages at ~8 GB/s per thread: one thread per node.
    // A node that comes up short is topped up afterwards, one at a time with
    // reclaim, which forks and so must not overlap other driver calls.
    std::unique_ptr<CumemHostPool> pool(new CumemHostPool());
    const auto start = std::chrono::steady_clock::now();
    std::vector<std::vector<Part>> got(numa_nodes.size());
    auto allocate = [&](size_t i, bool reclaim) {
        const int node = numa_nodes[i];
        auto prop = hostNumaProp(node);
        size_t have = 0;
        for (const auto& part : got[i]) have += part.size;
        while (have < per_node) {
            const size_t size = std::min(piece_bytes, per_node - have);
            CUmemGenericAllocationHandle handle = 0;
            CUresult rc = d.create(&handle, size, &prop, 0);
            // Compaction turns about half of what reclaim faults into free
            // 2 MiB blocks on a fragmented node, and the next attempt first
            // takes back the blocks the previous one left: fault more each
            // time.
            for (int attempt = 1; reclaim && attempt <= 3 &&
                                  rc != CUDA_SUCCESS &&
                                  reclaimOnNode(node, 2 * attempt * size);
                 ++attempt)
                rc = d.create(&handle, size, &prop, 0);
            if (rc != CUDA_SUCCESS) {
                if (reclaim)
                    LOG(WARNING) << "CumemHostPool: NUMA node " << node
                                 << " gave " << have << " of " << per_node
                                 << " bytes: "
                                 << cuFailure("cuMemCreate", rc).ToString();
                return;
            }
            got[i].push_back({0, size, handle, -1, node});
            have += size;
        }
    };
    {
        std::vector<std::thread> threads;
        for (size_t i = 0; i < numa_nodes.size(); ++i)
            threads.emplace_back(allocate, i, false);
        for (auto& t : threads) t.join();
    }
    for (size_t i = 0; i < numa_nodes.size(); ++i) allocate(i, true);
    for (auto& parts : got) {
        for (auto& part : parts) {
            part.offset = pool->length_;
            pool->parts_.push_back(part);
            pool->length_ += part.size;
        }
    }
    if (pool->parts_.empty())
        return Status::InternalError("CumemHostPool: no memory" LOC_MARK);

    CUdeviceptr base = 0;
    CU_CHECK(d.reserve(&base, pool->length_, gran, 0, 0), "cuMemAddressReserve");
    pool->base_ = base;
    for (auto& part : pool->parts_) {
        CU_CHECK(d.map(base + part.offset, part.size, 0, part.handle, 0),
                 "cuMemMap");
        CU_CHECK(d.export_handle(&part.fd, part.handle,
                                 CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR, 0),
                 "cuMemExportToShareableHandle");
    }
    // The owner touches the pool with the CPU (store bookkeeping, TCP).
    CUmemAccessDesc access{};
    access.location.type = CU_MEM_LOCATION_TYPE_HOST;
    access.location.id = 0;
    access.flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
    CU_CHECK(d.set_access(base, pool->length_, &access, 1),
             "cuMemSetAccess(host)");
    LOG(INFO) << "CumemHostPool: " << pool->length_ << " of "
              << per_node * numa_nodes.size() << " bytes over "
              << numa_nodes.size() << " NUMA node(s) in "
              << pool->parts_.size() << " pieces at "
              << reinterpret_cast<void*>(base) << " in "
              << std::chrono::duration<double>(
                     std::chrono::steady_clock::now() - start)
                     .count()
              << " s";
    out = std::move(pool);
    return Status::OK();
}

CumemHostPool::~CumemHostPool() {
    if (listen_fd_ >= 0) {
        shutdown(listen_fd_, SHUT_RDWR);
        close(listen_fd_);
        if (server_.joinable()) server_.join();
        unlink(socket_path_.c_str());
    }
    {
        std::lock_guard<std::mutex> lock(g_registry_mutex);
        g_registry.erase(base_);
    }
    std::lock_guard<std::mutex> lock(clients_mutex_);
    for (int fd : clients_) close(fd);
    const auto& d = driver();
    if (!d.ok) return;
    if (base_) {
        d.unmap(base_, length_);
        d.address_free(base_, length_);
    }
    for (auto& part : parts_) {
        if (part.fd >= 0) close(part.fd);
        d.release(part.handle);
    }
}

// Whether a process accepts connections on the unix socket at `path`.
static bool socketServed(const std::string& path) {
    sockaddr_un addr{};
    if (path.size() >= sizeof(addr.sun_path)) return false;
    int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) return true;  // cannot tell: keep it
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
    const bool refused =
        connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 &&
        (errno == ECONNREFUSED || errno == ENOENT);
    close(fd);
    return !refused;
}

Status CumemHostPool::Serve(const std::string& socket_dir) {
    if (DIR* dir = opendir(socket_dir.c_str())) {
        while (dirent* entry = readdir(dir)) {
            const std::string name = entry->d_name;
            if (name.rfind(kSocketPrefix, 0) != 0) continue;
            const std::string path = socket_dir + "/" + name;
            if (!socketServed(path)) {
                LOG(WARNING) << "CumemHostPool: removing stale " << path;
                unlink(path.c_str());
            }
        }
        closedir(dir);
    }
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    const std::string socket_path =
        socket_dir + "/" + kSocketPrefix +
        std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(now)
                           .count()) +
        ".sock";
    sockaddr_un addr{};
    if (socket_path.size() >= sizeof(addr.sun_path))
        return Status::InvalidArgument("socket path too long" LOC_MARK);
    listen_fd_ = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (listen_fd_ < 0)
        return Status::InternalError(std::string("socket: ") + strerror(errno));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, socket_path.c_str(), sizeof(addr.sun_path) - 1);
    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
        listen(listen_fd_, 64) != 0)
        return Status::InternalError("bind/listen " + socket_path + ": " +
                                     strerror(errno));
    // Whoever connects can map the whole pool.
    chmod(socket_path.c_str(), 0600);
    socket_path_ = socket_path;
    {
        ServedPool served{base_ + length_,
                          socket_path.substr(socket_path.find_last_of('/') + 1),
                          {}};
        for (const auto& part : parts_) {
            auto& ranges = served.ranges;
            if (!ranges.empty() && ranges.back().node == part.node)
                ranges.back().length += part.size;
            else
                ranges.push_back({base_ + part.offset, part.size, part.node});
        }
        std::lock_guard<std::mutex> lock(g_registry_mutex);
        g_registry[base_] = std::move(served);
    }
    server_ = std::thread([this] { serveLoop(); });
    LOG(INFO) << "CumemHostPool: serving " << parts_.size() << " fd(s) at "
              << socket_path;
    return Status::OK();
}

void CumemHostPool::serveLoop() {
    while (true) {
        int conn = accept4(listen_fd_, nullptr, nullptr, SOCK_CLOEXEC);
        if (conn < 0) {
            if (errno == EINTR) continue;
            return;  // listen socket closed
        }
        // The header and part table, then the fds in batches, each batch a
        // 4-byte count carrying them.
        std::vector<char> payload(sizeof(WireHeader) +
                                  parts_.size() * sizeof(WirePart));
        WireHeader header{kMagic, kVersion, base_, length_,
                          static_cast<uint32_t>(parts_.size()), 0};
        memcpy(payload.data(), &header, sizeof(header));
        for (size_t i = 0; i < parts_.size(); ++i) {
            WirePart part{parts_[i].offset, parts_[i].size};
            memcpy(payload.data() + sizeof(header) + i * sizeof(WirePart),
                   &part, sizeof(part));
        }
        bool sent = send(conn, payload.data(), payload.size(), MSG_NOSIGNAL) ==
                    static_cast<ssize_t>(payload.size());
        for (size_t first = 0; sent && first < parts_.size();
             first += kFdsPerMessage) {
            uint32_t count = std::min<size_t>(kFdsPerMessage,
                                              parts_.size() - first);
            std::vector<char> control(CMSG_SPACE(sizeof(int) * count));
            iovec iov{&count, sizeof(count)};
            msghdr msg{};
            msg.msg_iov = &iov;
            msg.msg_iovlen = 1;
            msg.msg_control = control.data();
            msg.msg_controllen = control.size();
            cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
            cmsg->cmsg_level = SOL_SOCKET;
            cmsg->cmsg_type = SCM_RIGHTS;
            cmsg->cmsg_len = CMSG_LEN(sizeof(int) * count);
            for (uint32_t i = 0; i < count; ++i)
                memcpy(CMSG_DATA(cmsg) + i * sizeof(int),
                       &parts_[first + i].fd, sizeof(int));
            sent = sendmsg(conn, &msg, MSG_NOSIGNAL) ==
                   static_cast<ssize_t>(sizeof(count));
        }
        if (!sent) {
            LOG(WARNING) << "CumemHostPool: send failed: " << strerror(errno);
            close(conn);
            continue;
        }
        // Held open: the peer sees EOF when this process exits. Peers send
        // nothing, so a readable connection has hung up.
        std::lock_guard<std::mutex> lock(clients_mutex_);
        clients_.push_back(conn);
        std::vector<pollfd> fds;
        for (int fd : clients_) fds.push_back({fd, POLLIN | POLLRDHUP, 0});
        if (poll(fds.data(), fds.size(), 0) > 0) {
            clients_.clear();
            for (const auto& p : fds) {
                if (p.revents) close(p.fd);
                else clients_.push_back(p.fd);
            }
        }
    }
}

std::string CumemHostPool::SocketNameFor(uint64_t addr, uint64_t length) {
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    auto it = g_registry.upper_bound(addr);
    if (it == g_registry.begin()) return "";
    --it;
    if (addr + length > it->second.end) return "";
    return it->second.socket_name;
}

std::vector<CumemHostPool::NumaRange> CumemHostPool::NumaRangesFor(
    uint64_t addr, uint64_t length) {
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    auto it = g_registry.upper_bound(addr);
    if (it == g_registry.begin()) return {};
    --it;
    if (addr + length > it->second.end) return {};
    std::vector<NumaRange> out;
    for (const auto& range : it->second.ranges) {
        const uint64_t lo = std::max(addr, range.start);
        const uint64_t hi = std::min(addr + length, range.start + range.length);
        if (lo < hi) out.push_back({lo, hi - lo, range.node});
    }
    return out;
}

Status CumemHostMapping::Import(const std::string& socket_path,
                                std::unique_ptr<CumemHostMapping>& out) {
    const auto& d = driver();
    if (!d.ok) return Status::NotImplemented("CUDA driver unavailable" LOC_MARK);
    sockaddr_un addr{};
    if (socket_path.size() >= sizeof(addr.sun_path))
        return Status::InvalidArgument("socket path too long" LOC_MARK);
    std::unique_ptr<CumemHostMapping> m(new CumemHostMapping());
    m->conn_fd_ = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, socket_path.c_str(), sizeof(addr.sun_path) - 1);
    if (m->conn_fd_ < 0 ||
        connect(m->conn_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0)
        return Status::InternalError("connect " + socket_path + ": " +
                                     strerror(errno));
    WireHeader header{};
    if (recv(m->conn_fd_, &header, sizeof(header), MSG_WAITALL) !=
            static_cast<ssize_t>(sizeof(header)) ||
        header.magic != kMagic || header.version != kVersion ||
        header.count == 0 || header.count > kMaxParts)
        return Status::InternalError("bad CumemHostPool header" LOC_MARK);
    std::vector<char> payload(header.count * sizeof(WirePart));
    if (recv(m->conn_fd_, payload.data(), payload.size(), MSG_WAITALL) !=
        static_cast<ssize_t>(payload.size()))
        return Status::InternalError("short CumemHostPool parts" LOC_MARK);
    std::vector<int> fds;
    auto close_fds = [&] {
        for (int fd : fds) close(fd);
    };
    while (fds.size() < header.count) {
        uint32_t count = 0;
        std::vector<char> control(CMSG_SPACE(sizeof(int) * kFdsPerMessage));
        iovec iov{&count, sizeof(count)};
        msghdr msg{};
        msg.msg_iov = &iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control.data();
        msg.msg_controllen = control.size();
        const ssize_t n = recvmsg(m->conn_fd_, &msg, MSG_CMSG_CLOEXEC);
        size_t got = 0;
        for (cmsghdr* c = CMSG_FIRSTHDR(&msg); c; c = CMSG_NXTHDR(&msg, c)) {
            if (c->cmsg_level != SOL_SOCKET || c->cmsg_type != SCM_RIGHTS)
                continue;
            const size_t k = (c->cmsg_len - CMSG_LEN(0)) / sizeof(int);
            for (size_t i = 0; i < k; ++i) {
                int fd;
                memcpy(&fd, CMSG_DATA(c) + i * sizeof(int), sizeof(int));
                fds.push_back(fd);
            }
            got += k;
        }
        if (n != static_cast<ssize_t>(sizeof(count)) || got != count ||
            (msg.msg_flags & MSG_CTRUNC) || fds.size() > header.count) {
            close_fds();
            return Status::InternalError("bad CumemHostPool fds" LOC_MARK);
        }
    }

    CUdeviceptr base = 0;
    CUresult rc = d.reserve(&base, header.length, 2 << 20, 0, 0);
    if (rc != CUDA_SUCCESS) {
        close_fds();
        return cuFailure("cuMemAddressReserve", rc);
    }
    m->base_ = base;
    m->length_ = header.length;
    m->peer_base_ = header.base;
    m->part_offsets_.reserve(header.count + 1);
    for (uint32_t i = 0; i < header.count; ++i) {
        WirePart part;
        memcpy(&part, payload.data() + i * sizeof(WirePart), sizeof(part));
        m->part_offsets_.push_back(part.offset);
        CUmemGenericAllocationHandle handle = 0;
        rc = d.import_handle(&handle,
                             reinterpret_cast<void*>(static_cast<intptr_t>(fds[i])),
                             CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR);
        if (rc == CUDA_SUCCESS) {
            rc = d.map(base + part.offset, part.size, 0, handle, 0);
            d.release(handle);  // the mapping keeps its own reference
        }
        if (rc != CUDA_SUCCESS) {
            close_fds();
            return cuFailure("import/map CumemHostPool part", rc);
        }
    }
    close_fds();
    m->part_offsets_.push_back(header.length);
    LOG(INFO) << "CumemHostMapping: " << header.length << " bytes of "
              << socket_path << " at " << reinterpret_cast<void*>(base)
              << " (owner " << reinterpret_cast<void*>(header.base) << ")";
    out = std::move(m);
    return Status::OK();
}

CumemHostMapping::~CumemHostMapping() {
    const auto& d = driver();
    if (d.ok && base_) {
        d.unmap(base_, length_);
        d.address_free(base_, length_);
    }
    if (conn_fd_ >= 0) close(conn_fd_);
}

Status CumemHostMapping::EnableDeviceAccess(uint64_t offset, uint64_t length,
                                            const void* device_hint,
                                            int& device_out) {
    const auto& d = driver();
    device_out = -1;
    unsigned int mem_type = 0;
    int ordinal = -1;
    const auto ptr = reinterpret_cast<CUdeviceptr>(device_hint);
    if (d.pointer_attr(&mem_type, CU_POINTER_ATTRIBUTE_MEMORY_TYPE, ptr) !=
            CUDA_SUCCESS ||
        mem_type != CU_MEMORYTYPE_DEVICE ||
        d.pointer_attr(&ordinal, CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL, ptr) !=
            CUDA_SUCCESS)
        return Status::InvalidArgument("device_hint is not device memory" LOC_MARK);
    if (length == 0 || offset + length > length_)
        return Status::InvalidArgument("range outside the pool" LOC_MARK);
    std::lock_guard<std::mutex> lock(access_mutex_);
    if (access_.size() <= static_cast<size_t>(ordinal))
        access_.resize(ordinal + 1);
    auto& granted = access_[ordinal];
    const size_t parts = part_offsets_.size() - 1;
    if (granted.empty()) granted.resize(parts);
    // The parts overlapping the range.
    size_t first = std::upper_bound(part_offsets_.begin(), part_offsets_.end(),
                                    offset) -
                   part_offsets_.begin() - 1;
    size_t last = std::lower_bound(part_offsets_.begin(), part_offsets_.end(),
                                   offset + length) -
                  part_offsets_.begin();
    std::unique_ptr<ScopedPrimaryContext> scope;
    for (size_t i = first; i < last && i < parts; ++i) {
        if (granted[i]) continue;
        if (!scope) {
            scope = std::make_unique<ScopedPrimaryContext>(ordinal);
            if (!scope->ok())
                return Status::InternalError("no CUDA context for device " +
                                             std::to_string(ordinal) +
                                             LOC_MARK);
        }
        CUmemAccessDesc access{};
        access.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
        access.location.id = ordinal;
        access.flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
        CU_CHECK(d.set_access(base_ + part_offsets_[i],
                              part_offsets_[i + 1] - part_offsets_[i], &access,
                              1),
                 "cuMemSetAccess(device)");
        granted[i] = true;
    }
    device_out = ordinal;
    return Status::OK();
}

#else  // !USE_CUDA

Status CumemHostPool::Create(size_t, const std::vector<int>&, size_t,
                             std::unique_ptr<CumemHostPool>&) {
    return Status::NotImplemented("built without CUDA" LOC_MARK);
}
CumemHostPool::~CumemHostPool() = default;
Status CumemHostPool::Serve(const std::string&) {
    return Status::NotImplemented("built without CUDA" LOC_MARK);
}
void CumemHostPool::serveLoop() {}
std::string CumemHostPool::SocketNameFor(uint64_t, uint64_t) { return ""; }
std::vector<CumemHostPool::NumaRange> CumemHostPool::NumaRangesFor(uint64_t,
                                                                  uint64_t) {
    return {};
}
Status CumemHostMapping::Import(const std::string&,
                                std::unique_ptr<CumemHostMapping>&) {
    return Status::NotImplemented("built without CUDA" LOC_MARK);
}
CumemHostMapping::~CumemHostMapping() = default;
Status CumemHostMapping::EnableDeviceAccess(uint64_t, uint64_t, const void*,
                                            int& device_out) {
    device_out = -1;
    return Status::NotImplemented("built without CUDA" LOC_MARK);
}

#endif  // USE_CUDA

std::vector<int> CumemHostPool::OnlineNumaNodes() {
    // A list such as "0-3" or "0,2-3".
    std::ifstream in("/sys/devices/system/node/online");
    std::string list;
    std::getline(in, list);
    std::vector<int> nodes;
    std::istringstream ranges(list);
    std::string range;
    while (std::getline(ranges, range, ',')) {
        const auto dash = range.find('-');
        try {
            const int first = std::stoi(range.substr(0, dash));
            const int last = dash == std::string::npos
                                 ? first
                                 : std::stoi(range.substr(dash + 1));
            for (int n = first; n <= last; ++n) nodes.push_back(n);
        } catch (const std::exception&) {
        }
    }
    if (nodes.empty()) nodes.push_back(0);
    return nodes;
}

}  // namespace tent
}  // namespace mooncake
