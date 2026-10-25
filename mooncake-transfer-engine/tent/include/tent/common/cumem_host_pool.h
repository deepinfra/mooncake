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

#ifndef TENT_COMMON_CUMEM_HOST_POOL_H
#define TENT_COMMON_CUMEM_HOST_POOL_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/status.h"

namespace mooncake {
namespace tent {

// Host memory allocated through the CUDA driver (cuMemCreate on host NUMA
// nodes) and shared with same-host processes as POSIX file descriptors over
// a unix socket. A peer GPU reaches it without page faults (HMM) and without
// per-process registration, and the kernel keeps no per-page state for it.
// The owner needs no CUDA context, so it uses no GPU memory; it only needs a
// visible device for cuInit. The driver is loaded at runtime, so this builds
// and links without CUDA; every call then fails with NotImplemented.
class CumemHostPool {
   public:
    // The length split evenly over `numa_nodes`, each node's share allocated
    // in pieces of about `piece_bytes` and all of them mapped back to back
    // with CPU access. A node that runs out of memory contributes what it
    // got, so the pool may be shorter than asked; it fails only when empty.
    static Status Create(size_t length, const std::vector<int>& numa_nodes,
                         size_t piece_bytes,
                         std::unique_ptr<CumemHostPool>& pool);

    // The host's online NUMA nodes.
    static std::vector<int> OnlineNumaNodes();

    ~CumemHostPool();

    void* base() const { return reinterpret_cast<void*>(base_); }
    size_t length() const { return length_; }

    // Serves the layout and the allocation fds at a new socket in
    // `socket_dir`, first removing sockets there that nobody serves. Peers
    // keep the connection open; it closes when this process exits, which
    // tells them to unmap.
    Status Serve(const std::string& socket_dir);

    // Socket file name served for the pool containing [addr, addr+length)
    // in this process, or "" when no pool does.
    static std::string SocketNameFor(uint64_t addr, uint64_t length);

    // The NUMA nodes of [addr, addr+length) within a served pool, as
    // coalesced ranges; empty when no pool contains it.
    struct NumaRange {
        uint64_t start;
        uint64_t length;
        int node;
    };
    static std::vector<NumaRange> NumaRangesFor(uint64_t addr,
                                                uint64_t length);

   private:
    CumemHostPool() = default;
    void serveLoop();

    uint64_t base_ = 0;
    size_t length_ = 0;
    struct Part {
        uint64_t offset;
        uint64_t size;
        unsigned long long handle;
        int fd;
        int node;
    };
    std::vector<Part> parts_;
    std::string socket_path_;
    int listen_fd_ = -1;
    std::thread server_;
    std::mutex clients_mutex_;
    std::vector<int> clients_;
};

// A peer's CumemHostPool mapped into this process. Device access is enabled
// per range on demand; until then the range must not be touched. The import
// needs, for each NUMA node of the pool, a GPU on that node visible to the
// container (its device files; CUDA_VISIBLE_DEVICES may hide it), else the
// driver refuses it.
class CumemHostMapping {
   public:
    static Status Import(const std::string& socket_path,
                         std::unique_ptr<CumemHostMapping>& mapping);

    ~CumemHostMapping();

    void* base() const { return reinterpret_cast<void*>(base_); }
    size_t length() const { return length_; }
    // The owner's address of the first byte.
    uint64_t peer_base() const { return peer_base_; }
    // Becomes readable (EOF) once the owner exits.
    int connection_fd() const { return conn_fd_; }

    // Lets the device owning `device_hint` access [offset, offset+length),
    // widened to whole pool pieces (the driver grants access per mapping);
    // `device_out` receives that device. InvalidArgument when device_hint is
    // not device memory.
    Status EnableDeviceAccess(uint64_t offset, uint64_t length,
                              const void* device_hint, int& device_out);

   private:
    CumemHostMapping() = default;

    uint64_t base_ = 0;
    size_t length_ = 0;
    uint64_t peer_base_ = 0;
    int conn_fd_ = -1;
    std::vector<uint64_t> part_offsets_;  // ascending; part i ends at i+1
    std::mutex access_mutex_;
    // Per device ordinal, which parts it may access.
    std::vector<std::vector<bool>> access_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_COMMON_CUMEM_HOST_POOL_H
