// Copyright 2025 KVCache.AI
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

#ifndef SHM_TRANSPORT_H_
#define SHM_TRANSPORT_H_

#include <atomic>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <chrono>
#include <queue>
#include <string>
#include <thread>

#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {

class CumemHostMapping;

struct ShmTask {
    Request request;
    volatile TransferStatusEnum status_word;
    volatile size_t transferred_bytes;
    uint64_t target_addr = 0;
};

struct ShmSubBatch : public Transport::SubBatch {
    std::vector<ShmTask> task_list;
    size_t max_size;
    virtual size_t size() const { return task_list.size(); }
};

class ShmTransport : public Transport {
   public:
    ShmTransport();

    ~ShmTransport();

    virtual Status install(std::string &local_segment_name,
                           std::shared_ptr<ControlService> metadata,
                           std::shared_ptr<Topology> local_topology,
                           std::shared_ptr<Config> conf = nullptr);

    virtual Status uninstall();

    virtual Status allocateSubBatch(SubBatchRef &batch, size_t max_size);

    virtual Status freeSubBatch(SubBatchRef &batch);

    virtual Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request> &request_list);

    virtual Status getTransferStatus(SubBatchRef batch, int task_id,
                                     TransferStatus &status);

    virtual Status addMemoryBuffer(BufferDesc &desc,
                                   const MemoryOptions &options);

    virtual Status removeMemoryBuffer(BufferDesc &desc);

    virtual const char *getName() const { return "shm"; }

    virtual Status allocateLocalMemory(void **addr, size_t size,
                                       MemoryOptions &options);

    virtual Status freeLocalMemory(void *addr, size_t size);

    // Relocates and copies `request_list` synchronously; NotImplemented
    // unless device_copy is on and every target is a same-host shared
    // buffer (see TransferEngine::copySameHost).
    Status copyDirect(const std::vector<Request> &request_list);

   private:
    friend class ShmTransportTestPeer;

    struct OpenedShmEntry;

    void startTransfer(ShmTask *task, ShmSubBatch *batch);

    void submitBatchedCopies(ShmSubBatch *batch,
                             const std::vector<Request> &request_list,
                             const std::vector<uint64_t> &target_addrs,
                             const std::vector<std::shared_ptr<OpenedShmEntry>>
                                 &mappings);

    Status copyRelocated(const std::vector<Request> &request_list,
                         const std::vector<uint64_t> &target_addrs,
                         const std::vector<std::shared_ptr<OpenedShmEntry>>
                             &mappings);

    void *createSharedMemory(const std::string &path, size_t size);

    // `mapping`, when given, pins the peer mapping that dest_addr now points
    // into so it outlives a concurrent reap.
    Status relocateSharedMemoryAddress(
        uint64_t &dest_addr, uint64_t length, uint64_t target_id,
        std::shared_ptr<OpenedShmEntry> *mapping = nullptr);

    // Finds the shared file mapping under cxl_mount_path that starts at
    // desc.addr, for buffers this transport did not allocate (e.g. a store
    // segment mmap'ed from a hugetlbfs file). Empty if there is none.
    std::string findSharedMappingPath(const BufferDesc &desc) const;

    // Pins `chunks` of `entry` for one batch of copies and registers those
    // not yet registered, evicting the least recently used registered chunks
    // beyond register_max_bytes. unpinChunks() ends the batch.
    void pinChunks(OpenedShmEntry &entry, const std::vector<size_t> &chunks,
                   void *device_hint);

    static void unpinChunks(OpenedShmEntry &entry,
                            const std::vector<size_t> &chunks);

    // Makes chunk `c` reachable by the device of `device_hint`: CUDA
    // registration of a plain mapping, or device access to a CUDA VMM one.
    static Status registerChunk(OpenedShmEntry &entry, size_t c,
                                void *device_hint, int &device);

    // Registers up to register_ahead_chunks chunks past `last` in the
    // background: the store allocates sequentially, so the next objects a
    // peer writes (and this process then loads) land there.
    void registerAhead(const std::shared_ptr<OpenedShmEntry> &entry,
                       size_t last, void *device_hint);
    void registerAheadLoop();

    void reapUnlinkedMappings();

   private:
    bool installed_;
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<ControlService> metadata_;

    // A peer buffer mapped into this process. Unmapped (and unregistered
    // from the device runtime) when the last holder drops it: the relocate
    // map, or a copy in flight.
    struct OpenedShmEntry {
        void *shm_addr = nullptr;
        uint64_t length = 0;
        int fd = -1;  // kept open to notice the owner unlinking the file
        // Set for an owner's CumemHostPool: reachable by a device only after
        // registerChunk, never by the CPU.
        std::unique_ptr<CumemHostMapping> cumem;
        uint64_t chunk_bytes = 0;
        size_t num_chunks = 0;
        // Guarded by register_mutex. chunk_device: kChunkUnregistered, or the
        // device id passed back by Platform::registerHostMemory (-1 =
        // registered by someone else or registration failed; never retried).
        std::mutex register_mutex;
        std::unique_ptr<int[]> chunk_device;
        std::unique_ptr<uint64_t[]> chunk_last_use;
        size_t registered_chunks = 0;
        // Copies in flight per chunk; a pinned chunk is never evicted.
        std::unique_ptr<std::atomic<uint32_t>[]> chunk_pins;
        // Chunks below this index were already queued for registerAhead.
        std::atomic<size_t> ahead_until{0};

        ~OpenedShmEntry();
    };
    static constexpr int kChunkUnregistered = -2;

    using RelocateMap =
        std::unordered_map<uint64_t, std::shared_ptr<OpenedShmEntry>>;
    using HashMap = std::unordered_map<SegmentID, RelocateMap>;

    static bool tryResolve(const RelocateMap &relocate_map, uint64_t &dest_addr,
                           uint64_t length,
                           std::shared_ptr<OpenedShmEntry> *mapping = nullptr);

    RWSpinlock relocate_lock_;
    HashMap relocate_map_;
    std::shared_ptr<Config> conf_;

    std::string machine_id_;

    std::mutex shm_path_mutex_;
    std::unordered_map<void *, std::string> shm_path_map_;

    std::string cxl_mount_path_;
    // Where owners' CumemHostPool sockets are (published as "cumem:<name>").
    std::string cumem_socket_dir_;
    // Pools whose import failed, and when: retried after a pause, not on
    // every transfer (each attempt costs the owner a connection and fds).
    std::unordered_map<std::string, std::chrono::steady_clock::time_point>
        cumem_import_failed_;

    // transports/shm/device_copy: copy device memory to and from peer DRAM
    // through the platform runtime (one batched submission per sub-batch).
    bool device_copy_ = false;
    bool register_peer_memory_ = false;
    uint64_t register_chunk_bytes_ = 1ULL << 30;
    // Registration costs kernel memory and device page tables per process
    // (a few MiB per GiB each); 0 = unlimited.
    uint64_t register_max_chunks_ = 0;
    std::atomic<uint64_t> use_clock_{0};

    struct AheadWork {
        std::weak_ptr<OpenedShmEntry> entry;
        size_t first;
        size_t count;
        void *device_hint;
    };
    uint64_t register_ahead_chunks_ = 0;
    std::thread ahead_thread_;
    std::mutex ahead_mutex_;
    std::condition_variable ahead_cv_;
    std::queue<AheadWork> ahead_queue_;
    bool ahead_stop_ = false;

    uint64_t reap_interval_ms_ = 0;
    std::thread reaper_;
    std::mutex reaper_mutex_;
    std::condition_variable reaper_cv_;
    bool reaper_stop_ = false;

    static constexpr int kShmCreateMaxRetries = 8;
};

}  // namespace tent
}  // namespace mooncake

#endif  // SHM_TRANSPORT_H_
