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

#include "tent/transport/shm/shm_transport.h"

#include <bits/stdint-uintn.h>
#include <errno.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <poll.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <memory>
#include <sstream>
#include <unordered_map>

#include "tent/common/cumem_host_pool.h"
#include "tent/common/status.h"
#include "tent/runtime/slab.h"
#include "tent/runtime/control_plane.h"
#include "tent/common/utils/random.h"
#include "tent/common/utils/string_builder.h"

namespace mooncake {
namespace tent {

ShmTransport::ShmTransport() : installed_(false) {}

ShmTransport::~ShmTransport() { uninstall(); }

Status ShmTransport::install(std::string &local_segment_name,
                             std::shared_ptr<ControlService> metadata,
                             std::shared_ptr<Topology> local_topology,
                             std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "SHM transport has been installed" LOC_MARK);
    }

    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    conf_ = conf;
    machine_id_ = metadata->segmentManager().getLocal()->machine_id;
    installed_ = true;
    cxl_mount_path_ = conf_->get("transports/shm/cxl_mount_path", "");
    cumem_socket_dir_ = conf_->get("transports/shm/cumem_socket_dir", "");
    caps.dram_to_dram = true;
    // The platform copy already handles device pointers (cudaMemcpy), so a
    // same-host peer's DRAM is reachable from GPU memory without a NIC.
    device_copy_ = conf_->get("transports/shm/device_copy", false) &&
                   Platform::getLoader().type() == "cuda";
    if (device_copy_) {
        caps.gpu_to_dram = true;
        caps.dram_to_gpu = true;
    }
    register_peer_memory_ =
        conf_->get("transports/shm/register_peer_memory", device_copy_);
    register_chunk_bytes_ = std::max<uint64_t>(
        conf_->get("transports/shm/register_chunk_bytes", 1ULL << 30),
        4096);
    register_max_chunks_ =
        conf_->get("transports/shm/register_max_bytes", 0ULL) /
        register_chunk_bytes_;
    register_ahead_chunks_ =
        conf_->get("transports/shm/register_ahead_chunks", 0ULL);
    if (register_ahead_chunks_ > 0) {
        ahead_stop_ = false;
        ahead_thread_ = std::thread([this] { registerAheadLoop(); });
    }
    reap_interval_ms_ = conf_->get("transports/shm/reap_interval_ms",
                                   device_copy_ ? 2000ULL : 0ULL);
    if (reap_interval_ms_ > 0) {
        reaper_stop_ = false;
        reaper_ = std::thread([this] {
            std::unique_lock<std::mutex> lock(reaper_mutex_);
            while (!reaper_cv_.wait_for(
                lock, std::chrono::milliseconds(reap_interval_ms_),
                [this] { return reaper_stop_; })) {
                lock.unlock();
                reapUnlinkedMappings();
                lock.lock();
            }
        });
    }
    LOG(INFO) << "SHM transport installed: cxl_mount_path="
              << (cxl_mount_path_.empty() ? "<posix shm>" : cxl_mount_path_)
              << " device_copy=" << device_copy_
              << " register_peer_memory=" << register_peer_memory_
              << " register_max_chunks=" << register_max_chunks_
              << " register_ahead_chunks=" << register_ahead_chunks_
              << " reap_interval_ms=" << reap_interval_ms_;
    return Status::OK();
}

Status ShmTransport::uninstall() {
    if (installed_) {
        if (reaper_.joinable()) {
            {
                std::lock_guard<std::mutex> lock(reaper_mutex_);
                reaper_stop_ = true;
            }
            reaper_cv_.notify_all();
            reaper_.join();
        }
        if (ahead_thread_.joinable()) {
            {
                std::lock_guard<std::mutex> lock(ahead_mutex_);
                ahead_stop_ = true;
            }
            ahead_cv_.notify_all();
            ahead_thread_.join();
        }
        RWSpinlock::WriteGuard guard(relocate_lock_);
        metadata_.reset();
        relocate_map_.clear();  // ~OpenedShmEntry unmaps each mapping
        installed_ = false;
    }
    return Status::OK();
}

ShmTransport::OpenedShmEntry::~OpenedShmEntry() {
    if (cumem) {
        cumem.reset();  // unmaps; device access needs no undo
        return;
    }
    for (size_t i = 0; i < num_chunks; ++i) {
        const int device = chunk_device[i];
        if (device < 0) continue;
        auto *chunk = static_cast<char *>(shm_addr) + i * chunk_bytes;
        auto status = Platform::getLoader().unregisterHostMemory(chunk, device);
        if (!status.ok())
            LOG(WARNING) << "Failed to unregister peer shared memory chunk: "
                         << status.ToString();
    }
    if (shm_addr) munmap(shm_addr, length);
    if (fd >= 0) close(fd);
}

Status ShmTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    auto shm_batch = Slab<ShmSubBatch>::Get().allocate();
    if (!shm_batch)
        return Status::InternalError("Unable to allocate SHM sub-batch");
    batch = shm_batch;
    shm_batch->task_list.reserve(max_size);
    shm_batch->max_size = max_size;
    return Status::OK();
}

Status ShmTransport::freeSubBatch(SubBatchRef &batch) {
    auto shm_batch = dynamic_cast<ShmSubBatch *>(batch);
    if (!shm_batch)
        return Status::InvalidArgument("Invalid SHM sub-batch" LOC_MARK);
    Slab<ShmSubBatch>::Get().deallocate(shm_batch);
    batch = nullptr;
    return Status::OK();
}

Status ShmTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request> &request_list) {
    auto shm_batch = dynamic_cast<ShmSubBatch *>(batch);
    if (!shm_batch)
        return Status::InvalidArgument("Invalid SHM sub-batch" LOC_MARK);
    if (request_list.size() + shm_batch->task_list.size() > shm_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);
    // All-or-nothing submission (see Transport::submitTransferTasks):
    // relocate every request first so any error is reported before a single
    // transfer starts; otherwise a mid-batch relocate failure would leave the
    // earlier requests already copied, and the engine's failover retry would
    // execute them twice.
    std::vector<uint64_t> target_addrs;
    std::vector<std::shared_ptr<OpenedShmEntry>> mappings;
    target_addrs.reserve(request_list.size());
    mappings.reserve(request_list.size());
    for (auto &request : request_list) {
        uint64_t target_addr = request.target_offset;
        std::shared_ptr<OpenedShmEntry> mapping;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            auto status = relocateSharedMemoryAddress(
                target_addr, request.length, request.target_id, &mapping);
            if (!status.ok()) return status;
        }
        target_addrs.push_back(target_addr);
        mappings.push_back(std::move(mapping));
    }
    if (device_copy_) {
        submitBatchedCopies(shm_batch, request_list, target_addrs, mappings);
        return Status::OK();
    }
    for (size_t i = 0; i < request_list.size(); ++i) {
        shm_batch->task_list.push_back(ShmTask{});
        auto &task = shm_batch->task_list[shm_batch->task_list.size() - 1];
        task.target_addr = target_addrs[i];
        task.request = request_list[i];
        task.status_word = TransferStatusEnum::PENDING;
        startTransfer(&task, shm_batch);
    }
    return Status::OK();
}

void ShmTransport::submitBatchedCopies(
    ShmSubBatch *batch, const std::vector<Request> &request_list,
    const std::vector<uint64_t> &target_addrs,
    const std::vector<std::shared_ptr<OpenedShmEntry>> &mappings) {
    // All-or-nothing: the sub-batch is one submission, so a failure fails
    // every task and the engine fails over the whole set.
    auto status = copyRelocated(request_list, target_addrs, mappings);
    if (!status.ok())
        LOG(ERROR) << "SHM batched copy failed: " << status.ToString();
    for (size_t i = 0; i < request_list.size(); ++i) {
        batch->task_list.push_back(ShmTask{});
        auto &task = batch->task_list.back();
        task.target_addr = target_addrs[i];
        task.request = request_list[i];
        task.transferred_bytes = status.ok() ? request_list[i].length : 0;
        task.status_word = status.ok() ? TransferStatusEnum::COMPLETED
                                       : TransferStatusEnum::FAILED;
    }
    batch->notifyProgress();
}

Status ShmTransport::copyRelocated(
    const std::vector<Request> &request_list,
    const std::vector<uint64_t> &target_addrs,
    const std::vector<std::shared_ptr<OpenedShmEntry>> &mappings) {
    // Registration chunks the batch touches, per mapping (only mappings that
    // have chunks: registered ones, and CUDA VMM ones).
    std::unordered_map<OpenedShmEntry *, std::vector<size_t>> touched;
    {
        for (size_t i = 0; i < request_list.size(); ++i) {
            auto *entry = mappings[i].get();
            if (!entry || !entry->num_chunks || !request_list[i].length)
                continue;
            const uint64_t offset =
                target_addrs[i] - reinterpret_cast<uint64_t>(entry->shm_addr);
            auto &chunks = touched[entry];
            for (size_t c = offset / entry->chunk_bytes;
                 c <= (offset + request_list[i].length - 1) / entry->chunk_bytes;
                 ++c)
                if (chunks.empty() || chunks.back() != c) chunks.push_back(c);
        }
        for (auto &[entry, chunks] : touched) {
            std::sort(chunks.begin(), chunks.end());
            chunks.erase(std::unique(chunks.begin(), chunks.end()),
                         chunks.end());
            pinChunks(*entry, chunks, request_list.front().source);
        }
        if (register_ahead_chunks_ > 0) {
            std::vector<OpenedShmEntry *> done;
            for (size_t i = 0; i < request_list.size() &&
                               done.size() < touched.size();
                 ++i) {
                auto *entry = mappings[i].get();
                auto it = touched.find(entry);
                if (it == touched.end() || it->second.empty() ||
                    std::find(done.begin(), done.end(), entry) != done.end())
                    continue;
                registerAhead(mappings[i], it->second.back(),
                              request_list.front().source);
                done.push_back(entry);
            }
        }
    }

    std::vector<CopyOp> ops;
    ops.reserve(request_list.size());
    for (size_t i = 0; i < request_list.size(); ++i) {
        const auto &request = request_list[i];
        // A copy must stay inside one cudaHostRegister region, so split it
        // at registration chunk boundaries.
        const uint64_t chunk = mappings[i] && mappings[i]->num_chunks
                                   ? mappings[i]->chunk_bytes
                                   : 0;
        const uint64_t base =
            mappings[i] ? reinterpret_cast<uint64_t>(mappings[i]->shm_addr)
                        : 0;
        uint64_t done = 0;
        while (done < request.length) {
            const uint64_t peer_addr = target_addrs[i] + done;
            uint64_t piece = request.length - done;
            if (chunk)
                piece = std::min(piece,
                                 chunk - (peer_addr - base) % chunk);
            auto *peer = reinterpret_cast<void *>(peer_addr);
            auto *local = static_cast<char *>(request.source) + done;
            // Pinned above, so the chunk stays registered for the copy.
            const int mapped_device =
                chunk ? mappings[i]->chunk_device[(peer_addr - base) / chunk]
                      : -1;
            // Unregistered peer memory is still a plain mapping the device
            // may reach through HMM; a CUDA VMM one only through its device.
            const bool vmm = mappings[i] && mappings[i]->cumem;
            if (vmm && mapped_device < 0) {
                for (auto &[entry, chunks] : touched) unpinChunks(*entry, chunks);
                return Status::NotImplemented(
                    "CUDA VMM peer memory needs a device source" LOC_MARK);
            }
            if (request.opcode == Request::READ)
                ops.push_back({local, peer, piece, mapped_device, true,
                               mappings[i] != nullptr && !vmm, vmm});
            else
                ops.push_back({peer, local, piece, mapped_device, false,
                               mappings[i] != nullptr && !vmm, vmm});
            done += piece;
        }
    }
    auto status = Platform::getLoader().copyBatch(ops);
    for (auto &[entry, chunks] : touched) unpinChunks(*entry, chunks);
    return status;
}

Status ShmTransport::copyDirect(const std::vector<Request> &request_list) {
    if (!installed_ || !device_copy_)
        return Status::NotImplemented("SHM device copy is off" LOC_MARK);
    std::vector<uint64_t> target_addrs;
    std::vector<std::shared_ptr<OpenedShmEntry>> mappings;
    target_addrs.reserve(request_list.size());
    mappings.reserve(request_list.size());
    SegmentID checked = LOCAL_SEGMENT_ID;
    for (const auto &request : request_list) {
        if (request.target_id == LOCAL_SEGMENT_ID)
            return Status::NotImplemented("local segment target" LOC_MARK);
        if (request.target_id != checked) {
            // Only peers on this host: a same-named file elsewhere is not it.
            SegmentDescRef desc;
            auto status = metadata_->segmentManager().getRemoteCached(
                desc, request.target_id);
            if (!status.ok() || !desc || desc->machine_id != machine_id_)
                return Status::NotImplemented("target not on this host" LOC_MARK);
            checked = request.target_id;
        }
        uint64_t target_addr = request.target_offset;
        std::shared_ptr<OpenedShmEntry> mapping;
        auto status = relocateSharedMemoryAddress(target_addr, request.length,
                                                  request.target_id, &mapping);
        if (!status.ok())
            return Status::NotImplemented(std::string("not shared memory: ") +
                                          std::string(status.message()));
        target_addrs.push_back(target_addr);
        mappings.push_back(std::move(mapping));
    }
    return copyRelocated(request_list, target_addrs, mappings);
}

void ShmTransport::pinChunks(OpenedShmEntry &entry,
                             const std::vector<size_t> &chunks,
                             void *device_hint) {
    std::lock_guard<std::mutex> lock(entry.register_mutex);
    const uint64_t now = ++use_clock_;
    for (auto c : chunks) {
        entry.chunk_pins[c].fetch_add(1);
        entry.chunk_last_use[c] = now;
    }
    for (auto c : chunks) {
        if (entry.chunk_device[c] != kChunkUnregistered) continue;
        // Device access to a CUDA VMM mapping is cheap to keep: no eviction.
        while (register_max_chunks_ && !entry.cumem &&
               entry.registered_chunks >= register_max_chunks_) {
            size_t victim = entry.num_chunks;
            for (size_t i = 0; i < entry.num_chunks; ++i)
                if (entry.chunk_device[i] >= 0 && !entry.chunk_pins[i] &&
                    (victim == entry.num_chunks ||
                     entry.chunk_last_use[i] < entry.chunk_last_use[victim]))
                    victim = i;
            if (victim == entry.num_chunks) break;  // all in use: go over
            auto status = Platform::getLoader().unregisterHostMemory(
                static_cast<char *>(entry.shm_addr) +
                    victim * entry.chunk_bytes,
                entry.chunk_device[victim]);
            if (!status.ok())
                LOG(WARNING) << "Failed to unregister peer shared memory "
                                "chunk: "
                             << status.ToString();
            entry.chunk_device[victim] = kChunkUnregistered;
            --entry.registered_chunks;
        }
        int device = -1;
        auto status = registerChunk(entry, c, device_hint, device);
        // A host source has no device to register for; leave the chunks for
        // a batch that has one.
        if (status.code() == Status::Code::kInvalidArgument) return;
        if (!status.ok()) {
            LOG_FIRST_N(WARNING, 8)
                << "Peer shared memory chunk stays unregistered (copies "
                   "bounce through driver staging): "
                << status.ToString();
        }
        entry.chunk_device[c] = device;
        if (device >= 0) ++entry.registered_chunks;
    }
}

void ShmTransport::registerAhead(const std::shared_ptr<OpenedShmEntry> &entry,
                                 size_t last, void *device_hint) {
    // A window covering the whole mapping means: register all of it, once.
    // Once the store is full, new objects land in holes anywhere in it.
    if (register_ahead_chunks_ >= entry->num_chunks) {
        size_t expected = 0;
        if (!entry->ahead_until.compare_exchange_strong(expected,
                                                        entry->num_chunks))
            return;
        {
            std::lock_guard<std::mutex> lock(ahead_mutex_);
            ahead_queue_.push({entry, 0, entry->num_chunks, device_hint});
        }
        ahead_cv_.notify_one();
        return;
    }
    const size_t first = last + 1;
    if (first >= entry->num_chunks) return;
    const size_t until =
        std::min<size_t>(entry->num_chunks, first + register_ahead_chunks_);
    // Re-queue only once the frontier has moved by a quarter of the window.
    size_t queued = entry->ahead_until.load();
    if (queued >= first + register_ahead_chunks_ / 4 && queued > first)
        return;
    if (!entry->ahead_until.compare_exchange_strong(queued, until)) return;
    {
        std::lock_guard<std::mutex> lock(ahead_mutex_);
        ahead_queue_.push({entry, first, until - first, device_hint});
    }
    ahead_cv_.notify_one();
}

void ShmTransport::registerAheadLoop() {
    std::unique_lock<std::mutex> lock(ahead_mutex_);
    while (true) {
        ahead_cv_.wait(lock,
                       [this] { return ahead_stop_ || !ahead_queue_.empty(); });
        if (ahead_stop_) return;
        auto work = ahead_queue_.front();
        ahead_queue_.pop();
        lock.unlock();
        for (size_t c = work.first; c < work.first + work.count; ++c) {
            auto entry = work.entry.lock();
            if (!entry || ahead_stop_) break;
            std::lock_guard<std::mutex> guard(entry->register_mutex);
            if (entry->chunk_device[c] != kChunkUnregistered) continue;
            // Speculative: never evict for it.
            if (register_max_chunks_ && !entry->cumem &&
                entry->registered_chunks >= register_max_chunks_)
                break;
            int device = -1;
            auto status = registerChunk(*entry, c, work.device_hint, device);
            if (status.code() == Status::Code::kInvalidArgument) break;
            entry->chunk_device[c] = device;
            entry->chunk_last_use[c] = ++use_clock_;
            if (device >= 0) ++entry->registered_chunks;
        }
        if (work.first == 0 && work.count > 1)
            LOG(INFO) << "SHM registered peer mapping ahead: " << work.count
                      << " chunks queued";
        lock.lock();
    }
}

void ShmTransport::unpinChunks(OpenedShmEntry &entry,
                               const std::vector<size_t> &chunks) {
    for (auto c : chunks) entry.chunk_pins[c].fetch_sub(1);
}

Status ShmTransport::registerChunk(OpenedShmEntry &entry, size_t c,
                                   void *device_hint, int &device) {
    const uint64_t offset = c * entry.chunk_bytes;
    const uint64_t length =
        std::min<uint64_t>(entry.chunk_bytes, entry.length - offset);
    if (entry.cumem)
        return entry.cumem->EnableDeviceAccess(offset, length, device_hint,
                                               device);
    return Platform::getLoader().registerHostMemory(
        static_cast<char *>(entry.shm_addr) + offset, length, device_hint,
        device);
}

void ShmTransport::reapUnlinkedMappings() {
    // Drop mappings whose backing file the owner has unlinked (it exited or
    // freed the buffer). Holding them would keep its memory, e.g. HugeTLB
    // pages a restarted owner needs, until this process exits. Entries are
    // released outside the lock; copies in flight keep theirs alive.
    std::vector<std::shared_ptr<OpenedShmEntry>> released;
    {
        RWSpinlock::WriteGuard guard(relocate_lock_);
        for (auto target = relocate_map_.begin();
             target != relocate_map_.end();) {
            auto &mappings = target->second;
            for (auto it = mappings.begin(); it != mappings.end();) {
                struct stat st;
                bool gone = false;
                if (it->second->cumem) {
                    // The owner sends nothing after the layout: readable
                    // means EOF, i.e. it exited.
                    pollfd p{it->second->cumem->connection_fd(),
                             POLLIN | POLLRDHUP, 0};
                    gone = poll(&p, 1, 0) > 0 && p.revents != 0;
                } else {
                    gone = it->second->fd >= 0 &&
                           fstat(it->second->fd, &st) == 0 && st.st_nlink == 0;
                }
                if (gone) {
                    released.push_back(std::move(it->second));
                    it = mappings.erase(it);
                } else {
                    ++it;
                }
            }
            target = mappings.empty() ? relocate_map_.erase(target)
                                      : std::next(target);
        }
    }
    for (auto &entry : released)
        LOG(INFO) << "Released unlinked peer shared memory "
                  << entry->shm_addr << " (" << entry->length << " bytes)";
}

void ShmTransport::startTransfer(ShmTask *task, ShmSubBatch *batch) {
    Status status;
    if (task->request.opcode == Request::READ)
        status = Platform::getLoader().copy(task->request.source,
                                            (void *)task->target_addr,
                                            task->request.length);
    else
        status = Platform::getLoader().copy((void *)task->target_addr,
                                            task->request.source,
                                            task->request.length);
    if (status.ok()) {
        task->transferred_bytes = task->request.length;
        task->status_word = TransferStatusEnum::COMPLETED;
    } else {
        task->status_word = TransferStatusEnum::FAILED;
    }
    batch->notifyProgress();
}

Status ShmTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                       TransferStatus &status) {
    auto shm_batch = dynamic_cast<ShmSubBatch *>(batch);
    if (task_id < 0 || task_id >= (int)shm_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto &task = shm_batch->task_list[task_id];
    status = TransferStatus{task.status_word, task.transferred_bytes};
    return Status::OK();
}

Status ShmTransport::addMemoryBuffer(BufferDesc &desc,
                                     const MemoryOptions &options) {
    desc.shm_path = options.shm_path;
    if (desc.shm_path.empty()) {
        // The basic allocation API cannot return MemoryOptions to the caller.
        std::lock_guard<std::mutex> lock(shm_path_mutex_);
        auto it = shm_path_map_.find(reinterpret_cast<void *>(desc.addr));
        if (it != shm_path_map_.end()) desc.shm_path = it->second;
    }
    if (desc.shm_path.empty()) {
        auto name = CumemHostPool::SocketNameFor(desc.addr, desc.length);
        if (!name.empty()) desc.shm_path = "cumem:" + name;
    }
    if (desc.shm_path.empty()) desc.shm_path = findSharedMappingPath(desc);
    if (desc.shm_path.empty()) return Status::OK();
    desc.transports.push_back(TransportType::SHM);
    // desc.shm_offset = options.shm_offset;
    LOG(INFO) << "Registered shared memory: " << (void *)desc.addr << "--"
              << (void *)(desc.addr + desc.length);
    return Status::OK();
}

std::string ShmTransport::findSharedMappingPath(const BufferDesc &desc) const {
    if (cxl_mount_path_.empty() || desc.length == 0) return "";
    std::string prefix = cxl_mount_path_;
    if (prefix.back() != '/') prefix += '/';
    std::ifstream maps("/proc/self/maps");
    std::string line, path;
    uint64_t covered_end = 0;
    while (std::getline(maps, line)) {
        // start-end perms offset dev inode [path]
        std::istringstream fields(line);
        std::string range, perms, offset_hex, dev, inode, name;
        fields >> range >> perms >> offset_hex >> dev >> inode;
        std::getline(fields >> std::ws, name);
        const auto dash = range.find('-');
        if (dash == std::string::npos || perms.size() < 4) continue;
        const uint64_t start = std::stoull(range.substr(0, dash), nullptr, 16);
        const uint64_t end = std::stoull(range.substr(dash + 1), nullptr, 16);
        const uint64_t offset = std::stoull(offset_hex, nullptr, 16);
        if (covered_end == 0) {
            if (start != desc.addr || offset != 0 || perms[3] != 's' ||
                name.compare(0, prefix.size(), prefix) != 0 ||
                name.find(" (deleted)") != std::string::npos)
                continue;
            path = name;
            covered_end = end;
        } else if (start == covered_end && name == path &&
                   offset == start - desc.addr) {
            covered_end = end;  // mlock/madvise can split one mapping
        } else {
            break;
        }
        if (covered_end >= desc.addr + desc.length) break;
    }
    if (covered_end < desc.addr + desc.length) return "";
    LOG(INFO) << "Shared mapping " << path << " backs buffer "
              << (void *)desc.addr;
    return path.substr(prefix.size());
}

Status ShmTransport::removeMemoryBuffer(BufferDesc &desc) {
    desc.shm_path.clear();
    return Status::OK();
}

static inline std::string randomFileName() {
    // Include pid so concurrent processes rarely collide and leftovers are
    // easier to attribute during debugging.
    std::string result =
        "mooncake_" + std::to_string(static_cast<long>(getpid())) + "_";
    for (int i = 0; i < 8; ++i) result += 'a' + SimpleRandom::Get().next(26);
    return result;
}

static inline std::string joinPath(const std::string &path,
                                   const std::string &filename) {
    if (path.empty()) return filename;
    if (path[path.size() - 1] == '/') return path + filename;
    return path + "/" + filename;
}

Status ShmTransport::allocateLocalMemory(void **addr, size_t size,
                                         MemoryOptions &options) {
    LocationParser location(options.location);
    if (location.type() != "cpu") {
        return Status::InvalidArgument("ShmTransport allocates DRAM only");
    }
    options.shm_offset = 0;
    for (int attempt = 0; attempt < kShmCreateMaxRetries; ++attempt) {
        options.shm_path = randomFileName();
        *addr = createSharedMemory(options.shm_path, size);
        if (*addr) {
            return Status::OK();
        }
        // createSharedMemory returns nullptr on EEXIST or other failures.
        // Only retry when the name collided with an existing object.
        if (errno != EEXIST) {
            break;
        }
    }
    return Status::InternalError("Failed to allocate shared memory");
}

Status ShmTransport::freeLocalMemory(void *addr, size_t size) {
    std::lock_guard<std::mutex> lock(shm_path_mutex_);
    if (!shm_path_map_.count(addr)) {
        return Status::InvalidArgument("Memory not allocated by ShmTransport");
    }
    munmap(addr, size);
    if (cxl_mount_path_.empty())
        shm_unlink(shm_path_map_[addr].c_str());
    else {
        auto full_path = joinPath(cxl_mount_path_, shm_path_map_[addr]);
        unlink(full_path.c_str());
    }
    shm_path_map_.erase(addr);
    return Status::OK();
}

void *ShmTransport::createSharedMemory(const std::string &path, size_t size) {
    int shm_fd = -1;
    // O_EXCL prevents silently opening and truncating an existing object that
    // another process may still be using (which would SIGBUS that peer).
    if (cxl_mount_path_.empty())
        shm_fd = shm_open(path.c_str(), O_CREAT | O_EXCL | O_RDWR, 0644);
    else {
        auto full_path = joinPath(cxl_mount_path_, path);
        shm_fd = open(full_path.c_str(), O_CREAT | O_EXCL | O_RDWR, 0644);
    }
    if (shm_fd == -1) {
        // Preserve errno for allocateLocalMemory retry decisions (EEXIST).
        if (errno != EEXIST) {
            PLOG(ERROR) << "Failed to open shared memory file";
        }
        return nullptr;
    }

    if (ftruncate64(shm_fd, size) == -1) {
        PLOG(ERROR) << "Failed to truncate shared memory file";
        close(shm_fd);
        if (cxl_mount_path_.empty())
            shm_unlink(path.c_str());
        else
            unlink(joinPath(cxl_mount_path_, path).c_str());
        return nullptr;
    }

    void *mapped_addr =
        mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (mapped_addr == MAP_FAILED) {
        PLOG(ERROR) << "Failed to map shared memory file";
        close(shm_fd);
        if (cxl_mount_path_.empty())
            shm_unlink(path.c_str());
        else
            unlink(joinPath(cxl_mount_path_, path).c_str());
        return nullptr;
    }

    close(shm_fd);
    std::lock_guard<std::mutex> lock(shm_path_mutex_);
    shm_path_map_[mapped_addr] = path;
    return mapped_addr;
}

bool ShmTransport::tryResolve(const RelocateMap &relocate_map,
                              uint64_t &dest_addr, uint64_t length,
                              std::shared_ptr<OpenedShmEntry> *mapping) {
    for (const auto &entry : relocate_map) {
        if (entry.first <= dest_addr &&
            dest_addr + length <= entry.first + entry.second->length) {
            dest_addr = dest_addr - entry.first +
                        reinterpret_cast<uint64_t>(entry.second->shm_addr);
            if (mapping) *mapping = entry.second;
            return true;
        }
    }
    return false;
}

Status ShmTransport::relocateSharedMemoryAddress(
    uint64_t &dest_addr, uint64_t length, uint64_t target_id,
    std::shared_ptr<OpenedShmEntry> *mapping) {
    {
        RWSpinlock::ReadGuard guard(relocate_lock_);
        auto target = relocate_map_.find(target_id);
        if (target != relocate_map_.end() &&
            tryResolve(target->second, dest_addr, length, mapping))
            return Status::OK();
    }

    RWSpinlock::WriteGuard guard(relocate_lock_);
    if (!metadata_) {
        return Status::InvalidArgument(
            "SHM transport is not installed" LOC_MARK);
    }
    // Another thread may have published this mapping while the writer lock was
    // pending. Recheck before opening and mapping the same shared-memory file.
    auto target = relocate_map_.find(target_id);
    if (target != relocate_map_.end() &&
        tryResolve(target->second, dest_addr, length, mapping))
        return Status::OK();

    BufferDesc *buffer;
    // Owning reference: `buffer` is used after the lambda returns.
    SegmentDescRef pin;
    auto &segment_manager = metadata_->segmentManager();
    // Do not munmap relocate_map_ entries on NeedsRefreshCache: memcpy runs
    // after this lock is released, and there is no transfer-level refcount /
    // quiesce that proves no reader still holds a resolved address. POSIX
    // shm_unlink already keeps the object alive for existing mappings; keep
    // those mappings until uninstall().
    CHECK_STATUS(segment_manager.withCachedSegment(
        target_id, pin, [&](SegmentDesc *segment) {
            buffer = segment->findBuffer(dest_addr, length);
            if (!buffer || buffer->shm_path.empty())
                return Status::NeedsRefreshCache(
                    "Requested address is not in registered buffer" LOC_MARK);
            return Status::OK();
        }));

    std::shared_ptr<OpenedShmEntry> opened;
    if (target != relocate_map_.end()) {
        auto found = target->second.find(buffer->addr);
        if (found != target->second.end()) opened = found->second;
    }

    if (!opened && buffer->shm_path.rfind("cumem:", 0) == 0) {
        const auto now = std::chrono::steady_clock::now();
        auto failed = cumem_import_failed_.find(buffer->shm_path);
        if (failed != cumem_import_failed_.end() &&
            now - failed->second < std::chrono::seconds(30))
            return Status::InternalError("CUDA VMM pool import failed recently" LOC_MARK);
        std::unique_ptr<CumemHostMapping> cumem;
        auto import_status = CumemHostMapping::Import(
            joinPath(cumem_socket_dir_, buffer->shm_path.substr(6)), cumem);
        if (!import_status.ok()) {
            if (failed == cumem_import_failed_.end())
                LOG(WARNING) << "SHM: cannot import " << buffer->shm_path
                             << " (retrying every 30 s): " << import_status;
            cumem_import_failed_[buffer->shm_path] = now;
            return import_status;
        }
        cumem_import_failed_.erase(buffer->shm_path);
        if (buffer->addr != cumem->peer_base() ||
            buffer->length > cumem->length())
            return Status::InternalError(
                "CUDA VMM peer buffer does not match its pool" LOC_MARK);
        opened = std::make_shared<OpenedShmEntry>();
        opened->shm_addr = cumem->base();
        opened->length = buffer->length;
        opened->cumem = std::move(cumem);
        opened->chunk_bytes = register_chunk_bytes_;
        opened->num_chunks =
            (buffer->length + register_chunk_bytes_ - 1) / register_chunk_bytes_;
        opened->chunk_device = std::make_unique<int[]>(opened->num_chunks);
        std::fill_n(opened->chunk_device.get(), opened->num_chunks,
                    kChunkUnregistered);
        opened->chunk_last_use =
            std::make_unique<uint64_t[]>(opened->num_chunks);
        opened->chunk_pins =
            std::make_unique<std::atomic<uint32_t>[]>(opened->num_chunks);
        relocate_map_[target_id][buffer->addr] = opened;
    }

    if (!opened) {
        LocationParser location(buffer->location);
        if (location.type() == "cuda") {
            return Status::NotImplemented(
                "CUDA supported not enabled in this package " LOC_MARK);
        } else {
            int shm_fd = -1;
            // Consumer path must never create: a missing object means the peer
            // has not published (or has already unlinked) the shared memory.
            // Creating here would yield a 0-byte file and SIGBUS on access.
            if (cxl_mount_path_.empty())
                shm_fd = shm_open(buffer->shm_path.c_str(), O_RDWR, 0644);
            else {
                auto full_path = joinPath(cxl_mount_path_, buffer->shm_path);
                shm_fd = open(full_path.c_str(), O_RDWR, 0644);
            }
            if (shm_fd < 0) {
                return Status::InternalError(
                    std::string("Failed to open shared memory file ") +
                    buffer->shm_path + LOC_MARK);
            }
            // mmap can succeed even when the backing object is shorter than
            // buffer->length; accessing past EOF then SIGBUS. Reject early.
            struct stat st;
            if (fstat(shm_fd, &st) != 0) {
                close(shm_fd);
                return Status::InternalError(
                    std::string("Failed to fstat shared memory file ") +
                    buffer->shm_path + LOC_MARK);
            }
            if (st.st_size < 0 ||
                static_cast<uint64_t>(st.st_size) < buffer->length) {
                close(shm_fd);
                return Status::InternalError(
                    std::string("Shared memory file shorter than registered "
                                "buffer length: ") +
                    buffer->shm_path + LOC_MARK);
            }
            void *shm_addr = mmap(nullptr, buffer->length,
                                  PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
            if (shm_addr == MAP_FAILED) {
                close(shm_fd);
                return Status::InternalError(
                    "Failed to map shared memory " LOC_MARK);
            }
            LOG(INFO) << "Original shared memory: " << (void *)buffer->addr
                      << "--" << (void *)(buffer->addr + buffer->length);
            LOG(INFO) << "Remapped shared memory: " << (void *)shm_addr << "--"
                      << (void *)((uintptr_t)shm_addr + buffer->length);
            opened = std::make_shared<OpenedShmEntry>();
            opened->shm_addr = shm_addr;
            opened->length = buffer->length;
            if (reap_interval_ms_ > 0)
                opened->fd = shm_fd;
            else
                close(shm_fd);
            if (register_peer_memory_) {
                opened->chunk_bytes = register_chunk_bytes_;
                opened->num_chunks =
                    (buffer->length + register_chunk_bytes_ - 1) /
                    register_chunk_bytes_;
                opened->chunk_device =
                    std::make_unique<int[]>(opened->num_chunks);
                std::fill_n(opened->chunk_device.get(), opened->num_chunks,
                            kChunkUnregistered);
                opened->chunk_last_use =
                    std::make_unique<uint64_t[]>(opened->num_chunks);
                opened->chunk_pins = std::make_unique<std::atomic<uint32_t>[]>(
                    opened->num_chunks);
            }
            relocate_map_[target_id][buffer->addr] = opened;
        }
    }

    dest_addr = dest_addr - buffer->addr +
                reinterpret_cast<uint64_t>(opened->shm_addr);
    if (mapping) *mapping = opened;
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
