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

#include "tent/platform/cuda.h"
#include "tent/platform/cuda_utils.h"
#include "tent/common/status.h"
#ifdef TENT_BATCHED_COPY_KERNEL
#include "batched_copy.h"
#endif

#include <bits/stdint-uintn.h>
#include <cuda.h>
#include <cuda_runtime.h>
#include <glog/logging.h>
#include <mutex>
#include <unordered_map>
#include <numa.h>
#include <vector>
#include <cstring>

namespace mooncake {
namespace tent {
namespace {

// Driver-API probe: true iff this process already has a primary context on
// `device`. Does not create one. Topology lists every visible GPU for NIC
// affinity; cudaSetDevice on those names would allocate idle-card contexts
// when the process can see more GPUs than this rank uses.
bool cudaPrimaryContextIsActive(int device) {
    if (!ensureCudaDriverInit()) return false;
    CUdevice cu_dev = 0;
    if (cuDeviceGet(&cu_dev, device) != CUDA_SUCCESS) return false;
    unsigned int flags = 0;
    int active = 0;
    if (cuDevicePrimaryCtxGetState(cu_dev, &flags, &active) != CUDA_SUCCESS) {
        return false;
    }
    return active != 0;
}

// Bare cudaGetDevice() can implicitly create GPU 0 when this thread has no
// current context. Only save/restore the caller's device when one exists.
bool cudaHasCurrentContext() {
    if (!ensureCudaDriverInit()) return false;
    CUcontext ctx = nullptr;
    return cuCtxGetCurrent(&ctx) == CUDA_SUCCESS && ctx != nullptr;
}

// Pins `device` on the calling thread for one scope, then restores the
// caller's exact context (see CudaPlatform::copy for why a device ordinal
// alone cannot do that).
// Device ranges seen by one copyBatch() call, so each allocation is looked up
// once instead of once per copy. lookup() returns the owning device, or -1.
class DeviceRangeCache {
   public:
    int lookup(const void* ptr) {
        const auto addr = reinterpret_cast<uint64_t>(ptr);
        for (const auto& range : ranges_)
            if (addr >= range.start && addr < range.end) return range.device;
        if (!ensureCudaDriverInit()) return -1;
        unsigned int mem_type = 0;
        int ordinal = -1;
        CUdeviceptr start = 0;
        size_t size = 0;
        CUpointer_attribute attrs[] = {CU_POINTER_ATTRIBUTE_MEMORY_TYPE,
                                       CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL,
                                       CU_POINTER_ATTRIBUTE_RANGE_START_ADDR,
                                       CU_POINTER_ATTRIBUTE_RANGE_SIZE};
        void* data[] = {&mem_type, &ordinal, &start, &size};
        if (cuPointerGetAttributes(4, attrs, data,
                                   static_cast<CUdeviceptr>(addr)) !=
                CUDA_SUCCESS ||
            mem_type != CU_MEMORYTYPE_DEVICE)
            return -1;
        if (size > 0) ranges_.push_back({start, start + size, ordinal});
        return ordinal;
    }

   private:
    struct Range {
        uint64_t start;
        uint64_t end;
        int device;
    };
    std::vector<Range> ranges_;
};

class ScopedCudaDevice {
   public:
    explicit ScopedCudaDevice(int device) : device_(device) {
        if (ensureCudaDriverInit() &&
            cuCtxGetCurrent(&caller_ctx_) != CUDA_SUCCESS) {
            status_ = Status::InternalError(
                std::string("cuCtxGetCurrent failed") + LOC_MARK);
            return;
        }
        if (caller_ctx_ != nullptr &&
            cudaGetDevice(&caller_device_) != cudaSuccess) {
            caller_device_ = -1;
            (void)cudaGetLastError();
        }
        active_ = true;
        cudaError_t err = cudaSetDevice(device);
        if (err != cudaSuccess) {
            status_ = Status::InternalError(std::string("cudaSetDevice: ") +
                                            cudaGetErrorString(err) + LOC_MARK);
        }
    }

    ~ScopedCudaDevice() {
        if (!active_) return;
        if (caller_device_ != -1 && caller_device_ != device_)
            (void)cudaSetDevice(caller_device_);
        (void)cuCtxSetCurrent(caller_ctx_);
    }

    const Status& status() const { return status_; }

   private:
    int device_;
    CUcontext caller_ctx_ = nullptr;
    int caller_device_ = -1;
    bool active_ = false;
    Status status_ = Status::OK();
};

}  // namespace

Status CudaPlatform::allocate(void** pptr, size_t size,
                              MemoryOptions& options) {
    LocationParser location(options.location);
    if (location.type() == "cuda") {
        int cuda_dev = 0;
        CHECK_CUDA(cudaGetDevice(&cuda_dev));
        CHECK_CUDA(cudaSetDevice(location.index()));
        CHECK_CUDA(cudaMalloc(pptr, size));
        cudaSetDevice(cuda_dev);
        return Status::OK();
    }
    int socket_id = 0;
    if (location.type() == "cpu") socket_id = location.index();
    *pptr = numa_alloc_onnode(size, socket_id);
    if (!(*pptr))
        return Status::InternalError("Unable to allocate DRAM memory");
    return Status::OK();
}

Status CudaPlatform::free(void* ptr, size_t size) {
    if (getCudaDeviceForPtr(ptr) >= 0) {
        CHECK_CUDA(cudaFree(ptr));
    } else {
        numa_free(ptr, size);
    }
    return Status::OK();
}

Status CudaPlatform::allocatePinnedHost(void** pptr, size_t size) {
    // Portable, so copies against any device's memory run as DMA. Like any
    // CUDA runtime call this initializes the calling thread's current device
    // if it has no context yet.
    CHECK_CUDA(cudaHostAlloc(pptr, size, cudaHostAllocPortable));
    return Status::OK();
}

Status CudaPlatform::freePinnedHost(void* ptr, size_t size) {
    (void)size;
    CHECK_CUDA(cudaFreeHost(ptr));
    return Status::OK();
}

Status CudaPlatform::copy(void* dst, void* src, size_t length) {
    // Use cudaMemcpyAsync with a non-blocking stream instead of cudaMemcpy(),
    // as the latter relies on the legacy default stream and can introduce
    // unintended synchronization or even deadlocks in downstream
    // components (e.g. mooncake-pg).
    //
    // cudaMemcpyAsync routes the copy through its stream's device context, so
    // the stream must live on the device owning the device-side buffer.
    // Control-plane RPC worker threads sit on cuda:0 while a registered buffer
    // may live on cuda:R; taking the stream from the buffer's device routes the
    // copy correctly without mutating the calling thread's current device.
    // Host-only copies keep the current device.
    int device_id = getPointerDeviceId(dst);
    if (device_id == CUDAStreamPool::kCurrentDevice) {
        device_id = getPointerDeviceId(src);
    }
    if (device_id == CUDAStreamPool::kCurrentDevice) {
        // Host-to-host: plain memcpy. The stream pool would create a primary
        // context on the thread's default device (GPU 0).
        ::memcpy(dst, src, length);
        return Status::OK();
    }

    // cudaMemcpyAsync() validates the stream against the current device's
    // context, so the copy has to run with the buffer's device pinned. A
    // device ordinal alone cannot restore the caller afterwards: it cannot
    // tell a custom non-primary CUcontext apart from the primary one, and a
    // thread that had no current context must not be left holding the one
    // this copy creates. Save the exact identity through the driver API.
    CUcontext caller_ctx = nullptr;
    if (ensureCudaDriverInit() &&
        cuCtxGetCurrent(&caller_ctx) != CUDA_SUCCESS) {
        return Status::InternalError(std::string("cuCtxGetCurrent failed") +
                                     LOC_MARK);
    }
    // Bare cudaGetDevice() can implicitly create GPU 0 when this thread has
    // no current context; only query the runtime binding when one exists.
    int caller_device = -1;
    if (caller_ctx != nullptr && cudaGetDevice(&caller_device) != cudaSuccess) {
        caller_device = -1;
        (void)cudaGetLastError();
    }

    Status status;
    cudaError_t err = cudaSetDevice(device_id);
    if (err != cudaSuccess) {
        status = Status::InternalError(std::string("cudaSetDevice: ") +
                                       cudaGetErrorString(err) + LOC_MARK);
    } else {
        // The handle returns the stream to the pool at the end of this scope,
        // before the caller's state is restored below.
        CUDAStreamHandle stream;
        status = getStreamFromPool(stream, device_id);
        if (status.ok()) {
            err = cudaMemcpyAsync(dst, src, length, cudaMemcpyDefault,
                                  stream.get());
            if (err != cudaSuccess) {
                status =
                    Status::InternalError(std::string("cudaMemcpyAsync: ") +
                                          cudaGetErrorString(err) + LOC_MARK);
            }
        }
        if (status.ok()) {
            err = cudaStreamSynchronize(stream.get());
            if (err != cudaSuccess) {
                status = Status::InternalError(
                    std::string("cudaStreamSynchronize: ") +
                    cudaGetErrorString(err) + LOC_MARK);
            }
        }
    }

    // Restore on every exit path, success or failure. cudaSetDevice() first
    // brings the runtime binding back (and may swap in that device's primary
    // context), then cuCtxSetCurrent() restores the exact context identity: a
    // custom CUcontext, or none at all for a caller that started bare.
    if (caller_device != -1 && caller_device != device_id) {
        err = cudaSetDevice(caller_device);
        if (status.ok() && err != cudaSuccess) {
            status =
                Status::InternalError(std::string("cudaSetDevice(restore): ") +
                                      cudaGetErrorString(err) + LOC_MARK);
        }
    }
    const CUresult restore_rc = cuCtxSetCurrent(caller_ctx);
    if (status.ok() && restore_rc != CUDA_SUCCESS) {
        status = Status::InternalError(
            std::string("cuCtxSetCurrent failed to restore the caller "
                        "context") +
            LOC_MARK);
    }
    if (!status.ok()) {
        LOG(ERROR) << "CudaPlatform::copy failed: " << status.message();
    }
    return status;
}

Status CudaPlatform::synchronizeDevices(const Topology* topology) {
    const std::vector<int> devices =
        topologyDeviceIndices(topology, Topology::MEM_CUDA);
    if (devices.empty()) return Status::OK();

    int device_count = 0;
    cudaError_t err = cudaGetDeviceCount(&device_count);
    if (err != cudaSuccess || device_count <= 0) {
        if (err != cudaSuccess) {
            LOG(WARNING) << "CudaPlatform::synchronizeDevices "
                            "cudaGetDeviceCount failed: "
                         << cudaGetErrorString(err);
            (void)cudaGetLastError();
        }
        return Status::OK();
    }

    int saved = 0;
    const bool have_saved =
        cudaHasCurrentContext() && cudaGetDevice(&saved) == cudaSuccess;
    if (!have_saved) (void)cudaGetLastError();

    for (int device : devices) {
        if (device >= device_count) continue;
        if (!cudaPrimaryContextIsActive(device)) continue;
        err = cudaSetDevice(device);
        if (err != cudaSuccess) {
            LOG(WARNING) << "CudaPlatform::synchronizeDevices cudaSetDevice("
                         << device << ") failed: " << cudaGetErrorString(err);
            (void)cudaGetLastError();
            continue;
        }
        err = cudaDeviceSynchronize();
        if (err != cudaSuccess) {
            LOG(WARNING)
                << "CudaPlatform::synchronizeDevices cudaDeviceSynchronize "
                   "device "
                << device << " failed: " << cudaGetErrorString(err);
            (void)cudaGetLastError();
        }
    }
    if (have_saved) {
        err = cudaSetDevice(saved);
        if (err != cudaSuccess) {
            LOG(WARNING)
                << "CudaPlatform::synchronizeDevices restore cudaSetDevice("
                << saved << ") failed: " << cudaGetErrorString(err);
            (void)cudaGetLastError();
        }
    }
    return Status::OK();
}

Status CudaPlatform::copyBatch(const std::vector<CopyOp>& ops) {
    // Group by the device owning either side; host-to-host copies stay memcpy.
    // Copies whose host side is mapped for their device can run as one
    // kernel (kernel_groups); the rest use the copy engines.
    std::unordered_map<int, std::vector<const CopyOp*>> by_device;
    std::unordered_map<int, std::vector<const CopyOp*>> kernel_groups;
    DeviceRangeCache ranges;
    for (const auto& op : ops) {
        if (op.host_mapped_only) {
            void* device_side = op.host_is_src ? op.dst : op.src;
            if (op.mapped_device < 0 ||
                ranges.lookup(device_side) != op.mapped_device ||
                !kernelCopySupported(op.mapped_device))
                return Status::InvalidArgument(
                    "peer memory is not mapped for this device" LOC_MARK);
        }
        if (op.mapped_device >= 0) {
            void* device_side = op.host_is_src ? op.dst : op.src;
            if (ranges.lookup(device_side) == op.mapped_device &&
                kernelCopySupported(op.mapped_device)) {
                kernel_groups[op.mapped_device].push_back(&op);
                continue;
            }
        } else if (op.host_pageable) {
            void* device_side = op.host_is_src ? op.dst : op.src;
            const int device = ranges.lookup(device_side);
            if (device >= 0 && pageableKernelCopySupported(device)) {
                kernel_groups[device].push_back(&op);
                continue;
            }
        }
        int device = getPointerDeviceId(op.dst);
        if (device == CUDAStreamPool::kCurrentDevice)
            device = getPointerDeviceId(op.src);
        if (device == CUDAStreamPool::kCurrentDevice) {
            ::memcpy(op.dst, op.src, op.length);
            continue;
        }
        by_device[device].push_back(&op);
    }
    // A handful of copies is not worth a launch.
    for (auto it = kernel_groups.begin(); it != kernel_groups.end();) {
        if (it->second.size() >= kMinKernelCopies) {
            ++it;
            continue;
        }
        auto& rest = by_device[it->first];
        rest.insert(rest.end(), it->second.begin(), it->second.end());
        it = kernel_groups.erase(it);
    }

    for (const auto& [device, group] : kernel_groups) {
        ScopedCudaDevice scope(device);
        CHECK_STATUS(scope.status());
        CUDAStreamHandle stream;
        CHECK_STATUS(getStreamFromPool(stream, device));
        CHECK_STATUS(kernelCopy(group, stream.get()));
    }

    for (const auto& [device, group] : by_device) {
        ScopedCudaDevice scope(device);
        CHECK_STATUS(scope.status());
        CUDAStreamHandle stream;
        CHECK_STATUS(getStreamFromPool(stream, device));

        cudaError_t err = cudaErrorNotSupported;
#if CUDART_VERSION >= 12080
        // One submission for the whole group: per-copy cudaMemcpyAsync costs
        // a few microseconds each, which dominates KV-sized (tens of KB)
        // slices.
        std::vector<void*> dsts, srcs;
        std::vector<size_t> sizes;
        dsts.reserve(group.size());
        srcs.reserve(group.size());
        sizes.reserve(group.size());
        for (const auto* op : group) {
            dsts.push_back(op->dst);
            srcs.push_back(op->src);
            sizes.push_back(op->length);
        }
        cudaMemcpyAttributes attr{};
        attr.srcAccessOrder = cudaMemcpySrcAccessOrderStream;
        size_t attr_index = 0;
#if CUDART_VERSION >= 13000
        err = cudaMemcpyBatchAsync(dsts.data(), srcs.data(), sizes.data(),
                                   group.size(), &attr, &attr_index, 1,
                                   stream.get());
#else
        size_t fail_index = 0;
        err = cudaMemcpyBatchAsync(dsts.data(), srcs.data(), sizes.data(),
                                   group.size(), &attr, &attr_index, 1,
                                   &fail_index, stream.get());
#endif
        if (err != cudaSuccess) {
            LOG_FIRST_N(WARNING, 3)
                << "cudaMemcpyBatchAsync failed (" << cudaGetErrorString(err)
                << "), falling back to per-copy cudaMemcpyAsync";
            (void)cudaGetLastError();
        }
#endif
        // Re-running copies the batch may have finished is harmless: each
        // writes the same bytes to the same destination.
        if (err != cudaSuccess) {
            for (const auto* op : group) {
                err = cudaMemcpyAsync(op->dst, op->src, op->length,
                                      cudaMemcpyDefault, stream.get());
                if (err != cudaSuccess) break;
            }
        }
        if (err == cudaSuccess) err = cudaStreamSynchronize(stream.get());
        if (err != cudaSuccess) {
            (void)cudaGetLastError();
            return Status::InternalError(
                std::string("CudaPlatform::copyBatch: ") +
                cudaGetErrorString(err) + LOC_MARK);
        }
    }
    return Status::OK();
}

bool CudaPlatform::kernelCopySupported(int device) {
#ifdef TENT_BATCHED_COPY_KERNEL
    std::lock_guard<std::mutex> lock(kernel_copy_mutex_);
    auto it = kernel_copy_supported_.find(device);
    if (it != kernel_copy_supported_.end()) return it->second;
    // Kernels may use a registered host pointer as is only with this.
    int usable = 0;
    if (cudaDeviceGetAttribute(&usable,
                               cudaDevAttrCanUseHostPointerForRegisteredMem,
                               device) != cudaSuccess) {
        (void)cudaGetLastError();
        usable = 0;
    }
    kernel_copy_supported_[device] = usable != 0;
    LOG(INFO) << "CudaPlatform: batched copy kernel "
              << (usable ? "enabled" : "disabled") << " on device " << device;
    return usable != 0;
#else
    (void)device;
    return false;
#endif
}

bool CudaPlatform::pageableKernelCopySupported(int device) {
#ifdef TENT_BATCHED_COPY_KERNEL
    std::lock_guard<std::mutex> lock(kernel_copy_mutex_);
    auto it = pageable_copy_supported_.find(device);
    if (it != pageable_copy_supported_.end()) return it->second;
    // Kernels may dereference any host mapping (HMM) only with this.
    int usable = 0;
    if (cudaDeviceGetAttribute(&usable, cudaDevAttrPageableMemoryAccess,
                               device) != cudaSuccess) {
        (void)cudaGetLastError();
        usable = 0;
    }
    pageable_copy_supported_[device] = usable != 0;
    LOG(INFO) << "CudaPlatform: batched copy kernel on unregistered host "
                 "memory "
              << (usable ? "enabled" : "disabled") << " on device " << device;
    return usable != 0;
#else
    (void)device;
    return false;
#endif
}

Status CudaPlatform::kernelCopy(const std::vector<const CopyOp*>& group,
                                cudaStream_t stream) {
#ifdef TENT_BATCHED_COPY_KERNEL
    // Descriptors live in mapped pinned memory the kernel reads directly,
    // one buffer per calling thread, reused after each synchronous batch.
    // Large copies are split so that they spread over blocks.
    constexpr uint64_t kPiece = 256 * 1024;
    size_t count = 0;
    for (const auto* op : group) count += (op->length + kPiece - 1) / kPiece;
    thread_local BatchedCopyDesc* descs = nullptr;
    thread_local size_t capacity = 0;
    if (count > capacity) {
        if (descs) cudaFreeHost(descs);
        descs = nullptr;
        capacity = 0;
        size_t want = std::max<size_t>(count, 4096);
        cudaError_t err =
            cudaHostAlloc(reinterpret_cast<void**>(&descs),
                          want * sizeof(BatchedCopyDesc),
                          cudaHostAllocMapped | cudaHostAllocPortable);
        if (err != cudaSuccess) {
            (void)cudaGetLastError();
            descs = nullptr;
            return Status::InternalError(std::string("cudaHostAlloc: ") +
                                         cudaGetErrorString(err) + LOC_MARK);
        }
        capacity = want;
    }
    size_t n = 0;
    for (const auto* op : group) {
        for (uint64_t off = 0; off < op->length; off += kPiece) {
            descs[n++] = BatchedCopyDesc{
                static_cast<const char*>(op->src) + off,
                static_cast<char*>(op->dst) + off,
                std::min<uint64_t>(kPiece, op->length - off)};
        }
    }
    cudaError_t err = launchBatchedCopy(descs, static_cast<int>(n), stream);
    if (err == cudaSuccess) err = cudaStreamSynchronize(stream);
    if (err != cudaSuccess) {
        (void)cudaGetLastError();
        return Status::InternalError(std::string("batched copy kernel: ") +
                                     cudaGetErrorString(err) + LOC_MARK);
    }
    return Status::OK();
#else
    (void)group;
    (void)stream;
    return Status::NotImplemented("batched copy kernel not built" LOC_MARK);
#endif
}

Status CudaPlatform::registerHostMemory(void* addr, size_t length,
                                        void* device_hint, int& device_out) {
    device_out = -1;
    const int device = getPointerDeviceId(device_hint);
    // Without a device on the other side there is nothing to make DMA-able,
    // and touching the runtime here would create a context on GPU 0.
    if (device == CUDAStreamPool::kCurrentDevice)
        return Status::InvalidArgument(
            "device_hint is not device memory" LOC_MARK);
    ScopedCudaDevice scope(device);
    CHECK_STATUS(scope.status());
    // Only this device's context: Portable maps the range into every context
    // of the process, multiplying the device page-table cost.
    cudaError_t err = cudaHostRegister(addr, length, cudaHostRegisterDefault);
    if (err == cudaErrorHostMemoryAlreadyRegistered) {
        (void)cudaGetLastError();
        return Status::OK();  // owned by someone else; leave device_out -1
    }
    if (err != cudaSuccess) {
        (void)cudaGetLastError();
        return Status::InternalError(std::string("cudaHostRegister: ") +
                                     cudaGetErrorString(err) + LOC_MARK);
    }
    device_out = device;
    return Status::OK();
}

Status CudaPlatform::unregisterHostMemory(void* addr, int device) {
    if (device < 0) return Status::OK();
    ScopedCudaDevice scope(device);
    CHECK_STATUS(scope.status());
    cudaError_t err = cudaHostUnregister(addr);
    if (err != cudaSuccess) {
        (void)cudaGetLastError();
        if (err != cudaErrorHostMemoryNotRegistered)
            return Status::InternalError(std::string("cudaHostUnregister: ") +
                                         cudaGetErrorString(err) + LOC_MARK);
    }
    return Status::OK();
}
}  // namespace tent
}  // namespace mooncake
