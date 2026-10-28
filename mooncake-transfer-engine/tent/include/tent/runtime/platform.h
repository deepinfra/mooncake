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

#ifndef PLATFORM_H
#define PLATFORM_H

#include "tent/runtime/topology.h"

namespace mooncake {
namespace tent {

// Append new memory types to preserve existing numeric values. Add device
// types to isGpuMemoryType() as well so routing and staging agree.
// Intel XPU VRAM is NIC-addressable only when exported as a dma-buf and
// registered with ibv_reg_dmabuf_mr (see Platform::exportDmabuf); otherwise
// transfers are staged through host DRAM.
enum MemoryType {
    MTYPE_UNKNOWN,
    MTYPE_CPU,
    MTYPE_CUDA,
    MTYPE_ROCM,
    MTYPE_TPU,
    MTYPE_XPU,
    MTYPE_MLU
};

// Shared by routing and staging. Copy capabilities depend on the transport.
inline bool isGpuMemoryType(MemoryType t) {
    return t == MTYPE_CUDA || t == MTYPE_ROCM || t == MTYPE_TPU ||
           t == MTYPE_XPU || t == MTYPE_MLU;
}

// A device allocation exported as a Linux dma-buf so a NIC can register it
// with ibv_reg_dmabuf_mr. `fd` is owned by the exporting runtime and stays
// valid for the lifetime of the allocation; callers must not close it. Level
// Zero, for instance, caches one descriptor per allocation and returns that
// same fd on every export, so a caller-side close would leave the driver
// holding a stale number. The MR keeps its own reference to the dma-buf.
// `offset` is the position of the requested address within the exported
// buffer, which covers the whole underlying allocation, not just the
// requested range.
struct DmabufExport {
    int fd = -1;
    uint64_t offset = 0;
};

// One independent copy of a copyBatch() call. Either side may be device
// memory owned by the platform.
struct CopyOp {
    void *dst;
    void *src;
    size_t length;
    // >= 0: the host side (src if host_is_src, else dst) is registered with
    // this device's context, so the device may address it directly.
    int mapped_device = -1;
    bool host_is_src = false;
    // The host side is ordinary mapped memory the device may reach through
    // its pageable-memory access (HMM), if it has that, without registration.
    bool host_pageable = false;
    // The host side is reachable only by mapped_device (a CUDA VMM mapping
    // without CPU access): copy it there or not at all.
    bool host_mapped_only = false;
};

class Platform {
   public:
    static Platform &getLoader(std::shared_ptr<Config> conf = nullptr);

    Platform() {}

    virtual ~Platform() {}

    virtual Status probe(std::vector<Topology::NicEntry> &nic_list,
                         std::vector<Topology::MemEntry> &mem_list) = 0;

    virtual Status allocate(void **pptr, size_t size,
                            MemoryOptions &options) = 0;

    virtual Status free(void *ptr, size_t size) = 0;

    virtual Status copy(void *dst, void *src, size_t length) = 0;

    virtual MemoryType getMemoryType(void *addr) = 0;

    virtual const std::vector<RangeLocation> getLocation(
        void *start, size_t len, bool skip_prefault = false) = 0;

    virtual const std::string type() const = 0;

    // Make GPUDirect / device DMA visible on devices named in `topology`.
    // GPU platforms flush those devices; CPU and others no-op. Failures are
    // logged and still return OK so teardown can continue.
    virtual Status synchronizeDevices(const Topology *topology);

    // Export the device allocation containing [addr, addr+length) as a
    // dma-buf for direct NIC registration. Returns NotImplemented when the
    // platform has no dma-buf export path (the default), InvalidArgument when
    // `addr` is not device memory owned by this platform (register it as host
    // memory instead), and InternalError when the export itself failed. Only
    // an OK status populates `out`; per the DmabufExport contract the fd stays
    // owned by the exporting runtime and the caller must not close it.
    virtual Status exportDmabuf(void *addr, size_t length, DmabufExport &out);

    // Run independent copies and wait for all of them. Device platforms
    // submit them together (one stream, one sync per device) instead of one
    // synchronous copy() each. The default loops over copy(). OK only if
    // every copy succeeded.
    virtual Status copyBatch(const std::vector<CopyOp> &ops);

    // Make host memory a direct DMA target for the device runtime (e.g.
    // cudaHostRegister), so copies to it do not bounce through driver
    // staging. `device_hint` is device memory the region will be copied
    // with; it picks the context and is reported in `device_out` for the
    // matching unregisterHostMemory(). InvalidArgument when `device_hint` is
    // not device memory. The default is a no-op.
    virtual Status registerHostMemory(void *addr, size_t length,
                                      void *device_hint, int &device_out);

    virtual Status unregisterHostMemory(void *addr, int device);

    // Page-aligned host memory that this platform's device copies reach by
    // DMA (pinned / page-locked), for staging buffers that are allocated once
    // and reused. Release it with freePinnedHost, never with free(). The
    // default returns ordinary page-aligned host memory, so platforms without
    // a pinned-host API still work, only without the faster copies.
    virtual Status allocatePinnedHost(void **pptr, size_t size);

    virtual Status freePinnedHost(void *ptr, size_t size);

   protected:
    static std::vector<int> topologyDeviceIndices(const Topology *topology,
                                                  Topology::MemType mem_type);
};

}  // namespace tent
}  // namespace mooncake

#endif  // PLATFORM_H
