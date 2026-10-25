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

#include "batched_copy.h"

#include <algorithm>
#include <cstdint>

namespace mooncake {
namespace tent {
namespace {

// One block per descriptor (grid-stride over them); the widest access the
// descriptor's alignment allows.
__global__ void batchedCopyKernel(const BatchedCopyDesc* __restrict__ descs,
                                  int count) {
    for (int i = blockIdx.x; i < count; i += gridDim.x) {
        const auto src = reinterpret_cast<uintptr_t>(descs[i].src);
        const auto dst = reinterpret_cast<uintptr_t>(descs[i].dst);
        const uint64_t len = descs[i].length;
        const uint64_t align = src | dst | len;
        if ((align & 15) == 0) {
            auto* s = reinterpret_cast<const uint4*>(src);
            auto* d = reinterpret_cast<uint4*>(dst);
            for (uint64_t j = threadIdx.x; j < len / 16; j += blockDim.x)
                d[j] = s[j];
        } else if ((align & 3) == 0) {
            auto* s = reinterpret_cast<const uint32_t*>(src);
            auto* d = reinterpret_cast<uint32_t*>(dst);
            for (uint64_t j = threadIdx.x; j < len / 4; j += blockDim.x)
                d[j] = s[j];
        } else {
            auto* s = reinterpret_cast<const unsigned char*>(src);
            auto* d = reinterpret_cast<unsigned char*>(dst);
            for (uint64_t j = threadIdx.x; j < len; j += blockDim.x)
                d[j] = s[j];
        }
    }
}

}  // namespace

cudaError_t launchBatchedCopy(const BatchedCopyDesc* descs, int count,
                              cudaStream_t stream) {
    if (count <= 0) return cudaSuccess;
    constexpr int kThreads = 512;
    const int grid = std::min(count, 4096);
    batchedCopyKernel<<<grid, kThreads, 0, stream>>>(descs, count);
    return cudaGetLastError();
}

}  // namespace tent
}  // namespace mooncake
