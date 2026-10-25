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

#ifndef TENT_PLATFORM_CUDA_BATCHED_COPY_H
#define TENT_PLATFORM_CUDA_BATCHED_COPY_H

#include <cuda_runtime.h>

#include <cstdint>

namespace mooncake {
namespace tent {

struct BatchedCopyDesc {
    const void* src;
    void* dst;
    uint64_t length;
};

// Copies every descriptor with one kernel launch on `stream`. Both sides of
// each copy must be addressable by the stream's device: its own memory, or
// host memory registered with (mapped into) its context. `descs` itself must
// be device-readable, e.g. mapped pinned host memory. Many small copies run
// near PCIe speed this way, where the copy engines pay a few microseconds
// per copy (and host-to-device copies of a few KB go through the CPU).
cudaError_t launchBatchedCopy(const BatchedCopyDesc* descs, int count,
                              cudaStream_t stream);

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_PLATFORM_CUDA_BATCHED_COPY_H
