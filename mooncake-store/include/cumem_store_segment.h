#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {
namespace tent {
class CumemHostPool;
}  // namespace tent

struct CumemPoolDeleter {
    void operator()(tent::CumemHostPool* pool) const;
};
using CumemPoolPtr = std::unique_ptr<tent::CumemHostPool, CumemPoolDeleter>;

// MC_STORE_CUMEM_SOCKET_DIR, or empty when store segments are not in CUDA
// VMM host memory.
std::string CumemSocketDirFromEnv();

// Allocates a store segment in CUDA VMM host memory and serves its fds on a
// unix socket in socket_dir, so same-host clients import it and their GPUs
// copy it without registering. A short pool (memory ran out) is trimmed to a
// multiple of alignment; *ptr and *mapped_size describe what was mapped.
tl::expected<CumemPoolPtr, ErrorCode> CreateCumemStoreSegment(
    const std::string& socket_dir, size_t segment_size, size_t alignment,
    void** ptr, size_t* mapped_size);

}  // namespace mooncake
