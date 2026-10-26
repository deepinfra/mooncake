#include "aligned_client_buffer.h"

#include <glog/logging.h>
#include <sys/mman.h>
#include <cstdlib>
#include <cstring>
#include <string_view>

#include "common/client_buffer_allocation.h"

#if defined(USE_SUNRISE)
#include "sunrise_allocator.h"
#endif

namespace mooncake {
namespace {
constexpr std::string_view kAscendProtocol = "ascend";
constexpr std::string_view kUbshmemProtocol = "ubshmem";
constexpr std::string_view kSunriseLinkProtocol = "sunrise_link";

bool UseProtocolAllocator(const std::string& protocol) {
    return protocol == kAscendProtocol || protocol == kUbshmemProtocol ||
           protocol == kSunriseLinkProtocol;
}

void FreeAlignedBuffer(void* buffer, size_t size, const std::string& protocol,
                       bool use_hugepage) {
    if (use_hugepage) {
        free_buffer_mmap_memory(buffer, size);
    } else if (UseProtocolAllocator(protocol)) {
        free_memory(protocol, buffer);
    } else {
        free(buffer);
    }
}

void* AllocateProtocolAlignedBuffer(size_t aligned_size,
                                    const std::string& protocol) {
#if defined(USE_SUNRISE)
    if (protocol == kSunriseLinkProtocol) {
        void* buf = sunrise_allocate_memory(
            aligned_size, AlignedClientBufferAllocator::kDirectIOAlignment,
            false);
        if (!buf) {
            LOG(ERROR) << "AlignedClientBufferAllocator: failed to allocate "
                       << "sunrise_link host memory of size " << aligned_size;
        }
        return buf;
    }
#endif
    void* aligned_buffer = allocate_buffer_allocator_memory(
        aligned_size, protocol,
        AlignedClientBufferAllocator::kDirectIOAlignment);
    if (!aligned_buffer) {
        LOG(ERROR) << "AlignedClientBufferAllocator: failed to allocate "
                   << "protocol-aware memory of size " << aligned_size
                   << " for protocol " << protocol;
    }
    return aligned_buffer;
}

void* AllocateHugepageAlignedBuffer(size_t aligned_size) {
    void* aligned_buffer = allocate_buffer_mmap_memory(
        aligned_size, AlignedClientBufferAllocator::kDirectIOAlignment);
    if (!aligned_buffer) {
        LOG(ERROR) << "AlignedClientBufferAllocator: failed to allocate "
                   << "hugepage memory of size " << aligned_size;
    }
    return aligned_buffer;
}

void* AllocatePosixAlignedBuffer(size_t aligned_size) {
    void* aligned_buffer = nullptr;
    int ret = posix_memalign(&aligned_buffer,
                             AlignedClientBufferAllocator::kDirectIOAlignment,
                             aligned_size);
    if (ret != 0) {
        LOG(ERROR) << "AlignedClientBufferAllocator: posix_memalign failed "
                   << "with error " << ret << " (" << strerror(ret) << ")";
        return nullptr;
    }
    memset(aligned_buffer, 0, aligned_size);
    return aligned_buffer;
}

void* AllocateAlignedBuffer(size_t aligned_size, const std::string& protocol,
                            bool use_mmap_hugepage) {
    if (UseProtocolAllocator(protocol)) {
        return AllocateProtocolAlignedBuffer(aligned_size, protocol);
    }
    if (use_mmap_hugepage) {
        return AllocateHugepageAlignedBuffer(aligned_size);
    }
    return AllocatePosixAlignedBuffer(aligned_size);
}
}  // namespace

std::shared_ptr<AlignedClientBufferAllocator>
AlignedClientBufferAllocator::create(size_t size, const std::string& protocol,
                                     bool use_hugepage) {
    if (size == 0) {
        LOG(ERROR)
            << "AlignedClientBufferAllocator: size must be greater than 0";
        return nullptr;
    }

    // Align size up to kDirectIOAlignment
    size_t aligned_size = align_up(size, kDirectIOAlignment);

    const bool use_protocol_allocator = UseProtocolAllocator(protocol);
    // SSD restores land here and peers read them out. With the store
    // segments in a CUDA VMM host pool, this buffer is another served pool,
    // so same-host peers import it and copy without a NIC like the segments.
    const std::string cumem_socket_dir = CumemSocketDirFromEnv();
    if (!cumem_socket_dir.empty() && !use_protocol_allocator) {
        void* pool_buffer = nullptr;
        size_t mapped_size = 0;
        auto pool = CreateCumemStoreSegment(cumem_socket_dir, aligned_size,
                                            kDirectIOAlignment, &pool_buffer,
                                            &mapped_size);
        if (!pool) return nullptr;
        // FileStorage registers and uses the full size; a short pool (host
        // memory ran out) cannot back it.
        if (mapped_size < aligned_size) {
            LOG(ERROR) << "AlignedClientBufferAllocator: CUDA VMM pool got "
                       << mapped_size << " of " << aligned_size << " bytes";
            return nullptr;
        }
        auto allocator = std::shared_ptr<AlignedClientBufferAllocator>(
            new AlignedClientBufferAllocator(pool_buffer, mapped_size, protocol,
                                             false));
        allocator->cumem_pool_ = std::move(*pool);
        LOG(INFO) << "AlignedClientBufferAllocator: " << mapped_size
                  << " bytes of CUDA VMM host memory at " << pool_buffer;
        return allocator;
    }
    // On a shared file a same-host peer copies them without a NIC.
    const char* shm_dir = getenv("MC_STORE_SHM_SEGMENT_DIR");
    if (shm_dir && *shm_dir && !use_protocol_allocator) {
        size_t mapped_size = 0;
        std::string path;
        void* shm_buffer =
            allocate_buffer_shm_file(shm_dir, aligned_size, &mapped_size, &path);
        if (!shm_buffer) return nullptr;
        auto allocator = std::shared_ptr<AlignedClientBufferAllocator>(
            new AlignedClientBufferAllocator(shm_buffer, aligned_size, protocol,
                                             false));
        allocator->allocated_size_ = mapped_size;
        allocator->shm_path_ = std::move(path);
        return allocator;
    }
    const bool use_mmap_hugepage = use_hugepage && !use_protocol_allocator;
    void* aligned_buffer =
        AllocateAlignedBuffer(aligned_size, protocol, use_mmap_hugepage);
    if (!aligned_buffer) {
        return nullptr;
    }

    // Verify alignment
    if (reinterpret_cast<uintptr_t>(aligned_buffer) % kDirectIOAlignment != 0) {
        LOG(ERROR) << "AlignedClientBufferAllocator: allocated buffer is not "
                   << "aligned to " << kDirectIOAlignment << " bytes";
        FreeAlignedBuffer(aligned_buffer, aligned_size, protocol,
                          use_mmap_hugepage);
        return nullptr;
    }

    LOG(INFO) << "AlignedClientBufferAllocator: allocated " << aligned_size
              << " bytes at address " << aligned_buffer << " (aligned to "
              << kDirectIOAlignment << " bytes)";

    // Use custom deleter to properly free the aligned memory
    return std::shared_ptr<AlignedClientBufferAllocator>(
        new AlignedClientBufferAllocator(aligned_buffer, aligned_size, protocol,
                                         use_mmap_hugepage));
}

AlignedClientBufferAllocator::AlignedClientBufferAllocator(
    void* aligned_buffer, size_t size, const std::string& protocol,
    bool use_hugepage)
    : ClientBufferAllocator(aligned_buffer, size, protocol),
      owns_memory_(true),
      allocated_size_(size),
      protocol_(protocol) {
    // Store hugepage flag in parent class's use_hugepage_ member
    // We need this for proper cleanup
    use_hugepage_ = use_hugepage;
}

AlignedClientBufferAllocator::~AlignedClientBufferAllocator() {
    // Free the aligned memory we allocated
    // The parent class destructor will not free it because is_external_memory_
    // is set to true
    if (cumem_pool_) {
        // The pool owns the mapping; peers' mappings outlive it and its
        // pages go back once the last one unmaps.
        buffer_ = nullptr;
        cumem_pool_.reset();
    } else if (owns_memory_ && buffer_ && !shm_path_.empty()) {
        free_buffer_shm_file(buffer_, allocated_size_, shm_path_);
        buffer_ = nullptr;
    } else if (owns_memory_ && buffer_) {
        if (use_hugepage_) {
            LOG(INFO)
                << "AlignedClientBufferAllocator: freeing hugepage memory "
                << "at " << buffer_ << " (" << allocated_size_ << " bytes)";
            FreeAlignedBuffer(buffer_, allocated_size_, protocol_,
                              use_hugepage_);
        } else {
            LOG(INFO) << "AlignedClientBufferAllocator: freeing aligned memory "
                      << "at " << buffer_ << " (" << allocated_size_
                      << " bytes)";
            FreeAlignedBuffer(buffer_, allocated_size_, protocol_,
                              use_hugepage_);
        }
        buffer_ = nullptr;
    }
}

}  // namespace mooncake
