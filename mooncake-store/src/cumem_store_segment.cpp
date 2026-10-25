#include "cumem_store_segment.h"

#include <algorithm>
#include <cstdlib>
#include <sstream>
#include <vector>

#include <glog/logging.h>

#include "tent/common/cumem_host_pool.h"

namespace mooncake {

void CumemPoolDeleter::operator()(tent::CumemHostPool* pool) const {
    delete pool;
}

std::string CumemSocketDirFromEnv() {
    const char* env = getenv("MC_STORE_CUMEM_SOCKET_DIR");
    return env ? env : "";
}

tl::expected<CumemPoolPtr, ErrorCode> CreateCumemStoreSegment(
    const std::string& socket_dir, size_t segment_size, size_t alignment,
    void** ptr, size_t* mapped_size) {
    auto nodes = tent::CumemHostPool::OnlineNumaNodes();
    if (const char* env = getenv("MC_STORE_CUMEM_NUMA_NODES")) {
        nodes.clear();
        std::stringstream list(env);
        std::string node;
        while (std::getline(list, node, ',')) nodes.push_back(std::stoi(node));
    }
    const size_t piece_bytes =
        getenv("MC_STORE_CUMEM_PIECE_BYTES")
            ? std::stoull(getenv("MC_STORE_CUMEM_PIECE_BYTES"))
            : 2ull << 30;
    std::unique_ptr<tent::CumemHostPool> pool;
    auto status =
        tent::CumemHostPool::Create(segment_size, nodes, piece_bytes, pool);
    if (status.ok()) status = pool->Serve(socket_dir);
    if (!status.ok()) {
        LOG(ERROR) << "CUDA VMM segment failed: " << status;
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    *ptr = pool->base();
    *mapped_size =
        std::min(segment_size, pool->length() / alignment * alignment);
    return CumemPoolPtr(pool.release());
}

}  // namespace mooncake
