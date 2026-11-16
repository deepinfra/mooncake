#pragma once

#include <chrono>
#include <string_view>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake::ha {

// A key that reads as absent once its deadline passes, owned by one writer:
// what the snapshot maintenance lock is built on.
class LeasedKeyStore {
   public:
    virtual ~LeasedKeyStore() = default;
    // ETCD_TRANSACTION_FAIL while another owner holds the key.
    virtual ErrorCode CreateLeasedKey(std::string_view key,
                                      std::string_view owner,
                                      std::chrono::milliseconds ttl,
                                      EtcdRevisionId& create_revision) = 0;
    virtual tl::expected<bool, ErrorCode> RefreshLeasedKey(
        std::string_view key, std::string_view owner,
        EtcdRevisionId create_revision, std::chrono::milliseconds ttl) = 0;
    virtual ErrorCode DeleteLeasedKey(std::string_view key,
                                      std::string_view owner,
                                      EtcdRevisionId create_revision) = 0;
};

}  // namespace mooncake::ha
