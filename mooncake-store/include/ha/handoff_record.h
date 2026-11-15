#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <ylt/struct_pack.hpp>

#include "allocator.h"
#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/oplog_batch_types.h"

namespace mooncake::ha {

// What a leader stopped on purpose leaves for the next one: written after
// its listener and OpLog writer stopped, before its lease is released.
struct HandoffRecord {
    uint64_t view_version{0};
    uint64_t durable_last_seq{0};
    std::vector<AllocatedBuffer::Descriptor> in_flight;
    YLT_REFL(HandoffRecord, view_version, durable_last_seq, in_flight);
};

inline std::string HandoffKey(const std::string& cluster_id) {
    std::string key = BuildDurablePrefixKey(cluster_id);
    if (key.empty()) {
        return key;
    }
    return key.substr(0, key.rfind('/')) + "/handoff";
}

// The record of a hand-off to `view_version` at `durable_last_seq`, if the
// previous term ended with one.
inline std::optional<HandoffRecord> ReadCleanHandoff(
    HaKvBackend& backend, const std::string& cluster_id, uint64_t view_version,
    uint64_t durable_last_seq) {
    const std::string key = HandoffKey(cluster_id);
    std::string value;
    if (key.empty() || backend.Get(key, value) != ErrorCode::OK) {
        return std::nullopt;
    }
    HandoffRecord record;
    if (struct_pack::deserialize_to(record, value) != struct_pack::errc::ok ||
        record.view_version + 1 != view_version ||
        record.durable_last_seq != durable_last_seq) {
        return std::nullopt;
    }
    return record;
}

}  // namespace mooncake::ha
