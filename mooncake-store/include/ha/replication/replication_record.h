#pragma once

#include <cstdint>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>

#include <ylt/util/tl/expected.hpp>

#include "ha/replication/change_set.h"
#include "types.h"

namespace mooncake::ha::replication {

// What the leader of `term` vouches for: the copy `replica` holds every
// change of `epoch` the leader completed, while `in_sync`. It lives outside
// both nodes (on the Lease), so a follower can trust its copy after the
// leader's node is gone.
struct ReplicationRecord {
    uint64_t term{0};
    std::string epoch;
    std::string replica;
    bool in_sync{false};

    bool operator==(const ReplicationRecord&) const = default;
};

inline std::string EncodeReplicationRecord(const ReplicationRecord& record) {
    return "term=" + std::to_string(record.term) + ",epoch=" + record.epoch +
           ",replica=" + record.replica +
           ",in_sync=" + (record.in_sync ? "1" : "0");
}

inline std::optional<ReplicationRecord> ParseReplicationRecord(
    std::string_view text) {
    ReplicationRecord record;
    bool term = false, epoch = false, replica = false, in_sync = false;
    size_t start = 0;
    while (start <= text.size()) {
        const size_t comma = text.find(',', start);
        const std::string_view field = text.substr(
            start, comma == std::string_view::npos ? text.size() - start
                                                   : comma - start);
        const size_t eq = field.find('=');
        if (eq == std::string_view::npos) {
            return std::nullopt;
        }
        const std::string_view name = field.substr(0, eq);
        const std::string value(field.substr(eq + 1));
        if (name == "term") {
            try {
                size_t used = 0;
                record.term = std::stoull(value, &used);
                term = used == value.size();
            } catch (...) {
                return std::nullopt;
            }
        } else if (name == "epoch") {
            record.epoch = value;
            epoch = true;
        } else if (name == "replica") {
            record.replica = value;
            replica = true;
        } else if (name == "in_sync") {
            if (value != "0" && value != "1") {
                return std::nullopt;
            }
            record.in_sync = value == "1";
            in_sync = true;
        }
        if (comma == std::string_view::npos) {
            break;
        }
        start = comma + 1;
    }
    if (!term || !epoch || !replica || !in_sync) {
        return std::nullopt;
    }
    return record;
}

// Where the record lives. Write succeeds only while this process still holds
// the term the store was made for: ETCD_TRANSACTION_FAIL when it does not
// (the term is over), another error when the store could not be reached.
class ReplicationRecordStore {
   public:
    virtual ~ReplicationRecordStore() = default;
    // nullopt when none was ever written.
    virtual tl::expected<std::optional<ReplicationRecord>, ErrorCode>
    Read() = 0;
    virtual ErrorCode Write(const ReplicationRecord& record) = 0;
};

// Whether a copy may keep its index when it takes the lead. A former
// primary may if no one has led a new epoch since; a follower may if the
// last leader vouched for exactly this copy.
inline bool CopyIsTrusted(const ReplState& local,
                          const std::optional<ReplicationRecord>& record) {
    if (!record.has_value() || local.epoch.empty() ||
        record->epoch != local.epoch) {
        return false;
    }
    if (local.primary) {
        return true;
    }
    return record->in_sync && !local.copy_id.empty() &&
           record->replica == local.copy_id;
}

}  // namespace mooncake::ha::replication
