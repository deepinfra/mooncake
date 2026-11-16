#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <ylt/struct_pack.hpp>

namespace mooncake::ha::replication {

// One kv row as the primary committed it. The follower stores it verbatim, so
// create revisions, which transaction compares fence on, match.
struct RowImage {
    std::string key;
    std::string value;
    int64_t create_rev{0};
    int64_t mod_rev{0};
    int64_t expires_ms{0};
};
YLT_REFL(RowImage, key, value, create_rev, mod_rev, expires_ms);

struct DeletedRange {
    std::string begin;
    std::string end;
};
YLT_REFL(DeletedRange, begin, end);

// Everything one primary write transaction changed, in commit order.
struct ChangeSet {
    uint64_t seq{0};
    int64_t revision{0};
    std::vector<DeletedRange> deleted;
    std::vector<RowImage> rows;
};
YLT_REFL(ChangeSet, seq, revision, deleted, rows);

inline size_t ChangeSetBytes(const ChangeSet& changes) {
    size_t bytes = 64;
    for (const auto& range : changes.deleted) {
        bytes += range.begin.size() + range.end.size();
    }
    for (const auto& row : changes.rows) {
        bytes += row.key.size() + row.value.size() + 24;
    }
    return bytes;
}

// Which history a store's data belongs to. `epoch` names a primary's term
// of writes; `copy_id` names this copy of it (new at each full load and each
// promotion); `seq` is the last change set applied or committed. An empty
// epoch is a store that holds nothing replicated.
struct ReplState {
    std::string epoch;
    uint64_t seq{0};
    std::string copy_id;
    bool primary{false};
};

}  // namespace mooncake::ha::replication
