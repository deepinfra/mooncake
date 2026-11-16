#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "ha/ha_types.h"
#include "ha/kv/ha_kv_backend.h"
#include "ha/replication/change_set.h"
#include "types.h"

struct sqlite3;

namespace mooncake {
namespace ha {
namespace sqlite {

// One SQLite file holds the OpLog, the snapshot pointers, the maintenance lock
// and the leader record of the masters sharing a host directory. Each instance
// owns one connection; SQLite's file locks order writers across processes.
//
// Keys order bytewise like etcd. Every write transaction advances one global
// revision, so create revisions fence the same way etcd's do. A leased key
// (expires_ms != 0) reads as absent once its deadline passes.
class SqliteHaStore {
   public:
    // connstring is a file path, optionally prefixed with "sqlite://".
    static tl::expected<std::unique_ptr<SqliteHaStore>, ErrorCode> Open(
        std::string_view connstring);
    static std::string ResolvePath(std::string_view connstring);

    ~SqliteHaStore();
    SqliteHaStore(const SqliteHaStore&) = delete;
    SqliteHaStore& operator=(const SqliteHaStore&) = delete;

    ErrorCode Get(std::string_view key, std::string& value,
                  EtcdRevisionId* create_revision = nullptr);
    // With a change sink set, `repl_seq` receives the change set's sequence;
    // it is left alone when the call changed nothing.
    ErrorCode Put(std::string_view key, std::string_view value,
                  uint64_t* repl_seq = nullptr);
    // An empty end reads begin alone; a single NUL end reads to the last key.
    ErrorCode Range(std::string_view begin_key, std::string_view end_key,
                    size_t limit, std::vector<KvPair>& kvs);
    ErrorCode DeleteRange(std::string_view begin_key, std::string_view end_key,
                          uint64_t* repl_seq = nullptr);
    // ETCD_TRANSACTION_FAIL when a compare does not hold.
    ErrorCode Txn(const KvTxn& txn, uint64_t* repl_seq = nullptr);

    // Replication. With a sink, each Put, DeleteRange and Txn that changes
    // rows commits a ChangeSet under the next repl_seq and hands it to the
    // sink, in repl_seq order, under the store's lock. Leased keys and the
    // leader record are not replicated.
    using ChangeSink = std::function<void(replication::ChangeSet&&)>;
    void SetChangeSink(ChangeSink sink);
    tl::expected<replication::ReplState, ErrorCode> ReadReplState();
    // Sets epoch, copy id and role; the sequence stays.
    ErrorCode WriteReplState(const replication::ReplState& state);
    // Applies a primary's change set verbatim. INVALID_PARAMS unless it is the
    // one after the last applied.
    ErrorCode ApplyChangeSet(const replication::ChangeSet& changes);
    tl::expected<bool, ErrorCode> Empty();
    // Deletes every kv row and forgets the replicated history.
    ErrorCode WipeAll();

    // Replaces every kv row with a primary's copy in one transaction on its
    // own connection; readers see the old rows until Commit.
    class FullLoad {
       public:
        ~FullLoad();
        ErrorCode Add(const std::vector<replication::RowImage>& rows);
        ErrorCode Commit(int64_t revision, const replication::ReplState& state);

       private:
        friend class SqliteHaStore;
        explicit FullLoad(::sqlite3* db) : db_(db) {}
        ::sqlite3* db_;
        bool done_ = false;
    };
    tl::expected<std::unique_ptr<FullLoad>, ErrorCode> BeginFullLoad();

    // Every kv row as of one point, on its own connection and read
    // transaction, in key order.
    class SnapshotReader {
       public:
        ~SnapshotReader();
        // The rows after `after` (from the first when `from_start`), up to
        // about max_bytes and at least one; `done` once none remain.
        ErrorCode Next(std::string_view after, bool from_start,
                       size_t max_bytes,
                       std::vector<replication::RowImage>& rows, bool& done);
        int64_t revision() const { return revision_; }
        const replication::ReplState& state() const { return state_; }

       private:
        friend class SqliteHaStore;
        explicit SnapshotReader(::sqlite3* db) : db_(db) {}
        ::sqlite3* db_;
        int64_t revision_{0};
        replication::ReplState state_;
    };
    tl::expected<std::unique_ptr<SnapshotReader>, ErrorCode>
    OpenSnapshotReader();

    // ETCD_TRANSACTION_FAIL while another owner holds the key.
    ErrorCode CreateLeasedKey(std::string_view key, std::string_view owner,
                              std::chrono::milliseconds ttl,
                              EtcdRevisionId& create_revision);
    tl::expected<bool, ErrorCode> RefreshLeasedKey(
        std::string_view key, std::string_view owner,
        EtcdRevisionId create_revision, std::chrono::milliseconds ttl);
    ErrorCode DeleteLeasedKey(std::string_view key, std::string_view owner,
                              EtcdRevisionId create_revision);

    tl::expected<std::optional<MasterView>, ErrorCode> ReadLeader(
        std::string_view cluster_namespace);
    // The new view version, or nullopt while a live leader holds the record.
    tl::expected<std::optional<ViewVersionId>, ErrorCode> AcquireLeader(
        std::string_view cluster_namespace, std::string_view leader_address,
        std::string_view owner_token, std::chrono::milliseconds ttl);
    tl::expected<bool, ErrorCode> RenewLeader(
        std::string_view cluster_namespace, std::string_view owner_token,
        std::chrono::milliseconds ttl);
    // INVALID_PARAMS when another live owner holds the record.
    ErrorCode ReleaseLeader(std::string_view cluster_namespace,
                            std::string_view owner_token);

   private:
    explicit SqliteHaStore(::sqlite3* db, std::string path)
        : db_(db), path_(std::move(path)) {}

    std::mutex mu_;
    ::sqlite3* db_ = nullptr;
    std::string path_;
    ChangeSink sink_;
};

}  // namespace sqlite
}  // namespace ha
}  // namespace mooncake
