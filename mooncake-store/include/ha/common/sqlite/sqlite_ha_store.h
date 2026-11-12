#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "ha/ha_types.h"
#include "ha/kv/ha_kv_backend.h"
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
    ErrorCode Put(std::string_view key, std::string_view value);
    // An empty end reads begin alone; a single NUL end reads to the last key.
    ErrorCode Range(std::string_view begin_key, std::string_view end_key,
                    size_t limit, std::vector<KvPair>& kvs);
    ErrorCode DeleteRange(std::string_view begin_key, std::string_view end_key);
    // ETCD_TRANSACTION_FAIL when a compare does not hold.
    ErrorCode Txn(const KvTxn& txn);

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
    explicit SqliteHaStore(::sqlite3* db) : db_(db) {}

    std::mutex mu_;
    ::sqlite3* db_ = nullptr;
};

}  // namespace sqlite
}  // namespace ha
}  // namespace mooncake
