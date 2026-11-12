#include "ha/common/sqlite/sqlite_ha_store.h"

#include <glog/logging.h>

#ifdef STORE_USE_SQLITE
#include <sqlite3.h>
#endif

namespace mooncake {
namespace ha {
namespace sqlite {

namespace {
constexpr std::string_view kScheme = "sqlite://";
}  // namespace

std::string SqliteHaStore::ResolvePath(std::string_view connstring) {
    if (connstring.substr(0, kScheme.size()) == kScheme) {
        connstring.remove_prefix(kScheme.size());
    }
    return std::string(connstring);
}

#ifndef STORE_USE_SQLITE

tl::expected<std::unique_ptr<SqliteHaStore>, ErrorCode> SqliteHaStore::Open(
    std::string_view) {
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}
SqliteHaStore::~SqliteHaStore() = default;
ErrorCode SqliteHaStore::Get(std::string_view, std::string&, EtcdRevisionId*) {
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}
ErrorCode SqliteHaStore::Put(std::string_view, std::string_view) {
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}
ErrorCode SqliteHaStore::Range(std::string_view, std::string_view, size_t,
                               std::vector<KvPair>&) {
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}
ErrorCode SqliteHaStore::DeleteRange(std::string_view, std::string_view) {
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}
ErrorCode SqliteHaStore::Txn(const KvTxn&) {
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}
ErrorCode SqliteHaStore::CreateLeasedKey(std::string_view, std::string_view,
                                         std::chrono::milliseconds,
                                         EtcdRevisionId&) {
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}
tl::expected<bool, ErrorCode> SqliteHaStore::RefreshLeasedKey(
    std::string_view, std::string_view, EtcdRevisionId,
    std::chrono::milliseconds) {
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}
ErrorCode SqliteHaStore::DeleteLeasedKey(std::string_view, std::string_view,
                                         EtcdRevisionId) {
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}
tl::expected<std::optional<MasterView>, ErrorCode> SqliteHaStore::ReadLeader(
    std::string_view) {
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}
tl::expected<std::optional<ViewVersionId>, ErrorCode>
SqliteHaStore::AcquireLeader(std::string_view, std::string_view,
                             std::string_view, std::chrono::milliseconds) {
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}
tl::expected<bool, ErrorCode> SqliteHaStore::RenewLeader(
    std::string_view, std::string_view, std::chrono::milliseconds) {
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}
ErrorCode SqliteHaStore::ReleaseLeader(std::string_view, std::string_view) {
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

#else

namespace {

constexpr int kBusyTimeoutMs = 10000;
constexpr char kSchema[] = R"SQL(
CREATE TABLE IF NOT EXISTS kv (
  key BLOB PRIMARY KEY NOT NULL,
  value BLOB NOT NULL,
  create_rev INTEGER NOT NULL,
  mod_rev INTEGER NOT NULL,
  expires_ms INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS revision (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  rev INTEGER NOT NULL
);
INSERT OR IGNORE INTO revision (id, rev) VALUES (1, 0);
CREATE TABLE IF NOT EXISTS leader (
  ns TEXT PRIMARY KEY NOT NULL,
  view_version INTEGER NOT NULL,
  leader_address TEXT NOT NULL,
  owner_token TEXT NOT NULL,
  expires_ms INTEGER NOT NULL
);
)SQL";

int64_t NowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

int64_t DeadlineMs(std::chrono::milliseconds ttl) {
    return NowMs() + std::max<int64_t>(ttl.count(), 1);
}

class Stmt {
   public:
    Stmt(::sqlite3* db, const char* sql) : db_(db) {
        rc_ = sqlite3_prepare_v2(db, sql, -1, &stmt_, nullptr);
        if (rc_ != SQLITE_OK) {
            LOG(ERROR) << "SQLite prepare failed: " << sqlite3_errmsg(db)
                       << " sql=" << sql;
        }
    }
    ~Stmt() { sqlite3_finalize(stmt_); }
    Stmt(const Stmt&) = delete;
    Stmt& operator=(const Stmt&) = delete;

    bool ok() const { return rc_ == SQLITE_OK; }

    // Keys and values bind as blobs so bytes, including NUL, compare
    // bytewise. A null pointer would bind SQL NULL, so empty binds "".
    Stmt& Blob(int index, std::string_view bytes) {
        if (ok()) {
            rc_ = sqlite3_bind_blob(
                stmt_, index, bytes.empty() ? "" : bytes.data(),
                static_cast<int>(bytes.size()), SQLITE_TRANSIENT);
        }
        return *this;
    }
    Stmt& Text(int index, std::string_view text) {
        if (ok()) {
            rc_ = sqlite3_bind_text(
                stmt_, index, text.empty() ? "" : text.data(),
                static_cast<int>(text.size()), SQLITE_TRANSIENT);
        }
        return *this;
    }
    Stmt& Int(int index, int64_t value) {
        if (ok()) {
            rc_ = sqlite3_bind_int64(stmt_, index, value);
        }
        return *this;
    }

    // SQLITE_ROW, SQLITE_DONE or an error.
    int Step() {
        if (!ok()) {
            return rc_;
        }
        const int rc = sqlite3_step(stmt_);
        if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
            LOG(WARNING) << "SQLite step failed: " << sqlite3_errmsg(db_);
        }
        return rc;
    }
    ErrorCode Run() {
        const int rc = Step();
        return rc == SQLITE_DONE || rc == SQLITE_ROW
                   ? ErrorCode::OK
                   : ErrorCode::ETCD_OPERATION_ERROR;
    }

    std::string ColumnBytes(int index) const {
        const void* data = sqlite3_column_blob(stmt_, index);
        const int size = sqlite3_column_bytes(stmt_, index);
        if (data == nullptr || size <= 0) {
            return {};
        }
        return std::string(static_cast<const char*>(data),
                           static_cast<size_t>(size));
    }
    int64_t ColumnInt(int index) const {
        return sqlite3_column_int64(stmt_, index);
    }

   private:
    ::sqlite3* db_;
    sqlite3_stmt* stmt_ = nullptr;
    int rc_ = SQLITE_OK;
};

ErrorCode Exec(::sqlite3* db, const char* sql) {
    char* error = nullptr;
    const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &error);
    if (rc != SQLITE_OK) {
        LOG(WARNING) << "SQLite exec failed: "
                     << (error != nullptr ? error : sqlite3_errmsg(db))
                     << " sql=" << sql;
        sqlite3_free(error);
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

// BEGIN IMMEDIATE takes the write lock up front, so two writers never
// deadlock upgrading read locks. Rolls back unless committed.
class WriteTxn {
   public:
    explicit WriteTxn(::sqlite3* db) : db_(db) {
        begun_ = Exec(db_, "BEGIN IMMEDIATE") == ErrorCode::OK;
    }
    ~WriteTxn() {
        if (begun_ && !done_) {
            (void)Exec(db_, "ROLLBACK");
        }
    }
    bool begun() const { return begun_; }
    ErrorCode Commit() {
        done_ = true;
        if (Exec(db_, "COMMIT") != ErrorCode::OK) {
            (void)Exec(db_, "ROLLBACK");
            return ErrorCode::ETCD_OPERATION_ERROR;
        }
        return ErrorCode::OK;
    }

   private:
    ::sqlite3* db_;
    bool begun_ = false;
    bool done_ = false;
};

tl::expected<EtcdRevisionId, ErrorCode> NextRevision(::sqlite3* db) {
    if (Stmt(db, "UPDATE revision SET rev = rev + 1 WHERE id = 1").Run() !=
        ErrorCode::OK) {
        return tl::make_unexpected(ErrorCode::ETCD_OPERATION_ERROR);
    }
    Stmt read(db, "SELECT rev FROM revision WHERE id = 1");
    if (read.Step() != SQLITE_ROW) {
        return tl::make_unexpected(ErrorCode::ETCD_OPERATION_ERROR);
    }
    return read.ColumnInt(0);
}

struct LiveRow {
    std::string value;
    EtcdRevisionId create_revision = 0;
};

tl::expected<std::optional<LiveRow>, ErrorCode> ReadLive(::sqlite3* db,
                                                         std::string_view key,
                                                         int64_t now_ms) {
    Stmt read(db,
              "SELECT value, create_rev FROM kv WHERE key = ?1 AND "
              "(expires_ms = 0 OR expires_ms > ?2)");
    read.Blob(1, key).Int(2, now_ms);
    const int rc = read.Step();
    if (rc == SQLITE_DONE) {
        return std::optional<LiveRow>{};
    }
    if (rc != SQLITE_ROW) {
        return tl::make_unexpected(ErrorCode::ETCD_OPERATION_ERROR);
    }
    return std::optional<LiveRow>{LiveRow{
        .value = read.ColumnBytes(0), .create_revision = read.ColumnInt(1)}};
}

// An expired leased key is gone: the next put recreates it with a new create
// revision, as etcd does once a lease's keys are deleted.
ErrorCode UpsertLocked(::sqlite3* db, std::string_view key,
                       std::string_view value, EtcdRevisionId revision,
                       int64_t expires_ms, int64_t now_ms) {
    Stmt purge(db,
               "DELETE FROM kv WHERE key = ?1 AND expires_ms != 0 AND "
               "expires_ms <= ?2");
    if (purge.Blob(1, key).Int(2, now_ms).Run() != ErrorCode::OK) {
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    Stmt upsert(db,
                "INSERT INTO kv (key, value, create_rev, mod_rev, expires_ms) "
                "VALUES (?1, ?2, ?3, ?3, ?4) ON CONFLICT(key) DO UPDATE SET "
                "value = excluded.value, mod_rev = excluded.mod_rev, "
                "expires_ms = excluded.expires_ms");
    return upsert.Blob(1, key)
        .Blob(2, value)
        .Int(3, revision)
        .Int(4, expires_ms)
        .Run();
}

bool CompareHolds(const KvCompare& compare, const std::optional<LiveRow>& row) {
    switch (compare.kind) {
        case KvCompareKind::kValueEquals:
            return row.has_value() && row->value == compare.expected_value;
        case KvCompareKind::kKeyNotExists:
            return !row.has_value();
        case KvCompareKind::kCreateRevisionEquals:
            // etcd reports create revision 0 for a missing key.
            return row.has_value()
                       ? row->create_revision == compare.expected_revision
                       : compare.expected_revision == 0;
    }
    return false;
}

}  // namespace

tl::expected<std::unique_ptr<SqliteHaStore>, ErrorCode> SqliteHaStore::Open(
    std::string_view connstring) {
    const std::string path = ResolvePath(connstring);
    if (path.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    ::sqlite3* db = nullptr;
    const int rc = sqlite3_open_v2(
        path.c_str(), &db,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX,
        nullptr);
    if (rc != SQLITE_OK) {
        LOG(ERROR) << "Failed to open SQLite HA store " << path << ": "
                   << (db != nullptr ? sqlite3_errmsg(db) : "out of memory");
        sqlite3_close(db);
        return tl::make_unexpected(ErrorCode::ETCD_OPERATION_ERROR);
    }
    std::unique_ptr<SqliteHaStore> store(new SqliteHaStore(db));
    sqlite3_busy_timeout(db, kBusyTimeoutMs);
    {
        Stmt wal(db, "PRAGMA journal_mode=WAL");
        if (wal.Step() != SQLITE_ROW || wal.ColumnBytes(0) != "wal") {
            LOG(ERROR) << "SQLite HA store " << path
                       << " cannot use WAL journaling";
            return tl::make_unexpected(ErrorCode::ETCD_OPERATION_ERROR);
        }
    }
    // FULL syncs the WAL on every commit, so an acknowledged OpLog batch
    // survives a host crash, not only a process kill.
    if (Exec(db, "PRAGMA synchronous=FULL") != ErrorCode::OK ||
        Exec(db, kSchema) != ErrorCode::OK) {
        return tl::make_unexpected(ErrorCode::ETCD_OPERATION_ERROR);
    }
    return store;
}

SqliteHaStore::~SqliteHaStore() { sqlite3_close_v2(db_); }

ErrorCode SqliteHaStore::Get(std::string_view key, std::string& value,
                             EtcdRevisionId* create_revision) {
    std::lock_guard<std::mutex> lock(mu_);
    auto row = ReadLive(db_, key, NowMs());
    if (!row) {
        return row.error();
    }
    if (!row->has_value()) {
        return ErrorCode::ETCD_KEY_NOT_EXIST;
    }
    value = std::move(row->value().value);
    if (create_revision != nullptr) {
        *create_revision = row->value().create_revision;
    }
    return ErrorCode::OK;
}

ErrorCode SqliteHaStore::Put(std::string_view key, std::string_view value) {
    std::lock_guard<std::mutex> lock(mu_);
    WriteTxn txn(db_);
    if (!txn.begun()) {
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    auto revision = NextRevision(db_);
    if (!revision) {
        return revision.error();
    }
    const ErrorCode err = UpsertLocked(db_, key, value, *revision, 0, NowMs());
    if (err != ErrorCode::OK) {
        return err;
    }
    return txn.Commit();
}

ErrorCode SqliteHaStore::Range(std::string_view begin_key,
                               std::string_view end_key, size_t limit,
                               std::vector<KvPair>& kvs) {
    kvs.clear();
    std::lock_guard<std::mutex> lock(mu_);
    const int64_t now_ms = NowMs();
    const int64_t max_rows = limit == 0 ? -1 : static_cast<int64_t>(limit);
    if (end_key.empty()) {
        auto row = ReadLive(db_, begin_key, now_ms);
        if (!row) {
            return row.error();
        }
        if (row->has_value() && max_rows != 0) {
            kvs.push_back({.key = std::string(begin_key),
                           .value = std::move(row->value().value)});
        }
        return ErrorCode::OK;
    }
    const bool unbounded = end_key == std::string_view("\0", 1);
    Stmt read(db_, unbounded
                       ? "SELECT key, value FROM kv WHERE key >= ?1 AND "
                         "(expires_ms = 0 OR expires_ms > ?3) ORDER BY key "
                         "LIMIT ?4"
                       : "SELECT key, value FROM kv WHERE key >= ?1 AND key < "
                         "?2 AND (expires_ms = 0 OR expires_ms > ?3) ORDER BY "
                         "key LIMIT ?4");
    read.Blob(1, begin_key).Int(3, now_ms).Int(4, max_rows);
    if (!unbounded) {
        read.Blob(2, end_key);
    }
    while (true) {
        const int rc = read.Step();
        if (rc == SQLITE_DONE) {
            return ErrorCode::OK;
        }
        if (rc != SQLITE_ROW) {
            kvs.clear();
            return ErrorCode::ETCD_OPERATION_ERROR;
        }
        kvs.push_back(
            {.key = read.ColumnBytes(0), .value = read.ColumnBytes(1)});
    }
}

ErrorCode SqliteHaStore::DeleteRange(std::string_view begin_key,
                                     std::string_view end_key) {
    // Same bounds contract as the etcd backend: finite and nonempty.
    if (begin_key.empty() || end_key.empty() ||
        end_key == std::string_view("\0", 1) || begin_key > end_key) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (begin_key == end_key) {
        return ErrorCode::OK;
    }
    std::lock_guard<std::mutex> lock(mu_);
    WriteTxn txn(db_);
    if (!txn.begun()) {
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    if (!NextRevision(db_)) {
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    Stmt remove(db_, "DELETE FROM kv WHERE key >= ?1 AND key < ?2");
    if (remove.Blob(1, begin_key).Blob(2, end_key).Run() != ErrorCode::OK) {
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    return txn.Commit();
}

ErrorCode SqliteHaStore::Txn(const KvTxn& kv_txn) {
    std::lock_guard<std::mutex> lock(mu_);
    WriteTxn txn(db_);
    if (!txn.begun()) {
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    const int64_t now_ms = NowMs();
    for (const auto& compare : kv_txn.compares) {
        auto row = ReadLive(db_, compare.key, now_ms);
        if (!row) {
            return row.error();
        }
        if (!CompareHolds(compare, *row)) {
            return ErrorCode::ETCD_TRANSACTION_FAIL;
        }
    }
    if (kv_txn.puts.empty()) {
        return txn.Commit();
    }
    auto revision = NextRevision(db_);
    if (!revision) {
        return revision.error();
    }
    for (const auto& put : kv_txn.puts) {
        const ErrorCode err =
            UpsertLocked(db_, put.key, put.value, *revision, 0, now_ms);
        if (err != ErrorCode::OK) {
            return err;
        }
    }
    return txn.Commit();
}

ErrorCode SqliteHaStore::CreateLeasedKey(std::string_view key,
                                         std::string_view owner,
                                         std::chrono::milliseconds ttl,
                                         EtcdRevisionId& create_revision) {
    std::lock_guard<std::mutex> lock(mu_);
    WriteTxn txn(db_);
    if (!txn.begun()) {
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    const int64_t now_ms = NowMs();
    auto row = ReadLive(db_, key, now_ms);
    if (!row) {
        return row.error();
    }
    if (row->has_value()) {
        return ErrorCode::ETCD_TRANSACTION_FAIL;
    }
    auto revision = NextRevision(db_);
    if (!revision) {
        return revision.error();
    }
    const ErrorCode err =
        UpsertLocked(db_, key, owner, *revision, DeadlineMs(ttl), now_ms);
    if (err != ErrorCode::OK) {
        return err;
    }
    const ErrorCode commit = txn.Commit();
    if (commit == ErrorCode::OK) {
        create_revision = *revision;
    }
    return commit;
}

tl::expected<bool, ErrorCode> SqliteHaStore::RefreshLeasedKey(
    std::string_view key, std::string_view owner,
    EtcdRevisionId create_revision, std::chrono::milliseconds ttl) {
    std::lock_guard<std::mutex> lock(mu_);
    Stmt refresh(db_,
                 "UPDATE kv SET expires_ms = ?1 WHERE key = ?2 AND value = ?3 "
                 "AND create_rev = ?4 AND expires_ms > ?5");
    refresh.Int(1, DeadlineMs(ttl))
        .Blob(2, key)
        .Blob(3, owner)
        .Int(4, create_revision)
        .Int(5, NowMs());
    if (refresh.Run() != ErrorCode::OK) {
        return tl::make_unexpected(ErrorCode::ETCD_OPERATION_ERROR);
    }
    return sqlite3_changes(db_) == 1;
}

ErrorCode SqliteHaStore::DeleteLeasedKey(std::string_view key,
                                         std::string_view owner,
                                         EtcdRevisionId create_revision) {
    std::lock_guard<std::mutex> lock(mu_);
    Stmt remove(db_,
                "DELETE FROM kv WHERE key = ?1 AND value = ?2 AND create_rev "
                "= ?3");
    return remove.Blob(1, key).Blob(2, owner).Int(3, create_revision).Run();
}

tl::expected<std::optional<MasterView>, ErrorCode> SqliteHaStore::ReadLeader(
    std::string_view cluster_namespace) {
    std::lock_guard<std::mutex> lock(mu_);
    Stmt read(db_,
              "SELECT view_version, leader_address FROM leader WHERE ns = ?1 "
              "AND owner_token != '' AND expires_ms > ?2");
    read.Text(1, cluster_namespace).Int(2, NowMs());
    const int rc = read.Step();
    if (rc == SQLITE_DONE) {
        return std::optional<MasterView>{};
    }
    if (rc != SQLITE_ROW) {
        return tl::make_unexpected(ErrorCode::ETCD_OPERATION_ERROR);
    }
    return std::optional<MasterView>{
        MasterView{.leader_address = read.ColumnBytes(1),
                   .view_version = read.ColumnInt(0)}};
}

tl::expected<std::optional<ViewVersionId>, ErrorCode>
SqliteHaStore::AcquireLeader(std::string_view cluster_namespace,
                             std::string_view leader_address,
                             std::string_view owner_token,
                             std::chrono::milliseconds ttl) {
    std::lock_guard<std::mutex> lock(mu_);
    WriteTxn txn(db_);
    if (!txn.begun()) {
        return tl::make_unexpected(ErrorCode::ETCD_OPERATION_ERROR);
    }
    Stmt read(db_,
              "SELECT view_version, owner_token != '' AND expires_ms > ?2 "
              "FROM leader WHERE ns = ?1");
    read.Text(1, cluster_namespace).Int(2, NowMs());
    const int rc = read.Step();
    if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
        return tl::make_unexpected(ErrorCode::ETCD_OPERATION_ERROR);
    }
    ViewVersionId previous = 0;
    if (rc == SQLITE_ROW) {
        if (read.ColumnInt(1) != 0) {
            return std::optional<ViewVersionId>{};
        }
        previous = read.ColumnInt(0);
    }
    // The version survives releases and expiries, so every term outranks the
    // producer view its predecessor claimed in the OpLog.
    const ViewVersionId next = previous + 1;
    Stmt write(db_,
               "INSERT OR REPLACE INTO leader (ns, view_version, "
               "leader_address, owner_token, expires_ms) VALUES (?1, ?2, ?3, "
               "?4, ?5)");
    write.Text(1, cluster_namespace)
        .Int(2, next)
        .Text(3, leader_address)
        .Text(4, owner_token)
        .Int(5, DeadlineMs(ttl));
    if (write.Run() != ErrorCode::OK || txn.Commit() != ErrorCode::OK) {
        return tl::make_unexpected(ErrorCode::ETCD_OPERATION_ERROR);
    }
    return std::optional<ViewVersionId>{next};
}

tl::expected<bool, ErrorCode> SqliteHaStore::RenewLeader(
    std::string_view cluster_namespace, std::string_view owner_token,
    std::chrono::milliseconds ttl) {
    std::lock_guard<std::mutex> lock(mu_);
    Stmt renew(db_,
               "UPDATE leader SET expires_ms = ?1 WHERE ns = ?2 AND "
               "owner_token = ?3 AND expires_ms > ?4");
    renew.Int(1, DeadlineMs(ttl))
        .Text(2, cluster_namespace)
        .Text(3, owner_token)
        .Int(4, NowMs());
    if (renew.Run() != ErrorCode::OK) {
        return tl::make_unexpected(ErrorCode::ETCD_OPERATION_ERROR);
    }
    return sqlite3_changes(db_) == 1;
}

ErrorCode SqliteHaStore::ReleaseLeader(std::string_view cluster_namespace,
                                       std::string_view owner_token) {
    std::lock_guard<std::mutex> lock(mu_);
    Stmt release(db_,
                 "UPDATE leader SET owner_token = '', leader_address = '', "
                 "expires_ms = 0 WHERE ns = ?1 AND owner_token = ?2");
    if (release.Text(1, cluster_namespace).Text(2, owner_token).Run() !=
        ErrorCode::OK) {
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    if (sqlite3_changes(db_) == 1) {
        return ErrorCode::OK;
    }
    Stmt other(db_,
               "SELECT 1 FROM leader WHERE ns = ?1 AND owner_token != '' AND "
               "expires_ms > ?2");
    other.Text(1, cluster_namespace).Int(2, NowMs());
    const int rc = other.Step();
    if (rc == SQLITE_ROW) {
        return ErrorCode::INVALID_PARAMS;
    }
    return rc == SQLITE_DONE ? ErrorCode::OK : ErrorCode::ETCD_OPERATION_ERROR;
}

#endif

}  // namespace sqlite
}  // namespace ha
}  // namespace mooncake
