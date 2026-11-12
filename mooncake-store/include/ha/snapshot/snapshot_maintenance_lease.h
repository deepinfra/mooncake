#pragma once

#include <memory>
#include <mutex>
#include <string>

#include "types.h"

namespace mooncake {

namespace ha::sqlite {
class SqliteHaStore;
}

class SnapshotMaintenanceLease {
   public:
    static constexpr int64_t kTtlSeconds = 30;

    explicit SnapshotMaintenanceLease(std::string cluster_id);
    // Holds the lock as a leased key in the SQLite HA store instead of an
    // etcd session.
    SnapshotMaintenanceLease(std::string cluster_id,
                             std::shared_ptr<ha::sqlite::SqliteHaStore> store);
    ~SnapshotMaintenanceLease();

    SnapshotMaintenanceLease(const SnapshotMaintenanceLease&) = delete;
    SnapshotMaintenanceLease& operator=(const SnapshotMaintenanceLease&) =
        delete;

    ErrorCode Acquire();
    ErrorCode Release();

    bool IsHeld() const;
    EtcdLeaseId lease_id() const;
    EtcdRevisionId lock_create_revision() const;
    std::string owner_token() const;
    const std::string& lock_key() const { return lock_key_; }

    // Keeps publisher transaction tests independent of a live etcd process.
    static std::unique_ptr<SnapshotMaintenanceLease> MakeForTesting(
        std::string cluster_id, std::string owner_token,
        EtcdRevisionId lock_create_revision = 1);

   private:
    struct SqliteKeepalive;

    SnapshotMaintenanceLease(std::string cluster_id, std::string owner_token,
                             bool testing, EtcdRevisionId create_revision);
    ErrorCode AcquireSqlite();

    std::string cluster_id_;
    std::string lock_key_;
    std::string owner_token_;
    int64_t session_handle_{0};
    EtcdLeaseId lease_id_{0};
    EtcdRevisionId lock_create_revision_{0};
    mutable std::mutex mutex_;
    bool testing_{false};
    bool lock_created_{false};
    std::shared_ptr<ha::sqlite::SqliteHaStore> sqlite_store_;
    std::shared_ptr<SqliteKeepalive> sqlite_keepalive_;
};

}  // namespace mooncake
