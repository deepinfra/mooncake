#include "ha/snapshot/snapshot_maintenance_lease.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <thread>
#include <utility>

#include "etcd_helper.h"
#include "ha/common/sqlite/sqlite_ha_store.h"
#include "ha/snapshot/batch_oplog/metadata.h"

namespace mooncake {

namespace {
constexpr std::chrono::milliseconds kSqliteTtl =
    std::chrono::seconds(SnapshotMaintenanceLease::kTtlSeconds);
}  // namespace

// Pushes the lock key's deadline forward while the lease is held, as an etcd
// session's keepalive does.
struct SnapshotMaintenanceLease::SqliteKeepalive {
    std::mutex mutex;
    std::condition_variable cv;
    bool stop{false};
    std::atomic<bool> lost{false};
    std::atomic<std::chrono::steady_clock::rep> held_until{0};
    std::thread thread;

    bool Held() const {
        return !lost.load() &&
               std::chrono::steady_clock::now().time_since_epoch().count() <
                   held_until.load();
    }

    void Stop() {
        {
            std::lock_guard<std::mutex> lock(mutex);
            stop = true;
        }
        cv.notify_all();
        if (thread.joinable()) {
            thread.join();
        }
    }
};

SnapshotMaintenanceLease::SnapshotMaintenanceLease(std::string cluster_id)
    : cluster_id_(std::move(cluster_id)),
      lock_key_(ha::BuildBatchOpLogSnapshotMaintenanceKey(cluster_id_)) {}

SnapshotMaintenanceLease::SnapshotMaintenanceLease(
    std::string cluster_id, std::shared_ptr<ha::sqlite::SqliteHaStore> store)
    : cluster_id_(std::move(cluster_id)),
      lock_key_(ha::BuildBatchOpLogSnapshotMaintenanceKey(cluster_id_)),
      sqlite_store_(std::move(store)) {}

SnapshotMaintenanceLease::SnapshotMaintenanceLease(
    std::string cluster_id, std::string owner_token, bool testing,
    EtcdRevisionId create_revision)
    : cluster_id_(std::move(cluster_id)),
      lock_key_(ha::BuildBatchOpLogSnapshotMaintenanceKey(cluster_id_)),
      owner_token_(std::move(owner_token)),
      lock_create_revision_(create_revision),
      testing_(testing),
      lock_created_(testing) {}

SnapshotMaintenanceLease::~SnapshotMaintenanceLease() { (void)Release(); }

std::unique_ptr<SnapshotMaintenanceLease>
SnapshotMaintenanceLease::MakeForTesting(std::string cluster_id,
                                         std::string owner_token,
                                         EtcdRevisionId create_revision) {
    return std::unique_ptr<SnapshotMaintenanceLease>(
        new SnapshotMaintenanceLease(std::move(cluster_id),
                                     std::move(owner_token), true,
                                     create_revision));
}

ErrorCode SnapshotMaintenanceLease::Acquire() {
    if (testing_) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (lock_key_.empty()) {
        return ErrorCode::INVALID_PARAMS;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (lock_created_ || session_handle_ != 0) {
            return ErrorCode::INVALID_PARAMS;
        }
    }
    if (sqlite_store_ != nullptr) {
        return AcquireSqlite();
    }

    int64_t session_handle = 0;
    EtcdLeaseId lease_id = 0;
    EtcdRevisionId create_revision = 0;
    ErrorCode err = EtcdHelper::AcquireMaintenanceSession(
        lock_key_, kTtlSeconds, session_handle, lease_id, create_revision);
    if (err != ErrorCode::OK) {
        return err;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        session_handle_ = session_handle;
        lease_id_ = lease_id;
        lock_create_revision_ = create_revision;
        owner_token_ = std::to_string(lease_id);
        lock_created_ = true;
    }
    if (!IsHeld()) {
        (void)Release();
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode SnapshotMaintenanceLease::AcquireSqlite() {
    const std::string owner = UuidToString(generate_uuid());
    const auto started = std::chrono::steady_clock::now();
    EtcdRevisionId create_revision = 0;
    ErrorCode err = sqlite_store_->CreateLeasedKey(lock_key_, owner, kSqliteTtl,
                                                   create_revision);
    if (err != ErrorCode::OK) {
        return err;
    }
    auto keepalive = std::make_shared<SqliteKeepalive>();
    keepalive->held_until = (started + kSqliteTtl).time_since_epoch().count();
    keepalive->thread =
        std::thread([keepalive_ptr = keepalive.get(), store = sqlite_store_,
                     key = lock_key_, owner, create_revision] {
            SqliteKeepalive& keepalive = *keepalive_ptr;
            while (true) {
                {
                    std::unique_lock<std::mutex> lock(keepalive.mutex);
                    if (keepalive.cv.wait_for(lock, kSqliteTtl / 3,
                                              [&] { return keepalive.stop; })) {
                        return;
                    }
                }
                const auto attempt = std::chrono::steady_clock::now();
                auto refreshed = store->RefreshLeasedKey(
                    key, owner, create_revision, kSqliteTtl);
                if (refreshed && refreshed.value()) {
                    keepalive.held_until =
                        (attempt + kSqliteTtl).time_since_epoch().count();
                    continue;
                }
                if (refreshed || !keepalive.Held()) {
                    keepalive.lost = true;
                    return;
                }
            }
        });
    std::lock_guard<std::mutex> lock(mutex_);
    lease_id_ = create_revision;
    lock_create_revision_ = create_revision;
    owner_token_ = owner;
    lock_created_ = true;
    sqlite_keepalive_ = std::move(keepalive);
    return ErrorCode::OK;
}

bool SnapshotMaintenanceLease::IsHeld() const {
    if (testing_) {
        std::lock_guard<std::mutex> lock(mutex_);
        return lock_created_ && !owner_token_.empty();
    }
    int64_t session_handle = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!lock_created_) {
            return false;
        }
        if (sqlite_keepalive_ != nullptr) {
            return sqlite_keepalive_->Held();
        }
        session_handle = session_handle_;
    }
    auto alive = EtcdHelper::MaintenanceSessionAlive(session_handle);
    return alive && *alive;
}

EtcdLeaseId SnapshotMaintenanceLease::lease_id() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return lease_id_;
}

std::string SnapshotMaintenanceLease::owner_token() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return owner_token_;
}

EtcdRevisionId SnapshotMaintenanceLease::lock_create_revision() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return lock_create_revision_;
}

ErrorCode SnapshotMaintenanceLease::Release() {
    if (testing_) {
        std::lock_guard<std::mutex> lock(mutex_);
        lock_created_ = false;
        return ErrorCode::OK;
    }
    int64_t session_handle = 0;
    std::shared_ptr<SqliteKeepalive> keepalive;
    std::string owner;
    EtcdRevisionId create_revision = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        session_handle = session_handle_;
        keepalive = std::move(sqlite_keepalive_);
        owner = std::move(owner_token_);
        create_revision = lock_create_revision_;
        session_handle_ = 0;
        lease_id_ = 0;
        lock_create_revision_ = 0;
        owner_token_.clear();
        lock_created_ = false;
    }
    if (keepalive != nullptr) {
        keepalive->Stop();
        return sqlite_store_->DeleteLeasedKey(lock_key_, owner,
                                              create_revision);
    }
    if (session_handle == 0) {
        return ErrorCode::OK;
    }
    return EtcdHelper::CloseMaintenanceSession(session_handle);
}

}  // namespace mooncake
