#include "ha/leadership/backends/sqlite/sqlite_leader_coordinator.h"

#include <algorithm>
#include <utility>

#include <glog/logging.h>

#include "config/ha_cluster_namespace_config.h"

namespace mooncake {
namespace ha {
namespace backends {
namespace sqlite {

namespace {

constexpr auto kViewChangePollInterval = std::chrono::milliseconds(100);
constexpr auto kMinimumRenewInterval = std::chrono::milliseconds(200);

class SqliteLeadershipMonitorHandle final : public LeadershipMonitorHandle {
   public:
    explicit SqliteLeadershipMonitorHandle(
        std::shared_ptr<std::atomic<bool>> armed)
        : armed_(std::move(armed)) {}
    void Stop() override { armed_->store(false); }

   private:
    std::shared_ptr<std::atomic<bool>> armed_;
};

bool SameVersion(const std::optional<MasterView>& view,
                 std::optional<ViewVersionId> known_version) {
    if (!view.has_value() || !known_version.has_value()) {
        return view.has_value() == known_version.has_value();
    }
    return view->view_version == *known_version;
}

}  // namespace

SqliteLeaderCoordinator::SqliteLeaderCoordinator(const HABackendSpec& spec)
    : spec_(spec),
      namespace_(
          !spec.cluster_namespace.empty()
              ? spec.cluster_namespace
              : HaClusterNamespaceConfig::FromEnvironment().cluster_namespace),
      lease_ttl_(std::chrono::seconds(
          spec.leader_lease_ttl_sec > 0 ? spec.leader_lease_ttl_sec
                                        : DEFAULT_MASTER_VIEW_LEASE_TTL_SEC)) {}

SqliteLeaderCoordinator::~SqliteLeaderCoordinator() {
    OwnerToken owner;
    {
        std::lock_guard<std::mutex> lock(mu_);
        owner = renew_owner_;
    }
    StopRenewThread();
    if (store_ != nullptr && !owner.empty()) {
        (void)store_->ReleaseLeader(namespace_, owner);
    }
}

ErrorCode SqliteLeaderCoordinator::Connect() {
    if (store_ != nullptr) {
        return ErrorCode::OK;
    }
    auto store = ha::sqlite::SqliteHaStore::Open(spec_.connstring);
    if (!store) {
        return store.error();
    }
    store_ = std::move(store.value());
    return ErrorCode::OK;
}

tl::expected<std::optional<MasterView>, ErrorCode>
SqliteLeaderCoordinator::ReadCurrentView() {
    return store_->ReadLeader(namespace_);
}

tl::expected<AcquireLeadershipResult, ErrorCode>
SqliteLeaderCoordinator::TryAcquireLeadership(
    const std::string& leader_address) {
    OwnerToken owner = UuidToString(generate_uuid());
    auto acquired =
        store_->AcquireLeader(namespace_, leader_address, owner, lease_ttl_);
    if (!acquired) {
        return tl::make_unexpected(acquired.error());
    }
    if (!acquired->has_value()) {
        auto observed = ReadCurrentView();
        if (!observed) {
            return tl::make_unexpected(observed.error());
        }
        return AcquireLeadershipResult{
            .status = AcquireLeadershipStatus::CONTENDED,
            .session = std::nullopt,
            .observed_view = observed.value(),
        };
    }
    return AcquireLeadershipResult{
        .status = AcquireLeadershipStatus::ACQUIRED,
        .session =
            LeadershipSession{
                .view = MasterView{.leader_address = leader_address,
                                   .view_version = acquired->value()},
                .owner_token = std::move(owner),
                .lease_ttl = lease_ttl_,
            },
        .observed_view = std::nullopt,
    };
}

// The first successful call starts the keepalive; later calls report whether
// it still holds the lease.
tl::expected<bool, ErrorCode> SqliteLeaderCoordinator::RenewLeadership(
    const LeadershipSession& session) {
    if (session.owner_token.empty() ||
        session.lease_ttl <= std::chrono::milliseconds::zero()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::thread finished;
    bool finished_was_ours = false;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (stop_requested_) {
            return false;
        }
        if (renew_thread_.joinable()) {
            if (renew_alive_) {
                return renew_owner_ == session.owner_token;
            }
            finished_was_ours = renew_owner_ == session.owner_token;
            finished = std::move(renew_thread_);
            renew_owner_.clear();
        }
    }
    if (finished.joinable()) {
        finished.join();
    }
    if (finished_was_ours) {
        return false;
    }
    auto renewed =
        store_->RenewLeader(namespace_, session.owner_token, session.lease_ttl);
    if (!renewed || !renewed.value()) {
        return renewed;
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (renew_thread_.joinable()) {
        return renew_owner_ == session.owner_token && renew_alive_;
    }
    renew_owner_ = session.owner_token;
    renew_alive_ = true;
    renew_thread_ = std::thread([this, session] { RenewLoop(session); });
    return true;
}

void SqliteLeaderCoordinator::RenewLoop(LeadershipSession session) {
    const auto interval =
        std::max(session.lease_ttl / 3,
                 std::chrono::duration_cast<std::chrono::milliseconds>(
                     kMinimumRenewInterval));
    auto held_until = std::chrono::steady_clock::now() + session.lease_ttl;
    LeadershipLossReason reason = LeadershipLossReason::kLostLeadership;
    while (true) {
        {
            std::unique_lock<std::mutex> lock(mu_);
            if (cv_.wait_for(lock, interval,
                             [this] { return stop_requested_; })) {
                return;
            }
        }
        const auto attempt = std::chrono::steady_clock::now();
        auto renewed = store_->RenewLeader(namespace_, session.owner_token,
                                           session.lease_ttl);
        if (renewed && renewed.value()) {
            held_until = attempt + session.lease_ttl;
            continue;
        }
        if (renewed) {
            reason = LeadershipLossReason::kLostLeadership;
            break;
        }
        // A failed write may be a passing lock wait; give up only once the
        // last renewal has run out, since a contender can then take over.
        if (std::chrono::steady_clock::now() >= held_until) {
            reason = LeadershipLossReason::kRenewError;
            break;
        }
        LOG(WARNING) << "SQLite leader renew failed, retrying: "
                     << toString(renewed.error());
    }
    LeadershipLostCallback callback;
    {
        std::lock_guard<std::mutex> lock(mu_);
        renew_alive_ = false;
        if (monitor_armed_ != nullptr && monitor_armed_->exchange(false)) {
            callback = std::move(monitor_callback_);
        }
        monitor_callback_ = nullptr;
    }
    LOG(WARNING) << "SQLite leadership lost, reason="
                 << LeadershipLossReasonToString(reason);
    if (callback) {
        callback(reason);
    }
}

tl::expected<ViewChangeResult, ErrorCode>
SqliteLeaderCoordinator::WaitForViewChange(
    std::optional<ViewVersionId> known_version,
    std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (true) {
        auto view = ReadCurrentView();
        if (!view) {
            return tl::make_unexpected(view.error());
        }
        if (!SameVersion(view.value(), known_version)) {
            return ViewChangeResult{.changed = true,
                                    .timed_out = false,
                                    .current_view = view.value()};
        }
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            return ViewChangeResult{
                .changed = false, .timed_out = true, .current_view = {}};
        }
        std::this_thread::sleep_for(std::min<std::chrono::nanoseconds>(
            kViewChangePollInterval, deadline - now));
    }
}

tl::expected<std::unique_ptr<LeadershipMonitorHandle>, ErrorCode>
SqliteLeaderCoordinator::StartLeadershipMonitor(
    const LeadershipSession& session,
    LeadershipLostCallback on_leadership_lost) {
    if (!on_leadership_lost) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (stop_requested_ || !renew_thread_.joinable() || !renew_alive_ ||
        renew_owner_ != session.owner_token) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    if (monitor_armed_ != nullptr && monitor_armed_->load()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    monitor_callback_ = std::move(on_leadership_lost);
    monitor_armed_ = std::make_shared<std::atomic<bool>>(true);
    return std::unique_ptr<LeadershipMonitorHandle>(
        std::make_unique<SqliteLeadershipMonitorHandle>(monitor_armed_));
}

ErrorCode SqliteLeaderCoordinator::ReleaseLeadership(
    const LeadershipSession& session) {
    if (session.owner_token.empty()) {
        return ErrorCode::INVALID_PARAMS;
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!renew_owner_.empty() && renew_owner_ != session.owner_token) {
            return ErrorCode::INVALID_PARAMS;
        }
    }
    StopRenewThread();
    {
        // A later term on this coordinator starts a fresh keepalive.
        std::lock_guard<std::mutex> lock(mu_);
        stop_requested_ = false;
    }
    return store_->ReleaseLeader(namespace_, session.owner_token);
}

void SqliteLeaderCoordinator::StopRenewThread() {
    std::thread thread;
    {
        std::lock_guard<std::mutex> lock(mu_);
        stop_requested_ = true;
        thread = std::move(renew_thread_);
        if (monitor_armed_ != nullptr) {
            monitor_armed_->store(false);
            monitor_armed_.reset();
        }
        monitor_callback_ = nullptr;
    }
    cv_.notify_all();
    if (thread.joinable()) {
        thread.join();
    }
    std::lock_guard<std::mutex> lock(mu_);
    renew_owner_.clear();
    renew_alive_ = false;
}

}  // namespace sqlite
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
