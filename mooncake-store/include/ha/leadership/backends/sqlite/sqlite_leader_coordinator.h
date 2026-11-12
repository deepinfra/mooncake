#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include "ha/common/sqlite/sqlite_ha_store.h"
#include "ha/leadership/leader_coordinator.h"

namespace mooncake {
namespace ha {
namespace backends {
namespace sqlite {

// Leader election among masters sharing one SQLite file on one host. The
// leader record carries a deadline the holder pushes forward every third of
// the TTL; a contender takes the record once the deadline passes.
class SqliteLeaderCoordinator final : public LeaderCoordinator {
   public:
    explicit SqliteLeaderCoordinator(const HABackendSpec& spec);
    ~SqliteLeaderCoordinator() override;

    ErrorCode Connect();

    tl::expected<std::optional<MasterView>, ErrorCode> ReadCurrentView()
        override;
    tl::expected<AcquireLeadershipResult, ErrorCode> TryAcquireLeadership(
        const std::string& leader_address) override;
    tl::expected<bool, ErrorCode> RenewLeadership(
        const LeadershipSession& session) override;
    tl::expected<ViewChangeResult, ErrorCode> WaitForViewChange(
        std::optional<ViewVersionId> known_version,
        std::chrono::milliseconds timeout) override;
    tl::expected<std::unique_ptr<LeadershipMonitorHandle>, ErrorCode>
    StartLeadershipMonitor(const LeadershipSession& session,
                           LeadershipLostCallback on_leadership_lost) override;
    ErrorCode ReleaseLeadership(const LeadershipSession& session) override;

   private:
    void RenewLoop(LeadershipSession session);
    void StopRenewThread();

    HABackendSpec spec_;
    std::string namespace_;
    std::chrono::milliseconds lease_ttl_;
    std::unique_ptr<ha::sqlite::SqliteHaStore> store_;

    std::mutex mu_;
    std::condition_variable cv_;
    std::thread renew_thread_;
    OwnerToken renew_owner_;
    bool renew_alive_ = false;
    bool stop_requested_ = false;
    LeadershipLostCallback monitor_callback_;
    std::shared_ptr<std::atomic<bool>> monitor_armed_;
};

}  // namespace sqlite
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
