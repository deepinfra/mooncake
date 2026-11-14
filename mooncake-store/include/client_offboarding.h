#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "client_liveness.h"
#include "types.h"

namespace mooncake {

class MasterService;
namespace test {
class MasterServiceTest;
}

struct PendingSegmentOffboarding {
    UUID segment_id;
    std::string segment_name;
    std::string transport_endpoint;
};

struct PreparedSegmentOffboarding {
    UUID segment_id;
    std::string segment_name;
    std::string transport_endpoint;
    size_t metrics_dec_capacity{0};
};

// Process-local residual work for one terminal Client incarnation. The job is
// intentionally not serializable: snapshots stay behind the pending-work
// barrier until the residual work converges.
struct ClientOffboardingJob {
    UUID client_id;
    std::shared_ptr<ClientLivenessRecord> liveness;
    std::vector<PendingSegmentOffboarding> pending_prepare_segments;
    std::vector<PreparedSegmentOffboarding> prepared_segments;
    bool metadata_cleanup_accepted{false};
    bool local_ssd_unregistered{false};
    // Set when the job is scheduled, after the client went offline and its
    // segments were taken out of service: a metadata sweep that starts later
    // sees everything the client left.
    uint64_t scheduled_seq{0};
    // The oldest sweep whose result this job may take. Raised when an attempt
    // takes a segment out of service itself or has to wait for a replica that
    // is still being written.
    uint64_t min_sweep_id{0};
    uint64_t retry_count{0};
    std::chrono::steady_clock::time_point next_attempt_at{
        std::chrono::steady_clock::now()};
    std::chrono::steady_clock::time_point enqueued_at{
        std::chrono::steady_clock::now()};
};

class ClientOffboardingWorker {
   public:
    explicit ClientOffboardingWorker(MasterService* service)
        : service_(service) {}
    ~ClientOffboardingWorker();

    ClientOffboardingWorker(const ClientOffboardingWorker&) = delete;
    ClientOffboardingWorker& operator=(const ClientOffboardingWorker&) = delete;

    void Start();
    void Stop();
    [[nodiscard]] bool HasPending() const {
        return pending_jobs_.load(std::memory_order_acquire) != 0;
    }
    // The scheduled_seq of the job scheduled last; 0 before the first.
    [[nodiscard]] uint64_t LastScheduledSeq() const {
        return last_scheduled_seq_.load(std::memory_order_acquire);
    }

   private:
    friend class MasterService;
    friend class test::MasterServiceTest;

    void ReserveJob();
    void ScheduleReserved(ClientOffboardingJob job);
    void ThreadFunc();
    void CompleteJob(const ClientOffboardingJob& job);
    void DropJob(const ClientOffboardingJob& job, const char* reason);
    static std::chrono::seconds RetryDelay(uint64_t retry_count);
    static bool ShouldAlert(uint64_t retry_count) {
        return retry_count >= kAlertRetryThreshold;
    }

    static constexpr uint64_t kAlertRetryThreshold = 10;

    MasterService* service_;
    std::thread thread_;
    bool running_{false};
    std::deque<ClientOffboardingJob> jobs_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<size_t> pending_jobs_{0};
    std::atomic<uint64_t> last_scheduled_seq_{0};
};

}  // namespace mooncake
