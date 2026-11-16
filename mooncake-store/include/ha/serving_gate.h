#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

namespace mooncake::ha {

// A Lease-elected leader answers only while its last renewal, as sent, is
// within the renew deadline by this host's monotonic clock. A candidate takes
// over only a full Lease duration after it saw that renewal, so the two
// never answer at once. A leader resuming from a pause, or cut off from the
// API server, stops answering at once instead of when its election loop
// notices. Open while not armed.
class ServingGate {
   public:
    static ServingGate& Instance() {
        static ServingGate gate;
        return gate;
    }

    static int64_t NowMonoNs() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    void Arm(std::chrono::nanoseconds deadline, int64_t renewed_mono_ns) {
        deadline_ns_.store(deadline.count(), std::memory_order_relaxed);
        renewed_ns_.store(renewed_mono_ns, std::memory_order_relaxed);
        armed_.store(true, std::memory_order_release);
    }
    void Disarm() { armed_.store(false, std::memory_order_release); }

    // A renewal sent at `mono_ns` succeeded; older ones are ignored.
    void Renewed(int64_t mono_ns) {
        int64_t seen = renewed_ns_.load(std::memory_order_relaxed);
        while (mono_ns > seen &&
               !renewed_ns_.compare_exchange_weak(seen, mono_ns,
                                                  std::memory_order_relaxed)) {
        }
    }

    bool Open() const {
        if (!armed_.load(std::memory_order_acquire)) {
            return true;
        }
        return NowMonoNs() - renewed_ns_.load(std::memory_order_relaxed) <
               deadline_ns_.load(std::memory_order_relaxed);
    }

   private:
    std::atomic<bool> armed_{false};
    std::atomic<int64_t> renewed_ns_{0};
    std::atomic<int64_t> deadline_ns_{0};
};

}  // namespace mooncake::ha
