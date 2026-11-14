#pragma once

// Tells the client liveness monitor when missed heartbeats say nothing about
// the clients, because the master itself was not serving or judging them.
//
// The monitor retires a client that has not been heard from for the active
// TTL and then the suspicion TTL. Both are measured on the master's clock, so
// time the master spent stopped, starved of CPU, or unable to answer a Ping
// counts against every client at once: two such stretches in a row and the
// whole fleet is declared offline, its segments dropped, while every client is
// alive and heartbeating. Two signs tell that case from clients going away:
//
// - the monitor itself ran late: it wakes every second, and a tick that comes
//   several seconds after the last one means this process was not running;
// - no client at all was heard for several seconds while a whole fleet is
//   registered: clients fail one at a time, and a fleet falling silent in the
//   same second is the master, or the network in front of it, not hearing
//   them. Below `min_fleet` clients the monitor cannot tell, and judges.
//
// In either case the monitor judges nobody, and when heartbeats are served
// again every client's window starts over. A fleet that really is gone is
// still retired: silence longer than `max_silence` is taken at its word.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>

namespace mooncake {

class LivenessBlackoutGuard {
   public:
    using Clock = std::chrono::steady_clock;

    enum class Verdict {
        // Judge the clients against `now`.
        kEvaluate,
        // The master is not hearing anyone: judge nobody this tick.
        kSkip,
        // Heartbeats are served again after a blackout: give every client a
        // fresh window from `now`, then judge.
        kGraceThenEvaluate,
    };

    // A zero `threshold` turns the guard off: every tick evaluates. It has to
    // be well above the monitor's tick interval.
    LivenessBlackoutGuard(Clock::duration threshold,
                          Clock::duration max_silence, size_t min_fleet)
        : threshold_(threshold),
          max_silence_(max_silence),
          min_fleet_(min_fleet) {}

    // Any client's heartbeat was answered. Called from the Ping path.
    void NoteHeartbeat(Clock::time_point now) {
        last_heartbeat_ns_.store(ToNs(now), std::memory_order_release);
    }

    // One monitor tick. `clients` counts those the monitor would judge.
    Verdict OnTick(Clock::time_point now, size_t clients) {
        if (threshold_ <= Clock::duration::zero()) {
            return Verdict::kEvaluate;
        }
        const bool first_tick = !ticked_;
        const Clock::time_point last_tick = last_tick_;
        last_tick_ = now;
        ticked_ = true;
        if (first_tick) {
            // Nothing to compare against yet; the fleet's silence starts now.
            StartSilenceAt(now);
            return Verdict::kEvaluate;
        }

        if (now - last_tick >= threshold_) {
            // This process was not running. Whatever the clients sent was not
            // read, so the silence so far is the master's.
            blackout_ = true;
            StartSilenceAt(now);
            return Verdict::kSkip;
        }

        const Clock::time_point heard = LastHeartbeat();
        const bool silent = clients >= min_fleet_ && now - heard >= threshold_;
        if (silent) {
            if (now - heard >= max_silence_) {
                // Nobody has been heard for this long: the fleet is gone, or
                // cut off for good. Retire it the ordinary way.
                blackout_ = false;
                return Verdict::kEvaluate;
            }
            blackout_ = true;
            return Verdict::kSkip;
        }
        if (blackout_) {
            blackout_ = false;
            return Verdict::kGraceThenEvaluate;
        }
        return Verdict::kEvaluate;
    }

    [[nodiscard]] bool InBlackout() const { return blackout_; }

   private:
    static int64_t ToNs(Clock::time_point at) {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   at.time_since_epoch())
            .count();
    }

    [[nodiscard]] Clock::time_point LastHeartbeat() const {
        return Clock::time_point(std::chrono::nanoseconds(
            last_heartbeat_ns_.load(std::memory_order_acquire)));
    }

    // Counts the fleet's silence from `now` unless a heartbeat came later.
    void StartSilenceAt(Clock::time_point now) {
        int64_t seen = last_heartbeat_ns_.load(std::memory_order_acquire);
        const int64_t floor = ToNs(now);
        while (seen < floor && !last_heartbeat_ns_.compare_exchange_weak(
                                   seen, floor, std::memory_order_acq_rel)) {
        }
    }

    const Clock::duration threshold_;
    const Clock::duration max_silence_;
    const size_t min_fleet_;
    std::atomic<int64_t> last_heartbeat_ns_{0};
    // Monitor thread only.
    Clock::time_point last_tick_{};
    bool ticked_{false};
    bool blackout_{false};
};

}  // namespace mooncake
