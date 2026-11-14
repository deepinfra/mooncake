#pragma once

// Keeps a long shared hold of a std::shared_mutex from holding up its writers.
//
// The master walks all of its metadata under a shared hold of
// master_state_mutex_: an offboarding sweep, a stale-handle sweep, an eviction
// round. A writer of that lock -- a client mounting its segments again -- would
// wait for the whole walk, and so would whoever waits behind the writer. A walk
// done in parts gives the lock up between parts through a SharedLockRelay, and
// a writer that says it is waiting through a WriteIntent gets in after one
// part.

#include <atomic>
#include <chrono>
#include <shared_mutex>
#include <thread>

namespace mooncake {

// Announces a writer for as long as it waits for the lock.
class WriteIntent {
   public:
    explicit WriteIntent(std::atomic<int>& intents) : intents_(intents) {
        intents_.fetch_add(1, std::memory_order_acq_rel);
    }
    ~WriteIntent() { intents_.fetch_sub(1, std::memory_order_acq_rel); }
    WriteIntent(const WriteIntent&) = delete;
    WriteIntent& operator=(const WriteIntent&) = delete;

   private:
    std::atomic<int>& intents_;
};

class SharedLockRelay {
   public:
    // How long one Yield stays out for a writer that keeps waiting. A writer
    // kept out by other readers must not stop the walk.
    static constexpr std::chrono::milliseconds kMaxWait{50};

    // `lock` is held by the caller, and is held again whenever Yield returns.
    SharedLockRelay(std::shared_lock<std::shared_mutex>& lock,
                    const std::atomic<int>& intents)
        : lock_(lock), intents_(intents) {}
    SharedLockRelay(const SharedLockRelay&) = delete;
    SharedLockRelay& operator=(const SharedLockRelay&) = delete;

    // Gives the lock up and takes it again. The caller holds nothing else that
    // a writer of this lock may be waiting on, and nothing it read under the
    // lock is trusted afterwards.
    void Yield() {
        lock_.unlock();
        // std::shared_mutex may hand the lock straight back to this reader
        // while a writer is still on its way to it. Stay out until every
        // announced writer has gone through.
        if (intents_.load(std::memory_order_acquire) > 0) {
            const auto give_up = std::chrono::steady_clock::now() + kMaxWait;
            do {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            } while (intents_.load(std::memory_order_acquire) > 0 &&
                     std::chrono::steady_clock::now() < give_up);
        }
        lock_.lock();
    }

   private:
    std::shared_lock<std::shared_mutex>& lock_;
    const std::atomic<int>& intents_;
};

}  // namespace mooncake
