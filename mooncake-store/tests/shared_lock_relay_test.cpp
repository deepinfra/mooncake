#include "shared_lock_relay.h"

#include <atomic>
#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <thread>

#include <gtest/gtest.h>

namespace mooncake::test {
namespace {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

TEST(SharedLockRelayTest, WriterGetsInBetweenTheParts) {
    std::shared_mutex mutex;
    std::atomic<int> intents{0};
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> parts{0};
    // A walk that never ends on its own, done in parts of about 1 ms.
    std::thread walker([&] {
        std::shared_lock<std::shared_mutex> lock(mutex);
        SharedLockRelay relay(lock, intents);
        while (!stop.load()) {
            std::this_thread::sleep_for(1ms);
            parts.fetch_add(1);
            relay.Yield();
        }
    });
    while (parts.load() < 5) {
        std::this_thread::sleep_for(1ms);
    }

    for (int round = 0; round < 20; ++round) {
        const auto asked = Clock::now();
        std::unique_lock<std::shared_mutex> writer(mutex, std::defer_lock);
        {
            WriteIntent intent(intents);
            writer.lock();
        }
        EXPECT_LT(Clock::now() - asked, 1s) << "round " << round;
        const uint64_t seen = parts.load();
        std::this_thread::sleep_for(5ms);
        EXPECT_EQ(parts.load(), seen) << "the walk ran under the writer";
        writer.unlock();
        std::this_thread::sleep_for(3ms);
    }

    const uint64_t before = parts.load();
    std::this_thread::sleep_for(50ms);
    EXPECT_GT(parts.load(), before) << "the walk did not go on";
    stop.store(true);
    walker.join();
}

TEST(SharedLockRelayTest, YieldWithoutWritersReturnsAtOnce) {
    std::shared_mutex mutex;
    std::atomic<int> intents{0};
    std::shared_lock<std::shared_mutex> lock(mutex);
    SharedLockRelay relay(lock, intents);
    const auto started = Clock::now();
    for (int i = 0; i < 1000; ++i) {
        relay.Yield();
        ASSERT_TRUE(lock.owns_lock());
    }
    EXPECT_LT(Clock::now() - started, 1s);
}

TEST(SharedLockRelayTest, WalkGoesOnWhenTheWriterNeverTakesTheLock) {
    std::shared_mutex mutex;
    // A writer announced itself and is kept out by someone else.
    std::atomic<int> intents{1};
    std::shared_lock<std::shared_mutex> lock(mutex);
    SharedLockRelay relay(lock, intents);
    const auto started = Clock::now();
    relay.Yield();
    const auto waited = Clock::now() - started;
    EXPECT_TRUE(lock.owns_lock());
    EXPECT_GE(waited, SharedLockRelay::kMaxWait);
    EXPECT_LT(waited, SharedLockRelay::kMaxWait + 1s);
}

}  // namespace
}  // namespace mooncake::test
