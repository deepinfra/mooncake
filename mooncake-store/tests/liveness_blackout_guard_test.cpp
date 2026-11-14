#include "liveness_blackout_guard.h"

#include <chrono>

#include <gtest/gtest.h>

#include "client_liveness.h"

namespace mooncake::test {
namespace {

using namespace std::chrono_literals;
using Guard = LivenessBlackoutGuard;
using Verdict = LivenessBlackoutGuard::Verdict;

constexpr size_t kFleet = 100;

Guard::Clock::time_point At(std::chrono::milliseconds offset) {
    return Guard::Clock::time_point(1000s) + offset;
}

// Ticks once a second from `from` to `to` with every client heartbeating.
void RunHealthy(Guard& guard, std::chrono::seconds from,
                std::chrono::seconds to) {
    for (auto t = from; t <= to; t += 1s) {
        guard.NoteHeartbeat(At(t) - 100ms);
        EXPECT_EQ(guard.OnTick(At(t), kFleet), Verdict::kEvaluate)
            << "at " << t.count() << " s";
    }
}

TEST(LivenessBlackoutGuardTest, HealthyFleetIsAlwaysJudged) {
    Guard guard(5s, 60s, 8);
    RunHealthy(guard, 0s, 30s);
    EXPECT_FALSE(guard.InBlackout());
}

TEST(LivenessBlackoutGuardTest, LateTickIsABlackoutAndEndsWithGrace) {
    Guard guard(5s, 60s, 8);
    RunHealthy(guard, 0s, 10s);

    // The process was stopped for 40 s: the next tick comes late.
    EXPECT_EQ(guard.OnTick(At(50s), kFleet), Verdict::kSkip);
    EXPECT_TRUE(guard.InBlackout());

    // Nobody has been heard yet one second later, and nobody is judged for
    // the time the master was away.
    EXPECT_EQ(guard.OnTick(At(51s), kFleet), Verdict::kGraceThenEvaluate);
    EXPECT_FALSE(guard.InBlackout());
    guard.NoteHeartbeat(At(51500ms));
    EXPECT_EQ(guard.OnTick(At(52s), kFleet), Verdict::kEvaluate);
}

TEST(LivenessBlackoutGuardTest, TwoStallsInARowJudgeNobody) {
    Guard guard(5s, 60s, 8);
    RunHealthy(guard, 0s, 10s);
    EXPECT_EQ(guard.OnTick(At(25s), kFleet), Verdict::kSkip);
    // Stopped again before a single heartbeat was read.
    EXPECT_EQ(guard.OnTick(At(51s), kFleet), Verdict::kSkip);
    guard.NoteHeartbeat(At(51200ms));
    EXPECT_EQ(guard.OnTick(At(52s), kFleet), Verdict::kGraceThenEvaluate);
}

TEST(LivenessBlackoutGuardTest, FleetWideSilenceIsABlackout) {
    Guard guard(5s, 60s, 8);
    RunHealthy(guard, 0s, 10s);

    // The monitor ticks on time, but no heartbeat is answered.
    for (auto t = 11s; t <= 14s; t += 1s) {
        EXPECT_EQ(guard.OnTick(At(t), kFleet), Verdict::kEvaluate)
            << "at " << t.count() << " s";
    }
    for (auto t = 15s; t <= 40s; t += 1s) {
        EXPECT_EQ(guard.OnTick(At(t), kFleet), Verdict::kSkip)
            << "at " << t.count() << " s";
    }
    guard.NoteHeartbeat(At(40500ms));
    EXPECT_EQ(guard.OnTick(At(41s), kFleet), Verdict::kGraceThenEvaluate);
    guard.NoteHeartbeat(At(41500ms));
    EXPECT_EQ(guard.OnTick(At(42s), kFleet), Verdict::kEvaluate);
}

TEST(LivenessBlackoutGuardTest, AFleetThatIsGoneIsJudgedAfterMaxSilence) {
    Guard guard(5s, 60s, 8);
    RunHealthy(guard, 0s, 10s);
    // Last heartbeat at 9.9 s; silence reaches the maximum at 69.9 s.
    for (auto t = 11s; t <= 14s; t += 1s) {
        EXPECT_EQ(guard.OnTick(At(t), kFleet), Verdict::kEvaluate)
            << "at " << t.count() << " s";
    }
    for (auto t = 15s; t <= 69s; t += 1s) {
        EXPECT_EQ(guard.OnTick(At(t), kFleet), Verdict::kSkip)
            << "at " << t.count() << " s";
    }
    for (auto t = 70s; t <= 120s; t += 1s) {
        EXPECT_EQ(guard.OnTick(At(t), kFleet), Verdict::kEvaluate)
            << "at " << t.count() << " s";
    }
    // Clients that come back are judged on their own heartbeats: no grace
    // for the ones that stayed away.
    guard.NoteHeartbeat(At(120500ms));
    EXPECT_EQ(guard.OnTick(At(121s), kFleet), Verdict::kEvaluate);
}

TEST(LivenessBlackoutGuardTest, ASmallFleetGoingSilentIsJudged) {
    Guard guard(5s, 60s, 8);
    for (auto t = 0s; t <= 10s; t += 1s) {
        guard.NoteHeartbeat(At(t) - 100ms);
        EXPECT_EQ(guard.OnTick(At(t), 2), Verdict::kEvaluate);
    }
    // One pod with two ranks went away: that is the clients, not the master.
    for (auto t = 11s; t <= 60s; t += 1s) {
        EXPECT_EQ(guard.OnTick(At(t), 2), Verdict::kEvaluate)
            << "at " << t.count() << " s";
    }
}

TEST(LivenessBlackoutGuardTest, PartOfTheFleetGoingSilentIsJudged) {
    Guard guard(5s, 60s, 8);
    RunHealthy(guard, 0s, 10s);
    // Half of the fleet stops; the other half is still heard every second.
    RunHealthy(guard, 11s, 60s);
}

TEST(LivenessBlackoutGuardTest, ZeroThresholdTurnsTheGuardOff) {
    Guard guard(0s, 60s, 8);
    EXPECT_EQ(guard.OnTick(At(0s), kFleet), Verdict::kEvaluate);
    EXPECT_EQ(guard.OnTick(At(500s), kFleet), Verdict::kEvaluate);
    EXPECT_EQ(guard.OnTick(At(501s), kFleet), Verdict::kEvaluate);
}

TEST(LivenessBlackoutGuardTest, GraceRestartsTheWindowsOfLiveClients) {
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord active(initial);
    ClientLivenessRecord suspected(initial);
    ClientLivenessRecord offline(initial);
    ASSERT_EQ(suspected.Evaluate(initial + 10s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    ASSERT_EQ(offline.Evaluate(initial + 10s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    ASSERT_EQ(offline.Evaluate(initial + 30s, 10s, 20s),
              ClientLivenessTransition::BECAME_OFFLINE);

    // The master was not hearing anyone until 100 s.
    active.GrantGrace(initial + 100s);
    suspected.GrantGrace(initial + 100s);
    offline.GrantGrace(initial + 100s);

    EXPECT_EQ(active.Evaluate(initial + 109s, 10s, 20s),
              ClientLivenessTransition::NONE);
    EXPECT_EQ(active.Evaluate(initial + 110s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    // A suspected client stays suspected, with its full window again.
    EXPECT_EQ(suspected.state(), ClientLivenessState::SUSPECTED);
    EXPECT_EQ(suspected.Evaluate(initial + 119s, 10s, 20s),
              ClientLivenessTransition::NONE);
    EXPECT_EQ(suspected.Evaluate(initial + 120s, 10s, 20s),
              ClientLivenessTransition::BECAME_OFFLINE);
    EXPECT_EQ(offline.state(), ClientLivenessState::OFFLINE);
}

}  // namespace
}  // namespace mooncake::test
