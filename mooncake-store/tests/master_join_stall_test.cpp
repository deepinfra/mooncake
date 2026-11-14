// A client that joins while the master walks its metadata must not hold up
// the heartbeats of the clients already there.

#include <chrono>
#include <future>
#include <shared_mutex>
#include <string>

#include <gtest/gtest.h>

#include "master_service.h"
#include "master_service/master_service_test_peer.h"

namespace mooncake::test {
namespace {

using namespace std::chrono_literals;

Segment MakeSegment(const std::string& name, uintptr_t base) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = name;
    segment.te_endpoint = name;
    segment.base = base;
    segment.size = 16ULL * 1024 * 1024;
    return segment;
}

TEST(MasterJoinStallTest, RemountWaitingForAMetadataWalkDoesNotBlockPing) {
    MasterService service;
    const UUID resident = generate_uuid();
    ASSERT_TRUE(
        service.MountSegment(MakeSegment("resident", 0x300000000), resident)
            .has_value());
    ASSERT_TRUE(service.ReMountSegment({}, resident).has_value());
    auto before = service.Ping(resident);
    ASSERT_TRUE(before.has_value());
    ASSERT_EQ(before->client_status, ClientStatus::OK);

    // Declared before the lock, so that a failed assertion lets the walk go
    // before it waits for the calls it left behind.
    std::future<tl::expected<void, ErrorCode>> remount;
    std::future<tl::expected<PingResponse, ErrorCode>> ping;
    // An offboarding sweep or an eviction round in progress.
    std::shared_lock<std::shared_mutex> walk(
        MasterServiceTestPeer::MasterStateMutex(service));

    const UUID joiner = generate_uuid();
    remount = std::async(std::launch::async,
                         [&] { return service.ReMountSegment({}, joiner); });
    ASSERT_EQ(remount.wait_for(300ms), std::future_status::timeout)
        << "the remount is expected to wait for the walk";

    ping =
        std::async(std::launch::async, [&] { return service.Ping(resident); });
    ASSERT_EQ(ping.wait_for(2s), std::future_status::ready)
        << "Ping waited behind a remount that waits for master_state_mutex_";
    auto during = ping.get();
    ASSERT_TRUE(during.has_value());
    EXPECT_EQ(during->client_status, ClientStatus::OK);

    walk.unlock();
    ASSERT_EQ(remount.wait_for(5s), std::future_status::ready);
    EXPECT_TRUE(remount.get().has_value());
    auto joined = service.Ping(joiner);
    ASSERT_TRUE(joined.has_value());
    EXPECT_EQ(joined->client_status, ClientStatus::OK);
}

TEST(MasterJoinStallTest, MountDoesNotWaitForAMetadataWalk) {
    MasterService service;
    std::future<tl::expected<void, ErrorCode>> mount;
    std::shared_lock<std::shared_mutex> walk(
        MasterServiceTestPeer::MasterStateMutex(service));
    const UUID joiner = generate_uuid();
    mount = std::async(std::launch::async, [&] {
        return service.MountSegment(MakeSegment("joiner", 0x400000000), joiner);
    });
    ASSERT_EQ(mount.wait_for(2s), std::future_status::ready);
    EXPECT_TRUE(mount.get().has_value());
}

TEST(MasterJoinStallTest, OfflineClientIsRefusedWithoutWaitingForAWalk) {
    MasterService service;
    const UUID client = generate_uuid();
    ASSERT_TRUE(service.MountSegment(MakeSegment("gone", 0x300000000), client)
                    .has_value());
    const auto record = MasterServiceTestPeer(service).FindClientRecord(client);
    ASSERT_TRUE(record);
    const auto now = ClientLivenessRecord::Clock::now();
    ASSERT_EQ(record->Evaluate(now + 1h, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    ASSERT_EQ(record->Evaluate(now + 2h, 10s, 20s),
              ClientLivenessTransition::BECAME_OFFLINE);

    std::future<tl::expected<void, ErrorCode>> remount;
    std::shared_lock<std::shared_mutex> walk(
        MasterServiceTestPeer::MasterStateMutex(service));
    remount = std::async(std::launch::async,
                         [&] { return service.ReMountSegment({}, client); });
    ASSERT_EQ(remount.wait_for(2s), std::future_status::ready);
    auto result = remount.get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

}  // namespace
}  // namespace mooncake::test
