#include "master_service.h"
#include "master_service/master_service_test_peer.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "types.h"

namespace mooncake::test {

class OffloadOnEvictTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("OffloadOnEvictTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    static constexpr size_t kDefaultSegmentBase = 0x300000000;

    Segment MakeSegment(std::string name, size_t base, size_t size) const {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = size;
        segment.te_endpoint = segment.name;
        return segment;
    }

    struct MountedSegmentContext {
        UUID segment_id;
        UUID client_id;
    };

    MountedSegmentContext PrepareSegment(MasterService& service,
                                         std::string name, size_t base,
                                         size_t size) const {
        Segment segment = MakeSegment(std::move(name), base, size);
        UUID client_id = generate_uuid();
        auto mount_result = service.MountSegment(segment, client_id);
        EXPECT_TRUE(mount_result.has_value());
        return {.segment_id = segment.id, .client_id = client_id};
    }

    // Put an object and complete it.
    void PutObject(MasterService& service, const UUID& client_id,
                   const std::string& key, size_t size = 1024) {
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start =
            service.PutStart(client_id, key, TenantId::Default(), size, config);
        ASSERT_TRUE(put_start.has_value()) << "PutStart failed for key=" << key;
        auto put_end = service.PutEnd(client_id, key, TenantId::Default(),
                                      ReplicaType::MEMORY);
        ASSERT_TRUE(put_end.has_value()) << "PutEnd failed for key=" << key;
    }

    // Drain the offload queue via OffloadObjectHeartbeat.
    std::unordered_map<std::string, int64_t> DrainOffloadQueue(
        MasterService& service, const UUID& client_id) {
        auto res = service.OffloadObjectHeartbeat(client_id, true);
        if (!res) {
            return {};
        }
        std::unordered_map<std::string, int64_t> queued;
        for (const auto& task : res.value()) {
            queued[task.key] = task.size;
        }
        return queued;
    }

    struct ShortfallResult {
        int queued, admitted_gone, old_gone, young_gone, newest_gone;
    };

    // Oldest first: `admitted` objects loaded once (so deferred to the offload
    // queue), then `old`, `young` and `newest` unloaded ones. One pass targets
    // `target` evictions.
    ShortfallResult RunShortfallScenario(int admitted, int old, int young,
                                         int newest, int target) {
        const uint64_t kv_lease_ttl = 200;
        MasterServiceConfig config;
        config.enable_offload = true;
        config.offload_on_evict = true;
        config.offload_min_loads = 1;
        config.default_kv_lease_ttl = kv_lease_ttl;
        auto service = std::make_unique<MasterService>(config);
        auto ctx = PrepareSegment(*service, "test_segment",
                                  kDefaultSegmentBase, 256 * 1024 * 1024);
        EXPECT_TRUE(service->MountLocalDiskSegment(ctx.client_id, true));

        auto put_batch = [&](const std::string& prefix, int n) {
            for (int i = 0; i < n; ++i) {
                PutObject(*service, ctx.client_id, prefix + std::to_string(i),
                          4096);
            }
        };
        auto gone = [&](const std::string& prefix, int n) {
            int count = 0;
            for (int i = 0; i < n; ++i) {
                count += !service
                              ->ExistKey(prefix + std::to_string(i),
                                         TenantId::Default())
                              .value_or(false);
            }
            return count;
        };
        put_batch("admitted_", admitted);
        for (int i = 0; i < admitted; ++i) {
            EXPECT_TRUE(service
                            ->GetReplicaList("admitted_" + std::to_string(i),
                                             TenantId::Default())
                            .has_value());
        }
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kv_lease_ttl + 100));
        put_batch("old_", old);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        put_batch("young_", young);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        put_batch("newest_", newest);
        std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl * 2));

        const double ratio =
            static_cast<double>(target) / (admitted + old + young + newest);
        MasterServiceTestPeer(*service).RunBatchEvictForTesting(ratio, ratio);

        ShortfallResult result{
            static_cast<int>(DrainOffloadQueue(*service, ctx.client_id).size()),
            gone("admitted_", admitted), gone("old_", old),
            gone("young_", young), gone("newest_", newest)};
        service->RemoveAll();
        return result;
    }
};

// =============================================================================
// UpsertStart interaction with an outstanding offload task.
//
// The object's offloading task passes through two observable states:
//   QUEUED    - mirror entry still present in offloading_objects; the store
//               worker has not observed the task. UpsertStart cancels the
//               task in place and allocates a fresh replica.
//   IN-FLIGHT - mirror already drained by OffloadObjectHeartbeat; the worker
//               is reading the source buffer for SSD write. UpsertStart
//               returns OBJECT_HAS_REPLICATION_TASK; the caller retries
//               after NotifyOffloadSuccess clears the marker.
// =============================================================================

TEST_F(OffloadOnEvictTest, UpsertPreemptsQueuedOffloadWithOffloadOnEvict) {
    // With offload_on_evict the task is queued by eviction rather than by
    // PutEnd.
    const uint64_t kv_lease_ttl = 500;
    MasterServiceConfig config;
    config.enable_offload = true;
    config.offload_on_evict = true;
    config.default_kv_lease_ttl = kv_lease_ttl;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    constexpr size_t object_size = 1024 * 1024;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);
    ASSERT_TRUE(service->MountLocalDiskSegment(ctx.client_id, true));

    std::vector<std::string> keys;
    for (int i = 0; i < 8; ++i) {
        keys.push_back("evict_upsert_" + std::to_string(i));
        PutObject(*service, ctx.client_id, keys.back(), object_size);
    }

    // PutEnd does not queue offloads in this mode; eviction does.
    auto queued = DrainOffloadQueue(*service, ctx.client_id);
    ASSERT_TRUE(queued.empty())
        << "offload_on_evict: PutEnd must not queue offloads";

    // Let leases expire so the keys are evictable, then run one eviction
    // cycle to queue their offloads without draining the mirrors.
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl * 2));
    MasterServiceTestPeer(*service).RunBatchEvictForTesting(1.0, 1.0);

    std::string offloading_key;
    for (const auto& k : keys) {
        auto upsert =
            service->UpsertStart(ctx.client_id, k, TenantId::Default(),
                                 object_size, ReplicateConfig{});
        if (upsert.has_value()) {
            offloading_key = k;
            EXPECT_EQ(upsert->size(), 1u);
            EXPECT_EQ(upsert->at(0).status, ReplicaStatus::PROCESSING);
            ASSERT_TRUE(service
                            ->PutEnd(ctx.client_id, k, TenantId::Default(),
                                     ReplicaType::MEMORY)
                            .has_value());
            break;
        }
    }
    ASSERT_FALSE(offloading_key.empty())
        << "expected UpsertStart to preempt an offload queued by eviction";

    // Preempt cleared the marker and the mirror, so no stale entry is handed
    // to the store worker for the preempted key.
    auto stale = DrainOffloadQueue(*service, ctx.client_id);
    EXPECT_EQ(stale.count(offloading_key), 0u)
        << "preempted key must not remain queued";

    service->RemoveAll();
}

// =============================================================================
// Offload admission: offload_min_loads and offload_max_bytes_per_sec decide
// which evicted objects go to disk; the rest are evicted without offload.
// =============================================================================

TEST_F(OffloadOnEvictTest, MinLoadsOffloadsOnlyLoadedObjects) {
    const uint64_t kv_lease_ttl = 500;
    MasterServiceConfig config;
    config.enable_offload = true;
    config.offload_on_evict = true;
    config.offload_min_loads = 1;
    config.default_kv_lease_ttl = kv_lease_ttl;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    constexpr size_t object_size = 1024 * 1024;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);
    ASSERT_TRUE(service->MountLocalDiskSegment(ctx.client_id, true));

    std::vector<std::string> keys;
    for (int i = 0; i < 8; ++i) {
        keys.push_back("admit_loads_" + std::to_string(i));
        PutObject(*service, ctx.client_id, keys.back(), object_size);
    }
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(
            service->GetReplicaList(keys[i], TenantId::Default()).has_value());
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl * 2));
    MasterServiceTestPeer(*service).RunBatchEvictForTesting(1.0, 1.0);

    auto queued = DrainOffloadQueue(*service, ctx.client_id);
    EXPECT_EQ(queued.size(), 3u);
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(queued.count(keys[i]), 1u) << keys[i] << " was loaded";
    }
    for (int i = 3; i < 8; ++i) {
        EXPECT_FALSE(
            service->GetReplicaList(keys[i], TenantId::Default()).has_value())
            << keys[i] << " was never loaded and must be evicted";
    }

    service->RemoveAll();
}

TEST_F(OffloadOnEvictTest, SpentWriteBudgetEvictsWithoutOffload) {
    const uint64_t kv_lease_ttl = 500;
    MasterServiceConfig config;
    config.enable_offload = true;
    config.offload_on_evict = true;
    config.offload_max_bytes_per_sec = 1;  // never covers a 1 MiB object
    config.default_kv_lease_ttl = kv_lease_ttl;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    constexpr size_t object_size = 1024 * 1024;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);
    ASSERT_TRUE(service->MountLocalDiskSegment(ctx.client_id, true));

    std::vector<std::string> keys;
    for (int i = 0; i < 4; ++i) {
        keys.push_back("admit_budget_" + std::to_string(i));
        PutObject(*service, ctx.client_id, keys.back(), object_size);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl * 2));
    MasterServiceTestPeer(*service).RunBatchEvictForTesting(1.0, 1.0);

    EXPECT_TRUE(DrainOffloadQueue(*service, ctx.client_id).empty());
    for (const auto& key : keys) {
        EXPECT_FALSE(
            service->GetReplicaList(key, TenantId::Default()).has_value());
    }

    service->RemoveAll();
}

// Deferred offloads free nothing during the pass, so a pass whose oldest
// candidates are mostly admitted falls short of its target and keeps evicting.
// It must keep going oldest first: the newest objects are the ones clients are
// about to read, and a fixed walk order would take the same ones every pass.
TEST_F(OffloadOnEvictTest, ShortfallEvictsOldestFirst) {
    // Above the pre-bypass ratio: every object is a candidate.
    auto r = RunShortfallScenario(1500, 1500, 600, 600, 2000);
    EXPECT_EQ(r.queued, 1500);
    EXPECT_EQ(r.admitted_gone, 0);
    EXPECT_EQ(r.old_gone, 1500);
    EXPECT_GE(r.young_gone, 500);
    EXPECT_EQ(r.newest_gone, 0);
}

TEST_F(OffloadOnEvictTest, ShortfallRefillEvictsOldestFirst) {
    // Below the pre-bypass ratio: a reserve frontier, then a refill from the
    // younger objects once the deferrals exhaust it.
    auto r = RunShortfallScenario(1500, 1500, 3000, 6000, 2400);
    EXPECT_EQ(r.queued, 1500);
    EXPECT_EQ(r.admitted_gone, 0);
    EXPECT_EQ(r.old_gone, 1500);
    EXPECT_GE(r.young_gone, 900);
    EXPECT_EQ(r.newest_gone, 0);
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
