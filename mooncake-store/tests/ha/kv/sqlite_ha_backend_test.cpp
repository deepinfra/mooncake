#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "ha/common/sqlite/sqlite_ha_store.h"
#include "ha/kv/ha_kv_backend_factory.h"
#include "ha/leadership/leader_coordinator_factory.h"
#include "ha/oplog/mock_metadata_store.h"
#include "ha/oplog/oplog_applier.h"
#include "ha/oplog/oplog_batch_standby_reader.h"
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/oplog/oplog_batch_types.h"
#include "ha/oplog/oplog_types.h"
#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/snapshot/snapshot_maintenance_lease.h"
#include "metadata_store.h"

namespace mooncake::test {
namespace {

using namespace std::chrono_literals;
using ha::sqlite::SqliteHaStore;

constexpr char kCluster[] = "sqlitecluster";

OpLogEntry MakePut(uint64_t seq, const std::string& key) {
    MetadataPayload payload;
    payload.client_id = {1, 2};
    payload.size = 64;
    const auto packed = struct_pack::serialize(payload);
    OpLogEntry entry;
    entry.sequence_id = seq;
    entry.timestamp_ms = 1234567890;
    entry.op_type = OpType::PUT_END;
    entry.tenant_id = "tenant";
    entry.object_key = key;
    entry.payload.assign(packed.begin(), packed.end());
    entry.checksum = ComputeOpLogChecksum(entry.payload);
    return entry;
}

OpLogBatchRecord MakeBatch(uint64_t batch_id, uint64_t seq,
                           const std::string& key) {
    OpLogBatchRecord batch;
    batch.batch_id = batch_id;
    batch.first_seq = seq;
    batch.last_seq = seq;
    batch.entries.push_back(MakePut(seq, key));
    return batch;
}

class SqliteHaTest : public ::testing::Test {
   protected:
    void SetUp() override {
        char pattern[] = "/tmp/sqlite-ha-test-XXXXXX";
        ASSERT_NE(nullptr, ::mkdtemp(pattern));
        dir_ = pattern;
        path_ = dir_ + "/ha.db";
    }
    void TearDown() override { std::filesystem::remove_all(dir_); }

    ha::HABackendSpec Spec(int64_t ttl_sec = 1) const {
        return ha::HABackendSpec{.type = ha::HABackendType::SQLITE,
                                 .connstring = "sqlite://" + path_,
                                 .cluster_namespace = kCluster,
                                 .leader_lease_ttl_sec = ttl_sec};
    }
    std::shared_ptr<HaKvBackend> Backend() const {
        auto backend = CreateHaKvBackend(Spec());
        EXPECT_TRUE(backend.has_value());
        return backend ? backend.value() : nullptr;
    }
    std::unique_ptr<SqliteHaStore> Store() const {
        auto store = SqliteHaStore::Open(path_);
        EXPECT_TRUE(store.has_value());
        return store ? std::move(store.value()) : nullptr;
    }

    std::string dir_;
    std::string path_;
};

TEST_F(SqliteHaTest, CompareAndSwapKeepsBinaryValues) {
    auto backend = Backend();
    const std::string key = "/oplog/c/durable_prefix";
    const std::string binary_value("a\0b", 3);

    KvTxn create;
    create.compares.push_back(
        {.key = key, .kind = KvCompareKind::kKeyNotExists});
    create.puts.push_back({.key = key, .value = binary_value});
    ASSERT_EQ(ErrorCode::OK, backend->Txn(create));
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, backend->Txn(create));

    KvTxn mismatch;
    mismatch.compares.push_back({.key = key,
                                 .kind = KvCompareKind::kValueEquals,
                                 .expected_value = "a"});
    mismatch.puts.push_back({.key = key, .value = "next"});
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, backend->Txn(mismatch));

    std::string value;
    ASSERT_EQ(ErrorCode::OK, backend->Get(key, value));
    EXPECT_EQ(binary_value, value);
    EXPECT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST, backend->Get("/missing", value));

    KvTxn missing_value;
    missing_value.compares.push_back({.key = "/missing",
                                      .kind = KvCompareKind::kValueEquals,
                                      .expected_value = ""});
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, backend->Txn(missing_value));
}

TEST_F(SqliteHaTest, RangeIsBytewiseOrderedAndPaginated) {
    auto backend = Backend();
    const std::string prefix = "/oplog/c/batches/";
    for (const char* id : {"00000000000000000003", "00000000000000000001",
                           "00000000000000000002"}) {
        ASSERT_EQ(ErrorCode::OK, backend->Put(prefix + id, id));
    }
    ASSERT_EQ(ErrorCode::OK, backend->Put("/oplog/c/batches0", "outside"));

    const std::string end = "/oplog/c/batches0";
    std::vector<KvPair> page;
    ASSERT_EQ(ErrorCode::OK, backend->Range(prefix, end, 1, page));
    ASSERT_EQ(1u, page.size());
    EXPECT_EQ(prefix + "00000000000000000001", page[0].key);

    ASSERT_EQ(ErrorCode::OK, backend->Range(page[0].key + '\0', end, 10, page));
    ASSERT_EQ(2u, page.size());
    EXPECT_EQ(prefix + "00000000000000000002", page[0].key);
    EXPECT_EQ(prefix + "00000000000000000003", page[1].key);

    ASSERT_EQ(ErrorCode::OK, backend->Range(prefix, end, 0, page));
    EXPECT_EQ(3u, page.size());
    ASSERT_EQ(ErrorCode::OK,
              backend->Range(prefix, std::string(1, '\0'), 0, page));
    EXPECT_EQ(4u, page.size());

    ASSERT_EQ(ErrorCode::OK,
              backend->DeleteRange(prefix + "00000000000000000001",
                                   prefix + "00000000000000000003"));
    ASSERT_EQ(ErrorCode::OK, backend->Range(prefix, end, 0, page));
    ASSERT_EQ(1u, page.size());
    EXPECT_EQ(prefix + "00000000000000000003", page[0].key);
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              backend->DeleteRange(prefix, std::string(1, '\0')));
}

TEST_F(SqliteHaTest, LeasedKeyFencesByCreateRevision) {
    auto store = Store();
    const std::string key = "/lock";
    EtcdRevisionId first = 0;
    ASSERT_EQ(ErrorCode::OK, store->CreateLeasedKey(key, "a", 300ms, first));
    EtcdRevisionId ignored = 0;
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL,
              store->CreateLeasedKey(key, "b", 300ms, ignored));

    KvTxn fenced;
    fenced.compares = {{.key = key,
                        .kind = KvCompareKind::kValueEquals,
                        .expected_value = "a"},
                       {.key = key,
                        .kind = KvCompareKind::kCreateRevisionEquals,
                        .expected_revision = first}};
    fenced.puts.push_back({.key = "/pointer", .value = "1"});
    EXPECT_EQ(ErrorCode::OK, store->Txn(fenced));

    std::this_thread::sleep_for(400ms);
    EXPECT_FALSE(store->RefreshLeasedKey(key, "a", first, 300ms).value());
    std::string value;
    EXPECT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST, store->Get(key, value));
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, store->Txn(fenced));

    EtcdRevisionId second = 0;
    ASSERT_EQ(ErrorCode::OK, store->CreateLeasedKey(key, "a", 300ms, second));
    EXPECT_GT(second, first);
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, store->Txn(fenced));
    EXPECT_TRUE(store->RefreshLeasedKey(key, "a", second, 300ms).value());
}

TEST_F(SqliteHaTest, StandbyFollowsAndOpLogSurvivesReopen) {
    auto backend = Backend();
    OpLogBatchStorage primary(kCluster, *backend);
    DurablePrefix prefix;
    ASSERT_EQ(ErrorCode::OK, primary.InitDurablePrefix(prefix));
    ASSERT_EQ(ErrorCode::OK, primary.ClaimProducerView(3));
    ASSERT_EQ(ErrorCode::OK, primary.WriteBatchAndAdvancePrefix(
                                 MakeBatch(1, 1, "alpha"), prefix, 3));
    prefix = {.batch_id = 1, .last_seq = 1};

    MockMetadataStore metadata;
    OpLogApplier applier(&metadata, kCluster);
    auto standby_backend = Backend();
    OpLogBatchStandbyReader reader(kCluster, *standby_backend, applier);
    auto first = reader.PollOnce();
    EXPECT_EQ(OpLogBatchStandbyPollDisposition::OK, first.disposition);
    EXPECT_EQ(1u, first.applied_entries);
    EXPECT_TRUE(metadata.GetMetadata("tenant", "alpha").has_value());

    ASSERT_EQ(ErrorCode::OK, primary.ClaimProducerView(4));
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL,
              primary.WriteBatchAndAdvancePrefix(MakeBatch(2, 2, "stale"),
                                                 prefix, 3));
    ASSERT_EQ(ErrorCode::OK, primary.WriteBatchAndAdvancePrefix(
                                 MakeBatch(2, 2, "beta"), prefix, 4));
    backend.reset();
    standby_backend.reset();

    auto reopened = Backend();
    OpLogBatchStorage restarted(kCluster, *reopened);
    DurablePrefix durable;
    ASSERT_EQ(ErrorCode::OK, restarted.InitDurablePrefix(durable));
    EXPECT_EQ(2u, durable.batch_id);
    std::vector<OpLogBatchRecord> batches;
    ASSERT_EQ(ErrorCode::OK, restarted.ReadBatchesAfter(0, 10, batches));
    ASSERT_EQ(2u, batches.size());
    EXPECT_EQ("beta", batches[1].entries[0].object_key);

    OpLogBatchStandbyReader resumed(kCluster, *reopened, applier);
    auto second = resumed.PollOnce();
    EXPECT_EQ(OpLogBatchStandbyPollDisposition::OK, second.disposition);
    EXPECT_TRUE(metadata.GetMetadata("tenant", "beta").has_value());
}

TEST_F(SqliteHaTest, LeaderElectionFailsOverOnExpiry) {
    auto first = ha::CreateLeaderCoordinator(Spec());
    auto second = ha::CreateLeaderCoordinator(Spec());
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_FALSE((*first)->ReadCurrentView().value().has_value());

    auto won = (*first)->TryAcquireLeadership("10.0.0.1:50051");
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, won->status);
    auto lost = (*second)->TryAcquireLeadership("10.0.0.2:50051");
    ASSERT_EQ(ha::AcquireLeadershipStatus::CONTENDED, lost->status);
    EXPECT_EQ("10.0.0.1:50051", lost->observed_view->leader_address);

    ASSERT_TRUE((*first)->RenewLeadership(*won->session).value());
    std::atomic<int> losses{0};
    auto monitor = (*first)->StartLeadershipMonitor(
        *won->session, [&](ha::LeadershipLossReason) { ++losses; });
    ASSERT_TRUE(monitor.has_value());

    std::this_thread::sleep_for(2500ms);
    EXPECT_EQ(ha::AcquireLeadershipStatus::CONTENDED,
              (*second)->TryAcquireLeadership("10.0.0.2:50051")->status);
    EXPECT_TRUE((*first)->RenewLeadership(*won->session).value());

    // A holder that stops renewing, as a killed process does, loses the
    // record one TTL later and the next term outranks it.
    auto store = Store();
    ASSERT_EQ(ErrorCode::OK,
              store->ReleaseLeader(kCluster, won->session->owner_token));
    auto taken = (*second)->TryAcquireLeadership("10.0.0.2:50051");
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, taken->status);
    EXPECT_GT(taken->session->view.view_version,
              won->session->view.view_version);
    ASSERT_TRUE((*second)->RenewLeadership(*taken->session).value());
    std::this_thread::sleep_for(1000ms);
    EXPECT_EQ(1, losses.load());
    EXPECT_FALSE((*first)->RenewLeadership(*won->session).value());

    auto changed =
        (*first)->WaitForViewChange(won->session->view.view_version, 100ms);
    ASSERT_TRUE(changed->changed);
    ASSERT_TRUE(changed->current_view.has_value());
    EXPECT_EQ("10.0.0.2:50051", changed->current_view->leader_address);

    ASSERT_EQ(ErrorCode::OK, (*second)->ReleaseLeadership(*taken->session));
    EXPECT_FALSE((*first)->ReadCurrentView().value().has_value());
    auto again = (*first)->TryAcquireLeadership("10.0.0.1:50051");
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, again->status);
    EXPECT_GT(again->session->view.view_version,
              taken->session->view.view_version);
    EXPECT_TRUE((*first)->RenewLeadership(*again->session).value());
}

TEST_F(SqliteHaTest, StaleLeaderRecordExpires) {
    auto store = Store();
    auto version =
        store->AcquireLeader(kCluster, "10.0.0.1:50051", "dead", 200ms);
    ASSERT_TRUE(version->has_value());
    auto coordinator = ha::CreateLeaderCoordinator(Spec());
    EXPECT_EQ(ha::AcquireLeadershipStatus::CONTENDED,
              (*coordinator)->TryAcquireLeadership("10.0.0.2:50051")->status);
    std::this_thread::sleep_for(300ms);
    auto taken = (*coordinator)->TryAcquireLeadership("10.0.0.2:50051");
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, taken->status);
    EXPECT_EQ(version->value() + 1, taken->session->view.view_version);
}

TEST_F(SqliteHaTest, MaintenanceLeaseIsExclusive) {
    std::shared_ptr<SqliteHaStore> store = Store();
    SnapshotMaintenanceLease first(kCluster, store);
    SnapshotMaintenanceLease second(kCluster, store);
    ASSERT_EQ(ErrorCode::OK, first.Acquire());
    EXPECT_TRUE(first.IsHeld());
    EXPECT_GT(first.lock_create_revision(), 0);
    EXPECT_EQ(std::to_string(first.lease_id()), first.owner_token());
    // A snapshot id is "<batch>-<owner token>" and must parse back.
    EXPECT_FALSE(ha::BuildBatchOpLogSnapshotArtifactPrefix(
                     "root", "7-" + first.owner_token())
                     .empty());
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, second.Acquire());
    EXPECT_FALSE(second.IsHeld());

    std::string owner;
    EtcdRevisionId revision = 0;
    ASSERT_EQ(ErrorCode::OK, store->Get(first.lock_key(), owner, &revision));
    EXPECT_EQ(first.owner_token(), owner);
    EXPECT_EQ(first.lock_create_revision(), revision);

    ASSERT_EQ(ErrorCode::OK, first.Release());
    EXPECT_FALSE(first.IsHeld());
    ASSERT_EQ(ErrorCode::OK, second.Acquire());
    EXPECT_TRUE(second.IsHeld());
}

TEST_F(SqliteHaTest, ConnstringIsRequired) {
    auto spec = Spec();
    spec.connstring.clear();
    EXPECT_FALSE(CreateHaKvBackend(spec).has_value());
    EXPECT_EQ("/a/b.db", SqliteHaStore::ResolvePath("sqlite:///a/b.db"));
    EXPECT_EQ("/a/b.db", SqliteHaStore::ResolvePath("/a/b.db"));
}

}  // namespace
}  // namespace mooncake::test
