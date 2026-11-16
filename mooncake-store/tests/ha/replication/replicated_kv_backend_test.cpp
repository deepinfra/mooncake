#include "ha/replication/replicated_kv_backend.h"

#include "ha/serving_gate.h"

#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <thread>

namespace mooncake::ha::replication {
namespace {

using namespace std::chrono_literals;

uint16_t FreePort() {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    socklen_t len = sizeof(addr);
    getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    const uint16_t port = ntohs(addr.sin_port);
    close(fd);
    return port;
}

std::string FreshDb(const std::string& name) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("repl_test_" + std::to_string(getpid()));
    std::filesystem::create_directories(dir);
    const auto path = dir / (name + ".db");
    for (const char* suffix : {"", "-wal", "-shm"}) {
        std::filesystem::remove(path.string() + suffix);
    }
    return path.string();
}

// The Lease annotation, in memory. `fence` makes every write report that
// the term is over.
class FakeRecords : public ReplicationRecordStore {
   public:
    tl::expected<std::optional<ReplicationRecord>, ErrorCode> Read() override {
        std::lock_guard<std::mutex> lock(mu);
        return record;
    }
    ErrorCode Write(const ReplicationRecord& next) override {
        std::lock_guard<std::mutex> lock(mu);
        if (fence) {
            return ErrorCode::ETCD_TRANSACTION_FAIL;
        }
        record = next;
        ++writes;
        return ErrorCode::OK;
    }
    std::optional<ReplicationRecord> Current() {
        std::lock_guard<std::mutex> lock(mu);
        return record;
    }

    std::mutex mu;
    std::optional<ReplicationRecord> record;
    bool fence = false;
    int writes = 0;
};

struct Node {
    std::shared_ptr<ReplicatedHaKvBackend> backend;
    uint16_t port;

    std::string Endpoint() const { return "127.0.0.1:" + std::to_string(port); }
};

Node OpenNode(const std::string& name, std::chrono::milliseconds sync_timeout =
                                           std::chrono::milliseconds(300)) {
    const uint16_t port = FreePort();
    auto backend = ReplicatedHaKvBackend::Open(
        ReplicationConfig{.connstring = FreshDb(name),
                          .port = port,
                          .token = "secret",
                          .sync_timeout = sync_timeout});
    EXPECT_TRUE(backend.has_value());
    EXPECT_EQ((*backend)->StartServer(), ErrorCode::OK);
    return Node{*backend, port};
}

std::string Value(ReplicatedHaKvBackend& backend, const std::string& key) {
    std::string value;
    return backend.Get(key, value) == ErrorCode::OK ? value : "<absent>";
}

bool WaitFor(const std::function<bool()>& done,
             std::chrono::milliseconds limit = 5000ms) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < deadline) {
        if (done()) {
            return true;
        }
        std::this_thread::sleep_for(10ms);
    }
    return done();
}

TEST(SqliteChangeCaptureTest, AReplicaHoldsThePrimarysRowsAndRevisions) {
    auto primary = sqlite::SqliteHaStore::Open(FreshDb("capture_primary"));
    auto replica = sqlite::SqliteHaStore::Open(FreshDb("capture_replica"));
    ASSERT_TRUE(primary && replica);
    std::vector<ChangeSet> log;
    (*primary)->SetChangeSink(
        [&log](ChangeSet&& changes) { log.push_back(std::move(changes)); });
    uint64_t seq = 0;
    ASSERT_EQ((*primary)->Put("/a", "1", &seq), ErrorCode::OK);
    EXPECT_EQ(seq, 1u);
    ASSERT_EQ((*primary)->Put("/b", "2"), ErrorCode::OK);
    ASSERT_EQ((*primary)->Txn(KvTxn{.compares = {{.key = "/a",
                                                   .kind = KvCompareKind::
                                                       kValueEquals,
                                                   .expected_value = "1"}},
                                    .puts = {{"/a", "3"}, {"/c", "4"}}}),
              ErrorCode::OK);
    ASSERT_EQ((*primary)->DeleteRange("/b", "/b\xff"), ErrorCode::OK);
    // A failed compare changes nothing and replicates nothing.
    EXPECT_EQ((*primary)->Txn(KvTxn{.compares = {{.key = "/a",
                                                   .kind = KvCompareKind::
                                                       kKeyNotExists}},
                                    .puts = {{"/d", "5"}}}),
              ErrorCode::ETCD_TRANSACTION_FAIL);
    ASSERT_EQ(log.size(), 4u);

    // Out of order is refused.
    EXPECT_EQ((*replica)->ApplyChangeSet(log[1]), ErrorCode::INVALID_PARAMS);
    for (const auto& changes : log) {
        ASSERT_EQ((*replica)->ApplyChangeSet(changes), ErrorCode::OK);
    }
    for (const char* key : {"/a", "/b", "/c", "/d"}) {
        std::string pv, rv;
        EtcdRevisionId prev = 0, rrev = 0;
        const auto pe = (*primary)->Get(key, pv, &prev);
        const auto re = (*replica)->Get(key, rv, &rrev);
        EXPECT_EQ(pe, re) << key;
        EXPECT_EQ(pv, rv) << key;
        EXPECT_EQ(prev, rrev) << key;
    }
    EXPECT_EQ((*replica)->ReadReplState()->seq, 4u);

    // A full copy is the same rows, revisions and sequence.
    auto copy = sqlite::SqliteHaStore::Open(FreshDb("capture_copy"));
    ASSERT_TRUE(copy);
    ASSERT_EQ((*copy)->Put("/stale", "x"), ErrorCode::OK);
    auto reader = (*primary)->OpenSnapshotReader();
    auto load = (*copy)->BeginFullLoad();
    ASSERT_TRUE(reader && load);
    std::vector<RowImage> rows;
    bool done = false;
    std::string after;
    bool first = true;
    while (!done) {
        ASSERT_EQ((*reader)->Next(after, first, 1, rows, done),
                  ErrorCode::OK);
        first = false;
        if (!rows.empty()) {
            after = rows.back().key;
        }
        ASSERT_EQ((*load)->Add(rows), ErrorCode::OK);
    }
    ASSERT_EQ((*load)->Commit((*reader)->revision(),
                              {.epoch = "e", .seq = 4, .copy_id = "c"}),
              ErrorCode::OK);
    std::string value;
    EXPECT_EQ((*copy)->Get("/stale", value), ErrorCode::ETCD_KEY_NOT_EXIST);
    EtcdRevisionId prev = 0, crev = 0;
    ASSERT_EQ((*primary)->Get("/c", value, &prev), ErrorCode::OK);
    ASSERT_EQ((*copy)->Get("/c", value, &crev), ErrorCode::OK);
    EXPECT_EQ(prev, crev);
    EXPECT_EQ((*copy)->ReadReplState()->epoch, "e");
}

class ReplicationTest : public ::testing::Test {
   protected:
    // A primary, and a follower that joined it and is in sync.
    void StartPair(const std::string& name) {
        primary = OpenNode(name + "_primary");
        follower = OpenNode(name + "_follower");
        ASSERT_EQ(primary.backend->BecomePrimary(records, 1),
                  PromotionDecision::kEmpty);
        follower.backend->StartFollowing(
            [endpoint = primary.Endpoint()] { return endpoint; }, 5000ms,
            [this] { history_changed = true; });
        ASSERT_TRUE(WaitFor([&] {
            auto record = records.Current();
            return record && record->in_sync;
        }));
    }

    FakeRecords records;
    Node primary;
    Node follower;
    std::atomic<bool> history_changed{false};
};

TEST_F(ReplicationTest, AWriteReturnsOnlyOnceTheFollowerHasIt) {
    StartPair("sync");
    for (int i = 0; i < 50; ++i) {
        const std::string key = "/k" + std::to_string(i);
        ASSERT_EQ(primary.backend->Put(key, "v"), ErrorCode::OK);
        EXPECT_EQ(Value(*follower.backend, key), "v") << key;
    }
    auto record = records.Current();
    ASSERT_TRUE(record);
    EXPECT_TRUE(record->in_sync);
    EXPECT_EQ(record->replica, follower.backend->store().ReadReplState()->copy_id);
}

TEST_F(ReplicationTest, ALostFollowerIsRecordedOutOfSyncBeforeTheLeaderGoesOn) {
    StartPair("solo");
    ASSERT_EQ(primary.backend->Put("/before", "v"), ErrorCode::OK);
    follower.backend.reset();  // the follower's node is gone
    const auto start = std::chrono::steady_clock::now();
    ASSERT_EQ(primary.backend->Put("/after", "v"), ErrorCode::OK);
    EXPECT_GE(std::chrono::steady_clock::now() - start, 250ms);
    auto record = records.Current();
    ASSERT_TRUE(record);
    EXPECT_FALSE(record->in_sync);
    // Alone, writes no longer wait.
    const auto quick = std::chrono::steady_clock::now();
    ASSERT_EQ(primary.backend->Put("/alone", "v"), ErrorCode::OK);
    EXPECT_LT(std::chrono::steady_clock::now() - quick, 200ms);
}

TEST_F(ReplicationTest, ADeposedLeaderCannotCompleteAnUnacknowledgedWrite) {
    StartPair("fenced");
    records.fence = true;  // another holder took the Lease
    follower.backend.reset();
    EXPECT_EQ(primary.backend->Put("/late", "v"),
              ErrorCode::ETCD_TRANSACTION_FAIL);
    EXPECT_EQ(primary.backend->Put("/later", "v"),
              ErrorCode::ETCD_TRANSACTION_FAIL);
}

TEST_F(ReplicationTest, AFollowersWriteGoesThroughThePrimary) {
    StartPair("forward");
    ASSERT_EQ(follower.backend->Put("/from_follower", "v"), ErrorCode::OK);
    EXPECT_EQ(Value(*primary.backend, "/from_follower"), "v");
    EXPECT_EQ(Value(*follower.backend, "/from_follower"), "v");
    ASSERT_EQ(follower.backend->Txn(KvTxn{
                  .compares = {{.key = "/from_follower",
                                .kind = KvCompareKind::kValueEquals,
                                .expected_value = "v"}},
                  .puts = {{"/txn", "t"}}}),
              ErrorCode::OK);
    EXPECT_EQ(Value(*follower.backend, "/txn"), "t");
    ASSERT_EQ(follower.backend->DeleteRange("/from", "/from\xff"),
              ErrorCode::OK);
    EXPECT_EQ(Value(*primary.backend, "/from_follower"), "<absent>");
    EXPECT_EQ(Value(*follower.backend, "/from_follower"), "<absent>");
}

TEST_F(ReplicationTest, ForwardsWaitingForAcksDoNotStarveThePulls) {
    StartPair("busy_forwards");
    const int writes_before = records.writes;
    std::vector<std::thread> writers;
    std::atomic<int> failed{0};
    for (int t = 0; t < 8; ++t) {
        writers.emplace_back([&, t] {
            for (int i = 0; i < 25; ++i) {
                const std::string key =
                    "/f" + std::to_string(t) + "_" + std::to_string(i);
                if (follower.backend->Put(key, std::string(4096, 'x')) !=
                    ErrorCode::OK) {
                    ++failed;
                }
            }
        });
    }
    for (auto& writer : writers) {
        writer.join();
    }
    EXPECT_EQ(failed.load(), 0);
    EXPECT_EQ(records.writes, writes_before) << "the record flapped";
    EXPECT_TRUE(records.Current()->in_sync);
}

TEST_F(ReplicationTest, AFollowerFromAnotherHistoryGetsAFullCopy) {
    primary = OpenNode("copy_primary");
    ASSERT_EQ(primary.backend->BecomePrimary(records, 1),
              PromotionDecision::kEmpty);
    for (int i = 0; i < 200; ++i) {
        ASSERT_EQ(primary.backend->Put("/row" + std::to_string(i),
                                       std::string(1000, 'x')),
                  ErrorCode::OK);
    }
    follower = OpenNode("copy_follower");
    ASSERT_EQ(follower.backend->store().Put("/stale", "old"), ErrorCode::OK);
    follower.backend->StartFollowing(
        [endpoint = primary.Endpoint()] { return endpoint; }, 5000ms, [] {});
    EXPECT_EQ(Value(*follower.backend, "/stale"), "<absent>");
    EXPECT_EQ(Value(*follower.backend, "/row199"), std::string(1000, 'x'));
    ASSERT_EQ(primary.backend->Put("/next", "n"), ErrorCode::OK);
    EXPECT_EQ(Value(*follower.backend, "/next"), "n");
}

TEST_F(ReplicationTest, AnInSyncFollowerKeepsItsIndexWhenItTakesTheLead) {
    StartPair("trusted");
    ASSERT_EQ(primary.backend->Put("/kept", "v"), ErrorCode::OK);
    primary.backend.reset();  // the leader's node is gone
    EXPECT_EQ(follower.backend->BecomePrimary(records, 2),
              PromotionDecision::kTrusted);
    EXPECT_EQ(Value(*follower.backend, "/kept"), "v");
    auto record = records.Current();
    ASSERT_TRUE(record);
    EXPECT_EQ(record->term, 2u);
    EXPECT_FALSE(record->in_sync);
    EXPECT_EQ(record->epoch, follower.backend->store().ReadReplState()->epoch);
}

TEST_F(ReplicationTest, AFollowerOutOfSyncIsWipedWhenItTakesTheLead) {
    StartPair("untrusted");
    ASSERT_EQ(primary.backend->Put("/held", "v"), ErrorCode::OK);
    auto follower_backend = follower.backend;
    // The leader went on alone: the follower may have missed changes.
    records.Write({.term = 1,
                   .epoch = primary.backend->store().ReadReplState()->epoch,
                   .replica = "",
                   .in_sync = false});
    primary.backend.reset();
    EXPECT_EQ(follower_backend->BecomePrimary(records, 2),
              PromotionDecision::kRestart);
    EXPECT_TRUE(*follower_backend->store().Empty());
    EXPECT_TRUE(follower_backend->store().ReadReplState()->epoch.empty());
}

TEST_F(ReplicationTest, AFormerPrimaryKeepsItsDataUntilANewHistoryStarts) {
    primary = OpenNode("former_primary");
    ASSERT_EQ(primary.backend->BecomePrimary(records, 1),
              PromotionDecision::kEmpty);
    ASSERT_EQ(primary.backend->Put("/mine", "v"), ErrorCode::OK);
    const std::string connstring = primary.backend->connstring();
    primary.backend.reset();  // restart of the only master
    auto reopened = ReplicatedHaKvBackend::Open(ReplicationConfig{
        .connstring = connstring, .port = FreePort(), .token = "secret"});
    ASSERT_TRUE(reopened);
    EXPECT_EQ((*reopened)->BecomePrimary(records, 2),
              PromotionDecision::kTrusted);
    EXPECT_EQ(Value(**reopened, "/mine"), "v");

    // Once another copy led a new history, this one is stale.
    records.Write({.term = 3, .epoch = "other", .replica = "", .in_sync = false});
    reopened->reset();
    auto stale = ReplicatedHaKvBackend::Open(ReplicationConfig{
        .connstring = connstring, .port = FreePort(), .token = "secret"});
    ASSERT_TRUE(stale);
    EXPECT_EQ((*stale)->BecomePrimary(records, 4),
              PromotionDecision::kRestart);
}

TEST_F(ReplicationTest, AWrongTokenIsRefused) {
    primary = OpenNode("token_primary");
    ASSERT_EQ(primary.backend->BecomePrimary(records, 1),
              PromotionDecision::kEmpty);
    auto response = primary.backend->HandlePull(
        PullRequest{.token = "guess", .epoch = "", .copy_id = "x"});
    EXPECT_EQ(response.status, static_cast<int32_t>(ReplStatus::kDenied));
    auto forward = primary.backend->HandleForward(ForwardRequest{
        .token = "",
        .kind = static_cast<int32_t>(ForwardKind::kPut),
        .key = "/x",
        .value = "v"});
    EXPECT_EQ(forward.status, static_cast<int32_t>(ReplStatus::kDenied));
    EXPECT_EQ(Value(*primary.backend, "/x"), "<absent>");
}

TEST(ServingGateTest, ClosesOnceTheLastRenewalIsPastTheDeadline) {
    ServingGate gate;
    EXPECT_TRUE(gate.Open());  // not armed
    gate.Arm(std::chrono::milliseconds(50), ServingGate::NowMonoNs());
    EXPECT_TRUE(gate.Open());
    std::this_thread::sleep_for(60ms);
    EXPECT_FALSE(gate.Open());  // e.g. resumed from a pause
    gate.Renewed(ServingGate::NowMonoNs() - 1'000'000'000);  // older: ignored
    EXPECT_FALSE(gate.Open());
    gate.Renewed(ServingGate::NowMonoNs());
    EXPECT_TRUE(gate.Open());
    gate.Disarm();
    std::this_thread::sleep_for(60ms);
    EXPECT_TRUE(gate.Open());
}

TEST(ReplicationRecordTest, RoundTripsAndRejectsGarbage) {
    const ReplicationRecord record{
        .term = 7, .epoch = "e1", .replica = "c1", .in_sync = true};
    EXPECT_EQ(ParseReplicationRecord(EncodeReplicationRecord(record)), record);
    EXPECT_FALSE(ParseReplicationRecord("term=x,epoch=e,replica=,in_sync=1"));
    EXPECT_FALSE(ParseReplicationRecord("term=1,epoch=e,in_sync=1"));
    EXPECT_FALSE(ParseReplicationRecord(""));
}

}  // namespace
}  // namespace mooncake::ha::replication
