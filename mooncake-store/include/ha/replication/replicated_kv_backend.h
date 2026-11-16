#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/struct_pack.hpp>
#include <ylt/util/tl/expected.hpp>

#include "ha/common/sqlite/sqlite_ha_store.h"
#include "ha/kv/ha_kv_backend.h"
#include "ha/replication/change_set.h"
#include "ha/replication/replication_record.h"
#include "rpc_worker_pool.h"
#include "types.h"

namespace mooncake::ha::replication {

enum class ReplStatus : int32_t {
    kOk = 0,
    kFullCopy = 1,    // the requester's history is not a prefix of ours
    kNotPrimary = 2,  // ask the Lease holder
    kDenied = 3,      // wrong token
    kRetry = 4,       // another copy is still being dropped
};

struct PullRequest {
    std::string token;
    std::string epoch;
    std::string copy_id;
    uint64_t after_seq{0};  // also acknowledges everything up to it
    uint32_t wait_ms{0};
    uint64_t max_bytes{0};
};
YLT_REFL(PullRequest, token, epoch, copy_id, after_seq, wait_ms, max_bytes);

struct PullResponse {
    int32_t status{0};
    std::string epoch;
    uint64_t committed_seq{0};
    std::vector<ChangeSet> changes;
};
YLT_REFL(PullResponse, status, epoch, committed_seq, changes);

struct CopyPageRequest {
    std::string token;
    uint64_t session{0};  // 0 opens a new consistent read
    std::string after;
    uint64_t max_bytes{0};
};
YLT_REFL(CopyPageRequest, token, session, after, max_bytes);

struct CopyPageResponse {
    int32_t status{0};
    uint64_t session{0};
    std::vector<RowImage> rows;
    bool done{false};
    int64_t revision{0};
    uint64_t seq{0};
    std::string epoch;
};
YLT_REFL(CopyPageResponse, status, session, rows, done, revision, seq, epoch);

struct WireCompare {
    std::string key;
    int32_t kind{0};
    std::string expected_value;
    int64_t expected_revision{0};
};
YLT_REFL(WireCompare, key, kind, expected_value, expected_revision);

struct WirePut {
    std::string key;
    std::string value;
};
YLT_REFL(WirePut, key, value);

enum class ForwardKind : int32_t { kPut = 0, kDeleteRange = 1, kTxn = 2 };

struct ForwardRequest {
    std::string token;
    int32_t kind{0};
    std::string key;
    std::string value;
    std::string end;
    std::vector<WireCompare> compares;
    std::vector<WirePut> puts;
};
YLT_REFL(ForwardRequest, token, kind, key, value, end, compares, puts);

struct ForwardResponse {
    int32_t status{0};
    int32_t error{0};
    uint64_t seq{0};
};
YLT_REFL(ForwardResponse, status, error, seq);

struct ReplicationConfig {
    std::string connstring;
    uint16_t port{0};
    std::string token;
    // How long a primary waits for its follower's acknowledgment before it
    // records the follower out of sync and goes on alone.
    std::chrono::milliseconds sync_timeout{1000};
    // Change sets a primary keeps for a follower that reconnects.
    size_t ring_bytes{256u << 20};
    size_t pull_max_bytes{16u << 20};
};

enum class PromotionDecision {
    kTrusted,  // this copy keeps its index
    kEmpty,    // nothing to trust or keep: serve empty
    kRestart,  // the copy was not trusted and is wiped; restart empty
    kFenced,   // lost the term before taking the lead
    kError,
};

// The HA KV store of a master whose two pods sit on different nodes. Each
// pod keeps its own SQLite store. The primary (the Lease holder) is the only
// writer: each change commits locally and returns once the follower has
// applied it, or once the follower is recorded out of sync. The follower
// applies the primary's change sets verbatim and forwards its own writes to
// the primary.
class ReplicatedHaKvBackend final
    : public HaKvBackend,
      public std::enable_shared_from_this<ReplicatedHaKvBackend> {
   public:
    // Locates the primary's replication endpoint ("host:port"), or nullopt.
    using PrimaryLocator = std::function<std::optional<std::string>()>;

    static tl::expected<std::shared_ptr<ReplicatedHaKvBackend>, ErrorCode>
    Open(ReplicationConfig config);
    ~ReplicatedHaKvBackend() override;

    const std::string& connstring() const { return config_.connstring; }
    sqlite::SqliteHaStore& store() { return *store_; }

    // HaKvBackend.
    ErrorCode Get(std::string_view key, std::string& value) override;
    ErrorCode Put(std::string_view key, std::string_view value) override;
    ErrorCode Range(std::string_view begin_key, std::string_view end_key,
                    size_t limit, std::vector<KvPair>& kvs) override;
    ErrorCode DeleteRange(std::string_view begin_key,
                          std::string_view end_key) override;
    bool SupportsTxn() const override { return true; }
    bool AcceptsUnboundedRecords() const override { return true; }
    ErrorCode Txn(const KvTxn& txn) override;

    ErrorCode StartServer();
    // Follows the primary the locator names. Returns once this copy caught
    // up with one, or after `initial_wait` without finding one, so the
    // standby starts on data that belongs to the current history. A later
    // change of history ends the process (`on_history_change`).
    // Gives up early once no primary has been located for `no_primary_wait`.
    void StartFollowing(PrimaryLocator locator,
                        std::chrono::milliseconds initial_wait,
                        std::function<void()> on_history_change,
                        std::chrono::milliseconds no_primary_wait =
                            std::chrono::milliseconds(8000));
    // At promotion, before any write of the new term: stops following,
    // decides whether this copy may be trusted, and makes this store the
    // primary of a new epoch.
    PromotionDecision BecomePrimary(ReplicationRecordStore& records,
                                    uint64_t term);

    // Server side, exposed for the RPC registration and tests.
    PullResponse HandlePull(PullRequest request);
    CopyPageResponse HandleCopyPage(CopyPageRequest request);
    ForwardResponse HandleForward(ForwardRequest request);

    struct Stats {
        bool primary{false};
        std::string mode;
        uint64_t committed_seq{0};
        uint64_t acked_seq{0};
        uint64_t applied_seq{0};
        bool fenced{false};
    };
    Stats GetStats();

   private:
    enum class Mode { kSolo, kJoining, kSync };

    explicit ReplicatedHaKvBackend(ReplicationConfig config);

    void OnCommitted(ChangeSet&& changes);
    ErrorCode WaitDurable(uint64_t seq);
    ErrorCode WriteRecord(bool in_sync, const std::string& copy);
    ErrorCode Forward(ForwardRequest request);
    ErrorCode Execute(const ForwardRequest& request, uint64_t& seq);
    bool TokenOk(const std::string& token) const;
    void FollowLoop();
    ErrorCode FullCopy(const std::string& endpoint);
    void StopFollowing();
    std::optional<std::string> PrimaryEndpoint();

    ReplicationConfig config_;
    std::unique_ptr<sqlite::SqliteHaStore> store_;
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    std::thread server_thread_;
    // Handlers block (a pull waits for changes, a forward for the follower's
    // acknowledgment), so they run off the io threads, on separate pools: a
    // forward waiting for an ack never holds up the pull that carries it.
    std::unique_ptr<RpcWorkerPool> pull_pool_;
    std::unique_ptr<RpcWorkerPool> forward_pool_;

    std::mutex mu_;
    std::condition_variable cv_;
    // Primary.
    bool primary_{false};
    std::string epoch_;
    uint64_t term_{0};
    uint64_t committed_seq_{0};
    std::deque<ChangeSet> ring_;
    size_t ring_size_{0};
    Mode mode_{Mode::kSolo};
    std::string replica_copy_;
    uint64_t acked_seq_{0};
    uint64_t join_target_{0};
    bool record_busy_{false};
    bool fenced_{false};
    ReplicationRecordStore* records_{nullptr};
    std::map<uint64_t, std::unique_ptr<sqlite::SqliteHaStore::SnapshotReader>>
        copy_sessions_;
    uint64_t next_session_{1};
    // Follower.
    PrimaryLocator locator_;
    std::function<void()> on_history_change_;
    std::thread follow_thread_;
    std::atomic<bool> stop_following_{false};
    bool caught_up_{false};
    bool standby_started_{false};
    uint64_t applied_seq_{0};
    std::string follow_epoch_;
    std::string follow_copy_;
    std::optional<std::string> primary_endpoint_;
};

// The process's replicated store, if it runs one: the HA KV factory returns
// it for its connstring, so every component writes through it.
void RegisterReplicatedBackend(std::shared_ptr<ReplicatedHaKvBackend> backend);
std::shared_ptr<ReplicatedHaKvBackend> RegisteredReplicatedBackend();

}  // namespace mooncake::ha::replication
