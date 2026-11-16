#include "ha/replication/replicated_kv_backend.h"

#include <glog/logging.h>

#include <algorithm>
#include <random>

#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

namespace mooncake::ha::replication {

namespace {

constexpr uint32_t kPullWaitMs = 500;
constexpr auto kRpcTimeout = std::chrono::seconds(10);
constexpr auto kConnectTimeout = std::chrono::seconds(2);
constexpr auto kForwardApplyWait = std::chrono::seconds(30);
constexpr auto kRecordRetryFor = std::chrono::seconds(30);
constexpr auto kCopySessionIdle = std::chrono::seconds(60);

std::string RandomId() {
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    static constexpr char kHex[] = "0123456789abcdef";
    std::string id(16, '0');
    uint64_t bits = rng();
    for (auto& c : id) {
        c = kHex[bits & 0xf];
        bits >>= 4;
    }
    return id;
}

bool ConstantTimeEquals(const std::string& a, const std::string& b) {
    if (a.size() != b.size()) {
        return false;
    }
    unsigned char diff = 0;
    for (size_t i = 0; i < a.size(); ++i) {
        diff |= static_cast<unsigned char>(a[i] ^ b[i]);
    }
    return diff == 0;
}

const char* ModeName(int mode) {
    switch (mode) {
        case 0:
            return "solo";
        case 1:
            return "joining";
        default:
            return "sync";
    }
}

std::mutex g_registry_mutex;
std::shared_ptr<ReplicatedHaKvBackend> g_registered;

class RpcClient {
   public:
    ErrorCode Connect(const std::string& endpoint) {
        if (client_ && endpoint == endpoint_) {
            return ErrorCode::OK;
        }
        client_ = std::make_unique<coro_rpc::coro_rpc_client>();
        endpoint_ = endpoint;
        auto ec = async_simple::coro::syncAwait(
            client_->connect(std::string_view(endpoint), kConnectTimeout));
        if (ec) {
            client_.reset();
            return ErrorCode::RPC_FAIL;
        }
        return ErrorCode::OK;
    }
    void Reset() { client_.reset(); }
    coro_rpc::coro_rpc_client* get() { return client_.get(); }

   private:
    std::unique_ptr<coro_rpc::coro_rpc_client> client_;
    std::string endpoint_;
};

}  // namespace

void RegisterReplicatedBackend(std::shared_ptr<ReplicatedHaKvBackend> backend) {
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    g_registered = std::move(backend);
}

std::shared_ptr<ReplicatedHaKvBackend> RegisteredReplicatedBackend() {
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    return g_registered;
}

ReplicatedHaKvBackend::ReplicatedHaKvBackend(ReplicationConfig config)
    : config_(std::move(config)) {}

tl::expected<std::shared_ptr<ReplicatedHaKvBackend>, ErrorCode>
ReplicatedHaKvBackend::Open(ReplicationConfig config) {
    if (config.token.empty()) {
        LOG(ERROR) << "HA replication needs a token";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto store = sqlite::SqliteHaStore::Open(config.connstring);
    if (!store) {
        return tl::make_unexpected(store.error());
    }
    std::shared_ptr<ReplicatedHaKvBackend> backend(
        new ReplicatedHaKvBackend(std::move(config)));
    backend->store_ = std::move(store.value());
    auto state = backend->store_->ReadReplState();
    if (!state) {
        return tl::make_unexpected(state.error());
    }
    backend->applied_seq_ = state->seq;
    backend->follow_epoch_ = state->epoch;
    backend->follow_copy_ = state->copy_id;
    LOG(INFO) << "action=replication_open, epoch=" << state->epoch
              << ", seq=" << state->seq << ", copy=" << state->copy_id
              << ", was_primary=" << state->primary;
    return backend;
}

ReplicatedHaKvBackend::~ReplicatedHaKvBackend() {
    StopFollowing();
    if (server_) {
        server_->stop();
    }
    if (server_thread_.joinable()) {
        server_thread_.join();
    }
    store_->SetChangeSink(nullptr);
}

bool ReplicatedHaKvBackend::TokenOk(const std::string& token) const {
    return ConstantTimeEquals(token, config_.token);
}

ErrorCode ReplicatedHaKvBackend::StartServer() {
    server_ = std::make_unique<coro_rpc::coro_rpc_server>(4, config_.port);
    server_->register_handler<&ReplicatedHaKvBackend::HandlePull,
                              &ReplicatedHaKvBackend::HandleCopyPage,
                              &ReplicatedHaKvBackend::HandleForward>(this);
    auto started = server_->async_start();
    if (started.hasResult()) {
        LOG(ERROR) << "HA replication server failed on port " << config_.port
                   << ": " << started.value().message();
        return ErrorCode::RPC_FAIL;
    }
    LOG(INFO) << "action=replication_server_started, port=" << config_.port;
    return ErrorCode::OK;
}

// Read paths are local on both roles: a follower's copy is a prefix of the
// primary's, and its own writes wait until they are applied here.
ErrorCode ReplicatedHaKvBackend::Get(std::string_view key, std::string& value) {
    return store_->Get(key, value);
}

ErrorCode ReplicatedHaKvBackend::Range(std::string_view begin_key,
                                       std::string_view end_key, size_t limit,
                                       std::vector<KvPair>& kvs) {
    return store_->Range(begin_key, end_key, limit, kvs);
}

ErrorCode ReplicatedHaKvBackend::Put(std::string_view key,
                                     std::string_view value) {
    ForwardRequest request;
    request.kind = static_cast<int32_t>(ForwardKind::kPut);
    request.key = std::string(key);
    request.value = std::string(value);
    return Forward(std::move(request));
}

ErrorCode ReplicatedHaKvBackend::DeleteRange(std::string_view begin_key,
                                             std::string_view end_key) {
    ForwardRequest request;
    request.kind = static_cast<int32_t>(ForwardKind::kDeleteRange);
    request.key = std::string(begin_key);
    request.end = std::string(end_key);
    return Forward(std::move(request));
}

ErrorCode ReplicatedHaKvBackend::Txn(const KvTxn& txn) {
    ForwardRequest request;
    request.kind = static_cast<int32_t>(ForwardKind::kTxn);
    for (const auto& compare : txn.compares) {
        request.compares.push_back(
            {.key = compare.key,
             .kind = static_cast<int32_t>(compare.kind),
             .expected_value = compare.expected_value,
             .expected_revision =
                 static_cast<int64_t>(compare.expected_revision)});
    }
    for (const auto& put : txn.puts) {
        request.puts.push_back({.key = put.key, .value = put.value});
    }
    return Forward(std::move(request));
}

ErrorCode ReplicatedHaKvBackend::Execute(const ForwardRequest& request,
                                         uint64_t& seq) {
    seq = 0;
    switch (static_cast<ForwardKind>(request.kind)) {
        case ForwardKind::kPut:
            return store_->Put(request.key, request.value, &seq);
        case ForwardKind::kDeleteRange:
            return store_->DeleteRange(request.key, request.end, &seq);
        case ForwardKind::kTxn: {
            KvTxn txn;
            for (const auto& compare : request.compares) {
                txn.compares.push_back(
                    {.key = compare.key,
                     .kind = static_cast<KvCompareKind>(compare.kind),
                     .expected_value = compare.expected_value,
                     .expected_revision = static_cast<EtcdRevisionId>(
                         compare.expected_revision)});
            }
            for (const auto& put : request.puts) {
                txn.puts.push_back({.key = put.key, .value = put.value});
            }
            return store_->Txn(txn, &seq);
        }
    }
    return ErrorCode::INVALID_PARAMS;
}

// A write on either role: the primary commits and waits until it is durable
// on both copies (or the follower is recorded out of sync); a follower asks
// the primary to do that, then waits until the change reached its own copy.
ErrorCode ReplicatedHaKvBackend::Forward(ForwardRequest request) {
    bool primary;
    {
        std::lock_guard<std::mutex> lock(mu_);
        primary = primary_;
    }
    if (primary) {
        uint64_t seq = 0;
        const ErrorCode err = Execute(request, seq);
        if (err != ErrorCode::OK || seq == 0) {
            return err;
        }
        return WaitDurable(seq);
    }
    auto endpoint = PrimaryEndpoint();
    if (!endpoint) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    request.token = config_.token;
    RpcClient client;
    if (client.Connect(*endpoint) != ErrorCode::OK) {
        return ErrorCode::RPC_FAIL;
    }
    auto result = async_simple::coro::syncAwait(
        client.get()->call_for<&ReplicatedHaKvBackend::HandleForward>(
            kRpcTimeout, std::move(request)));
    if (!result) {
        return ErrorCode::RPC_FAIL;
    }
    const ForwardResponse& response = result.value();
    if (response.status != static_cast<int32_t>(ReplStatus::kOk)) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    const auto error = static_cast<ErrorCode>(response.error);
    if (error != ErrorCode::OK || response.seq == 0) {
        return error;
    }
    std::unique_lock<std::mutex> lock(mu_);
    if (!cv_.wait_for(lock, kForwardApplyWait, [&] {
            return applied_seq_ >= response.seq || primary_;
        })) {
        return ErrorCode::RPC_TIMEOUT;
    }
    return ErrorCode::OK;
}

void ReplicatedHaKvBackend::OnCommitted(ChangeSet&& changes) {
    std::lock_guard<std::mutex> lock(mu_);
    committed_seq_ = changes.seq;
    ring_size_ += ChangeSetBytes(changes);
    ring_.push_back(std::move(changes));
    while (ring_size_ > config_.ring_bytes && ring_.size() > 1) {
        ring_size_ -= ChangeSetBytes(ring_.front());
        ring_.pop_front();
    }
    cv_.notify_all();
}

ErrorCode ReplicatedHaKvBackend::WriteRecord(bool in_sync,
                                             const std::string& copy) {
    const ReplicationRecord record{
        .term = term_, .epoch = epoch_, .replica = copy, .in_sync = in_sync};
    const auto give_up = std::chrono::steady_clock::now() + kRecordRetryFor;
    while (true) {
        const ErrorCode err = records_->Write(record);
        if (err == ErrorCode::OK || err == ErrorCode::ETCD_TRANSACTION_FAIL ||
            std::chrono::steady_clock::now() >= give_up) {
            LOG(INFO) << "action=write_replication_record, in_sync="
                      << in_sync << ", replica=" << copy
                      << ", term=" << term_ << ", error=" << toString(err);
            return err;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

// Waits until the change set `seq` may be reported durable: the follower
// applied it, or the record says the follower is not in sync. A change is
// never completed un-acknowledged while the record may still vouch for the
// follower.
ErrorCode ReplicatedHaKvBackend::WaitDurable(uint64_t seq) {
    std::unique_lock<std::mutex> lock(mu_);
    auto deadline = std::chrono::steady_clock::now() + config_.sync_timeout;
    while (true) {
        if (fenced_) {
            return ErrorCode::ETCD_TRANSACTION_FAIL;
        }
        if (acked_seq_ >= seq && !replica_copy_.empty()) {
            return ErrorCode::OK;
        }
        if (mode_ == Mode::kSolo) {
            return ErrorCode::OK;
        }
        if (std::chrono::steady_clock::now() < deadline) {
            cv_.wait_until(lock, deadline);
            continue;
        }
        if (record_busy_) {
            cv_.wait(lock);
            continue;
        }
        if (mode_ == Mode::kJoining) {
            // The record has said "not in sync" since before joining began.
            mode_ = Mode::kSolo;
            cv_.notify_all();
            continue;
        }
        record_busy_ = true;
        const std::string copy = replica_copy_;
        lock.unlock();
        LOG(WARNING) << "action=replication_ack_overdue, seq=" << seq
                     << ", acked=" << acked_seq_ << ", replica=" << copy;
        const ErrorCode err = WriteRecord(false, copy);
        lock.lock();
        record_busy_ = false;
        if (err == ErrorCode::OK) {
            mode_ = Mode::kSolo;
        } else if (err == ErrorCode::ETCD_TRANSACTION_FAIL) {
            fenced_ = true;
        } else {
            // Unknown whether the record changed: keep requiring acks.
            deadline = std::chrono::steady_clock::now() + config_.sync_timeout;
        }
        cv_.notify_all();
    }
}

PullResponse ReplicatedHaKvBackend::HandlePull(PullRequest request) {
    PullResponse response;
    if (!TokenOk(request.token)) {
        response.status = static_cast<int32_t>(ReplStatus::kDenied);
        return response;
    }
    std::unique_lock<std::mutex> lock(mu_);
    if (!primary_ || fenced_) {
        response.status = static_cast<int32_t>(ReplStatus::kNotPrimary);
        return response;
    }
    response.epoch = epoch_;
    const uint64_t ring_first =
        ring_.empty() ? committed_seq_ + 1 : ring_.front().seq;
    if (request.epoch != epoch_ || request.after_seq > committed_seq_ ||
        request.after_seq + 1 < ring_first) {
        response.status = static_cast<int32_t>(ReplStatus::kFullCopy);
        return response;
    }
    if (request.copy_id != replica_copy_) {
        if (mode_ != Mode::kSolo) {
            // The tracked copy's acks must lapse first; its record goes
            // "not in sync" before another copy can join.
            response.status = static_cast<int32_t>(ReplStatus::kRetry);
            return response;
        }
        LOG(INFO) << "action=replica_attached, copy=" << request.copy_id
                  << ", at_seq=" << request.after_seq;
        replica_copy_ = request.copy_id;
        acked_seq_ = request.after_seq;
    }
    acked_seq_ = std::max(acked_seq_, request.after_seq);
    cv_.notify_all();

    if (mode_ == Mode::kSolo && acked_seq_ == committed_seq_ &&
        !record_busy_) {
        // From here every change waits for this copy, so once it has
        // acknowledged everything completed alone, the record may vouch
        // for it.
        mode_ = Mode::kJoining;
        join_target_ = committed_seq_;
        record_busy_ = true;
        const std::string copy = replica_copy_;
        lock.unlock();
        const ErrorCode err = WriteRecord(true, copy);
        lock.lock();
        record_busy_ = false;
        if (err == ErrorCode::ETCD_TRANSACTION_FAIL) {
            fenced_ = true;
        } else {
            // On an unknown outcome the record may vouch for the copy:
            // requiring its acks is the safe assumption.
            mode_ = Mode::kSync;
        }
        cv_.notify_all();
        if (fenced_) {
            response.status = static_cast<int32_t>(ReplStatus::kNotPrimary);
            return response;
        }
    }

    const auto collect = [&] {
        size_t bytes = 0;
        for (const auto& changes : ring_) {
            if (changes.seq <= request.after_seq) {
                continue;
            }
            bytes += ChangeSetBytes(changes);
            response.changes.push_back(changes);
            if (bytes >= std::max<uint64_t>(request.max_bytes, 1)) {
                break;
            }
        }
    };
    if (committed_seq_ <= request.after_seq && request.wait_ms > 0) {
        cv_.wait_for(lock, std::chrono::milliseconds(request.wait_ms), [&] {
            return committed_seq_ > request.after_seq || !primary_;
        });
    }
    collect();
    response.committed_seq = committed_seq_;
    response.status = static_cast<int32_t>(ReplStatus::kOk);
    return response;
}

CopyPageResponse ReplicatedHaKvBackend::HandleCopyPage(
    CopyPageRequest request) {
    CopyPageResponse response;
    if (!TokenOk(request.token)) {
        response.status = static_cast<int32_t>(ReplStatus::kDenied);
        return response;
    }
    std::unique_lock<std::mutex> lock(mu_);
    if (!primary_) {
        response.status = static_cast<int32_t>(ReplStatus::kNotPrimary);
        return response;
    }
    sqlite::SqliteHaStore::SnapshotReader* reader = nullptr;
    bool from_start = false;
    if (request.session == 0) {
        if (copy_sessions_.size() >= 4) {
            copy_sessions_.erase(copy_sessions_.begin());
        }
        auto opened = store_->OpenSnapshotReader();
        if (!opened) {
            response.status = static_cast<int32_t>(ReplStatus::kRetry);
            return response;
        }
        request.session = next_session_++;
        reader = opened->get();
        copy_sessions_[request.session] = std::move(opened.value());
        from_start = true;
    } else {
        auto it = copy_sessions_.find(request.session);
        if (it == copy_sessions_.end()) {
            response.status = static_cast<int32_t>(ReplStatus::kFullCopy);
            return response;
        }
        reader = it->second.get();
    }
    response.session = request.session;
    response.revision = reader->revision();
    response.seq = reader->state().seq;
    response.epoch = reader->state().epoch;
    // Each session reads on its own connection; the lock only keeps the
    // session map consistent.
    lock.unlock();
    const ErrorCode err =
        reader->Next(request.after, from_start,
                     std::max<uint64_t>(request.max_bytes, 1), response.rows,
                     response.done);
    lock.lock();
    if (err != ErrorCode::OK || response.done) {
        copy_sessions_.erase(request.session);
    }
    response.status = static_cast<int32_t>(
        err == ErrorCode::OK ? ReplStatus::kOk : ReplStatus::kRetry);
    return response;
}

ForwardResponse ReplicatedHaKvBackend::HandleForward(ForwardRequest request) {
    ForwardResponse response;
    if (!TokenOk(request.token)) {
        response.status = static_cast<int32_t>(ReplStatus::kDenied);
        return response;
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!primary_ || fenced_) {
            response.status = static_cast<int32_t>(ReplStatus::kNotPrimary);
            return response;
        }
    }
    uint64_t seq = 0;
    ErrorCode err = Execute(request, seq);
    if (err == ErrorCode::OK && seq != 0) {
        err = WaitDurable(seq);
    }
    response.status = static_cast<int32_t>(ReplStatus::kOk);
    response.error = static_cast<int32_t>(err);
    response.seq = err == ErrorCode::OK ? seq : 0;
    return response;
}

std::optional<std::string> ReplicatedHaKvBackend::PrimaryEndpoint() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (primary_endpoint_) {
            return primary_endpoint_;
        }
    }
    if (!locator_) {
        return std::nullopt;
    }
    auto endpoint = locator_();
    std::lock_guard<std::mutex> lock(mu_);
    primary_endpoint_ = endpoint;
    return endpoint;
}

ErrorCode ReplicatedHaKvBackend::FullCopy(const std::string& endpoint) {
    RpcClient client;
    if (client.Connect(endpoint) != ErrorCode::OK) {
        return ErrorCode::RPC_FAIL;
    }
    auto load = store_->BeginFullLoad();
    if (!load) {
        return load.error();
    }
    CopyPageRequest request{.token = config_.token,
                            .session = 0,
                            .after = {},
                            .max_bytes = config_.pull_max_bytes};
    size_t rows = 0;
    while (!stop_following_.load()) {
        auto result = async_simple::coro::syncAwait(
            client.get()->call_for<&ReplicatedHaKvBackend::HandleCopyPage>(
                kRpcTimeout, request));
        if (!result || result.value().status !=
                           static_cast<int32_t>(ReplStatus::kOk)) {
            return ErrorCode::RPC_FAIL;
        }
        const CopyPageResponse& page = result.value();
        if (load.value()->Add(page.rows) != ErrorCode::OK) {
            return ErrorCode::ETCD_OPERATION_ERROR;
        }
        rows += page.rows.size();
        if (!page.rows.empty()) {
            request.after = page.rows.back().key;
        }
        request.session = page.session;
        if (page.done) {
            const ReplState state{.epoch = page.epoch,
                                  .seq = page.seq,
                                  .copy_id = RandomId(),
                                  .primary = false};
            const ErrorCode err = load.value()->Commit(page.revision, state);
            if (err != ErrorCode::OK) {
                return err;
            }
            std::lock_guard<std::mutex> lock(mu_);
            applied_seq_ = state.seq;
            follow_epoch_ = state.epoch;
            follow_copy_ = state.copy_id;
            LOG(INFO) << "action=replication_full_copy, rows=" << rows
                      << ", epoch=" << state.epoch << ", seq=" << state.seq
                      << ", copy=" << state.copy_id;
            return ErrorCode::OK;
        }
    }
    return ErrorCode::ETCD_CTX_CANCELLED;
}

void ReplicatedHaKvBackend::FollowLoop() {
    RpcClient client;
    auto backoff = std::chrono::milliseconds(100);
    const auto lost_primary = [&] {
        client.Reset();
        std::lock_guard<std::mutex> lock(mu_);
        primary_endpoint_.reset();
    };
    while (!stop_following_.load()) {
        auto endpoint = PrimaryEndpoint();
        if (!endpoint || client.Connect(*endpoint) != ErrorCode::OK) {
            lost_primary();
            std::this_thread::sleep_for(backoff);
            backoff = std::min(backoff * 2, std::chrono::milliseconds(1000));
            continue;
        }
        PullRequest request;
        {
            std::lock_guard<std::mutex> lock(mu_);
            request = PullRequest{.token = config_.token,
                                  .epoch = follow_epoch_,
                                  .copy_id = follow_copy_,
                                  .after_seq = applied_seq_,
                                  .wait_ms = kPullWaitMs,
                                  .max_bytes = config_.pull_max_bytes};
        }
        auto result = async_simple::coro::syncAwait(
            client.get()->call_for<&ReplicatedHaKvBackend::HandlePull>(
                kRpcTimeout, request));
        if (!result) {
            lost_primary();
            continue;
        }
        const PullResponse& response = result.value();
        const auto status = static_cast<ReplStatus>(response.status);
        if (status == ReplStatus::kFullCopy) {
            bool started;
            {
                std::lock_guard<std::mutex> lock(mu_);
                started = standby_started_;
            }
            if (started) {
                LOG(ERROR) << "action=replication_history_changed, local_epoch="
                           << request.epoch << ", primary_epoch="
                           << response.epoch;
                if (on_history_change_) {
                    on_history_change_();
                }
                return;
            }
            if (FullCopy(*endpoint) != ErrorCode::OK) {
                lost_primary();
                std::this_thread::sleep_for(backoff);
            }
            continue;
        }
        if (status != ReplStatus::kOk) {
            if (status == ReplStatus::kDenied) {
                LOG(ERROR) << "action=replication_denied, primary="
                           << *endpoint;
            }
            lost_primary();
            std::this_thread::sleep_for(backoff);
            continue;
        }
        backoff = std::chrono::milliseconds(100);
        for (const auto& changes : response.changes) {
            const ErrorCode err = store_->ApplyChangeSet(changes);
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "action=replication_apply_failed, seq="
                           << changes.seq << ", error=" << toString(err);
                lost_primary();
                break;
            }
            std::lock_guard<std::mutex> lock(mu_);
            applied_seq_ = changes.seq;
            cv_.notify_all();
        }
        std::lock_guard<std::mutex> lock(mu_);
        if (applied_seq_ >= response.committed_seq && !caught_up_) {
            caught_up_ = true;
            LOG(INFO) << "action=replication_caught_up, seq=" << applied_seq_
                      << ", epoch=" << follow_epoch_;
            cv_.notify_all();
        }
    }
}

void ReplicatedHaKvBackend::StartFollowing(
    PrimaryLocator locator, std::chrono::milliseconds initial_wait,
    std::function<void()> on_history_change) {
    locator_ = std::move(locator);
    on_history_change_ = std::move(on_history_change);
    stop_following_.store(false);
    follow_thread_ = std::thread([this] { FollowLoop(); });
    std::unique_lock<std::mutex> lock(mu_);
    if (!cv_.wait_for(lock, initial_wait, [&] { return caught_up_; })) {
        LOG(WARNING) << "action=replication_initial_sync_skipped, wait_ms="
                     << initial_wait.count();
    }
    standby_started_ = true;
}

void ReplicatedHaKvBackend::StopFollowing() {
    stop_following_.store(true);
    if (follow_thread_.joinable() &&
        follow_thread_.get_id() != std::this_thread::get_id()) {
        follow_thread_.join();
    }
}

PromotionDecision ReplicatedHaKvBackend::BecomePrimary(
    ReplicationRecordStore& records, uint64_t term) {
    StopFollowing();
    auto local = store_->ReadReplState();
    auto empty = store_->Empty();
    if (!local || !empty) {
        return PromotionDecision::kError;
    }
    tl::expected<std::optional<ReplicationRecord>, ErrorCode> record =
        tl::make_unexpected(ErrorCode::K8S_LEASE_OPERATION_ERROR);
    for (int attempt = 0; attempt < 20 && !record; ++attempt) {
        record = records.Read();
        if (!record) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    if (!record) {
        return PromotionDecision::kError;
    }
    const bool trusted = CopyIsTrusted(*local, *record);
    LOG(INFO) << "action=replication_promotion, trusted=" << trusted
              << ", empty=" << *empty << ", local_epoch=" << local->epoch
              << ", local_copy=" << local->copy_id
              << ", local_primary=" << local->primary << ", record="
              << (record->has_value() ? EncodeReplicationRecord(**record)
                                      : std::string("none"));
    if (!trusted && !*empty) {
        const ErrorCode err = store_->WipeAll();
        LOG(WARNING) << "action=replication_wipe_untrusted_copy, error="
                     << toString(err);
        return err == ErrorCode::OK ? PromotionDecision::kRestart
                                    : PromotionDecision::kError;
    }
    ReplState next{.epoch = RandomId(),
                   .seq = local->seq,
                   .copy_id = RandomId(),
                   .primary = true};
    // Local first: a crash before the record names the new epoch leaves the
    // old record, which still vouches for the old history.
    if (store_->WriteReplState(next) != ErrorCode::OK) {
        return PromotionDecision::kError;
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        records_ = &records;
        term_ = term;
        epoch_ = next.epoch;
        committed_seq_ = next.seq;
        ring_.clear();
        ring_size_ = 0;
        mode_ = Mode::kSolo;
        replica_copy_.clear();
        acked_seq_ = 0;
        fenced_ = false;
    }
    const ErrorCode err = WriteRecord(false, "");
    if (err == ErrorCode::ETCD_TRANSACTION_FAIL) {
        return PromotionDecision::kFenced;
    }
    if (err != ErrorCode::OK) {
        return PromotionDecision::kError;
    }
    store_->SetChangeSink(
        [this](ChangeSet&& changes) { OnCommitted(std::move(changes)); });
    {
        std::lock_guard<std::mutex> lock(mu_);
        primary_ = true;
        cv_.notify_all();
    }
    LOG(INFO) << "action=replication_primary, epoch=" << next.epoch
              << ", seq=" << next.seq << ", term=" << term;
    return trusted ? PromotionDecision::kTrusted : PromotionDecision::kEmpty;
}

ReplicatedHaKvBackend::Stats ReplicatedHaKvBackend::GetStats() {
    std::lock_guard<std::mutex> lock(mu_);
    return Stats{.primary = primary_,
                 .mode = ModeName(static_cast<int>(mode_)),
                 .committed_seq = committed_seq_,
                 .acked_seq = acked_seq_,
                 .applied_seq = applied_seq_,
                 .fenced = fenced_};
}

}  // namespace mooncake::ha::replication
