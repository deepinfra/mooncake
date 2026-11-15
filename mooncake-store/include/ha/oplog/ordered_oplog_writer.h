#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include <async_simple/Future.h>
#include <async_simple/Promise.h>
#include <ylt/util/tl/expected.hpp>

#include "ha/oplog/oplog_batch_types.h"
#include "types.h"

namespace mooncake {

struct OrderedOpLogWriterConfig {
    size_t max_entries_per_batch{1024};
    DurablePrefix initial_durable_prefix{};
    std::chrono::milliseconds retry_timeout{0};
    // Pending-queue slots background producers may not take, so a bulk job
    // such as eviction never leaves a client write without a slot.
    size_t background_headroom{0};
    // Threads that may wait in ReserveWait at once; past it a reserve fails
    // at once instead of parking another handler.
    size_t max_reserve_waiters{64};
    // Admit a group larger than max_entries_per_batch into an empty queue,
    // where it is sealed whole as one record.
    bool admit_oversized_groups{false};
};

enum class OpLogAdmission {
    kForeground,
    kBackground,
};

enum class OrderedOpLogWriterTerminalReason {
    kRetryTimeout,
    kFenced,
    kNonRetryableWriteError,
};

struct OrderedOpLogWriterTerminalState {
    ErrorCode error;
    OrderedOpLogWriterTerminalReason reason;
    DurablePrefix durable_prefix;
    uint64_t occurred_at_ms;
};

class OrderedOpLogWriter {
   public:
    using DurableCallback = std::function<void(const OpLogEntry&)>;
    using TerminalCallback =
        std::function<void(const OrderedOpLogWriterTerminalState&)>;
    using WriteBatchFn =
        std::function<ErrorCode(const OpLogBatchRecord&, const DurablePrefix&)>;

    class Reservation {
       public:
        Reservation();
        Reservation(Reservation&& other) noexcept;
        Reservation& operator=(Reservation&& other) noexcept;
        Reservation(const Reservation&) = delete;
        Reservation& operator=(const Reservation&) = delete;
        ~Reservation();

       private:
        friend class OrderedOpLogWriter;
        Reservation(OrderedOpLogWriter* writer, uint64_t id, uint64_t slots);

        OrderedOpLogWriter* writer_{nullptr};
        uint64_t id_{0};
        uint64_t slots_{0};
    };

    class PendingHandle {
       public:
        PendingHandle();
        uint64_t sequence_id() const;

       private:
        friend class OrderedOpLogWriter;
        explicit PendingHandle(uint64_t sequence_id);

        uint64_t sequence_id_{0};
    };

    OrderedOpLogWriter(OrderedOpLogWriterConfig config,
                       WriteBatchFn write_batch,
                       TerminalCallback terminal_callback = {});
    virtual ~OrderedOpLogWriter();

    tl::expected<Reservation, ErrorCode> Reserve(
        OpLogAdmission admission = OpLogAdmission::kForeground);
    // Reserve slots for one indivisible group of entries, to be committed
    // together via CommitBatch. A group larger than max_entries_per_batch is
    // rejected unless admit_oversized_groups is set and the queue is empty.
    tl::expected<Reservation, ErrorCode> ReserveBatch(
        size_t entry_count,
        OpLogAdmission admission = OpLogAdmission::kForeground);
    // ReserveBatch that waits until `deadline` while the pending queue is
    // full. Any other refusal (retrying, draining, stopped, failed) returns
    // at once.
    tl::expected<Reservation, ErrorCode> ReserveWait(
        size_t entry_count, OpLogAdmission admission,
        std::chrono::steady_clock::time_point deadline);
    virtual tl::expected<PendingHandle, ErrorCode> Commit(
        Reservation&& reservation, OpLogEntry entry, DurableCallback callback);
    // Commit all entries as one indivisible group: they are sealed into a
    // single batch record, so the durable prefix either covers every entry
    // or none of them. Splitting a repair-like set across records would let
    // a durable prefix capture a proper subset. The callback fires once per
    // entry, same as committing each entry on its own. The reservation must
    // come from ReserveBatch with a matching entry_count.
    virtual tl::expected<std::vector<PendingHandle>, ErrorCode> CommitBatch(
        Reservation&& reservation, std::vector<OpLogEntry> entries,
        DurableCallback callback);
    void Abort(Reservation&& reservation);

    // Return a future for durability, independently of callback completion,
    // without blocking the calling thread. An already-covered sequence is
    // immediately ready with OK, even after terminal failure or Stop().
    // Otherwise, terminal failure completes it with the terminal error, and
    // Stop() completes it with UNAVAILABLE_IN_CURRENT_STATUS. Promises are
    // fulfilled outside the writer mutex. Callers can co_await the future and
    // use via(executor) to select the continuation's execution context.
    async_simple::Future<ErrorCode> AwaitDurable(uint64_t sequence);

    bool IsAccepting() const;
    ErrorCode LastError() const;
    std::optional<OrderedOpLogWriterTerminalState> GetTerminalState() const;
    void SetTerminalCallback(TerminalCallback callback);
    // Called by the service only after installing this writer. Older writers
    // can no longer publish runtime metrics after this handoff.
    void ActivateRuntimeMetrics();
    void Start();
    virtual void Stop();
    // Refuses new reservations, waits up to `timeout` for every reserved and
    // committed entry to become durable, then stops. Returns whether all of
    // them did.
    bool FlushAndStop(std::chrono::milliseconds timeout);

   private:
    struct Impl;
    Reservation TakeReservationLocked(size_t entry_count);
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake
