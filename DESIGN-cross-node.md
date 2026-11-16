# Durable master across two nodes

Two master pods on different nodes, each with its own local SQLite HA store.
The leader is the single writer; the follower keeps an exact copy. A
Kubernetes Lease elects the leader and records whether the follower's copy
may be trusted.

## Why synchronous

The leader frees a removed object's range once the REMOVE is durable, then
can hand the range to another key. If the follower could miss that REMOVE,
it would take over with the old object pointing at the other key's bytes.
So "durable" must mean "on the follower's disk too": every KV mutation on
the leader (OpLog batch + durable prefix, hand-off record, snapshot data)
returns only after the follower acknowledged it. The existing durability
gating (`AppendOpLogWithDurableFinalize`) then carries over unchanged.
Audit: every free or reuse for another key waits for durability or a
delayed release of at least `put_start_release_timeout`; pre-durable
visible actions only expose data early (a lost one is a miss).

## Pieces

1. **Row-image replication** (`SqliteHaStore`). Each mutating call commits
   its rows and a `repl_seq` in one transaction and emits a `ChangeSet`:
   the written rows verbatim (value, create/mod revision, expiry), deleted
   ranges, the revision counter, the new `repl_seq`. The follower applies
   change sets verbatim, so revisions (which Txn compares use) match.
2. **ReplicatedHaKvBackend**, one per process, returned by the KV factory:
   - primary: local mutation, then wait for the follower's cumulative ack
     of its `repl_seq` (sync mode), or complete at once (solo mode);
   - replica: reads local; mutations are forwarded to the primary, then
     wait until the local copy reaches the returned `repl_seq`.
3. **Replication RPC** (coro_rpc, `--ha_replication_port`, shared token):
   `repl_pull` (long-poll; the next pull acks), `repl_snapshot_page`
   (consistent full copy from a read transaction), `repl_forward`.
   A replica whose epoch differs from the primary's, or whose position is
   no longer buffered, gets a full copy.
4. **Replication record** in the Lease's annotation
   `mooncake.kvcache.ai/replication`: `{term, epoch, replica, in_sync}`.
   Written only by the holder, by compare-and-swap on the Lease
   (holder and transitions checked, resourceVersion precondition), so a
   deposed leader cannot write it.
   - sync -> solo: a follower ack overdue (`--ha_replication_sync_timeout_ms`)
     or a lost connection: write `in_sync=false` first; until that write
     succeeds no un-acked mutation completes; a failed CAS (not holder) is
     an error for that mutation (the term is over).
   - solo -> sync: stop completing solo, wait until the follower acked
     everything, write `in_sync=true`, then require acks.
5. **Promotion** (supervisor, after the Lease is acquired, before any KV
   write): the follower stops replicating and switches to primary under a
   new epoch. It keeps its index only if the record says `in_sync`, names
   this process, and matches the term and epoch it replicated from.
   Otherwise:
   - local data non-empty: wipe it and exit; the restart comes up empty;
   - local data empty: serve empty; the crash quarantine (no hand-off
     record) holds free space of every remounted segment.
6. **Snapshots** live in the replicated KV (`snapshot_object_store_type=ha_kv`,
   objects chunked), so a fresh follower can always be rebuilt from the
   leader alone. Its maintenance lock is replicated too (item 9).
7. **Hand-off**: unchanged. The record is a KV Put, replicated before the
   leader releases the Lease.
8. **Fencing**: the Lease helper records when each successful renewal was
   sent (CLOCK_MONOTONIC). Past the renew deadline (3 s) the leader answers
   no RPC (checked before and after each pooled call; Ping and
   ServiceReady too), `/ready` fails, and a watchdog stops the server. A
   candidate gets the Lease only after 5 s without a renewal, so terms do
   not overlap, even across a pause or an API-server partition. Without
   this, a resumed zombie served next to the new leader (12,357 wrong
   values on the harness). The record CAS also fails for a deposed leader.
9. **Snapshot maintenance lock**: the snapshot publish compares the lock's
   owner and create revision in the HA store, so the lock is a replicated
   leased key (create/refresh/delete forwarded to the primary).

## Known limits of the prototype

- The Kubernetes API server is on the serving path: a leader that cannot
  renew stops within 3 s; with both pods cut off, no master serves.
- Each OpLog batch waits a pod-to-pod round trip and the follower's synced
  commit (loopback p50 1.5 ms / p99 13.5 ms vs 0.36 / 0.6 ms alone).

- Each process restart of the follower is a full copy unless its position
  is still in the leader's buffer.
- After an untrusted takeover the master serves empty and holds free space
  for `put_start_release_timeout`.
- Snapshot data passes the network twice (follower -> leader -> follower).
