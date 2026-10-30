# Merging upstream into di-main

`di-main` is upstream [kvcache-ai/Mooncake](https://github.com/kvcache-ai/Mooncake) `main` plus the
patches below. Our `main` mirrors upstream. Upstream is merged into `di-main` weekly.

## Procedure

1. Fetch both remotes (`origin` = upstream, `di` = this fork) and check that `di/main` equals `origin/main`.
2. On a clean `di-main`, run `git -c merge.conflictstyle=diff3 merge --no-commit --no-ff origin/main`.
   - In each conflict hunk, diff ours against the base to see what our patch did.
   - Before choosing a side, read the upstream commit that changed the area.
   - A patch can also auto-merge into code upstream has replaced, so check every row of the table below,
     not only the conflicts.
3. Build in a fresh tree:
   ```
   cmake .. -DBUILD_UNIT_TESTS=ON -DUSE_HTTP=ON -DUSE_ETCD=ON -DUSE_CUDA=ON -DUSE_INTRA_NVLINK=ON -DWITH_EP=OFF \
     -DSTORE_USE_ETCD=ON -DCMAKE_BUILD_TYPE=Release -DENABLE_KV_EVENTS=ON -DUSE_TENT=ON
   make -j"$(nproc)" -k
   ```
4. Run the tests for what our patches touch: `kv_snapshot_test`, `kv_event_publisher_test`,
   `offload_on_evict_test`, `evictor_test`, `master_service_evict_scenario_test`, `master_admin_server_test`,
   `object_route_test`.
5. Commit the merge as "Merge upstream kvcache-ai/Mooncake main (<sha>) into di-main". Put each port in the
   body, in 5 lines or fewer. Commits are signed before they are pushed.

## Our patches

| Patch | What | Where it lives now |
|---|---|---|
| `4d01e691` TENT SHM | Same-host GPU<->store copies without a NIC (hugetlbfs files or a cuMem pool); see [README](README.md) | `real_client.cpp` segment setup, `transfer_task.cpp`, TENT shm transport and platform |
| `2f6699c3` | KV removal published when eviction empties an object | `PublishKvRemovedAfterEvict`, called by the Evictor's `KvEventObserver` |
| `b7405f12` | SSD restore buffer from a CUDA VMM pool | `aligned_client_buffer.cpp` |
| `075d7ab5` | Offload admission (`--offload_min_loads`, `--offload_max_bytes_per_sec`) | `MasterService::AdmitOffload`, asked through `eviction::OffloadQueue::Admit` |
| `de1c056d` | KV event batching during eviction bursts | `kv_event_publisher.cpp` |
| `ace0b076` | Oldest-first eviction under offload deferrals | Dropped at the 2026-10-28 merge; its tests stay |
| `66bb2fec` | `GET /kv_events/snapshot` | `GetKvSnapshotPage`, which pages `ObjectRoute` stripes |
| `65405206` | Unmounted segments retract their KV media | `CleanupStaleHandles` |
| `MC_STORE_BATCHED_GET` (part of `4d01e691`) | Group multi-buffer reads per target | `GroupMultiBufferReadsFromEnv` in `real_client.cpp`, which turns on upstream's per-target grouping for `batch_get_into_multi_buffers` |

## Log

### 2026-10-30: upstream `6c83ea2b`

There were 17 upstream commits since the base `811c97f0`. Nothing conflicted, and none of them makes one of our patches
obsolete.

- **#5032 (`390290f0`) removed dead and test-only code from `MasterService`**: the `BatchReplicaClear` overload pair,
  `SubmitReplicaActionProposal`, `MetadataAccessorRW`'s `cleanup_invalid` flag, and the segment-op retry queue
  (`PersistSegmentOpForHAOrEnqueue` is now `PersistSegmentOpForHA`, without retry). It is the only commit that touches
  our files. None of our patches uses what it removed, and the functions it changed hold none of our code.
  `UnmountSegment` changed only in its HA oplog call; our retraction stays in `CleanupStaleHandles`.
- Closest in topic, but not overlapping:
  - **#4885 (`c5d355a2`)** lets the Conductor index vLLM CPU events as `cpu_local`, with the store's `cpu_share`
    taking precedence. We do not run the Conductor; our KV events and snapshot serve the Dynamo FE.
  - **#5068 (`5e50b615`)** waits for in-flight io_uring reads before TENT frees a sub-batch. That is TENT's io_uring
    transport, not the store's SSD tier (`MOONCAKE_OFFLOAD_USE_URING`) or our SHM transport.
- The rest are RDMA (TENT selective signaling #4632, classic congestion control #5064, no-atomics QPs #5047), hp_tcp,
  EFA, EP, PG, CI and docs.

Tests (fresh tree in `mc-build-mert`, the flags above):
- Passing: `kv_snapshot_test` 6/6, `kv_event_publisher_test` 16/16, `offload_on_evict_test` 5/5, `evictor_test` 26/26,
  `master_service_evict_scenario_test` 23/23, `object_route_test` 37/37, `eviction_index_test` 13/13.
- `master_admin_server_test` 64/65: the same `DfsShardCountExpandsAndValidatesRequests` failure as before.
- `high_availability_test` does not compile in this container (`StringFlagDestructor`, a gflags macro); neither
  upstream nor our patches touched it this round.

### 2026-10-28: upstream `811c97f0`

There were 65 upstream commits since the base `12a7e9f1`. Ten files conflicted, in 14 hunks.

- **#4887 replaced `BatchEvict` and the namespace eviction with `eviction::Evictor`**, which walks the
  lease-ordered `LeaseEvictionIndex` (100 ms buckets).
  - `ace0b076` was dropped, since its sort has nothing left to apply to. Its `ShortfallEvictsOldestFirst` and
    `ShortfallRefillEvictsOldestFirst` pass unchanged on the Evictor, so upstream keeps the order.
  - `075d7ab5` admission now runs as `eviction::OffloadQueue::Admit`, which `OffloadingReclaimer` asks after
    the local-disk and in-flight checks. An object it does not admit goes through the inner reclaimer, as
    before.
- **#4887 also removed the metadata shards**, so `GetKvSnapshotPage` pages the tenant's `ObjectRoute` stripes.
  - There are 1024 stripes, as there were shards, so the HTTP `shard` parameter is unchanged.
  - The stripes are walked with a new `ObjectRoute::ReadCursor(first_stripe, end_stripe)`.
  - Events are still enqueued under the key lock, so the fence argument holds. The fence test passes 6/6
    repeats.
- **#4799 removed `BatchGetWhenPreferSameNode`.** `MC_STORE_BATCHED_GET` now turns on upstream's per-target
  grouping for multi-buffer reads. Puts are unchanged. Keys whose replica is not an exact-size memory replica
  stay per key; before, they made the whole batch per key.
- **The TENT platform and buffer allocation hunks** were additive on both sides, so both were kept.

Tests:
- Passing: `kv_snapshot_test` 6/6, `kv_event_publisher_test` 16/16, `offload_on_evict_test` 5/5,
  `evictor_test` 26/26, `master_service_evict_scenario_test` 23/23, `object_route_test` 37/37,
  `eviction_index_test` 13/13.
- `master_admin_server_test` 64/65: `DfsShardCountExpandsAndValidatesRequests` (jsoncpp `failIfExtra`) failed
  before the merge too.
