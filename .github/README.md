# Mooncake — DeepInfra fork

This is DeepInfra's fork of [kvcache-ai/Mooncake](https://github.com/kvcache-ai/Mooncake).
See upstream's original [README](../README.md) and [docs](https://kvcache-ai.github.io/Mooncake/).

This fork adds to Mooncake a NIC-free same-host transfer capability
using shared mem (`shm`), which
 - is faster, and
 - doesn't require InfiniBand or RoCE

when the transfer happens on the same host.

## How it works

We aim to achieve SoTA KV cache hit rates on our nodes with no InfiniBand
or RoCE when the model takes 1 or 2 GPUs by scheduling carefully:

- All engines serving a model on a node share one Mooncake store on that node.
  KV moves between GPU memory and the store through shared host memory (TENT
  SHM transport), with no NIC involved.
- Our [router](https://github.com/deepinfra/dynamo) treats the cluster as one clique
  per node instead of an all-to-all mesh. A conversation stays on its node.
- When a node is overloaded, the next turn goes to the least-loaded node, and
  the conversation stays there from then on. That turn pays one cold prefill.
- New conversations go to the least-loaded node, so a node rises above average
  only as its own conversations grow. In simulation with a 2× threshold,
  conversations almost never move at 32 per node, and moves add about 0.1% to
  prefill at 8 per node. A conversation moves only if the destination stays
  under the threshold, so a huge one never bounces between nodes.

## Setup

Store (one per node):

```bash
MC_STORE_CUMEM_SOCKET_DIR=/dev/shm/mooncake-cumem  # shared with engine containers
MC_TENT_PLATFORM=cpu                               # store needs no GPU
MC_TENT_MACHINE_ID=boot_id
```

Engines (`MC_TENT_CONF`):

```json
{
  "transports": {
    "shm": {
      "enable": true,
      "device_copy": true,
      "cumem_socket_dir": "/dev/shm/mooncake-cumem"
    }
  }
}
```

plus `MC_TENT_MACHINE_ID=boot_id` and, for batched lookups,
`MC_STORE_BATCHED_GET=1`.

The socket directory must be mounted into the store container and every engine
container on the node. Without `MC_TENT_MACHINE_ID=boot_id`, containers from
different images look like different machines, and SHM is never selected.

## Reference

Store side:

| Variable | Effect |
|---|---|
| `MC_STORE_CUMEM_SOCKET_DIR=<dir>` | Store segments are CUDA VMM host memory (`cuMemCreate` on every NUMA node, no CUDA context), served as fds over a unix socket in `<dir>`. Engines' GPUs copy it without registering. What we run in production. |
| `MC_STORE_CUMEM_NUMA_NODES=0,1` | NUMA nodes for the segment (default: all online nodes). |
| `MC_STORE_CUMEM_PIECE_BYTES` | Allocation granularity (default 2 GiB). |
| `MC_STORE_SHM_SEGMENT_DIR=<dir>` | Alternative to cumem: segments and the SSD restore buffer are files on a hugetlbfs mount that engines map. |
| `MC_STORE_SHM_SEGMENT_WAIT_S` | After a store restart, seconds to wait for engines to release the previous store's hugepages (default 300). |
| `MC_TENT_PLATFORM=cpu` | Keep the store off the CUDA runtime. |

Engine side:

| Setting | Effect |
|---|---|
| `transports.shm.enable`, `transports.shm.device_copy` | Enable the SHM transport for GPU memory. |
| `transports.shm.cumem_socket_dir` | Same directory as the store's `MC_STORE_CUMEM_SOCKET_DIR`. |
| `transports.shm.register_peer_memory`, `register_max_bytes`, `register_ahead_chunks` | Only for hugetlbfs segments on hosts without HMM: register peer memory with CUDA (LRU-capped). |
| `MC_STORE_BATCHED_GET=1` | `BatchGet` sends all memory replicas of a segment as one transfer. |
| `MC_TENT_MACHINE_ID=boot_id` | Key host locality on the kernel `boot_id` instead of `/etc/machine-id`. |

## Maintenance

```
upstream/main ──merge──▶ di-main ──merge──▶ di-<model>
```

- `main` mirrors upstream and is never committed to.
- `di-main` is upstream `main` plus our changes. Its history is never
  rewritten (no rebase, no force-push), so branches built on it stay valid.
  Upstream comes in by merge:

  ```bash
  git remote add upstream https://github.com/kvcache-ai/Mooncake.git  # once
  git fetch upstream
  git switch di-main
  git merge upstream/main
  git push origin di-main
  ```

  `git diff upstream/main...di-main` shows everything we carry.
- Releases of `di-main` are tagged (`di-YYYY.MM.N`). Builds and model branches
  refer to a tag, not to the moving branch.
- Model branches (`di-<model>`) start from a `di-main` tag and take a newer one
  by merging it: `git merge di-YYYY.MM.N`.
- To send a change upstream, branch from `upstream/main` and cherry-pick or
  squash the change there; `di-main` is not rewritten for it.

- Fork code goes in new files (for example
  `mooncake-store/src/cumem_store_segment.cpp`), with only small call sites in
  files upstream changes often (`real_client.cpp`, `client_service.cpp`,
  `transfer_engine_impl.cpp`).
- Don't edit upstream's `README.md` or `docs/`. DeepInfra documentation lives
  in this file.
- Anything useful beyond DeepInfra should go upstream
  ([CONTRIBUTING.md](../CONTRIBUTING.md)). Code that lands there is code we no
  longer carry.
