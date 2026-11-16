#!/bin/bash
# Cross-node harness: two "nodes" (pod netns a=.11, b=.12) with separate data
# dirs, Lease leadership on a throwaway k3s (mcx-api network), proxy at .10.
#   up.sh wipe | pods | master a|b | lease
set -uo pipefail
T=${MC_WORK:?set MC_WORK to the dir holding the mooncake checkouts and builds}
H=${MC_HARNESS_DIR:-$(cd "$(dirname "$0")" && pwd)}
IMG=mc-build-mert-img:latest
OLD=localhost:30500/mooncake:v0.3.14-rc1-di-0d1a8040fa
BIN=${XNODE_BIN:-$T/mc-xnode/build-xnode/mooncake-store/src/mooncake_master}
LIBS=/opt/conda/lib:$T/mc-xnode/build-xnode/mooncake-common:$T/mc-xnode/build-xnode/mooncake-common/k8s-lease
ip_of() { [ "$1" = a ] && echo 11 || echo 12; }
case $1 in
  wipe)
    docker rm -f $(docker ps -aq --filter name=mcx-master- --filter name=mcx-pod-) >/dev/null 2>&1
    docker run --rm -v $H:/h --entrypoint rm $OLD -rf /h/data-a /h/data-b
    mkdir -p $H/data-a $H/data-b
    docker exec mcx-k3s kubectl -n mcx delete lease mooncake-master --ignore-not-found >/dev/null
    ;;
  pods)
    for n in a b; do
      docker run -d --name mcx-pod-$n --network mcsq --ip 172.31.250.$(ip_of $n) --entrypoint sleep $OLD infinity >/dev/null
      docker network connect --ip 172.31.251.$(ip_of $n) mcx-api mcx-pod-$n
    done
    ;;
  master)
    n=$2; ip=172.31.250.$(ip_of $n)
    docker rm -f mcx-master-$n >/dev/null 2>&1
    if [ -n "${XNODE_IMAGE:-}" ]; then  # the packaged image, as a pod runs it
      RUN="-v $H:$H --entrypoint mooncake_master $XNODE_IMAGE"
    else
      RUN="-v $T:$T -e LD_LIBRARY_PATH=$LIBS --entrypoint $BIN $IMG"
    fi
    docker run -d --name mcx-master-$n --network container:mcx-pod-$n --restart on-failure \
      --memory 16g --cpus 8 -v $H/data-$n:/data -e KUBECONFIG=$H/kubeconfig \
      -e MOONCAKE_SNAPSHOT_LOCAL_PATH=/data/snapshots $RUN \
      --rpc_port=50051 --metrics_port=9003 --rpc_address=$ip \
      --enable_ha=true --ha_backend_type=sqlite --ha_backend_connstring=/data/ha.db --cluster_id=mcx \
      --ha_leader_backend_type=k8s --ha_leader_backend_connstring=mcx/mooncake-master \
      --ha_replication_port=9005 --ha_replication_token_file=$H/token --ha_replication_sync_timeout_ms=1000 \
      --enable_oplog=true --enable_oplog_snapshot=true --snapshot_object_store_type=ha_kv \
      --snapshot_interval_seconds=${SNAP_INTERVAL:-60} --oplog_poll_interval_ms=50 --standby_serving_index=true \
      --eviction_high_watermark_ratio=0.95 --eviction_ratio=0.10 --enable_offload=false ${MASTER_EXTRA:-} --logtostderr >/dev/null
    ;;
  lease)
    docker exec mcx-k3s kubectl -n mcx get lease mooncake-master -o jsonpath='{.spec.holderIdentity} t={.spec.leaseTransitions} {.metadata.annotations.mooncake\.kvcache\.ai/replication}' 2>/dev/null; echo
    ;;
esac
