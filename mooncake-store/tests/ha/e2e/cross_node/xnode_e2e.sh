#!/usr/bin/env bash
# Cross-node takeover scenarios: master pods a (.11) and b (.12) each with
# their own data dir ("node"), Lease leadership on a throwaway k3s reached
# over a separate network, proxy at .10 routing to the pod whose /ready is
# 200, three verified-read clients, a TE late writer into the old leader's
# open writes, and the allocation probe.
#
#   ./xnode_e2e.sh OUTDIR SCENARIO
#   handoff         leader SIGTERM
#   crash           leader process SIGKILL; it restarts on its node
#   nodeloss        leader's node gone (killed, off both networks)
#   solo_nodeloss   follower paused past the sync timeout, leader goes on
#                   alone, then the leader's node is gone: the follower must
#                   not keep its index
#   resync_nodeloss as solo_nodeloss, but the follower catches up first
#   zombie          leader paused past its Lease, then resumed
#   apipartition    leader cut off from the API server only
#   apiboth         both pods cut off from the API server for 20 s
#   snapshot_rebuild snapshots every 15 s and the OpLog pruned; the follower
#                   is rebuilt on an empty disk from the leader alone, then the
#                   leader's node is gone
set -uo pipefail
H=${MC_HARNESS_DIR:-$(cd "$(dirname "$0")" && pwd)}
S=${MC_TAKEOVER_DIR:-$(cd "$(dirname "$0")/../takeover" && pwd)}
T=${MC_WORK:?set MC_WORK to the dir holding the mooncake checkouts and builds}
cd $H
OUT=${1:?outdir}; SCENARIO=${2:?scenario}
OLD=localhost:30500/mooncake:v0.3.14-rc1-di-0d1a8040fa
PROXY=172.31.250.10; A=172.31.250.11; B=172.31.250.12
PROBE="LD_LIBRARY_PATH=/tmp/cudastub:$T/mc-hot/build-hot/mooncake-common:$T/mc-hot/build-hot/mooncake-common/etcd $T/mc-hot/build-hot/mooncake-store/benchmarks/late_write_probe --master_server=$PROXY:50051"
rm -rf "$OUT"; mkdir -p "$OUT"; OUT=$(realpath "$OUT")
ev() { echo "$(date +%s.%N) $*" | tee -a "$OUT/events.log"; }
ready() { curl -s -m 1 -o /dev/null -w '%{http_code}' "http://$1:9003/ready"; }
leader_ip() { for ip in $A $B; do [ "$(ready $ip)" = 200 ] && { echo $ip; return; }; done; }
name_of() { [ "$1" = $A ] && echo a || echo b; }
other_of() { [ "$1" = $A ] && echo $B || echo $A; }
wait_ready() { local ip=$1 t=0; until [ "$(ready "$ip")" = 200 ] || [ $t -ge ${2:-1800} ]; do sleep 0.1; t=$((t+1)); done; }
wait_in_sync() { local t=0; until ./up.sh lease | grep -q "in_sync=1" || [ $t -ge 600 ]; do sleep 0.1; t=$((t+1)); done; }

cleanup() {
  docker rm -f mcsec-proxy mcsec-client-a mcsec-client-b mcsec-client-c mcsec-late >/dev/null 2>&1
  docker rm -f $(docker ps -aq --filter name=mcx-master- --filter name=mcx-pod-) >/dev/null 2>&1
  docker rm -f $(docker ps -aq --filter name=mcsec-master --filter name=mcsec-pod --filter name=mcsq-) >/dev/null 2>&1
}
[ "$SCENARIO" = snapshot_rebuild ] && export SNAP_INTERVAL=15
cleanup
docker network connect mcsq mc-build-flat >/dev/null 2>&1 || true
./up.sh wipe; ./up.sh pods
./up.sh master a; sleep 2; ./up.sh master b
docker run -d --name mcsec-proxy --network mcsq --ip $PROXY -v "$S/proxy.py":/t/proxy.py:ro \
  --entrypoint python3 $OLD -u /t/proxy.py $A $B >/dev/null
t=0; until [ -n "$(leader_ip)" ] || [ $t -ge 600 ]; do sleep 0.5; t=$((t+1)); done
L=$(leader_ip); F=$(other_of "$L"); ev "leader=$L follower=$F"
for n in a b c; do
  docker run -d --name "mcsec-client-$n" --network mcsq --memory 8g --cpus 3 -e MC_RPC_CONNECT_TIMEOUT_MS=1000 \
    -v "$S/client2.py":/t/client2.py:ro --entrypoint python3 $OLD -I /t/client2.py \
    --master $PROXY:50051 --prefix "$n" --keys 20000 --segment-gb 1.0 --write-frac 0.3 >/dev/null
done
for n in a b c; do until docker logs "mcsec-client-$n" 2>&1 | grep -q '"ev": "fill"'; do sleep 1; done; done
ev "FILLED"
wait_in_sync; ev "IN_SYNC $(./up.sh lease)"
sleep 15

curl -s -m 3 "http://$L:9003/metrics" > "$OUT/leader_metrics.txt"
docker exec mc-build-flat bash -lc "$PROBE --mode=start --count=256" > "$OUT/open_ranges.txt" 2> "$OUT/probe_start.err"
ev "OPEN_WRITES $(wc -l < "$OUT/open_ranges.txt") ranges: $(cat "$OUT/probe_start.err")"

node_gone() {  # the node of pod $1 is gone: process killed, pod off the networks
  docker update --restart=no "mcx-master-$1" >/dev/null
  docker kill "mcx-master-$1" >/dev/null
  docker network disconnect -f mcsq "mcx-pod-$1" >/dev/null 2>&1
  docker network disconnect -f mcx-api "mcx-pod-$1" >/dev/null 2>&1
}
takeover_done() {
  local t0=$1
  wait_ready "$F" 3000
  ev "NEW_LEADER_READY $F after_s=$(echo "$(date +%s.%N) - $t0" | bc) lease=$(./up.sh lease)"
}
late_write() {
  docker rm -f mcsec-late >/dev/null 2>&1
  docker run -d --name mcsec-late --network mcsq -v "$S/late_writer.py":/t/late_writer.py:ro \
    -v "$OUT":/out --entrypoint python3 $OLD -I /t/late_writer.py --ranges /out/open_ranges.txt --seconds 90 >/dev/null
  ev "LATE_WRITER started"
}
check() {
  docker exec mc-build-flat bash -lc "$PROBE --mode=check --count=3000 --ranges=$OUT/open_ranges.txt" \
    > "$OUT/check_$1.txt" 2>/dev/null
  ev "CHECK $1 $(tail -1 "$OUT/check_$1.txt")"
}
pause_follower() {
  ev "PAUSE_FOLLOWER $(name_of "$F")"
  docker pause "mcx-master-$(name_of "$F")" >/dev/null
  sleep "$1"
  ev "OUT_OF_SYNC? $(./up.sh lease)"
}

ln=$(name_of "$L"); fn=$(name_of "$F")
case $SCENARIO in
  handoff)
    ev "TAKEOVER term leader=$L"; t0=$(date +%s.%N)
    docker stop -t 30 "mcx-master-$ln" >/dev/null &
    takeover_done "$t0"
    wait
    docker start "mcx-master-$ln" >/dev/null   # the pod comes back on its node
    ;;
  crash)
    ev "TAKEOVER kill leader=$L"; t0=$(date +%s.%N)
    docker kill "mcx-master-$ln" >/dev/null
    takeover_done "$t0"
    ;;
  nodeloss)
    ev "TAKEOVER nodeloss leader=$L"; t0=$(date +%s.%N)
    node_gone "$ln"
    takeover_done "$t0"
    ;;
  solo_nodeloss)
    pause_follower 8
    sleep 10
    ev "TAKEOVER nodeloss leader=$L (follower still paused)"; t0=$(date +%s.%N)
    node_gone "$ln"
    docker unpause "mcx-master-$fn" >/dev/null
    takeover_done "$t0"
    ;;
  resync_nodeloss)
    pause_follower 8
    docker unpause "mcx-master-$fn" >/dev/null
    wait_in_sync; ev "RESYNCED $(./up.sh lease)"
    sleep 5
    ev "TAKEOVER nodeloss leader=$L"; t0=$(date +%s.%N)
    node_gone "$ln"
    takeover_done "$t0"
    ;;
  zombie)
    ev "TAKEOVER pause leader=$L"; t0=$(date +%s.%N)
    docker pause "mcx-master-$ln" >/dev/null
    takeover_done "$t0"
    sleep 5
    ev "RESUME_ZOMBIE $ln"
    docker unpause "mcx-master-$ln" >/dev/null
    for i in $(seq 1 20); do ev "zombie_ready=$(ready "$L") new_ready=$(ready "$F")"; sleep 0.25; done
    ;;
  apipartition)
    ev "TAKEOVER apipartition leader=$L"; t0=$(date +%s.%N)
    docker network disconnect -f mcx-api "mcx-pod-$ln" >/dev/null
    for i in $(seq 1 40); do ev "old_ready=$(ready "$L") new_ready=$(ready "$F")"; sleep 0.25; [ "$(ready "$F")" = 200 ] && break; done
    takeover_done "$t0"
    docker network connect --ip 172.31.251.$([ $ln = a ] && echo 11 || echo 12) mcx-api "mcx-pod-$ln"
    ;;
  apiboth)
    ev "TAKEOVER apiboth leader=$L"; t0=$(date +%s.%N)
    for n in a b; do docker network disconnect -f mcx-api "mcx-pod-$n" >/dev/null; done
    for i in $(seq 1 80); do ev "a_ready=$(ready $A) b_ready=$(ready $B)"; sleep 0.25; done
    for n in a b; do docker network connect --ip 172.31.251.$([ $n = a ] && echo 11 || echo 12) mcx-api "mcx-pod-$n"; done
    ev "API_BACK"; t1=$(date +%s.%N)
    t=0; until [ -n "$(leader_ip)" ] || [ $t -ge 1200 ]; do sleep 0.1; t=$((t+1)); done
    F=$(leader_ip); ev "NEW_LEADER_READY $F after_api_back_s=$(echo "$(date +%s.%N) - $t1" | bc) down_s=$(echo "$(date +%s.%N) - $t0" | bc) lease=$(./up.sh lease)"
    ;;
  snapshot_rebuild)
    sleep 40
    python3 "$H/kv_summary.py" "$H/data-$ln/ha.db" > "$OUT/leader_kv_before.txt"
    ev "LEADER_KV $(tr '\n' ' ' < "$OUT/leader_kv_before.txt")"
    ev "REBUILD_FOLLOWER $fn on an empty disk"
    docker stop -t 30 "mcx-master-$fn" >/dev/null
    docker run --rm -v $H:/h --entrypoint rm localhost:30500/mooncake:v0.3.14-rc1-di-0d1a8040fa -rf /h/data-$fn
    mkdir -p "$H/data-$fn"; docker start "mcx-master-$fn" >/dev/null
    t=0; until docker logs "mcx-master-$fn" 2>&1 | grep -q "Batch snapshot bootstrap complete: snapshot_seq=[1-9]" || [ $t -ge 600 ]; do sleep 0.5; t=$((t+1)); done
    ev "FOLLOWER_BOOTSTRAP $(docker logs "mcx-master-$fn" 2>&1 | grep -E 'replication_full_copy|Batch snapshot bootstrap complete' | tail -2 | cut -c60-200 | tr '\n' ' ')"
    sleep 1; wait_in_sync; ev "RESYNCED $(./up.sh lease)"
    sleep 10
    ev "LEADER_KEYS $(docker logs "mcx-master-$ln" 2>&1 | grep 'role=leader' | tail -1 | grep -o 'Keys: [0-9]*')"
    ev "TAKEOVER nodeloss leader=$L"; t0=$(date +%s.%N)
    node_gone "$ln"
    takeover_done "$t0"
    ev "ACTIVATED $(docker logs "mcx-master-$fn" 2>&1 | grep -o 'action=activate_follower, objects=[0-9]*' | tail -1)"
    ;;
esac
late_write
sleep 25; check t25
sleep 35; check t60
ev "REJOIN $(./up.sh lease)"
sleep 20
ev "END $(./up.sh lease)"

for n in a b c; do docker logs "mcsec-client-$n" > "$OUT/client_$n.log" 2>&1; done
for n in a b; do docker logs "mcx-master-$n" > "$OUT/master_$n.log" 2>&1; done
docker logs mcsec-proxy > "$OUT/proxy.log" 2>&1
docker logs mcsec-late > "$OUT/late.log" 2>&1 || true
cleanup
python3 - "$OUT" <<'EOF'
import collections, json, sys
out = sys.argv[1]
take = None
for line in open(f"{out}/events.log"):
    if " TAKEOVER " in line:
        take = float(line.split()[0])
tot = {"before": collections.Counter(), "after": collections.Counter()}
wrong = []
for n in "abc":
    for line in open(f"{out}/client_{n}.log"):
        line = line.strip()
        if not line.startswith("{"):
            continue
        e = json.loads(line)
        if e.get("ev") == "wrong":
            wrong.append(e)
        if e.get("ev") != "loop":
            continue
        side = "after" if take and e["t"] > take else "before"
        for k, v in e.items():
            if k not in ("ev", "t"):
                tot[side][k] += v
print("before takeover:", dict(tot["before"]))
print("after takeover: ", dict(tot["after"]))
print("WRONG values:", len(wrong), [w["first"] for w in wrong[:5]])
EOF
grep -hE "replication_(promotion|primary|wipe|restart|ack_overdue|full_copy)|write_replication_record|handed_off|arm_promotion|activate_follower|write_handoff|Leadership lost|stopped leading" \
  "$OUT"/master_*.log | cut -c1-210 | head -40
tail -2 "$OUT/late.log" 2>/dev/null
cat "$OUT/events.log"
