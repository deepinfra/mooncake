#!/usr/bin/env bash
# Takeover security scenarios on a two-pod layout closer to Kubernetes:
# master pods A (.11) and B (.12), each in its own network namespace, share
# one data dir (one local-path claim on one node); a proxy at .10:50051
# routes new connections to the pod whose /ready answered 200 (a Service).
# Three verified-read clients write and re-read through the proxy.
#
#   ./sec_e2e.sh IMAGE OUTDIR SCENARIO
#   SCENARIO: crash | handoff | zombie | lagging | stale | discard | remount
#     crash    leader SIGKILL; a late write lands in its open writes' ranges
#     handoff  leader SIGTERM; same late write
#     zombie   leader paused past the takeover, then resumed
#     lagging  follower paused under load, then the leader is stopped
#     stale    hand-off, then the new leader crashes: the record is stale
#     discard  the open writes expire and are restarted before the hand-off:
#              their first ranges are held for release, not in flight
#     remount  a client remounts twice inside the quarantine window
set -uo pipefail
cd ${MC_TAKEOVER_DIR:-$(cd "$(dirname "$0")/../takeover" && pwd)}
IMG=${1:?image}; OUT=${2:?outdir}; SCENARIO=${3:?scenario}
OLD=localhost:30500/mooncake:v0.3.14-rc1-di-0d1a8040fa
NET=mcsq; PROXY=172.31.250.10; A=172.31.250.11; B=172.31.250.12
DATA=${MC_TAKEOVER_DIR:-$(cd "$(dirname "$0")/../takeover" && pwd)}/data
T=${MC_WORK:?set MC_WORK to the dir holding the mooncake checkouts and builds}
PROBE="LD_LIBRARY_PATH=/tmp/cudastub:$T/mc-hot/build-hot/mooncake-common:$T/mc-hot/build-hot/mooncake-common/etcd $T/mc-hot/build-hot/mooncake-store/benchmarks/late_write_probe --master_server=$PROXY:50051"
rm -rf "$OUT"; mkdir -p "$OUT"; OUT=$(realpath "$OUT")
ev() { echo "$(date +%s.%N) $*" | tee -a "$OUT/events.log"; }
ready() { curl -s -m 1 -o /dev/null -w '%{http_code}' "http://$1:9003/ready"; }
leader_ip() { for ip in $A $B; do [ "$(ready $ip)" = 200 ] && { echo $ip; return; }; done; }
name_of() { [ "$1" = $A ] && echo mcsec-master-a || echo mcsec-master-b; }
other_of() { [ "$1" = $A ] && echo $B || echo $A; }
wait_ready() { local ip=$1 t=0; until [ "$(ready "$ip")" = 200 ] || [ $t -ge 1200 ]; do sleep 0.1; t=$((t+1)); done; }
status() { curl -s -m 2 "http://$1:9003/standby_status"; }

cleanup() {
  docker rm -f $(docker ps -aq --filter name=mcsq-) >/dev/null 2>&1
  docker rm -f mcsec-proxy mcsec-master-a mcsec-master-b mcsec-pod-a mcsec-pod-b \
    mcsec-client-a mcsec-client-b mcsec-client-c mcsec-late >/dev/null 2>&1
}
master() {  # name pod_ip
  docker rm -f "mcsec-master-$1" >/dev/null 2>&1
  docker run -d --name "mcsec-master-$1" --network "container:mcsec-pod-$1" --memory 16g --cpus 8 \
    -v "$DATA":/data -e MOONCAKE_SNAPSHOT_LOCAL_PATH=/data/snapshots --entrypoint mooncake_master "$IMG" \
    --rpc_port=50051 --metrics_port=9003 --rpc_address="$2" \
    --enable_ha=true --ha_backend_type=sqlite --ha_backend_connstring=/data/ha.db --cluster_id=mcsec \
    --enable_oplog=true --enable_oplog_snapshot=true --snapshot_object_store_type=local \
    --snapshot_interval_seconds=60 --oplog_poll_interval_ms=50 --standby_serving_index=true \
    --eviction_high_watermark_ratio=0.95 --eviction_ratio=0.10 --enable_offload=false ${MASTER_EXTRA:-} --logtostderr >/dev/null
}

[ "$SCENARIO" = discard ] && MASTER_EXTRA="--put_start_discard_timeout_sec=5"
cleanup
docker network inspect $NET >/dev/null 2>&1 || docker network create --subnet 172.31.250.0/24 $NET >/dev/null
docker network connect $NET mc-build-flat >/dev/null 2>&1 || true
docker run --rm -v "$(dirname "$DATA")":/r --entrypoint rm $OLD -rf /r/data
mkdir -p "$DATA"
docker run -d --name mcsec-pod-a --network $NET --ip $A --entrypoint sleep $OLD infinity >/dev/null
docker run -d --name mcsec-pod-b --network $NET --ip $B --entrypoint sleep $OLD infinity >/dev/null
master a $A; sleep 2; master b $B
docker run -d --name mcsec-proxy --network $NET --ip $PROXY -v "$PWD/proxy.py":/t/proxy.py:ro \
  --entrypoint python3 $OLD -u /t/proxy.py $A $B >/dev/null || { echo "proxy did not start" >&2; exit 1; }
sleep 1
[ "$(docker inspect -f '{{.State.Running}}' mcsec-proxy)" = true ] || { echo "proxy not running" >&2; exit 1; }
t=0; until [ -n "$(leader_ip)" ] || [ $t -ge 600 ]; do sleep 0.5; t=$((t+1)); done
L=$(leader_ip); F=$(other_of "$L"); ev "leader=$L follower=$F"
for n in a b c; do
  docker run -d --name "mcsec-client-$n" --network $NET --memory 8g --cpus 3 -e MC_RPC_CONNECT_TIMEOUT_MS=1000 \
    -v "$PWD/client2.py":/t/client2.py:ro --entrypoint python3 $OLD -I /t/client2.py \
    --master $PROXY:50051 --prefix "$n" --keys 20000 --segment-gb 1.0 --write-frac 0.3 >/dev/null
done
for n in a b c; do until docker logs "mcsec-client-$n" 2>&1 | grep -q '"ev": "fill"'; do sleep 1; done; done
ev "FILLED"
sleep 20
ev "standby_status $(status "$F")"

# Writes the old leader handed out and that never finish.
curl -s -m 3 "http://$L:9003/metrics" > "$OUT/leader_metrics.txt"
docker exec mc-build-flat bash -lc "$PROBE --mode=start --count=256" > "$OUT/open_ranges.txt" 2> "$OUT/probe_start.err"
ev "OPEN_WRITES $(wc -l < "$OUT/open_ranges.txt") ranges: $(cat "$OUT/probe_start.err")"
if [ "$SCENARIO" = discard ]; then
  sleep 7
  # Same keys past the discard timeout: the first ranges go to delayed release.
  docker exec mc-build-flat bash -lc "$PROBE --mode=start --count=256" > "$OUT/restarted_ranges.txt" 2> "$OUT/probe_restart.err"
  ev "RESTARTED_WRITES $(wc -l < "$OUT/restarted_ranges.txt") ranges: $(cat "$OUT/probe_restart.err")"
fi
if [ "$SCENARIO" = remount ]; then
  docker exec -d mc-build-flat bash -lc "$PROBE --mode=remount --key_prefix=remount-probe > $OUT/remount.txt 2> $OUT/remount.err"
  t=0; until grep -q READY "$OUT/remount.txt" 2>/dev/null || [ $t -ge 60 ]; do sleep 1; t=$((t+1)); done
  ev "REMOUNT_PROBE $(head -2 "$OUT/remount.txt" | tr '\n' ' ')"
fi

takeover() {  # how: kill | term | pause
  ev "TAKEOVER $1 leader=$L"
  case $1 in
    kill) docker kill "$(name_of "$L")" >/dev/null ;;
    term) docker stop -t 30 "$(name_of "$L")" >/dev/null & ;;
    pause) docker pause "$(name_of "$L")" >/dev/null ;;
  esac
  local t0; t0=$(date +%s.%N)
  wait_ready "$F"
  ev "NEW_LEADER_READY $F after_s=$(echo "$(date +%s.%N) - $t0" | bc)"
}
late_write() {
  docker rm -f mcsec-late >/dev/null 2>&1
  docker run -d --name mcsec-late --network $NET -v "$PWD/late_writer.py":/t/late_writer.py:ro \
    -v "$OUT":/out --entrypoint python3 $OLD -I /t/late_writer.py --ranges /out/open_ranges.txt --seconds 90 >/dev/null
  ev "LATE_WRITER started"
}
check() {
  docker exec mc-build-flat bash -lc "$PROBE --mode=check --count=3000 --ranges=$OUT/open_ranges.txt" \
    > "$OUT/check_$1.txt" 2>/dev/null
  ev "CHECK $1 $(tail -1 "$OUT/check_$1.txt")"
}

case $SCENARIO in
  crash|handoff|discard)
    [ "$SCENARIO" = crash ] && takeover kill || takeover term
    late_write
    sleep 25; check t25
    sleep 35; check t60
    sleep 40
    ;;
  zombie)
    takeover pause
    sleep 20
    ev "RESUME_ZOMBIE $(name_of "$L")"
    docker unpause "$(name_of "$L")" >/dev/null
    for i in $(seq 1 20); do ev "zombie_ready=$(ready "$L") follower_ready=$(ready "$F")"; sleep 0.25; done
    sleep 60
    ;;
  lagging)
    ev "PAUSE_FOLLOWER"
    docker pause "$(name_of "$F")" >/dev/null; sleep 20; docker unpause "$(name_of "$F")" >/dev/null
    ev "RESUMED_FOLLOWER status=$(status "$F")"
    takeover term
    sleep 60
    ;;
  remount)
    takeover term
    t=0; until grep -q "^allocated=" "$OUT/remount.txt" 2>/dev/null || [ $t -ge 300 ]; do sleep 1; t=$((t+1)); done
    ev "REMOUNT_CHECK $(tr '\n' ' ' < "$OUT/remount.txt")"
    sleep 30
    ;;
  stale)
    takeover term
    sleep 15
    docker start "$(name_of "$L")" >/dev/null
    t=0; until status "$L" | grep -q '"follower_ready":true' || [ $t -ge 300 ]; do sleep 1; t=$((t+1)); done
    ev "OLD_LEADER_BACK_AS_FOLLOWER after_s=$t"
    sleep 10
    L2=$F; F=$L; L=$L2
    takeover kill
    sleep 60
    ;;
esac

ev "END"
for n in a b c; do docker logs "mcsec-client-$n" > "$OUT/client_$n.log" 2>&1; done
docker logs mcsec-master-a > "$OUT/master_a.log" 2>&1
docker logs mcsec-master-b > "$OUT/master_b.log" 2>&1
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
grep -hE "handed_off|arm_promotion|activate_follower|handoff_drain|write_handoff|terminal|Trying to stop|Leadership lost|release_promotion" \
  "$OUT"/master_*.log | cut -c1-200 | head -20
cat "$OUT/proxy.log" | tail -8
cat "$OUT/late.log" 2>/dev/null | tail -2
cat "$OUT/events.log"
