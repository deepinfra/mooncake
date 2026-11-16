#!/bin/bash
cd ${MC_HARNESS_DIR:-$(cd "$(dirname "$0")" && pwd)}
for s in "$@"; do
  timeout 1200 ./xnode_e2e.sh runs/g_$s $s > runs/g_$s.out 2>&1
  echo "== $s rc=$?"
  grep -E "^before|^after|^WRONG|NEW_LEADER_READY|CHECK|REJOIN|old_ready|zombie_ready" runs/g_$s.out | sort -u | cut -c1-200 | head -30
  grep -h "lease_deadline\|Trying to stop server" runs/g_$s/master_*.log 2>/dev/null | cut -c1-160
done
