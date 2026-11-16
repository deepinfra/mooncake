# Master takeover harnesses

End-to-end checks that a master takeover never serves wrong bytes. Three
verified-read clients (`takeover/client2.py`) write and re-read keys through
a proxy that routes new connections to the master whose `/ready` is 200
(a Kubernetes Service stand-in). A late writer (`takeover/late_writer.py`)
writes a known pattern into the ranges of writes the old leader left open,
and `late_write_probe` (benchmarks) counts new allocations that overlap them.
Every run prints the clients' hit/miss/wrong totals; any `wrong` is a failure.

- `takeover/sec_e2e.sh IMAGE OUT SCENARIO`: same-node durable master (two
  pods sharing one SQLite file). Scenarios: crash, handoff, zombie, lagging,
  stale, discard, remount.
- `cross_node/xnode_e2e.sh OUT SCENARIO`: two pods with separate data dirs,
  Lease leadership on a throwaway k3s. Scenarios: handoff, crash, nodeloss,
  resync_nodeloss, solo_nodeloss, zombie, apipartition, apiboth,
  snapshot_rebuild. `XNODE_IMAGE=<image>` runs the packaged master instead
  of the build tree.

Setup (once):

    export MC_WORK=<dir with the checkouts>      # build-hot, mc-xnode/build-xnode
    docker network create --subnet 172.31.250.0/24 mcsq
    docker network create --subnet 172.31.251.0/24 mcx-api
    docker run -d --name mcx-k3s --network mcx-api --ip 172.31.251.30 --privileged \
      --tmpfs /run --tmpfs /var/run rancher/k3s:v1.31.4-k3s1 server --disable-agent \
      --disable=traefik,servicelb,metrics-server,local-storage,coredns \
      --tls-san 172.31.251.30 --write-kubeconfig-mode 644
    docker exec mcx-k3s cat /etc/rancher/k3s/k3s.yaml \
      | sed 's#https://127.0.0.1:6443#https://172.31.251.30:6443#' > cross_node/kubeconfig
    docker exec mcx-k3s kubectl create namespace mcx
    openssl rand -hex 16 > cross_node/token

The probe runs inside a build container named `mc-build-flat` attached to
`mcsq`; clients and the late writer use an existing mooncake image (OLD in
the scripts).
