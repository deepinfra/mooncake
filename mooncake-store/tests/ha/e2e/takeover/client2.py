"""Verified-read client for the takeover security runs: fills keys with
key-derived content, then loops over reads of both its initial keys and the
keys it wrote most recently, and fresh writes. Reports hit / miss / WRONG
(content that is not the key's) / errors and put results per second."""
import argparse
import collections
import hashlib
import json
import random
import socket
import sys
import time

from mooncake.store import MooncakeDistributedStore


def value_for(key: str, size: int) -> bytes:
    seed = hashlib.sha256(key.encode()).digest()
    return (seed * (size // len(seed) + 1))[:size]


def emit(**kw: object) -> None:
    kw['t'] = round(time.time(), 3)
    print(json.dumps(kw), flush=True)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument('--master', required=True)
    p.add_argument('--prefix', default='k')
    p.add_argument('--keys', type=int, default=20000)
    p.add_argument('--value-size', type=int, default=16384)
    p.add_argument('--segment-gb', type=float, default=1.0)
    p.add_argument('--batch', type=int, default=64)
    p.add_argument('--write-frac', type=float, default=0.3)
    p.add_argument('--recent', type=int, default=20000)
    p.add_argument('--loop-seconds', type=int, default=100000)
    a = p.parse_args()

    store = MooncakeDistributedStore()
    host = socket.gethostbyname(socket.gethostname())
    rc = store.setup(host, 'P2PHANDSHAKE', int(a.segment_gb * 2**30), 256 * 2**20, 'tcp', '', a.master)
    emit(ev='setup', rc=rc, host=host)
    if rc != 0:
        return 1
    keys = [f'{a.prefix}-{i}' for i in range(a.keys)]
    ok = bad = 0
    for i in range(0, len(keys), a.batch):
        chunk = keys[i:i + a.batch]
        res = store.put_batch(chunk, [value_for(k, a.value_size) for k in chunk])
        ok, bad = (ok + len(chunk), bad) if res == 0 else (ok, bad + len(chunk))
    emit(ev='fill', ok=ok, failed=bad)

    recent = collections.deque(maxlen=a.recent)
    fresh = 0
    end = time.time() + a.loop_seconds
    win = collections.Counter()
    win_start = time.time()
    while time.time() < end:
        if random.random() < a.write_frac:
            chunk = [f'{a.prefix}-w{fresh + j}' for j in range(a.batch)]
            fresh += a.batch
            try:
                res = store.put_batch(chunk, [value_for(k, a.value_size) for k in chunk])
            except Exception:  # noqa: BLE001
                res = -1
            if res == 0:
                win['put_ok'] += len(chunk)
                recent.extend(chunk)
            else:
                win['put_err'] += len(chunk)
        else:
            pool = keys if not recent or random.random() < 0.5 else list(recent)
            chunk = random.sample(pool, min(a.batch, len(pool)))
            kind = 'old' if pool is keys else 'new'
            try:
                vals = store.get_batch(chunk)
            except Exception:  # noqa: BLE001
                win['err'] += len(chunk)
                vals = []
            for k, v in zip(chunk, vals):
                if not v:
                    win[f'miss_{kind}'] += 1
                elif bytes(v) == value_for(k, a.value_size):
                    win[f'hit_{kind}'] += 1
                else:
                    win[f'wrong_{kind}'] += 1
                    emit(ev='wrong', key=k, first=bytes(v)[:8].hex())
        if time.time() - win_start >= 1.0:
            emit(ev='loop', **win)
            win = collections.Counter()
            win_start = time.time()
    store.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
