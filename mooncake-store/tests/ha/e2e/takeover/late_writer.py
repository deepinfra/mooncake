"""The late write: writes a garbage pattern, over the transfer engine, into
the ranges a leader had handed out to writes still open when it went, again
and again for a while, as a stalled transfer from the old term would."""
import argparse
import socket
import sys
import time

from mooncake.engine import TransferEngine

PATTERN = bytes([0xDE, 0xAD, 0xBE, 0xEF]) * 4096


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument('--ranges', required=True)
    p.add_argument('--seconds', type=int, default=90)
    p.add_argument('--interval', type=float, default=0.5)
    a = p.parse_args()
    ranges = []
    for line in open(a.ranges):
        parts = line.split()
        if len(parts) == 3:
            ranges.append((parts[0], int(parts[1]), int(parts[2])))
    size = max(r[2] for r in ranges)
    te = TransferEngine()
    host = socket.gethostbyname(socket.gethostname())
    if te.initialize(host, 'P2PHANDSHAKE', 'tcp', '') != 0:
        print('te init failed', flush=True)
        return 1
    buf = te.allocate_managed_buffer(size)
    te.write_bytes_to_buffer(buf, (PATTERN * (size // len(PATTERN) + 1))[:size], size)
    end = time.time() + a.seconds
    rounds = writes = failures = 0
    while time.time() < end:
        for endpoint, address, length in ranges:
            rc = te.transfer_sync_write(endpoint, buf, address, length)
            writes += 1
            failures += rc != 0
        rounds += 1
        time.sleep(a.interval)
    print(f'late_writer rounds={rounds} writes={writes} failed={failures}', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
