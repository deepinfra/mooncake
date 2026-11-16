"""Stands in for a Kubernetes Service in front of the two master pods: new
connections to :50051 go to a pod whose GET /ready answered 200 at the last
probe (every second, one failure removes it), as kube-proxy routes to ready
endpoints. Established connections are left alone."""
import asyncio
import sys
import time
import urllib.request

PODS = sys.argv[1:]
ready = []


def probe(pod: str) -> bool:
    try:
        with urllib.request.urlopen(f'http://{pod}:9003/ready', timeout=1) as resp:
            return resp.status == 200
    except Exception:  # noqa: BLE001
        return False


async def prober() -> None:
    global ready
    loop = asyncio.get_running_loop()
    while True:
        states = await asyncio.gather(*(loop.run_in_executor(None, probe, p) for p in PODS))
        now = [p for p, ok in zip(PODS, states) if ok]
        if now != ready:
            print(f'{time.time():.3f} endpoints={now}', flush=True)
        ready = now
        await asyncio.sleep(1)


async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except Exception:  # noqa: BLE001
        pass
    finally:
        writer.close()


async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    if not ready:
        writer.close()
        return
    target = ready[0]
    try:
        up_reader, up_writer = await asyncio.open_connection(target, 50051)
    except Exception:  # noqa: BLE001
        writer.close()
        return
    await asyncio.gather(pipe(reader, up_writer), pipe(up_reader, writer))


async def main() -> None:
    asyncio.create_task(prober())
    server = await asyncio.start_server(handle, '0.0.0.0', 50051)
    async with server:
        await server.serve_forever()


asyncio.run(main())
