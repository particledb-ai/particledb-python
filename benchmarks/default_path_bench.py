"""The Python SDK's DEFAULT native-wire path, measured.

THREADS application threads share ONE ``PdbWireClient(host, port)`` built
with every other argument at its default, each a closed loop of one
``query("SELECT KV_SET('s:K','v')")`` (``sqlset``) or
``query("SELECT KV_GET('s:K')")`` (``sqlget``) -- the statements kvbench's
``sqlset`` / ``sqlget`` send. Prints one kvbench-format line.

    python3 default_path_bench.py PORT THREADS SECONDS [sqlset|sqlget]
"""
import os
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from particledb.pdb_wire import PdbWireClient  # noqa: E402

KEYS = 100_000


def main():
    port, threads, secs = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
    wl = sys.argv[4] if len(sys.argv) > 4 else "sqlset"
    client = PdbWireClient("127.0.0.1", port)
    val = "x" * 64
    if wl == "sqlget":
        for k in range(0, KEYS, 100):
            client.batch([f"SELECT KV_SET('s:{j}','{val}')" for j in range(k, k + 100)])
    lats = [[] for _ in range(threads)]
    go = threading.Barrier(threads + 1)

    def run(i):
        x = 0x9E3779B97F4A7C15 ^ (i + 1)
        go.wait()
        end = time.monotonic() + secs
        out = lats[i]
        while True:
            t0 = time.monotonic()
            if t0 >= end:
                break
            x ^= (x << 13) & 0xFFFFFFFFFFFFFFFF
            x ^= x >> 7
            x ^= (x << 17) & 0xFFFFFFFFFFFFFFFF
            k = x % KEYS
            if wl == "sqlget":
                client.query(f"SELECT KV_GET('s:{k}')")
            else:
                client.query(f"SELECT KV_SET('s:{k}','{val}')")
            out.append(time.monotonic() - t0)

    ts = [threading.Thread(target=run, args=(i,)) for i in range(threads)]
    for t in ts:
        t.start()
    def srvstat():
        pid, kb = os.environ.get("KVBENCH_SERVER_PID"), os.environ.get("KVBENCH")
        if not pid or not kb:
            return None
        return subprocess.run([kb, "srvstat", pid], capture_output=True, text=True).stdout.strip()

    s0 = srvstat()
    go.wait()
    w0 = time.monotonic()
    for t in ts:
        t.join()
    wall = time.monotonic() - w0
    s1 = srvstat()
    allv = sorted(v for l in lats for v in l)
    n = len(allv)
    p = lambda q: allv[min(n - 1, int(n * q))] * 1000 if n else 0  # noqa: E731
    print(
        f"proto=pysdk clients={threads} wl={wl} pool={client.pool_size} ops={n} "
        f"ops/s={n / secs:.0f} p50_ms={p(0.5):.3f} p99_ms={p(0.99):.3f}"
    )
    if s0 and s1:
        print(f"SRVRAW {s0} | {s1} | {n} {wall:.3f}")


if __name__ == "__main__":
    main()
