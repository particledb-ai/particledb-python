"""Protocol-2 multiplexing, point latency and streamed-result memory, measured.

Runs against a live server; prints one JSON line per run. ``--sdk DIR`` picks
which SDK tree is imported (default: this one), so the same script measures a
previous tree (e.g. the protocol-1 SDK) for a before/after on one machine::

    python3 mux_bench.py setup  PORT                       # bench_mux table, 10k rows
    python3 mux_bench.py shared PORT THREADS SECONDS [SLEEP_MS]  # THREADS on ONE connection
    python3 mux_bench.py point  PORT SECONDS               # one thread, prepared point SELECT
    python3 mux_bench.py stream PORT ROWS                  # stream ROWS rows; peak RSS
    python3 mux_bench.py ... --sdk /path/to/old/particledb-python

``shared``: with this SDK, ONE ``particledb.connect()`` shared by every thread
(each statement its own stream); with an SDK that has no ``connect()`` (the
protocol-1 tree), ``PdbWireClient(pool_size=1)`` -- the only way it shares one
socket (statements take turns). ``stream`` with ``PDB_PY_DECODER=pyarrow``
decodes record batches with pyarrow instead of the built-in decoder.
"""
from __future__ import annotations

import json
import os
import resource
import sys
import threading
import time

ROWS = 10_000


def _sdk_path() -> str:
    if "--sdk" in sys.argv:
        i = sys.argv.index("--sdk")
        p = sys.argv[i + 1]
        del sys.argv[i: i + 2]
        return p
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


sys.path.insert(0, _sdk_path())
import particledb  # noqa: E402
from particledb.pdb_wire import PdbWireClient  # noqa: E402

V2 = hasattr(particledb, "connect")
SQL = "SELECT v FROM bench_mux WHERE id = $1"


def peak_mib() -> float:
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r / (1 << 20) if sys.platform == "darwin" else r / 1024


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * q))] * 1e6 if xs else 0.0


def one_shared(port: int, sleep_ms: float = 0.0):
    sql = SQL if not sleep_ms else f"SELECT v, pg_sleep({sleep_ms / 1000}) FROM bench_mux WHERE id = $1"
    if V2:
        c = particledb.connect(host="127.0.0.1", port=port)
        return (lambda k: c.execute(sql, [k]).rows), c.close, "v2: one Connection"
    w = PdbWireClient("127.0.0.1", port, pool_size=1)
    return (lambda k: w.query(sql, [k])), w.close, "v1: PdbWireClient(pool_size=1)"


def setup(port: int) -> None:
    w = PdbWireClient("127.0.0.1", port, pool_size=1)
    w.execute("DROP TABLE IF EXISTS bench_mux")
    w.execute("CREATE TABLE bench_mux (id int8 PRIMARY KEY, v text)")
    w.execute(f"INSERT INTO bench_mux SELECT g, 'v' || g FROM generate_series(1, {ROWS}) g")
    w.close()


def shared(port: int, threads: int, secs: float, sleep_ms: float = 0.0) -> dict:
    run, close, shape = one_shared(port, sleep_ms)
    run(1)  # prepare + warm
    lats = [[] for _ in range(threads)]
    errors = []
    go = threading.Barrier(threads + 1)

    def worker(i: int) -> None:
        x = 0x9E3779B97F4A7C15 ^ (i + 1)
        go.wait()
        end = time.monotonic() + secs
        out = lats[i]
        while True:
            t0 = time.monotonic()
            if t0 >= end:
                break
            x = (x * 6364136223846793005 + 1442695040888963407) & 0xFFFFFFFFFFFFFFFF
            k = 1 + (x >> 33) % ROWS
            try:
                r = run(k)
                if not r:
                    errors.append(f"no row for {k}")
            except Exception as e:  # noqa: BLE001
                errors.append(repr(e))
            out.append(time.monotonic() - t0)

    ts = [threading.Thread(target=worker, args=(i,)) for i in range(threads)]
    for t in ts:
        t.start()
    go.wait()
    w0 = time.monotonic()
    for t in ts:
        t.join()
    wall = time.monotonic() - w0
    close()
    allv = [v for l in lats for v in l]
    return {"bench": "shared", "shape": shape, "threads": threads, "server_sleep_ms": sleep_ms, "ops": len(allv),
            "ops_s": round(len(allv) / wall), "p50_us": round(pct(allv, 0.5), 1),
            "p99_us": round(pct(allv, 0.99), 1), "errors": len(errors)}


def point(port: int, secs: float) -> dict:
    r = shared(port, 1, secs)
    r["bench"] = "point"
    return r


def stream(port: int, rows: int) -> dict:
    q = f"SELECT g, repeat('x', 100) FROM generate_series(1, {rows}) g"
    t0 = time.monotonic()
    first = None
    n = s = 0
    if V2:
        c = particledb.connect(host="127.0.0.1", port=port)
        decoder = "pyarrow" if c._use_pyarrow else "builtin"
        it = c.stream(q)
    else:
        c = PdbWireClient("127.0.0.1", port, pool_size=1)
        decoder = "pyarrow (whole result buffered)"
        it = (tuple(r.values()) for r in c.stream(q))
    for row in it:
        if first is None:
            first = time.monotonic() - t0
        n += 1
        s += row[0]
    total = time.monotonic() - t0
    c.close()
    assert n == rows and s == rows * (rows + 1) // 2, (n, s)
    return {"bench": "stream", "rows": n, "decoder": decoder, "first_row_ms": round(first * 1000),
            "total_ms": round(total * 1000), "rows_s": round(n / total), "peak_rss_mib": round(peak_mib(), 1)}


def main() -> None:
    cmd, port = sys.argv[1], int(sys.argv[2])
    if cmd == "setup":
        setup(port)
        return
    if cmd == "shared":
        out = shared(port, int(sys.argv[3]), float(sys.argv[4]),
                     float(sys.argv[5]) if len(sys.argv) > 5 else 0.0)
    elif cmd == "point":
        out = point(port, float(sys.argv[3]))
    elif cmd == "stream":
        out = stream(port, int(sys.argv[3]))
    else:
        raise SystemExit(__doc__)
    out["sdk"] = "v2" if V2 else "v1"
    print(json.dumps(out), flush=True)


if __name__ == "__main__":
    main()
