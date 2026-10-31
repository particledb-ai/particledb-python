"""DB-API benchmark: particledb.dbapi / particledb.aio / the native API vs
psycopg 3 on PostgreSQL, same machine. DIRECTIONAL ONLY (one local host,
client and servers sharing the CPUs).

    PYTHONPATH=. python benchmarks/dbapi_bench.py \\
        --pdb-port 27310 --pg-dsn "host=127.0.0.1 port=27330 user=postgres dbname=postgres" \\
        [--only point,update,em,bulk,fetch] [--json out.json]

Workloads (each reports ops/s or rows/s, and p50 / p99 latency per operation
where an operation is one statement):

* point SELECT by primary key (100k-row table): 1 thread; 32 threads (one
  connection each, and for ParticleDB also 32 threads sharing ONE
  multiplexed connection); 32 asyncio tasks (ParticleDB: one connection;
  psycopg: 32 AsyncConnections, it cannot share one);
* small UPDATE by primary key, autocommit: 1 thread and 32 threads;
* executemany of 10k INSERT rows (ParticleDB: the bulk path, and the
  batched text path with bulk_executemany=False; psycopg: its pipeline);
* bulk_insert of 1M rows (psycopg: COPY FROM STDIN with write_row);
* a 1M-row result into a list (fetchall) and into a pandas DataFrame.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import statistics
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional

ROWS = 100_000
OPS_1 = 20_000
OPS_PER_WORKER = 1_500
WORKERS = 32


def pct(lat: List[float], p: float) -> float:
    if not lat:
        return float("nan")
    s = sorted(lat)
    return s[min(len(s) - 1, int(p * len(s)))] * 1e6


class Results:
    def __init__(self) -> None:
        self.rows: List[Dict[str, Any]] = []

    def add(self, workload: str, client: str, ops: int, secs: float, lat: Optional[List[float]] = None,
            unit: str = "ops/s") -> None:
        r = {"workload": workload, "client": client, "rate": ops / secs, "unit": unit, "secs": secs,
             "p50_us": pct(lat or [], 0.50), "p99_us": pct(lat or [], 0.99)}
        self.rows.append(r)
        p = f"p50 {r['p50_us']:8.1f} us  p99 {r['p99_us']:8.1f} us" if lat else ""
        print(f"{workload:34s} {client:34s} {r['rate']:>12,.0f} {unit:6s} {p}", flush=True)

    def markdown(self) -> str:
        out = ["| Workload | Client | Rate | p50 (us) | p99 (us) |", "|---|---|---:|---:|---:|"]
        for r in self.rows:
            lat = (f"{r['p50_us']:.1f} | {r['p99_us']:.1f}" if r["p50_us"] == r["p50_us"] else "- | -")
            out.append(f"| {r['workload']} | {r['client']} | {r['rate']:,.0f} {r['unit']} | {lat} |")
        return "\n".join(out)


# ── drivers ─────────────────────────────────────────────────────────────────

def pdb_connect(port: int, **kw: Any) -> Any:
    from particledb import dbapi

    return dbapi.connect(host="127.0.0.1", port=port, autocommit=True, **kw)


def pg_connect(dsn: str) -> Any:
    import psycopg

    return psycopg.connect(dsn, autocommit=True)


def setup(pdb_port: int, pg_dsn: str) -> None:
    rows = [(i, f"value-{i}", i) for i in range(ROWS)]
    c = pdb_connect(pdb_port)
    for t in ("bench_kv", "bench_em", "bench_bulk"):
        c.execute(f"DROP TABLE IF EXISTS {t}")
    c.execute("CREATE TABLE bench_kv (id INT PRIMARY KEY, v TEXT, n BIGINT)")
    c.execute("CREATE TABLE bench_em (id BIGINT PRIMARY KEY, name TEXT, score DOUBLE PRECISION, qty INT)")
    c.execute("CREATE TABLE bench_bulk (id BIGINT PRIMARY KEY, name TEXT, score DOUBLE PRECISION, qty INT)")
    c.bulk_insert("bench_kv", rows)
    c.close()
    p = pg_connect(pg_dsn)
    for t in ("bench_kv", "bench_em", "bench_bulk"):
        p.execute(f"DROP TABLE IF EXISTS {t}")
    p.execute("CREATE TABLE bench_kv (id INT PRIMARY KEY, v TEXT, n BIGINT)")
    p.execute("CREATE TABLE bench_em (id BIGINT PRIMARY KEY, name TEXT, score DOUBLE PRECISION, qty INT)")
    p.execute("CREATE TABLE bench_bulk (id BIGINT PRIMARY KEY, name TEXT, score DOUBLE PRECISION, qty INT)")
    with p.cursor().copy("COPY bench_kv FROM STDIN") as cp:
        for r in rows:
            cp.write_row(r)
    p.execute("VACUUM ANALYZE bench_kv")
    p.close()


# ── harness ─────────────────────────────────────────────────────────────────

def run_threads(make: Callable[[int], Callable[[int], None]], workers: int, ops: int) -> tuple:
    """``make(worker)`` returns ``op(i)``; each worker runs ``ops`` ops."""
    fns = [make(w) for w in range(workers)]
    for f in fns:
        for i in range(50):
            f(i)
    lat: List[List[float]] = [[] for _ in range(workers)]
    barrier = threading.Barrier(workers + 1)

    def body(w: int) -> None:
        f = fns[w]
        rec = lat[w]
        rnd = random.Random(w)
        barrier.wait()
        pc = time.perf_counter
        for _ in range(ops):
            i = rnd.randrange(ROWS)
            t = pc()
            f(i)
            rec.append(pc() - t)

    ts = [threading.Thread(target=body, args=(w,)) for w in range(workers)]
    for t in ts:
        t.start()
    barrier.wait()
    t0 = time.perf_counter()
    for t in ts:
        t.join()
    secs = time.perf_counter() - t0
    return workers * ops, secs, [x for r in lat for x in r]


async def run_tasks(make: Callable[[int], Any], workers: int, ops: int) -> tuple:
    fns = [await make(w) for w in range(workers)]
    for f in fns:
        for i in range(20):
            await f(i)
    lat: List[float] = []

    async def body(w: int) -> None:
        f = fns[w]
        rnd = random.Random(w)
        pc = time.perf_counter
        for _ in range(ops):
            i = rnd.randrange(ROWS)
            t = pc()
            await f(i)
            lat.append(pc() - t)

    t0 = time.perf_counter()
    await asyncio.gather(*(body(w) for w in range(workers)))
    return workers * ops, time.perf_counter() - t0, lat


# ── workloads ───────────────────────────────────────────────────────────────

SEL_PCT = "SELECT id, v, n FROM bench_kv WHERE id = %s"
SEL_NUM = "SELECT id, v, n FROM bench_kv WHERE id = $1"
UPD_PCT = "UPDATE bench_kv SET n = n + 1 WHERE id = %s"


def point(res: Results, port: int, dsn: str) -> None:
    import particledb

    def dbapi_op(_w: int, sql: str = SEL_PCT, conn: Any = None) -> Callable[[int], None]:
        c = conn or pdb_connect(port)
        cur = c.cursor()

        def op(i: int) -> None:
            cur.execute(sql, (i,))
            cur.fetchone()
        return op

    def native_op(_w: int) -> Callable[[int], None]:
        c = particledb.connect(host="127.0.0.1", port=port)

        def op(i: int) -> None:
            c.execute(SEL_NUM, [i]).rows
        return op

    def pg_op(_w: int) -> Callable[[int], None]:
        c = pg_connect(dsn)
        cur = c.cursor()

        def op(i: int) -> None:
            cur.execute(SEL_PCT, (i,))
            cur.fetchone()
        return op

    for name, mk in (("particledb.dbapi", dbapi_op), ("particledb native API", native_op), ("psycopg 3 / PG17", pg_op)):
        n, s, lat = run_threads(mk, 1, OPS_1)
        res.add("point SELECT, 1 thread", name, n, s, lat)
    shared = pdb_connect(port)
    for name, mk in (("particledb.dbapi (conn per thread)", dbapi_op),
                     ("particledb.dbapi (1 shared conn)", lambda w: dbapi_op(w, conn=shared)),
                     ("particledb native (conn per thread)", native_op),
                     ("psycopg 3 / PG17 (conn per thread)", pg_op)):
        n, s, lat = run_threads(mk, WORKERS, OPS_PER_WORKER)
        res.add(f"point SELECT, {WORKERS} threads", name, n, s, lat)
    shared.close()
    point_async(res, port, dsn)


def point_async(res: Results, port: int, dsn: str) -> None:
    import particledb.aio as pdb

    async def go() -> None:
        conn = await pdb.connect(host="127.0.0.1", port=port, autocommit=True)

        async def mk_pdb(_w: int) -> Callable[[int], Any]:
            async def op(i: int) -> None:
                cur = conn.cursor()
                await cur.execute(SEL_PCT, (i,))
                await cur.fetchone()
            return op

        n, s, lat = await run_tasks(mk_pdb, 1, OPS_1)
        res.add("point SELECT, 1 asyncio task", "particledb.aio", n, s, lat)
        n, s, lat = await run_tasks(mk_pdb, WORKERS, OPS_PER_WORKER)
        res.add(f"point SELECT, {WORKERS} asyncio tasks", "particledb.aio (1 shared conn)", n, s, lat)
        await conn.close()
        try:
            import psycopg
        except ImportError:
            return

        async def mk_pg(_w: int) -> Callable[[int], Any]:
            c = await psycopg.AsyncConnection.connect(dsn, autocommit=True)

            async def op(i: int) -> None:
                cur = c.cursor()
                await cur.execute(SEL_PCT, (i,))
                await cur.fetchone()
            return op

        n, s, lat = await run_tasks(mk_pg, 1, OPS_1)
        res.add("point SELECT, 1 asyncio task", "psycopg 3 async / PG17", n, s, lat)
        n, s, lat = await run_tasks(mk_pg, WORKERS, OPS_PER_WORKER)
        res.add(f"point SELECT, {WORKERS} asyncio tasks", "psycopg 3 async / PG17 (32 conns)", n, s, lat)

    asyncio.run(go())


def update(res: Results, port: int, dsn: str) -> None:
    def dbapi_op(_w: int) -> Callable[[int], None]:
        cur = pdb_connect(port).cursor()
        return lambda i: cur.execute(UPD_PCT, (i,))

    def pg_op(_w: int) -> Callable[[int], None]:
        cur = pg_connect(dsn).cursor()
        return lambda i: cur.execute(UPD_PCT, (i,))

    for name, mk in (("particledb.dbapi", dbapi_op), ("psycopg 3 / PG17", pg_op)):
        n, s, lat = run_threads(mk, 1, OPS_1 // 4)
        res.add("small UPDATE, 1 thread", name, n, s, lat)
    for name, mk in (("particledb.dbapi (conn per thread)", dbapi_op), ("psycopg 3 / PG17 (conn per thread)", pg_op)):
        n, s, lat = run_threads(mk, WORKERS, OPS_PER_WORKER // 3)
        res.add(f"small UPDATE, {WORKERS} threads", name, n, s, lat)


def best_of(n: int, fn: Callable[[], float]) -> float:
    return min(fn() for _ in range(n))


def executemany(res: Results, port: int, dsn: str) -> None:
    rows = [(i, f"name-{i}", i * 0.5, i % 1000) for i in range(10_000)]
    sql = "INSERT INTO bench_em VALUES (%s, %s, %s, %s)"

    def pdb_run(**kw: Any) -> float:
        c = pdb_connect(port, **kw)
        c.execute("DELETE FROM bench_em")
        cur = c.cursor()
        t = time.perf_counter()
        cur.executemany(sql, rows)
        s = time.perf_counter() - t
        assert cur.rowcount == len(rows)
        assert c.execute("SELECT count(*) FROM bench_em").fetchone()[0] == len(rows)
        c.close()
        return s

    def pg_run() -> float:
        c = pg_connect(dsn)
        c.execute("DELETE FROM bench_em")
        cur = c.cursor()
        t = time.perf_counter()
        cur.executemany(sql, rows)
        s = time.perf_counter() - t
        assert c.execute("SELECT count(*) FROM bench_em").fetchone()[0] == len(rows)
        c.close()
        return s

    res.add("executemany 10k INSERT", "particledb.dbapi (bulk path)", len(rows), best_of(3, pdb_run), unit="rows/s")
    res.add("executemany 10k INSERT", "particledb.dbapi (batched text)", len(rows),
            best_of(3, lambda: pdb_run(bulk_executemany=False)), unit="rows/s")
    res.add("executemany 10k INSERT", "psycopg 3 / PG17 (pipeline)", len(rows), best_of(3, pg_run), unit="rows/s")


def bulk(res: Results, port: int, dsn: str) -> None:
    import particledb

    rows = [(i, f"name-{i}", i * 0.5, i % 1000) for i in range(1_000_000)]

    def pdb_run() -> float:
        c = pdb_connect(port)
        c.execute("DELETE FROM bench_bulk")
        t = time.perf_counter()
        n = c.bulk_insert("bench_bulk", rows, ["id", "name", "score", "qty"])
        s = time.perf_counter() - t
        assert n == len(rows) and c.execute("SELECT count(*) FROM bench_bulk").fetchone()[0] == len(rows)
        c.close()
        return s

    def native_run() -> float:
        c = particledb.connect(host="127.0.0.1", port=port)
        c.execute("DELETE FROM bench_bulk")
        t = time.perf_counter()
        c.bulk_insert("bench_bulk", ["id", "name", "score", "qty"], rows)
        s = time.perf_counter() - t
        c.close()
        return s

    def pg_run() -> float:
        c = pg_connect(dsn)
        c.execute("TRUNCATE bench_bulk")
        t = time.perf_counter()
        with c.cursor().copy("COPY bench_bulk FROM STDIN") as cp:
            for r in rows:
                cp.write_row(r)
        s = time.perf_counter() - t
        assert c.execute("SELECT count(*) FROM bench_bulk").fetchone()[0] == len(rows)
        c.close()
        return s

    res.add("bulk_insert 1M rows", "particledb.dbapi bulk_insert", len(rows), best_of(2, pdb_run), unit="rows/s")
    res.add("bulk_insert 1M rows", "particledb native bulk_insert", len(rows), best_of(2, native_run), unit="rows/s")
    res.add("bulk_insert 1M rows", "psycopg 3 COPY write_row / PG17", len(rows), best_of(2, pg_run), unit="rows/s")


def fetch(res: Results, port: int, dsn: str) -> None:
    import particledb

    q = "SELECT * FROM bench_bulk"
    c = pdb_connect(port)
    n = c.execute("SELECT count(*) FROM bench_bulk").fetchone()[0]
    p = pg_connect(dsn)
    if n != 1_000_000 or p.execute("SELECT count(*) FROM bench_bulk").fetchone()[0] != 1_000_000:
        print("fetch: run the bulk workload first (bench_bulk must hold 1M rows)", file=sys.stderr)
        return

    def timed(fn: Callable[[], int]) -> float:
        t = time.perf_counter()
        got = fn()
        s = time.perf_counter() - t
        assert got == 1_000_000, got
        return s

    cur = c.cursor()
    native = particledb.connect(host="127.0.0.1", port=port)
    pcur = p.cursor()

    def pg_df() -> int:
        import pandas as pd

        pcur.execute(q)
        return len(pd.DataFrame(pcur.fetchall(), columns=[d.name for d in pcur.description]))

    cases = [
        ("1M-row result -> list", "particledb.dbapi fetchall", lambda: len(cur.execute(q).fetchall())),
        ("1M-row result -> list", "particledb native execute().rows", lambda: len(native.execute(q).rows)),
        ("1M-row result -> list", "psycopg 3 fetchall / PG17", lambda: len(pcur.execute(q).fetchall())),
        ("1M-row result -> DataFrame", "particledb.dbapi fetch_df", lambda: len(cur.execute(q).fetch_df())),
        ("1M-row result -> DataFrame", "particledb.dbapi fetch_polars", lambda: cur.execute(q).fetch_polars().height),
        ("1M-row result -> DataFrame", "psycopg 3 fetchall + pandas / PG17", pg_df),
    ]
    for w, name, fn in cases:
        try:
            s = best_of(2, lambda: timed(fn))
        except ImportError as e:
            print(f"{w} {name}: skipped ({e})")
            continue
        res.add(w, name, 1_000_000, s, unit="rows/s")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdb-port", type=int, required=True)
    ap.add_argument("--pg-dsn", required=True)
    ap.add_argument("--only", default="point,update,em,bulk,fetch")
    ap.add_argument("--json")
    ap.add_argument("--skip-setup", action="store_true")
    a = ap.parse_args()
    if not a.skip_setup:
        setup(a.pdb_port, a.pg_dsn)
    res = Results()
    steps = {"point": point, "update": update, "em": executemany, "bulk": bulk, "fetch": fetch}
    for k in a.only.split(","):
        steps[k](res, a.pdb_port, a.pg_dsn)
    print()
    print(res.markdown())
    if a.json:
        with open(a.json, "w") as f:
            json.dump(res.rows, f, indent=1)


if __name__ == "__main__":
    main()
