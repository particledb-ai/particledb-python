"""The Python SDK's conformance adapter (package ``particledb``).

Every connection is opened the documented zero-option way --
``particledb.connect(host=HOST, port=PORT)`` (no port when the runner's server
is on the default) -- because the DEFAULTS are what is under test. Run by
``../run_conformance.sh``; ``python3 adapter.py --stream-child`` is the
memory-bounded streaming probe SQL-STREAM-001 runs in a fresh process.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import os
import random
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import harness as H  # noqa: E402
from harness import Fail, Skip, check, eq  # noqa: E402

import particledb  # noqa: E402
from particledb import Float4, Int2, Int4, Json, Jsonb  # noqa: E402
from particledb.errors import BatchExecuteError, PdbError, QueryCanceledError  # noqa: E402

STREAM_ROWS = 3_000_000
STREAM_RSS_LIMIT_MIB = 256


def _arrow_has_pyarrow() -> bool:
    from particledb import _arrow

    return _arrow.HAVE_PYARROW


def connect(**kw: Any) -> particledb.Connection:
    return particledb.connect(host=H.HOST, port=H.port(), **kw)


# ═════════════════════════════════════════════════════════════════════════════
# Parameter binding and canonical rendering
# ═════════════════════════════════════════════════════════════════════════════

def _tz(v: str) -> dt.datetime:
    s = v.strip()
    if len(s) >= 3 and s[-3] in "+-" and s[-2:].isdigit():
        s += ":00"
    return dt.datetime.fromisoformat(s)


def scalar(t: str, v: str) -> Any:
    """A case file's canonical text -> the Python value an application binds."""
    if t == "int2":
        return Int2(int(v))
    if t == "int4":
        return Int4(int(v))
    if t == "int8":
        return int(v)
    if t == "float4":
        return Float4(float(v))
    if t == "float8":
        return float(v)
    if t == "numeric":
        return decimal.Decimal(v)
    if t == "bool":
        return v == "true"
    if t == "bytea":
        return H.unhex(v)
    if t == "date":
        return dt.date.fromisoformat(v)
    if t == "timestamp":
        return dt.datetime.fromisoformat(v)
    if t == "timestamptz":
        return _tz(v)
    if t == "uuid":
        return uuid.UUID(v)
    if t == "json":
        return Json(v)
    if t == "jsonb":
        return Jsonb(v)
    return v  # text, varchar


def bind(params: List[H.Param]) -> List[Any]:
    out: List[Any] = []
    for p in params:
        if p.value is None:
            out.append(None)
        elif p.type.endswith("[]"):
            et = p.type[:-2]
            out.append([None if e is None else scalar(et, e) for e in p.value])
        else:
            out.append(scalar(p.type, p.value))
    return out


def canon(t: str, v: Any) -> Any:
    """A typed Python value as the case files' canonical form (section 4)."""
    if v is None:
        return None
    if t.endswith("[]"):
        check(isinstance(v, list), f"{t} column decoded as {type(v).__name__}: {v!r}")
        return [canon(t[:-2], e) for e in v]
    if t == "vector":
        check(isinstance(v, list), f"vector column decoded as {type(v).__name__}: {v!r}")
        return [H.canon_float4(float(e)) for e in v]
    if t in ("int2", "int4", "int8"):
        check(isinstance(v, int) and not isinstance(v, bool), f"{t} decoded as {type(v).__name__}")
        return str(v)
    if t == "float8":
        check(isinstance(v, float), f"float8 decoded as {type(v).__name__}")
        return H.canon_float(v)
    if t == "float4":
        check(isinstance(v, float), f"float4 decoded as {type(v).__name__}")
        return H.canon_float4(v)
    if t == "numeric":
        check(isinstance(v, decimal.Decimal), f"numeric decoded as {type(v).__name__}: {v!r}")
        return "NaN" if v.is_nan() else format(v, "f")
    if t == "bool":
        check(isinstance(v, bool), f"bool decoded as {type(v).__name__}")
        return "true" if v else "false"
    if t == "bytea":
        check(isinstance(v, bytes), f"bytea decoded as {type(v).__name__}")
        return H.hexb(v)
    if t == "date":
        check(isinstance(v, dt.date) and not isinstance(v, dt.datetime), f"date decoded as {type(v).__name__}")
        return v.isoformat()
    if t == "timestamp":
        check(isinstance(v, dt.datetime) and v.tzinfo is None, f"timestamp decoded as {v!r}")
        return H.canon_timestamp(v)
    if t == "timestamptz":
        check(isinstance(v, dt.datetime) and v.tzinfo is not None, f"timestamptz decoded as {v!r} (no zone)")
        return H.canon_timestamp(v.astimezone(dt.timezone.utc)) + "+00"
    if t == "uuid":
        check(isinstance(v, (uuid.UUID, str)), f"uuid decoded as {type(v).__name__}")
        return str(v).lower()
    check(isinstance(v, str), f"{t} decoded as {type(v).__name__}: {v!r}")
    return v


# ═════════════════════════════════════════════════════════════════════════════
# Script sessions
# ═════════════════════════════════════════════════════════════════════════════

class PySession(H.Session):
    def __init__(self, c: particledb.Connection):
        self.c = c

    def exec(self, sql: str, params: List[H.Param]) -> None:
        self.c.execute(sql, bind(params) or None)

    def query(self, sql: str, params: List[H.Param], types: List[str]) -> List[List[Any]]:
        r = self.c.execute(sql, bind(params) or None)
        return [[canon(types[i], v) for i, v in enumerate(row)] for row in r.rows]

    def begin(self) -> None:
        self.c.begin()

    def commit(self) -> None:
        self.c.commit()

    def rollback(self) -> None:
        self.c.rollback()

    def isolation(self, level: str) -> None:
        self.c.set_isolation(level)

    def savepoint(self, name: str) -> None:
        self.c.savepoint(name)

    def rollback_to(self, name: str) -> None:
        self.c.rollback_to(name)

    def close(self) -> None:
        self.c.close()


class PyDriver(H.Driver):
    sdk = "python"
    version = particledb.__version__

    def open(self) -> H.Session:
        return PySession(connect())


# ═════════════════════════════════════════════════════════════════════════════
# Custom cases
# ═════════════════════════════════════════════════════════════════════════════

def scalar_of(c: particledb.Connection, sql: str, params: Optional[List[Any]] = None) -> Any:
    r = c.execute(sql, params)
    check(bool(r.rows), f"no row from {sql}")
    return r.rows[0][0]


def caps(csv: str) -> List[str]:
    return sorted(p.strip() for p in csv.split(",") if p.strip())


def build(defs: Dict[str, Dict[str, Any]]) -> Dict[str, Callable[[], None]]:
    required = defs["DEF-002"]["required_capabilities"]
    cases: Dict[str, Callable[[], None]] = {}

    def case(cid: str) -> Callable[[Callable[[], None]], Callable[[], None]]:
        def reg(fn: Callable[[], None]) -> Callable[[], None]:
            cases[cid] = fn
            return fn
        return reg

    # ── DEFAULTS ──
    @case("DEF-001")
    def _() -> None:
        with connect() as c:
            eq("pdb-native-v2", scalar_of(c, "SHOW pdb.wire_protocol"), "server-recorded protocol")

    @case("DEF-002")
    def _() -> None:
        with connect() as c:
            got = caps(scalar_of(c, "SHOW pdb.wire_capabilities"))
            missing = [r for r in required if r not in got]
            check(not missing, f"server recorded {got}; missing required {missing}")

    @case("DEF-003")
    def _() -> None:
        with connect() as c:
            eq("arrow_ipc", scalar_of(c, "SHOW pdb.wire_result_format"), "server-recorded result format")

    @case("DEF-004")
    def _() -> None:
        with connect() as c:
            eq(scalar_of(c, "SHOW pdb.wire_protocol"), c.negotiated_protocol, "client protocol vs server record")
            eq(caps(scalar_of(c, "SHOW pdb.wire_capabilities")), sorted(c.negotiated_capabilities),
               "client capability set vs server record")

    @case("DEF-005")
    def _() -> None:
        eq(H.default_port(), particledb.DEFAULT_PORT, "SDK default port vs server default port")
        if H.port() != H.default_port():
            raise Skip(f"default-port constant checked; the port-less connect needs the runner's server on "
                       f"{H.default_port()} but that port was busy (server on {H.port()})")
        with particledb.connect(host=H.HOST) as c:
            eq("pdb-native-v2", scalar_of(c, "SHOW pdb.wire_protocol"), "port-less connect protocol")
        with particledb.ParticleDB(host=H.HOST) as db:  # the pooled facade's default too
            eq([{"pdb.wire_protocol": "pdb-native-v2"}], db.query("SHOW pdb.wire_protocol"),
               "ParticleDB() default protocol")

    @case("DEF-006")
    def _() -> None:
        hostile = "o'clock; DROP TABLE x -- "
        sql = "SELECT $1::text || ''"
        with connect() as c:
            for i in range(10):  # the default parameterised API
                eq(hostile, scalar_of(c, sql, [hostile]), f"bound text param, execution {i}")
            check(c.is_server_prepared(sql), "execute(sql, params) did not run server-prepared")
            with c.prepare(sql) as ps:  # the explicit handle
                for i in range(10):
                    eq(hostile, ps.execute([hostile]).scalar(), f"prepared handle, execution {i}")
                check(ps.server_prepared, "PreparedStatement not server-prepared")

    # ── SQL ──
    @case("SQL-PREP-001")
    def _() -> None:
        with connect() as c:
            c.execute("DROP TABLE IF EXISTS conf_prep1")
            c.execute("CREATE TABLE conf_prep1 (id int8 PRIMARY KEY, v text)")
            c.execute("INSERT INTO conf_prep1 SELECT g, 'v' || g FROM generate_series(1, 50) g")
            with c.prepare("SELECT v FROM conf_prep1 WHERE id = $1") as ps:
                for pass_ in range(2):
                    for k in range(1, 51):
                        i = k if pass_ == 0 else 51 - k
                        r = ps.execute([i])
                        eq([(f"v{i}",)], r.rows, f"id {i}")

    @case("SQL-PREP-002")
    def _() -> None:
        with connect() as c:
            c.execute("DROP TABLE IF EXISTS conf_prep2")
            c.execute("CREATE TABLE conf_prep2 (id int8 PRIMARY KEY, a text)")
            c.execute("INSERT INTO conf_prep2 VALUES (1, 'x')")
            with c.prepare("SELECT * FROM conf_prep2 WHERE id = $1") as ps:
                for _i in range(6):
                    eq(2, len(ps.execute([1]).columns), "width before ALTER")
                c.execute("ALTER TABLE conf_prep2 ADD COLUMN b int8")
                c.execute("UPDATE conf_prep2 SET b = 5")
                try:
                    r = ps.execute([1])
                except PdbError as e:
                    check(e.sqlstate in ("0A000", "26000"), f"after ALTER: unexpected {e} [{e.sqlstate}]")
                    return
                eq(3, len(r.columns), "width after ALTER (stale describe?)")
                eq(5, r.rows[0][2], "column b after ALTER")

    @case("SQL-BATCH-001")
    def _() -> None:
        with connect() as c:
            c.execute("DROP TABLE IF EXISTS conf_b1")
            c.execute("CREATE TABLE conf_b1 (id int8 PRIMARY KEY, v text)")
            counts = c.execute_many("INSERT INTO conf_b1 VALUES ($1, $2)", [(i, f"r{i}") for i in range(1, 1001)])
            eq(1000, len(counts), "per-row count entries")
            bad = [i for i, n in enumerate(counts) if n != 1]
            check(not bad, f"counts not 1 at {bad[:5]}")
            eq(1000, scalar_of(c, "SELECT count(*) FROM conf_b1"), "rows")
            eq(500500, scalar_of(c, "SELECT sum(id) FROM conf_b1"), "sum")

    @case("SQL-BATCH-002")
    def _() -> None:
        with connect() as c:
            c.execute("DROP TABLE IF EXISTS conf_b2")
            c.execute("CREATE TABLE conf_b2 (id int8 PRIMARY KEY)")
            c.begin()
            try:
                c.execute_many("INSERT INTO conf_b2 VALUES ($1)", [(1,), (2,), (1,), (3,)])
                raise Fail("batch with a duplicate key succeeded")
            except BatchExecuteError as e:
                eq("23505", e.sqlstate, f"batch failure SQLSTATE ({e})")
            c.rollback()
            eq(0, scalar_of(c, "SELECT count(*) FROM conf_b2"), "rows after rollback")

    @case("SQL-STREAM-001")
    def _() -> None:
        results = []
        modes = [("default", {})]
        if _arrow_has_pyarrow():
            modes.append(("pyarrow decoder", {"PDB_PY_DECODER": "pyarrow"}))
        for label, extra in modes:
            envv = dict(os.environ, **extra)
            p = subprocess.run([sys.executable, os.path.abspath(__file__), "--stream-child"],
                               env=envv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=170)
            if p.returncode != 0:
                raise Fail(f"{label}: streaming child failed: {p.stderr.decode(errors='replace')[-400:]}")
            r = json.loads(p.stdout.decode().strip().splitlines()[-1])
            results.append((label, r))
            eq(STREAM_ROWS, r["rows"], f"{label}: rows")
            eq(4500001500000, r["sum"], f"{label}: sum")
            check(r["first_ms"] < r["total_ms"] / 2,
                  f"{label}: first row after {r['first_ms']} ms of {r['total_ms']} ms: buffered, not streamed")
            check(r["peak_rss_mib"] <= STREAM_RSS_LIMIT_MIB,
                  f"{label}: peak RSS {r['peak_rss_mib']:.0f} MiB > {STREAM_RSS_LIMIT_MIB} MiB")
        print("  SQL-STREAM-001 " + "; ".join(
            f"{l}: {r['decoder']} peak {r['peak_rss_mib']:.0f} MiB, {r['total_ms']} ms" for l, r in results),
            flush=True)

    @case("SQL-MULTI-001")
    def _() -> None:
        with connect() as c:
            c.execute("DROP TABLE IF EXISTS conf_m1")
            c.execute("CREATE TABLE conf_m1 (id int8 PRIMARY KEY)")
            try:
                c.execute("INSERT INTO conf_m1 VALUES (1); INSERT INTO conf_m1 VALUES (1)")
                raise Fail("multi-statement with a duplicate key succeeded")
            except PdbError as e:
                eq("23505", e.sqlstate, f"multi-statement failure SQLSTATE ({e})")
            eq(0, scalar_of(c, "SELECT count(*) FROM conf_m1"), "rows (implicit transaction must roll back)")

    @case("SQL-MULTI-002")
    def _() -> None:
        with connect() as c:
            c.execute("DROP TABLE IF EXISTS conf_m2")
            c.execute("CREATE TABLE conf_m2 (id int8 PRIMARY KEY)")
            r = c.execute("INSERT INTO conf_m2 VALUES (1); INSERT INTO conf_m2 VALUES (2); SELECT count(*) FROM conf_m2")
            eq(2, r.scalar(), "final SELECT result of the multi-statement string")

    @case("SQL-TXN-005")
    def _() -> None:
        with connect() as a, particledb.ParticleDB(host=H.HOST, pdb_port=H.port()) as pool:
            a.execute("DROP TABLE IF EXISTS conf_x5")
            a.execute("CREATE TABLE conf_x5 (id int8 PRIMARY KEY, v int8)")
            a.execute("INSERT INTO conf_x5 VALUES (1, 0)")
            for i in range(1, 201):
                eq(1, a.execute("UPDATE conf_x5 SET v = $1 WHERE id = 1", [i]).rowcount, f"update count {i}")
                eq(i, scalar_of(a, "SELECT v FROM conf_x5 WHERE id = 1"), f"same-connection read {i}")
                got = pool.query("SELECT v FROM conf_x5 WHERE id = $1", [1])
                eq(i, got[0]["v"], f"pooled-connection read {i}")

    @case("SQL-CANCEL-001")
    def _() -> None:
        with connect() as c:
            box: Dict[str, Any] = {}
            ready = threading.Event()

            def run() -> None:
                ready.set()
                try:
                    c.execute("SELECT pg_sleep(30)")
                    box["ok"] = True
                except BaseException as e:  # noqa: BLE001
                    box["err"] = e
                    box["at"] = time.monotonic()

            t = threading.Thread(target=run, daemon=True)
            t.start()
            ready.wait()
            time.sleep(0.3)
            cancelled_at = time.monotonic()
            n = c.cancel()
            t.join(10)
            check(not t.is_alive(), "statement still running 10 s after cancel()")
            check("ok" not in box, f"long statement completed despite cancel (cancel() reset {n} streams)")
            e = box["err"]
            eq("57014", getattr(e, "sqlstate", None), f"cancelled statement SQLSTATE ({e!r})")
            check(box["at"] - cancelled_at < 5, "cancel took more than 5 s")
            eq(1, scalar_of(c, "SELECT 1"), "connection usable after cancel")

    @case("SQL-TIMEOUT-001")
    def _() -> None:
        with connect() as c:
            t0 = time.monotonic()
            try:
                c.execute("SELECT pg_sleep(30)", timeout=1)
                raise Fail("timeout=1 statement completed")
            except QueryCanceledError as e:
                eq("57014", e.sqlstate, f"client timeout SQLSTATE ({e})")
                check(not e.ambiguous, f"server-answered timeout reported ambiguous: {e}")
                check(time.monotonic() - t0 < 6, "timeout=1 took > 6 s")
            check(not c.closed, "connection closed after a timeout the server answered")
            c.ping()
            c.execute("SET statement_timeout = '500ms'")
            t0 = time.monotonic()
            try:
                c.execute("SELECT pg_sleep(30)")
                raise Fail("statement_timeout statement completed")
            except PdbError as e:
                eq("57014", e.sqlstate, f"statement_timeout SQLSTATE ({e})")
                check(time.monotonic() - t0 < 6, "statement_timeout took > 6 s")
            c.execute("RESET statement_timeout")
            eq(1, scalar_of(c, "SELECT 1"), "connection usable after statement_timeout")

    # ── CONCURRENCY ──
    @case("CONC-001")
    def _() -> None:
        with connect() as c:
            c.execute("DROP TABLE IF EXISTS conf_c1")
            c.execute("CREATE TABLE conf_c1 (id int8 PRIMARY KEY, v text)")
            c.execute("INSERT INTO conf_c1 SELECT g, 'row-' || g FROM generate_series(1, 1000) g")
            errors: List[str] = []
            ok = [0]
            lock = threading.Lock()

            def worker(seed: int) -> None:
                rnd = random.Random(seed)
                for i in range(200):
                    k = rnd.randint(1, 1000)
                    try:
                        if i % 2 == 0:
                            r = c.execute("SELECT id, v FROM conf_c1 WHERE id = $1", [k])
                        else:
                            r = c.execute(f"SELECT id, v FROM conf_c1 WHERE id = {k}")
                        if r.rows != [(k, f"row-{k}")]:
                            with lock:
                                errors.append(f"asked {k}, got {r.rows}")
                        else:
                            with lock:
                                ok[0] += 1
                    except Exception as e:  # noqa: BLE001
                        with lock:
                            errors.append(repr(e))

            with ThreadPoolExecutor(32) as ex:
                list(ex.map(worker, range(32)))
            check(not errors, f"{len(errors)} errors, first: {errors[:1]}")
            eq(6400, ok[0], "successful statements")
            eq("pdb-native-v2", scalar_of(c, "SHOW pdb.wire_protocol"), "protocol of the shared connection")

    @case("CONC-002")
    def _() -> None:
        with connect() as c:
            slow_done: Dict[str, float] = {}
            slow_ones: List[str] = []
            t0 = time.monotonic()

            def slow() -> None:
                c.execute("SELECT pg_sleep(2)")
                slow_done["t"] = time.monotonic()

            def fast() -> float:
                last = 0.0
                for _i in range(20):
                    s0 = time.monotonic()
                    c.execute("SELECT 1")
                    d = time.monotonic() - s0
                    if d > 1:
                        slow_ones.append(f"{d * 1000:.0f} ms")
                    last = time.monotonic()
                return last

            with ThreadPoolExecutor(9) as ex:
                fs = ex.submit(slow)
                time.sleep(0.1)
                fasts = [ex.submit(fast) for _ in range(8)]
                last_fast = max(f.result(30) for f in fasts)
                fs.result(30)
            check(not slow_ones, f"fast statements blocked behind the slow one: {slow_ones}")
            check(last_fast < slow_done["t"],
                  f"fast statements finished {(last_fast - slow_done['t']) * 1000:.0f} ms AFTER the slow one "
                  f"(head-of-line blocking); slow took {(slow_done['t'] - t0) * 1000:.0f} ms")

    @case("CONC-003")
    def _() -> None:
        def stream_sum(c: particledb.Connection, sql: str, sign: int) -> List[int]:
            n = total = disorder = prev = 0
            for (v,) in c.stream(sql):
                if n and sign * v != sign * prev + 1:
                    disorder += 1
                prev = v
                n += 1
                total += v
            return [n, total, disorder]

        with connect() as c, ThreadPoolExecutor(2) as ex:
            fa = ex.submit(stream_sum, c, "SELECT g FROM generate_series(1, 200000) g", 1)
            fb = ex.submit(stream_sum, c, "SELECT -g FROM generate_series(1, 200000) g", -1)
            ra, rb = fa.result(120), fb.result(120)
        eq([200000, 20000100000, 0], ra, "stream A rows / sum / out-of-order")
        eq([200000, -20000100000, 0], rb, "stream B rows / sum / out-of-order")

    @case("CONC-004")
    def _() -> None:
        with connect() as a, connect() as b, connect() as z:
            z.execute("DROP TABLE IF EXISTS conf_c4")
            z.execute("CREATE TABLE conf_c4 (id int8 PRIMARY KEY, owner text)")
            a.begin()
            b.begin()
            a.execute("INSERT INTO conf_c4 VALUES (1, 'a')")
            b.execute("INSERT INTO conf_c4 VALUES (2, 'b')")

            def view(c: particledb.Connection) -> str:
                return "".join(r[0] for r in c.execute("SELECT owner FROM conf_c4 ORDER BY id").rows)

            with ThreadPoolExecutor(16) as ex:
                fs = [ex.submit(view, a if t % 2 == 0 else b) for t in range(16)]
                for t, f in enumerate(fs):
                    eq("a" if t % 2 == 0 else "b", f.result(60), f"thread {t} view inside its connection's transaction")
            a.commit()
            b.commit()
            eq("ab", scalar_of(z, "SELECT string_agg(owner, '' ORDER BY id) FROM conf_c4"), "after both commits")

    @case("CONC-005")
    def _() -> None:
        with connect() as c:
            cap = c.max_concurrent_streams
            check(0 < cap < 4096, f"implausible stream cap {cap}")
            n = cap * 3
            go = threading.Event()

            def one() -> Any:
                go.wait()
                return c.execute("SELECT pg_sleep(0.05), 1").rows[0][1]

            with ThreadPoolExecutor(n) as ex:
                fs = [ex.submit(one) for _ in range(n)]
                go.set()
                for f in fs:
                    eq(1, f.result(120), f"result past the stream cap ({cap})")

    # ── KV parity ──
    def facade() -> particledb.ParticleDB:
        return particledb.ParticleDB(host=H.HOST, pdb_port=H.port())

    @case("KV-001")
    def _() -> None:
        with connect() as c, H.resp() as r:
            c.kv_set("conf:kv1", "hello wörld")
            eq("hello wörld", r.str("GET", "conf:kv1"), "RESP GET of SDK SET")
            r.cmd("SET", "conf:kv1b", "from resp")
            eq(b"from resp", c.kv_get("conf:kv1b"), "SDK GET of RESP SET")
        with facade() as db:
            eq("from resp", db.kv.get("conf:kv1b"), "facade kv.get")

    @case("KV-002")
    def _() -> None:
        with connect() as c, H.resp() as r, facade() as db:
            eq(None, c.kv_get("conf:kv2-absent"), "GET of an absent key")
            eq(None, db.kv.get("conf:kv2-absent"), "facade GET of an absent key")
            db.kv.set("conf:kv2", "x")
            check(db.kv.delete("conf:kv2"), "DEL of a present key reported not-deleted")
            eq(0, r.num("EXISTS", "conf:kv2"), "RESP EXISTS after SDK DEL")
            check(not db.kv.exists("conf:kv2"), "SDK exists after DEL")

    @case("KV-003")
    def _() -> None:
        with connect() as c, H.resp() as r, facade() as db:
            r.cmd("DEL", "conf:kv3")
            eq(1, c.kv_incr("conf:kv3"), "SDK INCR of absent")
            eq(11, r.num("INCRBY", "conf:kv3", "10"), "RESP INCRBY")
            eq(12, db.kv.incr("conf:kv3"), "SDK INCR")
            eq("12", r.str("GET", "conf:kv3"), "RESP GET")

    @case("KV-004")
    def _() -> None:
        with H.resp() as r, facade() as db:
            db.kv.set("conf:kv4", "v")
            check(db.kv.expire("conf:kv4", 100), "expire reported no key")
            ttl = r.num("TTL", "conf:kv4")
            check(99 <= ttl <= 100, f"RESP TTL after SDK expire 100: {ttl}")
            r.cmd("SET", "conf:kv4b", "w", "EX", "200")
            t2 = db.kv.ttl("conf:kv4b")
            check(199 <= t2 <= 200, f"SDK TTL after RESP EX 200: {t2}")
            db.kv.set("conf:kv4c", "w", ttl=50)  # the SDK's own TTL set
            t3 = r.num("TTL", "conf:kv4c")
            check(49 <= t3 <= 50, f"RESP TTL after SDK set(ttl=50): {t3}")

    @case("KV-005")
    def _() -> None:
        with H.resp() as r, facade() as db:
            db.kv.set("conf:kv5", "v")
            eq(-1, db.kv.ttl("conf:kv5"), "SDK TTL of a key with none")
            eq(-1, r.num("TTL", "conf:kv5"), "RESP TTL of a key with none")
            eq(-2, db.kv.ttl("conf:kv5-absent"), "SDK TTL of a missing key")
            eq(-2, r.num("TTL", "conf:kv5-absent"), "RESP TTL of a missing key")

    @case("KV-006")
    def _() -> None:
        with H.resp() as r, facade() as db:
            r.cmd("SET", "conf:kv6", "a", "EX", "300")
            db.kv.set("conf:kv6", "b")
            eq(-1, r.num("TTL", "conf:kv6"), "RESP TTL after SDK plain SET")
            eq("b", r.str("GET", "conf:kv6"), "RESP GET")

    @case("KV-007")
    def _() -> None:
        with H.resp() as r, facade() as db:
            db.kv.set("conf:kv7", "v")
            db.kv.expire("conf:kv7", 1)
            time.sleep(2.2)
            eq(None, db.kv.get("conf:kv7"), "SDK GET of an expired key")
            eq(0, r.num("EXISTS", "conf:kv7"), "RESP EXISTS of an expired key")
            check(not db.kv.exists("conf:kv7"), "SDK exists of an expired key")

    @case("KV-008")
    def _() -> None:
        allb = bytes(range(256))
        with connect() as c, H.resp() as r, facade() as db:
            c.kv_set("conf:kv8", allb)
            eq(allb, r.cmd("GET", "conf:kv8"), "RESP GET of SDK binary SET")
            r.cmd("SET", b"conf:kv8b", allb)
            eq(allb, c.kv_get("conf:kv8b"), "SDK GET of RESP binary SET")
            eq(allb, db.kv.get_bytes("conf:kv8b"), "facade get_bytes")

    @case("KV-009")
    def _() -> None:
        big = "x" * (1 << 20)
        with H.resp() as r, facade() as db:
            db.kv.set("conf:kv9", big)
            eq(1048576, r.num("STRLEN", "conf:kv9"), "RESP STRLEN")
            eq(big, r.str("GET", "conf:kv9"), "RESP GET of 1 MiB")

    @case("KV-010")
    def _() -> None:
        with H.resp() as r, facade() as db:
            r.cmd("DEL", "conf:kv10-absent")
            db.kv.mset({"conf:kv10a": "1", "conf:kv10b": "2"})
            eq([b"1", b"2", None], r.cmd("MGET", "conf:kv10a", "conf:kv10b", "conf:kv10-absent"), "RESP MGET")
            got = db.kv.mget(["conf:kv10a", "conf:kv10-absent", "conf:kv10b"])
            eq(["1", None, "2"], [got["conf:kv10a"], got["conf:kv10-absent"], got["conf:kv10b"]], "SDK mget")

    @case("KV-011")
    def _() -> None:
        with connect() as c, H.resp() as r:
            r.cmd("DEL", "conf:h", "conf:l", "conf:s", "conf:z")
            c.execute("SELECT HASH_SET($1, $2, $3)", ["conf:h", "f", "v"])
            eq("v", r.str("HGET", "conf:h", "f"), "RESP HGET of SDK HASH_SET")
            c.execute("SELECT LIST_PUSH($1, $2)", ["conf:l", "a"])
            c.execute("SELECT LIST_PUSH($1, $2)", ["conf:l", "b"])
            eq(2, len(r.cmd("LRANGE", "conf:l", "0", "-1")), "RESP LRANGE length")
            c.execute("SELECT SET_ADD($1, $2)", ["conf:s", "m"])
            eq(1, r.num("SISMEMBER", "conf:s", "m"), "RESP SISMEMBER of SDK SET_ADD")
            c.execute("SELECT ZSET_ADD($1, $2, $3)", ["conf:z", 1.5, "m"])
            eq("1.5", r.str("ZSCORE", "conf:z", "m"), "RESP ZSCORE of SDK ZSET_ADD")

    @case("KV-012")
    def _() -> None:
        with connect() as c, H.resp() as r:
            r.cmd("DEL", "conf:kv12")
            c.begin()
            c.kv_set("conf:kv12", "kept")
            c.rollback()
            eq("kept", r.str("GET", "conf:kv12"), "KV write after rollback")

    # ── DURABILITY ──
    def crash_restart() -> None:
        H.control("kill9")
        H.control("restart")

    @case("DUR-001")
    def _() -> None:
        with connect() as c:
            c.execute("DROP TABLE IF EXISTS conf_dur1")
            c.execute("CREATE TABLE conf_dur1 (id int8 PRIMARY KEY, v text)")
            for i in range(1, 501):
                c.execute("INSERT INTO conf_dur1 VALUES ($1, 'v')", [i])
        crash_restart()
        with connect() as c:
            eq(500, scalar_of(c, "SELECT count(*) FROM conf_dur1"), "acked rows after kill -9")
            eq(125250, scalar_of(c, "SELECT sum(id) FROM conf_dur1"), "acked ids after kill -9")

    @case("DUR-002")
    def _() -> None:
        with connect() as c:
            c.execute("DROP TABLE IF EXISTS conf_dur2")
            c.execute("CREATE TABLE conf_dur2 (id int8 PRIMARY KEY, v int8)")
            c.begin()
            c.execute("INSERT INTO conf_dur2 SELECT g, 1 FROM generate_series(1, 100) g")
            c.execute("UPDATE conf_dur2 SET v = 101 WHERE id = 7")
            c.commit()
        crash_restart()
        with connect() as c:
            eq(100, scalar_of(c, "SELECT count(*) FROM conf_dur2"), "rows of an acked COMMIT after kill -9")
            eq(200, scalar_of(c, "SELECT sum(v) FROM conf_dur2"), "the COMMIT's UPDATE after kill -9")

    @case("DUR-003")
    def _() -> None:
        with connect() as c:
            c.execute("DROP TABLE IF EXISTS conf_dur3")
            c.execute("CREATE TABLE conf_dur3 (id int8 PRIMARY KEY)")
            c.execute_many("INSERT INTO conf_dur3 VALUES ($1)", [(i,) for i in range(1, 1001)])
            c.execute("DROP TABLE IF EXISTS conf_dur3b")
            c.execute("CREATE TABLE conf_dur3b (id int8 PRIMARY KEY, v text)")
            eq(1000, c.bulk_insert("conf_dur3b", ["id", "v"], [(i, f"v{i}") for i in range(1, 1001)]),
               "bulk_insert rows loaded")
        crash_restart()
        with connect() as c:
            eq(1000, scalar_of(c, "SELECT count(*) FROM conf_dur3"), "acked batch rows after kill -9")
            eq(1000, scalar_of(c, "SELECT count(*) FROM conf_dur3b"), "acked bulk_insert rows after kill -9")

    @case("DUR-004")
    def _() -> None:
        with connect() as c:
            c.kv_set("conf:dur4", "durable")
            c.kv_delete("conf:dur4n")
            for _i in range(3):
                c.kv_incr("conf:dur4n")
        crash_restart()
        with connect() as c, H.resp() as r:
            eq(b"durable", c.kv_get("conf:dur4"), "SDK GET after kill -9")
            eq("3", r.str("GET", "conf:dur4n"), "RESP GET of the counter after kill -9")

    @case("DUR-005")
    def _() -> None:
        a = connect()
        db = particledb.ParticleDB(host=H.HOST, pdb_port=H.port())
        try:
            eq(1, scalar_of(a, "SELECT 1"), "before the kill")
            eq(1, db.query("SELECT 1 AS x")[0]["x"], "pooled, before the kill")
            H.control("kill9")
            t0 = time.monotonic()
            try:
                a.execute("SELECT 1")
                raise Fail("statement on a connection to a killed server succeeded")
            except PdbError as e:
                check(bool(e.sqlstate) and e.sqlstate.startswith("08"),
                      f"connection-loss SQLSTATE {e.sqlstate} is not class 08 ({e})")
                check(time.monotonic() - t0 < 10, "connection loss took > 10 s to surface")
        finally:
            try:
                a.close()
            except Exception:  # noqa: BLE001
                pass
            H.control("restart")
        with connect() as b:
            eq(1, scalar_of(b, "SELECT 1"), "new connection after restart")
        # The pool notices its idle socket died with the server and opens a
        # fresh one: the first borrow after the restart works.
        eq(1, db.query("SELECT 1 AS x")[0]["x"], "pooled borrow after restart")
        db.close()

    return cases


# ═════════════════════════════════════════════════════════════════════════════
# The streaming child process (SQL-STREAM-001)
# ═════════════════════════════════════════════════════════════════════════════

def stream_child() -> None:
    from particledb import _arrow

    t0 = time.monotonic()
    first = -1.0
    n = total = 0
    with connect() as c:
        decoder = "pyarrow" if c._use_pyarrow else "builtin"
        for row in c.stream(f"SELECT g, repeat('x', 100) FROM generate_series(1, {STREAM_ROWS}) g"):
            if first < 0:
                first = time.monotonic() - t0
            n += 1
            total += row[0]
    tot = time.monotonic() - t0
    print(json.dumps({"rows": n, "sum": total, "first_ms": int(first * 1000), "total_ms": int(tot * 1000),
                      "peak_rss_mib": H.peak_rss_mib(), "decoder": decoder,
                      "pyarrow_installed": _arrow.HAVE_PYARROW}))


def main() -> int:
    sdk_root = os.environ.get("PDB_CONF_SDK_ROOT")
    if sdk_root and not os.path.abspath(particledb.__file__).startswith(os.path.abspath(sdk_root)):
        print(f"particledb imported from {particledb.__file__}, not the SDK under test ({sdk_root})",
              file=sys.stderr)
        return 2
    if "--stream-child" in sys.argv:
        stream_child()
        return 0
    from particledb import _arrow

    defs = H.load_cases(H.env("PDB_CONF_CASES_DIR"))
    return H.run(PyDriver(), build(defs), {"decoder": "pyarrow" if _arrow.USE_PYARROW else "builtin",
                                           "pyarrow_installed": _arrow.HAVE_PYARROW})


if __name__ == "__main__":
    sys.exit(main())
