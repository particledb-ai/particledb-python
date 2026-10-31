"""The language-neutral half of the Python conformance adapter.

The runner contract (env in, one results file out), the case files, the
generic ``script``-case interpreter and the canonical value comparison of
the SDK conformance spec (``conformance/SPEC.md``) section 4 -- a Python port of the Java bundle's
``Harness.java``. An adapter supplies a :class:`Driver` (how this SDK opens a
session, runs a statement, binds a typed parameter and renders a typed cell)
and its ``custom`` cases.

Env (set by ``run_conformance.sh``): ``PDB_CONF_HOST``, ``PDB_CONF_PORT``,
``PDB_CONF_DEFAULT_PORT``, ``PDB_CONF_RESP_PORT``, ``PDB_CONF_MODE``,
``PDB_CONF_CASES_DIR``, ``PDB_CONF_IDS``, ``PDB_CONF_RESULTS``,
``PDB_CONF_CONTROL``; ``PDB_CONF_ONLY`` (comma-separated ids) for debugging.
"""

from __future__ import annotations

import json
import math
import os
import socket
import struct
import subprocess
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from peak_memory import peak_rss_mib

CASE_TIMEOUT_S = 180


class Skip(Exception):
    """The case does not apply to this SDK; the reason is mandatory."""


class Fail(Exception):
    """An expectation did not hold."""


def check(ok: bool, detail: str) -> None:
    if not ok:
        raise Fail(detail)


def show(o: Any) -> str:
    s = "null" if o is None else (f"'{o}'" if isinstance(o, str) else repr(o))
    return s if len(s) <= 200 else s[:200] + f"...({len(s)} chars)"


def eq(want: Any, got: Any, what: str) -> None:
    if want != got:
        raise Fail(f"{what}: expected {show(want)}, got {show(got)}")


# ── env ──────────────────────────────────────────────────────────────────────

def env(k: str) -> str:
    v = os.environ.get(k)
    if not v:
        raise RuntimeError(f"{k} is not set (run via run_conformance.sh)")
    return v


HOST = os.environ.get("PDB_CONF_HOST", "127.0.0.1")


def port() -> int:
    return int(env("PDB_CONF_PORT"))


def default_port() -> int:
    return int(env("PDB_CONF_DEFAULT_PORT"))


def resp_port() -> int:
    return int(env("PDB_CONF_RESP_PORT"))


def mode() -> str:
    return env("PDB_CONF_MODE")


def control(action: str) -> None:
    """``kill9`` / ``restart`` the server; raises unless the hook exits 0."""
    p = subprocess.run([env("PDB_CONF_CONTROL"), action], stdout=subprocess.PIPE,
                       stderr=subprocess.STDOUT, timeout=120)
    if p.returncode != 0:
        raise RuntimeError(f"control {action} failed: {p.stdout.decode(errors='replace')}")


# ── a minimal RESP2 client (the independent KV reader/writer) ────────────────

class Resp:
    """Replies: bytes (bulk / simple string), int, None (nil), list; an error
    reply raises."""

    def __init__(self, host: str, port_: int):
        self.sock = socket.create_connection((host, port_), timeout=15)
        self.f = self.sock.makefile("rb")

    def cmd(self, *args: Any) -> Any:
        out = bytearray(b"*%d\r\n" % len(args))
        for a in args:
            v = a if isinstance(a, (bytes, bytearray)) else str(a).encode("utf-8")
            out += b"$%d\r\n" % len(v) + bytes(v) + b"\r\n"
        self.sock.sendall(bytes(out))
        return self._read()

    def str(self, *args: Any) -> Optional[str]:
        r = self.cmd(*args)
        if r is None:
            return None
        return r.decode("utf-8") if isinstance(r, bytes) else str(r)

    def num(self, *args: Any) -> int:
        r = self.cmd(*args)
        if not isinstance(r, int):
            raise IOError(f"RESP: expected an integer reply, got {r!r}")
        return r

    def _line(self) -> bytes:
        l = self.f.readline()
        if not l.endswith(b"\r\n"):
            raise IOError("RESP: connection closed or bad line ending")
        return l[:-2]

    def _read(self) -> Any:
        t = self.f.read(1)
        if not t:
            raise IOError("RESP: connection closed")
        l = self._line()
        if t == b"+":
            return l
        if t == b"-":
            raise IOError("RESP error: " + l.decode(errors="replace"))
        if t == b":":
            return int(l)
        if t == b"$":
            n = int(l)
            if n < 0:
                return None
            v = self.f.read(n)
            if len(v) != n or self.f.read(2) != b"\r\n":
                raise IOError("RESP: short bulk")
            return v
        if t == b"*":
            n = int(l)
            return None if n < 0 else [self._read() for _ in range(n)]
        raise IOError(f"RESP: unknown reply type {t!r}")

    def close(self) -> None:
        try:
            self.f.close()
            self.sock.close()
        except OSError:
            pass

    def __enter__(self) -> "Resp":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


def resp() -> Resp:
    return Resp(HOST, resp_port())


# ── what an adapter implements ───────────────────────────────────────────────

class Param:
    """A typed parameter from a case file: ``type`` and canonical ``value``
    (``None`` = typed NULL; a list for arrays)."""

    __slots__ = ("type", "value")

    def __init__(self, type_: str, value: Any):
        self.type = type_
        self.value = value


class Session:
    """One SDK session (a connection) the interpreter drives."""

    def exec(self, sql: str, params: List[Param]) -> None: ...
    def query(self, sql: str, params: List[Param], types: List[str]) -> List[List[Any]]: ...
    def begin(self) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def isolation(self, level: str) -> None: ...
    def savepoint(self, name: str) -> None: ...
    def rollback_to(self, name: str) -> None: ...
    def close(self) -> None: ...


class Driver:
    sdk = "?"
    version = "?"

    def open(self) -> Session:
        raise NotImplementedError

    def sqlstate(self, exc: BaseException) -> Optional[str]:
        c: Optional[BaseException] = exc
        while c is not None:
            st = getattr(c, "sqlstate", None)
            if st:
                return st
            c = c.__cause__
        return None


# ── case files ───────────────────────────────────────────────────────────────

def load_cases(d: str) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for name in sorted(os.listdir(d)):
        if name.endswith(".json"):
            with open(os.path.join(d, name)) as f:
                for c in json.load(f)["cases"]:
                    out[c["id"]] = c
    return out


# ── the run loop ─────────────────────────────────────────────────────────────

def run(driver: Driver, customs: Dict[str, Callable[[], None]], extra: Optional[Dict[str, Any]] = None) -> int:
    cases = load_cases(env("PDB_CONF_CASES_DIR"))
    with open(env("PDB_CONF_IDS")) as f:
        ids = [l.strip() for l in f if l.strip()]
    only = os.environ.get("PDB_CONF_ONLY")
    only_set = set(only.split(",")) if only else None
    results: Dict[str, Dict[str, Any]] = {}
    counts = {"pass": 0, "fail": 0, "skip": 0}
    for cid in ids:
        c = cases.get(cid)
        r: Dict[str, Any] = {}
        if c is None:
            r = {"status": "fail", "detail": "the runner listed a case id the case files do not define"}
        elif only_set is not None and cid not in only_set:
            r = {"status": "skip", "reason": "PDB_CONF_ONLY excluded it (debug run)"}
        else:
            t0 = time.monotonic()
            box: Dict[str, Any] = {}

            def body() -> None:
                try:
                    if c["kind"] == "script":
                        run_script(driver, c)
                    else:
                        fn = customs.get(cid)
                        if fn is None:
                            raise Fail("adapter has no implementation of this custom case")
                        fn()
                except BaseException as e:  # noqa: BLE001 - reported per case
                    box["exc"] = e

            th = threading.Thread(target=body, name=f"case-{cid}", daemon=True)
            th.start()
            th.join(CASE_TIMEOUT_S)
            if th.is_alive():
                r = {"status": "fail", "detail": f"case did not finish within {CASE_TIMEOUT_S} s"}
            elif "exc" not in box:
                r = {"status": "pass"}
            else:
                e = box["exc"]
                if isinstance(e, Skip):
                    r = {"status": "skip", "reason": str(e)}
                else:
                    st = driver.sqlstate(e)
                    detail = ("" if isinstance(e, Fail) else type(e).__name__ + ": ") + str(e)
                    if st and not isinstance(e, Fail):
                        detail += f" [SQLSTATE {st}]"
                    r = {"status": "fail", "detail": detail or type(e).__name__}
            r["ms"] = int((time.monotonic() - t0) * 1000)
        counts[r["status"]] += 1
        print(f"{cid:<16} {r['status']:<4} {r.get('detail', r.get('reason', ''))}", flush=True)
        results[cid] = r
    doc = {"sdk": driver.sdk, "sdk_version": driver.version, "mode": mode(), "results": results}
    if extra:
        doc.update(extra)
    with open(env("PDB_CONF_RESULTS"), "w") as f:
        f.write(json.dumps(doc, indent=1) + "\n")
    print(f"{driver.sdk}: {counts['pass']} pass, {counts['fail']} fail, {counts['skip']} skip", flush=True)
    return 0 if counts["fail"] == 0 else 1


# ── the script interpreter ───────────────────────────────────────────────────

def run_script(driver: Driver, c: Dict[str, Any]) -> None:
    sessions: Dict[str, Session] = {}
    skip_next: Dict[str, int] = {}
    steps = c["steps"]
    try:
        for n, st in enumerate(steps, 1):
            conn = st.get("conn", "a")
            s = sessions.get(conn)
            if s is None:
                s = sessions[conn] = driver.open()
            op = st["op"]
            sql = st.get("sql")
            params = parse_params(st.get("params"))
            where = f"step {n} ({op}{' ' + abbreviate(sql) if sql else ''})"
            try:
                if op == "exec":
                    s.exec(sql, params)
                elif op == "query":
                    types = st["types"]
                    compare_rows(types, st["expect_rows"], s.query(sql, params, types), where)
                elif op == "error":
                    want = st["sqlstate"]
                    try:
                        s.exec(sql, params)
                    except Fail:
                        raise
                    except Exception as e:  # noqa: BLE001
                        eq(want, driver.sqlstate(e), f"{where} SQLSTATE ({e})")
                        continue
                    raise Fail(f"{where}: expected SQLSTATE {want}, statement succeeded")
                elif op == "begin":
                    s.begin()
                elif op == "commit":
                    s.commit()
                elif op == "rollback":
                    s.rollback()
                elif op == "isolation":
                    s.isolation(st["level"])
                elif op == "savepoint":
                    s.savepoint(st["name"])
                elif op == "rollback_to":
                    s.rollback_to(st["name"])
                elif op == "commit_error":
                    want = st["sqlstate"]
                    if skip_next.pop(conn, None) is not None:
                        continue  # the awaited SQLSTATE was already raised at a statement
                    try:
                        s.commit()
                    except Exception as e:  # noqa: BLE001
                        eq(want, driver.sqlstate(e), f"{where} SQLSTATE ({e})")
                        try:
                            s.rollback()
                        except Exception:  # noqa: BLE001 - already ended
                            pass
                        continue
                    raise Fail(f"{where}: expected the commit to fail {want}, it succeeded")
                else:
                    raise Fail(f"unknown script op {op}")
            except (Fail, Skip):
                raise
            except Exception as e:  # noqa: BLE001
                stt = driver.sqlstate(e)
                # Write skew may surface at a statement instead of the COMMIT:
                # accept it when this conn's next step awaits the same SQLSTATE.
                nxt = next((m for m in steps[n:] if m.get("conn", "a") == conn), None)
                if nxt is not None and nxt.get("op") == "commit_error" and stt and stt == nxt.get("sqlstate"):
                    try:
                        s.rollback()
                    except Exception:  # noqa: BLE001
                        pass
                    skip_next[conn] = n
                    continue
                raise Fail(f"{where}: {type(e).__name__}: {e}" + (f" [SQLSTATE {stt}]" if stt else "")) from e
    finally:
        for s in sessions.values():
            try:
                s.close()
            except Exception:  # noqa: BLE001
                pass


def abbreviate(s: str) -> str:
    return s if len(s) <= 60 else s[:57] + "..."


def parse_params(raw: Optional[List[Dict[str, Any]]]) -> List[Param]:
    out = []
    for m in raw or []:
        v = gen(m["gen"]) if "gen" in m else m.get("value")
        out.append(Param(m["type"], v))
    return out


def gen(g: Dict[str, Any]) -> str:
    """``{"repeat": s, "count": n}`` -> s * n; ``{"bytes": "all256", "count": n}``
    -> the bytea canonical ``\\x00..ff`` repeated n times."""
    count = int(g["count"])
    if "repeat" in g:
        return str(g["repeat"]) * count
    return "\\x" + bytes(range(256)).hex() * count


# ── canonical comparison (CONFORMANCE.md section 4) ──────────────────────────

def compare_rows(types: List[str], want: List[List[Any]], got: List[List[Any]], where: str) -> None:
    if len(want) != len(got):
        raise Fail(f"{where}: expected {len(want)} rows, got {len(got)} {show(got)}")
    for r, (wr, gr) in enumerate(zip(want, got)):
        if len(wr) != len(gr):
            raise Fail(f"{where}: row {r} width {len(gr)} != {len(wr)}")
        for col, (w, g) in enumerate(zip(wr, gr)):
            if isinstance(w, dict):
                w = gen(w["gen"])
            if not cell_equals(types[col], w, g):
                raise Fail(f"{where}: row {r} col {col + 1} ({types[col]}): expected {show(w)}, got {show(g)}")


def _f32(x: float) -> float:
    return struct.unpack("<f", struct.pack("<f", x))[0]


def cell_equals(type_: str, want: Any, got: Any) -> bool:
    if want is None or got is None:
        return want is None and got is None
    if type_.endswith("[]") or type_ == "vector":
        if not isinstance(want, list) or not isinstance(got, list) or len(want) != len(got):
            return False
        et = "float4" if type_ == "vector" else type_[:-2]
        return all(cell_equals(et, w, g) for w, g in zip(want, got))
    ws, gs = str(want), str(got)
    if type_ in ("float4", "float8"):
        try:
            a, b = float(ws), float(gs)
        except ValueError:
            return False
        if math.isnan(a) or math.isnan(b):
            return math.isnan(a) and math.isnan(b)
        return _f32(a) == _f32(b) if type_ == "float4" else a == b
    return ws == gs


def canon_float(d: float) -> str:
    """NaN / Infinity / -Infinity, else the shortest round-trip decimal."""
    if math.isnan(d):
        return "NaN"
    if math.isinf(d):
        return "Infinity" if d > 0 else "-Infinity"
    return repr(float(d))


def canon_float4(f: float) -> str:
    if math.isnan(f) or math.isinf(f):
        return canon_float(f)
    # The shortest decimal that reads back as the same float32.
    f32 = _f32(f)
    for p in range(1, 10):
        s = f"{f32:.{p}g}"
        if _f32(float(s)) == f32:
            return repr(float(s))
    return repr(f32)


def canon_timestamp(t: Any) -> str:
    """``YYYY-MM-DD HH:MM:SS[.f]`` with the fraction's trailing zeros trimmed."""
    base = f"{t.year:04d}-{t.month:02d}-{t.day:02d} {t.hour:02d}:{t.minute:02d}:{t.second:02d}"
    if t.microsecond:
        base += "." + f"{t.microsecond:06d}".rstrip("0")
    return base


def hexb(b: bytes) -> str:
    return "\\x" + bytes(b).hex()


def unhex(s: str) -> bytes:
    if not s.startswith("\\x"):
        raise ValueError(f"bytea canonical form is \\x<hex>: {s}")
    return bytes.fromhex(s[2:])
