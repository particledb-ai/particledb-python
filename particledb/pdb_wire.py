"""Pooled native-wire client (``PdbWireClient``) over multiplexed connections.

Every pooled connection is a :class:`particledb.connection.Connection`:
protocol 2 (multiplexed), every capability the SDK implements, typed
server-side prepared statements, Arrow results decoded without pyarrow. A call
borrows one connection for its duration (a socket is one server session, and
one socket per caller is the faster shape -- the SDK reference results
"One socket per Connection"); a :class:`Session` pins one for a sequence of
statements. For many concurrent statements over ONE socket use a
``Connection`` directly (``particledb.connect()``).

Example::

    from particledb.pdb_wire import PdbWireClient

    db = PdbWireClient()  # localhost:7173, the server's Unix socket when local
    rows = db.query("SELECT * FROM users WHERE id = $1", [1])
    db.execute("INSERT INTO users VALUES ($1, $2)", [1, "alice"])
    db.batch(["BEGIN", "UPDATE ...", "INSERT ...", "COMMIT"])

    with db.session() as s, s.transaction():
        s.execute("UPDATE acct SET bal = bal - $1 WHERE id = $2", [10, 1])
"""

from __future__ import annotations

import os
import queue
import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, Generator, Iterator, List, Optional, Sequence, Tuple, Union

from . import _arrow
from . import _protocol as P
from .connection import (
    DEFAULT_PORT,
    DEFAULT_UNIX_SOCKET_DIR,
    Connection,
    Result,
    is_local_host,
    unix_socket_path,
)
from .errors import (
    BatchError,
    BatchExecuteError,
    ConnectionError,
    DiskFullError,
    PdbError,
    ProtocolError,
    QueryCanceledError,
    QueryError,
    TransactionRolledBackError,
    _ConnectFailed,
)

try:
    from prometheus_client import REGISTRY, Counter, Gauge, Histogram

    _HAVE_PROMETHEUS = True
except ImportError:
    _HAVE_PROMETHEUS = False

__all__ = [
    "PdbWireClient", "Session", "PdbError", "ConnectionError", "QueryError", "QueryCanceledError",
    "TransactionRolledBackError", "DiskFullError", "BatchExecuteError", "BatchError", "ProtocolError",
    "DEFAULT_PORT", "DEFAULT_POOL_SIZE", "DEFAULT_UNIX_SOCKET_DIR", "is_local_host", "unix_socket_path",
]

DEFAULT_TIMEOUT = 30.0
DEFAULT_POOL_SIZE = min(os.cpu_count() or 4, 16)
DEFAULT_MAX_RETRIES = 3
RETRY_BASE_DELAY_S = 0.1
FLAG_TXN_ROLLED_BACK = P.FLAG_TXN_ROLLED_BACK
_sleep = time.sleep  # the connect-retry backoff (tests replace it)


# ---------------------------------------------------------------------------
# Prometheus metrics (no-op stubs when prometheus_client is absent)
# ---------------------------------------------------------------------------


class _NoOpMetric:
    def labels(self, **kwargs: Any) -> "_NoOpMetric":
        return self

    def inc(self, amount: float = 1) -> None:
        pass

    def dec(self, amount: float = 1) -> None:
        pass

    def observe(self, value: float) -> None:
        pass

    def set(self, value: float) -> None:
        pass


class _Metrics:
    """Client-side Prometheus metrics (no-ops without prometheus_client)."""

    def __init__(self, namespace: str = "pdb_client"):
        if _HAVE_PROMETHEUS:
            def get(cls: Any, name: str, doc: str, labels: Sequence[str] = (), **kw: Any) -> Any:
                full = f"{namespace}_{name}"
                try:
                    return cls(full, doc, labelnames=list(labels), **kw)
                except ValueError:  # already registered (several clients)
                    reg = REGISTRY._names_to_collectors  # type: ignore[attr-defined]
                    for cand in (full, full + "_total"):
                        if cand in reg:
                            return reg[cand]
                    return _NoOpMetric()

            self.queries_total = get(Counter, "queries_total", "Total queries executed", ["status", "type"])
            self.query_duration_seconds = get(Histogram, "query_duration_seconds", "Query latency", ["type"],
                                              buckets=[.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5])
            self.connections_active = get(Gauge, "connections_active", "Connections currently in use")
            self.connections_idle = get(Gauge, "connections_idle", "Connections idle in pool")
            self.connections_created = get(Counter, "connections_created_total", "Total connections created")
            self.connections_closed = get(Counter, "connections_closed_total", "Total connections closed", ["reason"])
            self.connection_errors = get(Counter, "connection_errors_total", "Connection errors", ["type"])
            self.retries_total = get(Counter, "retries_total", "Total retry attempts")
            self.batch_size = get(Histogram, "batch_size", "Batch statement count",
                                  buckets=[1, 2, 5, 10, 25, 50, 100, 250, 500])
            self.pool_wait_seconds = get(Histogram, "pool_wait_seconds", "Time waiting for pool connection",
                                         buckets=[.0001, .001, .005, .01, .025, .05, .1, .25, .5, 1])
        else:
            noop = _NoOpMetric()
            for n in ("queries_total", "query_duration_seconds", "connections_active", "connections_idle",
                      "connections_created", "connections_closed", "connection_errors", "retries_total",
                      "batch_size", "pool_wait_seconds"):
                setattr(self, n, noop)

    def generate_latest(self) -> str:
        if _HAVE_PROMETHEUS:
            from prometheus_client import generate_latest as _gen

            return _gen().decode("utf-8")
        return "# prometheus_client not installed\n"


# ---------------------------------------------------------------------------
# Connection pool
# ---------------------------------------------------------------------------


class _ConnectionPool:
    """Bounded pool of :class:`Connection`; blocking acquire."""

    def __init__(self, factory: Any, pool_size: int, metrics: _Metrics):
        self._factory = factory
        self._pool_size = max(1, pool_size)
        self._metrics = metrics
        self._idle: "queue.LifoQueue[Connection]" = queue.LifoQueue()
        self._lock = threading.Lock()
        self._total = 0
        self._active = 0

    def acquire(self, wait_timeout: float = 30.0) -> Connection:
        t0 = time.monotonic()
        while True:
            try:
                conn = self._idle.get_nowait()
            except queue.Empty:
                break
            self._metrics.connections_idle.dec()
            if conn.peer_gone():
                self._forget(conn, "dead")
                continue
            return self._lend(conn, t0)
        with self._lock:
            reserved = self._total < self._pool_size
            if reserved:
                self._total += 1
        if reserved:
            try:
                conn = self._factory()
            except BaseException as exc:
                with self._lock:
                    self._total -= 1
                self._metrics.connection_errors.labels(type="connect").inc()
                if isinstance(exc, _ConnectFailed):
                    raise
                if isinstance(exc, PdbError):
                    raise
                raise _ConnectFailed(f"failed to connect: {exc}") from exc
            self._metrics.connections_created.inc()
            return self._lend(conn, t0)
        try:
            conn = self._idle.get(timeout=wait_timeout)
        except queue.Empty:
            raise ConnectionError(f"pool exhausted (size={self._pool_size}): timed out after {wait_timeout}s",
                                  0, "08004") from None
        self._metrics.connections_idle.dec()
        if conn.peer_gone():
            self._forget(conn, "dead")
            return self.acquire(max(0.0, wait_timeout - (time.monotonic() - t0)))
        return self._lend(conn, t0)

    def _lend(self, conn: Connection, t0: float) -> Connection:
        with self._lock:
            self._active += 1
        self._metrics.connections_active.inc()
        self._metrics.pool_wait_seconds.observe(time.monotonic() - t0)
        return conn

    def _forget(self, conn: Connection, reason: str) -> None:
        try:
            conn.close()
        except Exception:
            pass
        self._metrics.connections_closed.labels(reason=reason).inc()
        with self._lock:
            self._total -= 1

    def release(self, conn: Connection, *, discard: bool = False) -> None:
        """Return ``conn``; it is closed instead when ``discard``, when it is
        closed, when the server says it is inside a transaction block, or when
        a request is still in flight on it (an unfinished stream): the next
        borrower must get a session nobody else is using."""
        with self._lock:
            self._active -= 1
        self._metrics.connections_active.dec()
        if discard or conn.closed or conn.in_transaction or conn.busy:
            with self._lock:
                self._total -= 1
            try:
                conn.close()
            except Exception:
                pass
            self._metrics.connections_closed.labels(reason="discard").inc()
            return
        self._idle.put(conn)
        self._metrics.connections_idle.inc()

    def close_all(self) -> None:
        while True:
            try:
                conn = self._idle.get_nowait()
            except queue.Empty:
                break
            self._forget(conn, "idle")

    @property
    def idle_count(self) -> int:
        return self._idle.qsize()

    @property
    def active_count(self) -> int:
        return self._active

    @property
    def total_count(self) -> int:
        return self._total


# ---------------------------------------------------------------------------
# Batch-accumulating transaction (PdbWireClient.transaction)
# ---------------------------------------------------------------------------


class _Transaction:
    """Accumulates SQL statements and sends them as ONE ``BATCH`` with
    ``BEGIN`` / ``COMMIT`` on exit (no reads inside: use :meth:`PdbWireClient.session`
    for an interactive transaction)."""

    def __init__(self, client: "PdbWireClient"):
        self._client = client
        self._stmts: List[str] = ["BEGIN"]

    def execute(self, sql: str, _params: Optional[Sequence[Any]] = None) -> None:
        if _params:
            raise ValueError("a batched transaction carries SQL text only; use session() for parameters")
        self._stmts.append(sql)

    def _commit(self) -> int:
        self._stmts.append("COMMIT")
        return self._client.batch(self._stmts)

    def _rollback(self) -> None:
        # Nothing was sent: the statements only ever travel at commit.
        self._stmts = ["BEGIN"]


class _TransactionCtxMgr:
    def __init__(self, client: "PdbWireClient"):
        self._client = client
        self._txn: Optional[_Transaction] = None

    def __enter__(self) -> _Transaction:
        self._txn = _Transaction(self._client)
        return self._txn

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        assert self._txn is not None
        if exc_type is None:
            self._txn._commit()
        else:
            self._txn._rollback()


# ---------------------------------------------------------------------------
# Results helpers
# ---------------------------------------------------------------------------


def _to_pyarrow(result_ipc: bytes) -> Any:
    if not _arrow.HAVE_PYARROW:
        raise PdbError("query_arrow() returns a pyarrow.Table and needs pyarrow: pip install pyarrow "
                       "(query() / stream() decode results without it)", 0, "0A000")
    if not result_ipc:
        return _arrow.pyarrow_modules()[0].table({})
    return _arrow.pyarrow_modules()[1].open_stream(result_ipc).read_all()


def _entries_to_legacy(entries: List[Union[Result, PdbError]]) -> List[Union[int, List[Dict[str, Any]], QueryError]]:
    out: List[Union[int, List[Dict[str, Any]], QueryError]] = []
    for e in entries:
        if isinstance(e, Result):
            out.append(e.dicts() if e.has_rows else e.rowcount)
        else:
            out.append(e)  # type: ignore[arg-type]
    return out


def _sum_batch_entries(entries: List[Union[Result, PdbError]], total: int) -> int:
    affected = 0
    errors: List[Tuple[int, str]] = []
    causes: List[PdbError] = []
    for i, e in enumerate(entries):
        if isinstance(e, PdbError):
            errors.append((i, str(e)))
            causes.append(e)
        elif not e.has_rows:
            affected += e.rowcount
    if errors:
        raise BatchExecuteError(errors, affected, len(entries), total, causes[0].sqlstate,
                                causes[0].code, causes)
    if len(entries) < total:
        raise QueryError(f"server answered {len(entries)} of {total} parameter sets with no error", 0, "08P01")
    return affected


def _execute_each(conn: Connection, sql: str, rows: Sequence[Sequence[Any]],
                  timeout: Optional[float]) -> List[Union[Result, PdbError]]:
    return conn.execute_each(sql, rows, timeout)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# PdbWireClient
# ---------------------------------------------------------------------------


class PdbWireClient:
    """High-level native-wire client with a connection pool.

    Args:
        host: server host (a loopback host uses the server's Unix socket
            ``<unix_socket_dir>/.s.PDB.<port>`` when a server listens there).
        port: native-wire port (default :data:`DEFAULT_PORT`).
        pool_size: max pooled connections (default ``min(cpu_count, 16)``).
        timeout: connect timeout AND the default per-statement timeout, in
            seconds (default 30). A statement past it is cancelled (57014);
            only one the server does not answer is ambiguous (the connection is
            then closed).
        max_retries: retries of a failure to OPEN a connection (connect,
            HELLO, AUTH) -- the only failures that provably happen before any
            byte of a request is written. Once a request was sent, a lost
            connection raises :class:`ConnectionError` and is never retried.
        username, password: credentials (sent in ``F_AUTH``); ``None`` sends
            none (a ``--no-auth`` server).
        prefer_unix_socket, unix_socket_dir: transport choice.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: Optional[int] = None,
        pool_size: int = DEFAULT_POOL_SIZE,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        username: Optional[str] = None,
        password: Optional[str] = None,
        prefer_unix_socket: bool = True,
        unix_socket_dir: str = DEFAULT_UNIX_SOCKET_DIR,
        **connection_kwargs: Any,
    ):
        self.host = host
        self.port = DEFAULT_PORT if port is None else int(port)
        self.pool_size = pool_size
        self.timeout = timeout
        self.max_retries = max_retries
        self.username = username
        self.password = password
        self.prefer_unix_socket = prefer_unix_socket
        self.unix_socket_dir = unix_socket_dir
        self._conn_kwargs = connection_kwargs
        self._metrics = _Metrics()
        self._pool = _ConnectionPool(self._new_connection, pool_size, self._metrics)

    def _new_connection(self) -> Connection:
        kw = dict(self._conn_kwargs)
        kw.setdefault("statement_timeout", self.timeout)
        return Connection(
            self.host, self.port, user=self.username, password=self.password,
            connect_timeout=self.timeout, prefer_unix_socket=self.prefer_unix_socket,
            unix_socket_dir=self.unix_socket_dir, **kw)

    # ── plumbing ────────────────────────────────────────────────────────

    def _acquire_with_retry(self) -> Connection:
        delay = RETRY_BASE_DELAY_S
        last: Exception = RuntimeError("unreachable")
        for attempt in range(self.max_retries + 1):
            try:
                return self._pool.acquire(wait_timeout=self.timeout)
            except _ConnectFailed as exc:
                last = exc
                if attempt < self.max_retries:
                    self._metrics.retries_total.inc()
                    _sleep(delay)
                    delay = min(delay * 2, 5.0)
        raise ConnectionError(f"all {self.max_retries + 1} connection attempts failed: {last}",
                              0, "08001") from last

    def _ambiguous(self, conn: Connection, exc: BaseException) -> ConnectionError:
        """The connection failed after the request was sent: the server may
        have executed (and committed) it, so it is surfaced, never retried."""
        self._pool.release(conn, discard=True)
        self._metrics.connection_errors.labels(type="reset").inc()
        if isinstance(exc, QueryCanceledError):
            return exc  # type: ignore[return-value]
        return ConnectionError(
            "connection lost after the request was sent; it may or may not have been executed "
            f"(not retried): {exc}", getattr(exc, "code", 0), getattr(exc, "sqlstate", None) or "08006")

    def _with_conn(self, fn: Any, kind: str) -> Any:
        t0 = time.monotonic()
        conn = self._acquire_with_retry()
        try:
            result = fn(conn)
        except QueryCanceledError as exc:
            if exc.ambiguous:
                self._metrics.queries_total.labels(status="error", type=kind).inc()
                raise self._ambiguous(conn, exc) from exc
            self._pool.release(conn)
            self._metrics.queries_total.labels(status="error", type=kind).inc()
            raise
        except ConnectionError as exc:
            self._metrics.queries_total.labels(status="error", type=kind).inc()
            raise self._ambiguous(conn, exc) from exc
        except PdbError:
            # The server answered: the connection is in sync. It goes back to
            # the pool unless it is inside a transaction block (release checks).
            self._pool.release(conn)
            self._metrics.queries_total.labels(status="error", type=kind).inc()
            raise
        except (ValueError, TypeError):
            # A value that cannot be bound: raised before anything was sent.
            self._pool.release(conn)
            raise
        except BaseException:
            # Interrupted mid-request (KeyboardInterrupt, a bug): never reuse.
            self._pool.release(conn, discard=True)
            raise
        finally:
            self._metrics.query_duration_seconds.labels(type=kind).observe(time.monotonic() - t0)
        self._pool.release(conn)
        self._metrics.queries_total.labels(status="ok", type=kind).inc()
        return result

    # ── public API ──────────────────────────────────────────────────────

    def connection(self) -> Connection:
        """A NEW dedicated multiplexed :class:`Connection` (not pooled): many
        threads may run statements on it at once. Close it when done."""
        return self._new_connection()

    def execute_result(self, sql: str, params: Optional[Sequence[Any]] = None) -> Result:
        """The full :class:`Result` (columns, rows as tuples, rowcount, notices)."""
        return self._with_conn(lambda c: c.execute(sql, params), "query")

    def query(self, sql: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
        """Rows as dicts (typed values; no pyarrow needed). ``$1..$n``
        parameters are bound typed through a server-side prepared statement."""
        return self._with_conn(lambda c: c.execute(sql, params).dicts(), "query")

    def query_arrow(self, sql: str, params: Optional[Sequence[Any]] = None) -> Any:
        """The result as a ``pyarrow.Table`` (needs pyarrow)."""
        if not _arrow.HAVE_PYARROW:
            return _to_pyarrow(b"")
        return self._with_conn(lambda c: c.query_arrow(sql, params), "query_arrow")

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> int:
        """Affected row count."""
        return self._with_conn(lambda c: c.execute(sql, params).rowcount, "execute")

    def batch(self, sqls: Sequence[str]) -> int:
        """Run statements in ONE request; total affected rows. A failing
        statement raises :class:`BatchError` with its SQLSTATE (the server
        stopped there; a connection left in a failed block is not pooled)."""
        if not sqls:
            return 0
        self._metrics.batch_size.observe(len(sqls))
        counts = self._with_conn(lambda c: c.batch(list(sqls)), "batch")
        return sum(c for c in counts if c > 0)

    def execute_batch(self, sql: str, rows: Sequence[Sequence[Any]]
                      ) -> List[Union[int, List[Dict[str, Any]], QueryError]]:
        """One prepared statement per parameter set, in order; one entry per
        set the server ran: ``int`` (affected), ``list`` of row dicts, or the
        :class:`QueryError` of a set that failed. Outside a transaction each
        set commits on its own and a failure does not stop the rest."""
        if not rows:
            return []
        self._metrics.batch_size.observe(len(rows))
        return _entries_to_legacy(self._with_conn(lambda c: _execute_each(c, sql, rows, None), "execute_batch"))

    def execute_many(self, sql: str, rows: Sequence[Sequence[Any]]) -> int:
        """Like :meth:`execute_batch` but raises :class:`BatchExecuteError`
        (``sqlstate`` = the first failure's) if any set failed; returns the
        total affected rows."""
        if not rows:
            return 0
        self._metrics.batch_size.observe(len(rows))
        entries = self._with_conn(lambda c: _execute_each(c, sql, rows, None), "execute_batch")
        return _sum_batch_entries(entries, len(rows))

    def bulk_insert(self, table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> int:
        """ONE atomic native bulk ingest (``F_BULK``); rows loaded."""
        return self._with_conn(lambda c: c.bulk_insert(table, columns, rows), "bulk")

    def session(self) -> "Session":
        """Pin one pooled connection for a sequence of statements (an
        interactive transaction, session state)."""
        return Session(self)

    def stream(self, sql: str, params: Optional[Sequence[Any]] = None,
               batch_size: int = 1000) -> Iterator[Dict[str, Any]]:
        """Iterate a result's rows as dicts in bounded memory: decoded one
        Arrow record batch at a time as the server's chunks arrive (no pyarrow
        needed). ``batch_size`` is accepted for compatibility (the server
        sizes the batches)."""
        conn = self._acquire_with_retry()
        rs = None
        ok = False
        try:
            rs = conn.stream(sql, params)
            cols = rs.columns
            for row in rs:
                yield dict(zip(cols, row)) if len(set(cols)) == len(cols) else _first_wins(cols, row)
            ok = True
        finally:
            if rs is not None and not ok:
                rs.close()
            self._pool.release(conn, discard=not ok)

    def transaction(self) -> _TransactionCtxMgr:
        """Statements collected and sent as ONE batch ``BEGIN .. COMMIT``."""
        return _TransactionCtxMgr(self)

    def ping(self) -> bool:
        try:
            return self._with_conn(lambda c: c.ping() >= 0, "ping")
        except PdbError:
            return False

    def health(self) -> Dict[str, Any]:
        """``connected``, ``latency_ms``, ``protocol`` (``unix`` / ``tcp``),
        ``wire_protocol`` (``pdb-native-v2``), ``capabilities``, pool counts."""
        info: Dict[str, Any] = {"connected": False, "latency_ms": -1.0, "protocol": "unknown",
                                "wire_protocol": "unknown", "capabilities": []}
        try:
            conn = self._acquire_with_retry()
        except PdbError:
            conn = None
        if conn is not None:
            try:
                info["latency_ms"] = round(conn.ping(), 3)
                info["connected"] = True
                info["protocol"] = conn.transport.split(":", 1)[0]
                info["wire_protocol"] = conn.negotiated_protocol
                info["capabilities"] = conn.negotiated_capabilities
                self._pool.release(conn)
            except Exception:
                self._pool.release(conn, discard=True)
        info.update(pool_idle=self._pool.idle_count, pool_active=self._pool.active_count,
                    pool_total=self._pool.total_count)
        return info

    def metrics(self) -> str:
        return self._metrics.generate_latest()

    def close(self) -> None:
        self._pool.close_all()

    def __enter__(self) -> "PdbWireClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"PdbWireClient(host={self.host!r}, port={self.port!r}, pool_size={self.pool_size!r})"


def _first_wins(cols: List[str], row: Tuple[Any, ...]) -> Dict[str, Any]:
    d: Dict[str, Any] = {}
    for c, v in zip(cols, row):
        d.setdefault(c, v)
    return d


# ---------------------------------------------------------------------------
# Session -- one pinned connection
# ---------------------------------------------------------------------------


class Session:
    """A pooled connection pinned for a sequence of statements.

    ``BEGIN`` .. ``COMMIT`` here is an interactive transaction: reads see the
    transaction's own writes. On ``close()`` the connection returns to the pool
    only if the SERVER reports no open transaction block (the response flags
    of the last request) and the connection is healthy; otherwise it is
    closed.
    """

    def __init__(self, client: PdbWireClient):
        self._client = client
        self._conn: Optional[Connection] = client._acquire_with_retry()

    @property
    def connection(self) -> Connection:
        if self._conn is None:
            raise ConnectionError("session is closed", 0, "08003")
        return self._conn

    def _call(self, fn: Any) -> Any:
        conn = self.connection
        try:
            return fn(conn)
        except QueryCanceledError as exc:
            if exc.ambiguous:
                self._conn = None
                raise self._client._ambiguous(conn, exc) from exc
            raise
        except ConnectionError as exc:
            self._conn = None
            raise self._client._ambiguous(conn, exc) from exc
        except PdbError:
            raise
        except BaseException as exc:
            if isinstance(exc, (ValueError, TypeError)):
                raise  # a caller error raised before anything was sent
            self._conn = None
            self._client._pool.release(conn, discard=True)
            raise

    def execute_result(self, sql: str, params: Optional[Sequence[Any]] = None) -> Result:
        return self._call(lambda c: c.execute(sql, params))

    def query(self, sql: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
        return self._call(lambda c: c.execute(sql, params).dicts())

    def query_arrow(self, sql: str, params: Optional[Sequence[Any]] = None) -> Any:
        return self._call(lambda c: c.query_arrow(sql, params))

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> int:
        return self._call(lambda c: c.execute(sql, params).rowcount)

    def execute_batch(self, sql: str, rows: Sequence[Sequence[Any]]
                      ) -> List[Union[int, List[Dict[str, Any]], QueryError]]:
        return _entries_to_legacy(self._call(lambda c: _execute_each(c, sql, rows, None)))

    def execute_many(self, sql: str, rows: Sequence[Sequence[Any]]) -> int:
        if not rows:
            return 0
        return _sum_batch_entries(self._call(lambda c: _execute_each(c, sql, rows, None)), len(rows))

    def stream(self, sql: str, params: Optional[Sequence[Any]] = None) -> Iterator[Tuple[Any, ...]]:
        return self._call(lambda c: c.stream(sql, params))

    def begin(self) -> None:
        self.execute("BEGIN")

    def commit(self) -> None:
        """``COMMIT``; raises :class:`TransactionRolledBackError` when the
        server rolled back a failed transaction instead."""
        self._call(lambda c: c.commit())

    def rollback(self) -> None:
        self.execute("ROLLBACK")

    def savepoint(self, name: str) -> None:
        self._call(lambda c: c.savepoint(name))

    def rollback_to(self, name: str) -> None:
        self._call(lambda c: c.rollback_to(name))

    @contextmanager
    def transaction(self) -> Generator["Session", None, None]:
        """``BEGIN``; ``COMMIT`` on success, ``ROLLBACK`` on an exception."""
        self.begin()
        try:
            yield self
        except BaseException:
            if self._conn is not None:
                try:
                    self.rollback()
                except PdbError:
                    pass
            raise
        self.commit()

    def close(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            self._client._pool.release(conn)

    def __enter__(self) -> "Session":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
