"""DB-API 2.0 connection over one multiplexed native-wire connection."""

from __future__ import annotations

import enum
import threading
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Union
from urllib.parse import parse_qsl, unquote

from .. import connection as _native
from .._dsn import parse_connection_url
from . import errors as E
from . import rows as _rows
from ._sql import STYLES


class IsolationLevel(enum.IntEnum):
    """Transaction isolation levels (psycopg's enum)."""

    READ_UNCOMMITTED = 1
    READ_COMMITTED = 2
    REPEATABLE_READ = 3
    SERIALIZABLE = 4


_ISO_SQL = {1: "READ UNCOMMITTED", 2: "READ COMMITTED", 3: "REPEATABLE READ", 4: "SERIALIZABLE"}


def _iso(level: Any) -> Optional[IsolationLevel]:
    if level is None:
        return None
    if isinstance(level, str):
        key = "_".join(level.upper().replace("-", " ").split())
        try:
            return IsolationLevel[key]
        except KeyError:
            raise E.ProgrammingError(f"unknown isolation level {level!r}") from None
    return IsolationLevel(level)


class Rollback(Exception):
    """Raise inside ``with conn.transaction():`` to roll that block back and
    leave it without an error (psycopg's ``Rollback``)."""

    def __init__(self, transaction: Any = None):
        super().__init__()
        self.transaction = transaction


class Transaction:
    """``with conn.transaction():`` -- the outermost block opens a
    transaction (``BEGIN``) and commits it on success, rolls it back on an
    exception; a block nested inside an open transaction is a savepoint."""

    def __init__(self, conn: "Connection", savepoint_name: Optional[str] = None,
                 force_rollback: bool = False):
        self.connection = conn
        self.savepoint_name = savepoint_name
        self.force_rollback = force_rollback
        self._outer = False

    def __enter__(self) -> "Transaction":
        c = self.connection
        c._check()
        if c._native.txn_status:
            c._tx_depth += 1
            if self.savepoint_name is None:
                self.savepoint_name = f"_pdb_sp_{c._tx_depth}"
            c._control(f"SAVEPOINT {_native._ident(self.savepoint_name)}")
        else:
            self._outer = True
            c._tx_depth = 0
            c._control(c._begin_sql())
        return self

    def __exit__(self, et: Any, ev: Any, tb: Any) -> bool:
        c = self.connection
        rollback = et is not None or self.force_rollback
        try:
            if c.closed:
                return False
            if self._outer:
                if rollback:
                    c._control("ROLLBACK")
                else:
                    c._commit_sql()
            else:
                name = _native._ident(self.savepoint_name or "")
                if rollback:
                    c._control(f"ROLLBACK TO SAVEPOINT {name}")
                c._control(f"RELEASE SAVEPOINT {name}")
                c._tx_depth -= 1
        except BaseException:
            if et is None:
                raise
        return isinstance(ev, Rollback) and (ev.transaction is None or ev.transaction is self)


class Connection:
    """A PEP 249 connection: one native-wire socket (protocol 2, multiplexed)
    and one server session.

    Transactions follow PEP 249: ``autocommit`` is ``False`` by default and
    the first statement opens a transaction that :meth:`commit` /
    :meth:`rollback` end. The BEGIN travels ON that first statement
    (``FLAG_TXN_BEGIN``, one round trip), and :meth:`commit` sends nothing
    when the server reports no open transaction -- the JDBC driver's lazy
    protocol. ``threadsafety`` is 2: threads may share a connection (each
    statement is its own stream; they share the session and its transaction)
    but not a cursor.

    The native API is on :attr:`native` (the same session): KV, vector,
    ``bulk_insert``, ``stream``, ``submit`` and ``cancel``.
    """

    # PEP 249 optional extension: the exception classes as attributes.
    Warning = E.Warning
    Error = E.Error
    InterfaceError = E.InterfaceError
    DatabaseError = E.DatabaseError
    DataError = E.DataError
    OperationalError = E.OperationalError
    IntegrityError = E.IntegrityError
    InternalError = E.InternalError
    ProgrammingError = E.ProgrammingError
    NotSupportedError = E.NotSupportedError

    def __init__(self, native: _native.Connection, *, autocommit: bool = False,
                 paramstyle: str = "auto", isolation_level: Any = None,
                 read_only: Optional[bool] = None, deferrable: Optional[bool] = None,
                 row_factory: Callable[[Any], Any] = _rows.tuple_row,
                 raise_on_silent_rollback: bool = False, bulk_executemany: bool = True,
                 batch_executemany: bool = True):
        if paramstyle not in STYLES:
            raise E.ProgrammingError(f"unknown paramstyle {paramstyle!r} (one of {', '.join(STYLES)})")
        self._native = native
        self._autocommit = bool(autocommit)
        self.paramstyle = paramstyle
        self._isolation = _iso(isolation_level)
        self._read_only = read_only
        self._deferrable = deferrable
        self.row_factory = row_factory
        self.raise_on_silent_rollback = raise_on_silent_rollback
        self.bulk_executemany = bulk_executemany
        self.batch_executemany = batch_executemany
        self._begin_lock = threading.Lock()
        self._ddl_txn = False  # DDL ran in the transaction now open (see _ddl_in_open_txn)
        self._tx_depth = 0
        self._relcols: Dict[str, List[str]] = {}
        self._prokind: Dict[str, str] = {}
        self._notice_handlers: List[Callable[[Any], None]] = []
        self._ns: Optional[Any] = None
        native.notice_handler = self._on_notice

    # ── state ───────────────────────────────────────────────────────────

    @property
    def native(self) -> _native.Connection:
        """The underlying native-wire :class:`particledb.Connection` (same session)."""
        return self._native

    @property
    def closed(self) -> bool:
        return self._native.closed

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        self._check()
        if bool(value) != self._autocommit and self._native.txn_status:
            raise E.ProgrammingError("can't change 'autocommit' now: the connection is in a transaction "
                                     "(commit() or rollback() first)")
        self._autocommit = bool(value)

    def set_autocommit(self, value: bool) -> None:
        self.autocommit = value

    @property
    def isolation_level(self) -> Optional[IsolationLevel]:
        """The level transactions this connection opens run at (``None``: the
        server's default, READ COMMITTED unless the server is configured
        otherwise)."""
        return self._isolation

    @isolation_level.setter
    def isolation_level(self, value: Any) -> None:
        self._no_txn("isolation_level")
        self._isolation = _iso(value)

    @property
    def read_only(self) -> Optional[bool]:
        return self._read_only

    @read_only.setter
    def read_only(self, value: Optional[bool]) -> None:
        self._no_txn("read_only")
        self._read_only = value

    @property
    def deferrable(self) -> Optional[bool]:
        return self._deferrable

    @deferrable.setter
    def deferrable(self, value: Optional[bool]) -> None:
        self._no_txn("deferrable")
        self._deferrable = value

    @property
    def in_transaction(self) -> bool:
        """The server reports an open (or failed) transaction block."""
        return bool(self._native.txn_status)

    @property
    def transaction_status(self) -> str:
        """``"idle"``, ``"intrans"`` or ``"inerror"`` (as the server reports it)."""
        st = self._native.txn_status
        return "inerror" if st & 0x20 else ("intrans" if st else "idle")

    def _no_txn(self, what: str) -> None:
        self._check()
        if self._native.txn_status:
            raise E.ProgrammingError(f"can't change {what!r} now: the connection is in a transaction")

    def _check(self) -> None:
        if self._native.closed:
            raise E.InterfaceError("the connection is closed", sqlstate="08003")

    # ── transactions ────────────────────────────────────────────────────

    def _begin_sql(self) -> str:
        sql = "BEGIN"
        if self._isolation is not None:
            sql += " ISOLATION LEVEL " + _ISO_SQL[int(self._isolation)]
        if self._read_only is not None:
            sql += " READ ONLY" if self._read_only else " READ WRITE"
        if self._deferrable is not None:
            sql += " DEFERRABLE" if self._deferrable else " NOT DEFERRABLE"
        return sql

    def _plain_begin(self) -> bool:
        return self._isolation is None and self._read_only is None and self._deferrable is None

    def _needs_begin(self, txn_control: bool) -> bool:
        return not self._autocommit and not txn_control and not self._native.txn_status

    def _control(self, sql: str) -> Any:
        """A transaction-control statement. If it fails WITHOUT the server's
        answer, whether it ran is unknown, so the connection is closed (the
        JDBC driver's rule) rather than left under a guessed state."""
        self._check()
        try:
            return self._native._exec_raw(sql, None, None)
        except BaseException as e:
            from .. import errors as _ne

            if isinstance(e, _ne.ConnectionError) and not self._native.closed:
                self._native.close()
            raise E.translate(e) from e

    def _ddl_in_open_txn(self) -> bool:
        """DDL ran in the transaction that is still open. The server's bulk
        ingest cannot see a table created (or changed) earlier in the same open
        transaction (42P01, and the transaction is aborted), so executemany
        then takes the batched path and ``bulk_insert`` loads through it too."""
        if self._ddl_txn and not self._native.txn_status:
            self._ddl_txn = False
        return self._ddl_txn

    def _ensure_txn(self) -> None:
        """Manual-commit mode: open the transaction now if none is (a request
        that cannot carry the BEGIN flag: bulk ingest, a batch)."""
        if self._needs_begin(False):
            with self._begin_lock:
                if self._needs_begin(False):
                    self._control(self._begin_sql())

    def commit(self) -> None:
        """Commit the open transaction (nothing is sent when none is open)."""
        self._check()
        if not self._native.txn_status:
            return
        self._commit_sql()

    def _commit_sql(self) -> None:
        s = self._control("COMMIT")
        if s is not None and s.flags & 0x40 and self.raise_on_silent_rollback:
            raise E.InFailedSqlTransaction(
                "COMMIT did not commit: the server rolled back the transaction because a "
                "statement in it had failed", sqlstate="25P02")

    def rollback(self) -> None:
        """Roll back the open transaction (nothing is sent when none is open)."""
        self._check()
        if not self._native.txn_status:
            return
        self._control("ROLLBACK")

    def transaction(self, savepoint_name: Optional[str] = None, force_rollback: bool = False) -> Transaction:
        """A transaction block (see :class:`Transaction`); works in either
        autocommit mode."""
        return Transaction(self, savepoint_name, force_rollback)

    # ── cursors and shortcuts ───────────────────────────────────────────

    def cursor(self, name: Optional[str] = None, *, row_factory: Optional[Callable[[Any], Any]] = None,
               scrollable: Optional[bool] = None, withhold: bool = False) -> Any:
        """A client-side :class:`Cursor`, or with ``name`` a server-side
        :class:`ServerCursor` that streams the result in bounded memory."""
        from ._cursor import Cursor, ServerCursor

        self._check()
        if name is not None:
            if scrollable:
                raise E.NotSupportedError("server-side cursors are forward-only (scrollable=True)")
            return ServerCursor(self, name, row_factory or self.row_factory)
        return Cursor(self, row_factory or self.row_factory)

    def execute(self, query: str, params: Any = None, *, prepare: Optional[bool] = None) -> Any:
        """psycopg's shortcut: a new cursor, executed, returned."""
        cur = self.cursor()
        cur.execute(query, params)
        return cur

    def bulk_insert(self, table: str, data: Any, columns: Optional[Sequence[str]] = None, *,
                    timeout: Optional[float] = None) -> int:
        """Load ``data`` into ``table`` with ONE native bulk ingest (the JDBC
        ``bulkWriter``'s path): atomic, every row or none; inside a
        transaction it joins it. ``data`` may be an iterable of tuples (a
        generator streams), an iterable of dicts, a pandas / polars
        DataFrame, or a pyarrow Table / RecordBatch. ``columns`` defaults to
        the dict keys / DataFrame columns, else the table's columns in order.
        Returns the rows loaded. pyarrow is not needed."""
        from .. import _bulkdata

        self._check()
        self._ensure_txn()
        try:
            if self._ddl_in_open_txn():
                return self._insert_through_batches(table, data, columns)
            return _bulkdata.load(self._native, table, data, columns, self._table_columns, timeout)
        except BaseException as e:
            raise E.translate(e) from e

    def _insert_through_batches(self, table: str, data: Any, columns: Optional[Sequence[str]]) -> int:
        """``bulk_insert`` after DDL in the open transaction: the same rows as
        INSERT statements in that transaction (atomic at its commit)."""
        from .. import _bulkdata

        p = _bulkdata.plan(data, columns)
        if p is None:
            return 0
        cols = p.columns if p.columns is not None else self._table_columns(table)[: p.width]
        if p.mode != "rows":
            from .. import _arrow

            pa, pa_ipc = _arrow.pyarrow_modules()
            rows: List[Any] = []
            for chunk in p.payload:
                t = pa_ipc.open_stream(chunk).read_all()
                rows.extend(zip(*(c.to_pylist() for c in t.columns)))
        else:
            rows = list(p.payload)
        cur = self.cursor()
        saved, self.paramstyle = self.paramstyle, "numeric"
        try:
            cur.executemany(f"INSERT INTO {table} ({', '.join(_native._ident(c) for c in cols)}) "
                            f"VALUES ({', '.join(f'${i + 1}' for i in range(len(cols)))})", rows)
        finally:
            self.paramstyle = saved
        return cur.rowcount

    def _table_columns(self, relation: str) -> List[str]:
        cols = self._relcols.get(relation)
        if cols is None:
            s = self._native._exec_raw(f"SELECT * FROM {relation} LIMIT 0", None, None)
            from ._result import ResultSet

            cols = [f.name for f in ResultSet.from_stream(s, "SELECT", False, False).fields]
            if len(self._relcols) > 256:
                self._relcols.clear()
            self._relcols[relation] = cols
        return cols

    def cancel(self) -> int:
        """Cancel every statement in flight on this connection (57014)."""
        return self._native.cancel()

    # ── notices ─────────────────────────────────────────────────────────

    def add_notice_handler(self, fn: Callable[[Any], None]) -> None:
        """Call ``fn(notice)`` for each server notice as it arrives."""
        self._notice_handlers.append(fn)

    def remove_notice_handler(self, fn: Callable[[Any], None]) -> None:
        self._notice_handlers.remove(fn)

    def _on_notice(self, n: Any) -> None:
        for fn in list(self._notice_handlers):
            fn(n)

    # ── the rich API on the same session ────────────────────────────────

    def _namespaces(self) -> Any:
        if self._ns is None:
            self._ns = _SessionClient(self)
        return self._ns

    @property
    def kv(self) -> Any:
        """Key-value operations (:class:`particledb.KV`) on THIS session."""
        return self._namespaces().kv

    @property
    def vector(self) -> Any:
        """Vector search (:class:`particledb.Vector`) on THIS session."""
        return self._namespaces().vector

    @property
    def ai(self) -> Any:
        """AI SQL functions (:class:`particledb.AI`) on THIS session."""
        return self._namespaces().ai

    @property
    def rag(self) -> Any:
        return self._namespaces().rag

    @property
    def ops(self) -> Any:
        """Operations helpers (:class:`particledb.Ops`: changefeeds, AS OF, ...)."""
        return self._namespaces().ops

    # ── lifecycle ───────────────────────────────────────────────────────

    def close(self) -> None:
        """Close the connection; an open transaction is discarded (rolled back
        by the server)."""
        self._native.close()

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, et: Any, ev: Any, tb: Any) -> None:
        """psycopg's rule: commit on success, roll back on an exception, then close."""
        try:
            if not self.closed:
                if et is None:
                    self.commit()
                else:
                    try:
                        self.rollback()
                    except E.Error:
                        pass
        finally:
            self.close()

    def __repr__(self) -> str:
        return (f"<particledb.dbapi.Connection {self._native.transport} "
                f"{'closed' if self.closed else self.transaction_status}>")


class _SessionClient:
    """The ``ParticleDB``-shaped client the rich-API namespaces (KV, Vector,
    AI, RAG, Ops) take, running on one DB-API connection's session: their SQL
    joins the connection's open transaction when there is one (KV writes are
    never transactional), else autocommits."""

    def __init__(self, conn: Connection):
        from ..ai import AI
        from ..kv import KV
        from ..ops import Ops
        from ..rag import RAG
        from ..vector import Vector

        self._conn = conn
        self._wire = self
        self.kv = KV(self)
        self.vector = Vector(self)
        self.ai = AI(self)
        self.rag = RAG(self)
        self.ops = Ops(self)

    def _with_conn(self, fn: Callable[[Any], Any], _purpose: str = "") -> Any:
        return fn(self._conn.native)

    def _wire_call(self, fn: Callable[[Any], Any]) -> Any:
        try:
            return fn(self)
        except BaseException as e:
            raise E.translate(e) from e

    def query(self, sql: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
        try:
            return self._conn.native.query(sql, list(params) if params else None)
        except BaseException as e:
            raise E.translate(e) from e

    def query_one(self, sql: str, params: Optional[Sequence[Any]] = None) -> Optional[Dict[str, Any]]:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> int:
        try:
            return self._conn.native.execute(sql, list(params) if params else None).rowcount
        except BaseException as e:
            raise E.translate(e) from e


_BOOL_OPTS = {"autocommit", "prefer_unix_socket", "require_mux", "raise_on_silent_rollback",
              "bulk_executemany", "batch_executemany", "read_only", "deferrable", "use_pyarrow"}
_FLOAT_OPTS = {"connect_timeout", "statement_timeout", "cancel_grace", "write_timeout"}
_INT_OPTS = {"prepare_cache_size", "stream_buffer_limit"}
_DBAPI_OPTS = {"autocommit", "paramstyle", "isolation_level", "read_only", "deferrable", "row_factory",
               "raise_on_silent_rollback", "bulk_executemany", "batch_executemany"}


def _coerce(k: str, v: str) -> Any:
    if k in _BOOL_OPTS:
        return v.lower() in ("1", "true", "yes", "on")
    if k in _FLOAT_OPTS:
        return float(v)
    if k in _INT_OPTS:
        return int(v)
    return v


def parse_dsn(dsn: str) -> Dict[str, Any]:
    """``pdb://[user[:password]@]host[:port][?option=value&...]`` (also
    ``particledb://``) -> connect keyword arguments."""
    try:
        parts = parse_connection_url(dsn)
    except ValueError:
        parts = None
    if parts is None:
        raise E.InterfaceError("invalid connection URL") from None
    if parts.scheme.lower() not in ("pdb", "particledb", "particledb+native"):
        raise E.InterfaceError("unsupported DSN scheme (use pdb://, particledb:// or particledb+native://)")
    kw: Dict[str, Any] = {}
    if parts.hostname:
        kw["host"] = parts.hostname
    if parts.port:
        kw["port"] = parts.port
    if parts.username:
        kw["user"] = unquote(parts.username)
    if parts.password:
        kw["password"] = unquote(parts.password)
    for k, v in parse_qsl(parts.query):
        kw[k] = _coerce(k, v)
    return kw


def connect(dsn: Optional[str] = None, **kwargs: Any) -> Connection:
    """Open a DB-API :class:`Connection` (native wire, protocol 2).

    ``dsn``: ``pdb://user:pw@host:port?autocommit=true``. Keyword arguments:
    ``host`` (default ``localhost``; the Unix socket when local), ``port``
    (default 7173), ``user``, ``password``, ``autocommit`` (default False),
    ``paramstyle`` (``auto`` | ``format`` | ``pyformat`` | ``qmark`` |
    ``numeric``), ``isolation_level``, ``read_only``, ``deferrable``,
    ``row_factory``, ``raise_on_silent_rollback``, ``bulk_executemany``,
    ``batch_executemany``, and
    every option of :class:`particledb.Connection` (``connect_timeout``,
    ``statement_timeout``, ``unix_socket_path``, ...)."""
    if dsn:
        for k, v in parse_dsn(dsn).items():
            kwargs.setdefault(k, v)
    opts = {k: kwargs.pop(k) for k in list(kwargs) if k in _DBAPI_OPTS}
    kwargs.pop("database", None)
    kwargs.pop("dbname", None)
    try:
        native = _native.Connection(_extended_types=True, **kwargs)
    except TypeError as e:
        raise E.ProgrammingError(f"connect(): {e}") from None
    except BaseException as e:
        raise E.translate(e) from e
    try:
        return Connection(native, **opts)
    except BaseException:
        native.close()
        raise
