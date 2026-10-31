"""asyncio DB-API-shaped connection (psycopg's ``AsyncConnection`` surface)."""

from __future__ import annotations

import asyncio
from typing import Any, Callable, Dict, List, Optional, Sequence

from .. import _bulkdata
from .. import _protocol as P
from ..dbapi import errors as E
from ..dbapi import rows as _rows
from ..dbapi._connection import _DBAPI_OPTS, IsolationLevel, Rollback, _ISO_SQL, _iso, parse_dsn
from ..dbapi._sql import STYLES
from ..connection import _ident
from ._wire import AsyncWire, open_wire


class AsyncTransaction:
    """``async with conn.transaction():`` -- BEGIN / COMMIT (ROLLBACK on an
    exception); nested inside an open transaction it is a savepoint."""

    def __init__(self, conn: "AsyncConnection", savepoint_name: Optional[str] = None,
                 force_rollback: bool = False):
        self.connection = conn
        self.savepoint_name = savepoint_name
        self.force_rollback = force_rollback
        self._outer = False

    async def __aenter__(self) -> "AsyncTransaction":
        c = self.connection
        c._check()
        if c._wire.txn_status:
            c._tx_depth += 1
            if self.savepoint_name is None:
                self.savepoint_name = f"_pdb_sp_{c._tx_depth}"
            await c._control(f"SAVEPOINT {_ident(self.savepoint_name)}")
        else:
            self._outer = True
            c._tx_depth = 0
            await c._control(c._begin_sql())
        return self

    async def __aexit__(self, et: Any, ev: Any, tb: Any) -> bool:
        c = self.connection
        rollback = et is not None or self.force_rollback
        try:
            if c.closed:
                return False
            if self._outer:
                if rollback:
                    await c._control("ROLLBACK")
                else:
                    await c._commit_sql()
            else:
                name = _ident(self.savepoint_name or "")
                if rollback:
                    await c._control(f"ROLLBACK TO SAVEPOINT {name}")
                await c._control(f"RELEASE SAVEPOINT {name}")
                c._tx_depth -= 1
        except BaseException:
            if et is None:
                raise
        return isinstance(ev, Rollback) and (ev.transaction is None or ev.transaction is self)


class AsyncConnection:
    """One native-wire connection on asyncio: many coroutines may run
    statements on it at once (each is its own stream; they share the session
    and its transaction). Same transaction model as the blocking
    :class:`particledb.dbapi.Connection`: ``autocommit`` False by default, the
    BEGIN rides on the first statement, ``commit()`` sends nothing when no
    transaction is open."""

    Error = E.Error
    InterfaceError = E.InterfaceError
    DatabaseError = E.DatabaseError
    DataError = E.DataError
    OperationalError = E.OperationalError
    IntegrityError = E.IntegrityError
    InternalError = E.InternalError
    ProgrammingError = E.ProgrammingError
    NotSupportedError = E.NotSupportedError

    def __init__(self, wire: AsyncWire, *, autocommit: bool = False, paramstyle: str = "auto",
                 isolation_level: Any = None, read_only: Optional[bool] = None,
                 deferrable: Optional[bool] = None, row_factory: Callable[[Any], Any] = _rows.tuple_row,
                 raise_on_silent_rollback: bool = False, bulk_executemany: bool = True,
                 batch_executemany: bool = True):
        if paramstyle not in STYLES:
            raise E.ProgrammingError(f"unknown paramstyle {paramstyle!r} (one of {', '.join(STYLES)})")
        self._wire = wire
        self._autocommit = bool(autocommit)
        self.paramstyle = paramstyle
        self._isolation = _iso(isolation_level)
        self._read_only = read_only
        self._deferrable = deferrable
        self.row_factory = row_factory
        self.raise_on_silent_rollback = raise_on_silent_rollback
        self.bulk_executemany = bulk_executemany
        self.batch_executemany = batch_executemany
        self._begin_lock = asyncio.Lock()
        self._ddl_txn = False
        self._tx_depth = 0
        self._relcols: Dict[str, List[str]] = {}
        self._prokind: Dict[str, str] = {}
        self._notice_handlers: List[Callable[[Any], None]] = []
        wire.notice_handler = self._on_notice

    @classmethod
    async def connect(cls, dsn: Optional[str] = None, **kwargs: Any) -> "AsyncConnection":
        return await connect(dsn, **kwargs)

    # ── state ───────────────────────────────────────────────────────────

    @property
    def wire(self) -> AsyncWire:
        """The underlying protocol object (KV frames, ``ping``, ``cancel_all``)."""
        return self._wire

    @property
    def closed(self) -> bool:
        return self._wire.closed

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        self._check()
        if bool(value) != self._autocommit and self._wire.txn_status:
            raise E.ProgrammingError("can't change 'autocommit' now: the connection is in a transaction")
        self._autocommit = bool(value)

    async def set_autocommit(self, value: bool) -> None:
        self.autocommit = value

    @property
    def isolation_level(self) -> Optional[IsolationLevel]:
        return self._isolation

    @isolation_level.setter
    def isolation_level(self, value: Any) -> None:
        self._no_txn("isolation_level")
        self._isolation = _iso(value)

    async def set_isolation_level(self, value: Any) -> None:
        self.isolation_level = value

    @property
    def read_only(self) -> Optional[bool]:
        return self._read_only

    @read_only.setter
    def read_only(self, value: Optional[bool]) -> None:
        self._no_txn("read_only")
        self._read_only = value

    async def set_read_only(self, value: Optional[bool]) -> None:
        self.read_only = value

    @property
    def in_transaction(self) -> bool:
        return bool(self._wire.txn_status)

    @property
    def transaction_status(self) -> str:
        st = self._wire.txn_status
        return "inerror" if st & 0x20 else ("intrans" if st else "idle")

    def _no_txn(self, what: str) -> None:
        self._check()
        if self._wire.txn_status:
            raise E.ProgrammingError(f"can't change {what!r} now: the connection is in a transaction")

    def _check(self) -> None:
        if self._wire.closed:
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
        return not self._autocommit and not txn_control and not self._wire.txn_status

    async def _control(self, sql: str) -> Any:
        self._check()
        try:
            return await self._wire.exec_raw(sql, None)
        except BaseException as e:
            from .. import errors as _ne

            if isinstance(e, _ne.ConnectionError):
                self._wire.close()
            raise E.translate(e) from e

    def _ddl_in_open_txn(self) -> bool:
        """See the blocking ``Connection._ddl_in_open_txn``."""
        if self._ddl_txn and not self._wire.txn_status:
            self._ddl_txn = False
        return self._ddl_txn

    async def _ensure_txn(self) -> None:
        if self._needs_begin(False):
            async with self._begin_lock:
                if self._needs_begin(False):
                    await self._control(self._begin_sql())

    async def commit(self) -> None:
        self._check()
        if self._wire.txn_status:
            await self._commit_sql()

    async def _commit_sql(self) -> None:
        s = await self._control("COMMIT")
        if s is not None and s.flags & P.FLAG_TXN_ROLLED_BACK and self.raise_on_silent_rollback:
            raise E.InFailedSqlTransaction(
                "COMMIT did not commit: the server rolled back the transaction because a "
                "statement in it had failed", sqlstate="25P02")

    async def rollback(self) -> None:
        self._check()
        if self._wire.txn_status:
            await self._control("ROLLBACK")

    def transaction(self, savepoint_name: Optional[str] = None, force_rollback: bool = False) -> AsyncTransaction:
        return AsyncTransaction(self, savepoint_name, force_rollback)

    # ── cursors and shortcuts ───────────────────────────────────────────

    def cursor(self, name: Optional[str] = None, *, row_factory: Optional[Callable[[Any], Any]] = None) -> Any:
        from ._cursor import AsyncCursor, AsyncServerCursor

        self._check()
        if name is not None:
            return AsyncServerCursor(self, name, row_factory or self.row_factory)
        return AsyncCursor(self, row_factory or self.row_factory)

    async def execute(self, query: str, params: Any = None) -> Any:
        """A new cursor, executed, returned (``await conn.execute(...)``)."""
        cur = self.cursor()
        await cur.execute(query, params)
        return cur

    async def fetch(self, query: str, params: Any = None) -> List[Any]:
        """All rows of ``query`` (``fetchall`` of :meth:`execute`)."""
        cur = await self.execute(query, params)
        return await cur.fetchall()

    async def fetchrow(self, query: str, params: Any = None) -> Any:
        cur = await self.execute(query, params)
        return await cur.fetchone()

    async def fetchval(self, query: str, params: Any = None) -> Any:
        row = await self.fetchrow(query, params)
        return None if row is None else row[0]

    async def bulk_insert(self, table: str, data: Any, columns: Optional[Sequence[str]] = None, *,
                          timeout: Optional[float] = None) -> int:
        """ONE native bulk ingest (see the blocking ``Connection.bulk_insert``)."""
        self._check()
        await self._ensure_txn()
        try:
            p = _bulkdata.plan(data, columns)
            if p is None:
                return 0
            cols = p.columns if p.columns is not None else (await self._table_columns(table))[: p.width]
            if self._ddl_in_open_txn():
                return await self._insert_through_batches(table, p, cols)
            if p.mode == "rows":
                return await self._wire.bulk_insert(table, cols, p.payload, timeout)
            loaded = await self._wire.bulk_load_ipc(table, cols, p.payload, timeout)
            if p.expected >= 0 and loaded != p.expected:
                from ..errors import ProtocolError

                raise ProtocolError(f"bulk ingest loaded {loaded} of {p.expected} rows")
            return loaded
        except BaseException as e:
            raise E.translate(e) from e

    async def _insert_through_batches(self, table: str, p: Any, cols: List[str]) -> int:
        if p.mode != "rows":
            from .. import _arrow

            _pa, pa_ipc = _arrow.pyarrow_modules()
            rows: List[Any] = []
            for chunk in p.payload:
                t = pa_ipc.open_stream(chunk).read_all()
                rows.extend(zip(*(c.to_pylist() for c in t.columns)))
        else:
            rows = list(p.payload)
        cur = self.cursor()
        saved, self.paramstyle = self.paramstyle, "numeric"
        try:
            await cur.executemany(f"INSERT INTO {table} ({', '.join(_ident(c) for c in cols)}) "
                                  f"VALUES ({', '.join(f'${i + 1}' for i in range(len(cols)))})", rows)
        finally:
            self.paramstyle = saved
        return cur.rowcount

    async def _table_columns(self, relation: str) -> List[str]:
        cols = self._relcols.get(relation)
        if cols is None:
            from ..dbapi._result import ResultSet

            s = await self._wire.exec_raw(f"SELECT * FROM {relation} LIMIT 0", None)
            cols = [f.name for f in ResultSet.from_stream(s, "SELECT", False, False).fields]
            self._relcols[relation] = cols
        return cols

    def cancel(self) -> int:
        return self._wire.cancel_all()

    async def ping(self) -> float:
        try:
            return await self._wire.ping()
        except BaseException as e:
            raise E.translate(e) from e

    # ── KV (native frames) ──────────────────────────────────────────────

    async def kv_set(self, key: Any, value: Any) -> None:
        v = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        await self._kv(P.F_KV_SET, key, v)

    async def kv_get(self, key: Any) -> Optional[bytes]:
        from ..connection import _kv_value

        return _kv_value(await self._kv(P.F_KV_GET, key))

    async def kv_delete(self, key: Any) -> bool:
        return P.decode_ok((await self._kv(P.F_KV_DEL, key)).payload) > 0

    async def kv_incr(self, key: Any) -> int:
        from ..connection import _kv_int

        return _kv_int(await self._kv(P.F_KV_INCR, key))

    async def _kv(self, ftype: int, key: Any, value: Optional[bytes] = None) -> Any:
        try:
            return await self._wire.kv(ftype, key, value)
        except BaseException as e:
            raise E.translate(e) from e

    # ── notices ─────────────────────────────────────────────────────────

    def add_notice_handler(self, fn: Callable[[Any], None]) -> None:
        self._notice_handlers.append(fn)

    def remove_notice_handler(self, fn: Callable[[Any], None]) -> None:
        self._notice_handlers.remove(fn)

    def _on_notice(self, n: Any) -> None:
        for fn in list(self._notice_handlers):
            fn(n)

    # ── lifecycle ───────────────────────────────────────────────────────

    async def close(self) -> None:
        self._wire.close()

    async def __aenter__(self) -> "AsyncConnection":
        return self

    async def __aexit__(self, et: Any, ev: Any, tb: Any) -> None:
        try:
            if not self.closed:
                if et is None:
                    await self.commit()
                else:
                    try:
                        await self.rollback()
                    except E.Error:
                        pass
        finally:
            await self.close()

    def __repr__(self) -> str:
        return (f"<particledb.aio.AsyncConnection {self._wire.transport_name} "
                f"{'closed' if self.closed else self.transaction_status}>")


_WIRE_OPTS = {"host", "port", "user", "password", "connect_timeout", "statement_timeout", "cancel_grace",
              "sslmode", "sslrootcert", "sslcert", "sslkey",
              "prefer_unix_socket", "unix_socket_dir", "unix_socket_path", "require_mux",
              "prepare_cache_size", "stream_buffer_limit", "use_pyarrow"}


async def connect(dsn: Optional[str] = None, **kwargs: Any) -> AsyncConnection:
    """``await particledb.aio.connect("pdb://host:7173", autocommit=True)`` --
    the same options as :func:`particledb.dbapi.connect`."""
    if dsn:
        for k, v in parse_dsn(dsn).items():
            kwargs.setdefault(k, v)
    opts = {k: kwargs.pop(k) for k in list(kwargs) if k in _DBAPI_OPTS}
    kwargs.pop("database", None)
    kwargs.pop("dbname", None)
    unknown = set(kwargs) - _WIRE_OPTS
    if unknown:
        raise E.ProgrammingError(f"connect(): unknown option(s) {sorted(unknown)}")
    try:
        wire = await open_wire(**kwargs)
    except BaseException as e:
        raise E.translate(e) from e
    try:
        return AsyncConnection(wire, **opts)
    except BaseException:
        wire.close()
        raise
