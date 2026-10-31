"""asyncio cursors: ``await cur.execute(...)``, ``async for row in cur``."""

from __future__ import annotations


from typing import TYPE_CHECKING, Any, AsyncIterator, Iterable, List, Optional, Sequence

from .. import _protocol as P
from .. import errors as _ne
from ..connection import _raise_for
from ..dbapi import _literal, _sql
from ..dbapi import _types
from ..dbapi import errors as E
from ..dbapi._adapt import adapt_all
from ..dbapi._cursor import _DDL_VERBS, _bulkable
from ..dbapi._result import FetchMixin, ResultSet, lastrowid_of
from ._wire import AsyncRowStream

if TYPE_CHECKING:  # pragma: no cover
    from ._connection import AsyncConnection


class AsyncCursor(FetchMixin):
    """A client-side cursor on an :class:`AsyncConnection`. Fetch methods are
    coroutines (the result is already received, so they never block);
    ``fetch_df()`` / ``fetch_arrow()`` / ``fetch_polars()`` / ``fetch_numpy()``
    are plain methods."""

    def __init__(self, conn: "AsyncConnection", row_factory: Any = None):
        self._conn = conn
        self.row_factory = row_factory
        self.arraysize = 1
        self._closed = False
        self.lastrowid: Optional[Any] = None
        self._reset_results()

    @property
    def connection(self) -> "AsyncConnection":
        return self._conn

    @property
    def closed(self) -> bool:
        return self._closed

    server_side = False

    def setinputsizes(self, sizes: Any) -> None:
        pass

    def setoutputsize(self, size: Any, column: Any = None) -> None:
        pass

    async def close(self) -> None:
        self._closed = True
        for r in self._results:
            r.count = r.rowcount
            r.raw = None
            r.rows = []

    def _check(self) -> None:
        if self._closed:
            raise E.InterfaceError("the cursor is closed")
        self._conn._check()

    async def _run(self, sql: str, vals: List[Any], sh: "_sql.Shape") -> ResultSet:
        conn = self._conn
        w = conn._wire
        try:
            if conn._needs_begin(sh.txn_control):
                async with conn._begin_lock:
                    if conn._needs_begin(sh.txn_control):
                        if conn._plain_begin():
                            s = await w.exec_raw(sql, vals, None, True)
                            return ResultSet.from_stream(s, sh.verb, w.extended, w.use_pyarrow)
                        await conn._control(conn._begin_sql())
            s = await w.exec_raw(sql, vals, None, False)
            return ResultSet.from_stream(s, sh.verb, w.extended, w.use_pyarrow)
        except BaseException as e:
            raise E.translate(e) from e

    async def execute(self, query: str, params: Any = None) -> "AsyncCursor":
        self._check()
        self._reset_results()
        self.lastrowid = None
        sql, vals = _sql.convert(query, params, self._conn.paramstyle)
        if vals:
            vals = adapt_all(vals)
        sh = _sql.shape(sql)
        if sh.statements is not None and not vals:
            self._load(await self._run_script(sh))
        else:
            self._load([await self._run(sql, vals, sh)])
        if sh.verb in _DDL_VERBS and self._conn._wire.txn_status:
            self._conn._ddl_txn = True
        if sh.verb == "INSERT" and sh.returning:
            self.lastrowid = lastrowid_of(self._results)
        return self

    async def _run_script(self, sh: "_sql.Shape") -> List[ResultSet]:
        """See the blocking ``Cursor._run_script``: autocommit wraps the script
        in ``BEGIN .. COMMIT`` holding the connection exclusively; manual
        commit joins or opens the transaction."""
        conn = self._conn
        w = conn._wire
        stmts = list(sh.statements or [])
        if conn._autocommit:
            if not sh.txn_control:
                async with w.exclusive():
                    if w.txn_status:
                        return await self._send_script(sh, stmts, 0, 0)
                    try:
                        return await self._send_script(sh, ["BEGIN"] + stmts + ["COMMIT"], 1, 1)
                    except BaseException:
                        # Still holding the connection: end the hidden
                        # transaction (after a cancelled batch has answered)
                        # before anyone else may send. In THIS task (it owns
                        # the exclusive gate); a second cancel closes it.
                        try:
                            await self._end_hidden_txn()
                        except BaseException:
                            w.close()
                        raise
            return await self._send_script(sh, stmts, 0, 0)
        if w.txn_status or sh.verb in ("BEGIN", "START"):
            return await self._send_script(sh, stmts, 0, 0)
        async with conn._begin_lock:
            if w.txn_status:
                return await self._send_script(sh, stmts, 0, 0)
            if conn._plain_begin():
                return await self._send_script(sh, ["BEGIN"] + stmts, 1, 0)
            await conn._control(conn._begin_sql())
        return await self._send_script(sh, stmts, 0, 0)

    async def _end_hidden_txn(self) -> None:
        w = self._conn._wire
        try:
            if w.cancelling:
                await w._settle_cancelled()
            if w.txn_status and not w.closed:
                await w.exec_raw("ROLLBACK", None)
        except BaseException:
            # The hidden transaction's state is unknown: never let the
            # connection be reused under it.
            w.close()

    async def _send_script(self, sh: "_sql.Shape", stmts: List[str], lead: int, trail: int) -> List[ResultSet]:
        w = self._conn._wire
        try:
            br = await w.batch_raw(stmts)
        except BaseException as e:
            err = E.translate(e)
            if isinstance(e, _ne.BatchError):
                err.index = max(0, e.index - lead)  # type: ignore[attr-defined]
            if err is e:
                raise
            raise err from e
        out: List[ResultSet] = []
        notices = getattr(br, "notices", [])
        entries = br.results[lead: len(br.results) - trail if trail else None]
        for i, entry in enumerate(entries):
            verb = _sql.shape(sh.statements[i]).verb if sh.statements else ""
            if isinstance(entry, (bytes, bytearray)):
                out.append(ResultSet.from_ipc(bytes(entry), notices, False, verb, w.extended, w.use_pyarrow))
            else:
                out.append(ResultSet(None, [], int(entry), notices, False, verb, w.extended, w.use_pyarrow,
                                     False))
        return out

    async def executemany(self, query: str, params_seq: Iterable[Any], *, returning: bool = False) -> None:
        """See the blocking ``Cursor.executemany``: a plain INSERT is ONE
        atomic native bulk ingest, anything else one prepared execution per
        set, in order."""
        self._check()
        self._reset_results()
        self.lastrowid = None
        seq = params_seq if isinstance(params_seq, list) else list(params_seq)
        w = self._conn._wire
        if not seq:
            self._load([ResultSet(None, [], 0, [], False, "", False, False, False)])
            return
        conn = self._conn
        style = conn.paramstyle
        sql, first = _sql.convert(query, seq[0], style)
        sh = _sql.shape(sql)
        rows = [adapt_all(first)] + [adapt_all(_sql.convert(query, p, style)[1]) for p in seq[1:]]
        if (sh.insert is not None and not returning and conn.bulk_executemany
                and w.hello is not None and w.hello.has(ext=P.EXT_BULK_INGEST)
                and not conn._ddl_in_open_txn() and _bulkable(rows)):
            rel, cols, n = sh.insert
            try:
                if cols is None:
                    allc = await conn._table_columns(rel)
                    if n > len(allc):
                        raise E.ProgrammingError(f"INSERT has {n} values but {rel} has {len(allc)} columns")
                    cols = allc[:n]
                await conn._ensure_txn()
                loaded = await w.bulk_insert(rel, cols, rows)
            except BaseException as e:
                raise E.translate(e) from e
            self._load([ResultSet(None, [], loaded, [], False, "INSERT", False, False, False)])
            return
        if not returning and conn.batch_executemany:
            stmts = _literal.render_all(sql, rows)
            if stmts is not None:
                await self._run_batched(stmts, sh)
                return
        results: List[ResultSet] = []
        total = 0
        try:
            if conn._needs_begin(sh.txn_control) and not conn._plain_begin():
                await conn._control(conn._begin_sql())
            e = await w.prepare(sql)
            for vals in rows:
                flags = P.FLAG_END_STREAM
                if conn._needs_begin(sh.txn_control):
                    async with conn._begin_lock:
                        if conn._needs_begin(sh.txn_control):
                            flags |= P.FLAG_TXN_BEGIN
                        s = await w.execute_frame(e, vals, flags)
                        await w.wait(s)
                else:
                    s = await w.execute_frame(e, vals, flags)
                    await w.wait(s)
                try:
                    _raise_for(s)
                except _ne.QueryError as exc:
                    if exc.sqlstate != "26000":
                        raise
                    await w.reprepare(sql, e)
                    s = await w.execute_frame(e, vals, flags)
                    await w.wait(s)
                    _raise_for(s)
                r = ResultSet.from_stream(s, sh.verb, w.extended, w.use_pyarrow)
                if r.has_rows:
                    if returning:
                        results.append(r)
                    total += r.rowcount
                else:
                    total += r.count
        except BaseException as exc:
            raise E.translate(exc) from exc
        if returning:
            self._load(results or [ResultSet(None, [], 0, [], False, sh.verb, False, False, False)])
            if sh.verb == "INSERT":
                self.lastrowid = lastrowid_of(results)
        else:
            self._load([ResultSet(None, [], total, [], False, sh.verb, False, False, False)])

    async def _run_batched(self, stmts: List[str], sh: "_sql.Shape") -> None:
        conn = self._conn
        w = conn._wire
        total = 0
        base = 0
        try:
            for chunk in _literal.chunks(stmts):
                lead = 0
                try:
                    if conn._needs_begin(sh.txn_control):
                        async with conn._begin_lock:
                            if conn._needs_begin(sh.txn_control):
                                if conn._plain_begin():
                                    chunk = ["BEGIN"] + chunk
                                    lead = 1
                                else:
                                    await conn._control(conn._begin_sql())
                            br = await w.batch_raw(chunk)
                    else:
                        br = await w.batch_raw(chunk)
                except _ne.BatchError as e:
                    err = E.translate(e)
                    err.index = base + max(0, e.index - lead)  # type: ignore[attr-defined]
                    raise err from e
                total += sum(c for c in br.counts[lead:] if c > 0)
                base += len(chunk) - lead
        except E.Error:
            raise
        except BaseException as exc:
            raise E.translate(exc) from exc
        self._load([ResultSet(None, [], total, [], False, sh.verb, False, False, False)])

    async def callproc(self, procname: str, parameters: Sequence[Any] = ()) -> List[Any]:
        self._check()
        params = list(parameters)
        kind = self._conn._prokind.get(procname)
        if kind is None:
            bare = procname.split(".")[-1]
            bare = bare[1:-1].replace('""', '"') if bare.startswith('"') else bare.lower()
            c = AsyncCursor(self._conn)
            await c.execute("SELECT prokind FROM pg_catalog.pg_proc WHERE proname = $1", [bare])
            r = await c.fetchone()
            kind = r[0] if r else "f"
            self._conn._prokind[procname] = kind
        ph = ", ".join(f"${i + 1}" for i in range(len(params)))
        verb = "CALL" if kind == "p" else "SELECT"
        saved = self._conn.paramstyle
        try:
            self._conn.paramstyle = "numeric"
            await self.execute(f"{verb} {procname}({ph})", params if params else None)
        finally:
            self._conn.paramstyle = saved
        return params

    async def stream(self, query: str, params: Any = None) -> AsyncIterator[Any]:
        """Rows of ``query`` as they arrive, in bounded memory."""
        sc = AsyncServerCursor(self._conn, None, self.row_factory)
        await sc.execute(query, params)
        try:
            async for row in sc:
                yield row
        finally:
            await sc.close()

    async def fetchone(self) -> Any:
        return self._fetchone()

    async def fetchmany(self, size: Optional[int] = None) -> List[Any]:
        return self._fetchmany(size)

    async def fetchall(self) -> List[Any]:
        return self._fetchall()

    def nextset(self) -> Optional[bool]:
        return self._nextset()

    async def scroll(self, value: int, mode: str = "relative") -> None:
        self._scroll(value, mode)

    def __aiter__(self) -> "AsyncCursor":
        return self

    async def __anext__(self) -> Any:
        row = self._fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    async def __aenter__(self) -> "AsyncCursor":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()


class AsyncServerCursor(AsyncCursor):
    """A named (streaming) cursor: rows arrive one record batch at a time."""

    server_side = True

    def __init__(self, conn: "AsyncConnection", name: Optional[str], row_factory: Any = None):
        super().__init__(conn, row_factory)
        self.name = name
        self.itersize = 2000
        self._stream: Optional[AsyncRowStream] = None
        self._desc: Optional[List[Any]] = None
        self._fetched = 0
        self._done = False

    async def execute(self, query: str, params: Any = None) -> "AsyncServerCursor":
        self._check()
        self._close_stream()
        conn = self._conn
        sql, vals = _sql.convert(query, params, conn.paramstyle)
        if vals:
            vals = adapt_all(vals)
        sh = _sql.shape(sql)
        if sh.statements is not None:
            raise E.ProgrammingError("a server-side cursor runs one statement")
        w = conn._wire
        try:
            begin = False
            if conn._needs_begin(sh.txn_control):
                async with conn._begin_lock:
                    if conn._needs_begin(sh.txn_control):
                        if conn._plain_begin():
                            begin = True
                        else:
                            await conn._control(conn._begin_sql())
            s = await w.open_result_stream(sql, vals, begin)
            self._stream = AsyncRowStream(w, s)
            await self._stream.start()
            fields = await self._stream.fields()
        except BaseException as e:
            raise E.translate(e) from e
        self._desc = _types.description_for(fields) if fields else None
        self._fetched = 0
        self._done = self._desc is None
        self._maker = self.row_factory(self) if (self.row_factory is not None and self._desc) else None
        return self

    @property
    def description(self) -> Optional[List[Any]]:
        return self._desc

    @property
    def rowcount(self) -> int:
        if self._stream is None:
            return -1
        if self._desc is None:
            return self._stream.rowcount
        return self._fetched

    async def _next_row(self) -> Any:
        if self._closed:
            raise E.InterfaceError("the cursor is closed")
        if self._stream is None:
            raise E.ProgrammingError("no results to fetch: execute a statement first")
        if self._desc is None:
            raise E.ProgrammingError("the last operation didn't produce rows")
        if self._done:
            return None
        try:
            row = await self._stream.next()
        except BaseException as e:
            self._done = True
            raise E.translate(e) from e
        if row is None:
            self._done = True
            return None
        self._fetched += 1
        return row

    async def fetchone(self) -> Any:
        r = await self._next_row()
        if r is None:
            return None
        return r if self._maker is None else self._maker(r)

    async def fetchmany(self, size: Optional[int] = None) -> List[Any]:
        n = self.arraysize if size is None else size
        out = []
        for _ in range(n):
            r = await self.fetchone()
            if r is None:
                break
            out.append(r)
        return out

    async def fetchall(self) -> List[Any]:
        out = []
        while True:
            r = await self.fetchone()
            if r is None:
                return out
            out.append(r)

    async def __anext__(self) -> Any:
        r = await self.fetchone()
        if r is None:
            raise StopAsyncIteration
        return r

    def nextset(self) -> Optional[bool]:
        return None

    def _close_stream(self) -> None:
        if self._stream is not None:
            self._stream.close()
            self._stream = None

    async def close(self) -> None:
        self._close_stream()
        await super().close()
