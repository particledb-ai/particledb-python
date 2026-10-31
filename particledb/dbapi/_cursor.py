"""DB-API 2.0 cursors: client-side (:class:`Cursor`) and streaming
server-side (:class:`ServerCursor`)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Iterable, Iterator, List, Optional, Sequence, Tuple

from .. import _protocol as P
from .. import errors as _ne
from . import _literal, _sql
from . import errors as E
from ._adapt import adapt_all
from ._result import FetchMixin, ResultSet, lastrowid_of
from ..connection import _raise_for as _raise

if TYPE_CHECKING:  # pragma: no cover
    from ._connection import Connection


class Cursor(FetchMixin):
    """A client-side cursor: :meth:`execute` receives the whole result (the
    server's Arrow bytes; rows are decoded on first fetch)."""

    def __init__(self, conn: "Connection", row_factory: Any = None):
        self._conn = conn
        self.row_factory = row_factory
        self.arraysize = 1
        self._closed = False
        self.lastrowid: Optional[Any] = None
        self._reset_results()

    # ── PEP 249 attributes ──────────────────────────────────────────────

    @property
    def connection(self) -> "Connection":
        return self._conn

    @property
    def closed(self) -> bool:
        return self._closed

    #: A streaming server-side cursor (``ServerCursor``) or not.
    server_side = False

    def setinputsizes(self, sizes: Any) -> None:
        """Accepted and ignored (PEP 249 allows it): parameters are typed by value."""

    def setoutputsize(self, size: Any, column: Any = None) -> None:
        """Accepted and ignored (PEP 249 allows it)."""

    def close(self) -> None:
        """Close the cursor: its rows are released; ``rowcount`` keeps its
        value (as psycopg's does: SQLAlchemy reads it after closing)."""
        self._closed = True
        for r in self._results:
            r.count = r.rowcount
            r.raw = None
            r.rows = []

    def _check(self) -> None:
        if self._closed:
            raise E.InterfaceError("the cursor is closed")
        self._conn._check()

    # ── execution ───────────────────────────────────────────────────────

    def _run(self, sql: str, vals: List[Any], sh: "_sql.Shape") -> ResultSet:
        conn = self._conn
        native = conn._native
        ext = native._extended
        pa = native._use_pyarrow
        try:
            if conn._needs_begin(sh.txn_control):
                with conn._begin_lock:
                    if conn._needs_begin(sh.txn_control):
                        if conn._plain_begin():
                            s = native._exec_raw(sql, vals, None, True)
                            return ResultSet.from_stream(s, sh.verb, ext, pa)
                        conn._control(conn._begin_sql())
            s = native._exec_raw(sql, vals, None, False)
            return ResultSet.from_stream(s, sh.verb, ext, pa)
        except BaseException as e:
            raise E.translate(e) from e

    def execute(self, query: str, params: Any = None, *, prepare: Optional[bool] = None) -> "Cursor":
        """Run one statement (``params``: a sequence for ``%s`` / ``?`` / ``$n``,
        a mapping for ``%(name)s``). A string of several statements without
        parameters runs as one request (atomically, like PostgreSQL's simple
        query) and :meth:`nextset` walks their results. Returns the cursor."""
        self._check()
        self._reset_results()
        self.lastrowid = None
        sql, vals = _sql.convert(query, params, self._conn.paramstyle)
        if vals:
            vals = adapt_all(vals)
        sh = _sql.shape(sql)
        if sh.statements is not None and not vals:
            self._load(self._run_script(sh))
        else:
            self._load([self._run(sql, vals, sh)])
        if sh.verb in _DDL_VERBS and self._conn._native.txn_status:
            self._conn._ddl_txn = True
        if sh.verb == "INSERT" and sh.returning:
            self.lastrowid = lastrowid_of(self._results)
        return self

    def _run_script(self, sh: "_sql.Shape") -> List[ResultSet]:
        """Several statements as ONE BATCH request. Autocommit: wrapped
        ``BEGIN .. COMMIT`` (PostgreSQL's implicit-transaction atomicity),
        holding the connection EXCLUSIVELY so no other thread's statement
        joins that hidden transaction; a script with its own transaction
        control is sent as written. Manual commit: it joins the open
        transaction, or opens one first (as psycopg's BEGIN does), unless it
        starts with BEGIN / START itself."""
        conn = self._conn
        native = conn._native
        stmts = list(sh.statements or [])
        if conn._autocommit:
            if not sh.txn_control:
                with native.exclusive():
                    if native.txn_status:
                        return self._send_script(sh, stmts, 0, 0)
                    try:
                        return self._send_script(sh, ["BEGIN"] + stmts + ["COMMIT"], 1, 1)
                    except BaseException as e:
                        self._end_hidden_txn(e)  # still exclusive: before anyone else sends
                        raise
            return self._send_script(sh, stmts, 0, 0)
        if native.txn_status or sh.verb in ("BEGIN", "START"):
            return self._send_script(sh, stmts, 0, 0)
        with conn._begin_lock:
            if native.txn_status:
                return self._send_script(sh, stmts, 0, 0)
            if conn._plain_begin():
                return self._send_script(sh, ["BEGIN"] + stmts, 1, 0)
            conn._control(conn._begin_sql())
        return self._send_script(sh, stmts, 0, 0)

    def _end_hidden_txn(self, exc: BaseException) -> None:
        """After a failed autocommit script: roll its hidden transaction back,
        or close the connection when that cannot be done (state unknown: an
        interrupt while the batch was still running)."""
        native = self._conn._native
        if native.closed:
            return
        if not isinstance(exc, Exception):
            native.close()
            return
        if not native.txn_status:
            return
        try:
            native._exec_raw("ROLLBACK", None, None)
        except BaseException:
            native.close()

    def _send_script(self, sh: "_sql.Shape", stmts: List[str], lead: int, trail: int) -> List[ResultSet]:
        conn = self._conn
        native = conn._native
        try:
            br = native._batch_raw(stmts)
        except BaseException as e:
            err = E.translate(e)
            if isinstance(e, _ne.BatchError):
                err.index = max(0, e.index - lead)  # type: ignore[attr-defined]
            if err is e:
                raise
            raise err from e
        ext = native._extended
        pa = native._use_pyarrow
        out: List[ResultSet] = []
        notices = getattr(br, "notices", [])
        entries = br.results[lead: len(br.results) - trail if trail else None]
        for i, entry in enumerate(entries):
            verb = _sql.shape(sh.statements[i]).verb if sh.statements else ""
            if isinstance(entry, (bytes, bytearray)):
                out.append(ResultSet.from_ipc(bytes(entry), notices, False, verb, ext, pa))
            else:
                out.append(ResultSet(None, [], int(entry), notices, False, verb, ext, pa, False))
        return out

    def executemany(self, query: str, params_seq: Iterable[Any], *, returning: bool = False) -> None:
        """Run ``query`` once per parameter set, in order. A plain ``INSERT
        INTO t [(cols)] VALUES (%s, ...)`` (no RETURNING, no ON CONFLICT, bare
        placeholders) is ONE native bulk ingest: atomic, every row or none
        (the JDBC ``executeBatch`` path; ``bulk_executemany=False`` turns it
        off). Anything else runs the server-side prepared statement once per
        set; the first failure raises (in autocommit mode the sets before it
        are committed). Without arrays, the sets travel as ``BATCH`` requests
        of rendered statements (in order, one round trip per 1000; JDBC's
        ``executeBatch`` text path; ``batch_executemany=False`` keeps one
        prepared execution per set). ``rowcount`` is the total affected; with
        ``returning=True`` each set's rows are a result :meth:`nextset` walks."""
        self._check()
        self._reset_results()
        self.lastrowid = None
        seq = params_seq if isinstance(params_seq, list) else list(params_seq)
        if not seq:
            self._load([ResultSet(None, [], 0, [], False, "", False, False, False)])
            return
        conn = self._conn
        sql, first = _sql.convert(query, seq[0], conn.paramstyle)
        sh = _sql.shape(sql)
        convert = _sql.convert
        style = conn.paramstyle
        rows = [adapt_all(first)] + [adapt_all(convert(query, p, style)[1]) for p in seq[1:]]
        if (sh.insert is not None and not returning and conn.bulk_executemany
                and conn._native._has(ext=P.EXT_BULK_INGEST) and not conn._ddl_in_open_txn()
                and _bulkable(rows)):
            rel, cols, n = sh.insert
            try:
                if cols is None:
                    allc = conn._table_columns(rel)
                    if n > len(allc):
                        raise E.ProgrammingError(f"INSERT has {n} values but {rel} has {len(allc)} columns")
                    cols = allc[:n]
                conn._ensure_txn()
                loaded = conn._native.bulk_insert(rel, cols, rows)
            except BaseException as e:
                raise E.translate(e) from e
            self._load([ResultSet(None, [], loaded, [], False, "INSERT", False, False, False)])
            return
        if not returning and conn.batch_executemany:
            stmts = _literal.render_all(sql, rows)
            if stmts is not None:
                self._run_batched(stmts, sh)
                return
        self._run_each(sql, rows, sh, returning)

    def _run_batched(self, stmts: List[str], sh: "_sql.Shape") -> None:
        """The sets as ``BATCH`` requests of rendered statements (in order, one
        round trip per chunk; the first failure stops it, raised with the
        set's ``index``)."""
        conn = self._conn
        native = conn._native
        total = 0
        base = 0
        try:
            for chunk in _literal.chunks(stmts):
                lead = 0
                try:
                    if conn._needs_begin(sh.txn_control):
                        with conn._begin_lock:
                            if conn._needs_begin(sh.txn_control):
                                if conn._plain_begin():
                                    chunk = ["BEGIN"] + chunk
                                    lead = 1
                                else:
                                    conn._control(conn._begin_sql())
                            br = native._batch_raw(chunk)
                    else:
                        br = native._batch_raw(chunk)
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

    def _run_each(self, sql: str, rows: List[List[Any]], sh: "_sql.Shape", returning: bool) -> None:
        conn = self._conn
        native = conn._native
        ext = native._extended
        pa = native._use_pyarrow
        results: List[ResultSet] = []
        total = 0
        try:
            if conn._needs_begin(sh.txn_control) and not conn._plain_begin():
                conn._control(conn._begin_sql())
            e = native._prepare_entry(sql, None)
            try:
                for vals in rows:
                    flags = P.FLAG_END_STREAM
                    if conn._needs_begin(sh.txn_control):
                        with conn._begin_lock:
                            if conn._needs_begin(sh.txn_control):
                                flags |= P.FLAG_TXN_BEGIN
                            s = native._execute_frame(e, vals, None, False, flags)
                            native._await(s, None)
                    else:
                        s = native._execute_frame(e, vals, None, False, flags)
                        native._await(s, None)
                    try:
                        _raise(s)
                    except _ne.QueryError as exc:
                        if exc.sqlstate != "26000":
                            raise
                        native._reprepare(sql, e)
                        s = native._execute_frame(e, vals, None, False, flags)
                        native._await(s, None)
                        _raise(s)
                    r = ResultSet.from_stream(s, sh.verb, ext, pa)
                    if r.has_rows:
                        if returning:
                            results.append(r)
                        total += r.rowcount
                    else:
                        total += r.count
            finally:
                native._unpin(e)
        except BaseException as exc:
            raise E.translate(exc) from exc
        if returning:
            self._load(results or [ResultSet(None, [], 0, [], False, sh.verb, ext, pa, False)])
            if sh.verb == "INSERT":
                self.lastrowid = lastrowid_of(results)
        else:
            self._load([ResultSet(None, [], total, [], False, sh.verb, ext, pa, False)])

    def callproc(self, procname: str, parameters: Sequence[Any] = ()) -> List[Any]:
        """Call a stored procedure (``CALL name(...)``) or a function
        (``SELECT name(...)``; its result is fetchable). Returns the
        parameters unchanged (OUT parameters are not supported)."""
        self._check()
        params = list(parameters)
        kind = self._conn._prokind.get(procname)
        if kind is None:
            bare = procname.split(".")[-1]
            bare = bare[1:-1].replace('""', '"') if bare.startswith('"') else bare.lower()
            c = Cursor(self._conn)
            c.execute("SELECT prokind FROM pg_catalog.pg_proc WHERE proname = $1", [bare])
            r = c.fetchone()
            kind = r[0] if r else "f"
            self._conn._prokind[procname] = kind
        ph = ", ".join(f"${i + 1}" for i in range(len(params)))
        verb = "CALL" if kind == "p" else "SELECT"
        conn = self._conn
        saved = conn.paramstyle
        try:
            conn.paramstyle = "numeric"
            self.execute(f"{verb} {procname}({ph})", params if params else None)
        finally:
            conn.paramstyle = saved
        return params

    def stream(self, query: str, params: Any = None) -> Iterator[Any]:
        """Iterate the rows of ``query`` in bounded memory as they arrive
        (psycopg's ``Cursor.stream``); the statement is cancelled if the
        iteration stops early."""
        sc = ServerCursor(self._conn, None, self.row_factory)
        sc.execute(query, params)
        try:
            yield from sc
        finally:
            sc.close()

    # ── fetching ────────────────────────────────────────────────────────

    def fetchone(self) -> Any:
        return self._fetchone()

    def fetchmany(self, size: Optional[int] = None) -> List[Any]:
        return self._fetchmany(size)

    def fetchall(self) -> List[Any]:
        return self._fetchall()

    def nextset(self) -> Optional[bool]:
        return self._nextset()

    def scroll(self, value: int, mode: str = "relative") -> None:
        self._scroll(value, mode)

    def __iter__(self) -> "Cursor":
        return self

    def __next__(self) -> Any:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def next(self) -> Any:
        return self.__next__()

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"<particledb.dbapi.Cursor {'closed' if self._closed else 'open'}>"


#: Statements after which the server's bulk ingest cannot see a table changed
#: in the same, still open transaction (it answers 42P01 and aborts it).
_DDL_VERBS = frozenset({"CREATE", "ALTER", "DROP", "TRUNCATE", "COMMENT"})


def _bulkable(rows: List[List[Any]]) -> bool:
    """Every value is one the bulk path carries exactly (arrays and nested
    values take the per-row path)."""
    for r in rows[:4096]:
        for v in r:
            if isinstance(v, (list, tuple, P.TypedParam)):
                return False
    return True


class ServerCursor(Cursor):
    """A named (server-side, psycopg-style) cursor: the result STREAMS, one
    Arrow record batch at a time, so a result of any size is read in bounded
    memory. Forward-only. Other statements on the connection keep running
    while it is open (each is its own stream). ``close()`` cancels the
    statement if rows are still coming. ``rowcount`` is the rows fetched so
    far until the result is exhausted, then the total."""

    server_side = True

    def __init__(self, conn: "Connection", name: Optional[str], row_factory: Any = None):
        super().__init__(conn, row_factory)
        self.name = name
        self.itersize = 2000
        self._stream: Optional[Any] = None
        self._desc: Optional[List[Any]] = None
        self._fetched = 0
        self._done = False

    def execute(self, query: str, params: Any = None, *, prepare: Optional[bool] = None) -> "ServerCursor":
        self._check()
        self._close_stream()
        conn = self._conn
        sql, vals = _sql.convert(query, params, conn.paramstyle)
        if vals:
            vals = adapt_all(vals)
        sh = _sql.shape(sql)
        if sh.statements is not None:
            raise E.ProgrammingError("a server-side cursor runs one statement")
        native = conn._native
        try:
            begin = False
            if conn._needs_begin(sh.txn_control):
                with conn._begin_lock:
                    if conn._needs_begin(sh.txn_control):
                        if conn._plain_begin():
                            begin = True
                        else:
                            conn._control(conn._begin_sql())
            self._stream = native._stream(sql, vals, None, begin=begin)
            fields = self._stream.fields
        except BaseException as e:
            raise E.translate(e) from e
        from . import _types

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

    @property
    def rownumber(self) -> Optional[int]:
        return self._fetched if self._stream is not None else None

    @property
    def messages(self) -> List[Any]:
        return list(self._stream.notices) if self._stream is not None else []

    def _next_row(self) -> Optional[Tuple[Any, ...]]:
        if self._closed:
            raise E.InterfaceError("the cursor is closed")
        if self._stream is None:
            raise E.ProgrammingError("no results to fetch: execute a statement first")
        if self._desc is None:
            raise E.ProgrammingError("the last operation didn't produce rows")
        if self._done:
            return None
        try:
            row = next(self._stream)
        except StopIteration:
            self._done = True
            return None
        except BaseException as e:
            self._done = True
            raise E.translate(e) from e
        self._fetched += 1
        return row

    def fetchone(self) -> Any:
        r = self._next_row()
        if r is None:
            return None
        return r if self._maker is None else self._maker(r)

    def fetchmany(self, size: Optional[int] = None) -> List[Any]:
        n = self.arraysize if size is None else size
        out = []
        for _ in range(n):
            r = self._next_row()
            if r is None:
                break
            out.append(r if self._maker is None else self._maker(r))
        return out

    def fetchall(self) -> List[Any]:
        out = []
        m = self._maker
        while True:
            r = self._next_row()
            if r is None:
                return out
            out.append(r if m is None else m(r))

    def scroll(self, value: int, mode: str = "relative") -> None:
        if mode == "absolute":
            value -= self._fetched
        elif mode != "relative":
            raise E.ProgrammingError(f"bad scroll mode {mode!r}: 'relative' or 'absolute'")
        if value < 0:
            raise E.NotSupportedError("a server-side cursor is forward-only: cannot scroll backwards")
        for _ in range(value):
            if self._next_row() is None:
                raise IndexError("position out of bound")

    def nextset(self) -> Optional[bool]:
        return None

    def _remaining_rows(self) -> Tuple[List[Tuple[Any, ...]], List[Any]]:
        rows = []
        while True:
            r = self._next_row()
            if r is None:
                break
            rows.append(r)
        return rows, self._stream._dec.schema.fields if self._stream._dec.schema else []

    def fetch_arrow(self) -> Any:
        from .. import _frames

        rows, fields = self._remaining_rows()
        return _frames.arrow_from_rows(rows, fields)

    def fetch_df(self) -> Any:
        from .. import _frames

        rows, fields = self._remaining_rows()
        return _frames.pandas_frame(None, rows, fields, True)

    def fetch_polars(self) -> Any:
        from .. import _frames

        rows, fields = self._remaining_rows()
        return _frames.polars_frame(None, rows, fields, True)

    def fetch_numpy(self) -> Any:
        from .. import _frames

        rows, fields = self._remaining_rows()
        return _frames.numpy_arrays(None, rows, fields, True)

    def _close_stream(self) -> None:
        if self._stream is not None:
            try:
                self._stream.close()
            except _ne.PdbError:
                pass
            self._stream = None

    def close(self) -> None:
        self._close_stream()
        super().close()

    def __repr__(self) -> str:
        return f"<particledb.dbapi.ServerCursor {self.name!r} {'closed' if self._closed else 'open'}>"
