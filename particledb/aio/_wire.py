"""The native wire, protocol 2, on asyncio: one socket, many coroutines.

An :class:`asyncio.Protocol` reads frames as they arrive (``data_received``)
and completes the future of the stream each belongs to, so any number of
coroutines can have statements in flight on ONE connection at once -- the
multiplexing the sync :class:`particledb.Connection` does with its read role,
without a thread. The bytes are the same: every codec is
``particledb._protocol`` / ``particledb._arrow``, shared with the sync client.

Cancellation is the server's: cancelling the task that awaits a statement
(``asyncio.CancelledError``, ``asyncio.timeout``) resets the statement's
stream (``RST_STREAM(CANCEL)``), so the server stops it and the connection
stays usable.
"""

from __future__ import annotations

import asyncio
import os
import struct
from collections import OrderedDict, deque
from typing import Any, Deque, Dict, Iterable, List, Optional, Sequence

from .. import _arrow
from .. import _protocol as P
from .. import _tls
from ..connection import (
    DEFAULT_CANCEL_GRACE,
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_PORT,
    DEFAULT_PREPARE_CACHE,
    DEFAULT_STREAM_BUFFER,
    DEFAULT_UNIX_SOCKET_DIR,
    Notice,
    _raise_for,
    is_local_host,
    unix_socket_path as _sock_path,
)
from ..errors import (
    ConnectionError,
    PdbError,
    ProtocolError,
    QueryCanceledError,
    QueryError,
    _ConnectFailed,
)

_CANCELLABLE = (P.F_QUERY, P.F_EXECUTE, P.F_BATCH)
_STATUS_FRAMES = (P.F_QUERY, P.F_EXECUTE, P.F_BATCH, P.F_BULK)


class AStream:
    """One request's stream (the attributes ``_raise_for`` and the DB-API
    ``ResultSet`` read: ``kind``, ``flags``, ``payload``, ``chunks``,
    ``notices``, ``error``)."""

    __slots__ = ("sid", "ftype", "fut", "kind", "flags", "payload", "chunks", "notices", "error",
                 "done", "attached", "queue", "qbytes", "event", "discard", "ignore")

    def __init__(self, sid: int, ftype: int, loop: asyncio.AbstractEventLoop, attached: bool):
        self.sid = sid
        self.ftype = ftype
        self.fut: asyncio.Future = loop.create_future()
        self.kind = 0
        self.flags = 0
        self.payload = b""
        self.chunks: List[bytes] = []
        self.notices: List[Notice] = []
        self.error: Optional[BaseException] = None
        self.done = False
        self.attached = attached
        self.queue: Deque[bytes] = deque()
        self.qbytes = 0
        self.event = asyncio.Event() if attached else None
        self.discard = False
        self.ignore = False


class _Prep:
    __slots__ = ("name", "name_b", "nparams", "ready")

    def __init__(self, name: str, loop: asyncio.AbstractEventLoop):
        self.name = name
        self.name_b = name.encode("utf-8")
        self.nparams = -1
        self.ready: asyncio.Future = loop.create_future()


class AsyncWire(asyncio.Protocol):
    """The connection: transport, frame reader, stream table."""

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.transport: Optional[asyncio.Transport] = None
        self.buf = bytearray()
        self.pos = 0
        self.v2 = False
        self.hello_fut: asyncio.Future = loop.create_future()
        self.hello: Optional[P.HelloOk] = None
        self.streams: Dict[int, AStream] = {}
        self.next_sid = 1
        self.closed = False
        self.close_cause: Optional[BaseException] = None
        self.txn_status = 0
        self.slots: Optional[asyncio.Semaphore] = None
        self.cache: "OrderedDict[str, _Prep]" = OrderedDict()
        self.cache_size = DEFAULT_PREPARE_CACHE
        self.stmt_seq = 0
        self.statement_timeout: Optional[float] = None
        self.cancel_grace = DEFAULT_CANCEL_GRACE
        self.stream_buffer_limit = DEFAULT_STREAM_BUFFER
        self.notice_handler: Any = None
        self.extended = True
        self.use_pyarrow = False
        self.transport_name = "?"
        self.bulk_lock = asyncio.Lock()
        # Statements whose awaiting task was cancelled: reset sent, answer not
        # yet in. New requests wait for them (bounded by cancel_grace) so the
        # session's transaction state is known before anything else is sent.
        self.cancelling: set = set()
        # exclusive(): while a task holds it, no OTHER task opens a stream.
        self.excl_owner: Any = None
        self.excl_depth = 0
        self.excl_free = asyncio.Event()
        self.excl_free.set()

    # ── asyncio.Protocol ────────────────────────────────────────────────

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]
        sock = transport.get_extra_info("socket")
        if sock is not None:
            try:
                import socket

                if sock.family in (socket.AF_INET, socket.AF_INET6):
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass

    def data_received(self, data: bytes) -> None:
        if self.pos == len(self.buf):
            self.buf = bytearray(data)
            self.pos = 0
        else:
            self.buf += data
        buf = self.buf
        try:
            while True:
                avail = len(buf) - self.pos
                if not self.v2:
                    if avail < P.V1_HEADER:
                        break
                    n, _sid, t, _f = P.parse_v1_header(bytes(buf[self.pos: self.pos + P.V1_HEADER]))
                    if avail < P.V1_HEADER + n:
                        break
                    payload = bytes(buf[self.pos + P.V1_HEADER: self.pos + P.V1_HEADER + n])
                    self.pos += P.V1_HEADER + n
                    self.v2 = True
                    if not self.hello_fut.done():
                        self.hello_fut.set_result((t, payload))
                    continue
                if avail < P.V2_HEADER:
                    break
                t, flags, sid, n = P.parse_v2_header(buf, self.pos)
                if n > P.MAX_PAYLOAD:
                    raise ProtocolError(f"PDB native wire: frame of {n} bytes exceeds {P.MAX_PAYLOAD}")
                if avail < P.V2_HEADER + n:
                    break
                start = self.pos + P.V2_HEADER
                payload = bytes(buf[start: start + n])
                self.pos = start + n
                self._dispatch(t, flags, sid, payload)
        except ValueError as e:
            self.fail_all(ProtocolError(f"PDB native wire: {e}"))
            return
        except ProtocolError as e:
            self.fail_all(e)
            return
        if self.pos == len(self.buf):
            self.buf = bytearray()
            self.pos = 0
        elif self.pos > (1 << 20):
            del self.buf[: self.pos]
            self.pos = 0

    def connection_lost(self, exc: Optional[BaseException]) -> None:
        self.fail_all(ConnectionError(f"connection lost: {exc}" if exc else "connection closed by the server",
                                      0, "08006"))

    # ── dispatch ────────────────────────────────────────────────────────

    def _dispatch(self, t: int, flags: int, sid: int, payload: bytes) -> None:
        if t == P.F_GOAWAY:
            self.fail_all(ConnectionError("the server closed the connection (GOAWAY)", 0, "08006"))
            return
        s = self.streams.get(sid)
        if s is None:
            return
        if t == P.F_NOTICE:
            ns = P.decode_notices(payload)
            if ns:
                notices = [Notice(*n) for n in ns]
                s.notices.extend(notices)
                if self.notice_handler is not None:
                    for n in notices:
                        try:
                            self.notice_handler(n)
                        except Exception:  # pragma: no cover - a handler bug must not break the reader
                            pass
            return
        if t == P.F_ARROW_STREAM and flags & P.FLAG_MORE_CHUNKS:
            self._take_chunk(s, payload)
            if s.ftype in _STATUS_FRAMES and self.hello is not None and self.hello.has(P.CAP_PREPARED):
                self.txn_status = flags & (P.FLAG_TXN_OPEN | P.FLAG_TXN_FAILED)
            return
        if t == P.F_ARROW_STREAM:
            self._take_chunk(s, payload)
        else:
            s.payload = payload
        s.kind = t
        s.flags = flags
        s.done = True
        if s.ftype in _STATUS_FRAMES and self.hello is not None and self.hello.has(P.CAP_PREPARED):
            self.txn_status = flags & (P.FLAG_TXN_OPEN | P.FLAG_TXN_FAILED)
        del self.streams[sid]
        if self.slots is not None:
            self.slots.release()
        if s.event is not None:
            s.event.set()
        if not s.fut.done():
            s.fut.set_result(None)

    def _take_chunk(self, s: AStream, payload: bytes) -> None:
        if not s.attached:
            s.chunks.append(payload)
            return
        if s.discard:
            return
        s.queue.append(payload)
        s.qbytes += len(payload)
        s.event.set()  # type: ignore[union-attr]
        if s.qbytes > self.stream_buffer_limit:
            s.discard = True
            s.queue.clear()
            s.qbytes = 0
            s.error = QueryError(f"streamed result dropped: its consumer fell more than "
                                 f"{self.stream_buffer_limit} bytes behind", 0, "53200")
            self.reset(s)

    def fail_all(self, err: BaseException) -> None:
        if not self.closed:
            self.closed = True
            self.close_cause = err
        for s in list(self.streams.values()):
            s.error = err
            s.done = True
            if s.event is not None:
                s.event.set()
            if not s.fut.done():
                s.fut.set_result(None)
        n = len(self.streams)
        self.streams.clear()
        if self.slots is not None:
            for _ in range(n):
                self.slots.release()
        if not self.hello_fut.done():
            self.hello_fut.set_exception(err)
        if self.transport is not None:
            self.transport.abort()

    # ── requests ────────────────────────────────────────────────────────

    def check_open(self) -> None:
        if self.closed:
            c = self.close_cause
            raise ConnectionError(f"connection is closed{': ' + str(c) if c else ''}", 0, "08003")

    async def open_stream(self, ftype: int, attached: bool = False) -> AStream:
        me = asyncio.current_task()
        while True:
            self.check_open()
            if self.cancelling:
                await self._settle_cancelled()
                continue
            if self.excl_owner is not None and self.excl_owner is not me:
                await self.excl_free.wait()
                continue
            if self.slots is None:
                break
            await self.slots.acquire()
            # Re-check after the wait: the gate may have closed meanwhile.
            # Nothing below awaits, so the check and the registration are atomic.
            if self.closed or self.cancelling or (self.excl_owner is not None and self.excl_owner is not me):
                self.slots.release()
                continue
            break
        sid = self.next_sid
        self.next_sid += 2
        s = AStream(sid, ftype, self.loop, attached)
        self.streams[sid] = s
        return s

    async def _settle_cancelled(self) -> None:
        for s in list(self.cancelling):
            if not s.done:
                try:
                    await asyncio.wait_for(asyncio.shield(s.fut), self.cancel_grace)
                except asyncio.TimeoutError:
                    self._ambiguous_close()
            self.cancelling.discard(s)
        self.check_open()

    def _ambiguous_close(self) -> None:
        self.fail_all(QueryCanceledError(
            f"a cancelled statement did not answer its reset within {self.cancel_grace}s; "
            "outcome unknown, connection closed", 0, "57014", ambiguous=True))

    def abandon(self, s: AStream, block_next: bool) -> None:
        """The awaiting task gave up on ``s`` (cancelled, or a stream closed
        early): reset it, and close the connection if it is not answered
        within ``cancel_grace`` (its slot and outcome would otherwise stay
        unresolved). ``block_next``: later requests wait for its answer."""
        if s.done or self.closed:
            return
        self.reset(s)
        if block_next:
            self.cancelling.add(s)

        async def reap() -> None:
            try:
                await asyncio.wait_for(asyncio.shield(s.fut), self.cancel_grace)
            except asyncio.TimeoutError:
                self._ambiguous_close()
            finally:
                self.cancelling.discard(s)

        self.loop.create_task(reap())

    class _Exclusive:
        def __init__(self, w: "AsyncWire"):
            self.w = w

        async def __aenter__(self) -> None:
            w = self.w
            me = asyncio.current_task()
            while w.excl_owner is not None and w.excl_owner is not me:
                await w.excl_free.wait()
            w.excl_owner = me
            w.excl_depth += 1
            w.excl_free.clear()

        async def __aexit__(self, *exc: Any) -> None:
            w = self.w
            w.excl_depth -= 1
            if w.excl_depth == 0:
                w.excl_owner = None
                w.excl_free.set()

    def exclusive(self) -> "AsyncWire._Exclusive":
        """``async with wire.exclusive():`` no other task opens a stream meanwhile."""
        return AsyncWire._Exclusive(self)

    def send(self, frame: bytes) -> None:
        self.check_open()
        assert self.transport is not None
        self.transport.write(frame)

    async def request(self, ftype: int, payload: bytes, flags: int = P.FLAG_END_STREAM,
                      attached: bool = False) -> AStream:
        if len(payload) > P.MAX_PAYLOAD:
            raise ValueError(f"frame payload of {len(payload)} bytes exceeds the server's "
                             f"{P.MAX_PAYLOAD}-byte cap")
        s = await self.open_stream(ftype, attached)
        try:
            self.send(P.v2_frame(ftype, s.sid, payload, flags))
        except BaseException:
            self.streams.pop(s.sid, None)
            if self.slots is not None:
                self.slots.release()
            raise
        return s

    def reset(self, s: AStream) -> bool:
        if s.done or self.closed or s.ftype not in _CANCELLABLE:
            return False
        try:
            self.send(P.v2_frame(P.F_RST_STREAM, s.sid, P.rst_payload(P.ERR_CANCEL), 0))
            return True
        except PdbError:
            return False

    def cancel_all(self) -> int:
        live = [s for s in self.streams.values() if not s.done and s.ftype in _CANCELLABLE]
        return sum(1 for s in live if self.reset(s))

    async def wait(self, s: AStream, timeout: Optional[float] = None) -> None:
        """Wait for ``s``'s answer. A timeout, or the awaiting task being
        cancelled, resets the stream (the server answers 57014); a statement
        that does not answer within the grace period closes the connection
        (outcome unknown)."""
        if s.done:
            return
        if timeout is None:
            timeout = self.statement_timeout
        try:
            try:
                if timeout is None:
                    # Awaited directly (no shield: one loop iteration less per
                    # statement); a cancel cancels the future, re-armed below.
                    await s.fut
                else:
                    await asyncio.wait_for(asyncio.shield(s.fut), timeout)
            except asyncio.TimeoutError:
                await self._timed_out(s, timeout)
        except asyncio.CancelledError:
            # Cancelled while waiting for the answer OR during a timeout's
            # reset grace: either way the stream is abandoned (reset, reaped,
            # and later requests wait for its answer).
            self._rearm(s)
            self.abandon(s, block_next=True)
            raise

    def _rearm(self, s: AStream) -> None:
        """A fresh future for an unanswered stream whose future the cancelled
        waiter took down with it (the reaper and the settle step await it)."""
        if not s.done and s.fut.done():
            s.fut = self.loop.create_future()

    async def _timed_out(self, s: AStream, timeout: Optional[float]) -> None:
        if s.done:
            return
        if self.reset(s):
            try:
                await asyncio.wait_for(asyncio.shield(s.fut), self.cancel_grace)
                return
            except asyncio.TimeoutError:
                pass
        if s.done:
            return
        err = QueryCanceledError(
            f"statement did not answer within {timeout}s nor its cancel within "
            f"{self.cancel_grace}s; outcome unknown, connection closed", 0, "57014", ambiguous=True)
        self.fail_all(err)
        raise err

    async def call(self, ftype: int, payload: bytes, flags: int = P.FLAG_END_STREAM,
                   timeout: Optional[float] = None) -> AStream:
        s = await self.request(ftype, payload, flags)
        await self.wait(s, timeout)
        _raise_for(s)
        return s

    # ── prepared statements ─────────────────────────────────────────────

    async def prepare(self, sql: str) -> _Prep:
        e = self.cache.get(sql)
        if e is not None:
            self.cache.move_to_end(sql)
            if not e.ready.done():
                await asyncio.shield(e.ready)
            if e.ready.exception() is None:
                return e
            raise e.ready.exception()  # type: ignore[misc]
        if not (self.hello and self.hello.has(P.CAP_PREPARED)):
            raise QueryError("the server did not negotiate prepared statements", 0, "0A000")
        self.stmt_seq += 1
        e = _Prep(f"pya_{self.stmt_seq}", self.loop)
        self.cache[sql] = e
        try:
            s = await self.call(P.F_PREPARE, P.prepare_payload(e.name, sql))
            if s.kind != P.F_PREPARE_OK or len(s.payload) < 2:
                raise ProtocolError(f"PREPARE answered with 0x{s.kind:02x}")
            e.nparams = struct.unpack_from("<H", s.payload, 0)[0]
        except BaseException as exc:
            # Any failure, cancellation included: the entry must not stay
            # pending (a later prepare of the same text would wait forever).
            if self.cache.get(sql) is e:
                del self.cache[sql]
            if not e.ready.done():
                e.ready.set_exception(exc if isinstance(exc, Exception) else
                                      QueryError("prepare was cancelled", 0, "57014"))
                e.ready.exception()  # mark retrieved
            raise
        e.ready.set_result(None)
        self._evict(e)
        return e

    def _evict(self, keep: _Prep) -> None:
        """Drop least-recently-used ready entries past the cache size; their
        CLOSE_STMT is sent in the background (nothing here awaits)."""
        excess = len(self.cache) - self.cache_size
        if excess <= 0:
            return
        for k, old in list(self.cache.items()):
            if excess <= 0:
                break
            if old is keep or not old.ready.done():
                continue
            del self.cache[k]
            excess -= 1
            if old.ready.exception() is None and not self.closed:
                self.loop.create_task(self._close_stmt(old))

    async def _close_stmt(self, e: _Prep) -> None:
        try:
            st = await self.request(P.F_CLOSE_STMT, P.name_payload(e.name))
            st.ignore = True
        except PdbError:
            pass

    async def reprepare(self, sql: str, e: _Prep) -> None:
        s = await self.request(P.F_PREPARE, P.prepare_payload(e.name, sql))
        await self.wait(s)
        try:
            _raise_for(s)
        except QueryError as exc:
            if exc.sqlstate == "42P05":
                return
            raise
        e.nparams = struct.unpack_from("<H", s.payload, 0)[0]

    async def execute_frame(self, e: _Prep, params: List[Any], flags: int = P.FLAG_END_STREAM,
                            attached: bool = False, encoded: Optional[bytes] = None) -> AStream:
        if e.nparams != len(params):
            raise QueryError(f"statement expects {e.nparams} parameter(s), {len(params)} given", 0, "08P01")
        need = P.needs_ext(params)
        have = self.hello.ext if self.hello else 0
        if need & ~have:
            raise QueryError("a parameter needs a capability the server did not negotiate "
                             f"(ext bits 0x{need & ~have:02x})", 0, "0A000")
        payload = P.execute_payload(e.name_b, P.encode_params(params) if encoded is None else encoded,
                                    len(params))
        return await self.request(P.F_EXECUTE, payload, flags, attached)

    async def exec_raw(self, sql: str, params: Optional[Sequence[Any]], timeout: Optional[float] = None,
                       begin: bool = False) -> AStream:
        """One statement's answered stream (see the sync ``_exec_raw``)."""
        flags = P.FLAG_END_STREAM | P.FLAG_TXN_BEGIN if begin else P.FLAG_END_STREAM
        if not params:
            return await self.call(P.F_QUERY, sql.encode("utf-8"), flags, timeout)
        params = list(params)
        e = await self.prepare(sql)
        s = await self.execute_frame(e, params, flags)
        await self.wait(s, timeout)
        try:
            _raise_for(s)
            return s
        except QueryError as exc:
            if exc.sqlstate != "26000":
                raise
        await self.reprepare(sql, e)
        s = await self.execute_frame(e, params, flags)
        await self.wait(s, timeout)
        _raise_for(s)
        return s

    async def open_result_stream(self, sql: str, params: Sequence[Any], begin: bool = False) -> AStream:
        flags = P.FLAG_END_STREAM | P.FLAG_TXN_BEGIN if begin else P.FLAG_END_STREAM
        if params:
            e = await self.prepare(sql)
            return await self.execute_frame(e, list(params), flags, attached=True)
        return await self.request(P.F_QUERY, sql.encode("utf-8"), flags, attached=True)

    async def batch_raw(self, sqls: Sequence[str], timeout: Optional[float] = None) -> "P.BatchResult":
        from ..errors import BatchError

        s = await self.call(P.F_BATCH, P.batch_payload(list(sqls)), P.FLAG_END_STREAM, timeout)
        if s.kind != P.F_BATCH_RESULT:
            raise ProtocolError(f"BATCH answered with 0x{s.kind:02x}")
        try:
            br = P.decode_batch_result(s.payload)
        except ValueError as e:
            raise ProtocolError(f"malformed BATCH_RESULT: {e}") from e
        if br.error is not None:
            if br.error_sqlstate:
                state, text = br.error_sqlstate, P.server_error(br.error_code, br.error)[1]
            else:
                state, text = P.server_error(P.ERR_GENERIC_SQL, br.error)
            idx = len(br.counts)
            raise BatchError(f"batch statement {idx} failed: {text}", br.error_code, state, idx, br.counts)
        if br.entries != len(sqls):
            raise ProtocolError(f"BATCH_RESULT has {br.entries} entries for {len(sqls)} statements and no error")
        br.notices = s.notices
        return br

    # ── bulk ingest ─────────────────────────────────────────────────────

    async def bulk_load_ipc(self, table: str, columns: Sequence[str], chunks: Iterable[bytes],
                            timeout: Optional[float] = None, window: int = 4) -> int:
        if not (self.hello and self.hello.has(ext=P.EXT_BULK_INGEST)):
            raise QueryError("the server did not negotiate bulk ingest", 0, "0A000")
        it = iter(chunks)
        first = next(it, None)
        if first is None:
            return 0
        async with self.bulk_lock:
            async def wait_ok(s: AStream) -> int:
                await self.wait(s, timeout)
                _raise_for(s)
                return P.decode_ok(s.payload)

            await wait_ok(await self.request(P.F_BULK, P.bulk_begin_payload(table, list(columns))))
            inflight: Deque[AStream] = deque()
            sent = 0
            try:
                c: Optional[bytes] = first
                while c is not None:
                    if len(c) + 16 > P.MAX_PAYLOAD:
                        raise ValueError(f"a bulk DATA chunk of {len(c)} bytes exceeds the frame cap")
                    inflight.append(await self.request(P.F_BULK, P.bulk_data_payload(c)))
                    sent += 1
                    while len(inflight) >= window:
                        await wait_ok(inflight.popleft())
                    c = next(it, None)
                end = await self.request(P.F_BULK, P.bulk_end_payload(sent))
                inflight.append(end)  # drained with the DATA frames on a failure
                while len(inflight) > 1:
                    await wait_ok(inflight.popleft())
                return await wait_ok(inflight.popleft())
            except BaseException:
                for s in inflight:
                    try:
                        await self.wait(s, timeout)
                    except PdbError:
                        break
                if not self.closed:
                    try:
                        await wait_ok(await self.request(P.F_BULK, bytes([P.BULK_ABORT])))
                    except PdbError:
                        pass
                raise

    async def bulk_insert(self, table: str, columns: Sequence[str], rows: Iterable[Sequence[Any]],
                          timeout: Optional[float] = None) -> int:
        from ..arrow_ipc import encode_param_stream, infer_column_type
        from ..connection import BULK_CHUNK_ROWS, _encode_rows, _row_chunks

        cols = list(columns)
        n = len(cols)
        count = [0]

        def chunks() -> Any:
            for part in _row_chunks(rows, BULK_CHUNK_ROWS):
                for i, r in enumerate(part):
                    if len(r) != n:
                        raise ValueError(f"row {count[0] + i} has {len(r)} values, expected {n}")
                yield from _encode_rows(part, cols, encode_param_stream, infer_column_type)
                count[0] += len(part)

        loaded = await self.bulk_load_ipc(table, cols, chunks(), timeout)
        if loaded != count[0]:
            raise ProtocolError(f"bulk ingest loaded {loaded} of {count[0]} rows")
        return loaded

    # ── KV frames ───────────────────────────────────────────────────────

    async def kv(self, ftype: int, key: Any, value: Optional[bytes] = None,
                 timeout: Optional[float] = None) -> AStream:
        from ..connection import _raise_kv

        k = key.encode("utf-8") if isinstance(key, str) else bytes(key)
        s = await self.request(ftype, P.kv_payload(k, value))
        t = self.statement_timeout if timeout is None else timeout
        try:
            if t is None:
                await s.fut
            else:
                await asyncio.wait_for(asyncio.shield(s.fut), t)
        except asyncio.TimeoutError:
            err = QueryCanceledError(f"KV request did not answer within {t}s; outcome unknown, "
                                     "connection closed", 0, "57014", ambiguous=True)
            self.fail_all(err)
            raise err from None
        except asyncio.CancelledError:
            # A KV request cannot be reset: reap it (the connection closes if
            # it does not answer within cancel_grace, freeing its slot).
            self._rearm(s)
            self.abandon(s, block_next=False)
            raise
        _raise_kv(s)
        return s

    # ── lifecycle ───────────────────────────────────────────────────────

    async def ping(self, timeout: float = 30.0) -> float:
        t0 = self.loop.time()
        self.check_open()
        s = AStream(0, P.F_PING, self.loop, False)
        self.streams[0] = s
        if self.slots is not None:
            await self.slots.acquire()
        self.send(P.v2_frame(P.F_PING, 0, b""))
        await self.wait(s, timeout)
        _raise_for(s)
        return (self.loop.time() - t0) * 1000.0

    def close(self) -> None:
        if self.closed:
            return
        try:
            if self.transport is not None and not self.transport.is_closing():
                self.transport.write(P.v2_frame(P.F_GOAWAY, 0, P.goaway_payload(self.next_sid), 0))
        except Exception:
            pass
        self.fail_all(ConnectionError("connection closed", 0, "08003"))


async def open_wire(host: str = "localhost", port: Optional[int] = None, *, user: Optional[str] = None,
                    password: Optional[str] = None, connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
                    sslmode: str = "disable", sslrootcert: Optional[str] = None,
                    sslcert: Optional[str] = None, sslkey: Optional[str] = None,
                    statement_timeout: Optional[float] = None, cancel_grace: float = DEFAULT_CANCEL_GRACE,
                    prefer_unix_socket: bool = True, unix_socket_dir: str = DEFAULT_UNIX_SOCKET_DIR,
                    unix_socket_path: Optional[str] = None, require_mux: bool = True,
                    prepare_cache_size: int = DEFAULT_PREPARE_CACHE,
                    stream_buffer_limit: int = DEFAULT_STREAM_BUFFER, notice_handler: Any = None,
                    use_pyarrow: Optional[bool] = None, extended_types: bool = True,
                    _caps: int = P.DEFAULT_CAPS, _ext: int = P.DEFAULT_EXT) -> AsyncWire:
    """Connect, HELLO (protocol 2, every capability; fail closed), AUTH."""
    loop = asyncio.get_running_loop()
    tls_context = _tls.context(sslmode, sslrootcert, sslcert, sslkey)
    if tls_context is not None:
        if unix_socket_path:
            raise ValueError("TLS requires TCP; remove unix_socket_path")
        prefer_unix_socket = False
    port = DEFAULT_PORT if port is None else int(port)
    w: Optional[AsyncWire] = None
    name = ""
    if prefer_unix_socket and hasattr(asyncio, "open_unix_connection") and (unix_socket_path or is_local_host(host)):
        path = unix_socket_path or _sock_path(port, unix_socket_dir)
        if unix_socket_path or os.path.exists(path):
            try:
                _t, w = await asyncio.wait_for(
                    loop.create_unix_connection(lambda: AsyncWire(loop), path), connect_timeout)
                name = f"unix:{path}"
            except (FileNotFoundError, ConnectionRefusedError) as e:
                if unix_socket_path:
                    raise _ConnectFailed(f"unix_socket_path {unix_socket_path}: {e}") from e
                w = None
            except (OSError, asyncio.TimeoutError) as e:
                raise _ConnectFailed(f"PDB native wire: Unix socket {path} exists but connecting failed "
                                     f"({e}); not falling back to TCP") from e
    if w is None:
        try:
            _t, w = await asyncio.wait_for(
                loop.create_connection(lambda: AsyncWire(loop), host.strip("[]"), port), connect_timeout)
        except (OSError, asyncio.TimeoutError) as e:
            raise _ConnectFailed(f"cannot connect to {host}:{port}: {e}") from e
        name = f"tcp:{host}:{port}"
    assert w is not None
    w.transport_name = name
    w.statement_timeout = statement_timeout
    w.cancel_grace = cancel_grace
    w.cache_size = max(1, prepare_cache_size)
    w.stream_buffer_limit = stream_buffer_limit
    w.notice_handler = notice_handler
    w.extended = extended_types
    w.use_pyarrow = (_arrow.USE_PYARROW if use_pyarrow is None else bool(use_pyarrow)) and _arrow.HAVE_PYARROW
    try:
        w.transport.write(P.encode_hello(_caps | (P.CAP_TLS if tls_context else 0), _ext))  # type: ignore[union-attr]
        try:
            t, payload = await asyncio.wait_for(asyncio.shield(w.hello_fut), connect_timeout)
        except asyncio.TimeoutError:
            raise _ConnectFailed("PDB native wire: no HELLO_OK within the connect timeout") from None
        except ConnectionError as e:
            raise _ConnectFailed(f"PDB native wire: handshake failed: {e}") from e
        if t == P.F_ERROR:
            code, msg = P.decode_error(payload)
            state, text = P.server_error(code, msg)
            raise ConnectionError(f"PDB native wire: the server refused the handshake: {text}", code, state)
        if t != P.F_HELLO_OK:
            raise ProtocolError(f"PDB native wire: expected HELLO_OK, got 0x{t:02x}")
        h = P.HelloOk.parse(payload)
        if tls_context is not None:
            if not h.caps & P.CAP_TLS:
                raise ConnectionError("server did not accept TLS; refusing plaintext", 0, "08006")
            try:
                secured = await loop.start_tls(w.transport, w, tls_context, server_hostname=host.strip("[]"),
                                               ssl_handshake_timeout=connect_timeout)
                if secured is None:
                    raise ConnectionError("connection closed during TLS handshake", 0, "08006")
                w.transport = secured
                w.transport_name = f"tls:{host}:{port}"
            except OSError as e:
                raise ConnectionError(f"TLS handshake failed: {e}", 0, "08006") from e
        if not h.v2:
            raise _ConnectFailed("PDB native wire: the server did not negotiate the v2 multiplexed protocol "
                                 "(CAP_V2_MUX); this client requires it")
        if h.fmt != P.FMT_ARROW_IPC:
            raise ProtocolError(f"PDB native wire: server chose result format {h.fmt}, not Arrow IPC")
        w.hello = h
        if h.max_streams > 0:
            w.slots = asyncio.Semaphore(min(h.max_streams, 1 << 20))
        if user:
            try:
                await w.call(P.F_AUTH, P.auth_payload(user, password), P.FLAG_END_STREAM, connect_timeout)
            except PdbError as e:
                raise ConnectionError(f"PDB native wire: authentication failed: {e}", e.code,
                                      "28P01" if e.sqlstate in ("HY000", "28000") else e.sqlstate) from e
        elif h.auth_required:
            raise ConnectionError("PDB native wire: the server requires authentication; pass user= and password=",
                                  0x13, "28000")
    except BaseException:
        w.fail_all(ConnectionError("connection closed", 0, "08003"))
        raise
    return w


class AsyncRowStream:
    """Rows of one streamed result as they arrive, one record batch at a time."""

    def __init__(self, wire: AsyncWire, s: AStream):
        self.wire = wire
        self.s = s
        self.dec = _arrow.StreamDecoder(wire.use_pyarrow, wire.extended)
        self.rows: Deque[Any] = deque()
        self.closed = False
        self.rowcount = 0

    async def start(self) -> None:
        s = self.s
        while not (s.done or s.queue):
            s.event.clear()  # type: ignore[union-attr]
            if s.done or s.queue:
                break
            await self._wait_event()
        if s.done and not s.queue:
            _raise_for(s)
            if s.kind == P.F_ARROW_RESULT:
                self.dec.feed(s.payload)
            elif s.kind == P.F_OK:
                self.rowcount = P.decode_ok(s.payload)
        self._pump()

    async def _wait_event(self) -> None:
        try:
            await self.s.event.wait()  # type: ignore[union-attr]
        except asyncio.CancelledError:
            self.closed = True
            self.s.discard = True
            self.s.queue.clear()
            self.wire.abandon(self.s, block_next=False)
            raise

    async def fields(self) -> List[Any]:
        while self.dec.schema is None and not self.closed and await self._feed_one():
            pass
        return list(self.dec.schema.fields) if self.dec.schema else []

    async def _feed_one(self) -> bool:
        s = self.s
        while True:
            if s.queue:
                chunk = s.queue.popleft()
                s.qbytes -= len(chunk)
                self.dec.feed(chunk)
                self._pump()
                return True
            if s.done:
                _raise_for(s)
                return False
            s.event.clear()  # type: ignore[union-attr]
            if s.queue or s.done:
                continue
            await self._wait_event()

    def _pump(self) -> None:
        for cols in self.dec.batches():
            self.rows.extend(zip(*cols))

    async def next(self) -> Optional[Any]:
        while not self.rows:
            if self.closed or not await self._feed_one():
                self.closed = True
                return None
        self.rowcount += 1
        return self.rows.popleft()

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        s = self.s
        s.discard = True
        s.queue.clear()
        if not s.done:
            self.wire.abandon(s, block_next=False)
        self.rows.clear()
