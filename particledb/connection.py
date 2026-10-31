"""One native-wire connection: protocol 2, multiplexed, thread-safe.

``Connection`` is one socket and one SERVER SESSION (transaction, settings,
prepared statements, savepoints). Any number of threads may run statements on
it at once: each statement is its own stream, so a slow statement never
blocks a fast one (no head-of-line blocking) and results are never delivered
to the wrong caller. Statements run concurrently inside the session's open
transaction when there is one -- the JDBC ``Connection`` contract.

Default connect (no options) is the native wire's most efficient mode,
asserted by the server (``SHOW pdb.wire_protocol`` = ``pdb-native-v2``):

* HELLO asks for every capability this client implements -- ``v2_mux``,
  ``prepared``, ``numeric_text``, ``statement_notice`` and the extension bits
  ``typed_params``, ``array_params``, ``batch_sqlstate``, ``bulk_ingest`` (the
  JDBC reference's set);
* FAIL CLOSED: a server that does not confirm the multiplexed protocol is an
  error (``require_mux=True``), never a silent unmultiplexed connection;
* the server's Unix socket ``<unix_socket_dir>/.s.PDB.<port>`` for a loopback
  host when a server listens there, else TCP.

I/O model (the JDBC ``StreamMux`` model): no background thread. The calling
thread writes its request and, while it waits, takes the connection's READ
ROLE when it is free and reads frames itself, handing each to the stream it
belongs to; a thread whose answer another thread read is woken. With one
statement at a time this is a plain synchronous client: no thread hop.

Cancel: ``Pending.cancel()`` / ``Connection.cancel()`` send ``RST_STREAM
(CANCEL)``; the server ends the statement at its next interrupt check and
answers 57014, and the connection stays usable. A client ``timeout=`` does the
same, and only a statement the server still has not answered after the grace
period is ambiguous: the connection is then closed (never reused mid-
statement) and :class:`~particledb.errors.QueryCanceledError` says
``ambiguous=True``.
"""

from __future__ import annotations

import logging
import os
import select
import socket
import struct
import threading
import time
from collections import OrderedDict, deque
from typing import Any, Callable, Deque, Dict, Iterable, Iterator, List, NamedTuple, Optional, Sequence, Tuple, Union
from urllib.parse import unquote

from ._dsn import parse_connection_url

from . import _arrow
from . import _protocol as P
from . import _tls
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

log = logging.getLogger("particledb")

#: The native wire's default port. ONE constant: every default in this SDK
#: reads it, and it equals the server's own ``--pdb-port`` default (the
#: conformance suite's DEF-005 holds the two together).
DEFAULT_PORT = 7173
#: The server's default ``--unix-socket-dir``.
DEFAULT_UNIX_SOCKET_DIR = "/tmp"
DEFAULT_CONNECT_TIMEOUT = 10.0
#: How long a timed-out or cancelled statement may take to answer its reset
#: before the outcome is declared ambiguous and the connection closed.
DEFAULT_CANCEL_GRACE = 5.0
#: Prepared statements kept per connection (server cap: 4096).
DEFAULT_PREPARE_CACHE = 256
#: Bytes of a streamed result that may queue for a consumer that is not
#: reading while other requests drive the connection (protocol 2 has no
#: per-stream flow control); past it the stream is cancelled with 53200.
DEFAULT_STREAM_BUFFER = 256 * 1024 * 1024

_ROLE_BACKSTOP = 0.05  # longest park while another thread holds the read role
#: Rows per bulk-ingest DATA frame (re-split when a chunk encodes past the cap).
BULK_CHUNK_ROWS = 65536


class Notice(NamedTuple):
    """A statement advisory (PostgreSQL's NoticeResponse)."""

    severity: str
    sqlstate: str
    message: str


class _Timeout(Exception):
    pass


class _EOF(OSError):
    pass


def is_local_host(host: str) -> bool:
    """``localhost`` or a loopback IP literal (no DNS lookup)."""
    import ipaddress

    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host.strip("[]")).is_loopback
    except ValueError:
        return False


def unix_socket_path(port: int, socket_dir: str = DEFAULT_UNIX_SOCKET_DIR) -> str:
    """The server's native-wire socket for ``port``: ``<dir>/.s.PDB.<port>``."""
    return os.path.join(socket_dir, f".s.PDB.{port}")


# ═════════════════════════════════════════════════════════════════════════════
# Frame reader (resumable: a deadline never leaves it mid-frame)
# ═════════════════════════════════════════════════════════════════════════════

class _Reader:
    __slots__ = ("sock", "buf", "pos")

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.buf = bytearray()
        self.pos = 0

    def frame(self, v2: bool, deadline: Optional[float]) -> Tuple[int, int, int, bytes]:
        """``(type, flags, stream_id, payload)``."""
        hdr = P.V2_HEADER if v2 else P.V1_HEADER
        self._fill(hdr, deadline)
        if v2:
            try:
                t, flags, sid, n = P.parse_v2_header(self.buf, self.pos)
            except ValueError as e:
                raise ProtocolError(f"PDB native wire: {e}") from None
        else:
            n, sid, t, flags = P.parse_v1_header(bytes(self.buf[self.pos: self.pos + hdr]))
        if n > P.MAX_PAYLOAD:
            raise ProtocolError(f"PDB native wire: frame of {n} bytes exceeds {P.MAX_PAYLOAD}")
        self._fill(hdr + n, deadline)
        start = self.pos + hdr
        payload = bytes(self.buf[start: start + n])
        self.pos = start + n
        if self.pos == len(self.buf):
            self.buf = bytearray()
            self.pos = 0
        elif self.pos > (1 << 20):
            del self.buf[: self.pos]
            self.pos = 0
        return t, flags, sid, payload

    def _fill(self, need: int, deadline: Optional[float]) -> None:
        while len(self.buf) - self.pos < need:
            if isinstance(self.sock, _tls.Socket):
                try:
                    chunk = self.sock.recv_deadline(262144, deadline)
                except socket.timeout:
                    raise _Timeout() from None
            else:
                left = None if deadline is None else deadline - time.monotonic()
                if left is not None and left <= 0:
                    raise _Timeout()
                readable, _, _ = select.select([self.sock], [], [], left)
                if not readable:
                    raise _Timeout()
                try:
                    chunk = self.sock.recv(262144)
                except BlockingIOError:
                    continue
            if not chunk:
                raise _EOF("connection closed by the server")
            self.buf += chunk


# ═════════════════════════════════════════════════════════════════════════════
# Streams
# ═════════════════════════════════════════════════════════════════════════════

_CANCELLABLE = (P.F_QUERY, P.F_EXECUTE, P.F_BATCH)
#: Requests whose terminal frame reports the session's transaction status.
_STATUS_FRAMES = (P.F_QUERY, P.F_EXECUTE, P.F_BATCH, P.F_BULK)


class _Stream:
    __slots__ = ("sid", "cond", "done", "kind", "flags", "payload", "error", "notices",
                 "chunks", "queue", "qbytes", "attached", "discard", "slot", "ftype", "ignore", "started")

    def __init__(self, sid: int, cond: Optional[threading.Condition], ftype: int):
        self.sid = sid
        # Created on the first wait (under the connection's ``_mu``): a thread
        # that reads its own answer -- the single-threaded case -- never waits,
        # and a Condition costs microseconds per statement.
        self.cond = cond
        self.ftype = ftype
        self.done = False
        self.kind = 0
        self.flags = 0
        self.payload = b""
        self.error: Optional[BaseException] = None
        self.notices: List[Notice] = []
        self.chunks: List[bytes] = []
        self.queue: Deque[bytes] = deque()
        self.qbytes = 0
        self.attached = False
        self.discard = False
        self.slot = False
        self.ignore = False
        self.started = time.monotonic()


# ═════════════════════════════════════════════════════════════════════════════
# Results
# ═════════════════════════════════════════════════════════════════════════════

class Result:
    """A statement's answer: rows (tuples, columns in projection order,
    duplicate labels kept), ``rowcount`` (affected rows, or rows returned),
    ``notices``, and ``rolled_back`` (a COMMIT that ended a failed transaction
    by rolling it back)."""

    __slots__ = ("columns", "rows", "rowcount", "notices", "fields", "rolled_back", "has_rows")

    def __init__(self, columns: List[str], rows: List[Tuple[Any, ...]], rowcount: int,
                 notices: List[Notice], fields: Optional[List[Any]] = None,
                 rolled_back: bool = False, has_rows: bool = False):
        self.columns = columns
        self.rows = rows
        self.rowcount = rowcount
        self.notices = notices
        self.fields = fields or []
        self.rolled_back = rolled_back
        self.has_rows = has_rows

    def dicts(self) -> List[Dict[str, Any]]:
        """Rows as dicts. A repeated label keeps its FIRST column (use
        ``rows`` for positional access to every column)."""
        cols = self.columns
        if len(set(cols)) == len(cols):
            return [dict(zip(cols, r)) for r in self.rows]
        out = []
        for r in self.rows:
            d: Dict[str, Any] = {}
            for c, v in zip(cols, r):
                d.setdefault(c, v)
            out.append(d)
        return out

    def scalar(self) -> Any:
        """The first column of the first row, or ``None``."""
        return self.rows[0][0] if self.rows and self.rows[0] else None

    def __iter__(self) -> Iterator[Tuple[Any, ...]]:
        return iter(self.rows)

    def __len__(self) -> int:
        return len(self.rows)

    def __repr__(self) -> str:
        return f"Result(columns={self.columns!r}, rowcount={self.rowcount}, rows={len(self.rows)})"


def _raise_for(s: _Stream) -> None:
    """Raise the exception a terminal frame stands for (no-op on success)."""
    if s.error is not None:
        raise s.error
    if s.kind == P.F_ERROR:
        code, msg = P.decode_error(s.payload)
        state, text = P.server_error(code, msg)
        raise _error_for(state, code, text)
    if s.kind == P.F_RST_STREAM_S:
        code = struct.unpack_from("<H", s.payload, 0)[0] if len(s.payload) >= 2 else 0
        state = P.sqlstate_for_code(code)
        if code == P.ERR_CANCEL:
            raise QueryCanceledError("statement cancelled (stream reset by the server)", code)
        raise ConnectionError(f"stream reset by the server (code {code})", code, state) \
            if state.startswith("08") else QueryError(f"stream reset by the server (code {code})", code, state)


def _pyarrow_table(s: _Stream) -> Any:
    pa, pa_ipc = _arrow.pyarrow_modules()
    if s.kind in (P.F_ARROW_RESULT, P.F_ARROW_STREAM):
        data = s.payload if s.kind == P.F_ARROW_RESULT else b"".join(s.chunks)
        return pa_ipc.open_stream(data).read_all()
    return pa.table({})


def _error_for(state: str, code: int, text: str) -> PdbError:
    if state == "57014":
        return QueryCanceledError(text, code, state)
    if state == "53100" or code == 28:
        return DiskFullError(text, code, state)
    if state.startswith("08"):
        return ConnectionError(text, code, state)
    return QueryError(text, code, state)


# ═════════════════════════════════════════════════════════════════════════════
# Pending request
# ═════════════════════════════════════════════════════════════════════════════

class Pending:
    """A request in flight: ``result(timeout)`` waits for it, ``cancel()``
    resets its stream (the server answers 57014 if the statement was still
    running)."""

    __slots__ = ("_conn", "_s", "_decode", "_value", "_done")

    def __init__(self, conn: "Connection", s: _Stream, decode: Callable[[_Stream], Any]):
        self._conn = conn
        self._s = s
        self._decode = decode
        self._done = False
        self._value: Any = None

    @property
    def stream_id(self) -> int:
        return self._s.sid

    def done(self) -> bool:
        return self._s.done

    def cancel(self) -> bool:
        """Ask the server to cancel this statement. False when there is nothing
        to cancel (already answered, not a statement, connection closed)."""
        return self._conn._reset(self._s)

    def result(self, timeout: Optional[float] = None) -> Any:
        if not self._done:
            self._conn._await(self._s, timeout)
            _raise_for(self._s)
            self._value = self._decode(self._s)
            self._done = True
        return self._value


# ═════════════════════════════════════════════════════════════════════════════
# Prepared statements
# ═════════════════════════════════════════════════════════════════════════════

class _Prep:
    __slots__ = ("name", "name_b", "nparams", "ready", "error", "refs", "closed")

    def __init__(self, name: str):
        self.name = name
        self.name_b = name.encode("utf-8")
        self.nparams = -1
        self.ready = threading.Event()
        self.error: Optional[BaseException] = None
        self.refs = 0
        self.closed = False


class PreparedStatement:
    """A statement prepared on the server once and executed by handle with
    typed parameters (never interpolated text)."""

    def __init__(self, conn: "Connection", sql: str):
        self._conn = conn
        self.sql = sql
        self._closed = False
        e = conn._prepare_entry(sql, None)
        self._entry = e  # pinned (refs held) until close()

    @property
    def server_prepared(self) -> bool:
        """True once the server answered PREPARE_OK for this statement."""
        e = self._entry
        return e.ready.is_set() and e.error is None and e.nparams >= 0

    @property
    def param_count(self) -> int:
        return self._entry.nparams

    def submit(self, params: Sequence[Any] = ()) -> Pending:
        return self._conn._submit_prepared(self._entry, self.sql, list(params))

    def execute(self, params: Sequence[Any] = (), timeout: Optional[float] = None) -> Result:
        return self._conn._run_prepared(self.sql, list(params), timeout, entry=self._entry)

    def query(self, params: Sequence[Any] = (), timeout: Optional[float] = None) -> List[Dict[str, Any]]:
        return self.execute(params, timeout).dicts()

    def stream(self, params: Sequence[Any] = (), timeout: Optional[float] = None) -> "RowStream":
        return self._conn._stream(self.sql, list(params), timeout, entry=self._entry)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._conn._unpin(self._entry)

    def __enter__(self) -> "PreparedStatement":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


# ═════════════════════════════════════════════════════════════════════════════
# Streamed rows
# ═════════════════════════════════════════════════════════════════════════════

class RowStream:
    """Rows of one result as they arrive, decoded one Arrow record batch at a
    time: client memory is one partial IPC message plus one decoded batch,
    whatever the result's size. Iterate it (tuples), read ``columns`` (known
    once the first chunk is in), ``close()`` it to stop early (the statement is
    cancelled and the rest dropped). Other statements on the same connection
    keep being served between batches."""

    def __init__(self, conn: "Connection", s: _Stream, first_wait_timeout: Optional[float]):
        self._conn = conn
        self._s = s
        self._dec = _arrow.StreamDecoder(conn._use_pyarrow, conn._extended)
        self._rows: Deque[Tuple[Any, ...]] = deque()
        self._closed = False
        self.rowcount = 0
        self.notices: List[Notice] = s.notices
        self._chunk_timeout = first_wait_timeout  # also bounds each later wait
        # The stream was opened ATTACHED (``_open_stream(attached=True)``):
        # every chunk, from the first, is queued and counted against
        # ``stream_buffer_limit``, whoever reads it.
        deadline = None if first_wait_timeout is None else time.monotonic() + first_wait_timeout
        try:
            conn._wait(s, lambda: s.done or bool(s.queue), deadline)
        except _Timeout:
            conn._timed_out(s, first_wait_timeout)
        if s.done and not s.queue:
            _raise_for(s)
            if s.kind == P.F_ARROW_RESULT:
                self._dec.feed(s.payload)
            else:
                self.rowcount = P.decode_ok(s.payload) if s.kind == P.F_OK else 0
        self._pump()

    @property
    def columns(self) -> List[str]:
        """Column names (duplicates kept); waits for the schema if its bytes
        have not all arrived yet."""
        while self._dec.schema is None and not self._closed and self._feed_one():
            pass
        return list(self._dec.schema.names) if self._dec.schema else []

    @property
    def fields(self) -> List[Any]:
        """The result's Arrow fields (``[]`` for a statement without rows);
        waits for the schema like :attr:`columns`."""
        while self._dec.schema is None and not self._closed and self._feed_one():
            pass
        return list(self._dec.schema.fields) if self._dec.schema else []

    def _feed_one(self) -> bool:
        """Feed the decoder the next chunk, waiting for one; False at the end."""
        s = self._s
        while True:
            with self._conn._mu:
                chunk = s.queue.popleft() if s.queue else None
                if chunk is not None:
                    s.qbytes -= len(chunk)
            if chunk is not None:
                self._dec.feed(chunk)
                self._pump()
                return True
            if s.done:
                _raise_for(s)
                return False
            t = self._chunk_timeout
            try:
                self._conn._wait(s, lambda: bool(s.queue) or s.done,
                                 None if t is None else time.monotonic() + t)
            except _Timeout:
                self._conn._timed_out(s, t)

    def _pump(self) -> bool:
        """Decode whatever is buffered; False when nothing more will come."""
        for cols in self._dec.batches():
            self._rows.extend(zip(*cols))
        return not (self._s.done and not self._s.queue)

    def __iter__(self) -> "RowStream":
        return self

    def __next__(self) -> Tuple[Any, ...]:
        while not self._rows:
            if self._closed or not self._feed_one():
                self._closed = True
                raise StopIteration
        self.rowcount += 1
        return self._rows.popleft()

    def close(self) -> None:
        """Stop early: cancel the statement and drop the rest as it arrives."""
        if self._closed:
            return
        self._closed = True
        s = self._s
        with self._conn._mu:
            s.discard = True
            s.queue.clear()
        if not s.done:
            self._conn._reset(s)
        self._rows.clear()

    def __enter__(self) -> "RowStream":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


# ═════════════════════════════════════════════════════════════════════════════
# The connection
# ═════════════════════════════════════════════════════════════════════════════

class Connection:
    """A multiplexed native-wire connection (one socket, one server session).

    Args:
        host: server host; a loopback host uses the Unix socket when a server
            listens on it.
        port: native-wire port (default :data:`DEFAULT_PORT`).
        user, password: credentials for an auth-required server (sent in
            ``F_AUTH`` right after HELLO). ``None`` sends none (a ``--no-auth``
            server).
        connect_timeout: seconds for the socket connect and HELLO.
        statement_timeout: default client-side timeout (seconds) for every
            statement; ``None`` = no client limit.
        cancel_grace: seconds a timed-out statement may take to answer its
            reset before the connection is closed as ambiguous.
        prefer_unix_socket, unix_socket_dir, unix_socket_path: transport choice
            (an explicit ``unix_socket_path`` must work: no TCP fallback).
        require_mux: kept for symmetry with the JDBC driver's option; this
            client speaks only protocol 2 and fails closed either way (a
            server that does not negotiate it raises at connect).
        prepare_cache_size: prepared statements kept per connection.
        stream_buffer_limit: bytes a :class:`RowStream` may fall behind while
            other threads read the connection before it is cancelled (53200).
        notice_handler: called with each :class:`Notice` as it arrives, on
            whichever thread is reading the connection at that moment (keep it
            short; an exception it raises is logged, not propagated).
        use_pyarrow: decode large record batches with pyarrow (needs it
            installed). Default ``False``: the built-in decoder is faster at
            producing Python rows (``PDB_PY_DECODER=pyarrow`` flips the
            default); :meth:`query_arrow` always returns a pyarrow table.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: Optional[int] = None,
        *,
        user: Optional[str] = None,
        password: Optional[str] = None,
        sslmode: str = "disable",
        sslrootcert: Optional[str] = None,
        sslcert: Optional[str] = None,
        sslkey: Optional[str] = None,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        statement_timeout: Optional[float] = None,
        write_timeout: Optional[float] = 30.0,
        cancel_grace: float = DEFAULT_CANCEL_GRACE,
        prefer_unix_socket: bool = True,
        unix_socket_dir: str = DEFAULT_UNIX_SOCKET_DIR,
        unix_socket_path: Optional[str] = None,
        require_mux: bool = True,
        prepare_cache_size: int = DEFAULT_PREPARE_CACHE,
        stream_buffer_limit: int = DEFAULT_STREAM_BUFFER,
        notice_handler: Optional[Callable[[Notice], None]] = None,
        use_pyarrow: Optional[bool] = None,
        _extended_types: bool = False,
        _caps: int = P.DEFAULT_CAPS,
        _ext: int = P.DEFAULT_EXT,
    ):
        self.host = host
        self.port = DEFAULT_PORT if port is None else int(port)
        self.statement_timeout = statement_timeout
        if write_timeout is not None:
            import math
            if not math.isfinite(write_timeout) or write_timeout <= 0:
                raise ValueError("write_timeout must be positive and finite, or None")
        self.write_timeout = write_timeout
        self.cancel_grace = cancel_grace
        self.notice_handler = notice_handler
        self.stream_buffer_limit = stream_buffer_limit
        self._use_pyarrow = (_arrow.USE_PYARROW if use_pyarrow is None else bool(use_pyarrow)) \
            and _arrow.HAVE_PYARROW
        # The DB-API profile: json / jsonb as Python objects, interval as
        # timedelta (``_arrow.converter_for(extended=True)``).
        self._extended = bool(_extended_types)
        self._mu = threading.Lock()
        self._wlock = threading.Lock()
        self._role = threading.Lock()
        self._role_waiters: Deque[_Stream] = deque()
        self._streams: Dict[int, _Stream] = {}
        self._next_sid = 1
        self._closed = False
        self._close_cause: Optional[BaseException] = None
        self._slots: Optional[threading.Semaphore] = None
        self._ping_lock = threading.Lock()
        self._cache_lock = threading.Lock()
        self._cache: "OrderedDict[str, _Prep]" = OrderedDict()
        self._cache_size = max(1, prepare_cache_size)
        self._stmt_seq = 0
        self._bulk_lock = threading.Lock()
        # exclusive(): while a thread holds it, no OTHER thread opens a stream
        # (a hidden BEGIN .. COMMIT must not capture another thread's statement).
        self._excl_owner: Optional[int] = None
        self._excl_depth = 0
        self._excl_cv = threading.Condition(self._mu)
        self.txn_status = 0
        self.transport = "?"
        self.hello: Optional[P.HelloOk] = None
        self._tls_context = _tls.context(sslmode, sslrootcert, sslcert, sslkey)
        if self._tls_context is not None:
            if unix_socket_path:
                raise ValueError("TLS requires TCP; remove unix_socket_path")
            prefer_unix_socket = False
        self._sock = self._open(host, self.port, connect_timeout, prefer_unix_socket,
                                unix_socket_dir, unix_socket_path)
        self._reader = _Reader(self._sock)
        try:
            self._handshake(connect_timeout, require_mux, user, password, _caps, _ext)
        except BaseException:
            self._hard_close()
            raise

    # ── transport ────────────────────────────────────────────────────────

    def _open(self, host: str, port: int, timeout: float, prefer_unix: bool,
              sock_dir: str, sock_path: Optional[str]) -> socket.socket:
        if prefer_unix and hasattr(socket, "AF_UNIX") and (sock_path or is_local_host(host)):
            path = sock_path or unix_socket_path(port, sock_dir)
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(timeout)
            try:
                s.connect(path)
                s.settimeout(None)
                self.transport = f"unix:{path}"
                return s
            except (FileNotFoundError, ConnectionRefusedError) as e:
                s.close()
                if sock_path:
                    raise _ConnectFailed(f"unix_socket_path {sock_path}: {e}") from e
                log.debug("PDB native wire: no server on %s (%s); using TCP %s:%s", path, e, host, port)
            except OSError as e:
                s.close()
                raise _ConnectFailed(
                    f"PDB native wire: Unix socket {path} exists but connecting failed ({e}); "
                    f"not falling back to TCP (pass prefer_unix_socket=False to use TCP)") from e
        try:
            s = socket.create_connection((host.strip("[]"), port), timeout=timeout)
        except OSError as e:
            raise _ConnectFailed(f"cannot connect to {host}:{port}: {e}") from e
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.settimeout(None)
        self.transport = f"tcp:{host}:{port}"
        return s

    def _handshake(self, timeout: float, require_mux: bool, user: Optional[str],
                   password: Optional[str], caps: int, ext: int) -> None:
        deadline = time.monotonic() + timeout
        if self._tls_context is not None:
            caps |= P.CAP_TLS
        try:
            self._sock.sendall(P.encode_hello(caps, ext))
            t, _f, _sid, payload = self._reader.frame(False, time.monotonic() + timeout)
        except _Timeout:
            raise _ConnectFailed("PDB native wire: no HELLO_OK within the connect timeout") from None
        except OSError as e:
            raise _ConnectFailed(f"PDB native wire: handshake failed: {e}") from e
        if t == P.F_ERROR:
            code, msg = P.decode_error(payload)
            state, text = P.server_error(code, msg)
            raise ConnectionError(f"PDB native wire: the server refused the handshake: {text}", code, state)
        if t != P.F_HELLO_OK:
            raise ProtocolError(f"PDB native wire: expected HELLO_OK, got 0x{t:02x}")
        h = P.HelloOk.parse(payload)
        if self._tls_context is not None:
            if not h.caps & P.CAP_TLS:
                raise ConnectionError("server did not accept TLS; refusing plaintext", 0, "08006")
            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("TLS connection deadline exceeded")
                self._sock.settimeout(remaining)
                self._sock = self._tls_context.wrap_socket(self._sock, server_hostname=self.host.strip("[]"))
                self._sock = _tls.Socket(self._sock)
                self._reader = _Reader(self._sock)
                self.transport = f"tls:{self.host}:{self.port}"
            except OSError as e:
                raise ConnectionError(f"TLS handshake failed: {e}", 0, "08006") from e
        if not h.v2:
            # FAIL CLOSED: a v1 fallback "works", unmultiplexed, and nothing
            # would tell the caller. This client speaks only protocol 2.
            raise _ConnectFailed(
                "PDB native wire: the server did not negotiate the v2 multiplexed protocol "
                "(CAP_V2_MUX); this client requires it" + ("" if require_mux else
                                                           " (require_mux=False is not supported)"),
            )
        if h.fmt != P.FMT_ARROW_IPC:
            raise ProtocolError(f"PDB native wire: server chose result format {h.fmt}, not Arrow IPC")
        if not isinstance(self._sock, _tls.Socket):
            self._sock.setblocking(False)
        self.hello = h
        if h.max_streams > 0:
            self._slots = threading.BoundedSemaphore(min(h.max_streams, 1 << 20))
        if user:
            s = self._open_stream(P.F_AUTH, None)
            self._send(P.v2_frame(P.F_AUTH, s.sid, P.auth_payload(user, password)))
            try:
                self._await(s, timeout)
                _raise_for(s)
            except PdbError as e:
                raise ConnectionError(f"PDB native wire: authentication failed: {e}", e.code,
                                      "28P01" if e.sqlstate in ("HY000", "28000") else e.sqlstate) from e
        elif h.auth_required:
            raise ConnectionError("PDB native wire: the server requires authentication; pass user= and password=",
                                  0x13, "28000")

    # ── negotiated state ────────────────────────────────────────────────

    @property
    def negotiated_protocol(self) -> str:
        """``pdb-native-v2`` -- as the server's ``SHOW pdb.wire_protocol`` names it."""
        return self.hello.protocol if self.hello else "?"

    @property
    def negotiated_capabilities(self) -> List[str]:
        """Sorted capability names as ``SHOW pdb.wire_capabilities`` names them."""
        return self.hello.capability_names if self.hello else []

    @property
    def max_concurrent_streams(self) -> int:
        """The server's per-connection stream cap (0 = none advertised)."""
        return self.hello.max_streams if self.hello else 0

    @property
    def closed(self) -> bool:
        return self._closed

    def peer_gone(self) -> bool:
        """For an IDLE connection (nothing in flight): True when the server
        has closed the socket (EOF or reset pending). One non-blocking peek;
        a pool calls it before lending a connection out."""
        if self._closed:
            return True
        with self._mu:
            if self._streams or self._role.locked():
                return False
        try:
            data = self._sock.recv(1, socket.MSG_PEEK | getattr(socket, "MSG_DONTWAIT", 0))
        except BlockingIOError:
            return False
        except OSError as e:
            self._fail_all(ConnectionError(f"connection lost: {e}", 0, "08006"))
            return True
        if data == b"":
            self._fail_all(ConnectionError("connection closed by the server", 0, "08006"))
            return True
        # Unsolicited bytes on an idle connection (a GOAWAY before shutdown):
        # read them now, so the connection is known dead BEFORE a request is
        # written on it (which would then be ambiguous).
        if self._role.acquire(blocking=False):
            try:
                self._read_some(time.monotonic() + 0.05)
            finally:
                self._release_role()
        return self._closed

    @property
    def busy(self) -> bool:
        """A request (other than a fire-and-forget statement close) is still in
        flight: the connection's server session is in use."""
        with self._mu:
            return any(not s.ignore for s in self._streams.values())

    @property
    def in_transaction(self) -> bool:
        """The server reported an open (or failed) transaction block after the
        most recent response."""
        return bool(self.txn_status)

    def _has(self, cap: int = 0, ext: int = 0) -> bool:
        return bool(self.hello and self.hello.has(cap, ext))

    # ── low-level request plumbing ──────────────────────────────────────

    def _check_open(self) -> None:
        if self._closed:
            cause = self._close_cause
            raise ConnectionError(f"connection is closed{': ' + str(cause) if cause else ''}",
                                  0, "08003")

    def _open_stream(self, ftype: int, deadline: Optional[float], attached: bool = False) -> _Stream:
        self._check_open()
        while True:
            if self._slots is not None:
                self._acquire_slot(deadline)
            with self._mu:
                if self._closed:
                    if self._slots is not None:
                        self._slots.release()
                    self._check_open()
                owner = self._excl_owner
                if owner is None or owner == threading.get_ident():
                    return self._register(ftype, attached)
                # Another thread holds the connection exclusively: give the
                # slot back (it may need every one) and wait for it to finish.
                if self._slots is not None:
                    self._slots.release()
                while self._excl_owner is not None and not self._closed:
                    self._excl_cv.wait(_ROLE_BACKSTOP)

    def _register(self, ftype: int, attached: bool) -> _Stream:
        """Allocate and register a stream (caller holds ``_mu`` and a slot)."""
        sid = self._next_sid
        self._next_sid += 2
        s = _Stream(sid, None, ftype)
        s.attached = attached
        s.slot = self._slots is not None
        self._streams[sid] = s
        return s

    class _Exclusive:
        def __init__(self, conn: "Connection"):
            self.conn = conn

        def __enter__(self) -> None:
            c = self.conn
            me = threading.get_ident()
            with c._mu:
                while c._excl_owner is not None and c._excl_owner != me and not c._closed:
                    c._excl_cv.wait(_ROLE_BACKSTOP)
                c._excl_owner = me
                c._excl_depth += 1

        def __exit__(self, *exc: Any) -> None:
            c = self.conn
            with c._mu:
                c._excl_depth -= 1
                if c._excl_depth == 0:
                    c._excl_owner = None
                c._excl_cv.notify_all()

    def exclusive(self) -> "Connection._Exclusive":
        """``with conn.exclusive():`` no OTHER thread opens a stream on this
        connection until the block ends (it waits). For a request sequence
        that must not interleave with other threads' statements in the shared
        session: the DB-API layer's hidden BEGIN .. COMMIT around a script."""
        return Connection._Exclusive(self)

    def _acquire_slot(self, deadline: Optional[float]) -> None:
        slots = self._slots
        limit = time.monotonic() + 60.0 if deadline is None else deadline
        while not slots.acquire(blocking=False):
            self._check_open()
            if time.monotonic() >= limit:
                raise QueryError(
                    f"no stream slot: the server allows {self.max_concurrent_streams} concurrent "
                    "streams per connection and all stayed busy", 0, "53300")
            # Slots free when answers are READ: drive the reader if nobody is.
            if self._role.acquire(blocking=False):
                try:
                    self._read_some(time.monotonic() + 0.005)
                finally:
                    self._release_role()
            elif slots.acquire(timeout=0.005):
                return

    def _send(self, frame: bytes, deadline: Optional[float] = None) -> None:
        if self.write_timeout is not None:
            write_deadline = time.monotonic() + self.write_timeout
            deadline = write_deadline if deadline is None else min(deadline, write_deadline)
        acquired = self._wlock.acquire() if deadline is None else self._wlock.acquire(timeout=max(0, deadline - time.monotonic()))
        if not acquired:
            # No byte was sent on this stream. Preserve other in-flight work.
            raise QueryError("timed out waiting to send a request", 0, "53300")
        try:
            self._check_open()
            try:
                if isinstance(self._sock, _tls.Socket):
                    self._sock.sendall_deadline(frame, deadline)
                else:
                    remaining = memoryview(frame)
                    while remaining:
                        left = None if deadline is None else deadline - time.monotonic()
                        if left is not None and left <= 0:
                            raise socket.timeout("socket write timed out")
                        _, writable, _ = select.select([], [self._sock], [], left)
                        if not writable:
                            raise socket.timeout("socket write timed out")
                        try:
                            n = self._sock.send(remaining)
                        except BlockingIOError:
                            continue
                        if n == 0:
                            raise ConnectionResetError("connection closed during write")
                        remaining = remaining[n:]
            except (OSError, ValueError) as e:
                err = ConnectionError(f"connection lost while sending; outcome may be unknown: {e}", 0, "08006")
                self._fail_all(err)
                raise err from e
        finally:
            self._wlock.release()

    def _request(self, ftype: int, payload: bytes, flags: int = P.FLAG_END_STREAM,
                 deadline: Optional[float] = None, attached: bool = False) -> _Stream:
        s = self._open_stream(ftype, deadline, attached)
        try:
            self._send(P.v2_frame(ftype, s.sid, payload, flags), deadline)
        except (ValueError, QueryError):
            self._drop(s)
            raise
        return s

    def _drop(self, s: _Stream) -> None:
        with self._mu:
            if self._streams.pop(s.sid, None) is not None:
                s.done = True
                self._release_slot(s)

    def _release_slot(self, s: _Stream) -> None:
        if s.slot:
            s.slot = False
            self._slots.release()

    def _reset(self, s: _Stream) -> bool:
        if s.done or self._closed or s.ftype not in _CANCELLABLE:
            return False
        try:
            self._send(P.v2_frame(P.F_RST_STREAM, s.sid, P.rst_payload(P.ERR_CANCEL), 0),
                       time.monotonic() + self.cancel_grace)
            return True
        except PdbError:
            return False

    def cancel(self) -> int:
        """Cancel every statement in flight on this connection (each answers
        57014 if it was still running). Returns how many were reset."""
        with self._mu:
            live = [s for s in self._streams.values() if not s.done and s.ftype in _CANCELLABLE]
        return sum(1 for s in live if self._reset(s))

    # ── reading: the read role ───────────────────────────────────────────

    def _release_role(self) -> None:
        self._role.release()
        with self._mu:
            while self._role_waiters:
                w = self._role_waiters.popleft()
                if not w.done or w.attached:
                    if w.cond is not None:
                        w.cond.notify_all()
                    break

    def _read_some(self, deadline: Optional[float]) -> None:
        """Holding the role: read and dispatch one frame (or time out quietly)."""
        try:
            t, f, sid, p = self._reader.frame(True, deadline)
        except _Timeout:
            return
        except (OSError, ValueError, ProtocolError) as e:
            self._fail_all(e if isinstance(e, ConnectionError) else
                           ConnectionError(f"connection lost: {e}", 0, "08006"))
            return
        self._dispatch(t, f, sid, p)

    def _wait(self, s: _Stream, pred: Callable[[], bool], deadline: Optional[float]) -> None:
        """Block until ``pred()``; raise :class:`_Timeout` at ``deadline``."""
        while True:
            with self._mu:
                while True:
                    if pred():
                        return
                    if self._closed and s.done:
                        return
                    if self._role.acquire(blocking=False):
                        break
                    wait = _ROLE_BACKSTOP
                    if deadline is not None:
                        left = deadline - time.monotonic()
                        if left <= 0:
                            raise _Timeout()
                        wait = min(wait, left)
                    self._role_waiters.append(s)
                    if s.cond is None:
                        s.cond = threading.Condition(self._mu)
                    s.cond.wait(wait)
                    try:
                        self._role_waiters.remove(s)
                    except ValueError:
                        pass
            try:
                while not pred():
                    # Checked per frame, not only when the socket runs dry: other
                    # streams' traffic can keep it readable indefinitely.
                    if deadline is not None and time.monotonic() >= deadline:
                        raise _Timeout()
                    t, f, sid, p = self._reader.frame(True, deadline)
                    self._dispatch(t, f, sid, p)
            except _Timeout:
                raise
            except (OSError, ProtocolError) as e:
                self._fail_all(e if isinstance(e, ConnectionError) else
                               ConnectionError(f"connection lost: {e}", 0, "08006"))
            finally:
                self._release_role()

    def _await(self, s: _Stream, timeout: Optional[float]) -> None:
        """Wait for ``s``'s terminal frame; on a client timeout, cancel it and
        give it the grace period to answer, else close the connection."""
        if timeout is None:
            timeout = self.statement_timeout
        deadline = None if timeout is None else time.monotonic() + timeout
        try:
            self._wait(s, lambda: s.done, deadline)
        except _Timeout:
            self._timed_out(s, timeout)

    def _timed_out(self, s: _Stream, timeout: Optional[float]) -> None:
        if s.done:
            return  # another reader completed it as the deadline passed
        if self._reset(s):
            try:
                self._wait(s, lambda: s.done, time.monotonic() + self.cancel_grace)
                return  # answered: 57014, or its result if it finished first
            except _Timeout:
                pass
        if s.done:
            return
        # Never answered: the statement may still run (and commit). Close the
        # connection so nothing reuses it mid-statement.
        err = QueryCanceledError(
            f"statement did not answer within {timeout}s nor its cancel within "
            f"{self.cancel_grace}s; outcome unknown, connection closed", 0, "57014", ambiguous=True)
        self._fail_all(err)
        raise err

    def _dispatch(self, t: int, flags: int, sid: int, payload: bytes) -> None:
        if t == P.F_GOAWAY:
            self._fail_all(ConnectionError("the server closed the connection (GOAWAY)", 0, "08006"))
            return
        notices: List[Notice] = []
        overflow: Optional[_Stream] = None
        with self._mu:
            s = self._streams.get(sid)
            if s is None:
                return  # a stream we gave up on, or unknown: dropped per spec
            if t == P.F_NOTICE:
                ns = P.decode_notices(payload)
                if ns:  # a malformed notice frame is dropped whole
                    notices = [Notice(*n) for n in ns]
                    s.notices.extend(notices)
            elif t == P.F_KV_REPLY and flags & P.FLAG_MORE_CHUNKS:
                s.chunks.append(payload)
            elif t == P.F_ARROW_STREAM and flags & P.FLAG_MORE_CHUNKS:
                if self._take_chunk(s, payload):
                    overflow = s
                if s.ftype in _STATUS_FRAMES and self._has(P.CAP_PREPARED):
                    # Every frame of a response carries the session's status:
                    # a streamed result's transaction is open from its first
                    # chunk, not only once the stream ends.
                    self.txn_status = flags & (P.FLAG_TXN_OPEN | P.FLAG_TXN_FAILED)
                if s.cond is not None:
                    s.cond.notify_all()
            else:
                if t == P.F_ARROW_STREAM:
                    self._take_chunk(s, payload)
                elif t == P.F_KV_REPLY and s.chunks:
                    s.payload = b"".join(s.chunks) + payload
                    s.chunks = []
                else:
                    s.payload = payload
                s.kind = t
                s.flags = flags
                s.done = True
                if s.ftype in _STATUS_FRAMES and self._has(P.CAP_PREPARED):
                    # The server stamps the session's transaction status on
                    # every frame of a request's response (TxnStatusSink); the
                    # control stream (PONG) and KV replies answered inline are
                    # not requests that report it, so they never clear it.
                    self.txn_status = flags & (P.FLAG_TXN_OPEN | P.FLAG_TXN_FAILED)
                del self._streams[sid]
                self._release_slot(s)
                if s.cond is not None:
                    s.cond.notify_all()
        if overflow is not None:
            self._reset(overflow)
        if notices and self.notice_handler is not None:
            for n in notices:
                try:
                    self.notice_handler(n)
                except Exception:  # a handler bug must not break the reader
                    log.exception("particledb notice handler raised")

    def _take_chunk(self, s: _Stream, payload: bytes) -> bool:
        """File one result chunk (under ``_mu``); True when it overflowed an
        attached consumer's buffer and the stream must be cancelled."""
        if not s.attached:
            s.chunks.append(payload)
            return False
        if s.discard:
            return False
        s.queue.append(payload)
        s.qbytes += len(payload)
        if s.qbytes <= self.stream_buffer_limit:
            return False
        # The consumer fell this far behind while other requests drove the
        # reader. Protocol 2 has no per-stream flow control, so THIS stream is
        # failed loudly and cancelled rather than buffered without bound.
        s.discard = True
        s.queue.clear()
        s.qbytes = 0
        s.error = QueryError(f"streamed result dropped: its consumer fell more than "
                             f"{self.stream_buffer_limit} bytes behind", 0, "53200")
        return True

    def _fail_all(self, err: BaseException) -> None:
        with self._mu:
            if not self._closed:
                self._closed = True
                self._close_cause = err
            for s in list(self._streams.values()):
                s.error = err
                s.done = True
                self._release_slot(s)
                if s.cond is not None:
                    s.cond.notify_all()
            self._streams.clear()
        self._hard_close()

    def _hard_close(self) -> None:
        # shutdown() first: close() alone does not wake a thread blocked in
        # recv() on this socket (the read-role holder), shutdown() does (EOF).
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self._sock.close()
        except Exception:
            pass

    # ── statements ──────────────────────────────────────────────────────

    def submit(self, sql: str, params: Optional[Sequence[Any]] = None) -> Pending:
        """Send a statement and return at once; ``.result()`` waits for it."""
        if params:
            e = self._prepare_entry(sql, None)
            try:
                return self._submit_prepared(e, sql, list(params))
            finally:
                self._unpin(e)
        s = self._request(P.F_QUERY, sql.encode("utf-8"))
        return Pending(self, s, self._decode_result)

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None,
                timeout: Optional[float] = None) -> Result:
        """Run one statement (or a multi-statement string: one implicit
        transaction, the last statement's result). ``$1..$n`` parameters are
        bound as typed values of a server-side prepared statement."""
        if params:
            return self._run_prepared(sql, list(params), timeout)
        s = self._request(P.F_QUERY, sql.encode("utf-8"))
        return self._finish(s, timeout)

    def query(self, sql: str, params: Optional[Sequence[Any]] = None,
              timeout: Optional[float] = None) -> List[Dict[str, Any]]:
        """Rows as dicts (see :meth:`Result.dicts`)."""
        return self.execute(sql, params, timeout).dicts()

    def query_one(self, sql: str, params: Optional[Sequence[Any]] = None,
                  timeout: Optional[float] = None) -> Optional[Dict[str, Any]]:
        rows = self.query(sql, params, timeout)
        return rows[0] if rows else None

    def scalar(self, sql: str, params: Optional[Sequence[Any]] = None,
               timeout: Optional[float] = None) -> Any:
        return self.execute(sql, params, timeout).scalar()

    def stream(self, sql: str, params: Optional[Sequence[Any]] = None,
               timeout: Optional[float] = None) -> RowStream:
        """Iterate a result in bounded memory (see :class:`RowStream`);
        ``timeout`` bounds the wait for the first rows."""
        return self._stream(sql, list(params or ()), timeout)

    def is_server_prepared(self, sql: str) -> bool:
        """True when ``sql`` is held as a server-side prepared statement on
        this connection (every parameterised :meth:`execute` prepares on first
        use and reuses the handle after)."""
        with self._cache_lock:
            e = self._cache.get(sql)
        return bool(e is not None and e.ready.is_set() and e.error is None and e.nparams >= 0)

    def prepare(self, sql: str) -> PreparedStatement:
        """Prepare ``sql`` on the server now and return its handle."""
        return PreparedStatement(self, sql)

    def _batch_raw(self, sqls: Sequence[str], timeout: Optional[float] = None) -> "P.BatchResult":
        """:meth:`batch`, returning the decoded BATCH_RESULT (per-entry counts
        and, for a row-returning entry, its Arrow IPC bytes in ``results``)."""
        s = self._request(P.F_BATCH, P.batch_payload(list(sqls)))
        self._await(s, timeout)
        _raise_for(s)
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
        br.notices = s.notices  # type: ignore[attr-defined]
        return br

    def batch(self, sqls: Sequence[str], timeout: Optional[float] = None) -> List[int]:
        """Run statements in ONE request, in order; the server stops at the
        first failure, raised as :class:`~particledb.errors.BatchError` with
        that statement's SQLSTATE. Returns per-statement affected counts
        (``-1`` for a row-returning statement)."""
        if not sqls:
            return []
        s = self._request(P.F_BATCH, P.batch_payload(list(sqls)))
        self._await(s, timeout)
        _raise_for(s)
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
        return br.counts

    def execute_each(self, sql: str, rows: Sequence[Sequence[Any]],
                     timeout: Optional[float] = None) -> List[Union[Result, QueryError]]:
        """Run one prepared statement once per parameter set, IN ORDER, on this
        connection (the mux runs concurrent streams in parallel, so the sets are
        not pipelined). One entry per set that ran: its :class:`Result` or the
        :class:`~particledb.errors.QueryError` it failed with. Outside a
        transaction every set runs (each commits on its own); inside one the
        first failure stops it (the transaction is then failed). Every set is
        encoded before the first runs, so a value that cannot be bound fails
        the call before anything executes."""
        rows = [list(r) for r in rows]
        if not rows:
            return []
        if not rows[0]:
            raise ValueError("execute_many needs a statement with at least one parameter ($1); "
                             "run a parameterless statement with execute()")
        encoded: List[bytes] = []
        room = P.MAX_PAYLOAD - 64  # the EXECUTE header and statement name
        for i, r in enumerate(rows):
            if len(r) != len(rows[0]):
                raise ValueError(f"parameter row {i} has {len(r)} values, expected {len(rows[0])}")
            b = P.encode_params(r)
            if len(b) > room:
                raise ValueError(f"parameter row {i} encodes to {len(b)} bytes, over the server's "
                                 f"{P.MAX_PAYLOAD}-byte frame cap; nothing was sent")
            encoded.append(b)
        e = self._prepare_entry(sql, None)
        out: List[Union[Result, QueryError]] = []
        try:
            for r, b in zip(rows, encoded):
                try:
                    out.append(self._run_prepared(sql, r, timeout, entry=e, encoded=b))
                except QueryError as exc:
                    out.append(exc)
                    if self.in_transaction:
                        break
        finally:
            self._unpin(e)
        return out

    def execute_many(self, sql: str, rows: Sequence[Sequence[Any]],
                     timeout: Optional[float] = None) -> List[int]:
        """:meth:`execute_each`, raising :class:`~particledb.errors.BatchExecuteError`
        (``sqlstate`` = the first failure's) after the sets ran if any failed.
        Returns per-set affected counts."""
        entries = self.execute_each(sql, rows, timeout)
        errors = [(i, str(x)) for i, x in enumerate(entries) if isinstance(x, QueryError)]
        if errors:
            causes = [x for x in entries if isinstance(x, QueryError)]
            affected = sum(x.rowcount for x in entries if isinstance(x, Result))
            raise BatchExecuteError(errors, affected, len(entries), len(rows), causes[0].sqlstate,
                                    causes[0].code, causes)
        return [x.rowcount for x in entries]  # type: ignore[union-attr]

    def query_arrow(self, sql: str, params: Optional[Sequence[Any]] = None,
                    timeout: Optional[float] = None) -> Any:
        """The result as a ``pyarrow.Table`` (needs pyarrow; :meth:`execute`
        does not)."""
        if not _arrow.HAVE_PYARROW:
            raise QueryError("query_arrow() returns a pyarrow.Table and needs pyarrow: pip install pyarrow "
                             "(execute() / query() / stream() decode results without it)", 0, "0A000")
        p = self.submit(sql, params)
        p._decode = _pyarrow_table
        return p.result(timeout)

    def ping(self, timeout: Optional[float] = None) -> float:
        """Round trip of a PING on the control stream, in milliseconds."""
        with self._ping_lock:
            t0 = time.monotonic()
            with self._mu:
                self._check_open()
                s = _Stream(0, threading.Condition(self._mu), P.F_PING)
                self._streams[0] = s
            self._send(P.v2_frame(P.F_PING, 0, b""))
            self._await(s, timeout if timeout is not None else 30.0)
            _raise_for(s)
            if s.kind != P.F_PONG:
                raise ProtocolError(f"PING answered with 0x{s.kind:02x}")
            return (time.monotonic() - t0) * 1000.0

    def _finish(self, s: _Stream, timeout: Optional[float]) -> Result:
        self._await(s, timeout)
        _raise_for(s)
        return self._decode_result(s)

    def _decode_result(self, s: _Stream) -> Result:
        rolled = bool(s.flags & P.FLAG_TXN_ROLLED_BACK)
        if s.kind in (P.F_ARROW_RESULT, P.F_ARROW_STREAM):
            data = s.payload if s.kind == P.F_ARROW_RESULT else b"".join(s.chunks)
            s.chunks = []
            try:
                schema, rows = _arrow.decode_stream(data, self._use_pyarrow, self._extended)
            except _arrow.ArrowDecodeError as e:
                raise ProtocolError(f"cannot decode the result: {e}") from e
            return Result(schema.names, rows, len(rows), s.notices, schema.fields, rolled, True)
        if s.kind == P.F_OK:
            return Result([], [], P.decode_ok(s.payload), s.notices, None, rolled)
        if s.kind == P.F_BATCH_RESULT:
            br = P.decode_batch_result(s.payload)
            return Result([], [], sum(c for c in br.counts if c > 0), s.notices, None, rolled)
        raise ProtocolError(f"unexpected answer 0x{s.kind:02x} to a statement")

    # ── prepared statements ─────────────────────────────────────────────

    def _prepare_entry(self, sql: str, deadline: Optional[float]) -> _Prep:
        """The cached prepared statement for ``sql`` (PREPARE on first use),
        pinned: the caller must :meth:`_unpin` it."""
        evict: List[_Prep] = []
        with self._cache_lock:
            e = self._cache.get(sql)
            creator = e is None
            if e is not None:
                self._cache.move_to_end(sql)
            else:
                self._stmt_seq += 1
                e = _Prep(f"py_{self._stmt_seq}")
                self._cache[sql] = e
                if len(self._cache) > self._cache_size:
                    for k, old in list(self._cache.items()):
                        if len(self._cache) <= self._cache_size:
                            break
                        if old.refs == 0 and old is not e:
                            del self._cache[k]
                            old.closed = True
                            evict.append(old)
            e.refs += 1
        for old in evict:
            self._close_stmt(old)
        if creator:
            try:
                if not self._has(P.CAP_PREPARED):
                    raise QueryError("the server did not negotiate prepared statements", 0, "0A000")
                s = self._request(P.F_PREPARE, P.prepare_payload(e.name, sql))
                self._await(s, None)
                _raise_for(s)
                if s.kind != P.F_PREPARE_OK or len(s.payload) < 2:
                    raise ProtocolError(f"PREPARE answered with 0x{s.kind:02x}")
                e.nparams = struct.unpack_from("<H", s.payload, 0)[0]
            except BaseException as exc:
                e.error = exc
                with self._cache_lock:
                    if self._cache.get(sql) is e:
                        del self._cache[sql]
                    e.refs -= 1
                e.ready.set()
                raise
            e.ready.set()
        else:
            if not e.ready.is_set():
                # Another thread is preparing it; it drives the reader.
                e.ready.wait()
            if e.error is not None:
                with self._cache_lock:
                    e.refs -= 1
                raise e.error
        return e

    def _unpin(self, e: _Prep) -> None:
        with self._cache_lock:
            e.refs -= 1

    def _close_stmt(self, e: _Prep) -> None:
        try:
            s = self._request(P.F_CLOSE_STMT, P.name_payload(e.name))
            s.ignore = True
        except PdbError:
            pass

    def _execute_frame(self, e: _Prep, params: List[Any], encoded: Optional[bytes] = None,
                       attached: bool = False, flags: int = P.FLAG_END_STREAM) -> _Stream:
        if e.nparams != len(params):
            raise QueryError(f"statement expects {e.nparams} parameter(s), {len(params)} given", 0, "08P01")
        need = P.needs_ext(params)
        have = self.hello.ext if self.hello else 0
        if need & ~have:
            raise QueryError("a parameter needs a capability the server did not negotiate "
                             f"(ext bits 0x{need & ~have:02x})", 0, "0A000")
        payload = P.execute_payload(e.name_b, P.encode_params(params) if encoded is None else encoded,
                                    len(params))
        return self._request(P.F_EXECUTE, payload, flags, attached=attached)

    def _submit_prepared(self, e: _Prep, sql: str, params: List[Any]) -> Pending:
        return Pending(self, self._execute_frame(e, params), self._decode_result)

    def _run_prepared(self, sql: str, params: List[Any], timeout: Optional[float],
                      entry: Optional[_Prep] = None, encoded: Optional[bytes] = None) -> Result:
        e = entry if entry is not None else self._prepare_entry(sql, None)
        try:
            s = self._execute_frame(e, params, encoded)
            try:
                return self._finish(s, timeout)
            except QueryError as exc:
                # 26000: the server no longer has the statement (deallocated):
                # the EXECUTE never ran, so prepare it again under the same
                # name and execute once more.
                if exc.sqlstate != "26000":
                    raise
            self._reprepare(sql, e)
            return self._finish(self._execute_frame(e, params, encoded), timeout)
        finally:
            if entry is None:
                self._unpin(e)

    def _reprepare(self, sql: str, e: _Prep) -> None:
        s = self._request(P.F_PREPARE, P.prepare_payload(e.name, sql))
        self._await(s, None)
        try:
            _raise_for(s)
        except QueryError as exc:
            if exc.sqlstate == "42P05":  # another thread re-prepared it first
                return
            raise
        if s.kind != P.F_PREPARE_OK or len(s.payload) < 2:
            raise ProtocolError(f"PREPARE answered with 0x{s.kind:02x}")
        e.nparams = struct.unpack_from("<H", s.payload, 0)[0]

    def _exec_raw(self, sql: str, params: Optional[Sequence[Any]], timeout: Optional[float],
                  begin: bool = False) -> _Stream:
        """Run one statement and return its ANSWERED stream (``kind``,
        ``payload`` / ``chunks``, ``flags``, ``notices``), raising its error;
        the result is not decoded (the DB-API layer decodes lazily, or hands
        the Arrow bytes to pyarrow / polars untouched). ``begin`` sets
        ``FLAG_TXN_BEGIN``: the server opens a transaction and runs the
        statement in it, one round trip (the JDBC driver's lazy BEGIN)."""
        flags = P.FLAG_END_STREAM | P.FLAG_TXN_BEGIN if begin else P.FLAG_END_STREAM
        if not params:
            s = self._request(P.F_QUERY, sql.encode("utf-8"), flags)
            self._await(s, timeout)
            _raise_for(s)
            return s
        params = list(params)
        e = self._prepare_entry(sql, None)
        try:
            s = self._execute_frame(e, params, None, False, flags)
            self._await(s, timeout)
            try:
                _raise_for(s)
                return s
            except QueryError as exc:
                if exc.sqlstate != "26000":
                    raise
            self._reprepare(sql, e)
            s = self._execute_frame(e, params, None, False, flags)
            self._await(s, timeout)
            _raise_for(s)
            return s
        finally:
            self._unpin(e)

    def _stream(self, sql: Optional[str], params: List[Any], timeout: Optional[float],
                entry: Optional[_Prep] = None, begin: bool = False) -> RowStream:
        flags = P.FLAG_END_STREAM | P.FLAG_TXN_BEGIN if begin else P.FLAG_END_STREAM
        if entry is not None or params:
            e = entry if entry is not None else self._prepare_entry(sql, None)
            try:
                s = self._execute_frame(e, params, attached=True, flags=flags)
            finally:
                if entry is None:
                    self._unpin(e)
        else:
            s = self._request(P.F_QUERY, sql.encode("utf-8"), flags, attached=True)
        return RowStream(self, s, timeout if timeout is not None else self.statement_timeout)

    # ── transactions ────────────────────────────────────────────────────

    def begin(self, isolation: Optional[str] = None) -> None:
        """``BEGIN`` (optionally ``ISOLATION LEVEL <isolation>``)."""
        self.execute("BEGIN" + (f" ISOLATION LEVEL {_isolation(isolation)}" if isolation else ""))

    def commit(self) -> None:
        """``COMMIT``. Raises :class:`~particledb.errors.TransactionRolledBackError`
        when the server ended a FAILED transaction by rolling it back."""
        r = self.execute("COMMIT")
        if r.rolled_back:
            raise TransactionRolledBackError(
                "COMMIT did not commit: the server rolled back the transaction because a "
                "statement in it had failed")

    def rollback(self) -> None:
        self.execute("ROLLBACK")

    def savepoint(self, name: str) -> None:
        self.execute(f"SAVEPOINT {_ident(name)}")

    def rollback_to(self, name: str) -> None:
        self.execute(f"ROLLBACK TO SAVEPOINT {_ident(name)}")

    def release_savepoint(self, name: str) -> None:
        self.execute(f"RELEASE SAVEPOINT {_ident(name)}")

    def set_isolation(self, level: str) -> None:
        """The session's default isolation for transactions it begins from now
        on: ``read committed`` (the server default), ``repeatable read`` or
        ``serializable``."""
        self.execute(f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {_isolation(level)}")

    class _Txn:
        def __init__(self, conn: "Connection", isolation: Optional[str]):
            self.conn = conn
            self.isolation = isolation

        def __enter__(self) -> "Connection":
            self.conn.begin(self.isolation)
            return self.conn

        def __exit__(self, et: Any, ev: Any, tb: Any) -> None:
            if et is None:
                self.conn.commit()
            else:
                try:
                    if not self.conn.closed:
                        self.conn.rollback()
                except PdbError:
                    pass

    def transaction(self, isolation: Optional[str] = None) -> "Connection._Txn":
        """``with conn.transaction(): ...`` -- BEGIN, COMMIT on success,
        ROLLBACK on an exception."""
        return Connection._Txn(self, isolation)

    # ── key-value (native frames) ───────────────────────────────────────

    def _kv(self, ftype: int, key: Union[str, bytes], value: Optional[bytes] = None,
            timeout: Optional[float] = None) -> _Stream:
        s = self._kv_send(ftype, key, value)
        self._await_kv(s, timeout)
        _raise_kv(s)
        return s

    def _kv_send(self, ftype: int, key: Union[str, bytes], value: Optional[bytes] = None) -> _Stream:
        k = key.encode("utf-8") if isinstance(key, str) else bytes(key)
        return self._request(ftype, P.kv_payload(k, value))

    def _await_kv(self, s: _Stream, timeout: Optional[float]) -> None:
        if timeout is None:
            timeout = self.statement_timeout
        deadline = None if timeout is None else time.monotonic() + timeout
        try:
            self._wait(s, lambda: s.done, deadline)
        except _Timeout:
            # A KV request cannot be cancelled (a reset KV reply is dropped):
            # its outcome is unknown, so the connection is closed.
            err = QueryCanceledError(f"KV request did not answer within {timeout}s; outcome unknown, "
                                     "connection closed", 0, "57014", ambiguous=True)
            self._fail_all(err)
            raise err

    def kv_set(self, key: Union[str, bytes], value: Union[str, bytes],
               timeout: Optional[float] = None) -> None:
        """Native ``KV_SET`` (binary-safe). Like RESP ``SET``, it clears any TTL."""
        v = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        self._kv(P.F_KV_SET, key, v, timeout)

    def kv_get(self, key: Union[str, bytes], timeout: Optional[float] = None) -> Optional[bytes]:
        """Native ``KV_GET``: the value's bytes, or ``None`` when absent."""
        return _kv_value(self._kv(P.F_KV_GET, key, None, timeout))

    def kv_delete(self, key: Union[str, bytes], timeout: Optional[float] = None) -> bool:
        """Native ``KV_DEL``: True when the key existed."""
        s = self._kv(P.F_KV_DEL, key, None, timeout)
        return P.decode_ok(s.payload) > 0

    def kv_incr(self, key: Union[str, bytes], timeout: Optional[float] = None) -> int:
        """Native ``KV_INCR`` (+1; an absent key counts from 0)."""
        return _kv_int(self._kv(P.F_KV_INCR, key, None, timeout))

    def kv_command(self, name: str, *args: Any, timeout: Optional[float] = None) -> Any:
        """Any RESP command over the native wire (``F_KV_CMD``): the same
        command implementations, durability and commit mode as RESP. Returns
        the typed reply (see :func:`_protocol.decode_kv_reply`); a RESP error
        is raised with its SQLSTATE."""
        s = self._request(P.F_KV_CMD, P.kv_cmd_payload(name, args))
        self._await_kv(s, timeout)
        _raise_kv(s)
        return P.decode_kv_reply(s.payload)

    def kv_transaction(self, cmds: Sequence[Sequence[Any]], timeout: Optional[float] = None) -> Any:
        """``[[name, *args], ...]`` as ONE MULTI/EXEC transaction: the list of
        replies (an element may be a :class:`_protocol.KvReplyError`), or the
        EXEC error raised."""
        s = self._request(P.F_KV_CMD, P.kv_txn_payload(cmds))
        self._await_kv(s, timeout)
        _raise_kv(s)
        return P.decode_kv_reply(s.payload)

    def kv_pipeline(self, cmds: Sequence[Sequence[Any]], timeout: Optional[float] = None) -> List[Any]:
        """``[[name, *args], ...]`` PIPELINED: every command on its own
        stream, all sent before any answer is read. Each result is the reply,
        or the :class:`PdbError` its command raised (not atomic: see
        :meth:`kv_transaction`)."""
        streams = [self._request(P.F_KV_CMD, P.kv_cmd_payload(c[0], c[1:])) for c in cmds]
        out: List[Any] = []
        for s in streams:
            self._await_kv(s, timeout)
            try:
                _raise_kv(s)
                out.append(P.decode_kv_reply(s.payload))
            except PdbError as e:
                out.append(e)
        return out

    def kv_mget(self, keys: Sequence[Union[str, bytes]], timeout: Optional[float] = None
                ) -> List[Optional[bytes]]:
        """Native ``KV_GET`` of every key, PIPELINED: all requests go out at
        once on their own streams, then the answers are collected in order."""
        streams = [self._kv_send(P.F_KV_GET, k) for k in keys]
        out: List[Optional[bytes]] = []
        for s in streams:
            self._await_kv(s, timeout)
            _raise_kv(s)
            out.append(_kv_value(s))
        return out

    def kv_mset(self, pairs: Sequence[Tuple[Union[str, bytes], Union[str, bytes]]],
                timeout: Optional[float] = None) -> None:
        """Native ``KV_SET`` of every pair, pipelined (each key is its own
        durable write; not atomic across keys, as KV_MSET is not)."""
        streams = [self._kv_send(P.F_KV_SET, k, v.encode("utf-8") if isinstance(v, str) else bytes(v))
                   for k, v in pairs]
        first: Optional[BaseException] = None
        for s in streams:
            self._await_kv(s, timeout)
            try:
                _raise_kv(s)
            except PdbError as e:
                first = first or e
        if first is not None:
            raise first

    # ── native bulk ingest ──────────────────────────────────────────────

    def bulk_insert(self, table: str, columns: Sequence[str], rows: Iterable[Sequence[Any]],
                    timeout: Optional[float] = None) -> int:
        """Load ``rows`` into ``table`` with ONE native bulk ingest (``F_BULK``,
        Arrow over the wire): atomic -- every row or none; inside a transaction
        it joins it. Returns the rows loaded. ``rows`` may be any iterable
        (a generator streams: it is encoded and sent one chunk at a time).
        Values the Arrow writer carries natively: int, float, str, bool,
        bytes, None; anything else travels as text the server casts to the
        column's type."""
        from .arrow_ipc import encode_param_stream, infer_column_type

        cols = list(columns)
        n = len(cols)
        expected = [0]

        def chunks() -> Iterator[bytes]:
            for part in _row_chunks(rows, BULK_CHUNK_ROWS):
                for i, r in enumerate(part):
                    if len(r) != n:
                        raise ValueError(f"row {expected[0] + i} has {len(r)} values, expected {n}")
                yield from _encode_rows(part, cols, encode_param_stream, infer_column_type)
                expected[0] += len(part)

        loaded = self.bulk_load_ipc(table, cols, chunks(), timeout)
        if loaded != expected[0]:
            raise ProtocolError(f"bulk ingest loaded {loaded} of {expected[0]} rows")
        return loaded

    def bulk_load_ipc(self, table: str, columns: Sequence[str], chunks: Iterable[bytes],
                      timeout: Optional[float] = None, window: int = 4) -> int:
        """ONE native bulk ingest of ready-made Arrow IPC streams (each a
        complete stream: schema, batches, EOS; at most the 16 MiB frame cap).
        BEGIN is awaited, then the DATA frames are PIPELINED (``window`` in
        flight) and END carries their count; any failure aborts the load
        (nothing is loaded) and raises the server's error. Returns the rows
        loaded. Nothing is sent for an empty ``chunks``."""
        if not self._has(ext=P.EXT_BULK_INGEST):
            raise QueryError("the server did not negotiate bulk ingest", 0, "0A000")
        it = iter(chunks)
        first = next(it, None)
        if first is None:
            return 0
        with self._bulk_lock:
            def wait(s: _Stream) -> int:
                self._await(s, timeout)
                _raise_for(s)
                return P.decode_ok(s.payload)

            wait(self._request(P.F_BULK, P.bulk_begin_payload(table, list(columns))))
            inflight: Deque[_Stream] = deque()
            sent = 0
            try:
                c: Optional[bytes] = first
                while c is not None:
                    if len(c) + 16 > P.MAX_PAYLOAD:
                        raise ValueError(f"a bulk DATA chunk of {len(c)} bytes exceeds the "
                                         f"{P.MAX_PAYLOAD}-byte frame cap")
                    inflight.append(self._request(P.F_BULK, P.bulk_data_payload(c)))
                    sent += 1
                    while len(inflight) >= window:
                        wait(inflight.popleft())
                    c = next(it, None)
                end = self._request(P.F_BULK, P.bulk_end_payload(sent))
                inflight.append(end)  # drained with the DATA frames on a failure
                while len(inflight) > 1:
                    wait(inflight.popleft())
                return wait(inflight.popleft())
            except BaseException:
                for s in inflight:
                    try:
                        self._await(s, timeout)
                    except PdbError:
                        break
                if not self._closed:
                    try:
                        wait(self._request(P.F_BULK, bytes([P.BULK_ABORT])))
                    except PdbError:
                        pass
                raise

    # ── lifecycle ───────────────────────────────────────────────────────

    def close(self) -> None:
        """Polite GOAWAY, then close the socket; anything in flight fails 08003."""
        if self._closed:
            return
        try:
            if self._wlock.acquire(timeout=0.5):
                try:
                    self._sock.settimeout(0.5)  # a peer that stopped reading cannot hold close()
                    self._sock.sendall(P.v2_frame(P.F_GOAWAY, 0, P.goaway_payload(self._next_sid), 0))
                finally:
                    self._wlock.release()
        except OSError:
            pass
        self._fail_all(ConnectionError("connection closed", 0, "08003"))

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        return (f"Connection({self.transport}, protocol={self.negotiated_protocol!r}, "
                f"closed={self._closed})")


def _row_chunks(rows: Iterable[Sequence[Any]], size: int) -> Iterator[List[Sequence[Any]]]:
    """``rows`` in lists of at most ``size`` (a list is sliced, not copied row by row)."""
    if isinstance(rows, (list, tuple)):
        for a in range(0, len(rows), size):
            yield rows[a: a + size]  # type: ignore[misc]
        return
    part: List[Sequence[Any]] = []
    for r in rows:
        part.append(r)
        if len(part) >= size:
            yield part
            part = []
    if part:
        yield part


def _encode_rows(part: Sequence[Sequence[Any]], cols: List[str], enc: Callable[..., bytes],
                 infer: Callable[[Sequence[Any]], str]) -> Iterator[bytes]:
    """One rows chunk as Arrow IPC streams under the frame cap (halving on overflow)."""
    ipc = enc(part, None, names=cols)  # infers each column's type from its values
    if len(ipc) + 16 <= P.MAX_PAYLOAD:
        yield ipc
        return
    if len(part) == 1:
        raise ValueError("a single row encodes larger than the server's frame cap")
    mid = len(part) // 2
    yield from _encode_rows(part[:mid], cols, enc, infer)
    yield from _encode_rows(part[mid:], cols, enc, infer)


def _raise_kv(s: _Stream) -> None:
    """:func:`_raise_for` for a KV frame's answer. The server's KV handlers
    frame EVERY failure with the generic code 1 (``pdb_wire_mux.rs``
    ``kv_set`` / ``kv_get`` / ``kv_del`` / ``kv_incr``), framing complaints and
    operation errors ("value is not an integer", a read-only refusal) alike,
    and keep the connection open. Code 1 maps to 08P01, which would call a
    type error a broken connection; so a KV error is a :class:`QueryError`
    whose SQLSTATE is the text's own marker when it carries one, else HY000."""
    if s.error is None and s.kind == P.F_ERROR:
        code, msg = P.decode_error(s.payload)
        state, text = P.server_error(code, msg)
        if code == 1 and state == "08P01":
            state = "HY000"
        raise _error_for(state, code, text)
    _raise_for(s)


def _kv_value(s: _Stream) -> Optional[bytes]:
    if s.kind == P.F_KV_NIL:
        return None
    if s.kind != P.F_KV_BULK or len(s.payload) < 4:
        raise ProtocolError(f"KV_GET answered with 0x{s.kind:02x}")
    (n,) = struct.unpack_from("<I", s.payload, 0)
    return bytes(s.payload[4: 4 + n])


def _kv_int(s: _Stream) -> int:
    v = _kv_value(s)
    if v is None:
        raise ProtocolError("KV_INCR answered KV_NIL")
    return int(v.decode("ascii"))


_ISOLATIONS = {"read committed": "READ COMMITTED", "repeatable read": "REPEATABLE READ",
               "serializable": "SERIALIZABLE", "read uncommitted": "READ UNCOMMITTED"}


def _isolation(level: str) -> str:
    v = _ISOLATIONS.get(" ".join(level.lower().replace("_", " ").split()))
    if v is None:
        raise ValueError(f"unknown isolation level {level!r}")
    return v


def _ident(name: str) -> str:
    if not name or not all(c.isalnum() or c == "_" for c in name) or name[0].isdigit():
        return '"' + name.replace('"', '""') + '"'
    return name


def connect(dsn: Optional[str] = None, **kwargs: Any) -> Connection:
    """Open a :class:`Connection`. With no arguments: ``localhost`` on
    :data:`DEFAULT_PORT`, protocol 2 with every capability. ``dsn`` may be
    ``pdb://[user[:password]@]host[:port]`` (or ``particledb://``); keyword
    arguments are :class:`Connection`'s."""
    if dsn:
        parts = parse_connection_url(dsn)
        if parts.scheme.lower() not in ("pdb", "particledb"):
            raise ValueError("unsupported URL scheme (use pdb:// or particledb://)")
        kwargs.setdefault("host", parts.hostname or "localhost")
        if parts.port:
            kwargs.setdefault("port", parts.port)
        if parts.username:
            kwargs.setdefault("user", unquote(parts.username))
        if parts.password:
            kwargs.setdefault("password", unquote(parts.password))
    return Connection(**kwargs)
