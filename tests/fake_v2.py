"""A scriptable fake protocol-2 server for client tests (no ParticleDB needed).

It speaks the real framing: a v1-framed HELLO answered by a v1-framed
HELLO_OK (or an ERROR, or a dropped socket), then v2 frames both ways. Each
request frame is handed to ``handler(conn, frame)``, which answers with
``conn.send(...)`` (in any order, on any thread) or not at all. Every frame
received is recorded in ``server.frames`` (connection index, type, flags,
stream id, payload) and every HELLO payload in ``server.hellos``.
"""

from __future__ import annotations

import socket
import struct
import threading
from typing import Callable, List, NamedTuple, Optional

from particledb import _protocol as P
from particledb.arrow_ipc import encode_param_stream


class Frame(NamedTuple):
    conn: int
    type: int
    flags: int
    sid: int
    payload: bytes


def _recv_exact(s: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            raise EOFError
        buf += chunk
    return buf


def hello_ok(caps: int = P.CAP_V2_MUX | P.CAP_PREPARED | P.CAP_NUMERIC_TEXT | P.CAP_STATEMENT_NOTICE,
             ext: int = P.DEFAULT_EXT, max_streams: int = 8, fmt: int = P.FMT_ARROW_IPC) -> bytes:
    return bytes([2, caps, fmt]) + struct.pack("<II", max_streams, 65536) + bytes([1, ext])


def rows_ipc(rows: List[list], names: Optional[List[str]] = None) -> bytes:
    return encode_param_stream(rows, names=names)


def ok(n: int = 0) -> bytes:
    return struct.pack("<Q", n)


def err(code: int, msg: str) -> bytes:
    return struct.pack("<H", code) + msg.encode()


class FakeConn:
    def __init__(self, server: "FakeV2Server", sock: socket.socket, index: int):
        self.server = server
        self.sock = sock
        self.index = index
        self.wlock = threading.Lock()
        self.closed = False

    def send(self, ftype: int, sid: int, payload: bytes = b"", flags: int = P.FLAG_END_STREAM) -> None:
        with self.wlock:
            if not self.closed:
                try:
                    self.sock.sendall(P.v2_frame(ftype, sid, payload, flags))
                except OSError:
                    pass

    def close(self) -> None:
        self.closed = True
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()


def default_handler(conn: FakeConn, f: Frame) -> None:
    if f.type == P.F_QUERY:
        conn.send(P.F_ARROW_RESULT, f.sid, rows_ipc([[1]], ["?column?"]))
    elif f.type == P.F_PREPARE:
        sql = f.payload[8 + struct.unpack_from("<I", f.payload, 0)[0]:].decode()
        conn.send(P.F_PREPARE_OK, f.sid, struct.pack("<H", sql.count("$")))
    elif f.type == P.F_EXECUTE:
        conn.send(P.F_OK, f.sid, ok(1))
    elif f.type in (P.F_CLOSE_STMT, P.F_KV_SET, P.F_KV_DEL, P.F_AUTH):
        conn.send(P.F_OK, f.sid, ok(1))
    elif f.type == P.F_KV_GET:
        conn.send(P.F_KV_NIL, f.sid)


class FakeV2Server:
    def __init__(self, handler: Callable[[FakeConn, Frame], None] = default_handler,
                 hello: Optional[bytes] = None, hello_error: Optional[bytes] = None,
                 drop_hello: int = 0, unix_path: Optional[str] = None):
        self.handler = handler
        self.hello = hello if hello is not None else hello_ok()
        self.hello_error = hello_error
        self.drop_hello = drop_hello
        self.frames: List[Frame] = []
        self.hellos: List[bytes] = []
        self.conns: List[FakeConn] = []
        self.lock = threading.Lock()
        if unix_path:
            self.lsock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.lsock.bind(unix_path)
            self.port = 0
        else:
            self.lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.lsock.bind(("127.0.0.1", 0))
            self.port = self.lsock.getsockname()[1]
        self.lsock.listen(64)
        self.stopped = False
        threading.Thread(target=self._accept, daemon=True).start()

    @property
    def accepts(self) -> int:
        return len(self.conns)

    def requests(self, ftype: Optional[int] = None) -> List[Frame]:
        with self.lock:
            return [f for f in self.frames if ftype is None or f.type == ftype]

    def _accept(self) -> None:
        while not self.stopped:
            try:
                s, _ = self.lsock.accept()
            except OSError:
                return
            with self.lock:
                c = FakeConn(self, s, len(self.conns))
                self.conns.append(c)
            threading.Thread(target=self._serve, args=(c,), daemon=True).start()

    def _serve(self, c: FakeConn) -> None:
        s = c.sock
        try:
            plen, sid, t, _fl = struct.unpack("<IHBB", _recv_exact(s, 8))
            payload = _recv_exact(s, plen)
            with self.lock:
                self.hellos.append(payload)
                drop = self.drop_hello > 0
                if drop:
                    self.drop_hello -= 1
            if drop:
                c.close()
                return
            if self.hello_error is not None:
                s.sendall(P.v1_frame(P.F_ERROR, self.hello_error))
                c.close()
                return
            s.sendall(P.v1_frame(P.F_HELLO_OK, self.hello))
            while True:
                hdr = _recv_exact(s, 11)
                t, flags, sid, n = P.parse_v2_header(hdr)
                payload = _recv_exact(s, n) if n else b""
                f = Frame(c.index, t, flags, sid, payload)
                with self.lock:
                    self.frames.append(f)
                if t == P.F_GOAWAY:
                    c.close()
                    return
                if t == P.F_PING:  # the control stream answers itself
                    c.send(P.F_PONG, 0)
                    continue
                self.handler(c, f)
        except (EOFError, OSError, ValueError):
            pass

    def close(self) -> None:
        self.stopped = True
        try:
            self.lsock.close()
        except OSError:
            pass
        for c in list(self.conns):
            try:
                c.close()
            except OSError:
                pass
