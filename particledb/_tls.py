"""TLS configuration shared by the blocking and asyncio native clients."""

import ssl
import select
import socket
import threading
import time
from typing import Optional


def context(sslmode: str = "disable", sslrootcert: Optional[str] = None,
            sslcert: Optional[str] = None, sslkey: Optional[str] = None) -> Optional[ssl.SSLContext]:
    mode = sslmode.lower()
    if mode not in ("disable", "require", "verify-ca", "verify-full"):
        raise ValueError("sslmode must be disable, require, verify-ca or verify-full")
    if bool(sslcert) != bool(sslkey):
        raise ValueError("sslcert and sslkey must be supplied together")
    if mode == "disable":
        return None
    if mode == "require" and not sslrootcert:
        result = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        result.check_hostname = False
        result.verify_mode = ssl.CERT_NONE
    else:
        result = ssl.create_default_context(cafile=sslrootcert)
        result.check_hostname = mode == "verify-full"
    result.minimum_version = ssl.TLSVersion.TLSv1_2
    if sslcert:
        result.load_cert_chain(sslcert, sslkey)
    return result


class Socket:
    """Serialize SSL state changes while allowing reads and writes to wait independently."""

    def __init__(self, raw: ssl.SSLSocket):
        self.raw = raw
        self.raw.setblocking(False)
        self._lock = threading.Lock()
        self._timeout = None
        self._peeked = b""

    def _io(self, operation, deadline):
        while True:
            if deadline is not None and time.monotonic() >= deadline:
                raise socket.timeout("TLS operation timed out")
            with self._lock:
                try:
                    return operation()
                except ssl.SSLWantReadError:
                    readable = True
                except ssl.SSLWantWriteError:
                    readable = False
            left = None if deadline is None else deadline - time.monotonic()
            if left is not None and left <= 0:
                raise socket.timeout("TLS operation timed out")
            try:
                ready = select.select([self.raw] if readable else [], [] if readable else [self.raw], [], left)
            except ValueError as error:
                raise ConnectionResetError("TLS connection closed") from error
            if not ready[0] and not ready[1]:
                raise socket.timeout("TLS operation timed out")

    def recv_deadline(self, size, deadline):
        if self._peeked:
            result, self._peeked = self._peeked[:size], self._peeked[size:]
            return result
        return self._io(lambda: self.raw.recv(size), deadline)

    def recv(self, size, flags=0):
        if flags:
            if flags != socket.MSG_PEEK | socket.MSG_DONTWAIT:
                raise ValueError("unsupported TLS recv flags")
            if self._peeked:
                return self._peeked[:size]
            with self._lock:
                try:
                    self._peeked = self.raw.recv(size)
                    return self._peeked
                except (ssl.SSLWantReadError, ssl.SSLWantWriteError):
                    raise BlockingIOError() from None
        deadline = None if self._timeout is None else time.monotonic() + self._timeout
        return self.recv_deadline(size, deadline)

    def sendall(self, data):
        deadline = None if self._timeout is None else time.monotonic() + self._timeout
        return self.sendall_deadline(data, deadline)

    def sendall_deadline(self, data, deadline):
        remaining = memoryview(data)
        while remaining:
            sent = self._io(lambda: self.raw.send(remaining), deadline)
            if not sent:
                raise ConnectionResetError("TLS connection closed during write")
            remaining = remaining[sent:]

    def settimeout(self, timeout):
        self._timeout = timeout

    def gettimeout(self):
        return self._timeout

    def fileno(self):
        return self.raw.fileno()

    def shutdown(self, how):
        self.raw.shutdown(how)

    def close(self):
        self.raw.close()
