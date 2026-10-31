"""Unix-domain-socket selection for the native wire client.

Unit part (fake protocol-2 servers on a TCP port P and on
``<dir>/.s.PDB.P``): which hosts try the socket, the path convention
(``<unix_socket_dir>/.s.PDB.<port>``, the server's ``--unix-socket-dir``),
the ``prefer_unix_socket=False`` opt-out, the fallback to TCP when the file
is absent or nothing accepts on it, and an explicit ``unix_socket_path`` that
must work.

Live part (skipped unless ``PDB_NATIVE_PORT`` is set; a ``--no-auth`` server
with its default socket in ``/tmp``): ``health()["protocol"]`` reports which
transport the pooled connection really used.

Run::

    python -m pytest tests/test_unix_socket.py -v
    PDB_NATIVE_PORT=7173 python -m pytest tests/test_unix_socket.py -v
"""

from __future__ import annotations

import os
import socket
import tempfile
import unittest

from particledb.connection import Connection
from particledb.errors import _ConnectFailed
from particledb.pdb_wire import (
    DEFAULT_UNIX_SOCKET_DIR,
    PdbWireClient,
    is_local_host,
    unix_socket_path,
)

from tests.fake_v2 import FakeV2Server

PORT = os.environ.get("PDB_NATIVE_PORT")


class LocalHostLiterals(unittest.TestCase):
    def test_local_literals(self) -> None:
        for h in ("localhost", "LOCALHOST", "127.0.0.1", "127.9.9.9", "::1", "[::1]"):
            self.assertTrue(is_local_host(h), h)

    def test_remote_hosts(self) -> None:
        # No DNS lookup: a name is never "local", even one that resolves to loopback.
        for h in ("10.0.0.7", "db.example.com", "192.0.2.1", "::2"):
            self.assertFalse(is_local_host(h), h)

    def test_path_convention(self) -> None:
        self.assertEqual(DEFAULT_UNIX_SOCKET_DIR, "/tmp")
        self.assertEqual(unix_socket_path(7173), os.path.join("/tmp", ".s.PDB.7173"))
        self.assertEqual(unix_socket_path(7001, "/run/pdb"), os.path.join("/run/pdb", ".s.PDB.7001"))


@unittest.skipUnless(hasattr(socket, "AF_UNIX"), "no AF_UNIX on this platform")
class Selection(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="pdbpy", dir="/tmp" if os.name != "nt" else None)
        self.addCleanup(temporary.cleanup)
        self.dir = temporary.name
        self.tcp = FakeV2Server()
        self.addCleanup(self.tcp.close)
        self.port = self.tcp.port
        self.path = unix_socket_path(self.port, self.dir)

    def _route(self, host: str, **kw) -> str:
        c = Connection(host, self.port, unix_socket_dir=kw.pop("sock_dir", self.dir), **kw)
        try:
            return c.transport.split(":", 1)[0]
        finally:
            c.close()

    def test_live_socket_is_used_for_local_hosts(self) -> None:
        uds = FakeV2Server(unix_path=self.path)
        self.addCleanup(uds.close)
        for h in ("localhost", "127.0.0.1"):
            self.assertEqual(self._route(h), "unix", h)
        # Opt-out.
        self.assertEqual(self._route("127.0.0.1", prefer_unix_socket=False), "tcp")
        # Another dir (the server's --unix-socket-dir differs): no file there -> TCP.
        self.assertEqual(self._route("127.0.0.1", sock_dir="/nonexistent"), "tcp")
        self.assertEqual(uds.accepts, 2)

    def test_absent_or_stale_socket_falls_back_to_tcp(self) -> None:
        self.assertEqual(self._route("127.0.0.1"), "tcp")
        # Stale: bound then closed, the file stays and nobody accepts.
        st = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        st.bind(self.path)
        st.close()
        self.assertTrue(os.path.exists(self.path))
        self.assertEqual(self._route("127.0.0.1"), "tcp")

    def test_explicit_socket_path_never_falls_back(self) -> None:
        with self.assertRaises(_ConnectFailed):
            Connection("127.0.0.1", self.port, unix_socket_path=self.path)


@unittest.skipUnless(PORT, "PDB_NATIVE_PORT not set (live native-wire test)")
class LiveTransport(unittest.TestCase):
    def test_loopback_uses_the_server_socket(self) -> None:
        port = int(PORT or 0)
        if not os.path.exists(unix_socket_path(port)):
            self.skipTest(f"server has no socket at {unix_socket_path(port)}")
        with PdbWireClient(host="127.0.0.1", port=port, pool_size=1) as db:
            h = db.health()
            self.assertTrue(h["connected"])
            self.assertEqual(h["protocol"], "unix")
        with PdbWireClient(host="127.0.0.1", port=port, pool_size=1, prefer_unix_socket=False) as db:
            self.assertEqual(db.health()["protocol"], "tcp")
        with PdbWireClient(host="127.0.0.1", port=port, pool_size=1, unix_socket_dir="/nonexistent") as db:
            self.assertEqual(db.health()["protocol"], "tcp")

    def test_select_one_over_both(self) -> None:
        port = int(PORT or 0)
        for prefer in (True, False):
            with PdbWireClient(host="127.0.0.1", port=port, pool_size=1, prefer_unix_socket=prefer) as db:
                db.execute("SELECT 1")


if __name__ == "__main__":
    unittest.main()
