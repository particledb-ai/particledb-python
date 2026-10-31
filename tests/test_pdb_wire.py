"""``PdbWireClient`` (the pool), ``Session``, and the ``ParticleDB`` transport
rules, against the fake protocol-2 server (``tests/fake_v2.py``). No
ParticleDB server.
"""

from __future__ import annotations

import logging
import os
import struct
import threading
import time
import unittest
from unittest.mock import patch

import particledb
from particledb import _protocol as P
from particledb import connection as C
from particledb import pdb_wire as pw
from particledb.client import ParticleDB, ParticleDBConnectionError, ParticleDBError
from particledb.errors import ConnectionError as WireConnectionError
from particledb.errors import DiskFullError, QueryError, TransactionRolledBackError

from tests.fake_v2 import FakeV2Server, err, ok, rows_ipc


class Server:
    """A fake server with a tiny SQL dialect: ``SELECT n`` returns n, ``BEGIN``
    opens a block, ``COMMIT``/``ROLLBACK`` end it, ``FAIL <code>`` errors,
    ``FAILTXN`` fails an open block, ``COMMITFAILED`` answers ROLLED_BACK."""

    def __init__(self, **kw):
        self.txn = {}
        self.srv = FakeV2Server(handler=self.handle, **kw)
        self.port = self.srv.port

    def handle(self, c, f) -> None:
        st = self.txn.get(c.index, 0)
        if f.type == P.F_QUERY:
            q = f.payload.decode()
            flags = P.FLAG_END_STREAM
            if q == "BEGIN":
                st = P.FLAG_TXN_OPEN
            elif q in ("COMMIT", "ROLLBACK"):
                rolled = st == P.FLAG_TXN_FAILED and q == "COMMIT"
                st = 0
                if rolled:
                    flags |= P.FLAG_TXN_ROLLED_BACK
            elif q == "FAILTXN":
                st = P.FLAG_TXN_FAILED
                self.txn[c.index] = st
                c.send(P.F_ERROR, f.sid, err(23505, "duplicate key"), flags | st)
                return
            elif q.startswith("FAIL "):
                c.send(P.F_ERROR, f.sid, err(int(q.split()[1]), "failed as asked"), flags | st)
                return
            elif q.startswith("SELECT "):
                self.txn[c.index] = st
                c.send(P.F_ARROW_RESULT, f.sid, rows_ipc([[int(q.split()[1])]], ["n"]), flags | st)
                return
            self.txn[c.index] = st
            c.send(P.F_OK, f.sid, ok(1), flags | st)
        elif f.type == P.F_BATCH:
            n = struct.unpack_from("<I", f.payload, 0)[0]
            c.send(P.F_BATCH_RESULT, f.sid, struct.pack("<I", n) + (bytes([P.F_OK]) + struct.pack("<Q", 1)) * n)
        elif f.type == P.F_PREPARE:
            c.send(P.F_PREPARE_OK, f.sid, struct.pack("<H", 1))
        elif f.type == P.F_EXECUTE:
            c.send(P.F_ARROW_RESULT, f.sid, rows_ipc([[42]], ["x"]), P.FLAG_END_STREAM | st)
        elif f.type in (P.F_AUTH, P.F_CLOSE_STMT):
            c.send(P.F_OK, f.sid, ok(1))

    def close(self) -> None:
        self.srv.close()


def client(s: Server, **kw) -> pw.PdbWireClient:
    kw.setdefault("prefer_unix_socket", False)
    return pw.PdbWireClient(host="127.0.0.1", port=s.port, **kw)


class Defaults(unittest.TestCase):
    def test_one_default_port_constant(self) -> None:
        self.assertEqual(C.DEFAULT_PORT, 7173)  # the server's --pdb-port default: the database port
        self.assertIs(pw.DEFAULT_PORT, C.DEFAULT_PORT)
        self.assertIs(particledb.DEFAULT_PORT, C.DEFAULT_PORT)
        self.assertEqual(pw.PdbWireClient().port, C.DEFAULT_PORT)
        with patch("particledb.client.ParticleDB._init_wire", return_value=object()):
            self.assertEqual(ParticleDB().pdb_port, C.DEFAULT_PORT)
        # nothing else in the package spells the port
        here = os.path.dirname(particledb.__file__)
        for name in os.listdir(here):
            if name.endswith(".py") and name != "connection.py":
                with open(os.path.join(here, name), encoding="utf-8") as f:
                    for i, line in enumerate(f, 1):
                        code = line.split("#")[0]
                        self.assertNotRegex(code, r"=\s*7173\b", f"{name}:{i} hard-codes the native port")

    def test_pool_and_retry_defaults(self) -> None:
        self.assertEqual(pw.DEFAULT_POOL_SIZE, min(os.cpu_count() or 4, 16))
        c = pw.PdbWireClient()
        self.assertEqual((c.timeout, c.max_retries, c.pool_size), (30.0, 3, pw.DEFAULT_POOL_SIZE))

    def test_public_api(self) -> None:
        for name in ("connect", "Connection", "Result", "RowStream", "PreparedStatement", "ParticleDB",
                     "PdbWireClient", "Session", "Int2", "Int4", "Float4", "Json", "Jsonb", "QueryError",
                     "QueryCanceledError", "BatchExecuteError", "TransactionRolledBackError", "DEFAULT_PORT"):
            self.assertTrue(hasattr(particledb, name), name)
            self.assertIn(name, particledb.__all__)


class Pool(unittest.TestCase):
    def setUp(self) -> None:
        self.s = Server()
        self.addCleanup(self.s.close)

    def test_reuses_idle_connection(self) -> None:
        c = client(self.s, pool_size=4)
        for i in range(5):
            self.assertEqual(c.query(f"SELECT {i}"), [{"n": i}])
        self.assertEqual(self.s.srv.accepts, 1)
        c.close()

    def test_blocks_when_full_then_times_out(self) -> None:
        c = client(self.s, pool_size=1, timeout=0.3)
        s1 = c.session()
        t0 = time.monotonic()
        with self.assertRaises(WireConnectionError):
            c.query("SELECT 1")
        self.assertGreaterEqual(time.monotonic() - t0, 0.25)
        s1.close()
        self.assertEqual(c.query("SELECT 2"), [{"n": 2}])
        c.close()

    def test_never_exceeds_max(self) -> None:
        c = client(self.s, pool_size=3)
        ts = [threading.Thread(target=lambda: [c.query("SELECT 1") for _ in range(20)]) for _ in range(8)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(20)
        self.assertLessEqual(self.s.srv.accepts, 3)
        c.close()

    def test_server_error_keeps_the_connection(self) -> None:
        c = client(self.s, pool_size=1)
        with self.assertRaises(QueryError) as cm:
            c.execute("FAIL 42601")
        self.assertEqual(cm.exception.sqlstate, "42601")
        c.query("SELECT 1")
        self.assertEqual(self.s.srv.accepts, 1)  # answered: the socket was in sync and outside a block
        c.close()

    def test_connection_left_in_a_block_is_not_pooled(self) -> None:
        c = client(self.s, pool_size=1)
        c.execute("BEGIN")  # a single-statement API used to open a block
        c.query("SELECT 1")
        self.assertEqual(self.s.srv.accepts, 2)  # the BEGIN's socket was closed, not lent on
        c.close()

    def test_disk_full_class(self) -> None:
        c = client(self.s)
        with self.assertRaises(DiskFullError):
            c.execute("FAIL 53100")
        c.close()

    def test_health_metrics_repr(self) -> None:
        c = client(self.s)
        h = c.health()
        self.assertTrue(h["connected"])
        self.assertEqual(h["protocol"], "tcp")
        self.assertEqual(h["wire_protocol"], "pdb-native-v2")
        self.assertIn("v2_mux", h["capabilities"])
        self.assertEqual((h["pool_idle"], h["pool_active"], h["pool_total"]), (1, 0, 1))
        self.assertIsInstance(c.metrics(), str)
        self.assertIn("PdbWireClient(host='127.0.0.1'", repr(c))
        c.close()
        dead = pw.PdbWireClient(host="127.0.0.1", port=1, max_retries=0, prefer_unix_socket=False)
        self.assertFalse(dead.health()["connected"])
        self.assertFalse(dead.ping())

    def test_retry_only_before_send_with_backoff(self) -> None:
        sleeps = []
        with patch("particledb.pdb_wire._sleep", sleeps.append):
            c = pw.PdbWireClient(host="127.0.0.1", port=1, max_retries=3, prefer_unix_socket=False)
            with self.assertRaises(WireConnectionError) as cm:
                c.query("SELECT 1")
        self.assertIn("all 4 connection attempts failed", str(cm.exception))
        self.assertEqual(sleeps, [0.1, 0.2, 0.4])

    def test_auth_threaded_to_every_connection(self) -> None:
        c = client(self.s, username="app", password="")
        c.query("SELECT 1")
        a = self.s.srv.requests(P.F_AUTH)
        self.assertEqual([f.payload for f in a], [P.auth_payload("app", "")])
        c.close()
        c2 = client(self.s)
        c2.query("SELECT 1")
        self.assertEqual(len(self.s.srv.requests(P.F_AUTH)), 1)  # none without credentials
        c2.close()

    def test_batched_transaction(self) -> None:
        c = client(self.s)
        with c.transaction() as t:
            t.execute("INSERT 1")
            t.execute("INSERT 2")
        b = self.s.srv.requests(P.F_BATCH)
        self.assertEqual(b[0].payload, P.batch_payload(["BEGIN", "INSERT 1", "INSERT 2", "COMMIT"]))
        with self.assertRaises(RuntimeError):
            with c.transaction() as t:
                t.execute("INSERT 3")
                raise RuntimeError("abort")
        self.assertEqual(len(self.s.srv.requests(P.F_BATCH)), 1)  # nothing was sent
        c.close()

    def test_params_go_prepared(self) -> None:
        c = client(self.s)
        self.assertEqual(c.query("SELECT $1", [1]), [{"x": 42}])
        self.assertEqual(len(self.s.srv.requests(P.F_PREPARE)), 1)
        c.close()


class Sessions(unittest.TestCase):
    def setUp(self) -> None:
        self.s = Server()
        self.addCleanup(self.s.close)

    def test_clean_commit_pools_and_open_block_discards(self) -> None:
        c = client(self.s, pool_size=1)
        with c.session() as s:
            with s.transaction():
                s.execute("INSERT 1")
        self.assertEqual(c._pool.idle_count, 1)
        s = c.session()
        s.execute("BEGIN")
        s.close()  # the SERVER says a block is open: discarded
        self.assertEqual(c._pool.idle_count, 0)
        self.assertEqual(c._pool.total_count, 0)
        c.close()

    def test_unfinished_stream_is_not_pooled(self) -> None:
        data = rows_ipc([[i] for i in range(100)], ["n"])

        def h(c, f):
            if f.type == P.F_QUERY and f.payload == b"STREAM":
                c.send(P.F_ARROW_STREAM, f.sid, data[:40], P.FLAG_MORE_CHUNKS)  # never finished
            else:
                self.s.handle(c, f)
        self.s.srv.handler = h
        c = client(self.s, pool_size=1)
        s = c.session()
        s.stream("STREAM")  # abandoned mid-result
        s.close()
        self.assertEqual(c._pool.total_count, 0, "a connection with a request in flight was pooled")
        self.assertEqual(c.query("SELECT 3"), [{"n": 3}])
        c.close()

    def test_swallowed_failure_makes_commit_raise(self) -> None:
        c = client(self.s)
        s = c.session()
        with self.assertRaises(TransactionRolledBackError):
            with s.transaction():
                try:
                    s.execute("FAILTXN")
                except QueryError:
                    pass
        s.close()
        c.close()


class Transports(unittest.TestCase):
    def test_default_is_native_and_fails_closed(self) -> None:
        with self.assertRaises(ParticleDBConnectionError) as cm:
            ParticleDB(host="127.0.0.1", pdb_port=1, prefer_unix_socket=False)
        self.assertTrue(cm.exception.sqlstate.startswith("08"))
        self.assertIsInstance(cm.exception, ConnectionError)  # the builtin, for old callers

    def test_v1_only_server_raises_even_under_auto(self) -> None:
        srv = FakeV2Server(hello=bytes([1, 0x01, 1]) + struct.pack("<II", 1, 0))
        try:
            for t in ("pdb_wire", "auto"):
                with self.assertRaises(ParticleDBConnectionError, msg=t):
                    ParticleDB(host="127.0.0.1", pdb_port=srv.port, transport=t, prefer_unix_socket=False)
        finally:
            srv.close()

    def test_auto_falls_back_only_when_nothing_listens_and_says_so(self) -> None:
        with self.assertLogs("particledb", logging.WARNING) as logs:
            db = ParticleDB(host="127.0.0.1", pdb_port=1, transport="auto", prefer_unix_socket=False)
        self.assertEqual(db._active_transport(), "http")
        self.assertIn("not reachable", db.fallback_reason)
        self.assertTrue(any("HTTP API" in m for m in logs.output))

    def test_native_when_reachable(self) -> None:
        s = Server()
        try:
            db = ParticleDB(host="127.0.0.1", pdb_port=s.port, prefer_unix_socket=False)
            self.assertEqual(db._active_transport(), "pdb_wire")
            self.assertEqual(db.query("SELECT 5"), [{"n": 5}])
            self.assertIsNone(db.fallback_reason)
            with self.assertRaises(ParticleDBError) as cm:
                db.execute("FAIL 22012")
            self.assertEqual(cm.exception.sqlstate, "22012")
            db.close()
        finally:
            s.close()

    def test_http_and_invalid(self) -> None:
        db = ParticleDB(host="localhost", transport="http")
        self.assertEqual(db._active_transport(), "http")
        with self.assertRaises(ValueError):
            ParticleDB(transport="carrier-pigeon")


if __name__ == "__main__":
    unittest.main()
