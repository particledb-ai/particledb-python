"""Ambiguous transport failures are surfaced, never retried.

Once any byte of a request may have reached the server, a socket error, EOF,
reset, or a client timeout the server never answered is AMBIGUOUS: the server
may have executed and committed it. Resending would apply the writes twice (a
``SELECT`` is not necessarily read-only either -- ``SELECT KV_SET(...)``), and
returning the connection to the pool would hand the next caller a socket that
may still deliver the late reply or sit inside a transaction.

These tests run the fake protocol-2 server (``tests/fake_v2.py``, no
ParticleDB needed) with a per-request plan and COUNT the requests it received
and the connections it accepted:

* a request whose connection dies after the server read it is raised as
  ``ConnectionError`` and was received EXACTLY ONCE;
* the connection is discarded -- the next call opens a new socket;
* a client timeout is first a CANCEL: a server that answers it leaves the
  connection usable; one that never answers makes the timeout ambiguous and
  the socket is discarded;
* a failure BEFORE any byte of the request was written (connect refused, the
  HELLO handshake on a fresh connection dropped) is still retried.
"""

from __future__ import annotations

import struct
import threading
import unittest
from typing import List
from unittest.mock import patch

from particledb import _protocol as P
from particledb.errors import ConnectionError as PdbConnectionError, QueryCanceledError
from particledb.pdb_wire import PdbWireClient

from tests.fake_v2 import FakeV2Server, err, ok


def _raised(fn):
    try:
        fn()
    except Exception as exc:  # noqa: BLE001 -- the test inspects it
        return exc
    return None


class Plan:
    """One action per QUERY / BATCH request: ``close``, ``hang``, ``ok``."""

    def __init__(self, plan: List[str], answer_cancel: bool = False):
        self.plan = list(plan)
        self.answer_cancel = answer_cancel
        self.requests = 0
        self.lock = threading.Lock()

    def __call__(self, c, f) -> None:
        if f.type == P.F_RST_STREAM:
            if self.answer_cancel:
                c.send(P.F_ERROR, f.sid, err(57014, "canceling statement due to user request"))
            return
        if f.type not in (P.F_QUERY, P.F_BATCH):
            return
        with self.lock:
            self.requests += 1
            action = self.plan.pop(0) if self.plan else "ok"
        if action == "close":
            c.close()
        elif action == "hang":
            pass
        elif f.type == P.F_QUERY:
            c.send(P.F_OK, f.sid, ok(1))
        else:
            n = struct.unpack_from("<I", f.payload, 0)[0]
            c.send(P.F_BATCH_RESULT, f.sid, struct.pack("<I", n) + (bytes([P.F_OK]) + struct.pack("<Q", 1)) * n)


class AmbiguousFailureTests(unittest.TestCase):
    def setUp(self) -> None:
        s = patch("particledb.pdb_wire.RETRY_BASE_DELAY_S", 0.01)
        s.start()
        self.addCleanup(s.stop)

    def _server(self, plan: List[str], drop_hello: int = 0, answer_cancel: bool = False):
        p = Plan(plan, answer_cancel)
        srv = FakeV2Server(handler=p, drop_hello=drop_hello)
        self.addCleanup(srv.close)
        return srv, p

    def _client(self, srv: FakeV2Server, timeout: float = 5.0, **kw) -> PdbWireClient:
        c = PdbWireClient(host="127.0.0.1", port=srv.port, pool_size=2, timeout=timeout, max_retries=3,
                          prefer_unix_socket=False, **kw)
        self.addCleanup(c.close)
        return c

    def test_batch_dropped_after_send_is_raised_and_sent_once(self) -> None:
        srv, p = self._server(["close"])
        c = self._client(srv)
        e = _raised(lambda: c.batch(["INSERT INTO t VALUES (1)", "INSERT INTO t VALUES (2)"]))
        self.assertEqual(p.requests, 1, "the batch was resent")
        self.assertIsInstance(e, PdbConnectionError)
        self.assertEqual(e.sqlstate, "08006")
        self.assertEqual(c.batch(["INSERT INTO t VALUES (3)"]), 1)
        self.assertEqual(srv.accepts, 2)
        self.assertEqual(p.requests, 2)

    def test_transaction_commit_dropped_is_raised_and_sent_once(self) -> None:
        srv, p = self._server(["close"])
        c = self._client(srv)

        def txn_body() -> None:
            with c.transaction() as txn:
                txn.execute("INSERT INTO t VALUES (1)")

        e = _raised(txn_body)
        self.assertEqual(p.requests, 1, "the transaction was resent")
        self.assertIsInstance(e, PdbConnectionError)

    def test_single_statement_dropped_is_raised_and_sent_once(self) -> None:
        srv, p = self._server(["close"])
        c = self._client(srv)
        e = _raised(lambda: c.execute("SELECT KV_SET('k', 'v')"))
        self.assertEqual(p.requests, 1, "the statement was resent")
        self.assertIsInstance(e, PdbConnectionError)
        self.assertEqual(c.execute("SELECT 1"), 1)
        self.assertEqual(srv.accepts, 2)

    def test_unanswered_timeout_is_ambiguous_sent_once_and_socket_discarded(self) -> None:
        srv, p = self._server(["hang"])
        c = self._client(srv, timeout=0.3, cancel_grace=0.2)
        e = _raised(lambda: c.execute("UPDATE t SET x = x + 1"))
        self.assertEqual(p.requests, 1, "the timed-out statement was resent")
        self.assertIsInstance(e, QueryCanceledError)
        self.assertTrue(e.ambiguous)
        self.assertEqual(len(srv.requests(P.F_RST_STREAM)), 1)  # it was asked to cancel first
        self.assertEqual(c.execute("SELECT 1"), 1)
        self.assertEqual(srv.accepts, 2)  # the timed-out socket was not reused

    def test_answered_timeout_keeps_the_socket(self) -> None:
        srv, p = self._server(["hang"], answer_cancel=True)
        c = self._client(srv, timeout=0.3)
        e = _raised(lambda: c.execute("UPDATE t SET x = x + 1"))
        self.assertIsInstance(e, QueryCanceledError)
        self.assertFalse(e.ambiguous)
        self.assertEqual(e.sqlstate, "57014")
        self.assertEqual(c.execute("SELECT 1"), 1)
        self.assertEqual(srv.accepts, 1)  # the outcome was known: socket kept

    def test_refused_connect_is_retried(self) -> None:
        srv, p = self._server([])
        import socket as _socket

        real = _socket.create_connection
        calls = {"n": 0}

        def flaky(*a, **k):
            calls["n"] += 1
            if calls["n"] == 1:
                raise ConnectionRefusedError("refused (test)")
            return real(*a, **k)

        with patch("particledb.connection.socket.create_connection", flaky):
            c = self._client(srv)
            self.assertEqual(c.execute("SELECT 1"), 1)
        self.assertEqual(calls["n"], 2)
        self.assertEqual(srv.accepts, 1)
        self.assertEqual(p.requests, 1)

    def test_dropped_handshake_on_fresh_connection_is_retried(self) -> None:
        srv, p = self._server([], drop_hello=1)
        c = self._client(srv)
        self.assertEqual(c.batch(["INSERT INTO t VALUES (1)"]), 1)
        self.assertEqual(srv.accepts, 2)
        self.assertEqual(p.requests, 1)


if __name__ == "__main__":
    unittest.main()
