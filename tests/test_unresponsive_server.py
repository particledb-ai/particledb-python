"""A server that stops answering (SIGSTOP, partition) cannot hold a caller.

The server here completes the handshake, reads every request, and then writes
nothing -- or only part of a frame -- and ignores RST_STREAM: what a stopped
process looks like through an open socket.

* ``Pending.cancel()`` / ``Connection.cancel()`` with NO statement timeout:
  the waiting caller must still return within ``cancel_grace`` (plus slack),
  with the ambiguous 57014 error, and the connection is closed. Before this,
  cancel only sent the reset and the caller blocked on the socket forever
  (the JDBC driver had the same defect, found on a Jepsen pause-nemesis run).
* ``statement_timeout`` bounds a read that has received part of a frame.
"""

from __future__ import annotations

import logging
import threading
import time
import unittest

from particledb import _protocol as P
from particledb.connection import Connection
from particledb.errors import ConnectionError, QueryCanceledError

from tests.fake_v2 import FakeV2Server, ok

GRACE = 0.5
SLACK = 3.0


def _stopped(partial: int = 0) -> FakeV2Server:
    """Answer nothing; with ``partial`` > 0 write that many bytes of an OK frame first."""

    def h(c, f):
        if f.type in (P.F_QUERY, P.F_EXECUTE, P.F_BATCH) and partial:
            frame = P.v2_frame(P.F_OK, f.sid, ok(1), P.FLAG_END_STREAM)
            with c.wlock:
                c.sock.sendall(frame[:partial])
        # RST_STREAM, PING, everything else: silence.

    return FakeV2Server(handler=h)


def _await_request(srv: FakeV2Server, n: int = 1) -> None:
    until = time.monotonic() + 5
    while len(srv.requests(P.F_QUERY)) < n:
        if time.monotonic() > until:
            raise AssertionError("the server never saw the request")
        time.sleep(0.005)


class _Run:
    """Run ``fn`` on a daemon thread; ``join`` reports a hang instead of hanging."""

    def __init__(self, fn):
        self.exc = None
        self.value = None
        self.t = threading.Thread(target=self._run, args=(fn,), daemon=True)
        self.t.start()

    def _run(self, fn):
        try:
            self.value = fn()
        except BaseException as e:  # noqa: BLE001 -- the test inspects it
            self.exc = e

    def join(self, timeout: float) -> bool:
        self.t.join(timeout)
        return not self.t.is_alive()


class CancelOnAStoppedServer(unittest.TestCase):
    def _check(self, partial: int, whole_connection: bool) -> None:
        srv = _stopped(partial)
        try:
            c = Connection("127.0.0.1", srv.port, prefer_unix_socket=False, cancel_grace=GRACE)
            try:
                p = c.submit("UPDATE t SET x = 1")
                run = _Run(p.result)  # no timeout: only the cancel can end it
                _await_request(srv)
                t0 = time.monotonic()
                if whole_connection:
                    self.assertEqual(c.cancel(), 1)
                else:
                    self.assertTrue(p.cancel())
                self.assertTrue(run.join(GRACE + SLACK),
                                "cancel() did not end a call blocked on a stopped server")
                took = time.monotonic() - t0
                self.assertLess(took, GRACE + SLACK)
                self.assertIsInstance(run.exc, QueryCanceledError, repr(run.exc))
                self.assertTrue(run.exc.ambiguous)
                self.assertEqual(run.exc.sqlstate, "57014")
                self.assertTrue(c.closed, "an unanswered cancel left the connection open")
                self.assertEqual(len(srv.requests(P.F_RST_STREAM)), 1)
                self.assertEqual(len(srv.requests(P.F_QUERY)), 1, "the statement was resent")
                with self.assertRaises(ConnectionError) as cm:
                    c.execute("SELECT 1")
                self.assertTrue(cm.exception.sqlstate.startswith("08"))
            finally:
                c.close()
        finally:
            srv.close()

    def test_pending_cancel_no_bytes(self) -> None:
        self._check(0, False)

    def test_pending_cancel_mid_header(self) -> None:
        self._check(5, False)

    def test_connection_cancel_mid_payload(self) -> None:
        self._check(11 + 3, True)

    def test_cancel_answered_in_grace_keeps_the_connection(self) -> None:
        """Positive control: the watchdog must not fire for an answered cancel."""
        from tests.fake_v2 import err

        def h(c, f):
            if f.type == P.F_RST_STREAM:
                c.send(P.F_ERROR, f.sid, err(57014, "canceling statement due to user request"))

        srv = FakeV2Server(handler=h)
        try:
            c = Connection("127.0.0.1", srv.port, prefer_unix_socket=False, cancel_grace=GRACE)
            p = c.submit("SELECT pg_sleep(30)")
            _await_request(srv)
            self.assertTrue(p.cancel())
            with self.assertRaises(QueryCanceledError) as cm:
                p.result(5)
            self.assertFalse(cm.exception.ambiguous)
            time.sleep(GRACE + 0.3)  # past the watchdog
            self.assertFalse(c.closed, "an answered cancel closed the connection")
            c.close()
        finally:
            srv.close()


class StatementTimeoutOnAStoppedServer(unittest.TestCase):
    def test_statement_timeout_bounds_a_partial_frame(self) -> None:
        for partial in (0, 5, 11 + 3):
            srv = _stopped(partial)
            try:
                c = Connection("127.0.0.1", srv.port, prefer_unix_socket=False,
                               cancel_grace=GRACE, statement_timeout=0.3)
                t0 = time.monotonic()
                run = _Run(lambda: c.execute("UPDATE t SET x = 1"))
                self.assertTrue(run.join(0.3 + GRACE + SLACK), f"partial={partial}: blocked past the timeout")
                self.assertLess(time.monotonic() - t0, 0.3 + GRACE + SLACK)
                self.assertIsInstance(run.exc, QueryCanceledError, repr(run.exc))
                self.assertTrue(run.exc.ambiguous)
                self.assertTrue(c.closed)
                c.close()
            finally:
                srv.close()


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record)

    def slow(self):
        return [r for r in self.records if r.levelno == logging.WARNING and "slow call" in r.getMessage()]


class SlowCallWarning(unittest.TestCase):
    """With no statement_timeout a call against a stopped server waits without
    limit; ``slow_call_warn`` (default 60 s, 0 disables) is the alert: one
    WARNING on the ``particledb`` logger naming connection, stream, request
    kind and elapsed time -- never SQL or values -- at most once a minute per
    call, counted in ``Connection.slow_call_warnings``."""

    def setUp(self):
        self.cap = _Capture()
        logging.getLogger("particledb").addHandler(self.cap)

    def tearDown(self):
        logging.getLogger("particledb").removeHandler(self.cap)

    def test_warns_once_naming_ids_not_sql(self) -> None:
        srv = _stopped(11 + 3)
        try:
            c = Connection("127.0.0.1", srv.port, prefer_unix_socket=False, cancel_grace=GRACE,
                           statement_timeout=1.5, slow_call_warn=0.2)
            self.assertEqual(c.slow_call_warnings, 0)
            run = _Run(lambda: c.execute("UPDATE secret_table SET secret_col = %s", ["hunter2"]))
            self.assertTrue(run.join(1.5 + GRACE + SLACK))
            self.assertIsInstance(run.exc, QueryCanceledError, repr(run.exc))
            w = self.cap.slow()
            self.assertEqual(len(w), 1, [r.getMessage() for r in w])
            m = w[0].getMessage()
            self.assertIn(f"connection {c.connection_id}", m)
            self.assertRegex(m, r"stream \d+")
            self.assertRegex(m, r"(QUERY|EXECUTE|PREPARE)")
            self.assertRegex(m, r"waited \d+ ms")
            self.assertNotIn("secret", m)
            self.assertNotIn("hunter2", m)
            self.assertEqual(c.slow_call_warnings, 1)
            c.close()
        finally:
            srv.close()

    def test_zero_disables_and_default_is_a_minute(self) -> None:
        srv = _stopped(0)
        try:
            c = Connection("127.0.0.1", srv.port, prefer_unix_socket=False)
            self.assertEqual(c.slow_call_warn, 60.0)
            c.close()
            c = Connection("127.0.0.1", srv.port, prefer_unix_socket=False, cancel_grace=GRACE,
                           statement_timeout=0.6, slow_call_warn=0)
            run = _Run(lambda: c.execute("UPDATE t SET x = 1"))
            self.assertTrue(run.join(0.6 + GRACE + SLACK))
            self.assertEqual(self.cap.slow(), [])
            self.assertEqual(c.slow_call_warnings, 0)
            c.close()
            with self.assertRaises(ValueError):
                Connection("127.0.0.1", srv.port, prefer_unix_socket=False, slow_call_warn=-1)
        finally:
            srv.close()


if __name__ == "__main__":
    unittest.main()
