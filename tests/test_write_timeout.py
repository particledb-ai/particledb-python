from contextlib import closing
import socket
import threading
import time
import unittest

from particledb.connection import Connection
from particledb.errors import ConnectionError
from tests.fake_v2 import FakeV2Server, ok
from particledb import _protocol as P


class WriteDeadline(unittest.TestCase):
    def test_a_peer_that_stops_reading_does_not_block_a_write_forever(self):
        release = threading.Event()

        def handler(conn, frame):
            conn.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
            conn.send(P.F_OK, frame.sid, ok())
            release.wait(5)

        with closing(FakeV2Server(handler=handler)) as server:
            try:
                with Connection(host='127.0.0.1', port=server.port, prefer_unix_socket=False,
                                write_timeout=0.15) as db:
                    # Bound sender buffering as well as the peer's receive
                    # buffer so every platform must encounter backpressure.
                    db._sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
                    self.assertLess(db._sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF), 128 * 1024)
                    db.execute('arm')
                    # Windows can accept a whole large write into its loopback
                    # backlog. Fill the paused peer's queue in small writes
                    # first, and prove that the socket has stopped accepting
                    # bytes before testing the driver's write deadline.
                    filled = 0
                    fill_deadline = time.monotonic() + 2
                    while filled < 64 * 1024 * 1024 and time.monotonic() < fill_deadline:
                        try:
                            filled += db._sock.send(b'x' * 65536)
                        except BlockingIOError:
                            break
                    else:
                        self.fail(f'fixture did not establish backpressure after {filled} bytes')
                    start = time.monotonic()
                    with self.assertRaises(ConnectionError) as caught:
                        db.execute("SELECT '" + 'x' * (12 * 1024 * 1024) + "'")
                    self.assertEqual(caught.exception.sqlstate, '08006')
                    self.assertLess(time.monotonic() - start, 3)
                    self.assertTrue(db.closed)
                    self.assertEqual(len(server.conns), 1)
            finally:
                release.set()

    def test_invalid_write_timeouts_fail_before_connecting(self):
        for value in (0, -1, float('inf'), float('nan')):
            with self.assertRaises(ValueError):
                Connection(write_timeout=value)
