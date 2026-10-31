"""``particledb.connection.Connection`` against the fake protocol-2 server:
handshake (fail closed), multiplexing, cancel / timeout grace, stream slots,
prepared-statement cache, KV frames, batches, notices, streaming, connection
loss. No ParticleDB server.
"""

from __future__ import annotations

import struct
import threading
import time
import unittest

from particledb import _protocol as P
from particledb.connection import Connection
from particledb.errors import (
    BatchError,
    BatchExecuteError,
    ConnectionError,
    ProtocolError,
    QueryCanceledError,
    QueryError,
    _ConnectFailed,
)

from tests.fake_v2 import FakeV2Server, err, hello_ok, ok, rows_ipc


def conn(srv: FakeV2Server, **kw) -> Connection:
    return Connection("127.0.0.1", srv.port, prefer_unix_socket=False, **kw)


def wait_for(pred, timeout: float = 5.0) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        time.sleep(0.01)
    return pred()


def notice(sev: str, st: str, msg: str) -> bytes:
    return (struct.pack("<H", 1) + bytes([len(sev)]) + sev.encode() + bytes([len(st)]) + st.encode()
            + struct.pack("<I", len(msg)) + msg.encode())


class Handshake(unittest.TestCase):
    def test_default_hello_and_negotiated_view(self) -> None:
        srv = FakeV2Server()
        try:
            c = conn(srv)
            self.assertEqual(srv.hellos[0], P.encode_hello()[8:])
            self.assertEqual(c.negotiated_protocol, "pdb-native-v2")
            self.assertIn("v2_mux", c.negotiated_capabilities)
            self.assertEqual(c.max_concurrent_streams, 8)
            self.assertTrue(c.transport.startswith("tcp:"))
            c.close()
        finally:
            srv.close()

    def test_v1_server_is_refused_not_used(self) -> None:
        srv = FakeV2Server(hello=bytes([1, 0x01, 1]) + struct.pack("<II", 1, 0))
        try:
            with self.assertRaises(_ConnectFailed) as cm:
                conn(srv)
            self.assertIn("v2 multiplexed", str(cm.exception))
            self.assertEqual(srv.requests(), [])  # nothing was ever sent on it
        finally:
            srv.close()

    def test_non_arrow_format_is_refused(self) -> None:
        srv = FakeV2Server(hello=hello_ok(fmt=2))  # FMT_JSON
        try:
            with self.assertRaises(ProtocolError):
                conn(srv)
        finally:
            srv.close()

    def test_handshake_error_carries_its_sqlstate(self) -> None:
        srv = FakeV2Server(hello_error=err(0x15, "22023: FATAL: invalid value for parameter \"TimeZone\""))
        try:
            with self.assertRaises(ConnectionError) as cm:
                conn(srv)
            self.assertEqual(cm.exception.sqlstate, "22023")
        finally:
            srv.close()

    def test_auth_frame_and_failure(self) -> None:
        def h(c, f):
            if f.type == P.F_AUTH:
                ul = struct.unpack_from("<I", f.payload)[0]
                user = f.payload[4:4 + ul]
                if user == b"good":
                    c.send(P.F_OK, f.sid, ok(1))
                else:
                    c.send(P.F_ERROR, f.sid, err(0x14, "password authentication failed for user \"bad\""))
        srv = FakeV2Server(handler=h)
        try:
            c = conn(srv, user="good", password="pw")
            a = srv.requests(P.F_AUTH)[0]
            self.assertEqual(a.payload, P.auth_payload("good", "pw"))
            c.close()
            with self.assertRaises(ConnectionError) as cm:
                conn(srv, user="bad", password="x")
            self.assertEqual(cm.exception.sqlstate, "28P01")
        finally:
            srv.close()

    def test_auth_required_without_credentials(self) -> None:
        srv = FakeV2Server(hello=hello_ok(caps=P.CAP_V2_MUX | P.CAP_PREPARED | P.CAP_AUTH_REQUIRED))
        try:
            with self.assertRaises(ConnectionError) as cm:
                conn(srv)
            self.assertEqual(cm.exception.sqlstate, "28000")
        finally:
            srv.close()


class Multiplexing(unittest.TestCase):
    def test_out_of_order_answers_reach_their_callers(self) -> None:
        pending = []
        lock = threading.Lock()

        def h(c, f):
            if f.type == P.F_QUERY:
                with lock:
                    pending.append(f)
                    if len(pending) == 8:  # answer in REVERSE order
                        for g in reversed(pending):
                            n = int(g.payload.decode().split()[-1])
                            c.send(P.F_ARROW_RESULT, g.sid, rows_ipc([[n]], ["n"]))
        srv = FakeV2Server(handler=h)
        try:
            c = conn(srv)
            out = {}

            def run(i: int) -> None:
                out[i] = c.execute(f"SELECT {i}").rows

            ts = [threading.Thread(target=run, args=(i,)) for i in range(8)]
            for t in ts:
                t.start()
            for t in ts:
                t.join(10)
            self.assertEqual(out, {i: [(i,)] for i in range(8)})
            self.assertEqual(len({f.sid for f in srv.requests(P.F_QUERY)}), 8)
            self.assertTrue(all(f.sid % 2 == 1 for f in srv.requests(P.F_QUERY)))
            c.close()
        finally:
            srv.close()

    def test_stream_slots_bound_requests_in_flight(self) -> None:
        live = [0]
        peak = [0]
        lock = threading.Lock()

        def h(c, f):
            if f.type == P.F_QUERY:
                with lock:
                    live[0] += 1
                    peak[0] = max(peak[0], live[0])

                def later():
                    time.sleep(0.05)
                    with lock:
                        live[0] -= 1
                    c.send(P.F_OK, f.sid, ok(0))
                threading.Thread(target=later).start()
        srv = FakeV2Server(handler=h, hello=hello_ok(max_streams=2))
        try:
            c = conn(srv)
            ts = [threading.Thread(target=c.execute, args=("SELECT x",)) for _ in range(8)]
            for t in ts:
                t.start()
            for t in ts:
                t.join(20)
            self.assertEqual(len(srv.requests(P.F_QUERY)), 8)
            self.assertLessEqual(peak[0], 2)
            c.close()
        finally:
            srv.close()

    def test_notices_reach_the_result_and_the_handler(self) -> None:
        def h(c, f):
            if f.type == P.F_QUERY:
                c.send(P.F_NOTICE, f.sid, notice("WARNING", "01000", "careful"), 0)
                c.send(P.F_NOTICE, f.sid, b"\x05\x00garbage", 0)  # malformed: dropped whole
                c.send(P.F_OK, f.sid, ok(3))
        srv = FakeV2Server(handler=h)
        try:
            seen = []
            c = conn(srv, notice_handler=seen.append)
            r = c.execute("CREATE INDEX i ON t USING hash (a)")
            self.assertEqual(r.rowcount, 3)
            self.assertEqual([tuple(n) for n in r.notices], [("WARNING", "01000", "careful")])
            self.assertEqual(seen, r.notices)
            c.close()
        finally:
            srv.close()

    def test_chunked_result_split_mid_message(self) -> None:
        data = rows_ipc([[i, f"v{i}"] for i in range(300)], ["i", "v"])

        def h(c, f):
            if f.type == P.F_QUERY:
                cuts = list(range(0, len(data), 37))
                for k, a in enumerate(cuts):
                    last = k == len(cuts) - 1
                    c.send(P.F_ARROW_STREAM, f.sid, data[a:a + 37],
                           P.FLAG_END_STREAM if last else P.FLAG_MORE_CHUNKS)
        srv = FakeV2Server(handler=h)
        try:
            c = conn(srv)
            self.assertEqual(c.execute("SELECT").rows, [(i, f"v{i}") for i in range(300)])
            self.assertEqual(list(c.stream("SELECT")), [(i, f"v{i}") for i in range(300)])
            c.close()
        finally:
            srv.close()


class CancelAndTimeout(unittest.TestCase):
    def _sleepy(self, answer_cancel: bool):
        held = {}

        def h(c, f):
            if f.type == P.F_QUERY:
                held[f.sid] = c
            elif f.type == P.F_RST_STREAM and answer_cancel:
                c.send(P.F_ERROR, f.sid, err(57014, "canceling statement due to user request"))
            elif f.type == P.F_PING:
                c.send(P.F_PONG, 0)
        return FakeV2Server(handler=h), held

    def test_cancel_resets_the_stream_and_keeps_the_connection(self) -> None:
        srv, _ = self._sleepy(True)
        try:
            c = conn(srv)
            p = c.submit("SELECT pg_sleep(30)")
            time.sleep(0.05)
            self.assertTrue(p.cancel())
            with self.assertRaises(QueryCanceledError) as cm:
                p.result(5)
            self.assertEqual(cm.exception.sqlstate, "57014")
            self.assertFalse(cm.exception.ambiguous)
            rst = srv.requests(P.F_RST_STREAM)
            self.assertEqual([(r.sid, r.payload) for r in rst], [(p.stream_id, b"\x07\x00")])
            self.assertFalse(c.closed)
            c.close()
        finally:
            srv.close()

    def test_connection_cancel_resets_every_statement(self) -> None:
        srv, _ = self._sleepy(True)
        try:
            c = conn(srv)
            ps = [c.submit(f"SELECT {i}") for i in range(3)]
            time.sleep(0.05)
            self.assertEqual(c.cancel(), 3)
            for p in ps:
                with self.assertRaises(QueryCanceledError):
                    p.result(5)
            c.close()
        finally:
            srv.close()

    def test_timeout_answered_by_the_server_is_not_ambiguous(self) -> None:
        srv, _ = self._sleepy(True)
        try:
            c = conn(srv)
            t0 = time.monotonic()
            with self.assertRaises(QueryCanceledError) as cm:
                c.execute("SELECT pg_sleep(30)", timeout=0.2)
            self.assertFalse(cm.exception.ambiguous)
            self.assertLess(time.monotonic() - t0, 3)
            self.assertFalse(c.closed)
            c.close()
        finally:
            srv.close()

    def test_timeout_never_answered_closes_the_connection(self) -> None:
        srv, _ = self._sleepy(False)
        try:
            c = conn(srv, cancel_grace=0.2)
            other = c.submit("SELECT other")
            with self.assertRaises(QueryCanceledError) as cm:
                c.execute("SELECT pg_sleep(30)", timeout=0.2)
            self.assertTrue(cm.exception.ambiguous)
            self.assertTrue(c.closed)
            with self.assertRaises(QueryCanceledError):
                other.result(1)  # in flight on the closed connection: fails too
            with self.assertRaises(ConnectionError) as cm2:
                c.execute("SELECT 1")
            self.assertEqual(cm2.exception.sqlstate, "08003")
        finally:
            srv.close()

    def test_answer_racing_the_deadline_is_the_outcome(self) -> None:
        def h(c, f):
            if f.type == P.F_QUERY:
                c.send(P.F_OK, f.sid, ok(7))
        srv = FakeV2Server(handler=h)
        try:
            c = conn(srv)
            s = c._request(P.F_QUERY, b"SELECT 1")
            c._wait(s, lambda: s.done, None)  # answered by now
            c._timed_out(s, 0.0)               # the deadline "passed" anyway
            self.assertFalse(c.closed)
            self.assertEqual(srv.requests(P.F_RST_STREAM), [])
            c.close()
        finally:
            srv.close()

    def test_kv_is_never_reset(self) -> None:
        def h(c, f):
            pass  # never answers
        srv = FakeV2Server(handler=h)
        try:
            c = conn(srv)
            with self.assertRaises(QueryCanceledError) as cm:
                c.kv_get("k", timeout=0.2)
            self.assertTrue(cm.exception.ambiguous)
            self.assertEqual(srv.requests(P.F_RST_STREAM), [])
            self.assertTrue(c.closed)
        finally:
            srv.close()


class PreparedStatements(unittest.TestCase):
    def test_prepared_once_then_executed_by_handle(self) -> None:
        def h(c, f):
            if f.type == P.F_PREPARE:
                c.send(P.F_PREPARE_OK, f.sid, struct.pack("<H", 2))
            elif f.type == P.F_EXECUTE:
                c.send(P.F_ARROW_RESULT, f.sid, rows_ipc([[1]], ["x"]))
        srv = FakeV2Server(handler=h)
        try:
            c = conn(srv)
            for i in range(5):
                c.execute("SELECT $1, $2", [i, "a"])
            self.assertEqual(len(srv.requests(P.F_PREPARE)), 1)
            ex = srv.requests(P.F_EXECUTE)
            self.assertEqual(len(ex), 5)
            name = P.prepare_payload("py_1", "")[:8]
            self.assertTrue(ex[0].payload.startswith(name[:4] + b"py_1"))
            # [u16 n][tag int8][i64][tag text][u32][a]
            self.assertEqual(ex[4].payload[8:], struct.pack("<H", 2) + b"\x01" + struct.pack("<q", 4)
                             + b"\x02" + struct.pack("<I", 1) + b"a")
            self.assertTrue(c.is_server_prepared("SELECT $1, $2"))
            with self.assertRaises(QueryError) as cm:
                c.execute("SELECT $1, $2", [1])
            self.assertEqual(cm.exception.sqlstate, "08P01")
            c.close()
        finally:
            srv.close()

    def test_deallocated_statement_is_prepared_again(self) -> None:
        state = {"gone": True}

        def h(c, f):
            if f.type == P.F_PREPARE:
                c.send(P.F_PREPARE_OK, f.sid, struct.pack("<H", 1))
            elif f.type == P.F_EXECUTE:
                if state["gone"]:
                    state["gone"] = False
                    c.send(P.F_ERROR, f.sid, err(26, 'prepared statement "py_1" does not exist'))
                else:
                    c.send(P.F_OK, f.sid, ok(1))
        srv = FakeV2Server(handler=h)
        try:
            c = conn(srv)
            c.prepare("SELECT $1").close()
            state["gone"] = True
            self.assertEqual(c.execute("SELECT $1", [1]).rowcount, 1)
            self.assertEqual(len(srv.requests(P.F_PREPARE)), 2)
            self.assertEqual(len(srv.requests(P.F_EXECUTE)), 2)
            c.close()
        finally:
            srv.close()

    def test_cache_evicts_unpinned_and_closes_on_the_server(self) -> None:
        srv = FakeV2Server()
        try:
            c = conn(srv, prepare_cache_size=2)
            pinned = c.prepare("SELECT $1 AS a")
            for k in range(4):
                c.execute(f"SELECT $1 AS b{k}", [k])
            wait_for(lambda: len(srv.requests(P.F_CLOSE_STMT)) == 3)
            closes = srv.requests(P.F_CLOSE_STMT)
            self.assertEqual(len(closes), 3)
            self.assertNotIn(P.name_payload("py_1"), [f.payload for f in closes])  # the pinned one stays
            self.assertTrue(pinned.server_prepared)
            pinned.close()
            c.close()
        finally:
            srv.close()

    def test_typed_parameter_needs_its_capability(self) -> None:
        srv = FakeV2Server(hello=hello_ok(ext=0))
        try:
            c = conn(srv)
            with self.assertRaises(QueryError) as cm:
                c.execute("SELECT $1", [1.5])
            self.assertEqual(cm.exception.sqlstate, "0A000")
            self.assertEqual(srv.requests(P.F_EXECUTE), [])
            c.close()
        finally:
            srv.close()


class BatchesAndKv(unittest.TestCase):
    def test_batch_entry_sqlstate(self) -> None:
        def h(c, f):
            if f.type == P.F_BATCH:
                p = (struct.pack("<I", 2) + bytes([P.F_OK]) + struct.pack("<Q", 1)
                     + bytes([0x90]) + struct.pack("<H", 23505) + b"23505" + struct.pack("<I", 3) + b"dup")
                c.send(P.F_BATCH_RESULT, f.sid, p)
        srv = FakeV2Server(handler=h)
        try:
            c = conn(srv)
            with self.assertRaises(BatchError) as cm:
                c.batch(["INSERT 1", "INSERT 1", "INSERT 2"])
            e = cm.exception
            self.assertEqual((e.sqlstate, e.index, e.completed), ("23505", 1, [1]))
            c.close()
        finally:
            srv.close()

    def test_short_batch_without_error_is_a_protocol_error(self) -> None:
        def h(c, f):
            if f.type == P.F_BATCH:
                c.send(P.F_BATCH_RESULT, f.sid, struct.pack("<I", 1) + bytes([P.F_OK]) + struct.pack("<Q", 1))
        srv = FakeV2Server(handler=h)
        try:
            c = conn(srv)
            with self.assertRaises(ProtocolError):
                c.batch(["a", "b"])
            c.close()
        finally:
            srv.close()

    def test_execute_many_in_order_outside_and_inside_a_transaction(self) -> None:
        in_txn = {"on": False}

        def h(c, f):
            if f.type == P.F_PREPARE:
                c.send(P.F_PREPARE_OK, f.sid, struct.pack("<H", 1))
            elif f.type == P.F_QUERY:
                in_txn["on"] = f.payload == b"BEGIN"
                c.send(P.F_OK, f.sid, ok(0), P.FLAG_END_STREAM | (P.FLAG_TXN_OPEN if in_txn["on"] else 0))
            elif f.type == P.F_EXECUTE:
                v = struct.unpack_from("<q", f.payload, 4 + 4 + 2 + 1)[0]
                fl = P.FLAG_END_STREAM | (P.FLAG_TXN_FAILED if in_txn["on"] and v == 2 else
                                          P.FLAG_TXN_OPEN if in_txn["on"] else 0)
                if v == 2:
                    c.send(P.F_ERROR, f.sid, err(23505, "duplicate key"), fl)
                else:
                    c.send(P.F_OK, f.sid, ok(1), fl)
        srv = FakeV2Server(handler=h)
        try:
            c = conn(srv)
            with self.assertRaises(BatchExecuteError) as cm:
                c.execute_many("INSERT $1", [(1,), (2,), (3,)])
            e = cm.exception
            self.assertEqual((e.sqlstate, e.errors[0][0], e.attempted, e.affected), ("23505", 1, 3, 2))
            sent = [struct.unpack_from("<q", f.payload, 11)[0] for f in srv.requests(P.F_EXECUTE)]
            self.assertEqual(sent, [1, 2, 3])  # in order, all of them
            c.execute("BEGIN")
            with self.assertRaises(BatchExecuteError) as cm:
                c.execute_many("INSERT $1", [(1,), (2,), (3,)])
            self.assertEqual(cm.exception.attempted, 2)  # stopped at the failure
            c.close()
        finally:
            srv.close()

    def test_execute_many_refuses_an_oversized_row_before_sending(self) -> None:
        srv = FakeV2Server()
        try:
            c = conn(srv)
            with self.assertRaises(ValueError):
                c.execute_many("INSERT $1", [("ok",), ("x" * (P.MAX_PAYLOAD + 1),)])
            self.assertEqual(srv.requests(P.F_EXECUTE), [])
            c.close()
        finally:
            srv.close()

    def test_kv_frames(self) -> None:
        store = {}

        def h(c, f):
            kl = struct.unpack_from("<I", f.payload)[0]
            k = f.payload[4:4 + kl]
            if f.type == P.F_KV_SET:
                store[k] = f.payload[8 + kl:]
                c.send(P.F_OK, f.sid, ok(1))
            elif f.type == P.F_KV_GET:
                if k in store:
                    c.send(P.F_KV_BULK, f.sid, struct.pack("<I", len(store[k])) + store[k])
                else:
                    c.send(P.F_KV_NIL, f.sid)
            elif f.type == P.F_KV_DEL:
                c.send(P.F_OK, f.sid, ok(1 if store.pop(k, None) is not None else 0))
            elif f.type == P.F_KV_INCR:
                if k == b"bad":
                    c.send(P.F_ERROR, f.sid, err(1, "KV_INCR: value is not an integer"))
                    return
                n = int(store.get(k, b"0")) + 1
                store[k] = str(n).encode()
                c.send(P.F_KV_BULK, f.sid, struct.pack("<I", len(store[k])) + store[k])
        srv = FakeV2Server(handler=h)
        try:
            c = conn(srv)
            c.kv_set("a", bytes(range(256)))
            self.assertEqual(c.kv_get("a"), bytes(range(256)))
            self.assertIsNone(c.kv_get("absent"))
            self.assertEqual([c.kv_incr("n") for _ in range(3)], [1, 2, 3])
            self.assertTrue(c.kv_delete("a"))
            self.assertFalse(c.kv_delete("a"))
            c.kv_mset([("x", "1"), ("y", b"2")])
            self.assertEqual(c.kv_mget(["x", "nope", "y"]), [b"1", None, b"2"])
            with self.assertRaises(QueryError) as cm:  # an operation error, not a broken connection
                c.kv_incr("bad")
            self.assertEqual(cm.exception.sqlstate, "HY000")
            self.assertFalse(c.closed)
            c.close()
        finally:
            srv.close()


class Loss(unittest.TestCase):
    def test_eof_mid_request_is_08006_and_fails_every_stream(self) -> None:
        def h(c, f):
            if f.type == P.F_QUERY and f.payload == b"die":
                c.close()
        srv = FakeV2Server(handler=h)
        try:
            c = conn(srv)
            waiting = c.submit("SELECT slow")
            with self.assertRaises(ConnectionError) as cm:
                c.execute("die")
            self.assertEqual(cm.exception.sqlstate, "08006")
            with self.assertRaises(ConnectionError):
                waiting.result(2)
            self.assertTrue(c.closed)
        finally:
            srv.close()

    def test_goaway_fails_everything(self) -> None:
        def h(c, f):
            if f.type == P.F_QUERY:
                c.send(P.F_GOAWAY, 0, P.goaway_payload(0), 0)
        srv = FakeV2Server(handler=h)
        try:
            c = conn(srv)
            with self.assertRaises(ConnectionError) as cm:
                c.execute("SELECT 1")
            self.assertIn("GOAWAY", str(cm.exception))
        finally:
            srv.close()

    def test_peer_gone_detects_a_dead_idle_socket(self) -> None:
        srv = FakeV2Server()
        try:
            c = conn(srv)
            self.assertFalse(c.peer_gone())
            srv.conns[0].close()
            time.sleep(0.1)
            self.assertTrue(c.peer_gone())
            self.assertTrue(c.closed)
        finally:
            srv.close()


class RoundAFindings(unittest.TestCase):
    def test_close_wakes_a_reader_blocked_without_deadline(self) -> None:
        srv = FakeV2Server(handler=lambda c, f: None)  # never answers anything
        try:
            c = conn(srv, cancel_grace=0.1)
            box = {}

            def blocked() -> None:
                try:
                    c.execute("SELECT forever")  # holds the read role, recv() with no deadline
                except BaseException as e:  # noqa: BLE001
                    box["e"] = e

            t = threading.Thread(target=blocked)
            t.start()
            time.sleep(0.1)
            # Another statement times out unanswered: the connection is closed
            # from THIS thread while the first one sits in recv(). The server
            # keeps its end open, so only the client's own close can wake it.
            with self.assertRaises(QueryCanceledError):
                c.execute("SELECT other", timeout=0.1)
            t.join(3)
            self.assertFalse(t.is_alive(), "the reader stayed blocked in recv() after the connection closed")
            self.assertIsInstance(box["e"], QueryCanceledError)
            self.assertTrue(box["e"].ambiguous)
        finally:
            srv.close()

    def test_ping_and_kv_do_not_clear_the_transaction_status(self) -> None:
        def h(c, f):
            if f.type == P.F_QUERY:
                c.send(P.F_OK, f.sid, ok(0), P.FLAG_END_STREAM | (P.FLAG_TXN_OPEN if f.payload == b"BEGIN" else 0))
            elif f.type == P.F_KV_SET:
                c.send(P.F_OK, f.sid, ok(1))  # an inline KV reply carries no status bits
        srv = FakeV2Server(handler=h)
        try:
            c = conn(srv)
            c.execute("BEGIN")
            self.assertTrue(c.in_transaction)
            c.ping()
            c.kv_set("k", "v")
            self.assertTrue(c.in_transaction)
            c.execute("COMMIT")
            self.assertFalse(c.in_transaction)
            c.close()
        finally:
            srv.close()

    def test_idle_goaway_is_noticed_before_a_request(self) -> None:
        srv = FakeV2Server()
        try:
            c = conn(srv)
            srv.conns[0].send(P.F_GOAWAY, 0, P.goaway_payload(0), 0)
            time.sleep(0.1)
            self.assertTrue(c.peer_gone())
            self.assertEqual(srv.requests(P.F_QUERY), [])
        finally:
            srv.close()


class RoundDFindings(unittest.TestCase):
    def test_stream_bound_holds_before_the_consumer_attaches(self) -> None:
        data = rows_ipc([[i] for i in range(2000)], ["i"])

        release_slow = threading.Event()
        slow_started = threading.Event()

        def h(c, f):
            if f.type == P.F_QUERY and f.payload == b"slow":
                def later():
                    slow_started.set()
                    if release_slow.wait(5):
                        c.send(P.F_OK, f.sid, ok(0))
                threading.Thread(target=later).start()
            elif f.type == P.F_QUERY and f.payload == b"big":
                for a in range(0, len(data), 64):
                    c.send(P.F_ARROW_STREAM, f.sid, data[a:a + 64], P.FLAG_MORE_CHUNKS)
            elif f.type == P.F_RST_STREAM:
                c.send(P.F_ERROR, f.sid, err(57014, "canceling statement"))
        srv = FakeV2Server(handler=h)
        c = conn(srv, stream_buffer_limit=1024)
        failures = []

        def slow():
            try:
                c.execute("slow")
            except Exception as exc:
                failures.append(exc)

        t = threading.Thread(target=slow)
        try:
            t.start()
            self.assertTrue(slow_started.wait(5))
            # Open the same bounded stream used by stream(), but attach its
            # iterator only after the other request has driven the overflow.
            s = c._request(P.F_QUERY, b"big", P.FLAG_END_STREAM, attached=True)
            self.assertTrue(wait_for(lambda: s.discard), "the unread stream did not overflow")
            from particledb.connection import RowStream
            with self.assertRaises(QueryError) as cm:
                for _ in RowStream(c, s, 2):
                    pass
            self.assertEqual(cm.exception.sqlstate, "53200")
            self.assertEqual(s.qbytes, 0)
            self.assertTrue(wait_for(lambda: bool(srv.requests(P.F_RST_STREAM))))
            release_slow.set()
            t.join(5)
            self.assertFalse(t.is_alive())
            self.assertEqual(failures, [])
        finally:
            release_slow.set()
            c.close()
            t.join(5)
            srv.close()

    def test_deadline_holds_while_other_traffic_keeps_the_socket_readable(self) -> None:
        srv = FakeV2Server(handler=lambda c, f: None)
        try:
            c = conn(srv)
            other = c._request(P.F_QUERY, b"noise")
            mine = c._request(P.F_QUERY, b"stall")
            # The socket is never dry: every read returns another stream's frame.
            class Busy:
                def frame(self, v2, deadline):
                    return P.F_NOTICE, 0, other.sid, b"\x00\x00"

            c._reader = Busy()
            t0 = time.monotonic()
            from particledb.connection import _Timeout
            with self.assertRaises(_Timeout):
                c._wait(mine, lambda: mine.done, time.monotonic() + 0.2)
            self.assertLess(time.monotonic() - t0, 1.0, "the deadline was outlived by unrelated traffic")
        finally:
            srv.close()


class Streaming(unittest.TestCase):
    def test_overflowing_consumer_is_cancelled_loudly(self) -> None:
        data = rows_ipc([[i] for i in range(2000)], ["i"])

        def h(c, f):
            if f.type == P.F_QUERY and f.payload == b"big":
                for a in range(0, len(data), 64):
                    c.send(P.F_ARROW_STREAM, f.sid, data[a:a + 64], P.FLAG_MORE_CHUNKS)
                # never ends: only a cancel finishes it
            elif f.type == P.F_RST_STREAM:
                c.send(P.F_ERROR, f.sid, err(57014, "canceling statement"))
            elif f.type == P.F_QUERY:
                time.sleep(0.3)
                c.send(P.F_OK, f.sid, ok(0))
        srv = FakeV2Server(handler=h)
        try:
            c = conn(srv, stream_buffer_limit=1024)
            rs = c.stream("big")
            c.execute("other")  # this thread reads the big stream's chunks meanwhile
            with self.assertRaises(QueryError) as cm:
                for _ in rs:
                    pass
            self.assertEqual(cm.exception.sqlstate, "53200")
            self.assertEqual(len(srv.requests(P.F_RST_STREAM)), 1)
            self.assertFalse(c.closed)
            c.close()
        finally:
            srv.close()

    def test_close_early_cancels(self) -> None:
        data = rows_ipc([[i] for i in range(500)], ["i"])

        def h(c, f):
            if f.type == P.F_QUERY:
                c.send(P.F_ARROW_STREAM, f.sid, data[:len(data) // 2], P.FLAG_MORE_CHUNKS)
            elif f.type == P.F_RST_STREAM:
                c.send(P.F_ERROR, f.sid, err(57014, "canceling statement"))
        srv = FakeV2Server(handler=h)
        try:
            c = conn(srv)
            with c.stream("SELECT") as rs:
                self.assertEqual(rs.columns, ["i"])
            self.assertTrue(wait_for(lambda: len(srv.requests(P.F_RST_STREAM)) == 1))
            c.ping()
            c.close()
        finally:
            srv.close()


if __name__ == "__main__":
    unittest.main()
