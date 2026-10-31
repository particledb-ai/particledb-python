"""LIVE: a DEFAULT construction negotiates protocol 2 with every capability --
asserted from the SERVER's own record (``SHOW pdb.wire_*``,
``__pdb_stat_connections``), not from the client's belief.

Skipped unless ``PDB_NATIVE_PORT`` is set (a ``--no-auth`` server on
``PDB_NATIVE_HOST``, default 127.0.0.1). Prints ``DEFAULT_NEGOTIATION_PASS
python`` when every case ran and passed.

Sabotage record: with
``_protocol.DEFAULT_CAPS`` stripped of ``CAP_NUMERIC_TEXT`` this file fails at
the server-side capability assertion; stripped of ``CAP_V2_MUX`` (a v1 ask)
every default construction raises (fail closed) and the file fails. The v1
control below shows the observable itself distinguishes a v1 session.
"""

from __future__ import annotations

import os
import socket
import struct
import unittest

import particledb
from particledb import _arrow
from particledb import _protocol as P
from particledb.connection import Connection
from particledb.errors import _ConnectFailed

PORT = os.environ.get("PDB_NATIVE_PORT")
HOST = os.environ.get("PDB_NATIVE_HOST", "127.0.0.1")

#: DEF-002's required list (conformance/cases/defaults.json) plus
#: bulk_ingest, which this SDK implements.
REQUIRED = ["v2_mux", "prepared", "typed_params", "array_params", "batch_sqlstate",
            "statement_notice", "numeric_text", "bulk_ingest"]


def server_record(q) -> dict:
    return {
        "protocol": q("SHOW pdb.wire_protocol"),
        "capabilities": sorted(x for x in q("SHOW pdb.wire_capabilities").split(",") if x),
        "result_format": q("SHOW pdb.wire_result_format"),
        "transport": q("SHOW pdb.wire_transport"),
        "connection_id": q("SHOW pdb.connection_id"),
    }


def assert_default_record(tc: unittest.TestCase, rec: dict) -> None:
    tc.assertEqual(rec["protocol"], "pdb-native-v2", rec)
    missing = [c for c in REQUIRED if c not in rec["capabilities"]]
    tc.assertEqual(missing, [], f"server recorded {rec['capabilities']}")
    tc.assertEqual(rec["result_format"], "arrow_ipc", rec)


@unittest.skipUnless(PORT, "PDB_NATIVE_PORT not set (live native-wire test)")
class DefaultNegotiation(unittest.TestCase):
    passed = 0

    def test_connect_with_only_host_and_port(self) -> None:
        with particledb.connect(host=HOST, port=int(PORT)) as c:
            rec = server_record(c.scalar)
            assert_default_record(self, rec)
            # The client's own view equals the server's record (DEF-004).
            self.assertEqual(c.negotiated_protocol, rec["protocol"])
            self.assertEqual(sorted(c.negotiated_capabilities), rec["capabilities"])
            self.assertEqual(c.transport.split(":")[0], rec["transport"])
            # The same record through the registry, as another connection sees it.
            with particledb.connect(host=HOST, port=int(PORT)) as other:
                # Filtered here, not in SQL: the virtual table's uint64 `id`
                # cannot be compared with an integer on this server
                # ("operator does not exist: integer = bigint", 42883).
                all_rows = other.execute("SELECT id, protocol, transport, capabilities "
                                         "FROM __pdb_stat_connections").rows
                row = [r[1:] for r in all_rows if r[0] == int(rec["connection_id"])]
            self.assertEqual(len(row), 1, row)
            self.assertEqual(row[0][0], "pdb-native-v2")
            self.assertEqual(sorted(row[0][2].split(",")), rec["capabilities"])
        DefaultNegotiation.passed += 1

    def test_zero_option_connect(self) -> None:
        if int(PORT) != particledb.DEFAULT_PORT or HOST not in ("127.0.0.1", "localhost"):
            self.skipTest(f"the server is not on localhost:{particledb.DEFAULT_PORT}")
        with particledb.connect() as c:
            rec = server_record(c.scalar)
            assert_default_record(self, rec)
            self.assertEqual(rec["transport"], "unix")  # loopback -> the server's socket
        DefaultNegotiation.passed += 1

    def test_pooled_facades(self) -> None:
        with particledb.ParticleDB(host=HOST, pdb_port=int(PORT)) as db:
            assert_default_record(self, server_record(lambda q: db.query(q)[0][q.split()[1]]))
        with particledb.PdbWireClient(host=HOST, port=int(PORT), pool_size=2) as w:
            assert_default_record(self, server_record(lambda q: w.query(q)[0][q.split()[1]]))
        DefaultNegotiation.passed += 1

    def test_a_v1_ask_fails_closed(self) -> None:
        with self.assertRaises(_ConnectFailed):
            Connection(HOST, int(PORT), _caps=P.CAP_V1_LEGACY, _ext=0)
        DefaultNegotiation.passed += 1

    def test_control_the_observable_does_see_a_v1_session(self) -> None:
        """Positive control: the assertion above CAN fail. A raw legacy v1
        session (the HELLO the previous SDK sent) is recorded as v1."""
        s = socket.create_connection((HOST, int(PORT)), timeout=10)
        try:
            def send(t: int, payload: bytes, sid: int) -> None:
                s.sendall(struct.pack("<IHBB", len(payload), sid, t, 0) + payload)

            def recv() -> tuple:
                hdr = b""
                while len(hdr) < 8:
                    hdr += s.recv(8 - len(hdr))
                n, _sid, t, _f = struct.unpack("<IHBB", hdr)
                body = b""
                while len(body) < n:
                    body += s.recv(n - len(body))
                return t, body

            send(P.F_HELLO, bytes([1, 1, P.FMT_ARROW_IPC]), 0)
            t, _ = recv()
            self.assertEqual(t, P.F_HELLO_OK)
            send(P.F_QUERY, b"SHOW pdb.wire_protocol", 1)
            t, body = recv()
            self.assertIn(t, (P.F_ARROW_RESULT, P.F_ARROW_STREAM))
            _schema, rows = _arrow.decode_stream(body, use_pyarrow=False)
            self.assertEqual(rows, [("pdb-native-v1",)])
            with self.assertRaises(AssertionError):
                assert_default_record(self, {"protocol": rows[0][0], "capabilities": [],
                                             "result_format": "arrow_ipc"})
        finally:
            s.close()
        DefaultNegotiation.passed += 1

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.passed >= 4:
            print("DEFAULT_NEGOTIATION_PASS python")


if __name__ == "__main__":
    unittest.main()
