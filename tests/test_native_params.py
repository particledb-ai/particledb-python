"""Server-free tests: the Arrow IPC writer and the ``ParticleDB`` facade.

* ``particledb.arrow_ipc`` -- the hand-written Arrow IPC stream writer that
  carries native bulk-ingest rows (``F_BULK`` DATA), round-tripped through
  pyarrow's reader when pyarrow is installed and byte-checked for the framing
  the server's ``validate_ipc_stream_framing`` walks either way.
* ``ParticleDB`` routing over the native wire (a mocked ``PdbWireClient``),
  ``connect()`` URL parsing, error wrapping (SQLSTATE kept), ``ai.embed_batch``,
  ``vector.create_index(name=)``.

Run from the repository root: ``python -m pytest tests/test_native_params.py -v``.
"""

from __future__ import annotations

import io
import struct
import unittest
from unittest.mock import MagicMock, patch

from particledb import arrow_ipc
from particledb.arrow_ipc import encode_param_stream, infer_column_type
from particledb.errors import BatchExecuteError, QueryError

try:
    import pyarrow as pa
    import pyarrow.ipc as pa_ipc

    HAVE_PYARROW = True
except ImportError:  # pragma: no cover
    HAVE_PYARROW = False


def _read_back(data: bytes):
    return pa_ipc.open_stream(io.BytesIO(data)).read_all()


def _walk_messages(data: bytes):
    """Walk the IPC framing the way the server's framing validator does."""
    off, msgs = 0, []
    while off < len(data):
        assert data[off:off + 4] == b"\xff\xff\xff\xff", f"no continuation at {off}"
        meta_len = struct.unpack_from("<i", data, off + 4)[0]
        off += 8
        if meta_len == 0:
            msgs.append(("eos", 0))
            break
        assert meta_len > 0 and meta_len % 8 == 0, meta_len
        assert off % 8 == 0
        msgs.append(("msg", meta_len))
        off += meta_len
        # bodyLength is inside the flatbuffer; the schema message has none and
        # the record batch body is the rest up to the EOS marker.
        if len(msgs) == 2:
            body_end = len(data) - 8
            assert (body_end - off) % 8 == 0
            off = body_end
    assert off == len(data) - 0 or msgs[-1][0] == "eos"
    return msgs


class TestArrowIpcWriter(unittest.TestCase):
    def test_framing_schema_batch_eos(self) -> None:
        data = encode_param_stream([(1, "a")])
        msgs = _walk_messages(data)
        self.assertEqual([m[0] for m in msgs], ["msg", "msg", "eos"])
        self.assertTrue(data.endswith(b"\xff\xff\xff\xff\x00\x00\x00\x00"))

    def test_no_params_is_empty_payload(self) -> None:
        self.assertEqual(encode_param_stream([()]), b"")

    def test_type_inference(self) -> None:
        self.assertEqual(infer_column_type([1, 2, None]), arrow_ipc.INT64)
        self.assertEqual(infer_column_type([1, 2.5]), arrow_ipc.FLOAT64)
        self.assertEqual(infer_column_type([True, None]), arrow_ipc.BOOL)
        self.assertEqual(infer_column_type([b"x"]), arrow_ipc.BINARY)
        self.assertEqual(infer_column_type([None, None]), arrow_ipc.UTF8)
        self.assertEqual(infer_column_type([1, "x"]), arrow_ipc.UTF8)
        # bool is an int subclass: True must not become Int64.
        self.assertEqual(infer_column_type([True, False]), arrow_ipc.BOOL)

    def test_row_width_mismatch_raises(self) -> None:
        with self.assertRaises(ValueError):
            encode_param_stream([(1, 2), (3,)])

    def test_bigint_overflow_raises(self) -> None:
        with self.assertRaises(OverflowError):
            encode_param_stream([(1 << 63,)])

    @unittest.skipUnless(HAVE_PYARROW, "pyarrow not installed")
    def test_round_trip_all_types_and_nulls(self) -> None:
        import datetime
        import decimal

        rows = [
            (1, "a", 1.5, True, b"x", None, {"k": [1]}),
            (None, None, 2, False, None, None, None),
            (-(1 << 63), "héllo ☃", None, None, b"", None, decimal.Decimal("1.50")),
            ((1 << 63) - 1, "", -0.25, True, b"\x00\xff", None, datetime.date(2026, 1, 2)),
        ]
        table = _read_back(encode_param_stream(rows))
        table.validate(full=True)
        self.assertEqual(
            [str(t) for t in table.schema.types],
            ["int64", "string", "double", "bool", "binary", "string", "string"],
        )
        got = [tuple(r.values()) for r in table.to_pylist()]
        self.assertEqual(got[0], (1, "a", 1.5, True, b"x", None, '{"k": [1]}'))
        self.assertEqual(got[1], (None, None, 2.0, False, None, None, None))
        self.assertEqual(got[2], (-(1 << 63), "héllo ☃", None, None, b"", None, "1.50"))
        self.assertEqual(got[3], ((1 << 63) - 1, "", -0.25, True, b"\x00\xff", None, "2026-01-02"))

    @unittest.skipUnless(HAVE_PYARROW, "pyarrow not installed")
    def test_round_trip_10k_rows(self) -> None:
        rows = [(i, f"name-{i}", i % 3 == 0) for i in range(10_000)]
        table = _read_back(encode_param_stream(rows))
        self.assertEqual(table.num_rows, 10_000)
        self.assertEqual(sum(table.column(0).to_pylist()), sum(range(10_000)))
        self.assertEqual(table.column(1)[9_999].as_py(), "name-9999")

    @unittest.skipUnless(HAVE_PYARROW, "pyarrow not installed")
    def test_field_names(self) -> None:
        data = encode_param_stream([(1, "a")], names=["id", "v"])
        self.assertEqual(_read_back(data).column_names, ["id", "v"])
        with self.assertRaises(ValueError):
            encode_param_stream([(1, "a")], names=["id"])

    @unittest.skipUnless(HAVE_PYARROW, "pyarrow not installed")
    def test_explicit_types(self) -> None:
        data = encode_param_stream([(1,), (2,)], types=[arrow_ipc.FLOAT64])
        self.assertEqual(_read_back(data).column(0).to_pylist(), [1.0, 2.0])


class TestParticleDBFacade(unittest.TestCase):
    def _db(self, wire):
        from particledb.client import ParticleDB

        with patch("particledb.client.ParticleDB._init_wire", return_value=wire):
            return ParticleDB(host="localhost")

    def test_sql_routes_to_wire(self) -> None:
        wire = MagicMock()
        wire.query.return_value = [{"a": 1}]
        wire.execute.return_value = 1
        wire.execute_many.return_value = 2
        db = self._db(wire)
        self.assertEqual(db.query("SELECT $1", [1]), [{"a": 1}])
        wire.query.assert_called_once_with("SELECT $1", [1])
        self.assertEqual(db.execute("DELETE FROM t"), 1)
        wire.bulk_insert.return_value = 2
        self.assertEqual(db.bulk_insert("t", ["a", "b"], [[1, 2], [3, 4]]), 2)
        wire.bulk_insert.assert_called_once_with("t", ["a", "b"], [[1, 2], [3, 4]])
        self.assertEqual(db.execute_many("INSERT INTO t VALUES ($1)", [(1,), (2,)]), 2)

    def test_wire_errors_become_particledb_errors(self) -> None:
        from particledb.client import ParticleDBBatchError, ParticleDBError

        wire = MagicMock()
        wire.query.side_effect = QueryError("bad sql", 42601, "42601")
        wire.execute_many.side_effect = BatchExecuteError([(0, "dup")], 0, 1, 1, "23505", 23505)
        db = self._db(wire)
        with self.assertRaises(ParticleDBError) as cm:
            db.query("SELEKT")
        self.assertEqual((cm.exception.code, cm.exception.sqlstate), ("42601", "42601"))
        with self.assertRaises(ParticleDBBatchError) as cm2:
            db.execute_many("INSERT INTO t VALUES ($1)", [(1,)])
        self.assertEqual((cm2.exception.errors, cm2.exception.sqlstate), ([(0, "dup")], "23505"))
        # the native error classes ARE ParticleDBErrors too
        self.assertIsInstance(QueryError("x"), ParticleDBError)

    def test_http_only_features_refuse_loudly(self) -> None:
        from particledb.client import ParticleDB, ParticleDBError

        db = ParticleDB(host="localhost", transport="http")
        with self.assertRaises(ParticleDBError):
            db.execute_many("INSERT INTO t VALUES ($1)", [(1,)])
        with self.assertRaises(ParticleDBError):
            with db.transaction():
                pass

    def test_connect_urls(self) -> None:
        from particledb.client import ParticleDB

        with patch("particledb.client.ParticleDB._init_wire", return_value=MagicMock()) as iw:
            db = ParticleDB.connect("pdb://alice:s%40cret@db.example:6000")
        self.assertEqual(db.pdb_port, 6000)
        self.assertEqual(iw.call_args.kwargs["username"], "alice")
        self.assertEqual(iw.call_args.kwargs["password"], "s@cret")
        self.assertEqual(db._transport, "pdb_wire")
        db = ParticleDB.connect("https://db.example:9443")
        self.assertEqual((db._active_transport(), db.port, db.tls), ("http", 9443, True))
        with self.assertRaises(ValueError):
            ParticleDB.connect("postgresql://u@h:5432/db")

    def test_unreachable_wire_with_explicit_transport_raises(self) -> None:
        from particledb.client import ParticleDB

        with patch("particledb.client.ParticleDB._init_wire", return_value=None):
            with self.assertRaises(ConnectionError):
                ParticleDB(host="localhost", transport="pdb_wire")

    def test_ai_and_ops_properties(self) -> None:
        from particledb.ai import AI
        from particledb.ops import Ops

        db = self._db(MagicMock())
        self.assertIsInstance(db.ai, AI)
        self.assertIsInstance(db.ops, Ops)

    def test_embed_batch_is_one_execute_batch(self) -> None:
        wire = MagicMock()
        wire.execute_batch.return_value = [[{"result": [0.5, 1.0]}], [{"result": "[2, 3]"}]]
        db = self._db(wire)
        self.assertEqual(db.ai.embed_batch(["a", "b"]), [[0.5, 1.0], [2.0, 3.0]])
        wire.execute_batch.assert_called_once_with("SELECT EMBED($1) AS result", [["a"], ["b"]])
        wire.execute_batch.return_value = [[{"result": [1.0]}], QueryError("no model")]
        from particledb.client import ParticleDBError

        with self.assertRaises(ParticleDBError):
            db.ai.embed_batch(["a", "b"])

    def test_vector_create_index_name(self) -> None:
        wire = MagicMock()
        db = self._db(wire)
        db.vector.create_index("docs", "emb", name="my_idx", m=16)
        wire.execute.assert_called_once_with(
            'CREATE INDEX "my_idx" ON "docs" USING hnsw ("emb") WITH (m = 16)', None
        )


if __name__ == "__main__":
    unittest.main()
