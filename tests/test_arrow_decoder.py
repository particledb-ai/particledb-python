"""The built-in Arrow IPC reader (``particledb/_arrow.py``) and its conversion
layer, checked against pyarrow (when installed) on a broad type matrix, fed
whole and byte by byte. No server.
"""

from __future__ import annotations

import datetime as dt
import decimal
import io
import math
import struct
import unittest
import uuid

from particledb import _arrow as A
from particledb.arrow_ipc import encode_param_stream

try:
    import pyarrow as pa

    HAVE_PA = True
except ImportError:  # pragma: no cover
    HAVE_PA = False


def ipc(table, chunk=None) -> bytes:
    s = io.BytesIO()
    w = pa.ipc.new_stream(s, table.schema)
    w.write_table(table, max_chunksize=chunk)
    w.close()
    return s.getvalue()


def norm(v):
    """NaN-aware comparable form."""
    if isinstance(v, float) and math.isnan(v):
        return "NaN"
    if isinstance(v, list):
        return [norm(e) for e in v]
    if isinstance(v, dict):
        return {k: norm(e) for k, e in v.items()}
    return v


class BuiltinWithoutPyarrow(unittest.TestCase):
    """What a plain install decodes: our own writer's types, no pyarrow."""

    def test_basic_types(self) -> None:
        rows = [[1, 1.5, "héllo", True, b"\x00\xff"], [None, None, None, None, None], [-(1 << 63), -0.0, "", False, b""]]
        schema, got = A.decode_stream(encode_param_stream(rows), use_pyarrow=False)
        self.assertEqual(schema.names, ["p1", "p2", "p3", "p4", "p5"])
        self.assertEqual([list(r) for r in got], rows)

    def test_empty_stream_and_truncation(self) -> None:
        self.assertEqual(A.decode_stream(b"", use_pyarrow=False)[1], [])
        data = encode_param_stream([[1]])
        with self.assertRaises(A.ArrowDecodeError):
            A.decode_stream(data[:-12], use_pyarrow=False)

    def test_incremental_byte_by_byte(self) -> None:
        data = encode_param_stream([[i, f"v{i}"] for i in range(50)])
        d = A.StreamDecoder(use_pyarrow=False)
        rows = []
        for i in range(len(data)):
            d.feed(data[i: i + 1])
            for cols in d.batches():
                rows.extend(zip(*cols))
        self.assertTrue(d.ended)
        self.assertEqual(rows, [(i, f"v{i}") for i in range(50)])


class Conversion(unittest.TestCase):
    """The ParticleDB layer keyed on ``pdb:decl`` / ``pdb:tz``."""

    def _field(self, kind, decl=None, meta=None, **params):
        f = A.Field()
        f.kind = kind
        f.params = params
        f.metadata = dict(meta or {})
        if decl:
            f.metadata["pdb:decl"] = decl
        f.decl = f.metadata.get("pdb:decl")
        return f

    def test_numeric_text_keeps_scale(self) -> None:
        f = self._field(A._UTF8, "NUMERIC")
        got = A._convert_column(f, ["0.52", "1.50", None, "NaN"])
        self.assertEqual([None if g is None else str(g) for g in got], ["0.52", "1.50", None, "NaN"])
        self.assertTrue(all(isinstance(g, decimal.Decimal) for g in got if g is not None))
        self.assertEqual(str(A._convert_column(f, ["1.50"])[0]), "1.50")
        # a CONSTRAINED numeric decl is not the per-value text path
        self.assertEqual(A._convert_column(self._field(A._UTF8, "varchar(5)"), ["1.50"]), ["1.50"])

    def test_vector(self) -> None:
        f = self._field(A._BINARY, "vector(3)")
        raw = struct.pack("<3f", 0.5, -1, 0)
        self.assertEqual(A._convert_column(f, [raw, None]), [[0.5, -1.0, 0.0], None])
        with self.assertRaises(A.ArrowDecodeError):
            A._convert_column(f, [b"\x00\x00\x00"])

    def test_uuid(self) -> None:
        f = self._field(A._UTF8, "UUID")
        u = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
        self.assertEqual(A._convert_column(f, [u, None]), [uuid.UUID(u), None])
        self.assertEqual(A._convert_column(self._field(A._UTF8), [u]), [u])  # no decl: text

    def test_timestamptz(self) -> None:
        f = self._field(A._TIMESTAMP, "TIMESTAMPTZ", {"pdb:tz": "America/Los_Angeles"}, unit=2, tz=None)
        got = A._convert_column(f, [dt.datetime(2024, 3, 10, 10, 0), None])
        self.assertEqual(got[0], dt.datetime(2024, 3, 10, 10, 0, tzinfo=dt.timezone.utc))
        self.assertIsNone(got[1])
        plain = self._field(A._TIMESTAMP, "TIMESTAMP", unit=2, tz=None)
        self.assertIsNone(A._convert_column(plain, [dt.datetime(2020, 1, 1)])[0].tzinfo)

    def test_array_text(self) -> None:
        f = self._field(A._UTF8, "integer[]")
        self.assertEqual(A._convert_column(f, ["{1,NULL,3}", "{}", None, "{{1,2},{3,4}}"]),
                         [[1, None, 3], [], None, [[1, 2], [3, 4]]])
        t = self._field(A._UTF8, "TEXT[]")
        self.assertEqual(A._convert_column(t, ['{a,NULL,"c,d","x\\"y","NULL"}']),
                         [["a", None, "c,d", 'x"y', "NULL"]])
        n = self._field(A._UTF8, "numeric[]")
        self.assertEqual(A._convert_column(n, ["{1.50,-2}"]), [[decimal.Decimal("1.50"), decimal.Decimal("-2")]])
        b = self._field(A._UTF8, "boolean[]")
        self.assertEqual(A._convert_column(b, ["{t,f,NULL}"]), [[True, False, None]])

    def test_parse_pg_array_edge_cases(self) -> None:
        self.assertEqual(A.parse_pg_array('{"a\\\\b"}'), ["a\\b"])
        self.assertEqual(A.parse_pg_array("{ 1 , 2 }", int), [1, 2])
        self.assertEqual(A.parse_pg_array("[0:1]={7,8}", int), [7, 8])

    def test_out_of_range_dates_are_text_not_wrong(self) -> None:
        self.assertEqual(A._date_or_text(-719528), "0001-01-01 BC")  # proleptic year 0
        self.assertEqual(A._date_or_text(-719162), dt.date(1, 1, 1))
        self.assertEqual(A._date_or_text(2932897), "10000-01-01")
        self.assertEqual(A._date_or_text(0), dt.date(1970, 1, 1))


@unittest.skipUnless(HAVE_PA, "pyarrow not installed")
class ParityWithPyarrow(unittest.TestCase):
    def table(self):
        return pa.table({
            "i8": pa.array([1, None, -128], pa.int8()), "u16": pa.array([0, 65535, None], pa.uint16()),
            "i64": pa.array([2 ** 62, None, -(2 ** 63)], pa.int64()),
            "f32": pa.array([1.5, float("nan"), None], pa.float32()),
            "f64": pa.array([float("inf"), -0.0, float("nan")], pa.float64()),
            "s": pa.array(["a", None, "é✓𝄞"]), "ls": pa.array(["x", "", None], pa.large_string()),
            "b": pa.array([b"\x00", None, b""]), "lb": pa.array([b"q", None, b""], pa.large_binary()),
            "fsb": pa.array([b"0123456789abcdef", None, bytes(16)], pa.binary(16)),
            "bool": pa.array([True, None, False]),
            "dec": pa.array([decimal.Decimal("1.50"), None, decimal.Decimal("-12345678901234567890.0001")],
                            pa.decimal128(38, 4)),
            "dec256": pa.array([decimal.Decimal("1.5"), None, decimal.Decimal("-2")], pa.decimal256(50, 1)),
            "d32": pa.array([dt.date(2024, 2, 29), None, dt.date(1, 1, 1)], pa.date32()),
            "d64": pa.array([dt.date(1999, 12, 31), None, dt.date(2000, 1, 1)], pa.date64()),
            "ts": pa.array([dt.datetime(1999, 12, 31, 23, 59, 59, 999999), None, dt.datetime(1, 1, 1)], pa.timestamp("us")),
            "tsms": pa.array([dt.datetime(2020, 1, 1, 0, 0, 0, 5000), None, None], pa.timestamp("ms")),
            "tsz": pa.array([dt.datetime(2024, 7, 1, 0, 0, 0, 500000, tzinfo=dt.timezone.utc), None, None],
                            pa.timestamp("us", tz="UTC")),
            "t64": pa.array([dt.time(23, 59, 59, 123456), None, dt.time(0)], pa.time64("us")),
            "t32": pa.array([dt.time(1, 2, 3), None, dt.time(0)], pa.time32("s")),
            "dur": pa.array([dt.timedelta(seconds=5), None, dt.timedelta(0)], pa.duration("us")),
            "l": pa.array([[1, None], None, []], pa.list_(pa.int32())),
            "ll": pa.array([["a"], None, []], pa.large_list(pa.string())),
            "fsl": pa.array([[1.0, 2.0], None, [0.5, -1.0]], pa.list_(pa.float32(), 2)),
            "nested": pa.array([[[1], [2, 3]], None, [[]]], pa.list_(pa.list_(pa.int64()))),
            "st": pa.array([{"x": 1, "y": "a"}, None, {"x": None, "y": None}]),
            "map": pa.array([[("k", 1)], None, []], pa.map_(pa.string(), pa.int64())),
            "dict": pa.array(["x", "y", None]).dictionary_encode(),
            "null": pa.array([None, None, None], pa.null()),
        })

    def test_matrix_both_paths_equal(self) -> None:
        t = self.table()
        for chunk in (None, 1, 2):
            data = ipc(t, chunk)
            s1, builtin = A.decode_stream(data, use_pyarrow=False)
            s2, viapa = A.decode_stream(data, use_pyarrow=True)
            self.assertEqual(s1.names, t.column_names)
            self.assertEqual(norm([list(r) for r in builtin]), norm([list(r) for r in viapa]), chunk)
            # and both equal pyarrow's own values (row-wise, tz normalised)
            want = [[c[i].as_py() for c in t.columns] for i in range(t.num_rows)]
            self.assertEqual(len(builtin), len(want))
            for got_row, want_row, name in zip(builtin, want, [t.column_names] * len(want)):
                for g, w, n in zip(got_row, want_row, name):
                    if n == "map":
                        w = None if w is None else dict(w)
                    if n == "tsz" and w is not None:
                        w = w.astimezone(dt.timezone.utc)
                    self.assertEqual(norm(g), norm(w), n)

    def test_duplicate_labels_kept_positionally(self) -> None:
        t = pa.Table.from_arrays([pa.array([1]), pa.array([2])], names=["x", "x"])
        _, rows = A.decode_stream(ipc(t), use_pyarrow=False)
        self.assertEqual(rows, [(1, 2)])

    def test_zero_rows_keeps_schema(self) -> None:
        t = pa.table({"a": pa.array([], pa.int64()), "b": pa.array([], pa.string())})
        s, rows = A.decode_stream(ipc(t), use_pyarrow=False)
        self.assertEqual((s.names, rows), (["a", "b"], []))

    def test_incremental_split_anywhere(self) -> None:
        data = ipc(self.table(), 1)
        for step in (1, 7, 64, 1000):
            d = A.StreamDecoder(use_pyarrow=False)
            rows = []
            for i in range(0, len(data), step):
                d.feed(data[i: i + step])
                for cols in d.batches():
                    rows.extend(zip(*cols))
            self.assertEqual(norm([list(r) for r in rows]),
                             norm([list(r) for r in A.decode_stream(data, use_pyarrow=False)[1]]), step)

    def test_metadata_drives_conversion_in_both_paths(self) -> None:
        f = pa.field("n", pa.string(), metadata={"pdb:decl": "numeric"})
        g = pa.field("v", pa.binary(), metadata={"pdb:decl": "vector(2)"})
        t = pa.Table.from_arrays([pa.array(["0.520", None]), pa.array([struct.pack("<2f", 1, 2), None])],
                                 schema=pa.schema([f, g]))
        for use in (False, True):
            _, rows = A.decode_stream(ipc(t), use_pyarrow=use)
            self.assertEqual(rows, [(decimal.Decimal("0.520"), [1.0, 2.0]), (None, None)], use)
            self.assertEqual(str(rows[0][0]), "0.520")

    def test_unsupported_type_raises(self) -> None:
        un = pa.UnionArray.from_sparse(pa.array([0], pa.int8()), [pa.array([1])])
        with self.assertRaises(A.ArrowDecodeError):
            A.decode_stream(ipc(pa.table({"u": un})), use_pyarrow=False)


if __name__ == "__main__":
    unittest.main()
