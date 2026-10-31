"""Byte-exact tests of the protocol-2 codec (``particledb/_protocol.py``).

Every expected byte string is written out from the server's own layouts
(``pdb_wire_v2.rs``, ``pdb_wire_params.rs`` module table,
``pdb_wire_mux.rs`` KV handlers), not from the encoder under test. No server.
"""

from __future__ import annotations

import datetime as dt
import decimal
import struct
import unittest
import uuid

from particledb import _protocol as P
from particledb._protocol import Float4, Int2, Int4, Json, Jsonb


def enc(v):
    b = bytearray()
    P.encode_param(b, v)
    return bytes(b)


class Hello(unittest.TestCase):
    def test_default_hello_asks_for_every_capability(self) -> None:
        h = P.encode_hello()
        plen, sid, t, flags = struct.unpack_from("<IHBB", h)
        self.assertEqual((sid, t, flags), (0, P.F_HELLO, 0))
        body = h[8:]
        self.assertEqual(plen, len(body))
        # [max_ver=2][num_caps=1][caps][num_fmts=1][ARROW][num_ext=1][ext]
        self.assertEqual(body, bytes([2, 1, 0x01 | 0x02 | 0x80 | 0x10 | 0x40, 1, 1, 1, 0x01 | 0x02 | 0x10 | 0x04]))

    def test_session_params_block(self) -> None:
        h = P.encode_hello(session_params=[("TimeZone", "UTC")])
        body = h[8:]
        self.assertEqual(body[6] & P.EXT_SESSION_PARAMS, P.EXT_SESSION_PARAMS)
        self.assertEqual(body[7:], struct.pack("<H", 1) + struct.pack("<H", 8) + b"TimeZone" + struct.pack("<H", 3) + b"UTC")

    def test_hello_ok_parse_and_names(self) -> None:
        p = bytes([2, 0x02 | 0x80 | 0x10 | 0x40 | 0x20, 1]) + struct.pack("<II", 36, 65536) + bytes([1, 0x17])
        h = P.HelloOk.parse(p)
        self.assertTrue(h.v2)
        self.assertTrue(h.auth_required)
        self.assertEqual(h.max_streams, 36)
        self.assertEqual(h.protocol, "pdb-native-v2")
        # AUTH_REQUIRED is posture, not a capability: never named.
        self.assertEqual(h.capability_names, sorted([
            "v2_mux", "prepared", "numeric_text", "statement_notice",
            "typed_params", "bulk_ingest", "batch_sqlstate", "array_params"]))

    def test_legacy_hello_ok(self) -> None:
        h = P.HelloOk.parse(bytes([1, 1]))
        self.assertFalse(h.v2)
        self.assertEqual(h.protocol, "pdb-native-v1")
        self.assertEqual(h.capability_names, [])

    def test_hello_ok_without_ext_block(self) -> None:
        h = P.HelloOk.parse(bytes([2, 0x02, 1]) + struct.pack("<II", 8, 0))
        self.assertEqual((h.ext, h.capability_names), (0, ["v2_mux"]))


class Frames(unittest.TestCase):
    def test_v2_header(self) -> None:
        f = P.v2_frame(P.F_QUERY, 7, b"SELECT 1")
        self.assertEqual(f[:11], bytes([2, 0x01, 0x01]) + struct.pack("<II", 7, 8))
        self.assertEqual(P.parse_v2_header(f), (P.F_QUERY, P.FLAG_END_STREAM, 7, 8))

    def test_bad_version_byte(self) -> None:
        with self.assertRaises(ValueError):
            P.parse_v2_header(bytes([1, 0, 0]) + bytes(8))

    def test_oversized_frame_refused(self) -> None:
        with self.assertRaises(ValueError):
            P.v2_frame(P.F_QUERY, 1, b"x" * (P.MAX_PAYLOAD + 1))

    def test_rst_and_goaway_payloads(self) -> None:
        self.assertEqual(P.rst_payload(P.ERR_CANCEL), b"\x07\x00")
        self.assertEqual(P.goaway_payload(9), struct.pack("<IH", 9, 0))

    def test_request_payloads(self) -> None:
        self.assertEqual(P.prepare_payload("s1", "SELECT $1"),
                         struct.pack("<I", 2) + b"s1" + struct.pack("<I", 9) + b"SELECT $1")
        self.assertEqual(P.batch_payload(["a", "bc"]),
                         struct.pack("<I", 2) + struct.pack("<I", 1) + b"a" + struct.pack("<I", 2) + b"bc")
        self.assertEqual(P.kv_payload(b"k", b"\x00v"), struct.pack("<I", 1) + b"k" + struct.pack("<I", 2) + b"\x00v")
        self.assertEqual(P.kv_payload(b"key"), struct.pack("<I", 3) + b"key")
        self.assertEqual(P.auth_payload("u", None), struct.pack("<I", 1) + b"u" + struct.pack("<I", 0))
        with self.assertRaises(ValueError):
            P.auth_payload("", "x")
        self.assertEqual(P.execute_payload(b"n", b"\x00", 1), struct.pack("<I", 1) + b"n" + struct.pack("<H", 1) + b"\x00")

    def test_bulk_payloads(self) -> None:
        self.assertEqual(P.bulk_begin_payload("t", ["a"]),
                         bytes([1]) + struct.pack("<H", 1) + b"t" + struct.pack("<H", 1) + struct.pack("<H", 1) + b"a"
                         + bytes([0]) + struct.pack("<Q", 0))
        self.assertEqual(P.bulk_end_payload(3), bytes([3]) + struct.pack("<Q", 3))


class TypedParams(unittest.TestCase):
    """The ``pdb_wire_params.rs`` tag table, value by value."""

    def test_scalars(self) -> None:
        self.assertEqual(enc(None), b"\x00")
        self.assertEqual(enc(-2), b"\x01" + struct.pack("<q", -2))
        self.assertEqual(enc("hé"), b"\x02" + struct.pack("<I", 3) + "hé".encode())
        self.assertEqual(enc(True), b"\x03\x01")
        self.assertEqual(enc(False), b"\x03\x00")
        self.assertEqual(enc(0.1), b"\x04" + struct.pack("<d", 0.1))
        self.assertEqual(enc(float("nan"))[:1], b"\x04")
        self.assertEqual(enc(Float4(1.5)), b"\x05" + struct.pack("<f", 1.5))
        self.assertEqual(enc(Int2(-7)), b"\x0d" + struct.pack("<h", -7))
        self.assertEqual(enc(Int4(123456)), b"\x0e" + struct.pack("<i", 123456))
        self.assertEqual(enc(Int4(None)), b"\x00")
        self.assertEqual(enc(b"\x00\xff"), b"\x0c" + struct.pack("<I", 2) + b"\x00\xff")
        self.assertEqual(enc(bytearray(b"ab")), b"\x0c" + struct.pack("<I", 2) + b"ab")
        u = uuid.UUID("123e4567-e89b-12d3-a456-426614174000")
        self.assertEqual(enc(u), b"\x0b" + u.bytes)
        self.assertEqual(enc(Json('{"a": 1}')), b"\x0f" + struct.pack("<I", 8) + b'{"a": 1}')
        self.assertEqual(enc(Jsonb({"a": 1})), b"\x10" + struct.pack("<I", 8) + b'{"a": 1}')
        self.assertEqual(enc({"a": 1}), b"\x10" + struct.pack("<I", 8) + b'{"a": 1}')

    def test_int_past_64_bits_is_numeric(self) -> None:
        v = 1 << 70
        out = enc(v)
        self.assertEqual(out[0], P.P_NUMERIC)
        scale, n = struct.unpack_from("<iI", out, 1)
        self.assertEqual(scale, 0)
        self.assertEqual(int.from_bytes(out[9:9 + n], "big", signed=True), v)

    def test_numeric_keeps_scale_and_sign(self) -> None:
        for text, scale, unscaled in (("1.50", 2, 150), ("-12345678901234567890.0001", 4, -123456789012345678900001),
                                      ("0", 0, 0), ("-0.001", 3, -1), ("1E+3", 0, 1000), ("128", 0, 128),
                                      ("-128", 0, -128), ("255", 0, 255)):
            out = enc(decimal.Decimal(text))
            self.assertEqual(out[0], P.P_NUMERIC, text)
            s, n = struct.unpack_from("<iI", out, 1)
            mag = out[9:9 + n]
            self.assertEqual(len(out), 9 + n)
            self.assertEqual((s, int.from_bytes(mag, "big", signed=True)), (scale, unscaled), text)
            # minimal two's complement: BigInteger.toByteArray's length
            self.assertEqual(n, max(1, (unscaled.bit_length() + 8) // 8), text)

    def test_numeric_nan_travels_as_text(self) -> None:
        self.assertEqual(enc(decimal.Decimal("NaN")), b"\x02" + struct.pack("<I", 3) + b"NaN")

    def test_dates_and_times(self) -> None:
        self.assertEqual(enc(dt.date(1970, 1, 2)), b"\x07" + struct.pack("<i", 1))
        self.assertEqual(enc(dt.date(1969, 12, 31)), b"\x07" + struct.pack("<i", -1))
        self.assertEqual(enc(dt.datetime(2020, 6, 15, 12, 34, 56, 789012)),
                         b"\x08" + struct.pack("<q", 1592224496789012))
        tz = dt.datetime(2024, 7, 1, 2, 0, 0, 500000, tzinfo=dt.timezone(dt.timedelta(hours=2)))
        self.assertEqual(enc(tz), b"\x09" + struct.pack("<q", 1719792000500000) + struct.pack("<i", 7200))
        self.assertEqual(enc(dt.time(23, 59, 59, 123456)), b"\x0a" + struct.pack("<q", 86399123456))
        with self.assertRaises(TypeError):
            enc(dt.time(1, 0, tzinfo=dt.timezone.utc))

    def test_arrays(self) -> None:
        self.assertEqual(enc([7, None, -9]),
                         bytes([17, P.P_INT8, 1]) + struct.pack("<I", 3)
                         + b"\x01" + struct.pack("<q", 7) + b"\x00" + b"\x01" + struct.pack("<q", -9))
        self.assertEqual(enc([]), bytes([17, P.P_TEXT, 0]))
        self.assertEqual(enc([None, None]), bytes([17, P.P_TEXT, 1]) + struct.pack("<I", 2) + b"\x00\x00")
        two = enc([[1, 2], [3, 4]])
        self.assertEqual(two[:3 + 8], bytes([17, P.P_INT8, 2]) + struct.pack("<II", 2, 2))
        # int + float widens to float8, elements re-tagged
        mixed = enc([1, 2.5])
        self.assertEqual(mixed[:2], bytes([17, P.P_FLOAT8]))
        self.assertEqual(mixed[7:16], b"\x04" + struct.pack("<d", 1.0))
        with self.assertRaises(ValueError):
            enc([[1, 2], [3]])
        with self.assertRaises(ValueError):
            enc([[1], 2])
        with self.assertRaises(TypeError):
            enc([1, "a"])

    def test_unbindable(self) -> None:
        with self.assertRaises(TypeError):
            enc(object())

    def test_needs_ext(self) -> None:
        self.assertEqual(P.needs_ext([1, "a", None, True]), 0)
        self.assertEqual(P.needs_ext([1.5]), P.EXT_TYPED_PARAMS)
        self.assertEqual(P.needs_ext([[1]]), P.EXT_ARRAY_PARAMS)


class Responses(unittest.TestCase):
    def test_batch_result_entries(self) -> None:
        p = (struct.pack("<I", 3) + bytes([P.F_OK]) + struct.pack("<Q", 2)
             + bytes([P.F_ARROW_RESULT]) + struct.pack("<I", 3) + b"ipc"
             + bytes([0x90]) + struct.pack("<H", 23505) + b"23505" + struct.pack("<I", 3) + b"dup")
        r = P.decode_batch_result(p)
        self.assertEqual((r.counts, r.entries, r.error, r.error_code, r.error_sqlstate),
                         ([2, -1], 3, "dup", 23505, "23505"))
        self.assertEqual(r.results[1], b"ipc")

    def test_batch_result_legacy_error_entry(self) -> None:
        p = struct.pack("<I", 1) + bytes([P.F_ERROR]) + struct.pack("<I", 2) + b"no"
        r = P.decode_batch_result(p)
        self.assertEqual((r.error, r.error_sqlstate), ("no", None))

    def test_batch_result_malformed(self) -> None:
        for p in (b"\x01\x00", struct.pack("<I", 1) + bytes([0x77]) + bytes(8),
                  struct.pack("<I", 1) + bytes([P.F_OK]) + bytes(3),
                  struct.pack("<I", 2) + bytes([P.F_ERROR]) + struct.pack("<I", 1) + b"x" + bytes([P.F_OK]) + bytes(8),
                  struct.pack("<I", 99) + bytes(10)):
            with self.assertRaises(ValueError, msg=p.hex()):
                P.decode_batch_result(p)

    def test_notices(self) -> None:
        one = bytes([7]) + b"WARNING" + bytes([5]) + b"01000" + struct.pack("<I", 2) + b"hi"
        self.assertEqual(P.decode_notices(struct.pack("<H", 1) + one), [("WARNING", "01000", "hi")])
        self.assertIsNone(P.decode_notices(struct.pack("<H", 2) + one))
        self.assertIsNone(P.decode_notices(struct.pack("<H", 1) + one + b"x"))

    def test_ok_and_error(self) -> None:
        self.assertEqual(P.decode_ok(struct.pack("<Q", 5)), 5)
        self.assertEqual(P.decode_error(struct.pack("<H", 42601) + b"bad"), (42601, "bad"))


class KvCmd(unittest.TestCase):
    """The F_KV_CMD codec, byte-for-byte against the server's KV command codec."""

    def test_request_layout(self) -> None:
        p = P.kv_cmd_payload("HSET", ["h", b"\x00", 7, 1.5])
        want = (bytes([1, 0]) + struct.pack("<H", 1) + struct.pack("<HH", 0, 5)
                + struct.pack("<BI", 1, 4) + b"HSET" + struct.pack("<BI", 1, 1) + b"h"
                + struct.pack("<BI", 1, 1) + b"\x00" + struct.pack("<Bq", 2, 7) + struct.pack("<Bd", 3, 1.5))
        self.assertEqual(p, want)
        t = P.kv_txn_payload([["SET", "a", "1"], ["INCR", "a"]])
        self.assertEqual(t[:4], bytes([1, P.KVF_TXN]) + struct.pack("<H", 2))

    def test_reply_decoding(self) -> None:
        def lp(b: bytes) -> bytes:
            return struct.pack("<I", len(b)) + b
        body = (bytes([4]) + struct.pack("<I", 6) + bytes([0]) + bytes([1]) + struct.pack("<q", -3)
                + bytes([2]) + lp(b"v") + bytes([3]) + lp(b"OK")
                + bytes([6]) + struct.pack("<H", 42809) + b"42809" + lp(b"WRONGTYPE x")
                + bytes([7]) + struct.pack("<I", 1) + bytes([2]) + lp(b"f") + bytes([2]) + lp(b"1"))
        v = P.decode_kv_reply(bytes([1]) + body)
        self.assertEqual(v[:4], [None, -3, b"v", "OK"])
        self.assertIsInstance(v[3], P.KvStatus)
        self.assertIsInstance(v[4], P.KvReplyError)
        self.assertEqual((v[4].sqlstate, v[4].text), ("42809", "WRONGTYPE x"))
        self.assertEqual(v[5], {b"f": b"1"})
        with self.assertRaises(ValueError):
            P.decode_kv_reply(bytes([1, 0, 0]))


class SqlState(unittest.TestCase):
    def test_code_mapping(self) -> None:
        cases = {23505: "23505", 42601: "42601", 60001: "42P01", 60003: "25P02", 60033: "55P02",
                 60999: "HY000", 8006: "08006", 2000: "02000", 1064: "HY000", 0x07: "57014",
                 0x14: "28P01", 26: "26000", 0: "08006", 5: "08P01", 99: "HY000"}
        for code, want in cases.items():
            self.assertEqual(P.sqlstate_for_code(code), want, code)

    def test_marker_and_framing(self) -> None:
        self.assertEqual(P.server_error(1064, "SQL execution error: boom [P0001]"), ("P0001", "boom"))
        self.assertEqual(P.server_error(60033, "55P02: parameter is read-only"), ("55P02", "parameter is read-only"))
        self.assertEqual(P.server_error(0x15, "22023: FATAL: bad TimeZone"), ("22023", "FATAL: bad TimeZone"))
        self.assertEqual(P.server_error(22012, "division by zero"), ("22012", "division by zero"))
        # a bracket that is not a SQLSTATE stays in the text
        self.assertEqual(P.server_error(1064, "bad [value]"), ("HY000", "bad [value]"))


if __name__ == "__main__":
    unittest.main()
