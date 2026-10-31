"""Minimal, dependency-free Arrow IPC *stream* writer.

What it carries today: the rows of a native bulk ingest (``F_BULK`` DATA,
``Connection.bulk_insert``) -- one Schema message, one RecordBatch message,
end-of-stream marker; field names are the table's column names. The server
validates it with ``validate_ipc_stream_framing`` / ``validate_ipc_schema_types``
and conforms each column to the declared type (``engine::copy_conform``),
text through the type's input function. (Bound ``$n`` parameters no longer use
it: protocol 2 sends typed ``F_EXECUTE`` tags, ``particledb/_protocol.py``.)

This module writes exactly that subset, without pyarrow:

* column types: Int64, Float64, Utf8, Boolean, Binary -- all accepted by the
  server's ``validate_ipc_schema_types`` and bound by ``decode_arrow_param``.
  ``None`` is a null cell (validity bitmap) of a typed column; the server's
  schema check refuses the Arrow ``Null`` type, so a column whose cells are
  ALL ``None`` is written as a nullable Utf8 column of nulls.
* framing: continuation marker ``0xFFFFFFFF`` + i32 metadata length, metadata
  flatbuffer padded to 8 bytes, body buffers each padded to 8 bytes,
  little-endian, metadata version V5.

The flatbuffer encoder lays every object out front-to-back with parents
before children (so every ``uoffset`` is positive) and aligns every scalar to
its own size relative to the start of the flatbuffer, which is what the
flatbuffers verifier in the server's ``root_as_message`` checks.
"""

from __future__ import annotations

import array as _array
import datetime as _dt
import decimal as _decimal
import json as _json
import struct
import sys as _sys
from itertools import accumulate as _accumulate
from typing import Any, List, Optional, Sequence, Tuple

__all__ = [
    "encode_param_stream",
    "infer_column_type",
    "INT64",
    "FLOAT64",
    "UTF8",
    "BOOL",
    "BINARY",
]

# Logical column types this writer emits.
INT64 = "int64"
FLOAT64 = "float64"
UTF8 = "utf8"
BOOL = "bool"
BINARY = "binary"

# Arrow flatbuffer enum values (format/Schema.fbs, format/Message.fbs).
_METADATA_V5 = 4
_HEADER_SCHEMA = 1
_HEADER_RECORD_BATCH = 3
_TYPE_INT = 2
_TYPE_FLOAT = 3
_TYPE_BINARY = 4
_TYPE_UTF8 = 5
_TYPE_BOOL = 6
_PRECISION_DOUBLE = 2

_CONTINUATION = b"\xff\xff\xff\xff"
_EOS = _CONTINUATION + b"\x00\x00\x00\x00"

_LITTLE = _sys.byteorder == "little"
_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1


# ---------------------------------------------------------------------------
# Value -> column type
# ---------------------------------------------------------------------------


def _scalar_kind(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, bool):  # before int: bool is an int subclass
        return BOOL
    if isinstance(v, int):
        return INT64
    if isinstance(v, float):
        return FLOAT64
    if isinstance(v, (bytes, bytearray, memoryview)):
        return BINARY
    return UTF8  # str, Decimal, date/time, UUID, dict/list (JSON) ... as text


_TYPE_KIND = {bool: BOOL, int: INT64, float: FLOAT64, bytes: BINARY, bytearray: BINARY,
              memoryview: BINARY, str: UTF8}


def infer_column_type(values: Sequence[Any]) -> str:
    """Pick the Arrow column type for one parameter column.

    All-int -> Int64, ints mixed with floats -> Float64 (Utf8 text when an int
    is above 2^53, which a double cannot hold exactly), all-bool -> Boolean,
    all-bytes -> Binary; anything else (including a mix of kinds) -> Utf8 text,
    which the server coerces to the placeholder's SQL type. All-None -> Utf8.
    """
    # The value types, collected at C speed; exact-type lookups first, the
    # isinstance walk only for subclasses (numpy / pandas scalars, IntEnum).
    kinds = set()
    for t in set(map(type, values)):
        if t is type(None):
            continue
        k = _TYPE_KIND.get(t)
        if k is None:
            k = next((kk for kk in (_scalar_kind(v) for v in values if type(v) is t) if kk), UTF8)
        kinds.add(k)
    if not kinds:
        return UTF8
    if len(kinds) == 1:
        return next(iter(kinds))
    if kinds == {INT64, FLOAT64}:
        # An int above 2^53 is not exact as a double: send the column as text
        # (exact; the server coerces it to the placeholder's type) rather than
        # bind a rounded value.
        if any(
            _scalar_kind(v) == INT64 and abs(v) > _MAX_EXACT_FLOAT_INT for v in values
        ):
            return UTF8
        return FLOAT64
    return UTF8


# 2^53: every integer of at most this magnitude is exact in a float64.
_MAX_EXACT_FLOAT_INT = 1 << 53


def _to_text(v: Any) -> str:
    if isinstance(v, str):
        return v
    if isinstance(v, (dict, list, tuple)):
        return _json.dumps(v)
    if isinstance(v, (_dt.datetime, _dt.date, _dt.time)):
        return v.isoformat()
    if isinstance(v, _decimal.Decimal):
        return format(v, "f")
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).decode("utf-8")
    return str(v)


# ---------------------------------------------------------------------------
# Tiny front-to-back flatbuffer encoder
# ---------------------------------------------------------------------------

# A table is ("table", [(slot, kind, value), ...]); kinds:
#   "u8" / "bool" / "i16" / "i32" / "i64"  -> inline scalar
#   "ref"                                   -> uoffset to a child object
# Child objects: ("table", ...), ("str", text), ("tables", [table, ...]),
#                ("structs", raw_bytes, count)  (16-byte structs of 2 x i64)

_SCALAR_FMT = {"u8": ("<B", 1), "bool": ("<B", 1), "i16": ("<h", 2),
               "i32": ("<i", 4), "i64": ("<q", 8), "ref": ("<I", 4)}


class _FlatBuilder:
    def __init__(self) -> None:
        self.buf = bytearray()

    def _align(self, align: int, extra: int = 0) -> None:
        while (len(self.buf) + extra) % align:
            self.buf.append(0)

    def finish(self, root: tuple) -> bytes:
        self.buf += b"\x00\x00\x00\x00"  # root uoffset, patched below
        pos = self._write(root)
        struct.pack_into("<I", self.buf, 0, pos - 0)
        return bytes(self.buf)

    def _write(self, obj: tuple) -> int:
        kind = obj[0]
        if kind == "table":
            return self._write_table(obj[1])
        if kind == "str":
            data = obj[1].encode("utf-8")
            self._align(4)
            pos = len(self.buf)
            self.buf += struct.pack("<I", len(data)) + data + b"\x00"
            return pos
        if kind == "tables":
            items = obj[1]
            self._align(4)
            pos = len(self.buf)
            self.buf += struct.pack("<I", len(items))
            slots = []
            for _ in items:
                slots.append(len(self.buf))
                self.buf += b"\x00\x00\x00\x00"
            for slot, item in zip(slots, items):
                child = self._write(item)
                struct.pack_into("<I", self.buf, slot, child - slot)
            return pos
        if kind == "structs":
            raw, count = obj[1], obj[2]
            self._align(8, extra=4)  # elements (after the u32 length) 8-aligned
            pos = len(self.buf)
            self.buf += struct.pack("<I", count) + raw
            return pos
        raise ValueError(f"unknown flatbuffer object kind {kind!r}")

    def _write_table(self, fields: List[Tuple[int, str, Any]]) -> int:
        # Inline layout: soffset (4 bytes) at 0, then fields largest-first,
        # each aligned to its size. The table start is 8-aligned, so a field
        # aligned relative to the table is aligned absolutely too.
        order = sorted(fields, key=lambda f: -_SCALAR_FMT[f[1]][1])
        off = 4
        placed = []
        for slot, kind, value in order:
            size = _SCALAR_FMT[kind][1]
            off = (off + size - 1) // size * size
            placed.append((slot, kind, value, off))
            off += size
        tbl_size = (off + 3) // 4 * 4
        n_slots = (max(f[0] for f in fields) + 1) if fields else 0
        vt = [0] * n_slots
        for slot, _k, _v, foff in placed:
            vt[slot] = foff

        # vtable first (2-aligned), then the 8-aligned table after it.
        self._align(2)
        vt_pos = len(self.buf)
        self.buf += struct.pack("<HH", 4 + 2 * n_slots, tbl_size)
        self.buf += struct.pack(f"<{n_slots}H", *vt) if n_slots else b""
        self._align(8)
        tbl_pos = len(self.buf)
        inline = bytearray(tbl_size)
        struct.pack_into("<i", inline, 0, tbl_pos - vt_pos)
        refs = []
        for _slot, kind, value, foff in placed:
            if kind == "ref":
                refs.append((tbl_pos + foff, value))
            else:
                fmt = _SCALAR_FMT[kind][0]
                struct.pack_into(fmt, inline, foff, int(value))
        self.buf += inline
        for abs_pos, child in refs:
            child_pos = self._write(child)
            struct.pack_into("<I", self.buf, abs_pos, child_pos - abs_pos)
        return tbl_pos


def _message(header_type: int, header: tuple, body_len: int) -> bytes:
    return _FlatBuilder().finish(("table", [
        (0, "i16", _METADATA_V5),
        (1, "u8", header_type),
        (2, "ref", header),
        (3, "i64", body_len),
    ]))


def _field_type(col_type: str) -> Tuple[int, tuple]:
    if col_type == INT64:
        return _TYPE_INT, ("table", [(0, "i32", 64), (1, "bool", 1)])
    if col_type == FLOAT64:
        return _TYPE_FLOAT, ("table", [(0, "i16", _PRECISION_DOUBLE)])
    if col_type == UTF8:
        return _TYPE_UTF8, ("table", [])
    if col_type == BOOL:
        return _TYPE_BOOL, ("table", [])
    if col_type == BINARY:
        return _TYPE_BINARY, ("table", [])
    raise ValueError(f"unsupported parameter column type {col_type!r}")


def _schema_message(names: Sequence[str], types: Sequence[str]) -> bytes:
    fields = []
    for name, col_type in zip(names, types):
        type_tag, type_table = _field_type(col_type)
        fields.append(("table", [
            (0, "ref", ("str", name)),
            (1, "bool", 1),
            (2, "u8", type_tag),
            (3, "ref", type_table),
            (5, "ref", ("tables", [])),
        ]))
    schema = ("table", [(0, "i16", 0), (1, "ref", ("tables", fields))])
    return _message(_HEADER_SCHEMA, schema, 0)


def _frame(meta: bytes, body: bytes = b"") -> bytes:
    pad = (-(8 + len(meta))) % 8
    return (_CONTINUATION + struct.pack("<i", len(meta) + pad)
            + meta + b"\x00" * pad + body)


# ---------------------------------------------------------------------------
# Column buffers
# ---------------------------------------------------------------------------


def _bitmap(bits: Sequence[bool]) -> bytes:
    out = bytearray((len(bits) + 7) // 8)
    for i, b in enumerate(bits):
        if b:
            out[i >> 3] |= 1 << (i & 7)
    return bytes(out)


def _column_buffers(col_type: str, values: Sequence[Any], col: int) -> Tuple[int, List[bytes]]:
    """Return (null_count, [validity, *data buffers]) for one column."""
    null_count = values.count(None) if hasattr(values, "count") else sum(v is None for v in values)
    validity = _bitmap([v is not None for v in values]) if null_count else b""
    if col_type == INT64:
        vals = values if not null_count else [0 if v is None else v for v in values]
        try:
            a = _array.array("q", vals)  # C speed; OverflowError past 64 bits
        except OverflowError:
            for row, v in enumerate(values):
                if v is not None and not _INT64_MIN <= int(v) <= _INT64_MAX:
                    raise OverflowError(
                        f"parameter ${col + 1} (row {row}) = {v} does not fit BIGINT") from None
            raise
        except TypeError:  # int subclasses that array() refuses (numpy ints)
            a = _array.array("q", [int(v) for v in vals])
        if not _LITTLE:  # pragma: no cover
            a.byteswap()
        return null_count, [validity, a.tobytes()]
    if col_type == FLOAT64:
        vals = values if not null_count else [0.0 if v is None else v for v in values]
        try:
            a = _array.array("d", vals)
        except TypeError:
            a = _array.array("d", [float(v) for v in vals])
        if not _LITTLE:  # pragma: no cover
            a.byteswap()
        return null_count, [validity, a.tobytes()]
    if col_type == BOOL:
        return null_count, [validity, _bitmap([bool(v) for v in values])]
    # Variable width: Utf8 / Binary -> int32 offsets + data.
    if col_type == BINARY:
        chunks = [b"" if v is None else bytes(v) for v in values]
    elif not null_count and set(map(type, values)) == {str}:
        chunks = [v.encode("utf-8") for v in values]
    else:
        chunks = [b"" if v is None else _to_text(v).encode("utf-8") for v in values]
    offsets = _array.array("i", [0])
    lens = list(map(len, chunks))
    acc = list(_accumulate(lens))
    if acc and acc[-1] > 0x7FFFFFFF:
        raise OverflowError(f"parameter ${col + 1}: column data exceeds 2 GiB")
    offsets.extend(acc)
    if not _LITTLE:  # pragma: no cover
        offsets.byteswap()
    return null_count, [validity, offsets.tobytes(), b"".join(chunks)]


def encode_param_stream(
    rows: Sequence[Sequence[Any]],
    types: Optional[Sequence[str]] = None,
    names: Optional[Sequence[str]] = None,
) -> bytes:
    """Encode rows as an Arrow IPC stream (Schema + RecordBatch + EOS).

    Args:
        rows: One sequence of values per parameter set; every row must have
            the same number of values (one per ``$n`` placeholder).
        types: Optional explicit column types (``INT64``/``FLOAT64``/``UTF8``/
            ``BOOL``/``BINARY``); inferred per column with
            :func:`infer_column_type` when omitted.
        names: Optional field names (default ``p1..pN``); a native bulk
            ingest maps fields to table columns by these names.

    Returns:
        The stream bytes. A zero-column (no-parameter) input encodes as an
        empty ``bytes`` -- the server treats an empty payload as "no params".
    """
    n_rows = len(rows)
    n_cols = len(rows[0]) if n_rows else (len(types) if types else 0)
    for i, r in enumerate(rows):
        if len(r) != n_cols:
            raise ValueError(
                f"parameter row {i} has {len(r)} values, expected {n_cols}")
    if n_cols == 0:
        return b""
    columns = [list(c) for c in zip(*rows)]  # C-speed transpose
    if types is None:
        types = [infer_column_type(col) for col in columns]
    elif len(types) != n_cols:
        raise ValueError(f"{len(types)} column types for {n_cols} parameters")
    if names is None:
        names = [f"p{c + 1}" for c in range(n_cols)]
    elif len(names) != n_cols:
        raise ValueError(f"{len(names)} column names for {n_cols} columns")

    nodes = bytearray()
    buf_specs = bytearray()
    body = bytearray()
    for c, (col_type, col) in enumerate(zip(types, columns)):
        null_count, bufs = _column_buffers(col_type, col, c)
        nodes += struct.pack("<qq", n_rows, null_count)
        for b in bufs:
            buf_specs += struct.pack("<qq", len(body), len(b))
            body += b
            body += b"\x00" * ((-len(b)) % 8)
    batch = ("table", [
        (0, "i64", n_rows),
        (1, "ref", ("structs", bytes(nodes), len(nodes) // 16)),
        (2, "ref", ("structs", bytes(buf_specs), len(buf_specs) // 16)),
    ])
    out = bytearray()
    out += _frame(_schema_message(names, types))
    out += _frame(_message(_HEADER_RECORD_BATCH, batch, len(body)), bytes(body))
    out += _EOS
    return bytes(out)
