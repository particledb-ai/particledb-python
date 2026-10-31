"""Arrow IPC stream READER for native-wire results: built in, no pyarrow needed.

The server answers every row-returning statement with an Arrow IPC *stream*
(``F_ARROW_RESULT``, or ``F_ARROW_STREAM`` chunks that split ONE stream at
arbitrary byte offsets, sliced by the server at <= 65,536 rows / ~4 MiB per
record batch). This module decodes the subset of the format the server emits,
in pure Python, so a plain ``pip install particledb`` returns typed values;
when pyarrow is installed it is used for the per-batch column decode (faster),
and both paths go through the SAME ParticleDB-level conversion
(:func:`_convert_column`), so a value never depends on whether pyarrow is
present.

What is decoded (``format/Schema.fbs`` / ``Message.fbs``, metadata V4/V5,
little-endian, uncompressed):

* Null, Bool, Int (8..64, signed/unsigned), FloatingPoint (16/32/64), Utf8,
  LargeUtf8, Binary, LargeBinary, FixedSizeBinary, Decimal (128/256), Date
  (day/ms), Time, Timestamp (any unit; with a zone the value is an aware UTC
  ``datetime``), Duration, Interval, List, LargeList, FixedSizeList, Struct,
  Map, dictionary-encoded fields.
* Anything else (views, unions, run-end encoding, compressed bodies,
  big-endian) raises :class:`ArrowDecodeError` -- never raw bytes.

ParticleDB conversion (the engine stamps each field's declared SQL type in the
field's ``pdb:decl`` metadata):

* an unconstrained ``numeric`` column arrives as per-value-scale TEXT under
  ``CAP_NUMERIC_TEXT``; it is returned as ``decimal.Decimal`` with exactly the
  scale the server wrote (``0.52`` stays ``0.52``);
* ``vector`` arrives as Binary (little-endian float32 components) and is
  returned as ``list[float]``;
* ``timestamptz`` arrives as a zone-less Arrow timestamp holding the UTC
  instant (the session zone in ``pdb:tz``) and is returned as an aware UTC
  ``datetime``;
* ``uuid`` arrives as text (or 16 bytes) and is returned as ``uuid.UUID``
  when the field's ``pdb:decl`` says ``uuid`` (an expression such as
  ``'..'::uuid`` carries no declaration and stays text);
* an array column the engine carries as text is parsed back into a list of the
  element type.
"""

from __future__ import annotations

import array as _array
import datetime as _dt
import os
import decimal as _decimal
import json as _json
import struct
import sys
import uuid as _uuid
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple

import importlib.util as _ilu

#: pyarrow is OPTIONAL and imported only when used (``query_arrow()``, or the
#: opt-in pyarrow decoder): importing it costs ~25 MiB of RSS and its import
#: time, which a plain ``import particledb`` should not pay.
HAVE_PYARROW = _ilu.find_spec("pyarrow") is not None
_pa_mods: Optional[Tuple[Any, Any]] = None


def pyarrow_modules() -> Tuple[Any, Any]:
    """``(pyarrow, pyarrow.ipc)``, imported on first use."""
    global _pa_mods
    if _pa_mods is None:
        import pyarrow as pa  # type: ignore
        import pyarrow.ipc as pa_ipc  # type: ignore

        _pa_mods = (pa, pa_ipc)
    return _pa_mods


#: Whether record batches are decoded with pyarrow by default. OFF: measured on
#: this server's results the built-in decoder materialises Python rows faster
#: than pyarrow's ``to_pylist`` at every size (one-row point result ~1.5x,
#: 2k-65k-row batches 2-2.5x), so pyarrow
#: is what ``query_arrow()`` returns tables with, and an opt-in decoder
#: (``Connection(use_pyarrow=True)`` or ``PDB_PY_DECODER=pyarrow``).
USE_PYARROW = HAVE_PYARROW and os.environ.get("PDB_PY_DECODER") == "pyarrow"

_LITTLE = sys.byteorder == "little"


class ArrowDecodeError(ValueError):
    """The result bytes are not an Arrow IPC stream this client can decode."""


# ═════════════════════════════════════════════════════════════════════════════
# Flatbuffers (read only)
# ═════════════════════════════════════════════════════════════════════════════

class _T:
    """A flatbuffer table view."""

    __slots__ = ("b", "pos", "vt", "vtlen")

    def __init__(self, b: bytes, pos: int):
        self.b = b
        self.pos = pos
        (soff,) = struct.unpack_from("<i", b, pos)
        self.vt = pos - soff
        (self.vtlen,) = struct.unpack_from("<H", b, self.vt)

    def _off(self, i: int) -> int:
        o = 4 + 2 * i
        if o >= self.vtlen:
            return 0
        return struct.unpack_from("<H", self.b, self.vt + o)[0]

    def scalar(self, i: int, fmt: str, default: Any = 0) -> Any:
        o = self._off(i)
        return default if o == 0 else struct.unpack_from("<" + fmt, self.b, self.pos + o)[0]

    def _ref(self, i: int) -> int:
        o = self._off(i)
        if o == 0:
            return -1
        p = self.pos + o
        return p + struct.unpack_from("<I", self.b, p)[0]

    def table(self, i: int) -> Optional["_T"]:
        p = self._ref(i)
        return None if p < 0 else _T(self.b, p)

    def string(self, i: int) -> Optional[str]:
        p = self._ref(i)
        if p < 0:
            return None
        (n,) = struct.unpack_from("<I", self.b, p)
        return bytes(self.b[p + 4: p + 4 + n]).decode("utf-8")

    def vec(self, i: int) -> Tuple[int, int]:
        """``(element_start, count)``; ``(0, 0)`` when absent."""
        p = self._ref(i)
        if p < 0:
            return 0, 0
        (n,) = struct.unpack_from("<I", self.b, p)
        return p + 4, n

    def tables(self, i: int) -> List["_T"]:
        start, n = self.vec(i)
        out = []
        for k in range(n):
            e = start + 4 * k
            out.append(_T(self.b, e + struct.unpack_from("<I", self.b, e)[0]))
        return out


# Type union tags (Schema.fbs `Type`).
_NULL, _INT, _FLOAT, _BINARY, _UTF8, _BOOL, _DECIMAL, _DATE, _TIME, _TIMESTAMP = range(1, 11)
_INTERVAL, _LIST, _STRUCT, _UNION, _FSB, _FSL, _MAP, _DURATION = range(11, 19)
_LARGE_BINARY, _LARGE_UTF8, _LARGE_LIST = 19, 20, 21

_TYPE_NAMES = {
    _NULL: "null", _INT: "int", _FLOAT: "float", _BINARY: "binary", _UTF8: "utf8", _BOOL: "bool",
    _DECIMAL: "decimal", _DATE: "date", _TIME: "time", _TIMESTAMP: "timestamp", _INTERVAL: "interval",
    _LIST: "list", _STRUCT: "struct", _UNION: "union", _FSB: "fixed_size_binary", _FSL: "fixed_size_list",
    _MAP: "map", _DURATION: "duration", _LARGE_BINARY: "large_binary", _LARGE_UTF8: "large_utf8",
    _LARGE_LIST: "large_list",
}


class Field:
    """One schema field: name, Arrow type, children, dictionary, metadata."""

    __slots__ = ("name", "nullable", "kind", "params", "children", "dict_id", "dict_index",
                 "metadata", "decl", "_cv")

    def __init__(self) -> None:
        self.name = ""
        self.nullable = True
        self.kind = 0
        self.params: Dict[str, Any] = {}
        self.children: List["Field"] = []
        self.dict_id: Optional[int] = None
        self.dict_index: Optional[Tuple[int, bool]] = None  # (bit width, signed)
        self.metadata: Dict[str, str] = {}
        self.decl: Optional[str] = None
        self._cv: Optional[List[Any]] = None  # converter_for, cached per profile

    @property
    def type_name(self) -> str:
        """A short Arrow logical type token, e.g. ``int64``, ``decimal(38,2)``."""
        k, p = self.kind, self.params
        if k == _INT:
            return ("int" if p["signed"] else "uint") + str(p["bits"])
        if k == _FLOAT:
            return {16: "float16", 32: "float32", 64: "float64"}[p["bits"]]
        if k == _DECIMAL:
            return f"decimal({p['precision']},{p['scale']})"
        if k == _TIMESTAMP:
            return "timestamp" + ("[tz]" if p.get("tz") else "")
        return _TYPE_NAMES.get(k, f"type{k}")


def _parse_field(t: _T) -> Field:
    f = Field()
    f.name = t.string(0) or ""
    f.nullable = bool(t.scalar(1, "B", 0))
    f.kind = t.scalar(2, "B", 0)
    ty = t.table(3)
    p = f.params
    k = f.kind
    if k == _INT:
        p["bits"] = ty.scalar(0, "i", 0) if ty else 0
        p["signed"] = bool(ty.scalar(1, "B", 0)) if ty else False
    elif k == _FLOAT:
        p["bits"] = {0: 16, 1: 32, 2: 64}[ty.scalar(0, "h", 0) if ty else 0]
    elif k == _DECIMAL:
        p["precision"] = ty.scalar(0, "i", 0)
        p["scale"] = ty.scalar(1, "i", 0)
        p["bits"] = ty.scalar(2, "i", 128)
    elif k == _DATE:
        p["unit"] = ty.scalar(0, "h", 1) if ty else 1  # 0 day, 1 millisecond
    elif k == _TIME:
        p["unit"] = ty.scalar(0, "h", 1) if ty else 1
        p["bits"] = ty.scalar(1, "i", 32) if ty else 32
    elif k == _TIMESTAMP:
        p["unit"] = ty.scalar(0, "h", 0)
        p["tz"] = ty.string(1)
    elif k in (_DURATION,):
        p["unit"] = ty.scalar(0, "h", 1) if ty else 1
    elif k == _INTERVAL:
        p["unit"] = ty.scalar(0, "h", 0) if ty else 0
    elif k == _FSB:
        p["width"] = ty.scalar(0, "i", 0)
    elif k == _FSL:
        p["size"] = ty.scalar(0, "i", 0)
    elif k in (_NULL, _BINARY, _UTF8, _BOOL, _LIST, _STRUCT, _MAP, _LARGE_BINARY, _LARGE_UTF8, _LARGE_LIST):
        pass
    else:
        raise ArrowDecodeError(f"field {f.name!r}: Arrow type {_TYPE_NAMES.get(k, k)} is not supported")
    d = t.table(4)
    if d is not None:
        f.dict_id = d.scalar(0, "q", 0)
        it = d.table(1)
        f.dict_index = ((it.scalar(0, "i", 32), bool(it.scalar(1, "B", 1))) if it else (32, True))
    f.children = [_parse_field(c) for c in t.tables(5)]
    for kv in t.tables(6):
        f.metadata[kv.string(0) or ""] = kv.string(1) or ""
    f.decl = f.metadata.get("pdb:decl")
    return f


# ═════════════════════════════════════════════════════════════════════════════
# IPC messages
# ═════════════════════════════════════════════════════════════════════════════

_H_SCHEMA, _H_DICT, _H_BATCH = 1, 2, 3


class _Msg:
    __slots__ = ("kind", "table", "body")

    def __init__(self, kind: int, table: _T, body: memoryview):
        self.kind = kind
        self.table = table
        self.body = body


def split_message(buf: bytes, pos: int) -> Optional[Tuple[int, int, int]]:
    """Locate the message at ``pos``: ``(meta_start, meta_len, total_len)``,
    ``(0, 0, n)`` for end-of-stream, or ``None`` if ``buf`` holds less than one
    whole message (the caller waits for more bytes)."""
    if len(buf) - pos < 4:
        return None
    (word,) = struct.unpack_from("<I", buf, pos)
    if word == 0xFFFFFFFF:
        if len(buf) - pos < 8:
            return None
        (mlen,) = struct.unpack_from("<i", buf, pos + 4)
        hdr = 8
    else:  # pre-0.15 framing: no continuation marker
        mlen = struct.unpack_from("<i", buf, pos)[0]
        hdr = 4
    if mlen == 0:
        return (0, 0, hdr)
    if mlen < 0:
        raise ArrowDecodeError(f"negative IPC metadata length {mlen}")
    if len(buf) - pos < hdr + mlen:
        return None
    meta = _T(buf, pos + hdr + struct.unpack_from("<I", buf, pos + hdr)[0])
    body_len = meta.scalar(3, "q", 0)
    total = hdr + mlen + body_len
    if len(buf) - pos < total:
        return None
    return (pos + hdr, mlen, total)


def _read_message(buf: bytes, pos: int) -> Tuple[Optional[_Msg], int]:
    loc = split_message(buf, pos)
    if loc is None:
        raise ArrowDecodeError("truncated Arrow IPC stream")
    meta_start, mlen, total = loc
    if mlen == 0:
        return None, pos + total
    root = _T(buf, meta_start + struct.unpack_from("<I", buf, meta_start)[0])
    kind = root.scalar(1, "B", 0)
    header = root.table(2)
    body = memoryview(buf)[meta_start + mlen: pos + total]
    return _Msg(kind, header, body), pos + total


def _parse_schema(t: _T) -> List[Field]:
    if t.scalar(0, "h", 0) != 0:
        raise ArrowDecodeError("big-endian Arrow schema is not supported")
    return [_parse_field(f) for f in t.tables(1)]


# ═════════════════════════════════════════════════════════════════════════════
# Column decode (built-in)
# ═════════════════════════════════════════════════════════════════════════════

_EPOCH_DATE = _dt.date(1970, 1, 1)
_EPOCH = _dt.datetime(1970, 1, 1)
_UTC = _dt.timezone.utc
_EPOCH_UTC = _dt.datetime(1970, 1, 1, tzinfo=_UTC)
_TO_US = {0: 1_000_000, 1: 1_000, 2: 1, 3: None}  # time unit -> µs multiplier (ns: divide)
_INT_CODES = {(8, True): "b", (8, False): "B", (16, True): "h", (16, False): "H",
              (32, True): "i", (32, False): "I", (64, True): "q", (64, False): "Q"}


class _Batch:
    """Cursor over one record batch's nodes and buffers (depth-first order)."""

    __slots__ = ("body", "nodes", "buffers", "ni", "bi", "dicts")

    def __init__(self, t: _T, body: memoryview, dicts: Dict[int, List[Any]]):
        if t.table(3) is not None:
            raise ArrowDecodeError("compressed Arrow record batches are not supported")
        self.body = body
        s, n = t.vec(1)
        self.nodes = [struct.unpack_from("<qq", t.b, s + 16 * k) for k in range(n)]
        s, n = t.vec(2)
        self.buffers = [struct.unpack_from("<qq", t.b, s + 16 * k) for k in range(n)]
        self.ni = 0
        self.bi = 0
        self.dicts = dicts

    def node(self) -> Tuple[int, int]:
        n = self.nodes[self.ni]
        self.ni += 1
        return n

    def buf(self) -> memoryview:
        off, ln = self.buffers[self.bi]
        self.bi += 1
        return self.body[off: off + ln]


def _validity(bm: memoryview, n: int, nulls: int) -> Optional[List[bool]]:
    if nulls == 0 or len(bm) == 0:
        return None
    b = bytes(bm)
    return [bool(b[i >> 3] & (1 << (i & 7))) for i in range(n)]


def _fixed(buf: memoryview, code: str, n: int) -> List[Any]:
    a = _array.array(code)
    a.frombytes(bytes(buf[: n * a.itemsize]))
    if not _LITTLE:  # pragma: no cover
        a.byteswap()
    return a.tolist()


def _apply_validity(vals: List[Any], valid: Optional[List[bool]]) -> List[Any]:
    if valid is None:
        return vals
    return [v if ok else None for v, ok in zip(vals, valid)]


def _offsets(buf: memoryview, n: int, large: bool) -> List[int]:
    return _fixed(buf, "q" if large else "i", n + 1)


def _strings(data: memoryview, offs: List[int], n: int, text: bool) -> List[Any]:
    raw = bytes(data)
    if text:
        if raw.isascii():
            s = raw.decode("ascii")
            return [s[offs[i]: offs[i + 1]] for i in range(n)]
        return [raw[offs[i]: offs[i + 1]].decode("utf-8") for i in range(n)]
    return [raw[offs[i]: offs[i + 1]] for i in range(n)]


def _decimals(buf: memoryview, n: int, width: int, scale: int) -> List[Any]:
    raw = bytes(buf)
    q = _decimal.Decimal(1).scaleb(-scale)
    out = []
    ctx = _decimal.Context(prec=80)
    for i in range(n):
        u = int.from_bytes(raw[i * width: (i + 1) * width], "little", signed=True)
        out.append(_decimal.Decimal(u).scaleb(-scale, ctx).quantize(q, context=ctx))
    return out


def _date_or_text(days: int) -> Any:
    try:
        return _EPOCH_DATE + _dt.timedelta(days=days)
    except OverflowError:
        return _out_of_range_date(days)


def _out_of_range_date(days: int) -> str:
    # Outside Python's year 1..9999: the PostgreSQL text form instead of a
    # wrong or clamped value.
    # Proleptic Gregorian from days since 1970-01-01 (civil_from_days).
    z = days + 719468
    era = z // 146097  # floor division: the C form's negative-era correction is built in
    doe = z - era * 146097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    y = yoe + era * 400
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    d = doy - (153 * mp + 2) // 5 + 1
    m = mp + 3 if mp < 10 else mp - 9
    y += 1 if m <= 2 else 0
    return f"{y:04d}-{m:02d}-{d:02d}" if y > 0 else f"{1 - y:04d}-{m:02d}-{d:02d} BC"


def _ts(us: int, tz: bool) -> Any:
    try:
        return (_EPOCH_UTC if tz else _EPOCH) + _dt.timedelta(microseconds=us)
    except OverflowError:
        days, rem = divmod(us, 86_400_000_000)
        secs, frac = divmod(rem, 1_000_000)
        h, r = divmod(secs, 3600)
        mi, s = divmod(r, 60)
        txt = f"{_out_of_range_date(days)} {h:02d}:{mi:02d}:{s:02d}" + (f".{frac:06d}".rstrip("0") if frac else "")
        return txt + ("+00" if tz else "")


def _decode_field(f: Field, b: _Batch) -> List[Any]:
    n, nulls = b.node()
    k = f.kind
    if f.dict_id is not None:
        valid = _validity(b.buf(), n, nulls)
        bits, signed = f.dict_index or (32, True)
        idx = _fixed(b.buf(), _INT_CODES[(bits, signed)], n)
        values = b.dicts.get(f.dict_id)
        if values is None:
            raise ArrowDecodeError(f"field {f.name!r}: dictionary {f.dict_id} never sent")
        return _apply_validity([values[i] for i in idx], valid)
    if k == _NULL:
        return [None] * n
    valid = _validity(b.buf(), n, nulls)
    p = f.params
    if k == _INT:
        vals = _fixed(b.buf(), _INT_CODES[(p["bits"], p["signed"])], n)
    elif k == _FLOAT:
        if p["bits"] == 16:
            raw = bytes(b.buf())
            vals = [struct.unpack_from("<e", raw, 2 * i)[0] for i in range(n)]
        else:
            vals = _fixed(b.buf(), "f" if p["bits"] == 32 else "d", n)
    elif k == _BOOL:
        raw = bytes(b.buf())
        vals = [bool(raw[i >> 3] & (1 << (i & 7))) for i in range(n)]
    elif k in (_UTF8, _BINARY, _LARGE_UTF8, _LARGE_BINARY):
        offs = _offsets(b.buf(), n, k in (_LARGE_UTF8, _LARGE_BINARY))
        vals = _strings(b.buf(), offs, n, k in (_UTF8, _LARGE_UTF8))
    elif k == _FSB:
        raw = bytes(b.buf())
        w = p["width"]
        vals = [raw[i * w: (i + 1) * w] for i in range(n)]
    elif k == _DECIMAL:
        vals = _decimals(b.buf(), n, p["bits"] // 8, p["scale"])
    elif k == _DATE:
        if p["unit"] == 0:
            vals = [_date_or_text(d) for d in _fixed(b.buf(), "i", n)]
        else:
            vals = [_date_or_text(ms // 86_400_000) for ms in _fixed(b.buf(), "q", n)]
    elif k in (_TIMESTAMP, _TIME, _DURATION):
        bits = p.get("bits", 64)
        raw = _fixed(b.buf(), "i" if bits == 32 else "q", n)
        mul = _TO_US[p["unit"]]
        us = [v * mul for v in raw] if mul else [v // 1000 for v in raw]
        if k == _TIMESTAMP:
            tz = bool(p.get("tz"))
            vals = [_ts(u, tz) for u in us]
        elif k == _TIME:
            vals = [(_dt.datetime.min + _dt.timedelta(microseconds=u)).time() for u in us]
        else:
            vals = [_dt.timedelta(microseconds=u) for u in us]
    elif k == _INTERVAL:
        raw = bytes(b.buf())
        unit = p["unit"]
        if unit == 0:  # YEAR_MONTH: i32 months
            vals = [{"months": struct.unpack_from("<i", raw, 4 * i)[0]} for i in range(n)]
        elif unit == 1:  # DAY_TIME: i32 days, i32 ms
            vals = [_dt.timedelta(days=d, milliseconds=ms)
                    for d, ms in (struct.unpack_from("<ii", raw, 8 * i) for i in range(n))]
        else:  # MONTH_DAY_NANO
            vals = [{"months": mo, "days": d, "nanoseconds": ns}
                    for mo, d, ns in (struct.unpack_from("<iiq", raw, 16 * i) for i in range(n))]
    elif k in (_LIST, _LARGE_LIST, _MAP):
        offs = _offsets(b.buf(), n, k == _LARGE_LIST)
        child = _decode_field(f.children[0], b)
        if k == _MAP:  # entries are {key, value} structs
            vals = [dict(tuple(e.values())[:2] for e in child[offs[i]: offs[i + 1]]) for i in range(n)]
        else:
            vals = [child[offs[i]: offs[i + 1]] for i in range(n)]
    elif k == _FSL:
        size = p["size"]
        child = _decode_field(f.children[0], b)
        vals = [child[i * size: (i + 1) * size] for i in range(n)]
    elif k == _STRUCT:
        cols = [_decode_field(c, b) for c in f.children]
        names = [c.name for c in f.children]
        vals = [dict(zip(names, row)) for row in zip(*cols)] if cols else [{} for _ in range(n)]
    else:  # pragma: no cover - rejected in _parse_field
        raise ArrowDecodeError(f"field {f.name!r}: unsupported Arrow type {f.type_name}")
    return _apply_validity(vals, valid)


# ═════════════════════════════════════════════════════════════════════════════
# ParticleDB conversion (shared by both decode paths)
# ═════════════════════════════════════════════════════════════════════════════

_UNCONSTRAINED_NUMERIC = {"numeric", "decimal", "dec", "bignumeric", "bigdecimal"}
_F32 = struct.Struct("<f")


def _decl(f: Field) -> str:
    return (f.decl or "").strip().lower()


def is_vector_decl(d: str) -> bool:
    return d == "vector" or d.startswith("vector(") or d.startswith("vector ")


def _to_decimal(v: Any) -> Any:
    if v is None or isinstance(v, _decimal.Decimal):
        return v
    try:
        return _decimal.Decimal(str(v).strip())
    except _decimal.InvalidOperation:
        return v


def _vector(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray, memoryview)):
        raw = bytes(v)
        if len(raw) % 4:
            raise ArrowDecodeError(f"vector value of {len(raw)} bytes is not float32 components")
        a = _array.array("f")
        a.frombytes(raw)
        if not _LITTLE:  # pragma: no cover
            a.byteswap()
        return a.tolist()
    if isinstance(v, list):
        return [None if e is None else float(e) for e in v]
    if isinstance(v, str):
        s = v.strip()
        if s.startswith("[") and s.endswith("]"):
            return [float(x) for x in s[1:-1].split(",") if x.strip()]
    return v


def _uuid_value(v: Any) -> Any:
    if v is None or isinstance(v, _uuid.UUID):
        return v
    if isinstance(v, (bytes, bytearray)) and len(v) == 16:
        return _uuid.UUID(bytes=bytes(v))
    if isinstance(v, str):
        try:
            return _uuid.UUID(v)
        except ValueError:
            return v
    return v


def _elem_converter(elem_decl: str) -> Callable[[str], Any]:
    e = elem_decl.strip().lower()
    while e.endswith("[]"):  # INT[][]: the element type of every dimension
        e = e[:-2].rstrip()
    if e in ("int2", "int4", "int8", "smallint", "integer", "int", "bigint"):
        return int
    if e in ("float4", "float8", "real", "double precision", "float"):
        return float
    if e in _UNCONSTRAINED_NUMERIC or e.startswith("numeric") or e.startswith("decimal"):
        return _decimal.Decimal
    if e in ("bool", "boolean"):
        return lambda s: s.lower() in ("t", "true")
    if e == "uuid":
        return _uuid.UUID
    if e == "date":
        return lambda s: _dt.date.fromisoformat(s)
    return lambda s: s


def parse_pg_array(text: str, conv: Callable[[str], Any] = lambda s: s) -> Any:
    """PostgreSQL array external text (``{1,NULL,"c,d"}``, nested) -> list."""
    s = text.strip()
    if s.startswith("[") and "=" in s[: s.find("{") if "{" in s else 0]:
        s = s[s.find("{"):]  # explicit bounds prefix: '[0:1]={...}'
    pos = 0

    def parse() -> List[Any]:
        nonlocal pos
        if s[pos] != "{":
            raise ValueError(f"malformed array literal: {text!r}")
        pos += 1
        out: List[Any] = []
        if s[pos] == "}":
            pos += 1
            return out
        while True:
            c = s[pos]
            if c == "{":
                out.append(parse())
            elif c == '"':
                pos += 1
                buf = []
                while s[pos] != '"':
                    if s[pos] == "\\":
                        pos += 1
                    buf.append(s[pos])
                    pos += 1
                pos += 1
                out.append(conv("".join(buf)))
            else:
                j = pos
                while s[j] not in ",}":
                    j += 1
                tok = s[pos:j].strip()
                pos = j
                out.append(None if tok.upper() == "NULL" else conv(tok))
            if s[pos] == ",":
                pos += 1
                continue
            if s[pos] == "}":
                pos += 1
                return out
            raise ValueError(f"malformed array literal: {text!r}")

    return parse()


def parse_array_constructor(text: str, conv: Callable[[str], Any] = lambda s: s) -> List[Any]:
    """The engine's stored-array text, SQL constructor syntax:
    ``ARRAY['a',NULL,'it''s']``, ``ARRAY[1,NULL,3]``, ``ARRAY[ARRAY[1,2],ARRAY[3,4]]``,
    ``ARRAY[2024-01-01]`` (unquoted non-numeric scalars) -> list."""
    s = text.strip()
    pos = 0

    def parse() -> List[Any]:
        nonlocal pos
        if s[pos:pos + 6].upper() == "ARRAY[":
            pos += 6
        elif s[pos] == "[":
            pos += 1
        else:
            raise ValueError(f"malformed array constructor: {text!r}")
        out: List[Any] = []
        while s[pos] == " ":
            pos += 1
        if s[pos] == "]":
            pos += 1
            return out
        while True:
            while s[pos] == " ":
                pos += 1
            if s[pos:pos + 6].upper() == "ARRAY[" or s[pos] == "[":
                out.append(parse())
            elif s[pos] == "'":
                pos += 1
                buf = []
                while True:
                    if s[pos] == "'":
                        if s[pos + 1: pos + 2] == "'":
                            buf.append("'")
                            pos += 2
                            continue
                        pos += 1
                        break
                    buf.append(s[pos])
                    pos += 1
                out.append(conv("".join(buf)))
            else:
                j = pos
                while s[j] not in ",]":
                    j += 1
                tok = s[pos:j].strip()
                pos = j
                out.append(None if tok.upper() == "NULL" else conv(tok))
            while s[pos] == " ":
                pos += 1
            if s[pos] == ",":
                pos += 1
                continue
            if s[pos] == "]":
                pos += 1
                return out
            raise ValueError(f"malformed array constructor: {text!r}")

    out = parse()
    if s[pos:].strip():
        raise ValueError(f"trailing text after array constructor: {text!r}")
    return out


def _array_text(v: Any, conv: Callable[[str], Any]) -> Any:
    """An array column's text -> list. The engine sends two spellings: PostgreSQL
    external text (``{1,NULL,"c,d"}``, expressions) and SQL constructor text
    (``ARRAY[1,NULL,3]``, stored columns). Anything else is an error, never a
    silent string."""
    if v is None or not isinstance(v, str):
        return v
    t = v.strip()
    try:
        if t.upper().startswith("ARRAY["):
            return parse_array_constructor(t, conv)
        return parse_pg_array(t, conv)
    except (ValueError, IndexError, _decimal.InvalidOperation) as e:
        raise ArrowDecodeError(f"cannot read array value {v[:80]!r}: {e}") from None


def _time_value(value: Any) -> Any:
    if value is None or isinstance(value, _dt.time):
        return value
    if value.startswith("24:") and not value[3:].replace(":", "").replace(".", "").strip("0"):
        value = "00:" + value[3:]
    try:
        return _dt.time.fromisoformat(value)
    except (TypeError, ValueError) as error:
        raise ArrowDecodeError(f"cannot read TIME value {value!r}: {error}") from None


def _utc(v: Any) -> Any:
    if isinstance(v, _dt.datetime):
        return v.replace(tzinfo=_UTC) if v.tzinfo is None else v.astimezone(_UTC)
    if isinstance(v, str) and not v.endswith("+00"):
        return v + "+00"
    return v


_JSON_DECLS = ("json", "jsonb")


def _json_value(v: Any) -> Any:
    if v is None or not isinstance(v, str):
        return v
    return _json.loads(v)


_INTERVAL_UNITS = {
    "year": 365, "years": 365, "yr": 365, "yrs": 365, "y": 365,
    "mon": 30, "mons": 30, "month": 30, "months": 30,
    "day": 1, "days": 1, "d": 1,
    "week": 7, "weeks": 7, "w": 7,
}


def parse_interval(text: Any) -> Any:
    """PostgreSQL ``interval`` output text (``postgres`` IntervalStyle, the
    server's only one) -> ``datetime.timedelta``: ``1 day``, ``01:00:00``,
    ``1 year 2 mons -3 days +04:05:06.5``. A year counts 365 days and a month
    30, exactly as psycopg converts them. Text this does not recognise is
    returned unchanged (never a guessed value)."""
    if not isinstance(text, str):
        return text
    s = text.strip()
    if not s:
        return text
    days = 0
    micros = 0
    toks = s.split()
    i = 0
    try:
        while i < len(toks):
            t = toks[i]
            if ":" in t:
                neg = t.startswith("-")
                hms = t.lstrip("+-").split(":")
                if len(hms) == 2:
                    hms.append("0")
                if len(hms) != 3:
                    return text
                h, m = int(hms[0]), int(hms[1])
                sec, _, frac = hms[2].partition(".")
                us = ((h * 60 + m) * 60 + int(sec)) * 1_000_000 + (int((frac + "000000")[:6]) if frac else 0)
                micros += -us if neg else us
                i += 1
                continue
            if i + 1 >= len(toks):
                return text
            unit = _INTERVAL_UNITS.get(toks[i + 1].lower())
            if unit is None:
                return text
            days += int(t) * unit
            i += 2
        return _dt.timedelta(days=days, microseconds=micros)
    except (ValueError, OverflowError):
        return text


def converter_for(f: Field, extended: bool = False) -> Optional[Callable[[Any], Any]]:
    """The ParticleDB-level conversion of one column, or ``None`` for none.

    ``extended`` (the DB-API layer's profile) also turns ``json`` / ``jsonb``
    text into Python objects and ``interval`` text into ``timedelta``; the
    native API keeps both as text (its documented contract)."""
    d = _decl(f)
    k = f.kind
    if extended and k in (_UTF8, _LARGE_UTF8):
        if d in _JSON_DECLS:
            return _json_value
        if d == "interval" or d.startswith("interval "):
            return parse_interval
    if k == _TIMESTAMP and not f.params.get("tz") and (
            d in ("timestamptz", "timestamp with time zone") or "pdb:tz" in f.metadata):
        # The engine sends a timestamptz as the UTC instant in a zone-less
        # Arrow timestamp, the session zone in `pdb:tz`: an aware UTC value.
        return _utc
    if is_vector_decl(d):
        return _vector
    if k in (_UTF8, _LARGE_UTF8):
        if d in ("time", "time without time zone") or d.startswith("time("):
            return _time_value
        if d in _UNCONSTRAINED_NUMERIC:
            return _to_decimal
        if d == "uuid":
            return _uuid_value
        if d.endswith("[]") or d.startswith("_"):
            conv = _elem_converter(d[:-2] if d.endswith("[]") else d[1:])
            return lambda v: _array_text(v, conv)
    if k == _FSB and d == "uuid":
        return _uuid_value
    if k in (_LIST, _LARGE_LIST, _FSL) and f.children and _decl(f.children[0]) == "uuid":
        return lambda v: None if v is None else [_uuid_value(e) for e in v]
    return None


_UNSET = object()


def _converter(f: Field, extended: bool) -> Optional[Callable[[Any], Any]]:
    """:func:`converter_for`, memoised on the field (fields are cached per
    schema, so a statement's converters are computed once)."""
    cv = f._cv
    if cv is None:
        cv = f._cv = [_UNSET, _UNSET]
    c = cv[extended]
    if c is _UNSET:
        c = cv[extended] = converter_for(f, extended)
    return c  # type: ignore[return-value]


def _convert_column(f: Field, vals: List[Any], extended: bool = False) -> List[Any]:
    c = _converter(f, extended)
    if c is None:
        return vals
    return [c(v) for v in vals]


# ═════════════════════════════════════════════════════════════════════════════
# pyarrow path
# ═════════════════════════════════════════════════════════════════════════════

def _pa_column(col: Any, f: Field) -> List[Any]:
    vals = col.to_pylist()
    k = f.kind
    if k == _TIMESTAMP and f.params.get("tz"):
        vals = [v if v is None or isinstance(v, str) else v.astimezone(_UTC) for v in vals]
    elif k == _MAP:
        vals = [None if v is None else dict(v) for v in vals]
    return vals


# ═════════════════════════════════════════════════════════════════════════════
# Public: decode a stream / an incremental reader
# ═════════════════════════════════════════════════════════════════════════════

class Schema:
    """The result's columns: names (duplicates kept, in order) and fields."""

    __slots__ = ("fields", "names", "_schema_msg")

    def __init__(self, fields: List[Field], schema_msg: bytes = b""):
        self.fields = fields
        self.names = [f.name for f in fields]
        self._schema_msg = schema_msg


_EOS = b"\xff\xff\xff\xff\x00\x00\x00\x00"
_SCHEMA_CACHE: Dict[bytes, Schema] = {}
_SCHEMA_CACHE_MAX = 512
#: A record batch whose body is at least this big is decoded with pyarrow
#: (when installed and enabled); a smaller one is cheaper to decode here than
#: to hand to pyarrow (measured: a one-row point result decodes ~1.5x faster
#: built in).
PYARROW_MIN_BODY = 16 * 1024


class StreamDecoder:
    """Incremental decoder: feed IPC bytes in any slicing, take decoded batches.

    Holds at most one partial message plus the schema and dictionaries, so a
    caller that consumes each batch's rows before feeding more bytes decodes a
    result of any size in bounded memory.
    """

    def __init__(self, use_pyarrow: Optional[bool] = None, extended: bool = False):
        self.extended = extended
        self._buf = bytearray()
        self._pos = 0
        self.schema: Optional[Schema] = None
        self._dicts: Dict[int, List[Any]] = {}
        self._dict_msgs: Dict[int, bytes] = {}
        self.ended = False
        self._pa = USE_PYARROW if use_pyarrow is None else (use_pyarrow and HAVE_PYARROW)

    def feed(self, data: bytes) -> None:
        if self._pos and self._pos >= len(self._buf) // 2:
            del self._buf[: self._pos]
            self._pos = 0
        self._buf += data

    def batches(self) -> Iterator[List[List[Any]]]:
        """Yield each complete record batch now buffered, as columns (lists)."""
        while not self.ended:
            loc = split_message(self._buf, self._pos)
            if loc is None:
                return
            meta_start, mlen, total = loc
            start = self._pos
            if mlen == 0:
                self._pos += total
                self.ended = True
                return
            buf = bytes(self._buf[start: start + total])
            self._pos += total
            meta_start -= start
            root = _T(buf, meta_start + struct.unpack_from("<I", buf, meta_start)[0])
            kind = root.scalar(1, "B", 0)
            if kind == _H_SCHEMA:
                # Every answer of a statement repeats the same schema bytes:
                # parse each distinct schema once.
                sch = _SCHEMA_CACHE.get(buf)
                if sch is None:
                    sch = Schema(_parse_schema(root.table(2)), buf)
                    if len(_SCHEMA_CACHE) >= _SCHEMA_CACHE_MAX:
                        _SCHEMA_CACHE.clear()
                    _SCHEMA_CACHE[buf] = sch
                self.schema = sch
                continue
            msg = _Msg(kind, root.table(2), memoryview(buf)[meta_start + mlen: total])
            if msg.kind == _H_DICT:
                self._dictionary(msg, buf)
            elif msg.kind == _H_BATCH:
                if self.schema is None:
                    raise ArrowDecodeError("record batch before the schema")
                yield self._batch(msg, buf)
            # other message kinds (tensors) are not part of a result: skipped

    def _field_for_dict(self, did: int) -> Field:
        def walk(fs: Sequence[Field]) -> Optional[Field]:
            for f in fs:
                if f.dict_id == did:
                    return f
                r = walk(f.children)
                if r is not None:
                    return r
            return None

        f = walk(self.schema.fields if self.schema else [])
        if f is None:
            raise ArrowDecodeError(f"dictionary batch {did} for no field")
        return f

    def _dictionary(self, msg: _Msg, raw: bytes) -> None:
        did = msg.table.scalar(0, "q", 0)
        data = msg.table.table(1)
        f = self._field_for_dict(did)
        vf = Field()
        for slot in Field.__slots__:
            setattr(vf, slot, getattr(f, slot))
        vf.dict_id = None
        values = _decode_field(vf, _Batch(data, msg.body, self._dicts))
        if msg.table.scalar(2, "B", 0):
            self._dicts[did] = self._dicts.get(did, []) + values
        else:
            self._dicts[did] = values
        self._dict_msgs[did] = raw

    def _batch(self, msg: _Msg, raw: bytes) -> List[List[Any]]:
        fields = self.schema.fields
        if self._pa and len(msg.body) >= PYARROW_MIN_BODY:
            try:
                stream = self.schema._schema_msg + b"".join(self._dict_msgs.values()) + raw + _EOS
                rb = pyarrow_modules()[1].open_stream(stream).read_next_batch()
                return [_convert_column(f, _pa_column(rb.column(i), f), self.extended) for i, f in enumerate(fields)]
            except Exception:
                pass  # values pyarrow cannot convert (dates past year 9999 ...): decode here
        b = _Batch(msg.table, msg.body, self._dicts)
        return [_convert_column(f, _decode_field(f, b), self.extended) for f in fields]


def decode_stream(data: bytes, use_pyarrow: Optional[bool] = None,
                  extended: bool = False) -> Tuple[Schema, List[Tuple[Any, ...]]]:
    """Decode a whole Arrow IPC stream: ``(schema, rows as tuples)``."""
    pa = USE_PYARROW if use_pyarrow is None else (use_pyarrow and HAVE_PYARROW)
    if not pa and isinstance(data, bytes):
        fast = _decode_whole(data, extended)
        if fast is not None:
            return fast
    d = StreamDecoder(use_pyarrow, extended)
    d.feed(data)
    rows: List[Tuple[Any, ...]] = []
    for cols in d.batches():
        rows.extend(zip(*cols))
    if d.schema is None:
        if not data:
            return Schema([]), []
        raise ArrowDecodeError("Arrow IPC stream without a schema")
    if not d.ended and d._pos != len(d._buf):
        raise ArrowDecodeError("truncated Arrow IPC stream")
    return d.schema, rows


def read_schema(data: bytes) -> Schema:
    """The schema of an Arrow IPC stream, parsing only its first message
    (cached by its bytes like :class:`StreamDecoder`'s). An empty stream has
    no columns."""
    loc = split_message(data, 0) if data else None
    if loc is None or loc[1] == 0:
        return Schema([])
    meta_start, mlen, total = loc
    buf = bytes(data[:total])
    sch = _SCHEMA_CACHE.get(buf)
    if sch is not None:
        return sch
    root = _T(buf, meta_start + struct.unpack_from("<I", buf, meta_start)[0])
    if root.scalar(1, "B", 0) != _H_SCHEMA:
        raise ArrowDecodeError("Arrow IPC stream does not start with a schema")
    sch = Schema(_parse_schema(root.table(2)), buf)
    if len(_SCHEMA_CACHE) >= _SCHEMA_CACHE_MAX:
        _SCHEMA_CACHE.clear()
    _SCHEMA_CACHE[buf] = sch
    return sch


def count_rows(data: bytes) -> int:
    """Rows in an Arrow IPC stream, from the record-batch headers alone (no
    column is decoded)."""
    pos = 0
    n = 0
    while pos < len(data):
        loc = split_message(data, pos)
        if loc is None:
            raise ArrowDecodeError("truncated Arrow IPC stream")
        meta_start, mlen, total = loc
        if mlen == 0:
            break
        root = _T(data, meta_start + struct.unpack_from("<I", data, meta_start)[0])
        if root.scalar(1, "B", 0) == _H_BATCH:
            n += root.table(2).scalar(0, "q", 0)
        pos += total
    return n


def _decode_whole(data: bytes, extended: bool) -> Optional[Tuple[Schema, List[Tuple[Any, ...]]]]:
    """:func:`decode_stream` of a complete stream with the built-in decoder,
    reading the bytes in place (no copy into a buffer, no per-message copy).
    ``None`` for a stream it leaves to :class:`StreamDecoder` (dictionaries)."""
    if not data:
        return Schema([]), []
    pos = 0
    n = len(data)
    schema: Optional[Schema] = None
    rows: List[Tuple[Any, ...]] = []
    ended = False
    unpack = struct.unpack_from
    while pos < n:
        if n - pos < 8:
            raise ArrowDecodeError("truncated Arrow IPC stream")
        word, mlen = unpack("<Ii", data, pos)
        if word == 0xFFFFFFFF:
            hdr = 8
        else:  # pre-0.15 framing: no continuation marker
            mlen, hdr = word, 4
        if mlen == 0:
            pos += hdr
            ended = True
            break
        meta_start = pos + hdr
        if mlen < 0 or meta_start + mlen > n:
            raise ArrowDecodeError("truncated Arrow IPC stream")
        root = _T(data, meta_start + unpack("<I", data, meta_start)[0])
        total = hdr + mlen + root.scalar(3, "q", 0)
        if pos + total > n:
            raise ArrowDecodeError("truncated Arrow IPC stream")
        kind = root.scalar(1, "B", 0)
        if kind == _H_SCHEMA:
            key = data[pos: pos + total]
            schema = _SCHEMA_CACHE.get(key)
            if schema is None:
                schema = Schema(_parse_schema(root.table(2)), key)
                if len(_SCHEMA_CACHE) >= _SCHEMA_CACHE_MAX:
                    _SCHEMA_CACHE.clear()
                _SCHEMA_CACHE[key] = schema
        elif kind == _H_BATCH:
            if schema is None:
                raise ArrowDecodeError("record batch before the schema")
            b = _Batch(root.table(2), memoryview(data)[meta_start + mlen: pos + total], {})
            cols = [_convert_column(f, _decode_field(f, b), extended) for f in schema.fields]
            if len(cols) == 1:
                rows.extend([(v,) for v in cols[0]])
            else:
                rows.extend(zip(*cols))
        elif kind == _H_DICT:
            return None
        pos += total
    if schema is None:
        raise ArrowDecodeError("Arrow IPC stream without a schema")
    if not ended and pos != n:
        raise ArrowDecodeError("truncated Arrow IPC stream")
    return schema, rows
