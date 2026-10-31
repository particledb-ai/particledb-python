"""PDB native wire, protocol 2 (multiplexed): constants, frame codec, payloads.

Pure functions, no sockets: :mod:`particledb.connection` drives them. The
server side of every layout here is ``pdb_wire_v2.rs`` (frame
types, capability bits, HELLO / HELLO_OK), ``pdb_wire_params.rs``
(typed ``F_EXECUTE`` parameters) and ``pdb_wire_mux.rs`` (KV,
batch, bulk). The JDBC driver (``WireV2Codec.java``) is
the reference client of the same bytes.

Framing:

* HELLO and HELLO_OK are v1-framed: ``[u32 len][u16 stream][u8 type][u8 flags]``.
* Every later frame is v2: ``[u8 2][u8 type][u8 flags][u32 stream][u32 len]``.
"""

from __future__ import annotations

import datetime as _dt
import decimal as _decimal
import json as _json
import struct
import uuid as _uuid
from typing import Any, List, Optional, Sequence, Tuple

# ── framing ────────────────────────────────────────────────────────────────
V1_HEADER = 8
V2_HEADER = 11
PROTO_V2 = 2
MAX_PAYLOAD = 16 * 1024 * 1024

_V1_HDR = struct.Struct("<IHBB")
_V2_HDR = struct.Struct("<BBBII")

# ── client -> server ───────────────────────────────────────────────────────
F_HELLO = 0x00
F_QUERY = 0x01
F_BATCH = 0x02
F_PING = 0x03
F_CLOSE = 0x04
F_RST_STREAM = 0x06
F_AUTH = 0x09
F_KV_SET = 0x10
F_KV_GET = 0x11
F_KV_DEL = 0x12
F_KV_INCR = 0x13
F_KV_CMD = 0x14  # any RESP command, typed (the server's native KV protocol)
F_PREPARE = 0x20
F_EXECUTE = 0x21
F_CLOSE_STMT = 0x22
F_BULK = 0x40
BULK_BEGIN = 0x01
BULK_DATA = 0x02
BULK_END = 0x03
BULK_ABORT = 0x04

# ── server -> client ───────────────────────────────────────────────────────
F_HELLO_OK = 0x80
F_ARROW_RESULT = 0x81
F_OK = 0x82
F_ERROR = 0x83
F_PONG = 0x84
F_BATCH_RESULT = 0x85
F_ARROW_STREAM = 0x86
F_KV_BULK = 0x87
F_KV_NIL = 0x88
F_RST_STREAM_S = 0x89
F_GOAWAY = 0x8A
F_SETTINGS_ACK = 0x8B
F_NOTICE = 0x8C
F_PREPARE_OK = 0x8D
F_QUEUE_RESULT = 0x8E
F_KV_REPLY = 0x8F  # answer to F_KV_CMD; chunked with FLAG_MORE_CHUNKS

# ── frame flags ────────────────────────────────────────────────────────────
FLAG_END_STREAM = 0x01
FLAG_MORE_CHUNKS = 0x02
FLAG_COMPRESSED = 0x04
FLAG_TXN_BEGIN = 0x08
FLAG_TXN_OPEN = 0x10
FLAG_TXN_FAILED = 0x20
FLAG_TXN_ROLLED_BACK = 0x40

# ── capability bits (HELLO caps byte) ──────────────────────────────────────
CAP_V1_LEGACY = 0x01
CAP_V2_MUX = 0x02
CAP_COMPRESSION = 0x04
CAP_TLS = 0x08
CAP_NUMERIC_TEXT = 0x10
CAP_AUTH_REQUIRED = 0x20  # server -> client only: posture, not a capability
CAP_STATEMENT_NOTICE = 0x40
CAP_PREPARED = 0x80

# ── extension block, byte 0 ────────────────────────────────────────────────
EXT_TYPED_PARAMS = 0x01
EXT_BULK_INGEST = 0x02
EXT_BATCH_SQLSTATE = 0x04
EXT_SESSION_PARAMS = 0x08
EXT_ARRAY_PARAMS = 0x10
EXT_KV_CMD = 0x20

FMT_ARROW_IPC = 0x01

#: What a default connect asks for -- every capability this client implements,
#: the same set the JDBC reference asks for (``StreamMux.connect``). The
#: conformance suite's DEF-002 holds the server's record of it to the list.
DEFAULT_CAPS = CAP_V1_LEGACY | CAP_V2_MUX | CAP_PREPARED | CAP_NUMERIC_TEXT | CAP_STATEMENT_NOTICE
DEFAULT_EXT = EXT_TYPED_PARAMS | EXT_BULK_INGEST | EXT_ARRAY_PARAMS | EXT_BATCH_SQLSTATE

# Names exactly as the server's ``pdb_wire_v2::capability_names`` spells them
# (``SHOW pdb.wire_capabilities``).
_CAP_NAMES = (
    (CAP_V2_MUX, "v2_mux"),
    (CAP_COMPRESSION, "compression"),
    (CAP_TLS, "tls"),
    (CAP_NUMERIC_TEXT, "numeric_text"),
    (CAP_STATEMENT_NOTICE, "statement_notice"),
    (CAP_PREPARED, "prepared"),
)
_EXT_NAMES = (
    (EXT_TYPED_PARAMS, "typed_params"),
    (EXT_BULK_INGEST, "bulk_ingest"),
    (EXT_BATCH_SQLSTATE, "batch_sqlstate"),
    (EXT_SESSION_PARAMS, "session_params"),
    (EXT_ARRAY_PARAMS, "array_params"),
    (EXT_KV_CMD, "kv_cmd"),
)

# ── stream error codes (RST_STREAM / GOAWAY) ───────────────────────────────
ERR_NO_ERROR = 0x0
ERR_PROTOCOL = 0x1
ERR_INTERNAL = 0x2
ERR_STREAM_LIMIT = 0x4
ERR_REFUSED_STREAM = 0x6
ERR_CANCEL = 0x7

# ── typed parameter tags (pdb_wire_params.rs) ──────────────────────────────
P_NULL, P_INT8, P_TEXT, P_BOOL = 0, 1, 2, 3
P_FLOAT8, P_FLOAT4, P_NUMERIC, P_DATE = 4, 5, 6, 7
P_TIMESTAMP, P_TIMESTAMPTZ, P_TIME, P_UUID = 8, 9, 10, 11
P_BYTEA, P_INT2, P_INT4, P_JSON, P_JSONB, P_ARRAY = 12, 13, 14, 15, 16, 17

MAX_ARRAY_DIMS = 6


# ═════════════════════════════════════════════════════════════════════════════
# Frames
# ═════════════════════════════════════════════════════════════════════════════

def v1_frame(msg_type: int, payload: bytes, stream_id: int = 0, flags: int = 0) -> bytes:
    return _V1_HDR.pack(len(payload), stream_id & 0xFFFF, msg_type, flags) + payload


def v2_frame(msg_type: int, stream_id: int, payload: bytes = b"", flags: int = FLAG_END_STREAM) -> bytes:
    if len(payload) > MAX_PAYLOAD:
        raise ValueError(f"frame payload of {len(payload)} bytes exceeds the server's {MAX_PAYLOAD}-byte cap")
    return _V2_HDR.pack(PROTO_V2, msg_type, flags, stream_id & 0xFFFFFFFF, len(payload)) + payload


def parse_v1_header(b: bytes) -> Tuple[int, int, int, int]:
    """``(payload_len, stream_id, type, flags)``."""
    return _V1_HDR.unpack_from(b, 0)


def parse_v2_header(b: bytes, off: int = 0) -> Tuple[int, int, int, int]:
    """``(type, flags, stream_id, payload_len)``; raises on a bad version byte."""
    ver, t, flags, sid, n = _V2_HDR.unpack_from(b, off)
    if ver != PROTO_V2:
        raise ValueError(f"v2 frame with version byte {ver}")
    return t, flags, sid, n


# ═════════════════════════════════════════════════════════════════════════════
# HELLO / HELLO_OK
# ═════════════════════════════════════════════════════════════════════════════

def encode_hello(caps: int = DEFAULT_CAPS, ext: int = DEFAULT_EXT,
                 session_params: Optional[Sequence[Tuple[str, str]]] = None,
                 max_version: int = PROTO_V2) -> bytes:
    """A v1-framed HELLO: ``[max_ver][1][caps][1][FMT_ARROW_IPC][1][ext]`` then,
    with :data:`EXT_SESSION_PARAMS`, ``[u16 n] n x ([u16 len][key][u16 len][value])``."""
    body = bytearray([max_version, 1, caps & 0xFF, 1, FMT_ARROW_IPC])
    if session_params:
        ext |= EXT_SESSION_PARAMS
    if ext:
        body += bytes([1, ext & 0xFF])
    if session_params:
        body += struct.pack("<H", len(session_params))
        for k, v in session_params:
            for part in (k, v):
                u = part.encode("utf-8")
                body += struct.pack("<H", len(u)) + u
    return v1_frame(F_HELLO, bytes(body))


class HelloOk:
    """The server's HELLO_OK: what this connection actually negotiated."""

    __slots__ = ("version", "caps", "fmt", "max_streams", "window", "ext")

    def __init__(self, version: int, caps: int, fmt: int, max_streams: int, window: int, ext: int):
        self.version = version
        self.caps = caps
        self.fmt = fmt
        self.max_streams = max_streams
        self.window = window
        self.ext = ext

    @classmethod
    def parse(cls, p: bytes) -> "HelloOk":
        if len(p) < 3:
            # A legacy server: [version][format]; no capabilities.
            return cls(p[0] if p else 1, 0, p[1] if len(p) > 1 else FMT_ARROW_IPC, 1, 0, 0)
        max_streams, window = (struct.unpack_from("<II", p, 3) if len(p) >= 11 else (1, 0))
        ext = p[12] if len(p) >= 13 and p[11] >= 1 else 0
        return cls(p[0], p[1], p[2], max_streams, window, ext)

    @property
    def v2(self) -> bool:
        return bool(self.caps & CAP_V2_MUX)

    @property
    def auth_required(self) -> bool:
        return bool(self.caps & CAP_AUTH_REQUIRED)

    def has(self, cap: int = 0, ext: int = 0) -> bool:
        return bool((self.caps & cap) == cap and (self.ext & ext) == ext)

    @property
    def protocol(self) -> str:
        """As the server's ``SHOW pdb.wire_protocol`` names it."""
        return "pdb-native-v2" if self.v2 else "pdb-native-v1"

    @property
    def capability_names(self) -> List[str]:
        """Sorted names of the echoed bits, as ``SHOW pdb.wire_capabilities``
        names them (``CAP_AUTH_REQUIRED`` is posture and excluded)."""
        names = [n for bit, n in _CAP_NAMES if self.caps & bit]
        names += [n for bit, n in _EXT_NAMES if self.ext & bit]
        return sorted(names)


# ═════════════════════════════════════════════════════════════════════════════
# Request payloads
# ═════════════════════════════════════════════════════════════════════════════

def _lp32(b: bytes) -> bytes:
    return struct.pack("<I", len(b)) + b


def name_payload(name: str) -> bytes:
    return _lp32(name.encode("utf-8"))


def prepare_payload(name: str, sql: str) -> bytes:
    return _lp32(name.encode("utf-8")) + _lp32(sql.encode("utf-8"))


def execute_payload(name_b: bytes, encoded_params: bytes, n: int) -> bytes:
    return _lp32(name_b) + struct.pack("<H", n) + encoded_params


def batch_payload(sqls: Sequence[str]) -> bytes:
    parts = [struct.pack("<I", len(sqls))]
    for s in sqls:
        parts.append(_lp32(s.encode("utf-8")))
    return b"".join(parts)


def kv_payload(key: bytes, value: Optional[bytes] = None) -> bytes:
    if value is None:
        return _lp32(key)
    return _lp32(key) + _lp32(value)


# ── F_KV_CMD / F_KV_REPLY (the server's KV command codec is the reference codec) ──
KVF_TXN = 0x01
_ARG_BYTES, _ARG_INT, _ARG_FLOAT = 0x01, 0x02, 0x03
_V_NIL, _V_INT, _V_BYTES, _V_STATUS, _V_ARRAY, _V_NIL_ARRAY, _V_ERROR, _V_MAP = range(8)


class KvStatus(str):
    """A RESP simple-string reply (``OK``, ``PONG``, a TYPE name)."""


class KvReplyError(Exception):
    """An error ELEMENT of a transaction's reply array (the command's RESP
    error text and its SQLSTATE). A top-level error is raised instead."""

    def __init__(self, sqlstate: str, text: str):
        super().__init__(text)
        self.sqlstate = sqlstate
        self.text = text


def _kv_arg(a: Any) -> bytes:
    if isinstance(a, bool):
        raise TypeError("F_KV_CMD: pass booleans as 0/1")
    if isinstance(a, int):
        return struct.pack("<Bq", _ARG_INT, a)
    if isinstance(a, float):
        return struct.pack("<Bd", _ARG_FLOAT, a)
    b = a.encode("utf-8") if isinstance(a, str) else bytes(a)
    return struct.pack("<BI", _ARG_BYTES, len(b)) + b


def _kv_command(name: str, args: Sequence[Any]) -> bytes:
    # By name (command id 0): the server resolves it, so no id table is copied here.
    parts = [struct.pack("<HH", 0, len(args) + 1), _kv_arg(name)]
    parts += [_kv_arg(a) for a in args]
    return b"".join(parts)


def kv_cmd_payload(name: str, args: Sequence[Any]) -> bytes:
    """One ``F_KV_CMD`` command: ``name`` and its arguments (str / bytes /
    int / float)."""
    return struct.pack("<BBH", 1, 0, 1) + _kv_command(name, args)


def kv_txn_payload(cmds: Sequence[Sequence[Any]]) -> bytes:
    """One MULTI/EXEC transaction: ``[[name, *args], ...]``."""
    return struct.pack("<BBH", 1, KVF_TXN, len(cmds)) + b"".join(_kv_command(c[0], c[1:]) for c in cmds)


def decode_kv_reply(p: bytes) -> Any:
    """The concatenated ``F_KV_REPLY`` payloads: None (nil), int, bytes,
    :class:`KvStatus`, list (None for a nil array), dict (MAP), or a
    :class:`KvReplyError` element."""
    if not p or p[0] != 1:
        raise ValueError("F_KV_REPLY: unsupported format")

    def val(off: int) -> Tuple[Any, int]:
        t = p[off]
        off += 1
        if t == _V_NIL:
            return None, off
        if t == _V_INT:
            return struct.unpack_from("<q", p, off)[0], off + 8
        if t in (_V_BYTES, _V_STATUS):
            (n,) = struct.unpack_from("<I", p, off)
            b = bytes(p[off + 4: off + 4 + n])
            return (KvStatus(b.decode("utf-8", "replace")) if t == _V_STATUS else b), off + 4 + n
        if t == _V_ARRAY:
            (n,) = struct.unpack_from("<I", p, off)
            off += 4
            out = []
            for _ in range(n):
                v, off = val(off)
                out.append(v)
            return out, off
        if t == _V_NIL_ARRAY:
            return None, off
        if t == _V_ERROR:
            state = bytes(p[off + 2: off + 7]).decode("ascii", "replace")
            (n,) = struct.unpack_from("<I", p, off + 7)
            text = bytes(p[off + 11: off + 11 + n]).decode("utf-8", "replace")
            return KvReplyError(state, text), off + 11 + n
        if t == _V_MAP:
            (n,) = struct.unpack_from("<I", p, off)
            off += 4
            d = {}
            for _ in range(n):
                k, off = val(off)
                v, off = val(off)
                d[k] = v
            return d, off
        raise ValueError(f"F_KV_REPLY: unknown value tag {t:#x}")

    v, end = val(1)
    if end != len(p):
        raise ValueError("F_KV_REPLY: trailing bytes")
    return v


def auth_payload(user: str, password: Optional[str]) -> bytes:
    if not user:
        raise ValueError("PDB wire AUTH: username must not be empty")
    u = user.encode("utf-8")
    p = (password or "").encode("utf-8")
    if len(u) > 4096 or len(p) > 4096:
        raise ValueError("PDB wire AUTH: a credential field exceeds the server's 4096-byte limit")
    return _lp32(u) + _lp32(p)


def rst_payload(code: int) -> bytes:
    return struct.pack("<H", code)


def goaway_payload(last_stream: int, code: int = ERR_NO_ERROR) -> bytes:
    return struct.pack("<IH", last_stream & 0xFFFFFFFF, code)


def _u16_str(s: str) -> bytes:
    u = s.encode("utf-8")
    if len(u) > 0xFFFF:
        raise ValueError("bulk ingest: name longer than 65535 bytes")
    return struct.pack("<H", len(u)) + u


def bulk_begin_payload(relation: str, columns: Sequence[str], commit_every_rows: int = 0) -> bytes:
    """``[BEGIN][u16 len][relation][u16 n]{[u16 len][col]}[u8 flags][u64 commit_every]``."""
    out = bytearray([BULK_BEGIN]) + _u16_str(relation) + struct.pack("<H", len(columns))
    for c in columns:
        out += _u16_str(c)
    out += bytes([0]) + struct.pack("<Q", commit_every_rows)
    return bytes(out)


def bulk_data_payload(ipc_stream: bytes) -> bytes:
    return bytes([BULK_DATA]) + ipc_stream


def bulk_end_payload(data_frames: int) -> bytes:
    return bytes([BULK_END]) + struct.pack("<Q", data_frames)


# ═════════════════════════════════════════════════════════════════════════════
# Typed parameters (F_EXECUTE), pdb_wire_params.rs
# ═════════════════════════════════════════════════════════════════════════════

class TypedParam:
    """A value with an explicit wire type, for when the Python type alone does
    not say it (``Int2(5)``, ``Int4(5)``, ``Float4(1.5)``, ``Json(...)``,
    ``Jsonb(...)``). ``value`` may be ``None`` (a NULL)."""

    __slots__ = ("tag", "value")
    tag_name = "?"

    def __init__(self, value: Any):
        self.value = value

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.value!r})"

    def __eq__(self, other: object) -> bool:
        return type(other) is type(self) and other.value == self.value  # type: ignore[attr-defined]

    def __hash__(self) -> int:
        return hash((type(self), repr(self.value)))


class Int2(TypedParam):
    tag = P_INT2


class Int4(TypedParam):
    tag = P_INT4


class Float4(TypedParam):
    tag = P_FLOAT4


class Json(TypedParam):
    """A ``json`` parameter: a str is sent as its text, anything else as ``json.dumps``."""
    tag = P_JSON


class Jsonb(TypedParam):
    """A ``jsonb`` parameter: a str is sent as its text, anything else as ``json.dumps``."""
    tag = P_JSONB


_EPOCH_DATE = _dt.date(1970, 1, 1)
_EPOCH_NAIVE = _dt.datetime(1970, 1, 1)
_EPOCH_UTC = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)
_I64 = struct.Struct("<q")
_I32 = struct.Struct("<i")


def _numeric_bytes(d: _decimal.Decimal) -> Tuple[int, bytes]:
    """``(scale, unscaled two's-complement big-endian)`` of a finite Decimal."""
    sign, digits, exp = d.as_tuple()
    unscaled = int("".join(map(str, digits)) or "0")
    if sign:
        unscaled = -unscaled
    if exp > 0:  # 1E+3: no negative scales on the wire; expand
        unscaled *= 10 ** exp
        scale = 0
    else:
        scale = -exp
    n = max(1, (unscaled.bit_length() + 8) // 8)
    return scale, unscaled.to_bytes(n, "big", signed=True)


def _micros(td: _dt.timedelta) -> int:
    return (td.days * 86_400 + td.seconds) * 1_000_000 + td.microseconds


def _json_text(v: Any) -> str:
    return v if isinstance(v, str) else _json.dumps(v)


def param_tag(v: Any) -> int:
    """The wire tag a Python value travels as (see :func:`encode_param`)."""
    if v is None:
        return P_NULL
    if isinstance(v, TypedParam):
        return v.tag
    if isinstance(v, bool):
        return P_BOOL
    if isinstance(v, int):
        return P_INT8 if -(1 << 63) <= v < (1 << 63) else P_NUMERIC
    if isinstance(v, float):
        return P_FLOAT8
    if isinstance(v, _decimal.Decimal):
        return P_NUMERIC if v.is_finite() else P_TEXT
    if isinstance(v, str):
        return P_TEXT
    if isinstance(v, (bytes, bytearray, memoryview)):
        return P_BYTEA
    if isinstance(v, _dt.datetime):
        return P_TIMESTAMP if v.tzinfo is None or v.utcoffset() is None else P_TIMESTAMPTZ
    if isinstance(v, _dt.date):
        return P_DATE
    if isinstance(v, _dt.time):
        return P_TIME
    if isinstance(v, _uuid.UUID):
        return P_UUID
    if isinstance(v, (list, tuple)):
        return P_ARRAY
    if isinstance(v, dict):
        return P_JSONB
    raise TypeError(f"cannot bind a {type(v).__name__} parameter ({v!r})")


def encode_param(out: bytearray, v: Any, tag: Optional[int] = None) -> None:
    """Append ``[u8 tag][value]`` for ``v``.

    Python type -> tag: ``None`` NULL, ``bool`` bool, ``int`` int8 (numeric past
    64 bits), ``float`` float8, ``Decimal`` numeric (scale kept exactly; NaN /
    Infinity as text), ``str`` text, ``bytes`` bytea, ``datetime`` timestamp
    (naive: wall clock) or timestamptz (aware: the instant and its offset),
    ``date`` date, ``time`` time, ``UUID`` uuid, ``list``/``tuple`` array
    (tag 17; element type from the elements), ``dict`` jsonb, and the explicit
    wrappers :class:`Int2` :class:`Int4` :class:`Float4` :class:`Json`
    :class:`Jsonb`.
    """
    if tag is None:
        tag = param_tag(v)
    if isinstance(v, TypedParam):
        v = v.value
        if v is None:
            out.append(P_NULL)
            return
    if tag == P_NULL:
        out.append(P_NULL)
    elif tag == P_INT8:
        out.append(P_INT8)
        out += _I64.pack(int(v))
    elif tag == P_TEXT:
        b = str(v).encode("utf-8")
        out.append(P_TEXT)
        out += struct.pack("<I", len(b))
        out += b
    elif tag == P_BOOL:
        out.append(P_BOOL)
        out.append(1 if v else 0)
    elif tag == P_FLOAT8:
        out.append(P_FLOAT8)
        out += struct.pack("<d", float(v))
    elif tag == P_FLOAT4:
        out.append(P_FLOAT4)
        out += struct.pack("<f", float(v))
    elif tag == P_NUMERIC:
        d = v if isinstance(v, _decimal.Decimal) else _decimal.Decimal(int(v)) if isinstance(v, int) else _decimal.Decimal(str(v))
        scale, mag = _numeric_bytes(d)
        out.append(P_NUMERIC)
        out += struct.pack("<iI", scale, len(mag))
        out += mag
    elif tag == P_DATE:
        out.append(P_DATE)
        out += _I32.pack((v - _EPOCH_DATE).days)
    elif tag == P_TIMESTAMP:
        out.append(P_TIMESTAMP)
        out += _I64.pack(_micros(v - _EPOCH_NAIVE))
    elif tag == P_TIMESTAMPTZ:
        off = v.utcoffset()
        out.append(P_TIMESTAMPTZ)
        out += _I64.pack(_micros(v - _EPOCH_UTC))
        out += _I32.pack(int(off.total_seconds()))
    elif tag == P_TIME:
        if v.tzinfo is not None and v.utcoffset() is not None:
            raise TypeError("a time with a zone cannot be bound (the server has no timetz)")
        out.append(P_TIME)
        out += _I64.pack(((v.hour * 60 + v.minute) * 60 + v.second) * 1_000_000 + v.microsecond)
    elif tag == P_UUID:
        out.append(P_UUID)
        out += v.bytes
    elif tag == P_BYTEA:
        b = bytes(v)
        out.append(P_BYTEA)
        out += struct.pack("<I", len(b))
        out += b
    elif tag == P_INT2:
        out.append(P_INT2)
        out += struct.pack("<h", int(v))
    elif tag == P_INT4:
        out.append(P_INT4)
        out += _I32.pack(int(v))
    elif tag in (P_JSON, P_JSONB):
        b = _json_text(v).encode("utf-8")
        out.append(tag)
        out += struct.pack("<I", len(b))
        out += b
    elif tag == P_ARRAY:
        _encode_array(out, v)
    else:  # pragma: no cover - param_tag only returns the tags above
        raise TypeError(f"no encoder for tag {tag}")


def _array_shape(v: Any, dims: List[int], depth: int) -> None:
    if depth > MAX_ARRAY_DIMS:
        raise ValueError(f"array parameter nests deeper than {MAX_ARRAY_DIMS} dimensions")
    dims.append(len(v))
    if v and all(isinstance(e, (list, tuple)) for e in v):
        inner: List[int] = []
        _array_shape(v[0], inner, depth + 1)
        for e in v[1:]:
            other: List[int] = []
            _array_shape(e, other, depth + 1)
            if other != inner:
                raise ValueError("a multi-dimensional array parameter must be rectangular")
        dims.extend(inner)
    elif any(isinstance(e, (list, tuple)) for e in v):
        raise ValueError("an array parameter mixes sub-arrays and scalars")


def _flatten(v: Any, out: List[Any]) -> None:
    for e in v:
        if isinstance(e, (list, tuple)):
            _flatten(e, out)
        else:
            out.append(e)


_ARRAY_ELEM_OK = {P_INT8, P_TEXT, P_BOOL, P_FLOAT8, P_FLOAT4, P_NUMERIC, P_DATE, P_TIMESTAMP,
                  P_TIMESTAMPTZ, P_UUID, P_BYTEA, P_INT2, P_INT4, P_JSON, P_JSONB}


def array_elem_tag(elems: Sequence[Any]) -> int:
    """One element tag for every non-NULL element (int + float widens to float8;
    int + Decimal to numeric); text when every element is NULL."""
    tags = {param_tag(e) for e in elems if e is not None}
    if not tags:
        return P_TEXT
    if len(tags) == 1:
        t = tags.pop()
    elif tags <= {P_INT8, P_FLOAT8}:
        t = P_FLOAT8
    elif tags <= {P_INT8, P_NUMERIC}:
        t = P_NUMERIC
    else:
        raise TypeError(f"array parameter mixes element types {sorted(tags)}")
    if t not in _ARRAY_ELEM_OK:
        raise TypeError(f"array elements of wire tag {t} cannot be bound")
    return t


def _encode_array(out: bytearray, v: Any) -> None:
    dims: List[int] = []
    _array_shape(v, dims, 1)
    if not v:
        dims = []  # the empty array {}: ndims 0
    elems: List[Any] = []
    _flatten(v, elems)
    et = array_elem_tag(elems)
    out.append(P_ARRAY)
    out.append(et)
    out.append(len(dims))
    for d in dims:
        out += struct.pack("<I", d)
    for e in elems:
        if e is None:
            out.append(P_NULL)
        else:
            if et == P_FLOAT8 and isinstance(e, int) and not isinstance(e, bool):
                e = float(e)
            elif et == P_NUMERIC and isinstance(e, int):
                e = _decimal.Decimal(e)
            encode_param(out, e, et if not isinstance(e, TypedParam) else None)


def encode_params(params: Sequence[Any]) -> bytes:
    """``[u16 n]`` is NOT included: the caller writes it (see :func:`execute_payload`)."""
    if len(params) > 0xFFFF:
        raise ValueError(f"{len(params)} parameters exceed the wire's 65535")
    out = bytearray()
    for v in params:
        encode_param(out, v)
    return bytes(out)


def needs_ext(params: Sequence[Any]) -> int:
    """Extension bits the parameters need (typed tags 4..16, array tag 17)."""
    need = 0
    for v in params:
        t = param_tag(v)
        if t == P_ARRAY:
            need |= EXT_ARRAY_PARAMS
        elif t not in (P_NULL, P_INT8, P_TEXT, P_BOOL):
            need |= EXT_TYPED_PARAMS
    return need


# ═════════════════════════════════════════════════════════════════════════════
# Response payloads
# ═════════════════════════════════════════════════════════════════════════════

def decode_ok(p: bytes) -> int:
    return struct.unpack_from("<Q", p, 0)[0] if len(p) >= 8 else 0


def decode_error(p: bytes) -> Tuple[int, str]:
    if len(p) < 2:
        return 0, "unknown error"
    return struct.unpack_from("<H", p, 0)[0], bytes(p[2:]).decode("utf-8", errors="replace")


def decode_notices(p: bytes) -> Optional[List[Tuple[str, str, str]]]:
    """``[u16 n] n x ([u8 len][severity][u8 len][sqlstate][u32 len][message])``;
    ``None`` when malformed (the frame is then dropped whole, as the server's
    own decoder does)."""
    try:
        (n,) = struct.unpack_from("<H", p, 0)
        off = 2
        out = []
        for _ in range(n):
            sl = p[off]
            sev = bytes(p[off + 1: off + 1 + sl]).decode("utf-8", "replace")
            off += 1 + sl
            tl = p[off]
            st = bytes(p[off + 1: off + 1 + tl]).decode("utf-8", "replace")
            off += 1 + tl
            (ml,) = struct.unpack_from("<I", p, off)
            off += 4
            if off + ml > len(p):
                return None
            out.append((sev, st, bytes(p[off: off + ml]).decode("utf-8", "replace")))
            off += ml
        return out if off == len(p) else None
    except (IndexError, struct.error):
        return None


class BatchResult:
    """A decoded BATCH_RESULT: per-entry counts of the statements that
    succeeded (``-1`` for a row-returning one), then at most one failure."""

    __slots__ = ("counts", "entries", "error", "error_code", "error_sqlstate", "results", "notices")

    def __init__(self) -> None:
        self.counts: List[int] = []
        self.results: List[Any] = []  # per entry: int count or Arrow IPC bytes
        self.entries = 0
        self.error: Optional[str] = None
        self.error_code = -1
        self.error_sqlstate: Optional[str] = None
        self.notices: List[Any] = []


BATCH_ENTRY_ERROR_SQLSTATE = 0x90


def decode_batch_result(p: bytes) -> BatchResult:
    """Walk a BATCH_RESULT: ``[u32 n]`` then entries ``[0x82][u64]``,
    ``[0x81][u32 len][ipc]``, ``[0x83][u32 len][msg]`` (no code) or, with
    ``batch_sqlstate``, ``[0x90][u16 code][5 x SQLSTATE][u32 len][msg]``.
    Raises ``ValueError`` on any malformed payload."""
    if len(p) < 4:
        raise ValueError("batch result shorter than its 4-byte count")
    (n,) = struct.unpack_from("<I", p, 0)
    if n > (len(p) - 4) // 5:
        raise ValueError(f"batch result claims {n} entries in {len(p)} bytes")
    r = BatchResult()
    r.entries = n
    off = 4
    for _ in range(n):
        if r.error is not None:
            raise ValueError("batch result has entries after an ERROR entry")
        if off >= len(p):
            raise ValueError("batch result truncated")
        t = p[off]
        off += 1
        if t == F_OK:
            if off + 8 > len(p):
                raise ValueError("batch result truncated")
            c = struct.unpack_from("<Q", p, off)[0]
            off += 8
            r.counts.append(c)
            r.results.append(c)
        elif t == BATCH_ENTRY_ERROR_SQLSTATE:
            if off + 11 > len(p):
                raise ValueError("batch result truncated")
            r.error_code = struct.unpack_from("<H", p, off)[0]
            r.error_sqlstate = bytes(p[off + 2: off + 7]).decode("ascii", "replace")
            off += 7
            (ln,) = struct.unpack_from("<I", p, off)
            off += 4
            if off + ln > len(p):
                raise ValueError("batch result truncated")
            r.error = bytes(p[off: off + ln]).decode("utf-8", "replace")
            off += ln
        elif t in (F_ARROW_RESULT, F_ERROR):
            if off + 4 > len(p):
                raise ValueError("batch result truncated")
            (ln,) = struct.unpack_from("<I", p, off)
            off += 4
            if off + ln > len(p):
                raise ValueError("batch result truncated")
            if t == F_ARROW_RESULT:
                r.counts.append(-1)
                r.results.append(bytes(p[off: off + ln]))
            else:
                r.error = bytes(p[off: off + ln]).decode("utf-8", "replace")
            off += ln
        else:
            raise ValueError(f"unexpected batch entry type 0x{t:02x}")
    return r


# ═════════════════════════════════════════════════════════════════════════════
# Error code -> SQLSTATE (the inverse of the server's sqlstate_to_wire_code)
# ═════════════════════════════════════════════════════════════════════════════

ERR_GENERIC_SQL = 1064

#: Letter-bearing SQLSTATEs travel in the 6xxxx band (``SQLSTATE_WIRE_BAND`` in
#: ``pdb_wire.rs``); the JDBC driver's ``PdbSqlState.BAND`` is the
#: same table.
_BAND = {
    60000: "XX000", 60001: "42P01", 60002: "42P02", 60003: "25P02", 60004: "25P01",
    60005: "40P01", 60006: "55P03", 60007: "0A000", 60008: "22P02", 60009: "3B001",
    60010: "28P01", 60011: "22P04", 60012: "2201B", 60013: "2201E", 60014: "2201F",
    60015: "2201G", 60016: "2202E", 60017: "2200H", 60018: "22P05", 60019: "2BP01",
    60020: "3D000", 60021: "3F000", 60022: "42P05", 60023: "42P06", 60024: "42P07",
    60025: "42P10", 60026: "42P16", 60027: "42P17", 60028: "42P18", 60029: "42P19",
    60030: "42P20", 60031: "42P21", 60032: "428C9", 60033: "55P02", 60034: "57P01",
    60035: "57P03", 60036: "08P01", 60037: "P0001",
}

#: Protocol-level codes that are not numeric SQLSTATEs.
_PROTOCOL = {
    0x00: "08006", 0x01: "08P01", 0x02: "08P01", 0x03: "08P01", 0x04: "08P01",
    0x05: "08P01", 0x06: "08P01", 0x07: "57014", 0x08: "08006", 0x10: "08004",
    0x11: "53300", 0x12: "08004", 0x13: "28000", 0x14: "28P01", 0x15: "22023",
    26: "26000", ERR_GENERIC_SQL: "HY000",
}

_ENGINE_FRAMING = ("SQL execution error: ", "SQL plan error: ", "SQL parse error: ")


def sqlstate_for_code(code: int) -> str:
    """The SQLSTATE a wire error code stands for; never ``None``."""
    code &= 0xFFFF
    if code >= 60000:
        return _BAND.get(code, "HY000")
    s = _PROTOCOL.get(code)
    if s is not None:
        return s
    if code >= 10000:
        return str(code)
    if code >= 1000:
        return "0" + str(code)  # a leading-zero SQLSTATE (02000, 08006, ...)
    return "HY000"


def _is_sqlstate(s: str) -> bool:
    return len(s) == 5 and all(c.isdigit() or ("A" <= c <= "Z") for c in s)


def server_error(code: int, message: str) -> Tuple[str, str]:
    """``(sqlstate, message)`` for a server ERROR frame.

    An exact class the engine appended as a trailing ``" [XXXXX]"`` marker wins
    over the frame code (which cannot carry letter-bearing classes outside the
    band); the engine's ``Display`` framing and a leading ``"XXXXX: "`` that
    repeats the SQLSTATE are removed, as the JDBC driver removes them."""
    text = (message or "").strip()
    state = None
    # A leading "[XXXXX] " marker (``pdb_wire::kv_error_text``), possibly after
    # an opcode prefix such as "KV_INCR: ".
    head = text.find("[")
    if 0 <= head <= 16 and text[head + 6: head + 8] == "] " and _is_sqlstate(text[head + 1: head + 6]):
        state = text[head + 1: head + 6]
        text = (text[:head] + text[head + 8:]).strip()
    if state is None and text.endswith("]"):
        i = text.rfind("[")
        if i >= 0 and _is_sqlstate(text[i + 1: -1]):
            state = text[i + 1: -1]
            text = text[:i].rstrip()
    if state is None:
        state = sqlstate_for_code(code)
        # Some refusals spell "<SQLSTATE>: message" while the frame code is
        # generic: take the prefix when the code says nothing.
        if state == "HY000" and len(text) > 7 and text[5:7] == ": " and _is_sqlstate(text[:5]):
            state = text[:5]
    for f in _ENGINE_FRAMING:
        if text.startswith(f):
            text = text[len(f):]
            break
    if len(text) > 7 and text.startswith(state) and text[5:7] == ": ":
        text = text[7:]
    return state, text

