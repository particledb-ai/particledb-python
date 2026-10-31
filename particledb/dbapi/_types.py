"""DB-API type objects, constructors and ``cursor.description``.

``type_code`` is the PostgreSQL type OID of the column (``23`` int4, ``1700``
numeric, ``1184`` timestamptz, ...), as psycopg reports it, so tooling that
keys on OIDs (SQLAlchemy, pandas) reads it unchanged. The OID comes from the
column's declared SQL type (the ``pdb:decl`` field metadata the engine
stamps: ``INT``, ``NUMERIC(10,2)``, ``character varying``), else from the
Arrow type of the result column.
"""

from __future__ import annotations

import datetime as _dt
import re
import time as _time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .. import _arrow

# ── OIDs ────────────────────────────────────────────────────────────────────

BOOL, BYTEA, CHAR1, NAME, INT8, INT2, INT4, TEXT, OID = 16, 17, 18, 19, 20, 21, 23, 25, 26
JSON, FLOAT4, FLOAT8, UNKNOWN, BPCHAR, VARCHAR = 114, 700, 701, 705, 1042, 1043
DATE, TIME, TIMESTAMP, TIMESTAMPTZ, INTERVAL, TIMETZ = 1082, 1083, 1114, 1184, 1186, 1266
NUMERIC, UUID, JSONB, VECTOR = 1700, 2950, 3802, 6000

#: element OID -> array OID (PostgreSQL's pg_type.typarray).
ARRAY_OF = {
    BOOL: 1000, BYTEA: 1001, INT2: 1005, INT4: 1007, TEXT: 1009, BPCHAR: 1014, VARCHAR: 1015,
    INT8: 1016, FLOAT4: 1021, FLOAT8: 1022, TIMESTAMP: 1115, DATE: 1182, TIME: 1183,
    TIMESTAMPTZ: 1185, INTERVAL: 1187, NUMERIC: 1231, UUID: 2951, JSONB: 3807, JSON: 199,
}
ARRAY_OIDS = frozenset(ARRAY_OF.values())

#: Declared type name (lower case, no modifiers) -> OID.
_DECL_OID = {
    "bool": BOOL, "boolean": BOOL, "bytea": BYTEA, "blob": BYTEA, "name": NAME,
    "bigint": INT8, "int8": INT8, "bigserial": INT8, "serial8": INT8,
    "smallint": INT2, "int2": INT2, "smallserial": INT2, "serial2": INT2,
    "int": INT4, "integer": INT4, "int4": INT4, "serial": INT4, "serial4": INT4,
    "text": TEXT, "string": TEXT, "oid": OID, "json": JSON, "jsonb": JSONB,
    "real": FLOAT4, "float4": FLOAT4, "double precision": FLOAT8, "float8": FLOAT8,
    "double": FLOAT8, "float": FLOAT8,
    "char": BPCHAR, "character": BPCHAR, "bpchar": BPCHAR,
    "varchar": VARCHAR, "character varying": VARCHAR,
    "date": DATE, "time": TIME, "time without time zone": TIME, "timetz": TIMETZ,
    "time with time zone": TIMETZ,
    "timestamp": TIMESTAMP, "timestamp without time zone": TIMESTAMP, "datetime": TIMESTAMP,
    "timestamptz": TIMESTAMPTZ, "timestamp with time zone": TIMESTAMPTZ,
    "interval": INTERVAL, "numeric": NUMERIC, "decimal": NUMERIC, "dec": NUMERIC,
    "uuid": UUID, "vector": VECTOR,
}

#: OID -> PostgreSQL type name (``Column.type_name``).
TYPE_NAMES = {
    BOOL: "bool", BYTEA: "bytea", NAME: "name", INT8: "int8", INT2: "int2", INT4: "int4",
    TEXT: "text", OID: "oid", JSON: "json", FLOAT4: "float4", FLOAT8: "float8",
    UNKNOWN: "unknown", BPCHAR: "bpchar", VARCHAR: "varchar", DATE: "date", TIME: "time",
    TIMESTAMP: "timestamp", TIMESTAMPTZ: "timestamptz", INTERVAL: "interval", TIMETZ: "timetz",
    NUMERIC: "numeric", UUID: "uuid", JSONB: "jsonb", VECTOR: "vector",
}
for _e, _a in ARRAY_OF.items():
    TYPE_NAMES[_a] = "_" + TYPE_NAMES[_e]

#: Fixed internal sizes, as psycopg reports them.
_INTERNAL_SIZE = {BOOL: 1, INT2: 2, INT4: 4, INT8: 8, FLOAT4: 4, FLOAT8: 8, DATE: 4, TIME: 8,
                  TIMESTAMP: 8, TIMESTAMPTZ: 8, INTERVAL: 16, UUID: 16, OID: 4}

_MOD = re.compile(r"^\s*([a-z_][a-z0-9_ ]*)(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?\s*(.*)$")


def _decl_info(decl: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """``(oid, mod1, mod2)`` of a declared type (``NUMERIC(10,2)``,
    ``VARCHAR(20)``, ``INT[]``, ``_int4``); oid ``None`` when unknown."""
    d = decl.strip().lower()
    array = False
    if d.endswith("[]"):
        d, array = d[:-2].strip(), True
    elif d.startswith("_"):
        d, array = d[1:], True
    m = _MOD.match(d)
    if not m:
        return None, None, None
    base, p, s, rest = m.group(1).strip(), m.group(2), m.group(3), m.group(4).strip()
    if rest:
        base = (base + " " + rest).strip()
    oid = _DECL_OID.get(base)
    if oid is None and base.startswith("timestamp"):
        oid = TIMESTAMPTZ if "with time zone" in base else TIMESTAMP
    if oid is None:
        return None, None, None
    if array:
        return ARRAY_OF.get(oid, TEXT), None, None
    return oid, (int(p) if p else None), (int(s) if s else None)


def _arrow_oid(f: "_arrow.Field") -> int:
    k, p = f.kind, f.params
    if k == 2:  # int
        return {8: INT2, 16: INT2, 32: INT4}.get(p.get("bits", 64), INT8)
    if k == 3:  # float
        return FLOAT4 if p.get("bits") in (16, 32) else FLOAT8
    if k == 6:
        return BOOL
    if k in (5, 20):
        return TEXT
    if k in (4, 19, 15):
        return BYTEA
    if k == 7:
        return NUMERIC
    if k == 8:
        return DATE
    if k == 9:
        return TIME
    if k == 10:
        return TIMESTAMPTZ if (p.get("tz") or "pdb:tz" in f.metadata) else TIMESTAMP
    if k in (11, 18):
        return INTERVAL
    if k in (12, 16, 21) and f.children:
        return ARRAY_OF.get(_arrow_oid(f.children[0]), TEXT)
    if k == 1:
        return TEXT
    return UNKNOWN


class Column(tuple):
    """One ``cursor.description`` entry: the PEP 249 7-tuple ``(name,
    type_code, display_size, internal_size, precision, scale, null_ok)``,
    readable by attribute too, plus ``type_name`` (PostgreSQL's name for
    ``type_code``) and ``decl`` (the column's declared SQL type, when the
    server sent one)."""

    def __new__(cls, name: str, type_code: int, display_size: Optional[int], internal_size: Optional[int],
                precision: Optional[int], scale: Optional[int], null_ok: Optional[bool],
                decl: Optional[str] = None) -> "Column":
        self = tuple.__new__(cls, (name, type_code, display_size, internal_size, precision, scale, null_ok))
        self.decl = decl
        return self

    name = property(lambda self: self[0])
    type_code = property(lambda self: self[1])
    display_size = property(lambda self: self[2])
    internal_size = property(lambda self: self[3])
    precision = property(lambda self: self[4])
    scale = property(lambda self: self[5])
    null_ok = property(lambda self: self[6])

    @property
    def type_name(self) -> str:
        return TYPE_NAMES.get(self[1], "unknown")

    def __repr__(self) -> str:
        return f"<Column {self[0]!r}, type: {self.type_name} (oid: {self[1]})>"


def column_for(f: "_arrow.Field") -> Column:
    decl = f.decl
    oid = mod1 = mod2 = None
    if decl:
        oid, mod1, mod2 = _decl_info(decl)
    if oid is None:
        oid = _arrow_oid(f)
    display = precision = scale = None
    if oid in (VARCHAR, BPCHAR):
        display = mod1
    elif oid == NUMERIC:
        if mod1 is not None:
            precision, scale = mod1, (mod2 or 0)
        elif f.kind == 7 and not decl:
            precision, scale = f.params.get("precision"), f.params.get("scale")
    return Column(f.name, oid, display, _INTERNAL_SIZE.get(oid), precision, scale,
                  bool(f.nullable), decl)


_DESC_CACHE: Dict[int, Tuple[List[Any], List[Column]]] = {}


def description_for(fields: List["_arrow.Field"]) -> Optional[List[Column]]:
    """``cursor.description`` for a result's fields (cached per schema: the
    decoder hands back the same ``fields`` list for every answer with the
    same schema bytes)."""
    if not fields:
        return None
    hit = _DESC_CACHE.get(id(fields))
    if hit is not None and hit[0] is fields:
        return hit[1]
    desc = [column_for(f) for f in fields]
    if len(_DESC_CACHE) > 512:
        _DESC_CACHE.clear()
    _DESC_CACHE[id(fields)] = (fields, desc)
    return desc


# ── PEP 249 type objects ────────────────────────────────────────────────────

class DBAPITypeObject:
    """Compares equal to every type code of its group (PEP 249)."""

    def __init__(self, name: str, oids: Sequence[int]):
        self.name = name
        self.values = frozenset(oids)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DBAPITypeObject):
            return other.values == self.values
        return other in self.values

    def __ne__(self, other: object) -> bool:
        return not self == other

    def __hash__(self) -> int:
        return hash(self.values)

    def __repr__(self) -> str:
        return f"<DBAPITypeObject {self.name}>"


STRING_TYPE = DBAPITypeObject("STRING", [TEXT, VARCHAR, BPCHAR, NAME, CHAR1, JSON, JSONB, UNKNOWN])
BINARY_TYPE = DBAPITypeObject("BINARY", [BYTEA])
NUMBER_TYPE = DBAPITypeObject("NUMBER", [INT2, INT4, INT8, FLOAT4, FLOAT8, NUMERIC, OID])
DATETIME_TYPE = DBAPITypeObject("DATETIME", [DATE, TIME, TIMETZ, TIMESTAMP, TIMESTAMPTZ, INTERVAL])
ROWID_TYPE = DBAPITypeObject("ROWID", [OID])


# ── constructors ────────────────────────────────────────────────────────────

def Date(year: int, month: int, day: int) -> _dt.date:
    return _dt.date(year, month, day)


def Time(hour: int, minute: int, second: int) -> _dt.time:
    return _dt.time(hour, minute, second)


def Timestamp(year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0) -> _dt.datetime:
    return _dt.datetime(year, month, day, hour, minute, second)


def DateFromTicks(ticks: float) -> _dt.date:
    return Date(*_time.localtime(ticks)[:3])


def TimeFromTicks(ticks: float) -> _dt.time:
    return Time(*_time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks: float) -> _dt.datetime:
    return Timestamp(*_time.localtime(ticks)[:6])


def Binary(value: Any) -> bytes:
    return bytes(value)
