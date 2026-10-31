"""SQL literals for the batched ``executemany`` path.

``executemany`` of a statement that is not a plain INSERT sends its parameter
sets as ONE ``BATCH`` request of statement texts per chunk -- the server runs
them in order, one round trip per chunk -- exactly as the JDBC driver's
``executeBatch`` does (``PdbPreparedStatement.appendLiteral``). Values are
rendered as the literals that driver writes:

* ``None`` -> ``NULL``; ``bool`` -> ``true`` / ``false``;
* numbers (negative in parentheses, so ``x -$1`` cannot become a ``--``
  comment; NaN / Infinity quoted), bytes as ``'\\x<hex>'``, dates and
  timestamps as their ISO text, uuid / json as their text -- each CAST to the
  type the prepared path binds (see :func:`literal`);
* ``str`` -> its text as an untyped quoted literal (``'`` doubled). The server
  reads ordinary string literals standard-conforming ALWAYS (it refuses
  ``standard_conforming_strings = off``), so doubling the quote is the
  complete escape; a NUL character is refused, as the server refuses it.

Arrays and the explicit type
wrappers are not rendered: a statement with one takes the per-set prepared
path (as JDBC's array batches do).
"""

from __future__ import annotations

import datetime as _dt
import decimal as _decimal
import json as _json
import math
import re
import uuid as _uuid
from typing import Any, Dict, List, Sequence, Tuple

from .. import _protocol as P
from ._sql import _skip_quoted

_STOP = re.compile(r"[\'\"$]|--|/\*")
_PH = re.compile(r"\$(\d+)")
_POS: Dict[str, List[Tuple[int, int, int]]] = {}


class NotRenderable(Exception):
    """A value the literal path does not carry (take the prepared path)."""


def placeholders(sql: str) -> List[Tuple[int, int, int]]:
    """``[(start, end, index)]`` of every ``$n`` outside literals and comments (cached)."""
    out = _POS.get(sql)
    if out is not None:
        return out
    out = []
    i = 0
    ln = len(sql)
    while i < ln:
        m = _STOP.search(sql, i)
        if m is None:
            break
        j = m.start()
        if sql[j] == "$":
            pm = _PH.match(sql, j)
            if pm and (j == 0 or not (sql[j - 1].isalnum() or sql[j - 1] == "_")):
                out.append((j, pm.end(), int(pm.group(1)) - 1))
                i = pm.end()
                continue
        k = _skip_quoted(sql, j)
        i = k if k >= 0 else j + 1
    if len(_POS) > 1024:
        _POS.clear()
    _POS[sql] = out
    return out


def _quote(s: str) -> str:
    if "\x00" in s:
        raise NotRenderable("a NUL character")
    return "'" + s.replace("'", "''") + "'"


def literal(v: Any) -> str:
    """The literal for ``v``, CAST to the type the prepared path binds it as
    (``int`` -> int8, ``float`` -> float8, ``Decimal`` -> numeric, ``bytes`` ->
    bytea, ``date`` / ``datetime`` / aware ``datetime`` -> date / timestamp /
    timestamptz, ``UUID`` -> uuid, ``dict`` / ``Jsonb`` -> jsonb, ``Json`` ->
    json), so overloads resolve as they do for a bound value
    (``length(%s)`` of bytes is the bytea length). A ``str`` stays an untyped
    literal, as psycopg sends a str (type unknown: the server infers it)."""
    t = type(v)
    if v is None:
        return "NULL"
    if t is bool:
        return "true" if v else "false"
    if t is int:
        if -(1 << 63) <= v < (1 << 63):
            return f"{v}::int8" if v >= 0 else f"({v})::int8"
        return f"{v}::numeric" if v >= 0 else f"({v})::numeric"
    if t is float:
        if math.isnan(v):
            return "'NaN'::float8"
        if math.isinf(v):
            return "'Infinity'::float8" if v > 0 else "'-Infinity'::float8"
        r = repr(v)
        return f"({r})::float8" if r.startswith("-") else f"{r}::float8"  # -0.0 too
    if t is str:
        return _quote(v)
    if t is _decimal.Decimal:
        if not v.is_finite():
            return f"'{v}'::numeric" if not v.is_nan() else "'NaN'::numeric"
        r = format(v, "f")
        return f"({r})::numeric" if r.startswith("-") else f"{r}::numeric"
    if t in (bytes, bytearray, memoryview):
        return "'\\x" + bytes(v).hex() + "'::bytea"
    if t is _dt.datetime:
        aware = v.tzinfo is not None and v.utcoffset() is not None
        return _quote(v.isoformat(" ")) + ("::timestamptz" if aware else "::timestamp")
    if t is _dt.date:
        return _quote(v.isoformat()) + "::date"
    if t is _dt.time:
        if v.tzinfo is not None and v.utcoffset() is not None:
            raise NotRenderable("a time with a zone")
        return _quote(v.isoformat())  # the server has no time type: as bound, text
    if t is _uuid.UUID:
        return _quote(str(v)) + "::uuid"
    if t is dict:
        return _quote(_json.dumps(v)) + "::jsonb"
    if t in (P.Json, P.Jsonb):
        inner = v.value
        cast = "::json" if t is P.Json else "::jsonb"
        if inner is None:
            return "NULL" + cast
        return _quote(inner if isinstance(inner, str) else _json.dumps(inner)) + cast
    raise NotRenderable(t.__name__)


def render(sql: str, vals: Sequence[Any]) -> str:
    """``sql`` with every ``$n`` replaced by the literal of ``vals[n-1]``."""
    ph = placeholders(sql)
    if not ph:
        return sql
    out: List[str] = []
    prev = 0
    for a, b, k in ph:
        out.append(sql[prev:a])
        if a > 0 and (sql[a - 1].isalnum() or sql[a - 1] == "_"):
            out.append(" ")
        if k >= len(vals):
            raise NotRenderable(f"${k + 1} has no value")
        out.append(literal(vals[k]))
        prev = b
    out.append(sql[prev:])
    return "".join(out)


def render_all(sql: str, rows: Sequence[Sequence[Any]]) -> "List[str] | None":
    """Every set rendered, or ``None`` when one value is not renderable or a
    set holds an array / typed wrapper (the prepared path then runs them)."""
    try:
        out = []
        for r in rows:
            for v in r:
                if isinstance(v, (list, tuple, P.Int2, P.Int4, P.Float4)):
                    return None
            out.append(render(sql, r))
        return out
    except NotRenderable:
        return None


def chunks(stmts: List[str], max_n: int = 1000, max_bytes: int = 4 << 20) -> "List[List[str]]":
    """Statements in BATCH-sized groups (count and payload bounded)."""
    out: List[List[str]] = []
    cur: List[str] = []
    size = 0
    for s in stmts:
        n = len(s) * 4 + 4  # worst-case UTF-8
        if cur and (len(cur) >= max_n or size + n > max_bytes):
            out.append(cur)
            cur, size = [], 0
        cur.append(s)
        size += n
    if cur:
        out.append(cur)
    return out
