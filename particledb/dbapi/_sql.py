"""Statement text: placeholder styles -> the wire's ``$n``, and top-level
statement splitting. Pure functions, results cached per statement text.

Placeholder styles (``paramstyle`` names from PEP 249):

* ``format``   -- ``%s`` (also psycopg's ``%b`` / ``%t``), parameters a sequence;
* ``pyformat`` -- ``%(name)s``, parameters a mapping (a name may repeat);
* ``qmark``    -- ``?``, parameters a sequence (``??`` is a literal ``?``,
  for the jsonb ``?`` operators);
* ``numeric``  -- ``$1 .. $n`` (the wire's own form; sent unchanged);
* ``auto``     -- the default: a mapping means ``pyformat``; with a sequence,
  ``%s`` placeholders mean ``format``, else ``$n`` means ``numeric``, else
  ``?`` means ``qmark``.

``format`` / ``pyformat`` follow psycopg exactly: ``%`` is significant
everywhere in the text once parameters are given (write ``%%`` for a literal
``%``, also inside a string literal), and a statement executed WITHOUT
parameters (``params=None``) is sent verbatim. ``qmark`` and the statement
splitter understand SQL lexically: string literals (``'..'``, ``E'..'``),
quoted identifiers, dollar quotes and both comment forms are skipped.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from .errors import ProgrammingError

STYLES = ("auto", "format", "pyformat", "qmark", "numeric")

_PCT = re.compile(r"%(?:\(([^)]*)\))?(.)", re.S)
_HAS_FORMAT = re.compile(r"%(?:[sbt]|\([^)]*\)[sbt])")
_HAS_DOLLAR = re.compile(r"\$\d")
_IDENT_CHAR = re.compile(r"[\w$]")  # \w is Unicode: cafe\u0301$tag$ is an identifier
_DOLLAR_TAG = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$")


class Parsed:
    """A statement converted for the wire."""

    __slots__ = ("sql", "names", "nparams", "style")

    def __init__(self, sql: str, names: Optional[List[str]], nparams: int, style: str):
        self.sql = sql          # with $n placeholders
        self.names = names      # pyformat: name of $1, $2, ...
        self.nparams = nparams  # placeholders found (-1: not counted, $n passthrough)
        self.style = style


_CACHE: Dict[Tuple[str, str], Parsed] = {}
_CACHE_MAX = 2048


def _format(sql: str, named: bool) -> Parsed:
    out: List[str] = []
    names: List[str] = []
    index: Dict[str, int] = {}
    pos = 0
    n = 0
    for m in _PCT.finditer(sql):
        out.append(sql[pos:m.start()])
        pos = m.end()
        name, conv = m.group(1), m.group(2)
        if name is None and conv == "%":
            out.append("%")
            continue
        if conv not in "sbt":
            raise ProgrammingError(
                f"only '%s', '%b', '%t' are allowed as placeholders, got '%{conv}' "
                "(write '%%' for a literal '%')")
        if named:
            if name is None:
                raise ProgrammingError("positional %s placeholder with named (mapping) parameters")
            k = index.get(name)
            if k is None:
                names.append(name)
                k = index[name] = len(names)
            out.append(f"${k}")
        else:
            if name is not None:
                raise ProgrammingError(f"named placeholder %({name})s with positional (sequence) parameters")
            n += 1
            out.append(f"${n}")
    out.append(sql[pos:])
    return Parsed("".join(out), names if named else None, len(names) if named else n,
                  "pyformat" if named else "format")


def _skip_quoted(sql: str, i: int) -> int:
    """``i`` is at a quote, comment or dollar-quote start: index just past it
    (``len(sql)`` when unterminated); ``-1`` when ``i`` starts none of them."""
    c = sql[i]
    n = len(sql)
    if c == "'":
        backslash = i > 0 and sql[i - 1] in "eE" and (i < 2 or not _IDENT_CHAR.match(sql[i - 2]))
        j = i + 1
        while j < n:
            d = sql[j]
            if backslash and d == "\\":
                j += 2
                continue
            if d == "'":
                if j + 1 < n and sql[j + 1] == "'":
                    j += 2
                    continue
                return j + 1
            j += 1
        return n
    if c == '"':
        j = sql.find('"', i + 1)
        while j != -1 and j + 1 < n and sql[j + 1] == '"':
            j = sql.find('"', j + 2)
        return n if j == -1 else j + 1
    if c == "-" and sql.startswith("--", i):
        j = sql.find("\n", i)
        return n if j == -1 else j + 1
    if c == "/" and sql.startswith("/*", i):
        depth = 1
        j = i + 2
        while j < n and depth:
            if sql.startswith("/*", j):
                depth += 1
                j += 2
            elif sql.startswith("*/", j):
                depth -= 1
                j += 2
            else:
                j += 1
        return j
    if c == "$" and (i == 0 or not _IDENT_CHAR.match(sql[i - 1])):
        m = _DOLLAR_TAG.match(sql, i)
        if m:
            tag = m.group(0)
            j = sql.find(tag, m.end())
            return n if j == -1 else j + len(tag)
    return -1


_QM_STOP = re.compile(r"[\'\"?$]|--|/\*")


def _qmark(sql: str) -> Parsed:
    out: List[str] = []
    n = 0
    i = 0
    start = 0
    ln = len(sql)
    while True:
        m = _QM_STOP.search(sql, i)
        if m is None:
            break
        j = m.start()
        if sql[j] == "?":
            out.append(sql[start:j])
            if j + 1 < ln and sql[j + 1] == "?":
                out.append("?")
                i = j + 2
            else:
                n += 1
                out.append(f"${n}")
                i = j + 1
            start = i
            continue
        k = _skip_quoted(sql, j)
        i = k if k >= 0 else j + 1
    out.append(sql[start:])
    return Parsed("".join(out), None, n, "qmark")


def convert(sql: str, params: Any, style: str = "auto") -> Tuple[str, List[Any]]:
    """``(wire_sql, wire_params)`` for ``sql`` with ``params`` (``None`` ->
    sent verbatim, no parameters)."""
    if params is None:
        return sql, []
    mapping = isinstance(params, Mapping)
    if style == "auto":
        if mapping:
            style = "pyformat"
        elif "%" in sql and _HAS_FORMAT.search(sql):
            style = "format"
        elif "$" in sql and _HAS_DOLLAR.search(sql):
            style = "numeric"
        elif "?" in sql:
            style = "qmark"
        else:
            style = "format"
    key = (sql, style)
    p = _CACHE.get(key)
    if p is None:
        if style in ("format", "pyformat"):
            p = _format(sql, style == "pyformat")
        elif style == "qmark":
            p = _qmark(sql)
        elif style == "numeric":
            p = Parsed(sql, None, -1, "numeric")
        else:
            raise ProgrammingError(f"unknown paramstyle {style!r} (one of {', '.join(STYLES)})")
        if len(_CACHE) >= _CACHE_MAX:
            _CACHE.clear()
        _CACHE[key] = p
    if p.names is not None:
        if not mapping:
            raise ProgrammingError("the query has named placeholders %(name)s: pass a mapping")
        try:
            return p.sql, [params[k] for k in p.names]
        except KeyError as e:
            raise ProgrammingError(f"query parameter missing: {e.args[0]}") from None
    if mapping:
        if style == "numeric":
            if not params:
                return p.sql, []  # an empty mapping: no parameters (psycopg accepts it)
            raise ProgrammingError("$n placeholders take a sequence of parameters, not a mapping")
        if p.nparams:
            raise ProgrammingError("the query has positional placeholders: pass a sequence")
        return p.sql, []
    if isinstance(params, (str, bytes)):
        raise ProgrammingError("parameters must be a sequence or a mapping, not a string")
    vals = params if isinstance(params, list) else list(params)
    if p.nparams >= 0 and p.nparams != len(vals):
        raise ProgrammingError(
            f"the query has {p.nparams} placeholder(s) but {len(vals)} parameter(s) were passed")
    return p.sql, vals


# ── statement analysis ──────────────────────────────────────────────────────

class Shape:
    """What the client needs to know about a statement's text."""

    __slots__ = ("statements", "verb", "returning", "txn_control", "insert")

    def __init__(self, statements: Optional[List[str]], verb: str, returning: bool,
                 txn_control: bool, insert: Optional[Tuple[str, Optional[List[str]], int]]):
        self.statements = statements  # None: one statement
        self.verb = verb              # first keyword, upper case
        self.returning = returning    # a top-level RETURNING (heuristic)
        self.txn_control = txn_control
        self.insert = insert          # (relation, columns or None, n values): plain INSERT .. VALUES ($n..)


_SHAPES: Dict[str, Shape] = {}
_TXN_VERBS = {"BEGIN", "START", "COMMIT", "END", "ROLLBACK", "ABORT", "PREPARE"}
_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


_SCAN_STOP = re.compile(r"[\'\";$]|--|/\*")
_RETURNING = re.compile(r"(?<![A-Za-z0-9_$])RETURNING(?![A-Za-z0-9_$])", re.I)


def _scan(sql: str) -> Tuple[List[Tuple[int, int]], List[Tuple[int, int]]]:
    """``(statement spans, plain spans)``: top-level ``;``-separated spans, and
    the spans of text outside literals, quoted identifiers and comments."""
    spans: List[Tuple[int, int]] = []
    plain: List[Tuple[int, int]] = []
    i = start = seg = 0
    ln = len(sql)
    while True:
        m = _SCAN_STOP.search(sql, i)
        if m is None:
            break
        j = m.start()
        if sql[j] == ";":
            plain.append((seg, j))
            spans.append((start, j))
            start = seg = i = j + 1
            continue
        k = _skip_quoted(sql, j)
        if k < 0:
            i = j + 1
            continue
        plain.append((seg, j))
        seg = i = k
    plain.append((seg, ln))
    spans.append((start, ln))
    return spans, plain


def _first_word(sql: str, plain: List[Tuple[int, int]], a: int, b: int) -> str:
    for x, y in plain:
        x, y = max(x, a), min(y, b)
        if x < y:
            m = _WORD.search(sql, x, y)
            if m:
                return m.group(0).upper()
    return ""


def _meaningful(sql: str) -> bool:
    """Anything but whitespace and comments."""
    i = 0
    ln = len(sql)
    while i < ln:
        c = sql[i]
        if c.isspace():
            i += 1
            continue
        if c in "-/":
            j = _skip_quoted(sql, i)
            if j >= 0:
                i = j
                continue
        return True
    return False


_INSERT = re.compile(
    r"""^\s*INSERT\s+INTO\s+((?:"(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*)(?:\s*\.\s*(?:"(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*)){0,2})\s*"""
    r"""(?:\(\s*([^()]*?)\s*\))?\s*VALUES\s*\(\s*((?:\$\d+\s*,\s*)*\$\d+)\s*\)\s*;?\s*$""",
    re.I | re.S)
_RESERVED_REL = {"VALUES", "SELECT", "DEFAULT", "ON", "RETURNING", "OVERRIDING", "AS", "WITH"}


def _ident(tok: str) -> Optional[str]:
    tok = tok.strip()
    if tok.startswith('"') and tok.endswith('"') and len(tok) > 2:
        return tok[1:-1].replace('""', '"')
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", tok) and tok.upper() not in _RESERVED_REL:
        return tok.lower()
    return None


def _plain_insert(sql: str) -> Optional[Tuple[str, Optional[List[str]], int]]:
    """``INSERT INTO rel [(cols)] VALUES ($1, .., $n)``: every value the bare
    placeholders $1..$n in order, nothing else (no ON CONFLICT, RETURNING,
    expressions, DEFAULT, several tuples, comments) -- the JDBC driver's
    bulk-loadable ``executeBatch`` shape."""
    m = _INSERT.match(sql)
    if not m:
        return None
    rel = m.group(1)
    if _ident(rel.split(".")[-1]) is None:
        return None
    vals = [int(v.strip()[1:]) for v in m.group(3).split(",")]
    if vals != list(range(1, len(vals) + 1)):
        return None
    cols: Optional[List[str]] = None
    if m.group(2) is not None:
        cols = []
        for t in m.group(2).split(","):
            c = _ident(t)
            if c is None:
                return None
            cols.append(c)
        if len(cols) != len(vals) or len(set(cols)) != len(cols):
            return None
    return rel.strip(), cols, len(vals)


def shape(sql: str) -> Shape:
    """The (cached) :class:`Shape` of a statement text."""
    sh = _SHAPES.get(sql)
    if sh is not None:
        return sh
    spans, plain = _scan(sql)
    kept = [(a, b) for a, b in spans if _first_word(sql, plain, a, b) or _meaningful(sql[a:b])]
    statements = [sql[a:b] for a, b in kept] if len(kept) > 1 else None
    verbs = [_first_word(sql, plain, a, b) for a, b in kept] or [""]
    verb = verbs[0]
    returning = any(_RETURNING.search(sql, x, y) for x, y in plain)
    if verb == "WITH":
        tail = " ".join(sql[x:y] for x, y in plain).upper()
        for w in ("INSERT", "UPDATE", "DELETE", "MERGE"):
            if re.search(r"(?<![A-Z0-9_$])" + w + r"(?![A-Z0-9_$])", tail):
                verb = w
                break
    txn = any(v in _TXN_VERBS for v in verbs)
    ins = _plain_insert(sql) if verb == "INSERT" and statements is None and not returning else None
    sh = Shape(statements, verb, returning, txn, ins)
    if len(_SHAPES) >= _CACHE_MAX:
        _SHAPES.clear()
    _SHAPES[sql] = sh
    return sh
