"""One statement's result as a cursor holds it, and the fetch logic shared by
the blocking and the asyncio cursors.

A row-returning result keeps the server's Arrow IPC bytes and decodes them to
Python rows only when a row is first fetched; ``fetch_arrow()`` /
``fetch_df()`` / ``fetch_polars()`` hand the bytes to pyarrow / polars without
building a Python row.
"""

from __future__ import annotations

from typing import Any, Callable, List, Optional, Sequence, Tuple

from .. import _arrow, _frames
from .. import _protocol as P
from . import _types
from .errors import InterfaceError, ProgrammingError, translate


class ResultSet:
    __slots__ = ("raw", "rows", "fields", "count", "notices", "rolled_back", "verb", "extended",
                 "use_pa", "has_rows")

    def __init__(self, raw: Optional[bytes], fields: List[Any], count: int, notices: List[Any],
                 rolled_back: bool, verb: str, extended: bool, use_pa: bool, has_rows: bool):
        self.raw = raw            # Arrow IPC stream, until decoded
        self.rows: Optional[List[Tuple[Any, ...]]] = None
        self.fields = fields
        self.count = count        # affected rows (-1: count the result's rows)
        self.notices = notices
        self.rolled_back = rolled_back
        self.verb = verb
        self.extended = extended
        self.use_pa = use_pa
        self.has_rows = has_rows

    @classmethod
    def from_ipc(cls, data: bytes, notices: List[Any], rolled_back: bool, verb: str,
                 extended: bool, use_pa: bool) -> "ResultSet":
        try:
            fields = _arrow.read_schema(data).fields if data else []
        except _arrow.ArrowDecodeError as e:
            raise translate(_proto_error(e)) from e
        return cls(data, fields, -1, notices, rolled_back, verb, extended, use_pa, True)

    @classmethod
    def from_stream(cls, s: Any, verb: str, extended: bool, use_pa: bool) -> "ResultSet":
        rolled = bool(s.flags & P.FLAG_TXN_ROLLED_BACK)
        if s.kind == P.F_ARROW_RESULT:
            return cls.from_ipc(s.payload, s.notices, rolled, verb, extended, use_pa)
        if s.kind == P.F_ARROW_STREAM:
            data = b"".join(s.chunks)
            s.chunks = []
            return cls.from_ipc(data, s.notices, rolled, verb, extended, use_pa)
        if s.kind == P.F_OK:
            return cls(None, [], P.decode_ok(s.payload), s.notices, rolled, verb, extended, use_pa, False)
        if s.kind == P.F_BATCH_RESULT:
            br = P.decode_batch_result(s.payload)
            n = sum(c for c in br.counts if c > 0)
            return cls(None, [], n, s.notices, rolled, verb, extended, use_pa, False)
        raise translate(_proto_error(f"unexpected answer 0x{s.kind:02x} to a statement"))

    def materialize(self) -> List[Tuple[Any, ...]]:
        if self.rows is None:
            data = self.raw or b""
            self.raw = None
            try:
                _schema, self.rows = _arrow.decode_stream(data, self.use_pa, self.extended)
            except _arrow.ArrowDecodeError as e:
                raise translate(_proto_error(e)) from e
        return self.rows

    @property
    def rowcount(self) -> int:
        if not self.has_rows or self.count >= 0:
            return self.count
        if self.rows is not None:
            return len(self.rows)
        try:
            return _arrow.count_rows(self.raw or b"")
        except _arrow.ArrowDecodeError:
            return len(self.materialize())

    @property
    def description(self) -> Optional[List[_types.Column]]:
        return _types.description_for(self.fields) if self.has_rows else None


def _proto_error(e: Any) -> Exception:
    from ..errors import ProtocolError

    return ProtocolError(f"cannot decode the result: {e}")


class FetchMixin:
    """Client-side result navigation (no I/O). Cursors set ``_results``,
    ``_ri`` (current index), ``_pos`` and ``_maker`` through :meth:`_load`."""

    _results: List[ResultSet]
    _ri: int
    _pos: int
    _maker: Optional[Callable[[Tuple[Any, ...]], Any]]
    arraysize: int
    row_factory: Any
    _closed: bool

    def _reset_results(self) -> None:
        self._results = []
        self._ri = 0
        self._pos = 0
        self._maker = None

    def _load(self, results: List[ResultSet]) -> None:
        self._results = results
        self._ri = 0
        self._select(0)

    def _select(self, i: int) -> None:
        self._ri = i
        self._pos = 0
        self._maker = self.row_factory(self) if (self.row_factory is not None and self._cur_has_rows()) else None

    def _cur(self) -> Optional[ResultSet]:
        return self._results[self._ri] if self._ri < len(self._results) else None

    def _cur_has_rows(self) -> bool:
        r = self._cur()
        return r is not None and r.has_rows

    def _rows_or_raise(self) -> List[Tuple[Any, ...]]:
        if self._closed:
            raise InterfaceError("the cursor is closed")
        r = self._cur()
        if r is None:
            raise ProgrammingError("no results to fetch: execute a statement first")
        if not r.has_rows:
            raise ProgrammingError("the last operation didn't produce rows")
        return r.materialize()

    @property
    def description(self) -> Optional[List[_types.Column]]:
        r = self._cur()
        return r.description if r is not None else None

    @property
    def rowcount(self) -> int:
        r = self._cur()
        return r.rowcount if r is not None else -1

    @property
    def rownumber(self) -> Optional[int]:
        """Index of the next row to fetch in the current result (PEP 249 extension)."""
        r = self._cur()
        return self._pos if (r is not None and r.has_rows) else None

    @property
    def messages(self) -> List[Any]:
        """Server notices of the last execution (``Notice(severity, sqlstate, message)``)."""
        out: List[Any] = []
        for r in self._results:
            for n in r.notices:
                if n not in out:
                    out.append(n)
        return out

    def _fetchone(self) -> Any:
        rows = self._rows_or_raise()
        if self._pos >= len(rows):
            return None
        row = rows[self._pos]
        self._pos += 1
        return row if self._maker is None else self._maker(row)

    def _fetchmany(self, size: Optional[int]) -> List[Any]:
        rows = self._rows_or_raise()
        n = self.arraysize if size is None else size
        out = rows[self._pos: self._pos + n]
        self._pos += len(out)
        return out if self._maker is None else [self._maker(r) for r in out]

    def _fetchall(self) -> List[Any]:
        rows = self._rows_or_raise()
        out = rows[self._pos:] if self._pos else rows
        self._pos = len(rows)
        if self._maker is None:
            return out if out is not rows else list(rows)
        m = self._maker
        return [m(r) for r in out]

    def _nextset(self) -> Optional[bool]:
        if self._closed:
            raise InterfaceError("the cursor is closed")
        if self._ri + 1 < len(self._results):
            self._select(self._ri + 1)
            return True
        return None

    def _scroll(self, value: int, mode: str) -> None:
        rows = self._rows_or_raise()
        if mode == "relative":
            new = self._pos + value
        elif mode == "absolute":
            new = value
        else:
            raise ProgrammingError(f"bad scroll mode {mode!r}: 'relative' or 'absolute'")
        if not 0 <= new <= len(rows):
            raise IndexError("position out of bound")
        self._pos = new

    # ── DataFrames ──────────────────────────────────────────────────────

    def _frame_source(self) -> Tuple[Optional[bytes], Optional[List[Tuple[Any, ...]]], List[Any], bool]:
        """``(ipc bytes or None, remaining rows or None, fields, extended)``
        and the result marked consumed."""
        if self._closed:
            raise InterfaceError("the cursor is closed")
        r = self._cur()
        if r is None or not r.has_rows:
            raise ProgrammingError("the last operation didn't produce rows")
        if r.rows is None and self._pos == 0 and r.raw is not None:
            data = r.raw
            r.raw = None
            r.rows = []  # consumed: later fetches return nothing
            r.count = _arrow.count_rows(data) if data else 0
            self._pos = 0
            return data, None, r.fields, r.extended
        rows = r.materialize()
        rest = rows[self._pos:]
        self._pos = len(rows)
        return None, rest, r.fields, r.extended

    def fetch_arrow(self) -> Any:
        """The rest of the current result as a ``pyarrow.Table`` (needs pyarrow)."""
        data, rows, fields, ext = self._frame_source()
        try:
            if data is not None:
                return _frames.arrow_table(data, fields, ext)
            return _frames.arrow_from_rows(rows or [], fields)
        except Exception as e:
            raise translate(e) from e

    def fetch_df(self) -> Any:
        """The rest of the current result as a pandas ``DataFrame`` (needs
        pandas; through pyarrow when installed)."""
        data, rows, fields, ext = self._frame_source()
        try:
            return _frames.pandas_frame(data, rows, fields, ext)
        except Exception as e:
            raise translate(e) from e

    def fetch_polars(self) -> Any:
        """The rest of the current result as a polars ``DataFrame`` (needs
        polars only: polars reads the Arrow bytes itself)."""
        data, rows, fields, ext = self._frame_source()
        try:
            return _frames.polars_frame(data, rows, fields, ext)
        except Exception as e:
            raise translate(e) from e

    def fetch_numpy(self) -> Any:
        """The rest of the current result as ``{column: numpy.ndarray}``."""
        data, rows, fields, ext = self._frame_source()
        try:
            return _frames.numpy_arrays(data, rows, fields, ext)
        except Exception as e:
            raise translate(e) from e


def lastrowid_of(results: Sequence[ResultSet]) -> Optional[Any]:
    """PEP 249 ``lastrowid``: the first column of the last row an
    ``INSERT .. RETURNING`` returned (the server has no OIDs)."""
    for r in reversed(results):
        if r.verb == "INSERT" and r.has_rows:
            rows = r.materialize()
            if rows and rows[-1]:
                return rows[-1][0]
            return None
    return None
