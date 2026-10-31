"""Inputs of ``bulk_insert`` -> one native bulk ingest (``F_BULK``).

Accepted ``data``:

* an iterable of tuples / lists (a generator streams: it is encoded and sent
  one chunk at a time) or of dicts (``columns`` = the first dict's keys);
* a pyarrow ``Table`` / ``RecordBatch`` (sent as Arrow IPC as it is, after
  casting the few Arrow types the server's ingest does not read: string /
  binary views, ``date64``, fixed-size binary, dictionaries, null);
* a pandas DataFrame (through pyarrow when installed, NaN -> NULL; else row
  by row) and a polars DataFrame (its own IPC writer: no pyarrow needed).

Rows go through the built-in Arrow writer (``arrow_ipc``): pyarrow is never
required.
"""

from __future__ import annotations

import io
import math
from typing import Any, Callable, Iterable, Iterator, List, Optional, Sequence

#: Target bytes per DATA frame for Arrow inputs (the frame cap is 16 MiB).
TARGET_CHUNK_BYTES = 8 * 1024 * 1024


def _kind(data: Any) -> str:
    mod = type(data).__module__ or ""
    name = type(data).__name__
    if mod.startswith("pyarrow") and name in ("Table", "RecordBatch"):
        return "arrow"
    if mod.startswith("pandas") and name == "DataFrame":
        return "pandas"
    if mod.startswith("polars") and name == "DataFrame":
        return "polars"
    return "rows"


def _arrow_ok(pa: Any, t: Any) -> Any:
    """Cast the columns the server's ingest cannot read to ones it can."""
    import pyarrow.types as pt  # type: ignore

    cols = []
    changed = False
    for f, col in zip(t.schema, t.columns):
        ty = f.type
        new = None
        if pt.is_dictionary(ty):
            new = col.cast(ty.value_type)
            ty = ty.value_type
        target = None
        if pt.is_string_view(ty) if hasattr(pt, "is_string_view") else False:
            target = pa.large_string()
        elif pt.is_binary_view(ty) if hasattr(pt, "is_binary_view") else False:
            target = pa.large_binary()
        elif pt.is_date64(ty):
            target = pa.date32()
        elif pt.is_fixed_size_binary(ty):
            target = pa.binary()
        elif pt.is_null(ty):
            target = pa.string()
        if target is not None:
            new = (new if new is not None else col).cast(target)
        if new is not None:
            changed = True
            cols.append(new)
        else:
            cols.append(col)
    if not changed:
        return t
    return pa.Table.from_arrays(cols, names=t.schema.names)


def _ipc_chunks(pa: Any, table: Any) -> Iterator[bytes]:
    n = table.num_rows
    if n == 0:
        return
    per_row = max(1, table.nbytes // n)
    step = max(1, min(n, TARGET_CHUNK_BYTES // per_row))
    for batch in table.to_batches(max_chunksize=step):
        yield from _batch_ipc(pa, batch)


def _batch_ipc(pa: Any, batch: Any) -> Iterator[bytes]:
    from . import _protocol as P

    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, batch.schema) as w:
        w.write_batch(batch)
    b = sink.getvalue()
    if len(b) + 16 <= P.MAX_PAYLOAD or batch.num_rows <= 1:
        yield b
        return
    half = batch.num_rows // 2
    yield from _batch_ipc(pa, batch.slice(0, half))
    yield from _batch_ipc(pa, batch.slice(half))


def _pandas_rows(df: Any) -> Iterator[tuple]:
    for row in df.itertuples(index=False, name=None):
        yield tuple(None if (isinstance(v, float) and math.isnan(v)) or _is_na(v) else v for v in row)


def _is_na(v: Any) -> bool:
    n = type(v).__name__
    return n in ("NaTType", "NAType")


class Plan:
    """What one ``bulk_insert`` sends: ``mode`` ``"ipc"`` (``payload`` =
    Arrow IPC chunks, ``expected`` rows) or ``"rows"`` (``payload`` = row
    tuples); ``columns`` ``None`` means the table's first ``width`` columns."""

    __slots__ = ("mode", "columns", "payload", "expected", "width")

    def __init__(self, mode: str, columns: Optional[List[str]], payload: Any, expected: int = -1,
                 width: int = 0):
        self.mode = mode
        self.columns = columns
        self.payload = payload
        self.expected = expected
        self.width = width


def plan(data: Any, columns: Optional[Sequence[str]]) -> Optional[Plan]:
    """The :class:`Plan` for ``data``; ``None`` when there is nothing to load."""
    from . import _arrow

    kind = _kind(data)
    if kind == "pandas" and _arrow.HAVE_PYARROW:
        pa, _ = _arrow.pyarrow_modules()
        cols = list(columns) if columns is not None else [str(c) for c in data.columns]
        data = pa.Table.from_pandas(data, preserve_index=False)
        kind = "arrow"
        columns = cols
    if kind == "polars":
        cols = list(columns) if columns is not None else list(data.columns)
        if _arrow.HAVE_PYARROW:
            data = data.to_arrow()
            kind = "arrow"
            columns = cols
        else:
            import polars as pl  # type: ignore

            if data.height == 0:
                return None
            buf = io.BytesIO()
            data.write_ipc_stream(buf, compat_level=pl.CompatLevel.oldest())
            return Plan("ipc", cols, [buf.getvalue()], data.height)
    if kind == "arrow":
        pa, _ = _arrow.pyarrow_modules()
        if type(data).__name__ == "RecordBatch":
            data = pa.Table.from_batches([data])
        cols = list(columns) if columns is not None else list(data.column_names)
        if len(cols) != data.num_columns:
            raise ValueError(f"{len(cols)} column names for {data.num_columns} Arrow columns")
        if data.num_rows == 0:
            return None
        data = _arrow_ok(pa, data.rename_columns(cols))
        return Plan("ipc", cols, _ipc_chunks(pa, data), data.num_rows)
    if kind == "pandas":
        cols = list(columns) if columns is not None else [str(c) for c in data.columns]
        return Plan("rows", cols, _pandas_rows(data))
    it = iter(data)
    first = next(it, None)
    if first is None:
        return None
    if isinstance(first, dict):
        cols = list(columns) if columns is not None else list(first.keys())

        def dict_rows() -> Iterator[tuple]:
            yield tuple(first.get(c) for c in cols)
            for d in it:
                yield tuple(d.get(c) for c in cols)

        return Plan("rows", cols, dict_rows())
    if isinstance(data, list):
        rows: Iterable[Sequence[Any]] = data
    else:
        def tuple_rows() -> Iterator[Sequence[Any]]:
            yield first
            yield from it

        rows = tuple_rows()
    return Plan("rows", list(columns) if columns is not None else None, rows, -1, len(first))


def load(native: Any, table: str, data: Any, columns: Optional[Sequence[str]],
         table_columns: Callable[[str], List[str]], timeout: Optional[float] = None) -> int:
    """Run one bulk ingest of ``data`` into ``table`` on ``native`` (a
    :class:`particledb.Connection`); returns the rows loaded."""
    p = plan(data, columns)
    if p is None:
        return 0
    cols = p.columns if p.columns is not None else table_columns(table)[: p.width]
    if p.mode == "rows":
        return native.bulk_insert(table, cols, p.payload, timeout)
    loaded = native.bulk_load_ipc(table, cols, p.payload, timeout)
    if p.expected >= 0 and loaded != p.expected:
        from .errors import ProtocolError

        raise ProtocolError(f"bulk ingest loaded {loaded} of {p.expected} rows")
    return loaded
