"""Results as Arrow tables, pandas / polars DataFrames and numpy arrays.

Built on the native result: the server answers every row-returning statement
with an Arrow IPC stream, and these functions hand those bytes to pyarrow
(``pa.ipc.open_stream``) or polars (``pl.read_ipc_stream``, no pyarrow
needed) as they are -- no Python row is built. They then apply the same
ParticleDB-level corrections the row decoder applies, column by column:

* ``timestamptz`` (a zone-less Arrow timestamp holding the UTC instant) ->
  timestamp with zone ``UTC``;
* ``SMALLINT`` / ``INT`` (carried as int64) -> int16 / int32;
* unconstrained ``numeric`` (per-value-scale text) -> decimal128 at the
  column's largest scale (text when a value does not fit 38 digits or is
  NaN / Infinity);
* ``uuid``, ``vector``, arrays carried as text, ``json`` / ``jsonb``,
  ``interval`` -> the Python value's Arrow equivalent (uuid as its text, json
  as its text, vector as ``list<float32>``, interval as ``duration[us]``).

pyarrow, pandas, polars and numpy are all optional and imported on first use.
When pyarrow is missing, :func:`pandas_frame` and :func:`numpy_arrays` build
from the decoded Python rows instead, and :func:`polars_frame` uses polars'
own IPC reader.
"""

from __future__ import annotations

import decimal as _decimal
import io
from typing import Any, Dict, List, Optional, Sequence, Tuple

from . import _arrow
from .errors import QueryError

_INT_DECL = {"smallint": 16, "int2": 16, "int": 32, "integer": 32, "int4": 32,
             "serial": 32, "smallserial": 16, "serial4": 32, "serial2": 16}


def _need(mod: str, what: str) -> Any:
    try:
        return __import__(mod)
    except ImportError:
        raise QueryError(f"{what} needs {mod}: pip install {mod}", 0, "0A000") from None


def _is_tstz(f: "_arrow.Field") -> bool:
    d = (f.decl or "").strip().lower()
    return f.kind == 10 and not f.params.get("tz") and (
        d in ("timestamptz", "timestamp with time zone") or "pdb:tz" in f.metadata)


def _int_bits(f: "_arrow.Field") -> Optional[int]:
    if f.kind != 2:
        return None
    return _INT_DECL.get((f.decl or "").strip().lower())


def _numeric_text(f: "_arrow.Field") -> bool:
    d = (f.decl or "").strip().lower()
    return f.kind in (5, 20) and d in ("numeric", "decimal", "dec")


def _decimal_array(pa: Any, values: List[Any]) -> Any:
    ds = [None if v is None else (v if isinstance(v, _decimal.Decimal) else _decimal.Decimal(v))
          for v in values]
    scale = 0
    digits = 0
    for d in ds:
        if d is None:
            continue
        if not d.is_finite():
            return pa.array([None if v is None else str(v) for v in values], pa.string())
        sign, dg, exp = d.as_tuple()
        s = -exp if exp < 0 else 0
        scale = max(scale, s)
        digits = max(digits, len(dg) + (exp if exp > 0 else 0) - s)
    if digits + scale > 38:
        return pa.array([None if v is None else str(v) for v in values], pa.string())
    return pa.array(ds, pa.decimal128(38, scale))


def _python_converted(f: "_arrow.Field", extended: bool) -> Optional[Any]:
    """The row decoder's converter for ``f`` when the Arrow column needs one
    (other than the ones fixed natively above)."""
    if _is_tstz(f) or _numeric_text(f):
        return None
    return _arrow.converter_for(f, extended)


def _fix_arrow(pa: Any, table: Any, fields: Sequence["_arrow.Field"], extended: bool) -> Any:
    import pyarrow.compute as pc  # type: ignore

    cols = list(table.columns)
    changed = False
    for i, f in enumerate(fields):
        col = cols[i]
        new = None
        if _is_tstz(f):
            new = pc.assume_timezone(col, "UTC")
        elif _int_bits(f):
            new = col.cast(pa.int16() if _int_bits(f) == 16 else pa.int32())
        elif _numeric_text(f):
            new = _decimal_array(pa, col.to_pylist())
        else:
            conv = _python_converted(f, extended)
            if conv is not None:
                vals = [conv(v) for v in col.to_pylist()]
                if _arrow.is_vector_decl((f.decl or "").strip().lower()):
                    new = pa.array(vals, pa.list_(pa.float32()))
                elif extended and (f.decl or "").strip().lower() in ("json", "jsonb"):
                    new = None  # json stays its text in Arrow
                elif any(isinstance(v, str) for v in vals) and not all(
                        v is None or isinstance(v, str) for v in vals):
                    new = pa.array([None if v is None else str(v) for v in vals], pa.string())
                else:
                    try:
                        new = pa.array([str(v) if hasattr(v, "hex") and hasattr(v, "urn") else v
                                        for v in vals])
                    except (pa.ArrowInvalid, pa.ArrowTypeError):
                        new = pa.array([None if v is None else str(v) for v in vals], pa.string())
        if new is not None:
            cols[i] = new
            changed = True
    if not changed:
        return table
    names = [f.name for f in fields]
    return pa.Table.from_arrays([c if not hasattr(c, "combine_chunks") else c.combine_chunks()
                                 for c in cols], names=names)


def arrow_table(data: bytes, fields: Sequence["_arrow.Field"], extended: bool = False) -> Any:
    """The result's Arrow IPC stream as a corrected ``pyarrow.Table``."""
    _need("pyarrow", "an Arrow table")
    pa, pa_ipc = _arrow.pyarrow_modules()
    if not data:
        return pa.table({})
    table = pa_ipc.open_stream(data).read_all()
    return _fix_arrow(pa, table, fields, extended)


def arrow_from_rows(rows: List[Tuple[Any, ...]], fields: Sequence["_arrow.Field"]) -> Any:
    """Already-decoded Python rows as a ``pyarrow.Table`` (the rest of a result
    a caller started fetching row by row)."""
    _need("pyarrow", "an Arrow table")
    pa, _ = _arrow.pyarrow_modules()
    names = [f.name for f in fields]
    cols = list(zip(*rows)) if rows else [[] for _ in names]
    arrays = []
    for f, c in zip(fields, cols):
        c = list(c)
        if _numeric_text(f) or f.kind == 7:
            arrays.append(_decimal_array(pa, c))
            continue
        try:
            arrays.append(pa.array([str(v) if hasattr(v, "urn") else v for v in c]))
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            arrays.append(pa.array([None if v is None else str(v) for v in c], pa.string()))
    return pa.Table.from_arrays(arrays, names=names)


def pandas_frame(data: Optional[bytes], rows: Optional[List[Tuple[Any, ...]]],
                 fields: Sequence["_arrow.Field"], extended: bool = False) -> Any:
    """A pandas DataFrame: through pyarrow when installed (``data`` = the IPC
    stream), else from decoded ``rows``."""
    pd = _need("pandas", "a pandas DataFrame")
    if _arrow.HAVE_PYARROW:
        t = arrow_table(data, fields, extended) if data is not None else arrow_from_rows(rows or [], fields)
        return t.to_pandas()
    names = [f.name for f in fields]
    if rows is None:
        _, rows = _arrow.decode_stream(data or b"", False, extended)
    return pd.DataFrame.from_records(rows, columns=names)


def polars_frame(data: Optional[bytes], rows: Optional[List[Tuple[Any, ...]]],
                 fields: Sequence["_arrow.Field"], extended: bool = False) -> Any:
    """A polars DataFrame, read by polars' own IPC reader (no pyarrow needed)."""
    pl = _need("polars", "a polars DataFrame")
    names = [f.name for f in fields]
    if data is None:
        cols = list(zip(*rows)) if rows else [() for _ in names]
        return pl.DataFrame({n: list(c) for n, c in zip(names, cols)}, strict=False)
    if not data:
        return pl.DataFrame()
    df = pl.read_ipc_stream(io.BytesIO(data))
    exprs = []
    for i, f in enumerate(fields):
        n = df.columns[i]
        if _is_tstz(f):
            exprs.append(pl.col(n).dt.replace_time_zone("UTC"))
        elif _int_bits(f):
            exprs.append(pl.col(n).cast(pl.Int16 if _int_bits(f) == 16 else pl.Int32))
    if exprs:
        df = df.with_columns(exprs)
    for i, f in enumerate(fields):
        if _numeric_text(f):
            df = df.with_columns(_polars_decimal(pl, df.get_column(df.columns[i])))
    for i, f in enumerate(fields):
        conv = _python_converted(f, extended)
        if conv is None:
            continue
        d = (f.decl or "").strip().lower()
        if extended and d in ("json", "jsonb"):
            continue
        n = df.columns[i]
        vals = [conv(v) for v in df.get_column(n).to_list()]
        if _arrow.is_vector_decl(d):
            s = pl.Series(n, vals, dtype=pl.List(pl.Float32))
        else:
            vals = [str(v) if hasattr(v, "urn") else v for v in vals]
            s = pl.Series(n, vals, strict=False)
        df = df.with_columns(s)
    return df


def _polars_decimal(pl: Any, col: Any) -> Any:
    """Per-value-scale numeric text -> a polars Decimal at the largest scale
    (left as text when a value is NaN / Infinity or too wide)."""
    vals = col.to_list()
    scale = 0
    for v in vals:
        if v is None:
            continue
        if not v.replace(".", "", 1).lstrip("-").isdigit():
            return col
        if "." in v:
            scale = max(scale, len(v) - v.index(".") - 1)
    try:
        return col.str.to_decimal(scale=scale)
    except TypeError:  # older polars infers the scale itself
        return col.str.to_decimal()


def numpy_arrays(data: Optional[bytes], rows: Optional[List[Tuple[Any, ...]]],
                 fields: Sequence["_arrow.Field"], extended: bool = False) -> Dict[str, Any]:
    """``{column: numpy.ndarray}`` (DuckDB's ``fetchnumpy``): numeric columns
    without NULLs as their numpy dtype, others as object arrays (NULL ->
    ``None``; float columns keep NaN)."""
    np = _need("numpy", "numpy arrays")
    if _arrow.HAVE_PYARROW:
        t = arrow_table(data, fields, extended) if data is not None else arrow_from_rows(rows or [], fields)
        out: Dict[str, Any] = {}
        for name, col in zip(t.column_names, t.columns):
            try:
                out[name] = col.to_numpy()
            except Exception:
                out[name] = np.array(col.to_pylist(), dtype=object)
        return out
    if rows is None:
        _, rows = _arrow.decode_stream(data or b"", False, extended)
    names = [f.name for f in fields]
    cols = list(zip(*rows)) if rows else [() for _ in names]
    out = {}
    for n, c in zip(names, cols):
        if c and all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in c):
            out[n] = np.array(c)
        else:
            out[n] = np.array(list(c), dtype=object)
    return out
