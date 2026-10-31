"""Vector similarity search operations for ParticleDB.

Provides methods for nearest-neighbor search, vector insertion, and index
management via the REST API.

No caller value is spliced into SQL unescaped: the vector is validated
(real, finite numbers only) and sent as a bound parameter; table, column and
index names are quoted with ``ops._quote_ident``; ``create_index`` option
names must be plain identifiers and string option values become one
``ops._escape_literal`` literal.  ``filter`` is the one raw-SQL argument, by
design: never build it from untrusted input.

Example::

    from particledb import ParticleDB

    db = ParticleDB()
    results = db.vector.search("documents", "embedding", [0.1, 0.2, 0.3], k=5)
    for r in results:
        print(r["row"]["title"], "distance:", r["distance"])
"""

from __future__ import annotations

import decimal
import math
import numbers
import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

from particledb.ops import _escape_literal, _quote_ident

if TYPE_CHECKING:
    from particledb.client import ParticleDB


class Vector:
    """Vector similarity search operations backed by ParticleDB SQL.

    Typically accessed via ``ParticleDB.vector`` rather than instantiated
    directly.
    """

    def __init__(self, client: "ParticleDB"):
        self._client = client

    def search(
        self,
        table: str,
        column: str,
        query_vector: Sequence[float],
        *,
        k: int = 10,
        columns: Optional[List[str]] = None,
        filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Perform a vector similarity search.

        Parameters
        ----------
        table : str
            Table containing the vectors.
        column : str
            Column storing the vector data.
        query_vector : list of float
            The query vector.
        k : int
            Number of nearest neighbors to return.  Default: ``10``.
        columns : list of str, optional
            Column names to include in results (``"*"`` allowed).  Default:
            all (``*``).  Expressions and aliases are refused
            (``ValueError``): use ``filter`` or ``client.query`` with raw SQL.
        filter : str, optional
            SQL ``WHERE`` clause (without the ``WHERE`` keyword) to pre-filter.
            Spliced verbatim: it is SQL, so never build it from untrusted
            input.

        Returns
        -------
        list of dict
            Each dict has ``"row"`` (column values) and ``"distance"`` (float).
        """
        vec_text = _vector_text(query_vector)
        col_list = ", ".join(_result_column(c) for c in columns) if columns else "*"

        # L2 distance via the `<->` operator (the server has no
        # VECTOR_DISTANCE / DISTANCE function; `<->` is what an HNSW index
        # accelerates).  The vector is bound ($1), never part of the text.
        dist = f"({_quote_ident(column)} <-> VECTOR($1))"
        sql = f"SELECT {col_list}, {dist} AS __distance FROM {_table_name(table)}"
        if filter:
            sql += f" WHERE {filter}"
        sql += f" ORDER BY {dist} LIMIT {int(k)}"

        rows = self._client.query(sql, [vec_text])
        results: List[Dict[str, Any]] = []
        for row in rows:
            distance = float(row.pop("__distance", 0.0))
            results.append({"row": row, "distance": distance})
        return results

    def insert(
        self,
        table: str,
        data: Dict[str, Any],
        vector_column: str,
        vector: Sequence[float],
    ) -> None:
        """Insert a row with a vector value.

        Parameters
        ----------
        table : str
            Target table name.
        data : dict
            Non-vector column values as key-value pairs.
        vector_column : str
            Name of the vector column.
        vector : list of float
            The vector value to insert.

        Example
        -------
        >>> db.vector.insert(
        ...     "documents",
        ...     {"id": 1, "title": "My Doc"},
        ...     "embedding",
        ...     [0.1, 0.2, 0.3],
        ... )
        """
        all_data = dict(data)
        all_data[vector_column] = _vector_text(vector)

        col_names = list(all_data.keys())
        placeholders = ", ".join(f"${i + 1}" for i in range(len(col_names)))
        col_list = ", ".join(_quote_ident(c) for c in col_names)

        sql = f"INSERT INTO {_table_name(table)} ({col_list}) VALUES ({placeholders})"
        self._client.execute(sql, list(all_data.values()))

    def create_index(
        self,
        table: str,
        column: str,
        index_type: str = "hnsw",
        name: Optional[str] = None,
        **params: Any,
    ) -> None:
        """Create a vector index on a column.

        Parameters
        ----------
        table : str
            Table name.
        column : str
            Vector column name.
        index_type : str
            Index algorithm (``"hnsw"`` or ``"ivfflat"``), a plain identifier.
            Default: ``"hnsw"``.
        name : str, optional
            Index name.  Default: ``idx_<table>_<column>_<index_type>``.
        **params
            Additional index parameters (e.g. ``m=16``, ``ef_construction=200``).
            Names must be plain identifiers; values ``str`` (one quoted
            literal), ``int`` or finite ``float``.
        """
        if not isinstance(index_type, str) or not _PLAIN_IDENT.match(index_type):
            raise ValueError(f"index_type must be a plain identifier, got {index_type!r}")
        index_name = name or f"idx_{table.rsplit('.', 1)[-1]}_{column}_{index_type}"
        sql = (
            f"CREATE INDEX {_quote_ident(index_name)} ON {_table_name(table)} "
            f"USING {index_type} ({_quote_ident(column)})"
        )
        if params:
            parts = [f"{_option_name(k)} = {_option_value(k, v)}" for k, v in params.items()]
            sql += " WITH (" + ", ".join(parts) + ")"

        self._client.execute(sql)

    def __repr__(self) -> str:
        return f"Vector(client={self._client!r})"


_PLAIN_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_COLUMN_NAME = re.compile(r"[^\W\d]\w*\Z")


def _table_name(table: str) -> str:
    """``table`` or ``schema.table``: each dot-separated part quoted on its own."""
    parts = table.split(".")
    if any(p == "" for p in parts):
        raise ValueError(f"table name has an empty part: {table!r}")
    return ".".join(_quote_ident(p) for p in parts)


def _result_column(c: str) -> str:
    """A ``search(columns=)`` entry: ``"*"`` as is, else a bare column name, quoted."""
    if c == "*":
        return "*"
    if not isinstance(c, str) or not _COLUMN_NAME.match(c):
        raise ValueError(
            f"search(columns=) takes column names or '*', got {c!r}; for expressions "
            "or aliases use filter= or client.query() with raw SQL"
        )
    return _quote_ident(c)


def _vector_text(vec: Sequence[float]) -> str:
    """The ``[x,y,...]`` text of a vector, every element a real, finite number.

    The text is always sent as a bound parameter; validating here also stops a
    string element (``"1,2"``) from changing the vector's dimensions.  ``bool``
    and values neither ``numbers.Real`` nor ``Decimal`` (str, bytes, nested sequences, complex,
    None) raise ``TypeError``; NaN and infinities raise ``ValueError``.
    """
    if isinstance(vec, (str, bytes, bytearray)):
        raise TypeError("a vector must be a sequence of numbers, not a string")
    out = []
    for v in vec:
        if isinstance(v, bool) or not isinstance(v, (numbers.Real, decimal.Decimal)):
            raise TypeError(f"vector elements must be real numbers, got {type(v).__name__}")
        f = float(v)
        if not math.isfinite(f):
            raise ValueError(f"vector elements must be finite, got {f!r}")
        out.append(repr(f))
    return "[" + ",".join(out) + "]"


def _option_name(k: str) -> str:
    if not _PLAIN_IDENT.match(k):
        raise ValueError(f"index option name must be a plain identifier, got {k!r}")
    return k


def _option_value(k: str, v: Any) -> str:
    """A ``WITH (...)`` option value: ``str`` -> one quoted literal, numbers re-rendered."""
    if isinstance(v, str):
        return "'" + _escape_literal(v) + "'"
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        raise TypeError(f"index option {k!r} must be str, int or float, got {type(v).__name__}")
    if isinstance(v, int):
        return str(int(v))
    f = float(v)
    if not math.isfinite(f):
        raise ValueError(f"index option {k!r} must be finite, got {f!r}")
    return repr(f)
