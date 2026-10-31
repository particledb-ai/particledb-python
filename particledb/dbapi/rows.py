"""Row factories (psycopg's protocol): ``cursor.row_factory = dict_row``.

A row factory is called with the cursor once per result and returns the
function that turns one row (a tuple, columns in projection order) into what
``fetch*()`` hands back. ``tuple_row`` (the default) returns the tuple itself,
with no per-row call.
"""

from __future__ import annotations

from collections import namedtuple
from functools import lru_cache
from typing import Any, Callable, Optional, Sequence, Tuple

RowMaker = Optional[Callable[[Tuple[Any, ...]], Any]]


def _names(cursor: Any) -> Sequence[str]:
    d = cursor.description
    return [c[0] for c in d] if d else []


def tuple_row(cursor: Any) -> RowMaker:
    """Rows as tuples (the default): no conversion at all."""
    return None


def dict_row(cursor: Any) -> RowMaker:
    """Rows as ``{column: value}`` dicts (a repeated label keeps the LAST
    column, as ``dict(zip(...))`` does; use tuples to keep every column)."""
    names = _names(cursor)
    return lambda r: dict(zip(names, r))


@lru_cache(maxsize=512)
def _nt(names: Tuple[str, ...]) -> Any:
    return namedtuple("Row", names, rename=True)


def namedtuple_row(cursor: Any) -> RowMaker:
    """Rows as named tuples (invalid or repeated names are renamed ``_0``, ``_1`` ...)."""
    cls = _nt(tuple(_names(cursor)))
    return cls._make


def class_row(cls: Callable[..., Any]) -> Callable[[Any], RowMaker]:
    """Rows as ``cls(**{column: value})`` (a dataclass, a pydantic model)."""

    def factory(cursor: Any) -> RowMaker:
        names = _names(cursor)
        return lambda r: cls(**dict(zip(names, r)))

    return factory


def args_row(func: Callable[..., Any]) -> Callable[[Any], RowMaker]:
    """Rows as ``func(*values)``."""

    def factory(cursor: Any) -> RowMaker:
        return lambda r: func(*r)

    return factory


def kwargs_row(func: Callable[..., Any]) -> Callable[[Any], RowMaker]:
    """Rows as ``func(**{column: value})``."""

    def factory(cursor: Any) -> RowMaker:
        names = _names(cursor)
        return lambda r: func(**dict(zip(names, r)))

    return factory


def scalar_row(cursor: Any) -> RowMaker:
    """The first column's value of each row."""
    return lambda r: r[0]


__all__ = ["tuple_row", "dict_row", "namedtuple_row", "class_row", "args_row", "kwargs_row",
           "scalar_row"]
