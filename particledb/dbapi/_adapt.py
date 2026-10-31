"""Parameter adaptation: Python values the wire codec does not bind itself.

The native codec (``particledb._protocol.encode_param``) binds ``None``,
``bool``, ``int``, ``float``, ``Decimal``, ``str``, ``bytes`` / ``bytearray``
/ ``memoryview``, ``datetime`` (naive / aware), ``date``, ``time``, ``UUID``,
``list`` / ``tuple`` (arrays), ``dict`` (jsonb) and the explicit wrappers
(``Int2``, ``Int4``, ``Float4``, ``Json``, ``Jsonb``). Everything else goes
through here first:

* numpy scalars (``np.int64``, ``np.float32``, ``np.bool_``, ``np.datetime64``,
  ...) -> their Python value (``.item()``); pandas ``NaT`` / ``NA`` -> ``None``;
* ``timedelta`` -> interval text (``'<microseconds> microseconds'``);
* ``enum.Enum`` -> its ``value``;
* ``set`` / ``frozenset`` -> an array (sorted when the elements sort);
* anything registered with :func:`register_adapter`.

A value nothing adapts raises :class:`~particledb.dbapi.ProgrammingError`.
"""

from __future__ import annotations

import datetime as _dt
import decimal as _decimal
import enum as _enum
import uuid as _uuid
from typing import Any, Callable, Dict, List, Sequence, Type

from .. import _protocol as P
from .errors import ProgrammingError

#: Types the wire codec binds as they are (checked by exact type: the hot path).
NATIVE = frozenset({type(None), bool, int, float, str, bytes, bytearray, memoryview, _decimal.Decimal,
                    _dt.datetime, _dt.date, _dt.time, _uuid.UUID, list, tuple, dict,
                    P.Int2, P.Int4, P.Float4, P.Json, P.Jsonb})

_ADAPTERS: Dict[Type[Any], Callable[[Any], Any]] = {}


def register_adapter(cls: Type[Any], fn: Callable[[Any], Any]) -> None:
    """Bind instances of ``cls`` as ``fn(value)`` (which must return a value
    the codec binds, or another adaptable one). sqlite3's spelling."""
    _ADAPTERS[cls] = fn


def _timedelta(v: _dt.timedelta) -> str:
    us = (v.days * 86_400 + v.seconds) * 1_000_000 + v.microseconds
    return f"{us} microseconds"


def adapt(v: Any) -> Any:
    """``v`` as a value the codec binds (see the module docstring)."""
    t = type(v)
    if t in NATIVE:
        return v
    fn = _ADAPTERS.get(t)
    if fn is not None:
        return adapt(fn(v))
    for cls, f in _ADAPTERS.items():
        if isinstance(v, cls):
            return adapt(f(v))
    mod = t.__module__
    if mod == "numpy" or mod.startswith("numpy."):
        if t.__name__ == "ndarray":
            return [adapt(x) for x in v.tolist()]
        if t.__name__ == "datetime64":
            import numpy as np  # type: ignore

            if np.isnat(v):
                return None
            return v.astype("datetime64[us]").item()
        if t.__name__ == "timedelta64":
            import numpy as np  # type: ignore

            if np.isnat(v):
                return None
            return _timedelta(v.astype("timedelta64[us]").item())
        return v.item()
    if mod.startswith("pandas"):
        name = t.__name__
        if name in ("NaTType", "NAType"):
            return None
        if name == "Timedelta":
            return _timedelta(v.to_pytimedelta())
    if isinstance(v, _dt.timedelta):
        return _timedelta(v)
    if isinstance(v, (_dt.datetime, _dt.date, _dt.time, str, bytes, int, float, _decimal.Decimal,
                      _uuid.UUID, list, tuple, dict)):
        return v  # a subclass (pandas.Timestamp is a datetime)
    if isinstance(v, _enum.Enum):
        return adapt(v.value)
    if isinstance(v, (set, frozenset)):
        items = list(v)
        try:
            items.sort()
        except TypeError:
            pass
        return [adapt(x) for x in items]
    raise ProgrammingError(f"cannot adapt type {t.__name__!r} for a query parameter "
                           f"(register one with particledb.dbapi.register_adapter)")


def adapt_all(params: Sequence[Any]) -> List[Any]:
    """Every parameter adapted; the list itself when nothing needed it."""
    for v in params:
        if type(v) not in NATIVE:
            break
    else:
        return params if isinstance(params, list) else list(params)
    return [v if type(v) in NATIVE else adapt(v) for v in params]
