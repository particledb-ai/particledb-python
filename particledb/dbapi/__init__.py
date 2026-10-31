"""PEP 249 (DB-API 2.0) driver for ParticleDB on the native wire.

::

    from particledb import dbapi

    with dbapi.connect("pdb://localhost:7173") as conn:      # commit on success
        cur = conn.cursor()
        cur.execute("INSERT INTO users (id, name) VALUES (%s, %s)", (1, "alice"))
        cur.execute("SELECT * FROM users WHERE id = %(id)s", {"id": 1})
        print(cur.description, cur.fetchone())
        cur.executemany("INSERT INTO users VALUES (%s, %s)", rows)   # ONE bulk ingest
        df = conn.execute("SELECT * FROM users").fetch_df()        # pandas, no row objects

Everything is the native wire, protocol 2 (multiplexed): typed server-side
prepared statements for parameters, Arrow results decoded lazily. See
``SUPPORT.md`` for the feature matrix.

Module globals (PEP 249): ``apilevel = "2.0"``, ``threadsafety = 2`` (threads
may share a connection, not a cursor), ``paramstyle = "pyformat"`` (``%s`` and
``%(name)s`` like psycopg; ``?`` (qmark) and ``$1`` (numeric) are accepted too,
see :mod:`particledb.dbapi._sql`).
"""

from __future__ import annotations

from . import errors, rows
from ._adapt import register_adapter
from ._connection import Connection, IsolationLevel, Rollback, Transaction, connect, parse_dsn
from ._cursor import Cursor, ServerCursor
from ._types import (
    Binary,
    Column,
    Date,
    DateFromTicks,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
    BINARY_TYPE as BINARY,
    DATETIME_TYPE as DATETIME,
    NUMBER_TYPE as NUMBER,
    ROWID_TYPE as ROWID,
    STRING_TYPE as STRING,
)
from .errors import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)
from .._protocol import Float4, Int2, Int4, Json, Jsonb

apilevel = "2.0"
threadsafety = 2
paramstyle = "pyformat"

__all__ = [
    "apilevel", "threadsafety", "paramstyle", "connect", "parse_dsn", "Connection", "Cursor",
    "ServerCursor", "Transaction", "Rollback", "IsolationLevel", "Column", "register_adapter",
    "Warning", "Error", "InterfaceError", "DatabaseError", "DataError", "OperationalError",
    "IntegrityError", "InternalError", "ProgrammingError", "NotSupportedError",
    "Date", "Time", "Timestamp", "DateFromTicks", "TimeFromTicks", "TimestampFromTicks", "Binary",
    "STRING", "BINARY", "NUMBER", "DATETIME", "ROWID", "Int2", "Int4", "Float4", "Json", "Jsonb",
    "errors", "rows",
]
