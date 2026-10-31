"""asyncio client: the DB-API surface as coroutines, on the multiplexed wire.

::

    import particledb.aio as pdb

    conn = await pdb.connect("pdb://localhost:7173")
    cur = await conn.execute("SELECT * FROM users WHERE id = %s", [1])
    row = await cur.fetchone()
    async for row in await conn.execute("SELECT * FROM users"):
        ...
    async with conn.transaction():
        await conn.execute("UPDATE users SET name = %s WHERE id = %s", ["bob", 1])
    await conn.commit()

    # Many coroutines share ONE connection: each statement is its own stream.
    await asyncio.gather(*(conn.fetchval("SELECT $1::int", [i]) for i in range(100)))

    async with pdb.Pool("pdb://localhost:7173", max_size=4) as pool:
        await pool.fetch("SELECT 1")                 # shared, autocommit
        async with pool.connection() as c:           # exclusive, for a transaction
            await c.execute("INSERT ...")

Errors, type objects, row factories and ``IsolationLevel`` are the blocking
driver's (:mod:`particledb.dbapi`).
"""

from ..dbapi import errors, rows
from ..dbapi._connection import IsolationLevel, Rollback
from ..dbapi.errors import (
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
from ._connection import AsyncConnection, AsyncTransaction, connect
from ._cursor import AsyncCursor, AsyncServerCursor
from ._pool import Pool

__all__ = [
    "connect", "AsyncConnection", "AsyncCursor", "AsyncServerCursor", "AsyncTransaction", "Pool",
    "IsolationLevel", "Rollback", "errors", "rows", "Warning", "Error", "InterfaceError",
    "DatabaseError", "DataError", "OperationalError", "IntegrityError", "InternalError",
    "ProgrammingError", "NotSupportedError",
]
