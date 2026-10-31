"""asyncio connection pool that uses the multiplexing.

Two ways to use it:

* ``await pool.execute(sql, params)`` / ``fetch`` / ``fetchrow`` / ``fetchval``
  -- SHARED: the statement runs in autocommit on the least-busy pooled
  connection, alongside other coroutines' statements on the same socket (each
  is its own stream). Many coroutines need few connections.
* ``async with pool.connection() as conn:`` -- EXCLUSIVE: a connection for a
  transaction (the pool's ``autocommit`` setting, default False). It is lent
  only once no shared statement is running on it, and on return an open
  transaction is rolled back.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any, AsyncIterator, List, Optional

from ..dbapi import errors as E
from ._connection import AsyncConnection, connect


class _Slot:
    __slots__ = ("conn", "leased", "inflight")

    def __init__(self, conn: AsyncConnection):
        self.conn = conn
        self.leased = False
        self.inflight = 0


class Pool:
    """Args: ``dsn`` / keyword options as :func:`particledb.aio.connect`;
    ``min_size`` connections opened by :meth:`open`; at most ``max_size``;
    ``max_shared_streams``: shared statements per connection before the pool
    opens another one (when below ``max_size``); ``autocommit`` for leased
    connections."""

    def __init__(self, dsn: Optional[str] = None, *, min_size: int = 1, max_size: int = 4,
                 max_shared_streams: int = 64, autocommit: bool = False, **kwargs: Any):
        if min_size < 0 or max_size < 1 or min_size > max_size:
            raise ValueError("need 0 <= min_size <= max_size and max_size >= 1")
        self._dsn = dsn
        self._kw = kwargs
        self.min_size = min_size
        self.max_size = max_size
        self.max_shared_streams = max_shared_streams
        self.autocommit = autocommit
        self._slots: List[_Slot] = []
        self._opening = 0
        self._cond: Optional[asyncio.Condition] = None
        self._closed = False

    async def open(self) -> "Pool":
        self._cond = asyncio.Condition()
        conns = await asyncio.gather(*(self._connect() for _ in range(self.min_size)))
        self._slots.extend(_Slot(c) for c in conns)
        return self

    async def _connect(self) -> AsyncConnection:
        return await connect(self._dsn, autocommit=True, **self._kw)

    async def __aenter__(self) -> "Pool":
        if self._cond is None:
            await self.open()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    @property
    def size(self) -> int:
        return len(self._slots)

    def _alive(self) -> None:
        if self._closed:
            raise E.InterfaceError("the pool is closed")
        if self._cond is None:
            raise E.InterfaceError("the pool is not open: await pool.open() or use 'async with'")

    def _drop_dead(self) -> None:
        self._slots = [s for s in self._slots if not s.conn.closed or s.leased or s.inflight]

    async def _grow(self) -> Optional[_Slot]:
        if len(self._slots) + self._opening >= self.max_size:
            return None
        self._opening += 1
        try:
            conn = await self._connect()
        finally:
            self._opening -= 1
        slot = _Slot(conn)
        self._slots.append(slot)
        return slot

    async def _acquire(self, exclusive: bool, timeout: Optional[float]) -> _Slot:
        self._alive()
        assert self._cond is not None
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        async with self._cond:
            while True:
                self._drop_dead()
                free = [s for s in self._slots if not s.leased and not s.conn.closed]
                if exclusive:
                    idle = [s for s in free if s.inflight == 0]
                    if idle:
                        slot = idle[0]
                        slot.leased = True
                        return slot
                else:
                    if free:
                        slot = min(free, key=lambda s: s.inflight)
                        if slot.inflight < self.max_shared_streams or len(self._slots) >= self.max_size:
                            slot.inflight += 1
                            return slot
                grown = await self._grow()
                if grown is not None:
                    if exclusive:
                        grown.leased = True
                    else:
                        grown.inflight += 1
                    return grown
                left = None if deadline is None else deadline - loop.time()
                if left is not None and left <= 0:
                    raise E.OperationalError("no pooled connection became available in time",
                                             sqlstate="53300")
                try:
                    await asyncio.wait_for(self._cond.wait(), left)
                except asyncio.TimeoutError:
                    pass

    async def _release(self, slot: _Slot, exclusive: bool) -> None:
        assert self._cond is not None
        async with self._cond:
            if exclusive:
                slot.leased = False
            else:
                slot.inflight -= 1
            self._cond.notify_all()

    @contextlib.asynccontextmanager
    async def connection(self, timeout: Optional[float] = None) -> AsyncIterator[AsyncConnection]:
        """Lease a connection exclusively (for a transaction)."""
        slot = await self._acquire(True, timeout)
        conn = slot.conn
        try:
            conn._autocommit = self.autocommit
            yield conn
            if not conn.closed and conn.in_transaction:
                if self.autocommit:
                    await conn.rollback()
                else:
                    await conn.commit()
        except BaseException:
            if not conn.closed and conn.in_transaction:
                try:
                    await conn.rollback()
                except E.Error:
                    pass
            raise
        finally:
            if not conn.closed:
                conn._autocommit = True
            await self._release(slot, True)

    async def execute(self, query: str, params: Any = None) -> Any:
        """Run one autocommit statement on a shared connection; returns the
        executed cursor (its result is already received)."""
        slot = await self._acquire(False, None)
        try:
            return await slot.conn.execute(query, params)
        finally:
            await self._release(slot, False)

    async def fetch(self, query: str, params: Any = None) -> List[Any]:
        cur = await self.execute(query, params)
        return await cur.fetchall()

    async def fetchrow(self, query: str, params: Any = None) -> Any:
        cur = await self.execute(query, params)
        return await cur.fetchone()

    async def fetchval(self, query: str, params: Any = None) -> Any:
        row = await self.fetchrow(query, params)
        return None if row is None else row[0]

    async def executemany(self, query: str, params_seq: Any) -> int:
        slot = await self._acquire(False, None)
        try:
            cur = slot.conn.cursor()
            await cur.executemany(query, params_seq)
            return cur.rowcount
        finally:
            await self._release(slot, False)

    async def close(self) -> None:
        self._closed = True
        for s in self._slots:
            await s.conn.close()
        self._slots = []
