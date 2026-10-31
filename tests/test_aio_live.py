"""LIVE: the asyncio driver (particledb.aio) against a running server.

Skipped unless PDB_NATIVE_PORT is set. Concurrency checks assert that many
coroutines share ONE connection (one socket, one session) and each gets its
own answer.
"""

import asyncio
import os
import unittest

import particledb.aio as pdb
from particledb.aio import errors

PORT = os.environ.get("PDB_NATIVE_PORT")
HOST = os.environ.get("PDB_NATIVE_HOST", "127.0.0.1")


def run(coro):
    return asyncio.run(coro)


async def aconnect(**kw):
    return await pdb.connect(host=HOST, port=int(PORT), **kw)


async def reset():
    c = await aconnect(autocommit=True)
    await c.execute("DROP TABLE IF EXISTS aio_t")
    await c.execute("CREATE TABLE aio_t (id INT PRIMARY KEY, v TEXT)")
    await c.close()


async def count(where="TRUE"):
    c = await aconnect(autocommit=True)
    try:
        return await c.fetchval(f"SELECT count(*) FROM aio_t WHERE {where}")
    finally:
        await c.close()


@unittest.skipUnless(PORT, "PDB_NATIVE_PORT not set")
class AioLive(unittest.TestCase):
    def setUp(self):
        run(reset())

    def test_execute_fetch_and_async_iteration(self):
        async def go():
            async with await aconnect() as conn:
                cur = await conn.execute("INSERT INTO aio_t VALUES (%s, %s), (%s, %s) RETURNING id", [1, "a", 2, "b"])
                self.assertEqual(cur.lastrowid, 2)
                self.assertEqual(conn.transaction_status, "intrans")
                await conn.commit()
                cur = await conn.execute("SELECT id, v FROM aio_t ORDER BY id")
                self.assertEqual([c.name for c in cur.description], ["id", "v"])
                self.assertEqual(cur.rowcount, 2)
                self.assertEqual([r async for r in cur], [(1, "a"), (2, "b")])
                cur = await conn.execute("SELECT id FROM aio_t ORDER BY id")
                self.assertEqual(await cur.fetchone(), (1,))
                self.assertEqual(await cur.fetchmany(5), [(2,)])
                self.assertEqual(await cur.fetchall(), [])
            self.assertEqual(await count(), 2)
        run(go())

    def test_many_coroutines_share_one_connection(self):
        async def go():
            conn = await aconnect(autocommit=True)
            try:
                vals = await asyncio.gather(*(conn.fetchval("SELECT $1::int * 3", [i]) for i in range(500)))
                self.assertEqual(vals, [i * 3 for i in range(500)])
                # a slow statement does not block fast ones on the same socket
                slow = asyncio.ensure_future(conn.fetchval("SELECT pg_sleep(0.5) IS NULL"))
                await asyncio.sleep(0.05)
                t0 = asyncio.get_running_loop().time()
                self.assertEqual(await conn.fetchval("SELECT 7"), 7)
                self.assertLess(asyncio.get_running_loop().time() - t0, 0.4)
                await slow
            finally:
                await conn.close()
        run(go())

    def test_transactions_and_savepoints(self):
        async def go():
            conn = await aconnect()
            try:
                await conn.execute("INSERT INTO aio_t VALUES (1, 'x')")
                await conn.rollback()
                self.assertEqual(await count(), 0)
                async with conn.transaction():
                    await conn.execute("INSERT INTO aio_t VALUES (1, 'x')")
                    with self.assertRaises(errors.UniqueViolation):
                        async with conn.transaction():
                            await conn.execute("INSERT INTO aio_t VALUES (2, 'y')")
                            await conn.execute("INSERT INTO aio_t VALUES (1, 'dup')")
                self.assertEqual(conn.transaction_status, "idle")
                self.assertEqual(await count(), 1)
                with self.assertRaises(pdb.IntegrityError):
                    await conn.execute("INSERT INTO aio_t VALUES (1, 'dup')")
                self.assertEqual(conn.transaction_status, "inerror")
                await conn.rollback()
                conn.isolation_level = pdb.IsolationLevel.SERIALIZABLE
                self.assertEqual(await conn.fetchval("SHOW transaction_isolation"), "serializable")
                await conn.rollback()
            finally:
                await conn.close()
        run(go())

    def test_task_cancel_cancels_the_statement(self):
        async def go():
            conn = await aconnect(autocommit=True)
            try:
                t = asyncio.ensure_future(conn.execute("SELECT pg_sleep(10)"))
                await asyncio.sleep(0.2)
                t0 = asyncio.get_running_loop().time()
                t.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await t
                self.assertEqual(await conn.fetchval("SELECT 1"), 1)
                self.assertLess(asyncio.get_running_loop().time() - t0, 5)
                with self.assertRaises(errors.QueryCanceled):
                    await _timeout_stmt(conn)
                self.assertEqual(await conn.fetchval("SELECT 2"), 2)
            finally:
                await conn.close()
        run(go())

    def test_cancel_during_prepare_does_not_wedge_the_statement(self):
        async def go():
            conn = await aconnect(autocommit=True)
            try:
                for _ in range(20):
                    sql = f"SELECT $1::int + {random_tag()}"
                    t = asyncio.ensure_future(conn.fetchval(sql, [1]))
                    await asyncio.sleep(0)
                    t.cancel()
                    try:
                        await t
                    except (asyncio.CancelledError, pdb.Error):
                        pass
                    self.assertIsNotNone(await asyncio.wait_for(conn.fetchval(sql, [1]), 10))
                self.assertEqual(await conn.fetchval("SELECT 9"), 9)
            finally:
                await conn.close()
        run(go())

    def test_exclusive_holds_after_a_slot_wait(self):
        """A task that waited for a stream slot re-checks the exclusive gate
        before it registers (it must not slip into another task's hidden
        transaction)."""
        async def go():
            conn = await aconnect(autocommit=True)
            w = conn.wire
            try:
                w.slots = asyncio.Semaphore(1)
                slow = asyncio.ensure_future(conn.fetchval("SELECT pg_sleep(0.4) IS NULL"))
                await asyncio.sleep(0.05)
                order = []
                outsider = asyncio.ensure_future(conn.fetchval("SELECT 1"))
                await asyncio.sleep(0.05)  # waiting for the only slot
                async with w.exclusive():
                    await slow               # frees the slot while we own the connection
                    await asyncio.sleep(0.1)
                    order.append("owner-done" if not outsider.done() else "outsider-slipped-in")
                self.assertEqual(await outsider, 1)
                self.assertEqual(order, ["owner-done"])
            finally:
                await conn.close()
        run(go())

    def test_cancelled_autocommit_script_leaves_no_transaction(self):
        async def go():
            conn = await aconnect(autocommit=True)
            try:
                t = asyncio.ensure_future(conn.execute(
                    "INSERT INTO aio_t VALUES (1, 'x'); SELECT pg_sleep(5); SELECT 2"))
                await asyncio.sleep(0.3)
                t.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await t
                self.assertEqual(conn.transaction_status, "idle")
                await conn.execute("INSERT INTO aio_t VALUES (2, 'y')")
                self.assertEqual(conn.transaction_status, "idle")
            finally:
                await conn.close()
            self.assertEqual(await count("id = 1"), 0)  # the script was rolled back
            self.assertEqual(await count("id = 2"), 1)  # the next statement autocommitted
        run(go())

    def test_cancel_during_the_timeout_grace_keeps_the_hidden_txn_resolved(self):
        """Cancel the task while a timed-out statement waits out its reset
        grace: the stream is still abandoned, so the autocommit script's hidden
        transaction is settled before anything else runs."""
        async def go():
            conn = await aconnect(autocommit=True)
            w = conn.wire
            try:
                w.statement_timeout = 0.2
                orig = w._timed_out

                async def slow_grace(s, timeout):
                    await asyncio.sleep(10)  # the task is cancelled in here
                    await orig(s, timeout)

                w._timed_out = slow_grace
                t = asyncio.ensure_future(conn.execute(
                    "INSERT INTO aio_t VALUES (1, 'x'); SELECT pg_sleep(5)"))
                await asyncio.sleep(0.5)  # past the timeout, inside the grace
                t.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await t
                w._timed_out = orig
                w.statement_timeout = None
                await conn.execute("INSERT INTO aio_t VALUES (2, 'y')")
                self.assertEqual(conn.transaction_status, "idle")
            finally:
                await conn.close()
            self.assertEqual(await count("id = 1"), 0)
            self.assertEqual(await count("id = 2"), 1)
        run(go())

    def test_executemany_bulk_and_per_row(self):
        async def go():
            conn = await aconnect()
            try:
                cur = conn.cursor()
                await cur.executemany("INSERT INTO aio_t (id, v) VALUES (%s, %s)", [(i, str(i)) for i in range(5000)])
                self.assertEqual(cur.rowcount, 5000)
                await conn.commit()
                await cur.executemany("UPDATE aio_t SET v = %s WHERE id = %s", [("u", i) for i in range(10)])
                self.assertEqual(cur.rowcount, 10)
                await conn.commit()
                n = await conn.bulk_insert("aio_t", [{"id": 10_000, "v": "b"}, {"id": 10_001, "v": "c"}])
                self.assertEqual(n, 2)
                await conn.commit()
            finally:
                await conn.close()
            self.assertEqual(await count(), 5002)
            self.assertEqual(await count("v = 'u'"), 10)
        run(go())

    def test_server_side_cursor_streams(self):
        async def go():
            conn = await aconnect(autocommit=True)
            try:
                cur = conn.cursor("s")
                await cur.execute("SELECT g FROM generate_series(1, 200000) AS g")
                n = 0
                async for _ in cur:
                    n += 1
                self.assertEqual(n, 200000)
                self.assertEqual(cur.rowcount, 200000)
                await cur.close()
                self.assertEqual(await conn.fetchval("SELECT 5"), 5)
            finally:
                await conn.close()
        run(go())

    def test_multi_statement_nextset(self):
        async def go():
            conn = await aconnect(autocommit=True)
            try:
                cur = await conn.execute("SELECT 1 AS a; SELECT 'b' AS b")
                self.assertEqual(await cur.fetchall(), [(1,)])
                self.assertTrue(cur.nextset())
                self.assertEqual(await cur.fetchall(), [("b",)])
                self.assertIsNone(cur.nextset())
            finally:
                await conn.close()
        run(go())

    def test_pool_shared_and_exclusive(self):
        async def go():
            async with pdb.Pool(host=HOST, port=int(PORT), min_size=1, max_size=2) as pool:
                vals = await asyncio.gather(*(pool.fetchval("SELECT $1::int", [i]) for i in range(300)))
                self.assertEqual(sum(vals), sum(range(300)))
                self.assertLessEqual(pool.size, 2)
                async with pool.connection() as c:
                    self.assertFalse(c.autocommit)
                    await c.execute("INSERT INTO aio_t VALUES (1, 'p')")
                self.assertEqual(await pool.fetchval("SELECT count(*) FROM aio_t"), 1)
                with self.assertRaises(RuntimeError):
                    async with pool.connection() as c:
                        await c.execute("INSERT INTO aio_t VALUES (2, 'q')")
                        raise RuntimeError
                self.assertEqual(await pool.fetchval("SELECT count(*) FROM aio_t"), 1)
        run(go())

    def test_types_description_row_factory_callproc_frames(self):
        async def go():
            conn = await aconnect(autocommit=True)
            try:
                await conn.execute("CREATE OR REPLACE FUNCTION aio_add(a int, b int) RETURNS int AS $$ SELECT a + b $$ "
                                   "LANGUAGE sql")
                cur = conn.cursor(row_factory=pdb.rows.dict_row)
                await cur.execute("INSERT INTO aio_t VALUES (%(i)s, %(v)s)", {"i": 1, "v": "x"})
                await cur.execute("SELECT id, v FROM aio_t")
                self.assertEqual([d.type_code for d in cur.description], [23, 25])
                self.assertEqual(await cur.fetchall(), [{"id": 1, "v": "x"}])
                c2 = conn.cursor()
                self.assertEqual(await c2.callproc("aio_add", [2, 5]), [2, 5])
                self.assertEqual(await c2.fetchone(), (7,))
                await c2.execute("SELECT g FROM generate_series(1, 5) AS g")
                await c2.scroll(3, "absolute")
                self.assertEqual(await c2.fetchone(), (4,))
                await c2.execute("SELECT id FROM aio_t")
                try:
                    import pandas  # noqa: F401
                    self.assertEqual(len(c2.fetch_df()), 1)
                except ImportError:
                    pass
                with self.assertRaises(errors.UniqueViolation):
                    await conn.execute("INSERT INTO aio_t VALUES (2, 'a'); INSERT INTO aio_t VALUES (2, 'b')")
                self.assertEqual(await count("id = 2"), 0)  # atomic, as the blocking driver
                with self.assertRaises(pdb.ProgrammingError):
                    await conn.execute("SELEC 1")
            finally:
                await conn.close()
        run(go())

    def test_cancelled_kv_request_is_reaped(self):
        """A cancelled KV request (it cannot be reset) is reaped: its slot is
        freed when it answers, and the connection keeps working."""
        async def go():
            conn = await aconnect(autocommit=True)
            w = conn.wire
            try:
                w.slots = asyncio.Semaphore(1)
                t = asyncio.ensure_future(conn.kv_set("aio:kvc", "v"))
                await asyncio.sleep(0)
                t.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await t
                self.assertEqual(await asyncio.wait_for(conn.fetchval("SELECT 4"), 10), 4)
                self.assertIn(await conn.kv_get("aio:kvc"), (b"v", None))
            finally:
                await conn.close()
        run(go())

    def test_kv_frames(self):
        async def go():
            conn = await aconnect(autocommit=True)
            try:
                await conn.kv_set("aio:k", b"\x00v")
                self.assertEqual(await conn.kv_get("aio:k"), b"\x00v")
                self.assertTrue(await conn.kv_delete("aio:k"))
                self.assertIsNone(await conn.kv_get("aio:k"))
                self.assertGreater(await conn.ping(), 0)
            finally:
                await conn.close()
        run(go())


def random_tag():
    import random
    return random.randrange(1 << 30)


async def _timeout_stmt(conn):
    conn.wire.statement_timeout = 0.3
    try:
        await conn.execute("SELECT pg_sleep(10)")
    finally:
        conn.wire.statement_timeout = None


if __name__ == "__main__":
    unittest.main()
