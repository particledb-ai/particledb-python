"""TLS configuration and native client checks."""

import asyncio
import concurrent.futures
import os
import ssl
import unittest

from particledb import _tls, aio, dbapi
from particledb.connection import Connection


class TLSOptions(unittest.TestCase):
    def test_modes(self):
        self.assertIsNone(_tls.context())
        encrypted = _tls.context("require")
        self.assertEqual(encrypted.verify_mode, ssl.CERT_NONE)
        verified = _tls.context("verify-full")
        self.assertEqual(verified.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(verified.check_hostname)
        self.assertFalse(_tls.context("verify-ca").check_hostname)
        self.assertRaises(ValueError, _tls.context, "prefer")
        self.assertRaises(ValueError, _tls.context, "verify-full", sslcert="client.pem")


PORT = os.environ.get("PDB_TLS_PORT")
CA = os.environ.get("PDB_TLS_CA")


@unittest.skipUnless(PORT and CA, "PDB_TLS_PORT and PDB_TLS_CA required")
class TLSLive(unittest.TestCase):
    def options(self, **kwargs):
        return dict(host="localhost", port=int(PORT), sslmode="verify-full", sslrootcert=CA, **kwargs)

    def test_verified_queries_and_idle_pool_probe(self):
        with Connection(**self.options()) as connection:
            self.assertTrue(connection.transport.startswith("tls:"))
            self.assertFalse(connection.peer_gone())
            for _ in range(3):
                self.assertEqual(connection.scalar("SELECT 42"), 42)
                self.assertFalse(connection.peer_gone())

    def test_concurrent_queries_use_the_same_encrypted_session(self):
        with Connection(**self.options()) as connection:
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as workers:
                results = list(workers.map(lambda i: connection.scalar("SELECT $1", [i]), range(40)))
            self.assertEqual(results, list(range(40)))

    def test_verification_failures_are_database_connection_errors(self):
        opts = self.options()
        opts["host"] = "127.0.0.1"
        with self.assertRaises(dbapi.OperationalError):
            dbapi.connect(**opts)
        opts = self.options()
        opts.pop("sslrootcert")
        with self.assertRaises(dbapi.OperationalError):
            dbapi.connect(**opts)

    def test_require_and_chain_only_are_explicit_modes(self):
        for mode in ("require", "verify-ca"):
            opts = self.options()
            opts.update(host="127.0.0.1", sslmode=mode)
            with dbapi.connect(**opts) as connection:
                self.assertEqual(connection.execute("SELECT 9").fetchone(), (9,))

    def test_async_queries_and_cancellation(self):
        async def run():
            async with await aio.connect(**self.options(autocommit=True)) as connection:
                async def query(i):
                    cursor = await connection.execute("SELECT %s", [i])
                    return (await cursor.fetchone())[0]
                self.assertEqual(await asyncio.gather(*(query(i) for i in range(20))), list(range(20)))
                task = asyncio.create_task(connection.execute("SELECT pg_sleep(30)"))
                await asyncio.sleep(.05)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                cursor = await connection.execute("SELECT 1")
                self.assertEqual(await cursor.fetchone(), (1,))
        asyncio.run(run())

    def test_async_refuses_the_wrong_host(self):
        async def run():
            opts = self.options()
            opts["host"] = "127.0.0.1"
            with self.assertRaises(dbapi.OperationalError):
                await aio.connect(**opts)
        asyncio.run(run())
