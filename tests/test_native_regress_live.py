"""Live regressions for native-wire Session / transaction / batch edges.

Skipped unless ``PDB_NATIVE_PORT`` is set (``--no-auth`` server on 127.0.0.1).
Every case asserts the table state, not just the exception.

    PDB_NATIVE_PORT=7173 python -m pytest tests/test_native_regress_live.py -v
"""

from __future__ import annotations

import os
import unittest
import uuid

PORT = os.environ.get("PDB_NATIVE_PORT")
HOST = os.environ.get("PDB_NATIVE_HOST", "127.0.0.1")

@unittest.skipUnless(PORT, "PDB_NATIVE_PORT not set (live native-wire test)")
class NativeRegressLive(unittest.TestCase):
    def setUp(self) -> None:
        from particledb import PdbWireClient

        # pool_size=1: the next borrower gets the SAME connection, so a leaked
        # open transaction would be observable.
        self.wire = PdbWireClient(host=HOST, port=int(PORT), pool_size=1)
        self.t = f"nrg_py_{uuid.uuid4().hex[:10]}"
        self.wire.execute(f"CREATE TABLE {self.t} (id BIGINT PRIMARY KEY, v TEXT)")

    def tearDown(self) -> None:
        self.wire.execute(f"DROP TABLE IF EXISTS {self.t}")
        self.wire.close()

    def _count(self) -> int:
        return int(self.wire.query(f"SELECT count(*) AS c FROM {self.t}")[0]["c"])

    def test_swallowed_error_makes_commit_raise(self) -> None:
        from particledb import TransactionRolledBackError
        from particledb.pdb_wire import QueryError

        s = self.wire.session()
        with self.assertRaises(TransactionRolledBackError):
            with s.transaction():
                s.execute(f"INSERT INTO {self.t} VALUES ($1, $2)", [1, "a"])
                try:
                    s.execute(f"INSERT INTO {self.t} VALUES ($1, $2)", [1, "dup"])
                except QueryError:
                    pass  # swallowed by the caller
        s.close()
        self.assertEqual(self._count(), 0)

    def test_rollback_to_savepoint_keeps_session_in_transaction(self) -> None:
        # Comments are skipped by the server; the client no longer classifies
        # statements at all -- the SERVER's per-response transaction status
        # decides whether the session's connection may be pooled.
        for begin, rollback_to in (
            ("BEGIN", "ROLLBACK TO SAVEPOINT sp"),
            ("BEGIN", "ROLLBACK /* a b c */ TO SAVEPOINT sp"),
            ("/* lead */ BEGIN", "-- note\nROLLBACK WORK TO sp"),
            ("BEGIN", "ROLLBACK /* /*/ */ TO SAVEPOINT sp"),
        ):
            with self.subTest(begin=begin, rollback_to=rollback_to):
                s = self.wire.session()
                s.execute(begin)
                s.execute("SAVEPOINT sp")
                s.execute(rollback_to)
                s.execute(f"INSERT INTO {self.t} VALUES ($1, $2)", [7, "x"])
                self.assertTrue(s.connection.in_transaction)
                s.close()  # open transaction: discarded, not pooled
                # The next borrower must not be inside that transaction: its
                # COMMIT must not persist row 7.
                s2 = self.wire.session()
                s2.execute("COMMIT")
                s2.close()
                self.assertEqual(self._count(), 0)

    def test_plain_commit_still_pools_the_connection(self) -> None:
        # Positive control for the fail-closed tracking: a clean BEGIN .. COMMIT
        # leaves the session outside a transaction and the connection pooled.
        s = self.wire.session()
        s.execute("BEGIN")
        s.execute(f"INSERT INTO {self.t} VALUES ($1, $2)", [8, "y"])
        s.execute("COMMIT")
        self.assertFalse(s.connection.in_transaction)
        s.close()
        self.assertEqual(self.wire._pool.idle_count, 1)
        self.assertEqual(self._count(), 1)

    def test_int_above_2_53_mixed_with_float_binds_exactly(self) -> None:
        # Every value is bound with its own typed tag (int8 / float8): an int
        # above 2^53 next to a float in another row is never rounded.
        big = 9007199254740993
        t = self.t + "_n"
        self.wire.execute(f"CREATE TABLE {t} (k INT PRIMARY KEY, n NUMERIC)")
        try:
            self.wire.execute_many(f"INSERT INTO {t} VALUES ($1, $2)", [(1, big), (2, 1.5)])
            got = {int(r["k"]): str(r["n"]) for r in self.wire.query(f"SELECT k, n FROM {t}")}
            self.assertEqual(got[1].split(".")[0], str(big))
            self.assertEqual(float(got[2]), 1.5)
        finally:
            self.wire.execute(f"DROP TABLE IF EXISTS {t}")

    def test_oversized_frame_refused_before_any_frame(self) -> None:
        rows = [(1, "ok"), (2, "x" * (17 * 1024 * 1024))]
        with self.assertRaises(ValueError):
            self.wire.execute_many(f"INSERT INTO {self.t} VALUES ($1, $2)", rows)
        self.assertEqual(self._count(), 0)


if __name__ == "__main__":
    unittest.main()
