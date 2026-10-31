"""Live-server test: a failed statement inside a native-wire batch or
transaction must be RAISED, never reported as success, and a failed
transaction must not leave the pooled connection inside a failed transaction.

Runs only when ``PDB_NATIVE_PORT`` is set (host 127.0.0.1, a ``--no-auth``
server). Prints ``NBE_PASS python`` when every case passed, so a runner can
prove the test actually ran rather than skipped.

    PDB_NATIVE_PORT=48710 python -m pytest -s tests/test_native_batch_errors.py
"""

from __future__ import annotations

import os
import unittest

# A short BATCH_RESULT without an error entry is a protocol error: see
# tests/test_connection.py BatchesAndKv.test_short_batch_without_error_is_a_protocol_error.

from particledb.pdb_wire import BatchError, PdbError, PdbWireClient, QueryError

PORT = os.environ.get("PDB_NATIVE_PORT")
TABLE = "nbe_python"


def _ids(db: PdbWireClient, where: str = "") -> list:
    rows = db.query(f"SELECT id FROM {TABLE} {where} ORDER BY id")
    return [r["id"] for r in rows]


@unittest.skipUnless(PORT, "PDB_NATIVE_PORT not set; live native-wire test skipped")
class NativeBatchErrors(unittest.TestCase):
    def test_batch_and_transaction_errors_are_raised(self) -> None:
        # pool_size=1 so case (c) runs on whatever connection the pool hands
        # back after the failed transaction -- if the failed connection were
        # recycled, (c) would run inside the failed transaction.
        db = PdbWireClient(host="127.0.0.1", port=int(PORT), pool_size=1, max_retries=0)
        try:
            db.execute(f"DROP TABLE IF EXISTS {TABLE}")
            db.execute(f"CREATE TABLE {TABLE} (id INT PRIMARY KEY)")

            # (a) batch [INSERT k1, INSERT k1, INSERT k2] -> raises.
            with self.assertRaises(PdbError) as cm:
                db.batch([
                    f"INSERT INTO {TABLE} VALUES (1)",
                    f"INSERT INTO {TABLE} VALUES (1)",
                    f"INSERT INTO {TABLE} VALUES (2)",
                ])
            self.assertIn("duplicate key", str(cm.exception), "(a) message")
            self.assertIsInstance(cm.exception, BatchError)
            self.assertEqual((cm.exception.sqlstate, cm.exception.index), ("23505", 1), "(a) entry SQLSTATE")
            # The server stopped at the failing statement: k2 never ran.
            self.assertEqual(_ids(db), [1], "(a) k1 autocommitted, k2 never ran")
            print("NBE case a: batch error raised:", cm.exception)

            # (b) transaction [INSERT k3, INSERT k3] -> raises.
            with self.assertRaises(PdbError) as cm:
                with db.transaction() as txn:
                    txn.execute(f"INSERT INTO {TABLE} VALUES (3)")
                    txn.execute(f"INSERT INTO {TABLE} VALUES (3)")
            self.assertIn("duplicate key", str(cm.exception), "(b) message")
            self.assertEqual(cm.exception.sqlstate, "23505", "(b) SQLSTATE")
            print("NBE case b: transaction error raised:", cm.exception)

            # (c) a plain query on the same client succeeds -- the connection
            # was not left inside the failed transaction -- and the failed
            # transaction committed nothing.
            self.assertEqual(_ids(db, "WHERE id = 3"), [], "(c) failed txn committed nothing")
            self.assertEqual(db.execute(f"INSERT INTO {TABLE} VALUES (30)"), 1, "(c) plain insert")
            print("NBE case c: plain query after failed transaction succeeded")

            # (d) no false positives: an all-success batch returns success,
            # and a successful transaction commits (row visible).
            affected = db.batch([
                f"INSERT INTO {TABLE} VALUES (4)",
                f"INSERT INTO {TABLE} VALUES (5)",
            ])
            self.assertEqual(affected, 2, "(d) batch affected rows")
            with db.transaction() as txn:
                txn.execute(f"INSERT INTO {TABLE} VALUES (6)")
                txn.execute(f"INSERT INTO {TABLE} VALUES (7)")
            self.assertEqual(_ids(db), [1, 4, 5, 6, 7, 30], "(d) committed rows visible")
            print("NBE case d: successful batch + transaction committed")

            # (e) BEGIN sent through the single-statement API, then a failing
            # statement: the pooled connection is inside a FAILED transaction,
            # so it must not be handed out again -- the next statement runs
            # (and autocommits) on a fresh connection.
            db.execute("BEGIN")
            with self.assertRaises(QueryError):
                db.execute(f"INSERT INTO {TABLE} VALUES (1)")
            self.assertEqual(db.execute(f"INSERT INTO {TABLE} VALUES (40)"), 1, "(e) next statement")
            self.assertIn(40, _ids(db), "(e) the next statement committed")
            print("NBE case e: failed statement after a raw BEGIN did not poison the pool")

            db.execute(f"DROP TABLE IF EXISTS {TABLE}")
        finally:
            db.close()
        print("NBE_PASS python")


if __name__ == "__main__":
    unittest.main()
