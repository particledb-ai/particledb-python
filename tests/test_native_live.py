"""LIVE native-wire tests: query, params, 10k-row EXECUTE_BATCH, errors, and
every feature ported from the removed psycopg SDK (interactive transactions,
stream, connect URLs, context manager, KV/vector/bulk_insert over the wire,
ai.embed_batch error surfacing).

Skipped unless ``PDB_NATIVE_PORT`` is set. Expects a ``--no-auth`` server on
127.0.0.1 (``PDB_NATIVE_HOST`` overrides). Every assertion is an invariant on
data read back from the server (counts, sums, specific rows), not "no error".
Prints ``NATIVE_LIVE_PASS python`` from the last test when it ran.

Run::

    PDB_NATIVE_PORT=7173 python -m pytest tests/test_native_live.py -v -s
"""

from __future__ import annotations

import os
import unittest
import uuid

PORT = os.environ.get("PDB_NATIVE_PORT")
HOST = os.environ.get("PDB_NATIVE_HOST", "127.0.0.1")

try:
    import pyarrow  # noqa: F401  (only query_arrow needs it)

    HAVE_PYARROW = True
except ImportError:  # pragma: no cover
    HAVE_PYARROW = False


def _table(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


@unittest.skipUnless(PORT, "PDB_NATIVE_PORT not set (live native-wire test)")
class NativeLive(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from particledb import ParticleDB, PdbWireClient

        cls.wire = PdbWireClient(host=HOST, port=int(PORT), pool_size=4, timeout=600.0)
        cls.db = ParticleDB(host=HOST, pdb_port=int(PORT), transport="pdb_wire", timeout=600.0)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.db.close()
        cls.wire.close()

    def _create(self, prefix: str, ddl_cols: str) -> str:
        name = _table(prefix)
        self.wire.execute(f"CREATE TABLE {name} ({ddl_cols})")
        self.addCleanup(lambda: self.wire.execute(f"DROP TABLE IF EXISTS {name}"))
        return name

    def _count(self, t: str) -> int:
        return int(self.wire.query(f"SELECT count(*) AS c FROM {t}")[0]["c"])

    # -- query ----------------------------------------------------------
    def test_query(self) -> None:
        self.assertEqual(self.wire.query("SELECT 1 AS one, 'x' AS s"), [{"one": 1, "s": "x"}])
        self.assertEqual(self.db.query_one("SELECT 2 AS two"), {"two": 2})
        res = self.db.query_result("SELECT 1 AS a, 2 AS b")
        self.assertEqual((res.columns, res.row_count), (["a", "b"], 1))

    # -- params ---------------------------------------------------------
    def test_params_round_trip_types(self) -> None:
        t = self._create("py_params", "id BIGINT PRIMARY KEY, f DOUBLE PRECISION, s TEXT, b BOOLEAN")
        sql = f"INSERT INTO {t} (id, f, s, b) VALUES ($1, $2, $3, $4)"
        self.assertEqual(self.wire.execute(sql, [1, 1.25, "héllo '; DROP", True]), 1)
        self.assertEqual(self.wire.execute(sql, [2, None, None, None]), 1)
        self.assertEqual(self.wire.execute(sql, [(1 << 62), -0.5, "", False]), 1)
        rows = self.wire.query(f"SELECT id, f, s, b FROM {t} WHERE id = $1", [1])
        self.assertEqual(rows, [{"id": 1, "f": 1.25, "s": "héllo '; DROP", "b": True}])
        rows = self.wire.query(f"SELECT id, f, s, b FROM {t} WHERE id = $1", [2])
        self.assertEqual(rows, [{"id": 2, "f": None, "s": None, "b": None}])
        rows = self.db.query(f"SELECT id FROM {t} WHERE s = $1 AND b = $2", ["", False])
        self.assertEqual(rows, [{"id": 1 << 62}])
        # The same SQL text reuses the connection's prepared statement.
        self.assertEqual(self.db.execute(f"UPDATE {t} SET f = $1 WHERE id = $2", [9.5, 2]), 1)
        self.assertEqual(self.db.query(f"SELECT f FROM {t} WHERE id = $1", [2]), [{"f": 9.5}])

    # -- 10k EXECUTE_BATCH ---------------------------------------------
    def test_execute_batch_10k_rows(self) -> None:
        t = self._create("py_b10k", "id BIGINT PRIMARY KEY, name TEXT, pad TEXT")
        pad = "p" * 200  # 10k x 200 B > 1 MiB: the read-back below is streamed
        rows = [(i, f"name-{i}", pad) for i in range(10_000)]
        n = self.wire.execute_many(f"INSERT INTO {t} (id, name, pad) VALUES ($1, $2, $3)", rows)
        self.assertEqual(n, 10_000)
        agg = self.wire.query(f"SELECT count(*) AS c, sum(id) AS s FROM {t}")[0]
        self.assertEqual(int(agg["c"]), 10_000)
        self.assertEqual(int(agg["s"]), sum(range(10_000)))  # 49995000
        self.assertEqual(
            self.wire.query(f"SELECT name FROM {t} WHERE id = $1", [7_321]),
            [{"name": "name-7321"}],
        )
        # Whole-table read: > STREAM_THRESHOLD_BYTES, so it arrives as
        # F_ARROW_STREAM chunks; the first chunk carries the schema. Decoded
        # without pyarrow (the built-in reader) and, when installed, with it.
        rows = self.wire.query(f"SELECT id, pad FROM {t}")
        self.assertEqual(len(rows), 10_000)
        self.assertEqual(sum(r["id"] for r in rows), sum(range(10_000)))
        if HAVE_PYARROW:
            table = self.wire.query_arrow(f"SELECT id, pad FROM {t}")
            self.assertEqual(table.num_rows, 10_000)
            self.assertEqual(sum(table.column("id").to_pylist()), sum(range(10_000)))

    # -- errors ---------------------------------------------------------
    def test_bad_sql_is_typed_error(self) -> None:
        from particledb import ParticleDBError
        from particledb.pdb_wire import QueryError

        with self.assertRaises(QueryError):
            self.wire.query("SELEKT 1")
        with self.assertRaises(QueryError):
            self.wire.query("SELECT * FROM no_such_table_xyz WHERE id = $1", [1])
        with self.assertRaises(ParticleDBError):
            self.db.query("SELEKT 1")
        # The pool still works afterwards.
        self.assertEqual(self.wire.query("SELECT 3 AS x"), [{"x": 3}])

    def test_constraint_violation_in_batch_autocommit(self) -> None:
        from particledb.pdb_wire import BatchExecuteError

        t = self._create("py_bdup", "id BIGINT PRIMARY KEY, v TEXT")
        self.wire.execute(f"INSERT INTO {t} VALUES ($1, $2)", [5, "existing"])
        rows = [(i, f"v{i}") for i in range(10)]  # row 5 collides
        with self.assertRaises(BatchExecuteError) as cm:
            self.wire.execute_many(f"INSERT INTO {t} (id, v) VALUES ($1, $2)", rows)
        e = cm.exception
        self.assertEqual([i for i, _ in e.errors], [5])
        self.assertEqual((e.affected, e.attempted), (9, 10))
        # Autocommit: the 9 other rows committed, the collision did not.
        self.assertEqual(self._count(t), 10)
        self.assertEqual(self.wire.query(f"SELECT v FROM {t} WHERE id = 5"), [{"v": "existing"}])

    def test_constraint_violation_in_batch_inside_transaction(self) -> None:
        from particledb.pdb_wire import BatchExecuteError

        t = self._create("py_btxn", "id BIGINT PRIMARY KEY")
        self.wire.execute(f"INSERT INTO {t} VALUES (3)")
        with self.wire.session() as s:
            s.begin()
            with self.assertRaises(BatchExecuteError) as cm:
                s.execute_many(f"INSERT INTO {t} (id) VALUES ($1)", [(i,) for i in range(6)])
            # The server stops at the first failure inside a transaction.
            self.assertEqual(cm.exception.attempted, 4)
            s.rollback()
        self.assertEqual(self._count(t), 1)

    # -- ported: interactive transaction --------------------------------
    def test_interactive_transaction_reads_own_writes(self) -> None:
        from particledb import ParticleDBError

        t = self._create("py_tx", "id BIGINT PRIMARY KEY, bal BIGINT")
        self.db.execute_many(f"INSERT INTO {t} VALUES ($1, $2)", [(1, 100), (2, 0)])
        with self.db.transaction() as tx:
            tx.execute(f"UPDATE {t} SET bal = bal - $1 WHERE id = $2", [30, 1])
            tx.execute(f"UPDATE {t} SET bal = bal + $1 WHERE id = $2", [30, 2])
            self.assertEqual(tx.query(f"SELECT bal FROM {t} WHERE id = 1"), [{"bal": 70}])
        total = self.wire.query(f"SELECT sum(bal) AS s, min(bal) AS lo FROM {t}")[0]
        self.assertEqual((int(total["s"]), int(total["lo"])), (100, 30))

        with self.assertRaises(ValueError):
            with self.db.transaction() as tx:
                tx.execute(f"UPDATE {t} SET bal = 0 WHERE id = 1")
                raise ValueError("abort")
        self.assertEqual(self.wire.query(f"SELECT bal FROM {t} WHERE id = 1"), [{"bal": 70}])

        with self.assertRaises(ParticleDBError):
            with self.db.transaction() as tx:
                tx.execute(f"INSERT INTO {t} VALUES (2, 0)")  # duplicate key
        self.assertEqual(self._count(t), 2)

    # -- ported: stream, connect URL, context manager, bulk_insert -------
    def test_stream_yields_every_row(self) -> None:
        t = self._create("py_stream", "id BIGINT PRIMARY KEY")
        self.db.execute_many(f"INSERT INTO {t} (id) VALUES ($1)", [(i,) for i in range(2_500)])
        ids = [r["id"] for r in self.db.stream(f"SELECT id FROM {t} WHERE id >= $1", [0], batch_size=300)]
        self.assertEqual(sorted(ids), list(range(2_500)))

    def test_connect_url_and_context_manager(self) -> None:
        from particledb import ParticleDB

        with ParticleDB.connect(f"pdb://{HOST}:{PORT}") as db:
            self.assertEqual(db._active_transport(), "pdb_wire")
            self.assertEqual(db.query("SELECT $1::BIGINT + 1 AS v", [41]), [{"v": 42}])

    def test_bulk_insert_uses_execute_batch(self) -> None:
        t = self._create("py_bulk", "id BIGINT PRIMARY KEY, v TEXT")
        n = self.db.bulk_insert(t, ["id", "v"], [[i, f"v{i}"] for i in range(1_000)])
        self.assertEqual(n, 1_000)
        agg = self.wire.query(f"SELECT count(*) AS c, sum(id) AS s FROM {t}")[0]
        self.assertEqual((int(agg["c"]), int(agg["s"])), (1_000, sum(range(1_000))))

    # -- ported helpers over the wire -----------------------------------
    def test_kv_over_native_wire(self) -> None:
        k = f"py_kv_{uuid.uuid4().hex[:8]}"
        self.db.kv.set(k, "v1")
        self.assertEqual(self.db.kv.get(k), "v1")
        self.assertTrue(self.db.kv.exists(k))
        self.assertEqual(self.db.kv.incr(k + ":n", 5), 5)
        self.assertEqual(self.db.kv.incr(k + ":n", 2), 7)
        # mget: one value per key, None for a missing key, and a value that
        # contains a comma survives (the server's KV_MGET joins with commas).
        self.db.kv.set(k + ":c", "p,q")
        self.assertEqual(
            self.db.kv.mget([k, k + ":missing", k + ":c"]),
            {k: "v1", k + ":missing": None, k + ":c": "p,q"},
        )
        self.assertTrue(self.db.kv.delete(k + ":c"))
        self.assertTrue(self.db.kv.delete(k))
        self.assertIsNone(self.db.kv.get(k))

    def test_vector_named_index_and_search(self) -> None:
        t = self._create("py_vec", "id BIGINT PRIMARY KEY, emb VECTOR(3)")
        self.db.vector.insert(t, {"id": 1}, "emb", [1.0, 0.0, 0.0])
        self.db.vector.insert(t, {"id": 2}, "emb", [0.0, 1.0, 0.0])
        idx = f"{t}_named_idx"
        self.db.vector.create_index(t, "emb", name=idx)
        names = {r["indexname"] for r in self.db.query(
            "SELECT indexname FROM pg_indexes WHERE tablename = $1", [t])}
        self.assertIn(idx, names)
        hits = self.db.vector.search(t, "emb", [0.9, 0.1, 0.0], k=1)
        self.assertEqual([h["row"]["id"] for h in hits], [1])

    def test_embed_batch_failure_is_typed_error(self) -> None:
        """One EXECUTE_BATCH of EMBED($1): with no embedding provider set, the
        failure must surface as ParticleDBError, never as empty vectors."""
        from particledb import ParticleDBError

        try:
            vecs = self.db.ai.embed_batch(["alpha", "beta"])
        except ParticleDBError as exc:
            self.assertIn("text 0", str(exc))
            return
        # A server WITH an embedding provider: one non-empty vector per text.
        self.assertEqual(len(vecs), 2)
        self.assertTrue(all(len(v) > 0 for v in vecs))

    def test_zz_marker(self) -> None:
        print("NATIVE_LIVE_PASS python")


if __name__ == "__main__":
    unittest.main()
