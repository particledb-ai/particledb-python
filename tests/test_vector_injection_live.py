"""LIVE: ``db.vector`` caller values cannot inject SQL (native wire).

Each test creates a canary table, hands ``search`` / ``create_index`` /
``insert`` a value that WOULD drop it if spliced into the statement, and
asserts the canary still holds its row; payloads that are legitimately data
(``insert`` row values, incl. JSON with quotes / backslashes and unicode)
must read back byte-for-byte.

Skipped unless ``PDB_NATIVE_PORT`` is set (a ``--no-auth`` server on
``PDB_NATIVE_HOST``, default 127.0.0.1). Tables are ``py_inj_<hex>``.

Run::

    PDB_NATIVE_PORT=7173 python -m pytest tests/test_vector_injection_live.py -v
"""

from __future__ import annotations

import json
import os
import unittest
import uuid

PORT = os.environ.get("PDB_NATIVE_PORT")
HOST = os.environ.get("PDB_NATIVE_HOST", "127.0.0.1")

TEXT_PAYLOADS = [
    "'); DROP TABLE t; --",
    "\\'",
    "it's a \"quote\" \\ backslash",
    "ünï'cødé ✓ 日本語",
    "[[1, \"a'b\"], [\"c\\\\d\"]]",
]


@unittest.skipUnless(PORT, "PDB_NATIVE_PORT not set (live native-wire test)")
class VectorInjectionLive(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from particledb import ParticleDB

        cls.db = ParticleDB(host=HOST, pdb_port=int(PORT), transport="pdb_wire", timeout=120.0)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.db.close()

    def _table(self, cols: str) -> str:
        name = f"py_inj_{uuid.uuid4().hex[:10]}"
        self.db.execute(f"CREATE TABLE {name} ({cols})")
        self.addCleanup(lambda: self.db.execute(f"DROP TABLE IF EXISTS {name}"))
        return name

    def _canary(self) -> str:
        c = self._table("id BIGINT PRIMARY KEY")
        self.db.execute(f"INSERT INTO {c} VALUES (42)")
        return c

    def _assert_canary(self, c: str) -> None:
        self.assertEqual(self.db.query(f"SELECT id FROM {c}"), [{"id": 42}])

    def test_search_vector_payload_cannot_drop_canary(self) -> None:
        t = self._table("id BIGINT PRIMARY KEY, emb VECTOR(3)")
        c = self._canary()
        self.db.vector.insert(t, {"id": 1}, "emb", [1.0, 0.0, 0.0])
        payload = f"0,0,0]')) AS d FROM {t}; DROP TABLE {c}; --"
        try:
            self.db.vector.search(t, "emb", [payload])
        except Exception:
            pass
        self._assert_canary(c)
        hits = self.db.vector.search(t, "emb", [0.9, 0.1, 0], k=1)
        self.assertEqual([h["row"]["id"] for h in hits], [1])

    def test_create_index_option_payload_cannot_drop_canary(self) -> None:
        t = self._table("id BIGINT PRIMARY KEY, emb VECTOR(3)")
        c = self._canary()
        idx = f"{t}_bt"
        try:
            self.db.vector.create_index(
                t, "id", index_type="btree", name=idx, fillfactor=f"90'); DROP TABLE {c}; --"
            )
        except Exception:
            pass
        self._assert_canary(c)
        # A legitimate string option still works.
        self.db.vector.create_index(t, "emb", name=f"{t}_hnsw", m="8", ef_construction=64)
        defs = [r["indexdef"] for r in self.db.query(
            "SELECT indexdef FROM pg_indexes WHERE tablename = $1", [t])]
        self.assertTrue(any("m='8'" in d and "ef_construction='64'" in d for d in defs), defs)

    def test_star_column_and_schema_qualified_table(self) -> None:
        t = self._table("id BIGINT PRIMARY KEY, emb VECTOR(3)")
        q = f"public.{t}"
        self.db.vector.insert(q, {"id": 7}, "emb", [1.0, 0.0, 0.0])
        hits = self.db.vector.search(q, "emb", [1.0, 0.0, 0.0], columns=["*"], k=1)
        self.assertEqual(hits[0]["row"]["id"], 7)
        self.assertEqual(hits[0]["row"]["emb"], [1.0, 0.0, 0.0])
        self.assertEqual(hits[0]["distance"], 0.0)
        self.db.vector.create_index(q, "emb", name=f"{t}_q")
        names = {r["indexname"] for r in self.db.query(
            "SELECT indexname FROM pg_indexes WHERE tablename = $1", [t])}
        self.assertIn(f"{t}_q", names)

    def test_insert_element_smuggling_rejected(self) -> None:
        t = self._table("id BIGINT PRIMARY KEY, emb VECTOR(3)")
        try:
            self.db.vector.insert(t, {"id": 1}, "emb", ["1,2", 3])
        except Exception:
            pass
        self.assertEqual(self.db.query(f"SELECT count(*) AS n FROM {t}"), [{"n": 0}])

    def test_insert_row_values_stored_as_data(self) -> None:
        t = self._table("id BIGINT PRIMARY KEY, note TEXT, doc JSONB, emb VECTOR(3)")
        c = self._canary()
        doc = {"q": "a'b\"c", "bs": "x\\y", "u": "ünï ✓", "nested": [["'); DROP TABLE t; --"]]}
        for i, p in enumerate(TEXT_PAYLOADS):
            self.db.vector.insert(t, {"id": i, "note": p, "doc": json.dumps(doc)}, "emb", [i, 0.5, -1e-3])
        rows = self.db.query(f"SELECT id, note, doc, emb FROM {t} ORDER BY id")
        self.assertEqual([r["note"] for r in rows], TEXT_PAYLOADS)
        for i, r in enumerate(rows):
            got = r["doc"] if isinstance(r["doc"], dict) else json.loads(r["doc"])
            self.assertEqual(got, doc)
            self.assertEqual(len(r["emb"]), 3)
            self.assertAlmostEqual(r["emb"][0], float(i))
        self._assert_canary(c)


if __name__ == "__main__":
    unittest.main()
