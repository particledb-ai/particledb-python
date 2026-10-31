"""Server-free tests: ``db.vector`` never splices caller values into SQL text.

* ``search`` / ``insert``: the query vector is validated element by element
  (real numbers only: no ``str``, ``bool``, nested sequence, NaN or infinity)
  and travels as a BOUND text parameter (``VECTOR($1)`` / ``$n``), never as
  part of the statement.
* ``create_index``: option names must be plain identifiers; string option
  values become ONE single-quoted literal (quotes doubled), numbers are
  re-rendered from ``int`` / ``float``.
* Table, column, result-column and index names are double-quoted with
  ``ops._quote_ident`` (the SDK's one identifier quoter); ``index_type`` must
  be a plain identifier.

Run from the repository root: ``python -m pytest tests/test_vector_sql_safety.py -v``.
"""

from __future__ import annotations

import math
import unittest
from decimal import Decimal
from unittest.mock import MagicMock, patch

DROP = "'); DROP TABLE t; --"
PAYLOADS = [
    DROP,
    "\\'",
    "0]') ; DROP TABLE t; --",
    "0,0,0]')) AS d FROM t; DROP TABLE t; --",
    "ünï'cødé ✓",
]


def _db(wire):
    from particledb.client import ParticleDB

    with patch("particledb.client.ParticleDB._init_wire", return_value=wire):
        return ParticleDB(host="localhost")


class VectorSearchSafety(unittest.TestCase):
    def setUp(self) -> None:
        self.wire = MagicMock()
        self.wire.query.return_value = []
        self.db = _db(self.wire)

    def test_vector_is_bound_not_spliced(self) -> None:
        self.db.vector.search("t", "emb", [0.5, -1, 2.25e-7], k=3)
        sql, params = self.wire.query.call_args[0]
        self.assertEqual(
            sql,
            'SELECT *, ("emb" <-> VECTOR($1)) AS __distance FROM "t" '
            'ORDER BY ("emb" <-> VECTOR($1)) LIMIT 3',
        )
        self.assertEqual(params, ["[0.5,-1.0,2.25e-07]"])

    def test_string_elements_rejected_before_any_sql(self) -> None:
        for p in PAYLOADS:
            with self.subTest(p=p):
                with self.assertRaises(TypeError):
                    self.db.vector.search("t", "emb", [p])
        self.wire.query.assert_not_called()

    def test_non_real_elements_rejected(self) -> None:
        bad = [[True], [None], [[0.1, "x'"]], [{"a": "b'\\"}], [b"1"], [1 + 2j]]
        for v in bad:
            with self.subTest(v=v):
                with self.assertRaises(TypeError):
                    self.db.vector.search("t", "emb", v)
        for v in ([math.nan], [math.inf], [-math.inf], [Decimal("NaN")]):
            with self.subTest(v=v):
                with self.assertRaises(ValueError):
                    self.db.vector.search("t", "emb", v)
        with self.assertRaises(TypeError):
            self.db.vector.search("t", "emb", "0.1,0.2")  # a str is not a vector
        self.wire.query.assert_not_called()

    def test_identifiers_quoted(self) -> None:
        self.db.vector.search('t"x', 'e"; --', [1.0], columns=["ab", "c"], k=1)
        sql, _ = self.wire.query.call_args[0]
        self.assertTrue(sql.startswith('SELECT "ab", "c", ("e""; --" <-> VECTOR($1))'))
        self.assertIn('FROM "t""x"', sql)


class VectorNameForms(unittest.TestCase):
    def setUp(self) -> None:
        self.wire = MagicMock()
        self.wire.query.return_value = []
        self.db = _db(self.wire)

    def test_star_column_unquoted(self) -> None:
        self.db.vector.search("t", "emb", [1.0], columns=["*"], k=1)
        self.assertTrue(self.wire.query.call_args[0][0].startswith('SELECT *, ("emb"'))

    def test_schema_qualified_table_quoted_per_part(self) -> None:
        self.db.vector.search('pub"lic.t', "emb", [1.0], k=1)
        self.assertIn('FROM "pub""lic"."t" ORDER', self.wire.query.call_args[0][0])
        self.db.vector.insert("public.t", {"id": 1}, "emb", [1.0])
        self.assertTrue(self.wire.execute.call_args[0][0].startswith('INSERT INTO "public"."t" ('))
        self.db.vector.create_index("public.t", "emb")
        self.assertEqual(
            self.wire.execute.call_args[0][0],
            'CREATE INDEX "idx_t_emb_hnsw" ON "public"."t" USING hnsw ("emb")',
        )

    def test_empty_table_parts_rejected(self) -> None:
        for t in ("", ".t", "public.", "a..b"):
            with self.subTest(t=t):
                with self.assertRaises(ValueError):
                    self.db.vector.search(t, "emb", [1.0])
                with self.assertRaises(ValueError):
                    self.db.vector.insert(t, {}, "emb", [1.0])
                with self.assertRaises(ValueError):
                    self.db.vector.create_index(t, "emb", name="i")
        self.wire.query.assert_not_called()
        self.wire.execute.assert_not_called()

    def test_column_expressions_refused_with_alternative(self) -> None:
        for c in ("count(*)", "a AS b", "a+b", "t.a", 'x"; --', "a, b", "1a", "*, 1", ""):
            with self.subTest(c=c):
                with self.assertRaisesRegex(ValueError, "filter=|raw SQL"):
                    self.db.vector.search("t", "emb", [1.0], columns=[c])
        self.wire.query.assert_not_called()


class VectorInsertSafety(unittest.TestCase):
    def setUp(self) -> None:
        self.wire = MagicMock()
        self.db = _db(self.wire)

    def test_vector_text_is_validated_and_bound(self) -> None:
        self.db.vector.insert("t", {"id": 1, "note": DROP}, "emb", [1, 2.5, -0.0])
        sql, params = self.wire.execute.call_args[0]
        self.assertEqual(sql, 'INSERT INTO "t" ("id", "note", "emb") VALUES ($1, $2, $3)')
        self.assertEqual(params, [1, DROP, "[1.0,2.5,-0.0]"])

    def test_element_smuggling_rejected(self) -> None:
        # "1,2" used to become two dimensions of the stored vector.
        for v in (["1,2", "3"], [DROP], [[1.0]], [True, 1.0], [math.nan]):
            with self.subTest(v=v):
                with self.assertRaises((TypeError, ValueError)):
                    self.db.vector.insert("t", {"id": 1}, "emb", v)
        self.wire.execute.assert_not_called()


class VectorCreateIndexSafety(unittest.TestCase):
    def setUp(self) -> None:
        self.wire = MagicMock()
        self.db = _db(self.wire)

    def _sql(self) -> str:
        return self.wire.execute.call_args[0][0]

    def test_numbers_unquoted_names_quoted(self) -> None:
        self.db.vector.create_index("docs", "emb", name="my_idx", m=16, ef_construction=200)
        self.assertEqual(
            self._sql(),
            'CREATE INDEX "my_idx" ON "docs" USING hnsw ("emb") '
            "WITH (m = 16, ef_construction = 200)",
        )

    def test_string_values_are_one_literal(self) -> None:
        for p in PAYLOADS:
            with self.subTest(p=p):
                self.db.vector.create_index("t", "emb", name="i", m=p)
                expect = "'" + p.replace("'", "''") + "'"
                self.assertTrue(self._sql().endswith(f"WITH (m = {expect})"), self._sql())

    def test_bad_option_names_and_values_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.db.vector.create_index("t", "emb", **{"m = 1); DROP TABLE t; --": 1})
        for v in (True, None, [1], {"a": 1}, math.nan, math.inf):
            with self.subTest(v=v):
                with self.assertRaises((TypeError, ValueError)):
                    self.db.vector.create_index("t", "emb", m=v)
        with self.assertRaises(ValueError):
            self.db.vector.create_index("t", "emb", index_type="hnsw (emb); DROP TABLE t; --")
        self.wire.execute.assert_not_called()

    def test_default_name_is_quoted(self) -> None:
        self.db.vector.create_index('t"', "emb")
        self.assertEqual(self._sql(), 'CREATE INDEX "idx_t""_emb_hnsw" ON "t""" USING hnsw ("emb")')


if __name__ == "__main__":
    unittest.main()
