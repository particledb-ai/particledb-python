"""Server-free tests of the DB-API layer: module globals, placeholder
conversion, statement shapes, type codes, error mapping, adaptation, row
factories, interval parsing, DSN parsing."""

import datetime as dt
import decimal
import enum
import unittest
import uuid

from particledb import _arrow
from particledb import dbapi
from particledb.dbapi import _adapt, _sql, _types, errors, rows


class ModuleGlobals(unittest.TestCase):
    def test_pep249_globals(self):
        self.assertEqual(dbapi.apilevel, "2.0")
        self.assertEqual(dbapi.threadsafety, 2)
        self.assertEqual(dbapi.paramstyle, "pyformat")
        self.assertTrue(callable(dbapi.connect))

    def test_exception_hierarchy(self):
        E = dbapi
        self.assertTrue(issubclass(E.Warning, Exception))
        self.assertTrue(issubclass(E.Error, Exception))
        for c in (E.InterfaceError, E.DatabaseError):
            self.assertTrue(issubclass(c, E.Error))
        for c in (E.DataError, E.OperationalError, E.IntegrityError, E.InternalError,
                  E.ProgrammingError, E.NotSupportedError):
            self.assertTrue(issubclass(c, E.DatabaseError))
        import particledb
        self.assertTrue(issubclass(E.Error, particledb.ParticleDBError))

    def test_connection_exposes_exceptions(self):
        for name in ("Warning", "Error", "InterfaceError", "DatabaseError", "DataError", "OperationalError",
                     "IntegrityError", "InternalError", "ProgrammingError", "NotSupportedError"):
            if name == "Warning":
                continue
            self.assertIs(getattr(dbapi.Connection, name), getattr(dbapi, name))

    def test_type_objects_and_constructors(self):
        self.assertEqual(dbapi.NUMBER, _types.INT4)
        self.assertEqual(dbapi.NUMBER, _types.NUMERIC)
        self.assertEqual(dbapi.STRING, _types.VARCHAR)
        self.assertEqual(dbapi.DATETIME, _types.TIMESTAMPTZ)
        self.assertEqual(dbapi.BINARY, _types.BYTEA)
        self.assertEqual(dbapi.ROWID, _types.OID)
        self.assertNotEqual(dbapi.NUMBER, _types.TEXT)
        self.assertEqual(dbapi.Date(2024, 1, 2), dt.date(2024, 1, 2))
        self.assertEqual(dbapi.Time(1, 2, 3), dt.time(1, 2, 3))
        self.assertEqual(dbapi.Timestamp(2024, 1, 2, 3, 4, 5), dt.datetime(2024, 1, 2, 3, 4, 5))
        self.assertIsInstance(dbapi.DateFromTicks(0), dt.date)
        self.assertIsInstance(dbapi.TimeFromTicks(0), dt.time)
        self.assertIsInstance(dbapi.TimestampFromTicks(0), dt.datetime)
        self.assertEqual(dbapi.Binary(bytearray(b"\x00\x01")), b"\x00\x01")


class Placeholders(unittest.TestCase):
    def test_none_params_verbatim(self):
        self.assertEqual(_sql.convert("SELECT '%s' %% 1", None), ("SELECT '%s' %% 1", []))

    def test_format(self):
        self.assertEqual(_sql.convert("SELECT %s, %s, 100%%", (1, 2)), ("SELECT $1, $2, 100%", [1, 2]))
        self.assertEqual(_sql.convert("SELECT %b, %t", [1, 2])[0], "SELECT $1, $2")

    def test_pyformat_repeats_reuse_number(self):
        sql, vals = _sql.convert("SELECT %(a)s, %(b)s, %(a)s", {"a": 1, "b": 2})
        self.assertEqual((sql, vals), ("SELECT $1, $2, $1", [1, 2]))

    def test_qmark_skips_literals_and_comments(self):
        sql, vals = _sql.convert("SELECT ?, '?', \"?\", $$?$$, ??, -- ?\n ? /* ? */", [1, 2])
        self.assertEqual(sql, "SELECT $1, '?', \"?\", $$?$$, ?, -- ?\n $2 /* ? */")
        self.assertEqual(vals, [1, 2])

    def test_numeric_passthrough(self):
        self.assertEqual(_sql.convert("SELECT $1, $2", [1, 2]), ("SELECT $1, $2", [1, 2]))

    def test_count_mismatch_raises(self):
        with self.assertRaises(dbapi.ProgrammingError):
            _sql.convert("SELECT %s, %s", [1])
        with self.assertRaises(dbapi.ProgrammingError):
            _sql.convert("SELECT %(a)s", {"b": 1})
        with self.assertRaises(dbapi.ProgrammingError):
            _sql.convert("SELECT 'a%'", [1])  # psycopg: bare % with params is an error
        with self.assertRaises(dbapi.ProgrammingError):
            _sql.convert("SELECT %s", "abc")

    def test_forced_style(self):
        self.assertEqual(_sql.convert("SELECT ?", [1], "qmark")[0], "SELECT $1")
        with self.assertRaises(dbapi.ProgrammingError):
            _sql.convert("SELECT 1", [1], "nope")


class Shapes(unittest.TestCase):
    def test_split(self):
        sh = _sql.shape("SELECT 1; SELECT 'a;b'; -- c;\n SELECT $$x;y$$;")
        self.assertEqual([s.strip() for s in sh.statements], ["SELECT 1", "SELECT 'a;b'", "-- c;\n SELECT $$x;y$$"])
        self.assertIsNone(_sql.shape("SELECT 1;").statements)
        self.assertIsNone(_sql.shape("SELECT 'a;b' /* ; */").statements)

    def test_verbs(self):
        self.assertEqual(_sql.shape("  insert into t values ($1)").verb, "INSERT")
        sh = _sql.shape("WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x RETURNING id")
        self.assertEqual(sh.verb, "INSERT")
        self.assertTrue(sh.returning)
        self.assertFalse(_sql.shape("SELECT 'returning'").returning)
        self.assertTrue(_sql.shape("COMMIT").txn_control)
        self.assertTrue(_sql.shape("SELECT 1; COMMIT").txn_control)
        self.assertFalse(_sql.shape("SELECT CASE WHEN true THEN 1 END; SELECT 2").txn_control)

    def test_plain_insert(self):
        self.assertEqual(_sql.shape("INSERT INTO t (a, B) VALUES ($1, $2)").insert, ("t", ["a", "b"], 2))
        self.assertEqual(_sql.shape('insert into s."T" values ($1,$2);').insert, ('s."T"', None, 2))
        for q in ("INSERT INTO t VALUES ($1) ON CONFLICT DO NOTHING", "INSERT INTO t VALUES ($2, $1)",
                  "INSERT INTO t VALUES ($1) RETURNING id", "INSERT INTO t VALUES ($1 + 1)",
                  "INSERT INTO t (a, a) VALUES ($1, $2)", "INSERT INTO t SELECT $1"):
            self.assertIsNone(_sql.shape(q).insert, q)


class TypeCodes(unittest.TestCase):
    def test_decl_to_oid(self):
        cases = {"INT": (23, None, None), "integer": (23, None, None), "SMALLINT": (21, None, None),
                 "BIGINT": (20, None, None), "NUMERIC(10,2)": (1700, 10, 2), "VARCHAR(20)": (1043, 20, None),
                 "character varying": (1043, None, None), "TIMESTAMPTZ": (1184, None, None),
                 "timestamp(3) with time zone": (1184, 3, None), "JSONB": (3802, None, None),
                 "UUID": (2950, None, None), "INT[]": (1007, None, None), "_int4": (1007, None, None),
                 "double precision": (701, None, None), "BYTEA": (17, None, None), "INTERVAL": (1186, None, None)}
        for decl, want in cases.items():
            self.assertEqual(_types._decl_info(decl), want, decl)

    def test_column_is_7_tuple_with_attributes(self):
        c = _types.Column("n", 1700, None, None, 10, 2, True, "NUMERIC(10,2)")
        self.assertEqual(len(c), 7)
        self.assertEqual(tuple(c), ("n", 1700, None, None, 10, 2, True))
        self.assertEqual((c.name, c.type_code, c.precision, c.scale, c.null_ok), ("n", 1700, 10, 2, True))
        self.assertEqual(c.type_name, "numeric")
        self.assertEqual(c.decl, "NUMERIC(10,2)")


class ErrorMapping(unittest.TestCase):
    def test_lookup(self):
        self.assertIs(errors.lookup("23505"), errors.UniqueViolation)
        self.assertTrue(issubclass(errors.lookup("23505"), dbapi.IntegrityError))
        self.assertIs(errors.lookup("23999"), dbapi.IntegrityError)
        self.assertIs(errors.lookup("22012"), errors.DivisionByZero)
        self.assertTrue(issubclass(errors.lookup("22012"), dbapi.DataError))
        self.assertTrue(issubclass(errors.lookup("40001"), dbapi.OperationalError))
        self.assertTrue(issubclass(errors.lookup("42P01"), dbapi.ProgrammingError))
        self.assertTrue(issubclass(errors.lookup("0A000"), dbapi.NotSupportedError))
        self.assertTrue(issubclass(errors.lookup("25P02"), dbapi.InternalError))
        self.assertTrue(issubclass(errors.lookup("XX000"), dbapi.InternalError))
        self.assertTrue(issubclass(errors.lookup("08006"), dbapi.OperationalError))
        self.assertIs(errors.lookup("HY000"), dbapi.DatabaseError)
        self.assertIs(errors.lookup(None), dbapi.DatabaseError)

    def test_translate_native(self):
        from particledb import errors as ne

        e = errors.translate(ne.QueryError("dup", 23505, "23505"))
        self.assertIsInstance(e, errors.UniqueViolation)
        self.assertEqual((e.sqlstate, e.pgcode, e.code), ("23505", "23505", 23505))
        e = errors.translate(ne.QueryCanceledError("x", 0, "57014", ambiguous=True))
        self.assertIsInstance(e, errors.QueryCanceled)
        self.assertTrue(e.ambiguous)
        self.assertIsInstance(errors.translate(ne.ProtocolError("bad")), dbapi.OperationalError)
        self.assertIsInstance(errors.translate(TypeError("x")), dbapi.ProgrammingError)
        self.assertIsInstance(errors.translate(OverflowError("x")), dbapi.DataError)
        k = KeyboardInterrupt()
        self.assertIs(errors.translate(k), k)


class Adaptation(unittest.TestCase):
    def test_native_types_untouched(self):
        p = [1, "a", None, 1.5, decimal.Decimal("1.0"), b"x", dt.date.today(), uuid.uuid4(), [1], {"a": 1}]
        self.assertIs(_adapt.adapt_all(p), p)

    def test_adapters(self):
        self.assertEqual(_adapt.adapt(dt.timedelta(seconds=1, microseconds=5)), "1000005 microseconds")

        class Color(enum.Enum):
            RED = "r"

        self.assertEqual(_adapt.adapt(Color.RED), "r")
        self.assertEqual(_adapt.adapt({3, 1, 2}), [1, 2, 3])
        with self.assertRaises(dbapi.ProgrammingError):
            _adapt.adapt(object())

        class Money:
            def __init__(self, c):
                self.c = c

        dbapi.register_adapter(Money, lambda m: decimal.Decimal(m.c) / 100)
        self.assertEqual(_adapt.adapt(Money(150)), decimal.Decimal("1.5"))

    def test_numpy_scalars(self):
        try:
            import numpy as np
        except ImportError:
            self.skipTest("numpy not installed")
        self.assertEqual(_adapt.adapt(np.int64(5)), 5)
        self.assertIs(type(_adapt.adapt(np.int64(5))), int)
        self.assertEqual(_adapt.adapt(np.float32(1.5)), 1.5)
        self.assertIs(_adapt.adapt(np.bool_(True)), True)
        self.assertIsNone(_adapt.adapt(np.datetime64("NaT", "ns")))
        self.assertEqual(_adapt.adapt(np.datetime64("2024-01-02T03:04:05")), dt.datetime(2024, 1, 2, 3, 4, 5))
        self.assertEqual(_adapt.adapt(np.array([1, 2])), [1, 2])


class RowFactories(unittest.TestCase):
    class _Cur:
        description = [("a",), ("b",)]

    def test_factories(self):
        c = self._Cur()
        self.assertIsNone(rows.tuple_row(c))
        self.assertEqual(rows.dict_row(c)((1, 2)), {"a": 1, "b": 2})
        nt = rows.namedtuple_row(c)((1, 2))
        self.assertEqual((nt.a, nt.b), (1, 2))
        self.assertEqual(rows.scalar_row(c)((7, 8)), 7)
        self.assertEqual(rows.args_row(lambda a, b: a + b)(c)((1, 2)), 3)
        self.assertEqual(rows.kwargs_row(lambda a, b: a * b)(c)((3, 4)), 12)

        class P:
            def __init__(self, a, b):
                self.a, self.b = a, b

        self.assertEqual(rows.class_row(P)(c)((1, 2)).b, 2)


class IntervalText(unittest.TestCase):
    def test_parse(self):
        P = _arrow.parse_interval
        self.assertEqual(P("1 day"), dt.timedelta(days=1))
        self.assertEqual(P("01:00:00"), dt.timedelta(hours=1))
        self.assertEqual(P("1 year 2 mons 3 days 04:05:06.5"),
                         dt.timedelta(days=365 + 60 + 3, hours=4, minutes=5, seconds=6, microseconds=500000))
        self.assertEqual(P("-1 days +02:00:00"), dt.timedelta(days=-1, hours=2))
        self.assertEqual(P("1 day -00:00:01"), dt.timedelta(days=1, seconds=-1))
        self.assertEqual(P("00:00:00.000001"), dt.timedelta(microseconds=1))
        self.assertEqual(P("P1D"), "P1D")  # not this format: unchanged, never guessed


class Dsn(unittest.TestCase):
    def test_malformed_dsn_redacts_credentials(self):
        import particledb
        import traceback
        import asyncio
        from particledb import aio

        def parse_async(dsn):
            return asyncio.run(aio.connect(dsn))

        for dsn in ("pdb://app:private-credential@local\uff1ahost:invalid",
                    "pdb://app:private-credential@localhost:invalid",
                    "unsupported://app:private-credential@localhost"):
            for parse, error_type in ((particledb.connect, ValueError),
                                      (particledb.ParticleDB.connect, ValueError),
                                      (dbapi.parse_dsn, dbapi.InterfaceError),
                                      (parse_async, dbapi.InterfaceError)):
                with self.subTest(parser=parse):
                    try:
                        parse(dsn)
                    except Exception as exc:
                        self.assertIsInstance(exc, error_type)
                        self.assertIsNone(exc.__context__)
                        self.assertIsNone(exc.__cause__)
                        self.assertNotIn("private-credential", "".join(traceback.format_exception(exc)))
                        while exc is not None:
                            self.assertNotIn("private-credential", str(exc))
                            exc = exc.__context__
                    else:
                        self.fail("malformed connection URL was accepted")

    def test_parse(self):
        kw = dbapi.parse_dsn("pdb://u:p%40ss@db.example:7180?autocommit=true&statement_timeout=2.5&paramstyle=qmark")
        self.assertEqual(kw, {"host": "db.example", "port": 7180, "user": "u", "password": "p@ss",
                              "autocommit": True, "statement_timeout": 2.5, "paramstyle": "qmark"})
        with self.assertRaises(dbapi.InterfaceError):
            dbapi.parse_dsn("postgresql://x")


if __name__ == "__main__":
    unittest.main()
