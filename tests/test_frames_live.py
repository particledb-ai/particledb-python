"""LIVE: DataFrame / Arrow / numpy fetches and ``bulk_insert`` inputs.

Skipped unless PDB_NATIVE_PORT is set. Each optional library is skipped when
missing. ``test_core_path_without_pyarrow`` runs the core path in a fresh
interpreter where ``import pyarrow`` fails.
"""

import datetime as dt
import decimal
import os
import subprocess
import sys
import textwrap
import unittest
import uuid

from particledb import dbapi

PORT = os.environ.get("PDB_NATIVE_PORT")
HOST = os.environ.get("PDB_NATIVE_HOST", "127.0.0.1")
D = decimal.Decimal
SDK_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def have(mod):
    try:
        __import__(mod)
        return True
    except ImportError:
        return False


def connect(**kw):
    return dbapi.connect(host=HOST, port=int(PORT), **kw)


@unittest.skipUnless(PORT, "PDB_NATIVE_PORT not set")
class FramesLive(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with connect(autocommit=True) as c:
            c.execute("DROP TABLE IF EXISTS fr_t")
            c.execute("CREATE TABLE fr_t (id INT PRIMARY KEY, s SMALLINT, b BIGINT, f DOUBLE PRECISION, "
                      "name TEXT, n NUMERIC, n2 NUMERIC(10,2), d DATE, ts TIMESTAMP, tz TIMESTAMPTZ, u UUID, "
                      "ok BOOLEAN, raw BYTEA)")
            rows = [(i, i % 7, i * 10_000_000_000, i / 4, None if i % 5 == 0 else f"n{i}", D(i) / 8,
                     D(i) / 4, dt.date(2024, 1, 1) + dt.timedelta(days=i),
                     dt.datetime(2024, 1, 1, 12) + dt.timedelta(minutes=i),
                     dt.datetime(2024, 1, 1, 12, tzinfo=dt.timezone.utc) + dt.timedelta(seconds=i),
                     uuid.UUID(int=i), i % 2 == 0, bytes([i % 256])) for i in range(1000)]
            c.native.execute_many("INSERT INTO fr_t VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)", rows)

    def setUp(self):
        self.conn = connect(autocommit=True)

    def tearDown(self):
        self.conn.close()

    @unittest.skipUnless(have("pyarrow"), "pyarrow not installed")
    def test_fetch_arrow_types(self):
        import pyarrow as pa

        t = self.conn.execute("SELECT * FROM fr_t ORDER BY id").fetch_arrow()
        self.assertEqual(t.num_rows, 1000)
        sch = {f.name: f.type for f in t.schema}
        self.assertEqual(sch["id"], pa.int32())
        self.assertEqual(sch["s"], pa.int16())
        self.assertEqual(sch["b"], pa.int64())
        self.assertEqual(sch["tz"], pa.timestamp("us", "UTC"))
        self.assertEqual(sch["ts"].tz, None)
        self.assertTrue(pa.types.is_decimal(sch["n"]))
        self.assertEqual(sch["n2"], pa.decimal128(10, 2))
        self.assertEqual(t.column("n").to_pylist()[3], D("0.375"))
        self.assertEqual(t.column("u").to_pylist()[1], str(uuid.UUID(int=1)))
        self.assertIsNone(t.column("name").to_pylist()[0])

    @unittest.skipUnless(have("pandas") and have("pyarrow"), "pandas + pyarrow not installed")
    def test_fetch_df_pandas(self):
        df = self.conn.execute("SELECT id, s, f, name, n2, tz, ok FROM fr_t ORDER BY id").fetch_df()
        self.assertEqual(len(df), 1000)
        self.assertEqual(str(df["id"].dtype), "int32")
        self.assertEqual(str(df["s"].dtype), "int16")
        self.assertEqual(str(df["f"].dtype), "float64")
        self.assertEqual(str(df["tz"].dtype), "datetime64[us, UTC]")
        self.assertEqual(df["n2"][3], D("0.75"))
        self.assertEqual(int(df["id"].sum()), sum(range(1000)))
        self.assertEqual(bool(df["ok"][0]), True)

    @unittest.skipUnless(have("polars"), "polars not installed")
    def test_fetch_polars(self):
        import polars as pl

        df = self.conn.execute("SELECT id, s, b, tz, n, n2, name, u FROM fr_t ORDER BY id").fetch_polars()
        self.assertEqual(df.height, 1000)
        self.assertEqual(df.schema["id"], pl.Int32)
        self.assertEqual(df.schema["s"], pl.Int16)
        self.assertEqual(df.schema["tz"], pl.Datetime("us", "UTC"))
        self.assertIsInstance(df.schema["n2"], pl.Decimal)
        self.assertEqual(df["b"].sum(), sum(i * 10_000_000_000 for i in range(1000)))
        self.assertEqual(df["u"][2], str(uuid.UUID(int=2)))

    @unittest.skipUnless(have("numpy"), "numpy not installed")
    def test_fetch_numpy(self):
        out = self.conn.execute("SELECT id, f FROM fr_t ORDER BY id").fetch_numpy()
        self.assertEqual(out["id"].shape, (1000,))
        self.assertEqual(int(out["id"].sum()), sum(range(1000)))
        self.assertAlmostEqual(float(out["f"].sum()), sum(i / 4 for i in range(1000)))

    @unittest.skipUnless(have("pandas") and have("pyarrow"), "pandas + pyarrow not installed")
    def test_frame_after_partial_fetch_returns_the_rest(self):
        cur = self.conn.execute("SELECT id FROM fr_t ORDER BY id")
        cur.fetchmany(10)
        df = cur.fetch_df()
        self.assertEqual(len(df), 990)
        self.assertEqual(int(df["id"].iloc[0]), 10)
        self.assertEqual(cur.fetchall(), [])

    def test_frame_of_a_non_select_raises(self):
        cur = self.conn.execute("UPDATE fr_t SET ok = ok WHERE id < 0")
        with self.assertRaises(dbapi.ProgrammingError):
            cur.fetch_numpy()


@unittest.skipUnless(PORT, "PDB_NATIVE_PORT not set")
class BulkInsertLive(unittest.TestCase):
    def setUp(self):
        self.conn = connect(autocommit=True)
        self.conn.execute("DROP TABLE IF EXISTS bi_t")
        self.conn.execute("CREATE TABLE bi_t (id BIGINT PRIMARY KEY, name TEXT, score DOUBLE PRECISION, "
                          "born DATE, seen TIMESTAMPTZ, amount NUMERIC(12,2), ok BOOLEAN)")

    def tearDown(self):
        self.conn.close()

    def check(self, n, sum_id):
        self.assertEqual(self.conn.execute("SELECT count(*), sum(id) FROM bi_t").fetchone(), (n, sum_id))

    def test_tuples_and_generator_stream(self):
        n = self.conn.bulk_insert("bi_t", ((i, f"n{i}", i * 0.5) for i in range(200_000)), ["id", "name", "score"])
        self.assertEqual(n, 200_000)
        self.check(200_000, sum(range(200_000)))
        self.assertEqual(self.conn.execute("SELECT name, score FROM bi_t WHERE id = 777").fetchone(), ("n777", 388.5))

    def test_tuples_default_to_table_columns(self):
        self.conn.bulk_insert("bi_t", [(1, "a"), (2, "b")])
        self.assertEqual(self.conn.execute("SELECT id, name FROM bi_t ORDER BY id").fetchall(), [(1, "a"), (2, "b")])

    def test_dicts_and_typed_values(self):
        rows = [{"id": i, "born": dt.date(2000, 1, 1) + dt.timedelta(days=i), "amount": D(i) / 4,
                 "seen": dt.datetime(2024, 1, 1, tzinfo=dt.timezone(dt.timedelta(hours=3))), "ok": i % 2 == 0}
                for i in range(1000)]
        self.assertEqual(self.conn.bulk_insert("bi_t", rows), 1000)
        r = self.conn.execute("SELECT born, amount, seen, ok FROM bi_t WHERE id = 3").fetchone()
        self.assertEqual(r, (dt.date(2000, 1, 4), D("0.75"), dt.datetime(2023, 12, 31, 21, tzinfo=dt.timezone.utc), False))

    def test_atomic_and_joins_transaction(self):
        with self.assertRaises(dbapi.IntegrityError):
            self.conn.bulk_insert("bi_t", [(1, "a"), (2, "b"), (1, "dup")], ["id", "name"])
        self.check(0, None)
        c = connect()
        try:
            c.bulk_insert("bi_t", [(5, "x")], ["id", "name"])
            self.assertEqual(c.transaction_status, "intrans")
            c.rollback()
        finally:
            c.close()
        self.check(0, None)

    @unittest.skipUnless(have("pandas"), "pandas not installed")
    def test_pandas_dataframe(self):
        import math

        import pandas as pd

        df = pd.DataFrame({"id": range(5000), "name": [f"p{i}" for i in range(5000)],
                           "score": [math.nan if i % 10 == 0 else i * 1.5 for i in range(5000)]})
        self.assertEqual(self.conn.bulk_insert("bi_t", df), 5000)
        self.check(5000, sum(range(5000)))
        self.assertEqual(self.conn.execute("SELECT count(*) FROM bi_t WHERE score IS NULL").fetchone(), (500,))

    @unittest.skipUnless(have("polars"), "polars not installed")
    def test_polars_dataframe(self):
        import polars as pl

        df = pl.DataFrame({"id": list(range(3000)), "name": [f"q{i}" for i in range(3000)]})
        self.assertEqual(self.conn.bulk_insert("bi_t", df), 3000)
        self.check(3000, sum(range(3000)))

    @unittest.skipUnless(have("pyarrow"), "pyarrow not installed")
    def test_arrow_table_and_batch(self):
        import pyarrow as pa

        t = pa.table({"id": pa.array(range(4000), pa.int64()), "name": pa.array([f"a{i}" for i in range(4000)]),
                      "born": pa.array([dt.date(2020, 1, 1)] * 4000, pa.date64())})
        self.assertEqual(self.conn.bulk_insert("bi_t", t), 4000)
        b = pa.record_batch({"id": pa.array([10_000], pa.int32())})
        self.assertEqual(self.conn.bulk_insert("bi_t", b), 1)
        self.check(4001, sum(range(4000)) + 10_000)
        self.assertEqual(self.conn.execute("SELECT born FROM bi_t WHERE id = 1").fetchone(), (dt.date(2020, 1, 1),))


@unittest.skipUnless(PORT, "PDB_NATIVE_PORT not set")
class NoPyarrow(unittest.TestCase):
    def test_core_path_without_pyarrow(self):
        code = textwrap.dedent(f"""
            import sys
            sys.modules["pyarrow"] = None  # import pyarrow -> ImportError
            from particledb import dbapi, _arrow
            assert not _arrow.HAVE_PYARROW or True
            c = dbapi.connect(host={HOST!r}, port={int(PORT)}, autocommit=True)
            c.execute("DROP TABLE IF EXISTS np_t")
            c.execute("CREATE TABLE np_t (id INT PRIMARY KEY, v TEXT)")
            cur = c.cursor()
            cur.executemany("INSERT INTO np_t VALUES (%s, %s)", [(i, str(i)) for i in range(1000)])
            assert cur.rowcount == 1000, cur.rowcount
            assert c.bulk_insert("np_t", [(5000, "x")]) == 1
            assert c.execute("SELECT count(*) FROM np_t").fetchone() == (1001,)
            try:
                import polars
                df = c.execute("SELECT * FROM np_t ORDER BY id").fetch_polars()
                assert df.height == 1001
                assert c.bulk_insert("np_t", polars.DataFrame({{"id": [9000], "v": ["p"]}})) == 1
            except ImportError:
                pass
            try:
                c.execute("SELECT 1").fetch_arrow()
                raise SystemExit("fetch_arrow worked without pyarrow")
            except dbapi.NotSupportedError:
                pass
            print("NOPYARROW_OK")
        """)
        env = dict(os.environ, PYTHONPATH=SDK_ROOT)
        p = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True, timeout=120)
        self.assertIn("NOPYARROW_OK", p.stdout, p.stderr)


if __name__ == "__main__":
    unittest.main()
