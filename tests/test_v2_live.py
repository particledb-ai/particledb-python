"""LIVE tests of the protocol-2 client against a real server.

Skipped unless ``PDB_NATIVE_PORT`` is set (``--no-auth`` server on
``PDB_NATIVE_HOST``, default 127.0.0.1). The cross-SDK conformance suite
(``conformance/``) covers the shared contract; this file covers what
is Python-specific: typed values both ways, the built-in decoder against
pyarrow on real results, ``submit`` / per-stream cancel, notices, savepoints,
atomic ``bulk_insert``, and memory-bounded streaming in a fresh process.
Prints ``V2_LIVE_PASS python`` when every case ran.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import os
from pathlib import Path
import subprocess
import sys
import textwrap
import time
import unittest
import uuid

import particledb
from particledb import _arrow
from particledb.errors import QueryCanceledError, QueryError

PORT = os.environ.get("PDB_NATIVE_PORT")
HOST = os.environ.get("PDB_NATIVE_HOST", "127.0.0.1")
D = decimal.Decimal


def conn(**kw) -> particledb.Connection:
    return particledb.connect(host=HOST, port=int(PORT), **kw)


@unittest.skipUnless(PORT, "PDB_NATIVE_PORT not set (live native-wire test)")
class V2Live(unittest.TestCase):
    ran = 0

    def setUp(self) -> None:
        self.c = conn()
        self.t = f"v2l_{uuid.uuid4().hex[:8]}"

    def tearDown(self) -> None:
        try:
            self.c.execute(f"DROP TABLE IF EXISTS {self.t}")
        finally:
            self.c.close()
        V2Live.ran += 1

    def test_typed_values_round_trip(self) -> None:
        c, t = self.c, self.t
        c.execute(f"CREATE TABLE {t} (id int8 PRIMARY KEY, n numeric, f float8, r float4, b bytea, d date, "
                  f"ts timestamp, tz timestamptz, u uuid, j jsonb, tx text[], i int4[], v vector(3), ok bool)")
        tzv = dt.datetime(2024, 7, 1, 2, 0, 0, 500000, tzinfo=dt.timezone(dt.timedelta(hours=2)))
        u = uuid.uuid4()
        row = [1, D("-12345678901234567890.0001"), float("inf"), particledb.Float4(1.5), bytes(range(256)),
               dt.date(2024, 2, 29), dt.datetime(1999, 12, 31, 23, 59, 59, 999999), tzv, u,
               particledb.Jsonb({"k": [1, None]}), ["a", None, 'c,"d"'], [1, None, 3], "[0.5,-1,0]", True]
        c.execute(f"INSERT INTO {t} VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)", row)
        got = c.execute(f"SELECT n, f, r, b, d, ts, tz, u, j, tx, i, v, ok FROM {t} WHERE id = $1", [1]).rows[0]
        self.assertEqual(got[0], D("-12345678901234567890.0001"))
        self.assertEqual(str(got[0]), "-12345678901234567890.0001")
        self.assertEqual(got[1], float("inf"))
        self.assertEqual(got[2], 1.5)
        self.assertEqual(got[3], bytes(range(256)))
        self.assertEqual(got[4], dt.date(2024, 2, 29))
        self.assertEqual(got[5], dt.datetime(1999, 12, 31, 23, 59, 59, 999999))
        self.assertEqual(got[6], tzv)  # the same instant, now in UTC
        self.assertEqual(got[6].tzinfo, dt.timezone.utc)
        self.assertEqual(got[7], u)
        self.assertEqual(json.loads(got[8]), {"k": [1, None]})
        self.assertEqual(got[9], ["a", None, 'c,"d"'])
        self.assertEqual(got[10], [1, None, 3])
        self.assertEqual(got[11], [0.5, -1.0, 0.0])
        self.assertIs(got[12], True)
        # NaN in, NaN out; numeric scale kept per value (CAP_NUMERIC_TEXT)
        r = c.execute("SELECT 'NaN'::float8, x FROM (VALUES (0.52::numeric), (1.123456::numeric)) t(x) "
                      "ORDER BY x").rows
        self.assertNotEqual(r[0][0], r[0][0])
        self.assertEqual([str(x[1]) for x in r], ["0.52", "1.123456"])  # not padded to the batch's max scale
        self.assertEqual(str(c.scalar("SELECT 1.50::numeric(10,2)")), "1.50")

    def test_builtin_decoder_equals_pyarrow_on_server_results(self) -> None:
        if not _arrow.HAVE_PYARROW:
            self.skipTest("pyarrow not installed")
        q = ("SELECT g, g::float8 / 3, 'r' || g, g % 2 = 0, (g || '.5')::numeric, "
             "DATE '2024-01-01' + g, ARRAY[g, NULL] FROM generate_series(1, 70000) g")
        with conn(use_pyarrow=False) as a, conn(use_pyarrow=True) as b:
            ra, rb = a.execute(q), b.execute(q)
            self.assertEqual(ra.columns, rb.columns)
            self.assertEqual(ra.rows, rb.rows)
            self.assertEqual(len(ra.rows), 70000)  # past one server slice (65,536 rows)

    def test_submit_and_cancel_one_stream_of_many(self) -> None:
        slow = self.c.submit("SELECT pg_sleep(30)")
        fast = [self.c.submit(f"SELECT {i}") for i in range(20)]
        self.assertEqual([p.result(10).scalar() for p in fast], list(range(20)))
        self.assertTrue(slow.cancel())
        t0 = time.monotonic()
        with self.assertRaises(QueryCanceledError) as cm:
            slow.result(10)
        self.assertEqual(cm.exception.sqlstate, "57014")
        self.assertLess(time.monotonic() - t0, 5)
        self.assertEqual(self.c.scalar("SELECT 7"), 7)

    def test_notices_are_delivered(self) -> None:
        seen = []
        with conn(notice_handler=seen.append) as c:
            c.execute(f"CREATE TABLE {self.t} (a int8)")
            r = c.execute(f"CREATE INDEX {self.t}_i ON {self.t} USING hash (a)")
            self.assertTrue(r.notices, "CREATE INDEX USING hash answered no notice")
            self.assertIn("hash", r.notices[0].message)
            self.assertEqual(seen, r.notices)

    def test_savepoints_and_isolation(self) -> None:
        c, t = self.c, self.t
        c.execute(f"CREATE TABLE {t} (id int8 PRIMARY KEY)")
        with c.transaction():
            c.execute(f"INSERT INTO {t} VALUES (1)")
            c.savepoint("s1")
            c.execute(f"INSERT INTO {t} VALUES (2)")
            with self.assertRaises(QueryError) as cm:
                c.execute(f"INSERT INTO {t} VALUES (1)")
            self.assertEqual(cm.exception.sqlstate, "23505")
            c.rollback_to("s1")
            c.release_savepoint("s1")
            self.assertTrue(c.in_transaction)
        self.assertFalse(c.in_transaction)
        self.assertEqual([r[0] for r in c.execute(f"SELECT id FROM {t} ORDER BY id").rows], [1])
        with c.transaction(isolation="serializable"):
            self.assertEqual(c.scalar("SHOW transaction_isolation"), "serializable")
        self.assertEqual(c.scalar("SHOW transaction_isolation"), "read committed")

    def test_bulk_insert_is_atomic(self) -> None:
        c, t = self.c, self.t
        c.execute(f"CREATE TABLE {t} (id int8 PRIMARY KEY, v text, d date)")
        self.assertEqual(c.bulk_insert(t, ["id", "v", "d"], [(i, f"v{i}", dt.date(2024, 1, 1)) for i in range(1000)]),
                         1000)
        with self.assertRaises(QueryError) as cm:
            c.bulk_insert(t, ["id", "v", "d"], [(5000, "new", None), (1, "dup", None)])
        self.assertEqual(cm.exception.sqlstate, "23505")
        self.assertEqual(c.scalar(f"SELECT count(*) FROM {t}"), 1000)  # nothing of the failed load
        self.assertEqual(c.scalar(f"SELECT d FROM {t} WHERE id = 7"), dt.date(2024, 1, 1))

    def test_prepared_handle(self) -> None:
        with self.c.prepare("SELECT $1::int8 * 2") as ps:
            self.assertTrue(ps.server_prepared)
            self.assertEqual(ps.param_count, 1)
            self.assertEqual([ps.execute([i]).scalar() for i in range(5)], [0, 2, 4, 6, 8])
            self.assertEqual([r[0] for r in ps.stream([21])], [42])

    def test_stream_is_memory_bounded_in_a_fresh_process(self) -> None:
        code = Path(__file__).with_name("peak_memory.py").read_text() + "\n" + textwrap.dedent(f"""
            import json, sys
            import particledb
            n = s = 0
            with particledb.connect(host={HOST!r}, port={int(PORT)}) as c:
                for row in c.stream("SELECT g, repeat('x', 100) FROM generate_series(1, 1000000) g"):
                    n += 1; s += row[0]
            print(json.dumps({{"n": n, "s": s, "mib": peak_rss_mib()}}))
        """)
        for env in ({}, {"PDB_PY_DECODER": "pyarrow"}):
            p = subprocess.run([sys.executable, "-c", code], env=dict(os.environ, **env), capture_output=True,
                               timeout=120, cwd=os.path.dirname(os.path.dirname(particledb.__file__)))
            self.assertEqual(p.returncode, 0, p.stderr.decode()[-500:])
            out = json.loads(p.stdout.decode().strip().splitlines()[-1])
            self.assertEqual((out["n"], out["s"]), (1000000, 500000500000))
            # ~110 MB of row data; bounded means far below it plus the runtime.
            self.assertLess(out["mib"], 160, (env, out))

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.ran >= 8:
            print("V2_LIVE_PASS python")


if __name__ == "__main__":
    unittest.main()
