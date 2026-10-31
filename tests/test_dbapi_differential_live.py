"""LIVE differential: the same DB-API program against ParticleDB (particledb.dbapi)
and PostgreSQL 17 (psycopg 3), results compared (the pgjdbc-style differential).

Needs PDB_NATIVE_PORT (a --no-auth ParticleDB) and PDB_PG_DSN (a THROWAWAY
PostgreSQL, e.g. ``host=127.0.0.1 port=27330 user=postgres dbname=postgres``);
skipped otherwise, or when psycopg is not installed. Each scenario is ONE
function taking the driver module's connect; it returns plain data that must be
equal on both. Known server differences are expectedFailure tests naming the
server bug, so they stay visible and turn into an unexpected success when fixed.
"""

import datetime as dt
import decimal
import os
import unittest
import uuid

from particledb import dbapi

PORT = os.environ.get("PDB_NATIVE_PORT")
HOST = os.environ.get("PDB_NATIVE_HOST", "127.0.0.1")
PG_DSN = os.environ.get("PDB_PG_DSN")
try:
    import psycopg
except ImportError:  # pragma: no cover
    psycopg = None
D = decimal.Decimal


def pdb_connect(**kw):
    return dbapi.connect(host=HOST, port=int(PORT), **kw)


def pg_connect(**kw):
    c = psycopg.connect(PG_DSN)
    if kw.get("autocommit"):
        c.autocommit = True
    return c


def _err_family(e):
    """The PEP 249 class name an error belongs to (both drivers' trees)."""
    for name in ("IntegrityError", "DataError", "ProgrammingError", "OperationalError", "NotSupportedError",
                 "InternalError", "InterfaceError", "DatabaseError"):
        if any(c.__name__ == name for c in type(e).__mro__):
            return name
    return type(e).__name__


def _status(conn):
    if hasattr(conn, "transaction_status"):
        return conn.transaction_status
    return {0: "idle", 2: "intrans", 3: "inerror"}[int(conn.info.transaction_status)]


@unittest.skipUnless(PORT and PG_DSN and psycopg, "needs PDB_NATIVE_PORT, PDB_PG_DSN and psycopg")
class Differential(unittest.TestCase):
    TABLE = ("CREATE TABLE diff_t (id INT PRIMARY KEY, i2 SMALLINT, i8 BIGINT, n NUMERIC(12,3), r REAL, "
             "f DOUBLE PRECISION, s TEXT, vc VARCHAR(16), b BOOLEAN, d DATE, ts TIMESTAMP, tz TIMESTAMPTZ, "
             "u UUID, by BYTEA, j JSON, jb JSONB, ai INT[], at TEXT[], iv INTERVAL)")

    def run_both(self, scenario):
        out = []
        for connect in (pdb_connect, pg_connect):
            with connect(autocommit=True) as c:
                c.execute("DROP TABLE IF EXISTS diff_child")
                c.execute("DROP TABLE IF EXISTS diff_t")
                c.execute(self.TABLE)
                c.execute("CREATE TABLE diff_child (id INT PRIMARY KEY, pid INT REFERENCES diff_t (id), "
                          "q INT CHECK (q > 0))")
            out.append(scenario(connect))
        return out

    def assertSame(self, scenario):
        pdb, pg = self.run_both(scenario)
        self.assertEqual(pdb, pg)
        return pdb

    ROW = (1, 2, 3_000_000_000, D("12.345"), 1.5, 2.25, "text", "vc", True, dt.date(2024, 2, 29),
           dt.datetime(2024, 2, 29, 1, 2, 3, 456789),
           dt.datetime(2024, 2, 29, 1, 2, 3, 456789, tzinfo=dt.timezone(dt.timedelta(hours=-5))),
           uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"), b"\x00\x01\xfe", None, None,
           [1, None, 3], ["a", "b,c", None], dt.timedelta(days=1, hours=2, microseconds=7))

    def _insert_row(self, c, mod):
        Jsonb = getattr(mod, "Jsonb")
        Json = getattr(mod, "Json")
        row = list(self.ROW)
        row[14] = Json({"a": [1, 2]})
        row[15] = Jsonb({"b": {"c": None}})
        cur = c.cursor()
        cur.execute("INSERT INTO diff_t VALUES (" + ", ".join(["%s"] * 19) + ")", row)
        return cur.rowcount

    def test_type_round_trip(self):
        def sc(connect):
            mod = dbapi if connect is pdb_connect else __import__("psycopg.types.json", fromlist=["Json"])
            with connect() as c:
                n = self._insert_row(c, mod)
                cur = c.cursor()
                cur.execute("SELECT * FROM diff_t")
                return n, cur.fetchall()
        rows = self.assertSame(sc)
        self.assertEqual(rows[1][0][14], {"a": [1, 2]})

    def test_description_type_codes_of_table_columns(self):
        def sc(connect):
            with connect(autocommit=True) as c:
                cur = c.cursor()
                cur.execute("SELECT * FROM diff_t")
                return [(d[0], d[1], d[4], d[5]) for d in cur.description]
        self.assertSame(sc)

    def test_rowcounts(self):
        def sc(connect):
            with connect() as c:
                cur = c.cursor()
                out = []
                cur.execute("INSERT INTO diff_t (id, s) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
                out.append(cur.rowcount)
                cur.execute("UPDATE diff_t SET s = 'z' WHERE id > 1")
                out.append(cur.rowcount)
                cur.execute("SELECT * FROM diff_t")
                out.append(cur.rowcount)
                cur.execute("DELETE FROM diff_t WHERE id = 3")
                out.append(cur.rowcount)
                cur.execute("SELECT * FROM diff_t WHERE false")
                out.append((cur.rowcount, cur.fetchall(), [d[0] for d in cur.description][:2]))
                cur.executemany("INSERT INTO diff_t (id) VALUES (%s)", [(10,), (11,), (12,)])
                out.append(cur.rowcount)
                cur.executemany("UPDATE diff_t SET s = %s WHERE id = %s", [("u", 10), ("v", 99)])
                out.append(cur.rowcount)
                return out
        self.assertSame(sc)

    def test_failed_transaction_semantics(self):
        def sc(connect):
            out = []
            with connect() as c:
                cur = c.cursor()
                cur.execute("INSERT INTO diff_t (id) VALUES (1)")
                out.append(_status(c))
                try:
                    cur.execute("INSERT INTO diff_t (id) VALUES (1)")
                except Exception as e:
                    out.append((_err_family(e), e.sqlstate))
                out.append(_status(c))
                try:
                    cur.execute("SELECT 1")
                except Exception as e:
                    out.append((_err_family(e), e.sqlstate))
                c.commit()
                out.append(_status(c))
                cur.execute("SELECT count(*) FROM diff_t")
                out.append(cur.fetchone())
                c.rollback()
            return out
        r = self.assertSame(sc)
        self.assertEqual(r[0], "intrans")

    def test_error_classes(self):
        stmts = ["INSERT INTO diff_t (id) VALUES (NULL)",
                 "INSERT INTO diff_child (id, pid, q) VALUES (1, 999, 1)",
                 "INSERT INTO diff_child (id, pid, q) VALUES (1, NULL, -1)",
                 "SELECT 1/0",
                 "SELECT * FROM diff_no_such",
                 "SELECT no_such FROM diff_t",
                 "SELEC 1",
                 "SELECT 'abc'::int"]

        def sc(connect):
            out = []
            with connect(autocommit=True) as c:
                for s in stmts:
                    try:
                        c.execute(s)
                        out.append(None)
                    except Exception as e:
                        out.append((_err_family(e), e.sqlstate))
            return out
        self.assertSame(sc)

    def test_multi_statement_nextset(self):
        def sc(connect):
            with connect(autocommit=True) as c:
                cur = c.cursor()
                cur.execute("INSERT INTO diff_t (id) VALUES (1); SELECT id FROM diff_t; "
                            "UPDATE diff_t SET s = 'x'; SELECT 'a' AS k, 2 AS v")
                out = []
                while True:
                    out.append((cur.rowcount, cur.fetchall() if cur.description else None))
                    if not cur.nextset():
                        break
                return out
        self.assertSame(sc)

    def test_multi_statement_atomicity(self):
        def sc(connect):
            with connect(autocommit=True) as c:
                try:
                    c.execute("INSERT INTO diff_t (id) VALUES (1); INSERT INTO diff_t (id) VALUES (1)")
                except Exception as e:
                    err = _err_family(e)
                cur = c.execute("SELECT count(*) FROM diff_t")
                return err, cur.fetchone(), _status(c)
        self.assertSame(sc)

    def test_savepoints_via_transaction_blocks(self):
        def sc(connect):
            with connect() as c:
                with c.transaction():
                    c.execute("INSERT INTO diff_t (id) VALUES (1)")
                    try:
                        with c.transaction():
                            c.execute("INSERT INTO diff_t (id) VALUES (2)")
                            c.execute("INSERT INTO diff_t (id) VALUES (1)")
                    except Exception as e:
                        fam = _err_family(e)
                    c.execute("INSERT INTO diff_t (id) VALUES (3)")
                cur = c.execute("SELECT id FROM diff_t ORDER BY id")
                return fam, cur.fetchall(), _status(c)
        self.assertSame(sc)

    def test_isolation_show(self):
        def sc(connect):
            out = []
            with connect() as c:
                enum = dbapi.IsolationLevel if connect is pdb_connect else psycopg.IsolationLevel
                for lvl in ("SERIALIZABLE", "REPEATABLE_READ", "READ_COMMITTED"):
                    c.isolation_level = getattr(enum, lvl)
                    out.append(c.execute("SHOW transaction_isolation").fetchone()[0])
                    c.rollback()
            return out
        self.assertSame(sc)

    def test_fetch_and_scroll(self):
        def sc(connect):
            with connect(autocommit=True) as c:
                cur = c.cursor()
                cur.execute("SELECT g FROM generate_series(1, 6) AS g")
                out = [cur.fetchone(), cur.fetchmany(2)]
                cur.scroll(1)
                out.append(cur.fetchone())
                cur.scroll(0, "absolute")
                out.append(cur.fetchall())
                out.append(cur.fetchone())
                try:
                    cur.scroll(5)
                except Exception as e:
                    out.append(type(e).__name__)
                return out
        self.assertSame(sc)

    def test_null_and_empty(self):
        def sc(connect):
            with connect(autocommit=True) as c:
                cur = c.cursor()
                cur.execute("INSERT INTO diff_t (id) VALUES (1)")
                cur.execute("SELECT s, ai, jb, u FROM diff_t")
                return cur.fetchall()
        self.assertSame(sc)

    def test_script_with_transaction_control(self):
        def sc(connect):
            with connect() as c:
                c.execute("INSERT INTO diff_t (id) VALUES (1); ROLLBACK")
                c.commit()
                return c.execute("SELECT count(*) FROM diff_t").fetchone(), _status(c)
        self.assertSame(sc)

    def test_bound_parameter_types(self):
        """Declared native parameter types agree with PostgreSQL without a column context."""
        vals = [b"\x01", 3_000_000_000, 1.5, dt.date(2024, 1, 1), dt.datetime(2024, 1, 1),
                uuid.UUID(int=1)]

        def sc(connect):
            with connect(autocommit=True) as c:
                return [c.execute("SELECT pg_typeof(%s)::text", [v]).fetchone()[0] for v in vals]
        self.assertSame(sc)

    def test_unconstrained_numeric_column_keeps_scale(self):
        """Unconstrained numeric columns retain the value's scale."""
        def sc(connect):
            with connect(autocommit=True) as c:
                c.execute("DROP TABLE IF EXISTS diff_num")
                c.execute("CREATE TABLE diff_num (d NUMERIC)")
                c.execute("INSERT INTO diff_num VALUES (1.50)")
                return [str(r[0]) for r in c.execute("SELECT d FROM diff_num").fetchall()]
        self.assertSame(sc)


if __name__ == "__main__":
    unittest.main()
