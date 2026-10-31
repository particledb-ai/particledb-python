"""LIVE: the PEP 249 driver (particledb.dbapi) against a running server.

Skipped unless PDB_NATIVE_PORT is set (a --no-auth server on PDB_NATIVE_HOST,
default 127.0.0.1). Every check asserts the server-side effect (rows really
written or not), not only the absence of an error.
"""

import datetime as dt
import decimal
import os
import threading
import unittest
import uuid

from particledb import dbapi
from particledb.dbapi import errors

PORT = os.environ.get("PDB_NATIVE_PORT")
HOST = os.environ.get("PDB_NATIVE_HOST", "127.0.0.1")
D = decimal.Decimal


def connect(**kw):
    return dbapi.connect(host=HOST, port=int(PORT), **kw)


def ddl(*stmts):
    with connect(autocommit=True) as c:
        for s in stmts:
            c.execute(s)


@unittest.skipUnless(PORT, "PDB_NATIVE_PORT not set")
class Pep249Live(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ddl("DROP TABLE IF EXISTS dbapi_t",
            "CREATE TABLE dbapi_t (id INT PRIMARY KEY, name TEXT, score NUMERIC(10,2), born DATE, "
            "seen TIMESTAMPTZ, tag UUID, doc JSONB, data BYTEA, tags TEXT[], dur INTERVAL, flag BOOLEAN)")

    def setUp(self):
        ddl("DELETE FROM dbapi_t")
        self.conn = connect()

    def tearDown(self):
        self.conn.close()

    def count(self, where="TRUE"):
        with connect(autocommit=True) as c:
            return c.execute(f"SELECT count(*) FROM dbapi_t WHERE {where}").fetchone()[0]

    # ── transactions ────────────────────────────────────────────────────

    def test_autocommit_off_by_default_and_lazy_begin(self):
        self.assertFalse(self.conn.autocommit)
        self.assertEqual(self.conn.transaction_status, "idle")
        cur = self.conn.cursor()
        cur.execute("INSERT INTO dbapi_t (id, name) VALUES (%s, %s)", (1, "a"))
        self.assertEqual(self.conn.transaction_status, "intrans")  # the server says so
        self.assertEqual(self.count(), 0)                            # not visible yet
        self.conn.commit()
        self.assertEqual(self.conn.transaction_status, "idle")
        self.assertEqual(self.count(), 1)

    def test_rollback_discards(self):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO dbapi_t (id) VALUES (%s)", (1,))
        self.conn.rollback()
        self.assertEqual(self.count(), 0)
        self.conn.rollback()  # nothing open: a no-op
        self.conn.commit()

    def test_autocommit_true(self):
        self.conn.autocommit = True
        self.conn.cursor().execute("INSERT INTO dbapi_t (id) VALUES (%s)", (1,))
        self.assertEqual(self.conn.transaction_status, "idle")
        self.assertEqual(self.count(), 1)

    def test_autocommit_change_refused_in_transaction(self):
        self.conn.cursor().execute("SELECT 1")
        with self.assertRaises(dbapi.ProgrammingError):
            self.conn.autocommit = True
        self.conn.rollback()
        self.conn.autocommit = True

    def test_failed_transaction_then_commit_rolls_back(self):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO dbapi_t (id) VALUES (1)")
        with self.assertRaises(errors.UniqueViolation):
            cur.execute("INSERT INTO dbapi_t (id) VALUES (1)")
        self.assertEqual(self.conn.transaction_status, "inerror")
        with self.assertRaises(errors.InFailedSqlTransaction):
            cur.execute("SELECT 1")
        self.conn.commit()  # psycopg / pgjdbc parity: returns, the server rolled back
        self.assertEqual(self.count(), 0)

    def test_raise_on_silent_rollback(self):
        c = connect(raise_on_silent_rollback=True)
        try:
            cur = c.cursor()
            cur.execute("INSERT INTO dbapi_t (id) VALUES (1)")
            with self.assertRaises(dbapi.IntegrityError):
                cur.execute("INSERT INTO dbapi_t (id) VALUES (1)")
            with self.assertRaises(errors.InFailedSqlTransaction):
                c.commit()
        finally:
            c.close()
        self.assertEqual(self.count(), 0)

    def test_context_manager_commits_or_rolls_back(self):
        with connect() as c:
            c.execute("INSERT INTO dbapi_t (id) VALUES (1)")
        self.assertEqual(self.count(), 1)
        with self.assertRaises(ZeroDivisionError):
            with connect() as c:
                c.execute("INSERT INTO dbapi_t (id) VALUES (2)")
                1 / 0
        self.assertEqual(self.count(), 1)
        self.assertTrue(c.closed)

    def test_transaction_blocks_and_savepoints(self):
        conn = self.conn
        with conn.transaction():
            conn.execute("INSERT INTO dbapi_t (id) VALUES (1)")
            with self.assertRaises(ValueError):
                with conn.transaction():
                    conn.execute("INSERT INTO dbapi_t (id) VALUES (2)")
                    raise ValueError
            with conn.transaction():
                conn.execute("INSERT INTO dbapi_t (id) VALUES (3)")
            with conn.transaction():
                conn.execute("INSERT INTO dbapi_t (id) VALUES (4)")
                raise dbapi.Rollback()
        self.assertEqual(conn.transaction_status, "idle")
        ids = [r[0] for r in connect(autocommit=True).execute("SELECT id FROM dbapi_t ORDER BY id").fetchall()]
        self.assertEqual(ids, [1, 3])

    def test_isolation_level(self):
        conn = self.conn
        self.assertIsNone(conn.isolation_level)
        conn.isolation_level = dbapi.IsolationLevel.SERIALIZABLE
        cur = conn.execute("SHOW transaction_isolation")
        self.assertEqual(cur.fetchone()[0], "serializable")
        conn.rollback()
        conn.isolation_level = "repeatable read"
        self.assertEqual(conn.execute("SHOW transaction_isolation").fetchone()[0], "repeatable read")
        with self.assertRaises(dbapi.ProgrammingError):
            conn.isolation_level = None  # in a transaction
        conn.rollback()
        conn.read_only = True
        conn.execute("SELECT 1")
        with self.assertRaises(dbapi.Error):
            conn.execute("INSERT INTO dbapi_t (id) VALUES (9)")
        conn.rollback()
        conn.read_only = None
        conn.isolation_level = None
        self.assertEqual(conn.execute("SHOW transaction_isolation").fetchone()[0], "read committed")
        conn.rollback()

    def test_serialization_failure_is_operational(self):
        a, b = connect(isolation_level="serializable"), connect(isolation_level="serializable")
        try:
            a.execute("INSERT INTO dbapi_t (id, name) VALUES (1, 'x'), (2, 'y')")
            a.commit()
            a.execute("SELECT name FROM dbapi_t WHERE id = 1").fetchall()
            b.execute("SELECT name FROM dbapi_t WHERE id = 2").fetchall()
            a.execute("UPDATE dbapi_t SET name = 'a' WHERE id = 2")
            b.execute("UPDATE dbapi_t SET name = 'b' WHERE id = 1")
            a.commit()
            with self.assertRaises(dbapi.OperationalError) as cm:
                b.commit()
            self.assertIn(cm.exception.sqlstate, ("40001", "40P01"))
        finally:
            a.close()
            b.close()

    # ── cursor ──────────────────────────────────────────────────────────

    def test_description_types_and_values(self):
        conn = self.conn
        u = uuid.uuid4()
        seen = dt.datetime(2024, 5, 6, 7, 8, 9, 123456, tzinfo=dt.timezone(dt.timedelta(hours=2)))
        conn.execute("INSERT INTO dbapi_t VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                     (1, "ann", D("12.50"), dt.date(1990, 1, 2), seen, u, dbapi.Jsonb({"k": [1, None]}),
                      b"\x00\xff", ["a", None, "c"], dt.timedelta(hours=2, seconds=3), True))
        cur = conn.execute("SELECT * FROM dbapi_t")
        codes = [c.type_code for c in cur.description]
        self.assertEqual(codes, [23, 25, 1700, 1082, 1184, 2950, 3802, 17, 1009, 1186, 16])
        d = {c.name: c for c in cur.description}
        self.assertEqual((d["score"].precision, d["score"].scale), (10, 2))
        self.assertEqual(d["id"].internal_size, 4)
        self.assertFalse(d["id"].null_ok)
        self.assertTrue(all(len(c) == 7 for c in cur.description))
        self.assertEqual(d["id"].type_code, dbapi.NUMBER)
        self.assertEqual(d["name"].type_code, dbapi.STRING)
        self.assertEqual(d["seen"].type_code, dbapi.DATETIME)
        self.assertEqual(d["data"].type_code, dbapi.BINARY)
        row = cur.fetchone()
        self.assertEqual(row, (1, "ann", D("12.50"), dt.date(1990, 1, 2), seen, u, {"k": [1, None]},
                               b"\x00\xff", ["a", None, "c"], dt.timedelta(hours=2, seconds=3), True))
        self.assertEqual(str(row[2]), "12.50")          # scale kept
        self.assertEqual(row[4].utcoffset(), dt.timedelta(0))  # aware, UTC
        self.assertIsInstance(row[5], uuid.UUID)
        self.assertIsNone(cur.fetchone())

    def test_rowcount(self):
        cur = self.conn.cursor()
        self.assertEqual(cur.rowcount, -1)
        cur.execute("INSERT INTO dbapi_t (id) VALUES (1), (2), (3)")
        self.assertEqual(cur.rowcount, 3)
        self.assertIsNone(cur.description)
        cur.execute("UPDATE dbapi_t SET name = 'x' WHERE id >= %s", (2,))
        self.assertEqual(cur.rowcount, 2)
        cur.execute("SELECT * FROM dbapi_t")
        self.assertEqual(cur.rowcount, 3)
        cur.execute("DELETE FROM dbapi_t WHERE id = 99")
        self.assertEqual(cur.rowcount, 0)

    def test_lastrowid_via_returning(self):
        ddl("DROP TABLE IF EXISTS dbapi_serial", "CREATE TABLE dbapi_serial (id BIGSERIAL PRIMARY KEY, v TEXT)")
        cur = self.conn.cursor()
        cur.execute("INSERT INTO dbapi_serial (v) VALUES (%s) RETURNING id", ("a",))
        first = cur.lastrowid
        self.assertIsInstance(first, int)
        self.assertEqual(cur.fetchone(), (first,))
        cur.execute("INSERT INTO dbapi_serial (v) VALUES ('b'), ('c') RETURNING id")
        self.assertEqual(cur.lastrowid, first + 2)
        cur.execute("INSERT INTO dbapi_serial (v) VALUES ('d')")
        self.assertIsNone(cur.lastrowid)  # no RETURNING: the server has no OIDs
        self.conn.commit()

    def test_fetch_methods_arraysize_iteration_scroll(self):
        self.conn.autocommit = True
        cur = self.conn.cursor()
        cur.execute("SELECT g FROM generate_series(1, 10) AS g")
        self.assertEqual(cur.arraysize, 1)
        self.assertEqual(cur.fetchone(), (1,))
        self.assertEqual(cur.fetchmany(), [(2,)])
        cur.arraysize = 3
        self.assertEqual(cur.fetchmany(), [(3,), (4,), (5,)])
        self.assertEqual(cur.fetchmany(2), [(6,), (7,)])
        self.assertEqual(cur.rownumber, 7)
        cur.scroll(-2)
        self.assertEqual(cur.fetchone(), (6,))
        cur.scroll(0, "absolute")
        self.assertEqual([r[0] for r in cur], list(range(1, 11)))
        with self.assertRaises(IndexError):
            cur.scroll(1)
        self.assertEqual(cur.fetchall(), [])
        cur.execute("SELECT g FROM generate_series(1, 3) AS g")
        self.assertEqual(cur.fetchall(), [(1,), (2,), (3,)])

    def test_fetch_without_result_raises(self):
        cur = self.conn.cursor()
        with self.assertRaises(dbapi.ProgrammingError):
            cur.fetchone()
        cur.execute("INSERT INTO dbapi_t (id) VALUES (1)")
        with self.assertRaises(dbapi.ProgrammingError):
            cur.fetchall()

    def test_closed_cursor_and_connection(self):
        cur = self.conn.cursor()
        cur.close()
        with self.assertRaises(dbapi.InterfaceError):
            cur.execute("SELECT 1")
        c = connect()
        c.close()
        with self.assertRaises(dbapi.InterfaceError):
            c.cursor()
        with self.assertRaises(dbapi.InterfaceError):
            c.commit()

    def test_nextset_multi_statement(self):
        self.conn.autocommit = True
        cur = self.conn.cursor()
        cur.execute("SELECT 1 AS a; INSERT INTO dbapi_t (id) VALUES (7); SELECT 'x' AS b, 2 AS c")
        self.assertEqual(cur.fetchall(), [(1,)])
        self.assertTrue(cur.nextset())
        self.assertIsNone(cur.description)
        self.assertEqual(cur.rowcount, 1)
        self.assertTrue(cur.nextset())
        self.assertEqual([c.name for c in cur.description], ["b", "c"])
        self.assertEqual(cur.fetchall(), [("x", 2)])
        self.assertIsNone(cur.nextset())
        self.assertEqual(self.count("id = 7"), 1)

    def test_multi_statement_is_atomic_in_autocommit(self):
        self.conn.autocommit = True
        with self.assertRaises(errors.UniqueViolation) as cm:
            self.conn.execute("INSERT INTO dbapi_t (id) VALUES (1); INSERT INTO dbapi_t (id) VALUES (1)")
        self.assertEqual(cm.exception.index, 1)
        self.assertEqual(self.count(), 0)  # PostgreSQL's implicit transaction
        self.assertEqual(self.conn.transaction_status, "idle")

    def test_multi_statement_joins_transaction(self):
        self.conn.execute("INSERT INTO dbapi_t (id) VALUES (1); INSERT INTO dbapi_t (id) VALUES (2)")
        self.assertEqual(self.conn.transaction_status, "intrans")
        self.conn.rollback()
        self.assertEqual(self.count(), 0)

    def test_script_with_transaction_control_in_manual_commit(self):
        # psycopg semantics: the manual-commit connection opens a transaction
        # BEFORE the script, so the script's own ROLLBACK undoes its INSERT.
        self.conn.execute("INSERT INTO dbapi_t (id) VALUES (1); ROLLBACK")
        self.assertEqual(self.count(), 0)
        self.conn.execute("ROLLBACK; INSERT INTO dbapi_t (id) VALUES (2)")
        self.assertEqual(self.conn.transaction_status, "idle")  # the INSERT ran on its own
        self.assertEqual(self.count(), 1)

    def test_timed_out_autocommit_script_leaves_no_transaction(self):
        c = connect(autocommit=True, statement_timeout=0.5)
        try:
            with self.assertRaises(errors.QueryCanceled):
                c.execute("INSERT INTO dbapi_t (id) VALUES (1); SELECT pg_sleep(5)")
            self.assertEqual(c.transaction_status, "idle")
            c.execute("INSERT INTO dbapi_t (id) VALUES (2)")
        finally:
            c.close()
        self.assertEqual(self.count("id = 1"), 0)
        self.assertEqual(self.count("id = 2"), 1)

    def test_hidden_script_transaction_is_exclusive(self):
        """While one thread holds the connection exclusively (as an autocommit
        script's hidden BEGIN .. COMMIT does), another thread's statement waits
        instead of joining that transaction."""
        self.conn.autocommit = True
        import time
        done = []
        with self.conn.native.exclusive():
            t = threading.Thread(target=lambda: done.append(self.conn.execute("SELECT 1").fetchone()))
            t.start()
            time.sleep(0.3)
            self.assertEqual(done, [])          # blocked
            self.conn.execute("SELECT 2")        # the owner itself proceeds
        t.join(5)
        self.assertEqual(done, [(1,)])

    def test_batched_literals_keep_the_binding_type(self):
        self.conn.autocommit = True
        cur = self.conn.cursor()
        cur.execute("INSERT INTO dbapi_t (id) VALUES (1)")
        cur.executemany("UPDATE dbapi_t SET score = length(%s) WHERE id = %s", [(b"\xff\x00", 1)])
        self.assertEqual(self.count("score = 2"), 1)   # bytea length, not the hex text's
        cur.executemany("UPDATE dbapi_t SET name = pg_typeof(%s)::text WHERE id = %s", [(1.5, 1)])
        self.assertEqual(cur.connection.execute("SELECT name FROM dbapi_t").fetchone(), ("double precision",))

    def test_executemany_and_bulk_after_ddl_in_the_open_transaction(self):
        """The server's bulk ingest cannot see a table created in the still
        open transaction; the driver then loads through INSERT batches."""
        conn = self.conn
        with connect(autocommit=True) as c:
            c.execute("DROP TABLE IF EXISTS dbapi_new")
        conn.execute("CREATE TABLE dbapi_new (id INT PRIMARY KEY, v TEXT)")
        cur = conn.cursor()
        cur.executemany("INSERT INTO dbapi_new VALUES (%s, %s)", [(1, "a"), (2, "b")])
        self.assertEqual(cur.rowcount, 2)
        self.assertEqual(conn.bulk_insert("dbapi_new", [(3, "c")]), 1)
        conn.commit()
        with connect(autocommit=True) as c:
            self.assertEqual(c.execute("SELECT count(*) FROM dbapi_new").fetchone(), (3,))

    def test_rowcount_survives_close_and_empty_mapping(self):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO dbapi_t (id) VALUES (1), (2)")
        cur.close()
        self.assertEqual(cur.rowcount, 2)
        c = connect(paramstyle="numeric")
        try:
            self.assertEqual(c.execute("SELECT 1", {}).fetchone(), (1,))
        finally:
            c.close()
        self.conn.rollback()

    def test_paramstyles(self):
        self.conn.autocommit = True
        cur = self.conn.cursor()
        cur.execute("INSERT INTO dbapi_t (id, name) VALUES (%s, %s)", [1, "f"])
        cur.execute("INSERT INTO dbapi_t (id, name) VALUES (%(id)s, %(n)s)", {"id": 2, "n": "py"})
        cur.execute("INSERT INTO dbapi_t (id, name) VALUES (?, ?)", [3, "q"])
        cur.execute("INSERT INTO dbapi_t (id, name) VALUES ($1, $2)", [4, "num"])
        cur.execute("SELECT name FROM dbapi_t WHERE name LIKE %s ORDER BY id", ["%"])
        self.assertEqual([r[0] for r in cur], ["f", "py", "q", "num"])
        cur.execute("SELECT count(*) FROM dbapi_t WHERE name LIKE 'p%%' AND id > %s", [0])
        self.assertEqual(cur.fetchone(), (1,))
        c = connect(paramstyle="qmark", autocommit=True)
        try:
            self.assertEqual(c.execute("SELECT ?::int + 1", [1]).fetchone(), (2,))
        finally:
            c.close()

    def test_errors_carry_sqlstate(self):
        self.conn.autocommit = True
        cur = self.conn.cursor()
        cases = [("SELECT * FROM no_such_table_x", errors.UndefinedTable),
                 ("SELEC 1", errors.SyntaxError),
                 ("SELECT 1/0", dbapi.DataError),
                 ("INSERT INTO dbapi_t (id) VALUES (NULL)", errors.NotNullViolation),
                 ("SELECT no_such_col FROM dbapi_t", errors.UndefinedColumn)]
        for sql, cls in cases:
            with self.assertRaises(cls, msg=sql) as cm:
                cur.execute(sql)
            self.assertIsInstance(cm.exception, dbapi.DatabaseError)
            self.assertEqual(len(cm.exception.sqlstate), 5)
        with self.assertRaises(dbapi.ProgrammingError):
            cur.execute("SELECT %s", [object()])

    def test_executemany_plain_insert_is_one_bulk_ingest(self):
        cur = self.conn.cursor()
        rows = [(i, f"n{i}", D(i) / 4, dt.date(2020, 1, 1) + dt.timedelta(days=i)) for i in range(10_000)]
        cur.executemany("INSERT INTO dbapi_t (id, name, score, born) VALUES (%s, %s, %s, %s)", rows)
        self.assertEqual(cur.rowcount, 10_000)
        self.assertEqual(self.count(), 0)          # joined the transaction
        self.conn.commit()
        with connect(autocommit=True) as c:
            n, s, sc = c.execute("SELECT count(*), sum(id), sum(score) FROM dbapi_t").fetchone()
            self.assertEqual((n, s, sc), (10_000, sum(range(10_000)), sum(D(i) / 4 for i in range(10_000))))
            self.assertEqual(c.execute("SELECT name, born FROM dbapi_t WHERE id = 4321").fetchone(),
                             ("n4321", dt.date(2020, 1, 1) + dt.timedelta(days=4321)))

    def test_executemany_bulk_is_atomic(self):
        self.conn.autocommit = True
        cur = self.conn.cursor()
        with self.assertRaises(errors.UniqueViolation):
            cur.executemany("INSERT INTO dbapi_t (id) VALUES (%s)", [(1,), (2,), (1,)])
        self.assertEqual(self.count(), 0)

    def test_executemany_per_row_path(self):
        self.conn.autocommit = True
        cur = self.conn.cursor()
        cur.executemany("INSERT INTO dbapi_t (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
                        [(1, "a"), (2, "b"), (1, "dup")])
        self.assertEqual(cur.rowcount, 2)
        cur.executemany("UPDATE dbapi_t SET name = %(n)s WHERE id = %(id)s", [{"id": 1, "n": "x"}, {"id": 2, "n": "y"}])
        self.assertEqual(cur.rowcount, 2)
        self.assertEqual(self.count("name IN ('x', 'y')"), 2)
        with self.assertRaises(errors.UniqueViolation):
            cur.executemany("INSERT INTO dbapi_t (id, tags) VALUES (%s, %s)", [(3, ["a"]), (1, ["b"])])
        self.assertEqual(self.count("id = 3"), 1)  # autocommit: the set before the failure committed

    def test_executemany_batched_literals_round_trip(self):
        """The non-INSERT path renders literals into BATCH requests: every value
        must read back exactly as the typed (prepared) path writes it."""
        tricky = ["it's", "back\\slash \\x00", "dollar $1 $$ $tag$", "-- not a comment", "/* nor this */",
                  "unicode \u00e9 \u6f22", "", "'); DROP TABLE dbapi_t; --", "E'\\n'"]
        u = uuid.uuid4()
        seen = dt.datetime(2024, 1, 2, 3, 4, 5, 6, tzinfo=dt.timezone(dt.timedelta(hours=-3)))
        rows = [(i, s, D("-1.50") * i, dt.date(2024, 1, 1), seen, u, b"\x00'\xff", i % 2 == 0,
                 dt.timedelta(seconds=-i), {"k": s}, float("-inf") if i == 3 else -0.5 * i)
                for i, s in enumerate(tricky)]
        sql = ("INSERT INTO dbapi_t (id, name, score, born, seen, tag, data, flag, dur, doc, tags) "
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ARRAY[%s::text]) ON CONFLICT (id) DO NOTHING")
        cur = self.conn.cursor()
        cur.executemany(sql, rows)
        self.assertEqual(cur.rowcount, len(rows))
        self.conn.commit()
        got = connect(autocommit=True).execute(
            "SELECT id, name, score, born, seen, tag, data, flag, dur, doc, tags FROM dbapi_t ORDER BY id").fetchall()
        want = [(i, s, D("-1.50") * i, dt.date(2024, 1, 1), seen, u, b"\x00'\xff", i % 2 == 0,
                 dt.timedelta(seconds=-i), {"k": s}, [repr(float("-inf")) if i == 3 else repr(-0.5 * i)])
                for i, s in enumerate(tricky)]
        self.assertEqual([g[:10] for g in got], [w[:10] for w in want])
        self.assertEqual(self.count(), len(tricky))  # the injection attempt was data

    def test_executemany_batched_failure_index_and_commit_rule(self):
        self.conn.autocommit = True
        cur = self.conn.cursor()
        cur.execute("INSERT INTO dbapi_t (id) VALUES (1), (2), (3)")
        with self.assertRaises(dbapi.DataError) as cm:
            cur.executemany("UPDATE dbapi_t SET score = %s WHERE id = %s", [(1, 1), (2, 2), ("x", 3), (4, 3)])
        self.assertEqual(cm.exception.index, 2)
        self.assertEqual(self.count("score IS NOT NULL"), 2)   # autocommit: sets before it committed
        self.conn.autocommit = False
        with self.assertRaises(dbapi.DataError):
            cur.executemany("UPDATE dbapi_t SET score = %s WHERE id = %s", [(9, 1), ("x", 2)])
        self.assertEqual(self.conn.transaction_status, "inerror")
        self.conn.rollback()
        self.assertEqual(self.count("score = 9"), 0)
        c = connect(batch_executemany=False, autocommit=True)
        try:
            c.cursor().executemany("UPDATE dbapi_t SET score = %s WHERE id = %s", [(5, 1), (6, 2)])
        finally:
            c.close()
        self.assertEqual(self.count("score IN (5, 6)"), 2)

    def test_executemany_returning(self):
        ddl("DROP TABLE IF EXISTS dbapi_serial", "CREATE TABLE dbapi_serial (id BIGSERIAL PRIMARY KEY, v TEXT)")
        cur = self.conn.cursor()
        cur.executemany("INSERT INTO dbapi_serial (v) VALUES (%s) RETURNING id, v", [("a",), ("b",)], returning=True)
        got = []
        while True:
            got.extend(cur.fetchall())
            if not cur.nextset():
                break
        self.assertEqual([v for _, v in got], ["a", "b"])
        self.assertEqual(cur.lastrowid, got[-1][0])
        self.conn.commit()

    def test_callproc(self):
        ddl("CREATE OR REPLACE FUNCTION dbapi_add(a int, b int) RETURNS int AS $$ SELECT a + b $$ LANGUAGE sql",
            "CREATE OR REPLACE PROCEDURE dbapi_ins(i int) LANGUAGE sql AS $$ INSERT INTO dbapi_t (id) VALUES (i) $$")
        self.conn.autocommit = True
        cur = self.conn.cursor()
        self.assertEqual(cur.callproc("dbapi_add", (2, 3)), [2, 3])
        self.assertEqual(cur.fetchone(), (5,))
        cur.callproc("dbapi_ins", (42,))
        self.assertEqual(self.count("id = 42"), 1)

    def test_setinputsizes_setoutputsize_are_noops(self):
        cur = self.conn.cursor()
        cur.setinputsizes([int, str])
        cur.setoutputsize(100)
        cur.setoutputsize(100, 0)
        cur.execute("SELECT 1")
        self.assertEqual(cur.fetchone(), (1,))

    def test_row_factories(self):
        self.conn.autocommit = True
        self.conn.execute("INSERT INTO dbapi_t (id, name) VALUES (1, 'a')")
        cur = self.conn.cursor(row_factory=dbapi.rows.dict_row)
        cur.execute("SELECT id, name FROM dbapi_t")
        self.assertEqual(cur.fetchall(), [{"id": 1, "name": "a"}])
        cur.row_factory = dbapi.rows.namedtuple_row
        cur.execute("SELECT id, name FROM dbapi_t")
        self.assertEqual(cur.fetchone().name, "a")

    def test_server_side_cursor_streams(self):
        self.conn.autocommit = True
        with self.conn.cursor("big") as cur:
            cur.execute("SELECT g, g * 2 AS d FROM generate_series(1, 300000) AS g")
            self.assertEqual([c.name for c in cur.description], ["g", "d"])
            self.assertEqual(cur.fetchone(), (1, 2))
            self.assertEqual(len(cur.fetchmany(1000)), 1000)
            # The connection keeps serving other statements while it streams.
            self.assertEqual(self.conn.execute("SELECT 41 + 1").fetchone(), (42,))
            total = 1001 + sum(1 for _ in cur)
            self.assertEqual(total, 300000)
            self.assertEqual(cur.rowcount, 300000)
            with self.assertRaises(dbapi.NotSupportedError):
                cur.scroll(-1)
        # closing early cancels the rest; the connection stays usable
        cur = self.conn.cursor("early")
        cur.execute("SELECT g FROM generate_series(1, 5000000) AS g")
        cur.fetchmany(10)
        cur.close()
        self.assertEqual(self.conn.execute("SELECT 7").fetchone(), (7,))

    def test_cursor_stream_generator_and_attributes(self):
        self.conn.autocommit = True
        cur = self.conn.cursor()
        self.assertIs(cur.connection, self.conn)
        got = 0
        for row in cur.stream("SELECT g FROM generate_series(1, 100000) AS g WHERE g > %s", [10]):
            got += 1
        self.assertEqual(got, 99990)
        cur.execute("SELECT 1 UNION ALL SELECT 2")
        self.assertEqual(cur.next(), (1,))
        self.assertEqual(cur.rownumber, 1)

    def test_cancel_from_another_thread(self):
        self.conn.autocommit = True
        err = []

        def run():
            try:
                self.conn.cursor().execute("SELECT pg_sleep(10)")
            except dbapi.Error as e:
                err.append(e)

        t = threading.Thread(target=run)
        t.start()
        import time
        time.sleep(0.3)
        self.assertGreaterEqual(self.conn.cancel(), 1)
        t.join(8)
        self.assertFalse(t.is_alive())
        self.assertIsInstance(err[0], errors.QueryCanceled)
        self.assertIsInstance(err[0], dbapi.OperationalError)
        self.assertEqual(self.conn.execute("SELECT 3").fetchone(), (3,))

    def test_streaming_cursor_opens_the_transaction_from_its_first_chunk(self):
        sc = self.conn.cursor("txn")
        sc.execute("SELECT g FROM generate_series(1, 300000) AS g")
        sc.fetchone()
        self.assertEqual(self.conn.transaction_status, "intrans")  # before the stream ends
        self.conn.execute("INSERT INTO dbapi_t (id) VALUES (77)")   # joins it (no second BEGIN)
        sc.close()
        self.conn.rollback()
        self.assertEqual(self.count("id = 77"), 0)

    def test_threads_share_a_connection(self):
        self.conn.autocommit = True
        out, errs = [], []

        def work(i):
            try:
                cur = self.conn.cursor()
                for j in range(50):
                    cur.execute("SELECT %s::int * 2", [i * 100 + j])
                    out.append(cur.fetchone()[0] == 2 * (i * 100 + j))
            except Exception as e:  # pragma: no cover - reported below
                errs.append(e)

        ts = [threading.Thread(target=work, args=(i,)) for i in range(16)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(errs, [])
        self.assertEqual(len(out), 800)
        self.assertTrue(all(out))

    def test_notices_and_messages(self):
        self.conn.autocommit = True
        seen = []
        self.conn.add_notice_handler(seen.append)
        cur = self.conn.cursor()
        cur.execute("DROP INDEX IF EXISTS dbapi_hidx")
        cur.execute("CREATE INDEX dbapi_hidx ON dbapi_t USING hash (name)")
        self.assertTrue(cur.messages, "CREATE INDEX USING hash answered no notice")
        self.assertIn("hash", cur.messages[0].message)
        self.assertEqual(cur.messages, seen[-len(cur.messages):])

    def test_rich_api_on_the_same_session(self):
        conn = self.conn
        conn.kv.set("dbapi:k", "v1")
        self.assertEqual(conn.kv.get("dbapi:k"), "v1")
        conn.execute("INSERT INTO dbapi_t (id) VALUES (5)")
        # namespace SQL runs on THIS session: it sees the uncommitted row
        self.assertEqual(conn.ops._client.query("SELECT count(*) AS n FROM dbapi_t")[0]["n"], 1)
        conn.rollback()
        self.assertIs(conn.native.closed, False)
        self.assertEqual(conn.native.kv_get("dbapi:k"), b"v1")


if __name__ == "__main__":
    unittest.main()
