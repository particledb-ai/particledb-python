# particledb/dbapi - PEP 249 (DB-API 2.0) driver

`particledb.dbapi` is a DB-API 2.0 driver on ONE native-wire connection
(`particledb.Connection`, protocol 2, multiplexed). Feature matrix with the
test that proves each row: `SUPPORT.md`.

| File | What it is for |
|---|---|
| `__init__.py` | Module globals (`apilevel`, `threadsafety = 2`, `paramstyle = "pyformat"`), `connect`, the exception classes, type objects (`STRING`, `NUMBER`, `DATETIME`, `BINARY`, `ROWID`) and constructors, the typed wrappers (`Jsonb`, `Int4`, ...). |
| `_connection.py` | `Connection`: autocommit (default off), the lazy transaction protocol (`FLAG_TXN_BEGIN` on the first statement, `commit()` / `rollback()` send nothing when the server reports no transaction), `isolation_level` / `read_only` / `deferrable` (explicit `BEGIN ...` when set), `transaction()` blocks with savepoints and `Rollback`, `execute` shortcut, `bulk_insert`, notices, the rich API (`kv`, `vector`, `ai`, `rag`, `ops`) on the same session through `_SessionClient`, `parse_dsn`, `connect`. |
| `_cursor.py` | `Cursor` (execute, executemany, callproc, nextset, scroll, stream, fetch*, DataFrames) and `ServerCursor` (named: the result streams one record batch at a time, forward-only). `executemany` of a plain `INSERT .. VALUES (%s, ..)` is ONE native bulk ingest (the JDBC `executeBatch` path). |
| `_result.py` | `ResultSet` (keeps the server's Arrow bytes; decodes to rows on first fetch) and `FetchMixin`, the fetch / scroll / nextset / DataFrame logic shared with the asyncio cursors. |
| `_sql.py` | Placeholder conversion (`format`, `pyformat`, `qmark`, `numeric`, `auto`) to the wire's `$n`, and statement `shape` (top-level split, verb, RETURNING, transaction control, the plain-INSERT shape). Both cached per text. |
| `_literal.py` | Typed SQL literals for the batched `executemany` path (JDBC's `appendLiteral`, each value cast to the type the prepared path binds; a str untyped as psycopg sends it), `$n` positions outside literals, BATCH chunking. |
| `_adapt.py` | Parameter adaptation for types the codec does not bind (numpy scalars, pandas NA, `timedelta`, `Enum`, sets) and `register_adapter`. |
| `_types.py` | `cursor.description`: PostgreSQL OIDs from the declared type (`pdb:decl`) or the Arrow type; `Column` (7-tuple + `type_name`, `decl`); type objects and constructors. |
| `errors.py` | PEP 249 exception tree, per-SQLSTATE subclasses with psycopg's names (`UniqueViolation`, ...), `lookup(sqlstate)`, `translate(native_exc)`. |
| `rows.py` | Row factories (psycopg's protocol): `tuple_row`, `dict_row`, `namedtuple_row`, `class_row`, `args_row`, `kwargs_row`, `scalar_row`. |

Constraints:

* **Malformed URLs raise `InterfaceError`**: `parse_dsn` uses the shared
  `_dsn.parse_connection_url` validator and discards parser exception chains.
  The connection string does not appear in the error message. The same parser
  serves awaited `aio.connect`.

* **Every exception that leaves this package is a DB-API one** (`translate`,
  with the native error as `__cause__`); `KeyboardInterrupt` and friends pass
  through unchanged.
* **A transaction-control statement whose outcome is unknown closes the
  connection** (`Connection._control`): never continue under a guessed
  transaction state.
* **`format` / `pyformat` follow psycopg exactly** (`%` is significant
  everywhere once parameters are given, `%%` for a literal; `params=None`
  sends the text verbatim). `qmark` and the splitter are lexical (they skip
  literals, quoted identifiers, dollar quotes and comments).
* **A multi-statement string runs as ONE BATCH request**; in autocommit it is
  wrapped `BEGIN .. COMMIT` (PostgreSQL's implicit-transaction atomicity) and
  a failure rolls back; with transaction control inside it is sent as written.
* **The bulk path of `executemany` is atomic** (all rows or none). Other
  statements go as ordered BATCH requests of rendered literals; arrays, typed
  wrappers and `returning=True` take the per-set prepared path. Both stop at
  the first failure (`e.index` = the set).
* **An autocommit script's hidden BEGIN .. COMMIT holds the connection
  exclusively** (`Connection.exclusive()` in the native layer): no other
  thread's statement may join a transaction it did not ask for.
* **Every BEGIN-carrying send happens under `_begin_lock`** with the
  transaction status re-checked inside it (threads share the session).
