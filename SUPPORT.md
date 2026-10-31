# Python SDK support matrix

Every row names the test that proves it. `U` = server-free unit test, `L` =
live against a ParticleDB server (`PDB_NATIVE_PORT`), `D` = live differential
against PostgreSQL 17 through psycopg 3 (`PDB_PG_DSN`, a throwaway cluster).
Files are under `tests/`; `::` separates file, class and test.

Status: `yes` = implemented and tested; `partial` = implemented with a
stated limit; `no` = not supported (reason given).

## PEP 249 (DB-API 2.0): `particledb.dbapi`

### Module interface

| Feature | Status | Test |
|---|---|---|
| `connect()` (keywords, `pdb://` DSN with options) | yes | U `test_dbapi_unit.py::Dsn::test_parse`; L every `test_dbapi_live.py` test |
| `apilevel = "2.0"`, `threadsafety = 2`, `paramstyle = "pyformat"` | yes | U `test_dbapi_unit.py::ModuleGlobals::test_pep249_globals` |
| `threadsafety = 2`: threads share a connection | yes | L `test_dbapi_live.py::Pep249Live::test_threads_share_a_connection` |
| Exception tree (Warning, Error, InterfaceError, DatabaseError, DataError, OperationalError, IntegrityError, InternalError, ProgrammingError, NotSupportedError) | yes | U `test_dbapi_unit.py::ModuleGlobals::test_exception_hierarchy` |
| SQLSTATE -> exception class (psycopg's mapping and subclass names) | yes | U `test_dbapi_unit.py::ErrorMapping::test_lookup`, `::test_translate_native`; L `test_dbapi_live.py::Pep249Live::test_errors_carry_sqlstate`; D `test_dbapi_differential_live.py::Differential::test_error_classes` |
| Exceptions as `Connection` attributes (optional extension) | yes | U `test_dbapi_unit.py::ModuleGlobals::test_connection_exposes_exceptions` |
| Type objects `STRING`, `BINARY`, `NUMBER`, `DATETIME`, `ROWID` | yes | U `test_dbapi_unit.py::ModuleGlobals::test_type_objects_and_constructors`; L `test_dbapi_live.py::Pep249Live::test_description_types_and_values` |
| Constructors `Date`, `Time`, `Timestamp`, `*FromTicks`, `Binary` | yes | U `test_dbapi_unit.py::ModuleGlobals::test_type_objects_and_constructors` |

### Connection

| Feature | Status | Test |
|---|---|---|
| `close()`; use after close raises `InterfaceError` | yes | L `test_dbapi_live.py::Pep249Live::test_closed_cursor_and_connection` |
| `commit()` / `rollback()` (nothing sent when no transaction is open) | yes | L `test_dbapi_live.py::Pep249Live::test_autocommit_off_by_default_and_lazy_begin`, `::test_rollback_discards` |
| Implicit transactions, BEGIN carried on the first statement (`FLAG_TXN_BEGIN`) | yes | L `test_dbapi_live.py::Pep249Live::test_autocommit_off_by_default_and_lazy_begin` |
| `autocommit` property (refused inside a transaction, as psycopg) | yes | L `test_dbapi_live.py::Pep249Live::test_autocommit_true`, `::test_autocommit_change_refused_in_transaction` |
| Failed transaction: 25P02 after an error, COMMIT returns (server rolled back) | yes | L `test_dbapi_live.py::Pep249Live::test_failed_transaction_then_commit_rolls_back`; D `test_dbapi_differential_live.py::Differential::test_failed_transaction_semantics` |
| `raise_on_silent_rollback=True` (COMMIT of a failed transaction raises 25P02) | yes | L `test_dbapi_live.py::Pep249Live::test_raise_on_silent_rollback` |
| Context manager (commit on success, rollback on error, close) | yes | L `test_dbapi_live.py::Pep249Live::test_context_manager_commits_or_rolls_back` |
| `transaction()` blocks, nested savepoints, `Rollback` | yes | L `test_dbapi_live.py::Pep249Live::test_transaction_blocks_and_savepoints`; D `test_dbapi_differential_live.py::Differential::test_savepoints_via_transaction_blocks` |
| `isolation_level` (enum or name), `read_only`, `deferrable` | yes | L `test_dbapi_live.py::Pep249Live::test_isolation_level`; D `test_dbapi_differential_live.py::Differential::test_isolation_show` |
| Serialization failure surfaces as `OperationalError` (40001) | yes | L `test_dbapi_live.py::Pep249Live::test_serialization_failure_is_operational` |
| `cursor()` / `cursor(name)` (server-side) | yes | L `test_dbapi_live.py::Pep249Live::test_server_side_cursor_streams` |
| `execute()` shortcut (psycopg) | yes | L `test_dbapi_live.py::Pep249Live::test_transaction_blocks_and_savepoints` |
| `cancel()` (from another thread; 57014, connection kept) | yes | L `test_dbapi_live.py::Pep249Live::test_cancel_from_another_thread` |
| Notices (`add_notice_handler`, `Cursor.messages`) | yes | L `test_dbapi_live.py::Pep249Live::test_notices_and_messages` |
| Rich API on the same session (`kv`, `vector`, `ai`, `rag`, `ops`, `native`) | yes | L `test_dbapi_live.py::Pep249Live::test_rich_api_on_the_same_session` |
| `bulk_insert(table, rows / dicts / generator / DataFrame / Arrow)` | yes | L `test_frames_live.py::BulkInsertLive::*` |
| Two-phase commit (`tpc_*`) | no | the server has no `PREPARE TRANSACTION` |

### Cursor

| Feature | Status | Test |
|---|---|---|
| `execute()` with `%s`, `%(name)s`, `?`, `$n` | yes | U `test_dbapi_unit.py::Placeholders::*`; L `test_dbapi_live.py::Pep249Live::test_paramstyles` |
| psycopg's `%%` rule; `params=None` sent verbatim | yes | U `test_dbapi_unit.py::Placeholders::test_none_params_verbatim`, `::test_format`, `::test_count_mismatch_raises` |
| `description` (7-tuples, PostgreSQL OIDs, precision / scale / internal size / null_ok) | yes | L `test_dbapi_live.py::Pep249Live::test_description_types_and_values`; D `test_dbapi_differential_live.py::Differential::test_description_type_codes_of_table_columns` |
| `rowcount` (DML affected, SELECT rows, -1 before execute) | yes | L `test_dbapi_live.py::Pep249Live::test_rowcount`; D `test_dbapi_differential_live.py::Differential::test_rowcounts` |
| `lastrowid` (from `INSERT .. RETURNING`; `None` otherwise: no OIDs) | partial | L `test_dbapi_live.py::Pep249Live::test_lastrowid_via_returning` |
| `fetchone` / `fetchmany` / `fetchall`, `arraysize` | yes | L `test_dbapi_live.py::Pep249Live::test_fetch_methods_arraysize_iteration_scroll`; D `test_dbapi_differential_live.py::Differential::test_fetch_and_scroll` |
| fetch without a result raises `ProgrammingError` | yes | L `test_dbapi_live.py::Pep249Live::test_fetch_without_result_raises` |
| Iteration, `next()`, `rownumber`, `connection` | yes | L `test_dbapi_live.py::Pep249Live::test_fetch_methods_arraysize_iteration_scroll`, `::test_cursor_stream_generator_and_attributes` |
| `scroll(value, mode)` (IndexError out of range) | yes | L `test_dbapi_live.py::Pep249Live::test_fetch_methods_arraysize_iteration_scroll`; D `test_dbapi_differential_live.py::Differential::test_fetch_and_scroll` |
| `nextset()` over a multi-statement string (one request) | yes | L `test_dbapi_live.py::Pep249Live::test_nextset_multi_statement`; D `test_dbapi_differential_live.py::Differential::test_multi_statement_nextset` |
| Multi-statement atomicity (PostgreSQL implicit transaction) | yes | L `test_dbapi_live.py::Pep249Live::test_multi_statement_is_atomic_in_autocommit`, `::test_multi_statement_joins_transaction`; D `test_dbapi_differential_live.py::Differential::test_multi_statement_atomicity` |
| `executemany` plain INSERT -> one atomic bulk ingest | yes | L `test_dbapi_live.py::Pep249Live::test_executemany_plain_insert_is_one_bulk_ingest`, `::test_executemany_bulk_is_atomic` |
| `executemany` other statements -> ordered BATCH of rendered literals | yes | L `test_dbapi_live.py::Pep249Live::test_executemany_batched_literals_round_trip`, `::test_executemany_batched_failure_index_and_commit_rule` |
| `executemany` with arrays -> per-set prepared statement | yes | L `test_dbapi_live.py::Pep249Live::test_executemany_per_row_path` |
| `executemany(..., returning=True)` (results via `nextset`) | yes | L `test_dbapi_live.py::Pep249Live::test_executemany_returning` |
| `callproc` (procedure -> `CALL`, function -> `SELECT`) | partial | L `test_dbapi_live.py::Pep249Live::test_callproc` (no OUT parameters) |
| `setinputsizes` / `setoutputsize` (accepted, ignored) | yes | L `test_dbapi_live.py::Pep249Live::test_setinputsizes_setoutputsize_are_noops` |
| `close()`; closed cursor raises `InterfaceError` | yes | L `test_dbapi_live.py::Pep249Live::test_closed_cursor_and_connection` |
| Row factories (`dict_row`, `namedtuple_row`, `class_row`, ...) | yes | U `test_dbapi_unit.py::RowFactories::test_factories`; L `test_dbapi_live.py::Pep249Live::test_row_factories` |
| Server-side (named) cursor: streams in bounded memory, forward-only, other statements run meanwhile, early close cancels | yes | L `test_dbapi_live.py::Pep249Live::test_server_side_cursor_streams` |
| Streaming result opens the transaction from its first chunk | yes | L `test_dbapi_live.py::Pep249Live::test_streaming_cursor_opens_the_transaction_from_its_first_chunk` |
| `Cursor.stream()` generator (psycopg) | yes | L `test_dbapi_live.py::Pep249Live::test_cursor_stream_generator_and_attributes` |

### Python types

| Type | Status | Test |
|---|---|---|
| `Decimal` with the value's own scale (`NUMERIC(p,s)`) | yes | L `test_dbapi_live.py::Pep249Live::test_description_types_and_values`; D `test_dbapi_differential_live.py::Differential::test_type_round_trip` |
| Unconstrained `NUMERIC` column keeps trailing zeros | yes on the paired candidate server | D `test_dbapi_differential_live.py::Differential::test_unconstrained_numeric_column_keeps_scale` |
| tz-aware `datetime` for timestamptz (UTC), naive for timestamp | yes | D `test_dbapi_differential_live.py::Differential::test_type_round_trip` |
| `uuid.UUID`, `bytes`, `bool`, `date`, `int` (int2..int8), `float` (real, double) | yes | D `test_dbapi_differential_live.py::Differential::test_type_round_trip` |
| `list` for arrays (NULL elements) | yes | D `test_dbapi_differential_live.py::Differential::test_type_round_trip` |
| `dict` / `list` for json / jsonb | yes | D `test_dbapi_differential_live.py::Differential::test_type_round_trip` |
| `timedelta` for interval (read and bind) | yes | U `test_dbapi_unit.py::IntervalText::test_parse`; D `test_dbapi_differential_live.py::Differential::test_type_round_trip` |
| `datetime.time` | yes | L `test_helpers_live.py::PipelineDefinition::test_time_values_bind_and_decode_with_microseconds` |
| numpy / pandas scalars, `Enum`, sets, `register_adapter` | yes | U `test_dbapi_unit.py::Adaptation::*` |

## asyncio: `particledb.aio`

| Feature | Status | Test |
|---|---|---|
| `await connect()`, `await conn.execute()`, `await cur.fetch*()`, `async for row in cur` | yes | L `test_aio_live.py::AioLive::test_execute_fetch_and_async_iteration` |
| Many coroutines on ONE connection (multiplexed); a slow statement does not block a fast one | yes | L `test_aio_live.py::AioLive::test_many_coroutines_share_one_connection` |
| Lazy BEGIN, `commit` / `rollback`, `transaction()` savepoints, `isolation_level` | yes | L `test_aio_live.py::AioLive::test_transactions_and_savepoints` |
| Task cancel and timeout cancel the statement on the server (connection kept) | yes | L `test_aio_live.py::AioLive::test_task_cancel_cancels_the_statement` |
| `executemany` (bulk path + per-set), `bulk_insert` | yes | L `test_aio_live.py::AioLive::test_executemany_bulk_and_per_row` |
| Streaming (named) cursor | yes | L `test_aio_live.py::AioLive::test_server_side_cursor_streams` |
| `nextset`, multi-statement atomicity | yes | L `test_aio_live.py::AioLive::test_multi_statement_nextset`, `::test_types_description_row_factory_callproc_frames` |
| description OIDs, row factories, `callproc`, `scroll`, `fetch_df`, error classes | yes | L `test_aio_live.py::AioLive::test_types_description_row_factory_callproc_frames` |
| `Pool`: shared multiplexed statements + exclusive leases (rollback on error) | yes | L `test_aio_live.py::AioLive::test_pool_shared_and_exclusive` |
| KV frames, `ping` | yes | L `test_aio_live.py::AioLive::test_kv_frames` |

## DataFrames and bulk load

| Feature | Status | Test |
|---|---|---|
| `fetch_arrow()` (int32 / int16 from the declared type, UTC timestamptz, decimals, uuid text) | yes | L `test_frames_live.py::FramesLive::test_fetch_arrow_types` |
| `fetch_df()` (pandas; dtypes) | yes | L `test_frames_live.py::FramesLive::test_fetch_df_pandas` |
| `fetch_polars()` (polars reads the Arrow bytes itself) | yes | L `test_frames_live.py::FramesLive::test_fetch_polars` |
| `fetch_numpy()` | yes | L `test_frames_live.py::FramesLive::test_fetch_numpy` |
| A frame after a partial row fetch returns the rest | yes | L `test_frames_live.py::FramesLive::test_frame_after_partial_fetch_returns_the_rest` |
| `bulk_insert` of tuples / a 200k-row generator (streamed) / dicts / typed values | yes | L `test_frames_live.py::BulkInsertLive::test_tuples_and_generator_stream`, `::test_tuples_default_to_table_columns`, `::test_dicts_and_typed_values` |
| `bulk_insert` atomic and joins the open transaction | yes | L `test_frames_live.py::BulkInsertLive::test_atomic_and_joins_transaction` |
| `bulk_insert` of pandas (NaN -> NULL), polars, pyarrow Table / RecordBatch | yes | L `test_frames_live.py::BulkInsertLive::test_pandas_dataframe`, `::test_polars_dataframe`, `::test_arrow_table_and_batch` |
| Core path works with pyarrow NOT importable | yes | L `test_frames_live.py::NoPyarrow::test_core_path_without_pyarrow` |

## SQLAlchemy and Django

The SQLAlchemy dialect and the Django backend are separate packages on this
SDK: `sqlalchemy-particledb` (its own repository and `SUPPORT.md`) and
`django-particledb`.

## Candidate validation

The current source is tested against the matching ParticleDB server release. TLS is covered by
`tests/test_tls.py`, including verified sync and asyncio queries, concurrent
use, wrong-host rejection, cancellation and reuse. Use `sslmode="verify-full"`
for verified remote connections and `sslrootcert` for a private CA.

The current live dialect tests retain one expected failure for an unaliased
`pg_catalog` join. The dialect uses aliases for that query. This is a server limitation, not an advertised ORM feature. Run the full
suite again for every new server or SDK release.

Pipeline helpers use CREATE RAG PIPELINE and RAG(question, pipeline).
Pipeline DDL accepts simple identifiers, with an optional schema qualifier
for the table. Modes are vector or hybrid; simple is a compatibility alias
for vector. Model execution additionally requires a configured provider.
