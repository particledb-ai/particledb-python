# particledb - the Python SDK package

The importable `particledb` package: the native wire (protocol 2,
multiplexed) as the default and only SQL path besides the explicit HTTP door,
an optional gRPC client, and the SQL-backed operation helpers.

| File | What it is for |
|---|---|
| `_dsn.py` | `parse_connection_url`: shared URL splitting and port validation. Invalid syntax raises a static `ValueError` without chaining the original parser exception. Callers retain their own allowed schemes. |
| `__init__.py` | Package entry point: `connect`, `Connection`, `Result`, `RowStream`, `PreparedStatement`, `Pending`, `Notice`, `DEFAULT_PORT`, the typed-parameter wrappers (`Int2`, `Int4`, `Float4`, `Json`, `Jsonb`), `ParticleDB`, `PdbWireClient`, `Session`, the error types. |
| `connection.py` | `Connection`: ONE socket = one server session, thread-safe, protocol 2. HELLO asks for every capability the SDK implements (the JDBC set) and FAILS CLOSED on anything but protocol 2 + Arrow IPC. Caller-driven read role (no I/O thread), stream slots, typed server-side prepared statements (LRU cache, pinned while used, re-prepared on 26000), `submit` / `Pending.cancel` / `cancel` (RST_STREAM) with the timeout grace, `stream` (`RowStream`: incremental, bounded), `batch`, `execute_each` / `execute_many`, transactions and savepoints, native KV frames (`kv_set/get/delete/incr/mget/mset`; `kv_command` / `kv_transaction` / `kv_pipeline` send RESP commands using native frames), native bulk ingest (`bulk_insert` streams any iterable of rows; `bulk_load_ipc` pipelines ready Arrow IPC chunks, 4 DATA frames in flight), notices. `_exec_raw` / `_batch_raw` / `_stream(begin=)` are the undecoded entry points the DB-API layer uses (`FLAG_TXN_BEGIN` lazy BEGIN). `exclusive()` blocks other threads from opening streams for the duration of a block (the DB-API layer's hidden BEGIN .. COMMIT). `_extended_types=True` (the DB-API profile) decodes json / jsonb to Python objects and interval to `timedelta`. `DEFAULT_PORT` is THE one port constant (= the server's `--pdb-port` default). |
| `_protocol.py` | Protocol-2 codec, pure functions: frame headers, HELLO / HELLO_OK (capability names as the server's `SHOW pdb.wire_capabilities`), request payloads, the typed `F_EXECUTE` parameter encoder (tags 0..17), BATCH_RESULT / notice / error decoders, wire code -> SQLSTATE, the `F_KV_CMD` codec (`kv_cmd_payload` / `kv_txn_payload` by command NAME, so no id table is copied; `decode_kv_reply` -> None / int / bytes / `KvStatus` / list / dict / `KvReplyError`; byte layout pinned against `the server's KV command codec` in `tests/test_protocol.py`). |
| `_arrow.py` | Built-in Arrow IPC stream READER (flatbuffers, incremental `StreamDecoder`) and the ONE ParticleDB conversion layer (`pdb:decl` / `pdb:tz`: numeric text -> `Decimal`, vector -> `list[float]`, uuid, both array spellings, timestamptz -> aware UTC). pyarrow is optional, imported lazily, used for `query_arrow()` and as an opt-in decoder. `converter_for(extended=True)` adds json -> objects and interval text -> `timedelta` (`parse_interval`); `read_schema` / `count_rows` read a stream's schema and row count without decoding a column. |
| `errors.py` | Exception hierarchy: `ParticleDBError` > `PdbError` (`code`, `sqlstate`) > `QueryError`, `QueryCanceledError` (`ambiguous`), `ConnectionError` (08*), `ProtocolError` (08P01), `BatchError`, `BatchExecuteError`, `TransactionRolledBackError`, `DiskFullError`. |
| `pdb_wire.py` | `PdbWireClient`: a pool of `Connection`s, each borrowed for one call; connect-phase-only retries; `Session` pins one; buffered `transaction()`; metrics. |
| `client.py` | `ParticleDB` facade: native wire by default (`transport="pdb_wire"`, fail closed); `"auto"` falls back to HTTP only when nothing listens (logged, `fallback_reason`); `"http"` is the HTTP API (`POST /v1/sql`, stdlib `urllib`). `connect()` parses `pdb://` / `http(s)://`. |
| `arrow_ipc.py` | Pure-Python Arrow IPC stream WRITER (`encode_param_stream`, `infer_column_type`): the rows of a native bulk ingest (`F_BULK` DATA). |
| `kv.py` | `db.kv`: native KV frames for set/get/delete/incr(+1), pipelined mget/mset; bound `KV_*` SQL for TTLs, INCRBY, EXISTS, KEYS. RESP semantics (plain SET clears a TTL, `ttl()` -1/-2, not transactional). `command(name, *args)` / `transaction([[name, *args], ...])`: any RESP command over `F_KV_CMD` (native wire only). |
| `grpc_client.py` | gRPC client (optional `grpc` extra). |
| `vector.py`, `rag.py`, `ai.py`, `ops.py` | Feature helpers that issue the matching SQL functions through whichever transport the client uses. |
| `vector.py` (constraint) | Never splices a caller value into SQL: the vector is validated (real, finite numbers; `bool` / `str` / nested refused) and BOUND (`VECTOR($1)` / `$n`); names are quoted with `ops._quote_ident` (a dotted `schema.table` per part, empty parts refused; `search(columns=)` takes bare column names or `"*"`, and refuses expressions / aliases with a `ValueError`); `create_index` option names must be plain identifiers and string values go through `ops._escape_literal`. `search(filter=)` is the one raw-SQL argument, by design. |
| `particles.py` | Particles authoring decorators (`@scalar`, `@aggregate` function or streaming class, `@window`, `@cast`, reserved `@type`) and their option validation (`ParticleDefinitionError` for an unknown or malformed option). The server embeds this exact file and runs it in every worker, so the rules are one implementation. |
| `dbapi/` | PEP 249 (DB-API 2.0) driver on `Connection`: `particledb.dbapi.connect()`, cursors (client-side and streaming `ServerCursor`), the PEP 249 exception tree mapped from SQLSTATE, type objects, row factories, DataFrame fetches. See `dbapi/README.md`. |
| `aio/` | asyncio driver: the same surface as coroutines on an `asyncio.Protocol` mux (many coroutines, one socket), plus `Pool`. See `aio/README.md`. |
| `_frames.py` | Results as pyarrow tables, pandas / polars DataFrames, numpy arrays, straight from the result's Arrow IPC bytes (no Python rows); applies the same ParticleDB corrections as the row decoder. Every library optional. |
| `_bulkdata.py` | `bulk_insert` inputs (tuples, a generator, dicts, pandas / polars DataFrames, pyarrow Table / RecordBatch) -> one native bulk ingest `Plan` (Arrow IPC chunks, or rows for the built-in writer). |
| `__main__.py` | `python -m particledb particles verify <path> [--json]`: checks a module locally (no server) with the server's rules; exit 0 / 1. |
| `_collect.py` | Particles deploy collector (manifest for CLI deploy and server boot loader). |

## Constraints in `connection.py` / `pdb_wire.py`

* **Fail closed.** A HELLO_OK without `CAP_V2_MUX`, or a result format other
  than Arrow IPC, is an error. There is no v1 code path: never add a silent
  downgrade.
* **One reader at a time.** Only the thread holding `_role` reads the socket;
  the role is tried INSIDE `_mu`, so a releasing holder's wake-up cannot be
  lost. The deadline is checked per frame (other streams' traffic can keep
  the socket readable). `_hard_close` shuts the socket down before closing it
  (Linux does not wake a blocked `recv()` on `close()`).
* **Timeout = cancel first.** `_timed_out` resets the stream and waits
  `cancel_grace`; an answer (57014, or the result) is the outcome and the
  connection stays; only an unanswered reset closes it (`ambiguous=True`).
  A completed stream is never treated as unanswered. KV requests are never
  reset (the server drops a reset KV reply): a KV timeout closes the
  connection.
* **Transaction status** comes only from QUERY / EXECUTE / BATCH / BULK
  answers (`_STATUS_FRAMES`); a PONG or inline KV reply carries no status bits
  and must not clear it.
* **Streams are opened attached** and bounded: past `stream_buffer_limit` the
  stream fails 53200 and is cancelled (protocol 2 has no per-stream flow
  control).
* **The pool lends only idle, clean sessions**: `release` closes a
  connection that is closed, inside a transaction block (per the server's
  flags), or `busy` (a request in flight); `acquire` drops one whose peer is
  gone (`peer_gone`).
* **Only the connect phase is retried** (`_ConnectFailed`: connect, HELLO,
  AUTH on a NEW connection). Anything after a request was written is
  surfaced, never resent.
* **`execute_many` runs the sets IN ORDER** (the mux runs streams
  concurrently, so they are not pipelined), encodes and size-checks every set
  before the first runs, continues past failures outside a transaction and
  stops at the first inside one.
* **KV errors are not connection errors**: the server frames every KV failure
  with code 1 (`_raise_kv`).
* **`DEFAULT_PORT` is the only spelling of the native port** in the package
  (`tests/test_pdb_wire.py::Defaults` fails on another `= 7173`).
