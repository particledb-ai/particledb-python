# particledb/aio - asyncio driver

The DB-API surface as coroutines on the multiplexed native wire: many
coroutines run statements on ONE connection at once (each is its own stream).

| File | What it is for |
|---|---|
| `__init__.py` | Public names: `connect`, `AsyncConnection`, `AsyncCursor`, `AsyncServerCursor`, `AsyncTransaction`, `Pool`; errors / row factories / `IsolationLevel` re-exported from `particledb.dbapi`. |
| `_wire.py` | `AsyncWire` (`asyncio.Protocol`): frame reader in `data_received`, stream table, futures, stream slots, prepared-statement cache, `exec_raw` / `batch_raw` / `open_result_stream`, pipelined bulk ingest, KV frames, ping; `open_wire` (Unix socket for a loopback host when the server listens there, HELLO fail closed, AUTH); `AsyncRowStream`. |
| `_connection.py` | `AsyncConnection`: the blocking driver's transaction model (lazy BEGIN, explicit BEGIN for isolation / read-only), `transaction()` blocks, `execute` / `fetch` / `fetchrow` / `fetchval` shortcuts, `bulk_insert`, KV, `connect`. |
| `_cursor.py` | `AsyncCursor` (execute, executemany incl. the bulk path, callproc, nextset, `async for`, DataFrames via `FetchMixin`) and `AsyncServerCursor` (streaming). |
| `_pool.py` | `Pool`: SHARED autocommit statements on the least-busy connection (`execute` / `fetch*`), EXCLUSIVE leases for transactions (`connection()`), lent only with no shared statement in flight; an open transaction is rolled back on return. |

Constraints:

* **Cancelling the awaiting task cancels the statement on the server**
  (`AsyncWire.abandon`: `RST_STREAM(CANCEL)`, then a background reaper closes
  the connection if the reset is not answered within `cancel_grace`); later
  requests wait for the cancelled statement's answer (`_settle_cancelled`), so
  the session's transaction state is known before anything else is sent. A
  timeout resets and waits the grace in the awaiting task.
* **`prepare` never leaves a pending cache entry** (any failure or
  cancellation removes it), and eviction sends CLOSE_STMT in the background.
* **`exclusive()`**: an autocommit script's hidden BEGIN .. COMMIT holds the
  connection; other tasks' `open_stream` waits.
* **No shared state with the blocking client** except the pure codecs
  (`_protocol`, `_arrow`, `arrow_ipc`) and the DB-API helpers (`dbapi._sql`,
  `_adapt`, `_result`, `_types`, `errors`): the bytes and the semantics are
  identical by construction.
