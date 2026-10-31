# benchmarks

Measurements of the SDK's DEFAULT path (no tuning), run against a live server.
Each script imports the package from `../particledb` (not an installed copy),
so it measures this tree; `mux_bench.py --sdk DIR` measures another tree
(e.g. the protocol-1 SDK) for a before/after on one machine.

| file | what it is for |
|---|---|
| `dbapi_bench.py` | `particledb.dbapi` / `particledb.aio` / the native API vs psycopg 3 on PostgreSQL, same host, DIRECTIONAL: point SELECT (1 thread, 32 threads with a connection each and 32 on ONE shared connection, 1 and 32 asyncio tasks), small UPDATE (1 / 32 threads), executemany 10k INSERT (bulk path, batched text path, psycopg pipeline), bulk_insert 1M rows (psycopg: COPY), a 1M-row result into a list and a DataFrame. ops/s or rows/s, p50 / p99 per statement; `--json` writes the rows; every load is checked by count. Needs psycopg (and pandas / polars for the frame rows). |
| `mux_bench.py` | Protocol 2 measured: `shared` -- N threads on ONE connection, prepared point SELECT (optionally a server-side sleep per statement), ops/s and p50/p99; `point` -- one thread; `stream` -- a 1M-row result streamed, peak RSS (built-in decoder by default, `PDB_PY_DECODER=pyarrow` for pyarrow); one JSON line per run. |
| `default_path_bench.py` | N threads sharing ONE `PdbWireClient(host, port)` with every other argument at its default, each a closed loop of `query("SELECT KV_SET(..)")` / `query("SELECT KV_GET(..)")`. Prints a kvbench-format line; with `KVBENCH_SERVER_PID` + `KVBENCH` set, also the server's raw counters around the timed window. |

Constraints: one benchmark at a time; alternate the compared trees within one
session (the machine's load moves numbers between sessions). No pyarrow needed.
