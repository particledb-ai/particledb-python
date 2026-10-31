# ParticleDB SDK conformance suite

One set of cases that every ParticleDB SDK and driver runs, so that they all
behave the same way against the same server. The JDBC driver is the reference
implementation. The Java SDK, which uses the JDBC driver, is the second.

The case files in `cases/` are the source of truth for case ids, steps and
expected results. Every SDK repository carries an identical copy of `cases/`,
`lib/` and this document.

## 1. What the suite checks

- The native wire is the primary protocol, and its most efficient mode is the
  default for every SDK. A connection opened with no options must be the
  multiplexed protocol 2 with every capability, and the suite checks this from
  the server's side. A silent fallback to the older protocol 1 would still
  "work", so it needs a check that fails.
- The PostgreSQL and HTTP protocols are compatibility paths over the same
  engine.
- A client never reports success for something the server did not do.

## 2. Groups

| Group | File | Cases | Checks |
|---|---|---|---|
| DEFAULTS | `defaults.json` | 7 | A connection with no options is `pdb-native-v2` with every capability (`v2_mux`, `prepared`, `typed_params`, `array_params`, `batch_sqlstate`, `statement_notice`, `numeric_text`, and `bulk_ingest` where the SDK implements it), Arrow results, and the server's default port. Checked from the server (`SHOW pdb.wire_*`, section 3) and compared with the SDK's own view. |
| SQL | `sql.json` | 40 | Type round trips (integers, NaN and infinite floats, numeric scale, text and Unicode up to 1 MiB, bytea with all byte values up to 1 MiB, json and jsonb, date and timestamp, timestamptz as an instant, uuid, arrays with NULLs, vector; `time` is refused with `0A000`). Typed binding and typed NULLs, injection safety, prepared statement reuse and invalidation by DDL, batches (counts, and `23505` per entry), memory-bounded streaming (3 million rows in a 256 MiB heap), multi-statement strings (one implicit transaction, last result returned), SQLSTATE mapping (`23505`, `42P01`, `42601`, `42703`, `22012`, `23502`, `23503`, `22P02`, `42883`, `25P02`, `40001`), transactions (commit and rollback visibility, savepoints, READ COMMITTED sees a concurrent commit and REPEATABLE READ does not, the isolation setter reaches the server, reading your own writes on the same and on another connection), cancel and statement timeout (`57014`, connection stays usable), DDL then DML across connections, transactional DDL. |
| CONCURRENCY | `concurrency.json` | 5 | 32 threads on one connection each get their own rows. A slow statement does not block others. Two large streams interleave intact. Concurrent statements inside one connection's transaction share it, while another connection stays isolated. More requests than the stream limit wait instead of failing. |
| KV | `kv.json` | 12 | Parity between the SDK and RESP: values, absent keys, INCR, TTLs set either way, the TTL values -1 and -2, a plain SET clears a TTL, expiry, binary-safe values, 1 MiB values, MSET and MGET, hash, list, set and sorted set, non-transactional KV writes. `notes` lists which Redis commands SDKs can reach. |
| DURABILITY | `durability.json` | 5 | Acknowledged autocommit inserts, an acknowledged COMMIT, an acknowledged batch and acknowledged KV writes survive `kill -9` and a restart. A lost connection is reported promptly with a class `08` SQLSTATE. |

There are 69 cases. Each case lists the modes it runs in:

- `default`: the server's defaults (`--concurrency-control optimistic --durability groupsync`,
  sessions at READ COMMITTED). All 69 cases run.
- `strict`: `--concurrency-control strict-optimistic --durability sync`. 44 cases run.

## 3. What the server reports about a connection

A case must be able to ask the server what a connection negotiated. The server
records each connection's protocol, transport and capabilities at handshake
time, from the values it sent in its reply. These read-only settings expose
that record:

| Query | Answer |
|---|---|
| `SHOW pdb.wire_protocol` | `pdb-native-v2`, `pdb-native-v1`, `postgres-v3` or `mysql-v10` |
| `SHOW pdb.wire_transport` | `tcp`, `unix` or `tls` (`unknown` where the server cannot tell) |
| `SHOW pdb.wire_capabilities` | the sorted names of the negotiated capabilities |
| `SHOW pdb.wire_result_format` | `arrow_ipc` for the native wire, `text` for PostgreSQL and MySQL |
| `SHOW pdb.connection_id` | the connection's id |
| `__pdb_stat_connections` | the same record for every connection, in the columns `protocol`, `transport` and `capabilities` |

`SET` and `RESET` of these settings fail with `55P02`. They keep their values
across `RESET ALL`, `DISCARD ALL` and rollback.

## 4. Canonical value forms

A `script` case compares rows cell by cell. The adapter reads each cell with
the typed getter an application would use and renders it in this form:

| Type | Canonical form | Comparison |
|---|---|---|
| NULL | JSON `null` | identity (never 0, never `""`) |
| int2, int4, int8 | base 10 | string |
| float4, float8 | `NaN`, `Infinity`, `-Infinity`, otherwise the shortest decimal that round-trips | parsed IEEE value (NaN equals NaN) |
| numeric | plain decimal string with its scale (`1.50`, `0.52`) | string: `0.52` is not `0.520000` |
| bool | `true` or `false` | string |
| text, varchar, json | the exact text (json keeps its input text) | string |
| jsonb | PostgreSQL's normalised text (`{"a": [1, 2], "b": 1}`) | string |
| bytea | `\x` followed by lowercase hex | string |
| date | `YYYY-MM-DD` | string |
| timestamp | `YYYY-MM-DD HH:MM:SS[.f]`, trailing zeros of the fraction removed | string |
| timestamptz | the UTC instant, `YYYY-MM-DD HH:MM:SS[.f]+00` | string |
| uuid | lowercase with hyphens | string |
| array of T | JSON array of canonical elements, or `null` | element by element |
| vector | JSON array of components | float32 |

Generated values (`{"gen": {"repeat": s, "count": n}}` and
`{"gen": {"bytes": "all256", "count": n}}`) expand the same way in parameters
and in expected results.

## 5. Running the suite

Each SDK repository has `conformance/run_conformance.sh`. It builds the SDK
from source, compiles the adapter against it, runs every case for the mode,
and validates the results file. It exits 0 only when no case failed and the
results file is valid.

```bash
./run_conformance.sh [--mode default|strict] [--results FILE]
```

The runner works in one of two ways.

**Against a running server (no environment set).** The runner connects to a
server that is already listening on `127.0.0.1` at the default port, 7173.
Start that server in the mode you test, for example
`particledb start --no-auth` for `default`, or
`particledb start --no-auth --concurrency-control strict-optimistic --durability sync` for
`strict`. The adapter connects without naming a port, so every case also
checks that the SDK's default port is right. The RESP port defaults to 6379.
The runner did not start this server, so it cannot kill and restart it: the
cases that crash the server are reported as skipped, with that reason.

**With `PDB_BIN` set.** The runner starts a fresh server from that binary,
with a new data directory, in the requested mode. The server gets the default
port (7173) when the ports it needs are free, and a free block of ports
otherwise. On the default port the adapter connects without naming a port.
The runner also provides the control hook, so every case runs, including the
crash cases. The server is stopped when the run ends.

```bash
PDB_BIN=/path/to/particledb ./run_conformance.sh --mode strict
```

### Environment

The runner passes these variables to the adapter:

| Variable | Meaning |
|---|---|
| `PDB_CONF_HOST` | The server's host. Default `127.0.0.1`. |
| `PDB_CONF_PORT` | The server's port. When unset, the adapter connects without a port and the SDK's default is used. |
| `PDB_CONF_DEFAULT_PORT` | The server's default port. With `PDB_BIN` it is read from `particledb start --help`; against a running server it is 7173 unless set. An SDK's default port must equal it (DEF-005). No case hard-codes it. |
| `PDB_CONF_RESP_PORT` | The server's RESP port. Default 6379 against a running server. |
| `PDB_CONF_MODE` | `default` or `strict`. |
| `PDB_CONF_CASES_DIR`, `PDB_CONF_IDS` | The case files, and a file with the ids this mode runs. |
| `PDB_CONF_CONTROL` | Optional. An executable called with `kill9` or `restart` (same ports and data directory). Without it, the cases that crash the server are skipped. |
| `PDB_CONF_RESULTS` | Where the adapter writes the results file. |

`PDB_CONF_MVN_OFFLINE=1` runs the Maven steps offline.

### Results file

```json
{"sdk": "...", "sdk_version": "...", "mode": "default",
 "results": {"<id>": {"status": "pass|fail|skip", "reason": "...", "detail": "...", "ms": 12}}}
```

`lib/conformance.py validate` rejects a results file in which a case of the
mode is missing, a skip has no reason, a fail has no detail, or an id is
unknown. A skip states that the SDK does not offer the surface, and why. It is
never a way to hide a failure.

## 6. Design decisions

- **Compression is off by default.** Protocol 1 and protocol 2 servers do not
  offer it. For typical OLTP results the CPU time to compress is larger than
  the network time it saves. Only large text results on slow links gain. If
  compression is added, it will be opt-in with a size threshold.
- **Each `Connection` has its own socket.** The server keeps one session per
  socket (transaction, settings, savepoints, prepared statements), so two
  connections sharing a socket would share a transaction. Multiplexing is for
  concurrency inside one connection, and the driver does it by default.

## 7. Adding an SDK

1. Create `conformance/` in the SDK's repository with `run_conformance.sh`, an
   adapter, and copies of `cases/`, `lib/` and this document.
2. Implement the `script` interpreter once: parameters, canonical rendering,
   and the operations `exec`, `query`, `error`, `begin`, `commit`, `rollback`,
   `isolation`, `savepoint`, `rollback_to` and `commit_error`. That covers 31
   cases. Then implement each `custom` case by id.
3. A connection opened with no options must be what DEF-001 to DEF-004
   expect. Expose the negotiated protocol and capability names the way the
   JDBC driver does (`negotiatedProtocol()`, `negotiatedCapabilities()`).
