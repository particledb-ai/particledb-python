# particledb

Python client for ParticleDB over the native wire, with DB-API 2.0, asyncio and DataFrame support.

## Install

```sh
pip install particledb
```

The core package has no dependencies. Optional extras:

| Extra | Adds |
|---|---|
| `particledb[arrow]` | `query_arrow()` and the pyarrow decoder |
| `particledb[pandas]` | `fetch_df()` |
| `particledb[polars]` | `fetch_polars()` and polars `bulk_insert` |
| `particledb[grpc]` | gRPC client |

## Quick start

Start a local server with authentication off. It listens on port 7173.

```sh
curl -fsSL https://get.particledb.ai | bash
particledb start --data-dir ./pdb-dev --no-auth \
  --concurrency-control optimistic --durability groupsync --kv-durability async
```

```python
import particledb

with particledb.connect() as db:
    db.execute("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT)")
    db.execute("INSERT INTO items VALUES ($1, $2)", [1, "first"])
    print(db.query("SELECT name FROM items WHERE id = $1", [1]))
```

## Connect

`particledb.connect(dsn=None, **options)`. The DSN form is `pdb://[user[:password]@]host[:port]` (`particledb://` also works).

| Name | Default | Meaning |
|---|---|---|
| `host` | `localhost` | Server host |
| `port` | `7173` | Native wire port |
| `user` | `None` | Login role |
| `password` | `None` | Password |
| `sslmode` | `disable` | `disable`, `require`, `verify-ca` or `verify-full` |
| `sslrootcert` | `None` | CA file. Omit to use system roots |
| `sslcert`, `sslkey` | `None` | PEM client certificate and key |
| `connect_timeout` | `10.0` | Seconds to connect and handshake |
| `statement_timeout` | `None` | Seconds to wait for a response before cancelling on the server |
| `write_timeout` | `30.0` | Seconds for socket writes. `None` waits forever |
| `prefer_unix_socket` | `True` | Use the Unix socket for a local host |
| `unix_socket_dir` | `/tmp` | Server socket directory |
| `unix_socket_path` | `None` | Explicit socket path. Never falls back to TCP |
| `prepare_cache_size` | `256` | Prepared statements cached per connection |

`verify-full` checks the certificate and hostname. `require` encrypts without hostname verification. TLS requires TCP and refuses plaintext fallback.

```python
import os
import particledb

db = particledb.connect(
    host="db.example.com",
    user="app",
    password=os.environ["PARTICLEDB_PASSWORD"],
    sslmode="verify-full",
    sslrootcert="ca.pem",
)
```

A malformed connection URL raises `ValueError`. The message omits the URL.

## Usage

### Query

```python
rows = db.query("SELECT id, name FROM items")   # list of dicts
n = db.scalar("SELECT count(*) FROM items")     # first column of first row
result = db.execute("DELETE FROM items WHERE id = $1", [1])
```

### Parameters

Parameters are bound separately from the SQL, using `$1`, `$2`, ... placeholders.

### Streaming

`stream()` iterates rows in bounded memory. Close the iterator if you stop early.

```python
with db.stream("SELECT * FROM big_table") as rows:
    for row in rows:
        ...
```

### Transactions

The block commits on success and rolls back on an exception.

```python
with db.transaction():
    db.execute("UPDATE items SET name = $1 WHERE id = $2", ["updated", 1])
```

### Errors

SQL errors raise `particledb.QueryError` with a `sqlstate` attribute. A request whose outcome is unknown is never replayed. A write timeout closes the connection.

```python
try:
    db.execute("INSERT INTO items VALUES ($1, $2)", [1, "dup"])
except particledb.QueryError as e:
    print(e.sqlstate)   # 23505
```

### KV

```python
db.kv_set(b"session:123", b"active")
assert db.kv_get(b"session:123") == b"active"
db.kv_incr("hits")
db.kv_delete("session:123")
```

`kv_mget`, `kv_mset`, `kv_pipeline`, `kv_transaction` and `kv_command` are also available.

### DB-API 2.0

```python
from particledb import dbapi

with dbapi.connect() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT %s AS answer", (42,))
        assert cur.fetchone() == (42,)
```

| Behavior | Value |
|---|---|
| `paramstyle` | `pyformat` (`%s`, `%(name)s`). `?` and `$n` are also accepted |
| `autocommit` | `False`. The connection context commits on success |
| Named cursor | `conn.cursor("name")` streams in bounded memory |
| Errors | PEP 249 classes, mapped from SQLSTATE |
| Two-phase commit | Not supported |

### asyncio

```python
from particledb import aio

async def answer():
    async with await aio.connect() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT %s AS answer", (42,))
            return await cur.fetchone()
```

Task cancellation cancels the statement on the server. Many coroutines can share one connection. Do not share an open transaction between unrelated tasks.

### DataFrames and bulk load

DB-API cursors provide `fetch_arrow()`, `fetch_df()`, `fetch_polars()` and `fetch_numpy()`. `conn.bulk_insert(table, rows)` accepts tuples, dicts, generators, pandas and polars DataFrames and pyarrow tables.

### Type mapping

| SQL | Python |
|---|---|
| integer types | `int` |
| `real`, `double precision` | `float` |
| `boolean` | `bool` |
| `text` | `str` |
| `bytea` | `bytes` |
| `numeric` | `Decimal` (scale kept) |
| `date`, `time` | `date`, `time` |
| `timestamp` | naive `datetime` |
| `timestamptz` | aware `datetime` |
| `uuid` | `UUID` |
| arrays | `list` |
| `json`, `jsonb` | `dict` / `list` |
| `NULL` | `None` |

### ORMs

SQLAlchemy and Django support ship as separate packages: `sqlalchemy-particledb` and `django-particledb`.

## Compatibility

| Component | Version |
|---|---|
| ParticleDB server | 1.1.19-rc.15 or later |
| Python | 3.11 or later |

## Testing

Unit tests need no server. Live files are skipped unless `PDB_NATIVE_PORT` is set.

```sh
pip install -e ".[dev]"
python -m pytest -q tests/
```

Live run against a disposable server (the differential tests also need a throwaway PostgreSQL 17):

```sh
PDB_NATIVE_PORT=7173 PDB_TLS_PORT=TLS_PORT PDB_TLS_CA=ca.pem \
  PDB_PG_DSN=postgresql://user@localhost:PG_PORT/sdk_python \
  python -m pytest -q -ra
```

[SUPPORT.md](SUPPORT.md) lists each feature and the test that covers it.

## Links

- https://particledb.ai/sdks/overview/
- https://particledb.ai/sdks/python/

## License

MIT.
The ParticleDB server is under the ParticleDB License: https://particledb.ai/license/
