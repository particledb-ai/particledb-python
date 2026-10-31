"""ParticleDB Python SDK.

The default is the PDB native wire in its most efficient mode: protocol 2
(multiplexed), typed server-side prepared statements, Arrow results decoded
without any third-party package (pyarrow is optional: ``query_arrow()`` tables).

Basic usage::

    import particledb

    conn = particledb.connect()                      # localhost, native v2
    conn.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO users VALUES ($1, $2)", [1, "alice"])
    rows = conn.query("SELECT * FROM users WHERE id = $1", [1])
    for row in conn.stream("SELECT * FROM big_table"):   # bounded memory
        ...

    db = particledb.ParticleDB()                    # pooled facade, same wire
    db.execute_many("INSERT INTO users VALUES ($1, $2)",
                    [(i, f"u{i}") for i in range(2, 10_000)])
    with db.transaction() as tx:
        tx.execute("UPDATE users SET name = $1 WHERE id = $2", ["bob", 1])

A :class:`Connection` is thread-safe: many threads may run statements on it
at once, each on its own stream. With gRPC (``pip install particledb[grpc]``)::

    from particledb import GrpcClient
"""

from particledb.client import (
    ParticleDB,
    ParticleDBBatchError,
    ParticleDBError,
    QueryResult,
    _WireAdapter,
)
from particledb.kv import KV
from particledb.vector import Vector
from particledb.rag import RAG
from particledb.ai import AI
from particledb.ops import Ops
from particledb.pdb_wire import BatchExecuteError, PdbWireClient, Session, TransactionRolledBackError
from particledb.connection import (
    DEFAULT_PORT,
    Connection,
    Notice,
    Pending,
    PreparedStatement,
    Result,
    RowStream,
    connect,
)
from particledb._protocol import Float4, Int2, Int4, Json, Jsonb
from particledb.errors import (
    BatchError,
    ConnectionError as WireConnectionError,
    ProtocolError,
    QueryCanceledError,
    QueryError,
)

# Particles authoring decorators (the developer front door): write ordinary
# Python, mark it with a SQL concept, deploy with `pdb particles deploy`.
# NOTE: `type` is imported under an alias so it does not shadow the builtin
# `type(...)` used just below for the error-class aliases; it is re-exported as
# `type` at the end of this module (after those uses).
from particledb.particles import (
    scalar,
    aggregate,
    window,
    cast,
    type as _particle_type,
)

# Error class aliases for compatibility
PdbError = ParticleDBError
PdbConnectionError = type("PdbConnectionError", (ParticleDBError,), {})
PdbQueryError = type("PdbQueryError", (ParticleDBError,), {})
DiskFullError = type("DiskFullError", (ParticleDBError,), {})

__all__ = [
    "connect",
    "Connection",
    "Result",
    "RowStream",
    "PreparedStatement",
    "Pending",
    "Notice",
    "DEFAULT_PORT",
    "Int2",
    "Int4",
    "Float4",
    "Json",
    "Jsonb",
    "QueryError",
    "QueryCanceledError",
    "WireConnectionError",
    "ProtocolError",
    "BatchError",
    "ParticleDB",
    "ParticleDBError",
    "ParticleDBBatchError",
    "BatchExecuteError",
    "TransactionRolledBackError",
    "PdbError",
    "PdbConnectionError",
    "PdbQueryError",
    "DiskFullError",
    "QueryResult",
    "KV",
    "Vector",
    "RAG",
    "AI",
    "Ops",
    "PdbWireClient",
    "Session",
    # Particles authoring decorators.
    "scalar",
    "aggregate",
    "window",
    "type",
    "cast",
]

# Re-export the `@type` Particle decorator now that the module-scope builtin
# `type(...)` uses above are done. Import as `type as udt` if you prefer to
# avoid the builtin name in your own code.
type = _particle_type  # noqa: A001 - deliberate SQL-vocabulary name

__version__ = "0.2.1"

# Lazy import for gRPC client - avoids hard dependency on grpcio
def __getattr__(name):
    if name == "GrpcClient":
        from particledb.grpc_client import GrpcClient
        return GrpcClient
    raise AttributeError(f"module 'particledb' has no attribute {name!r}")
