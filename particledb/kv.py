"""Redis-compatible key-value operations for ParticleDB.

Over the native wire the four operations the wire carries as frames --
``set`` (no TTL), ``get``, ``delete``, ``incr`` (+1) -- are the native KV
frames (``F_KV_SET/GET/DEL/INCR``: binary-safe, no SQL parse), and
``mget`` / ``mset`` pipeline those frames on one multiplexed connection.
Everything else (TTLs, INCRBY, EXISTS, KEYS) is a bound ``KV_*`` SQL function
call. Over HTTP every operation is the ``KV_*`` SQL function. Both reach the
same keyspace RESP serves, with RESP's semantics:

* a plain ``set`` CLEARS an existing TTL (RESP ``SET``);
* ``ttl`` answers RESP's sentinels: ``-1`` (no TTL), ``-2`` (no such key);
* writes are NOT transactional: a KV write inside a transaction that rolls
  back stays applied.

KV opcodes and ``KV_*`` functions are superuser-only on an auth-enforcing
server.

Example::

    db = ParticleDB()
    db.kv.set("session:123", '{"user":"alice"}', ttl=3600)
    db.kv.get("session:123")          # str, or None
    db.kv.get_bytes("blob")           # bytes, or None
    db.kv.ttl("session:123")          # seconds left, -1 none, -2 missing
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union

if TYPE_CHECKING:
    from particledb.client import ParticleDB

Value = Union[str, bytes]


class KV:
    """Key-value operations; usually reached as ``ParticleDB.kv``."""

    def __init__(self, client: "ParticleDB"):
        self._client = client

    # ── plumbing ────────────────────────────────────────────────────────

    def _native(self, fn: Any) -> Any:
        """Run ``fn(connection)`` on a pooled native connection, or return
        ``NotImplemented`` when the client is on HTTP."""
        wire = getattr(self._client, "_wire", None)
        if wire is None:
            return NotImplemented
        return self._client._wire_call(lambda w: w._with_conn(fn, "kv"))

    def _sql_scalar(self, sql: str, params: Sequence[Any]) -> Any:
        rows = self._client.query(sql, list(params))
        if not rows:
            return None
        return next(iter(rows[0].values()), None)

    # ── strings ─────────────────────────────────────────────────────────

    def set(self, key: str, value: Value, ttl: Optional[int] = None) -> None:
        """Store ``value`` (str or bytes). ``ttl`` seconds sets an expiry;
        without it any existing TTL is CLEARED (RESP ``SET``).

        With a str value and a TTL this is ONE atomic ``KV_SET(k, v, ttl)``;
        a bytes value with a TTL is the native SET followed by ``KV_EXPIRE``.
        """
        if ttl is not None:
            if isinstance(value, str):
                self._sql_scalar("SELECT KV_SET($1, $2, $3)", [key, value, int(ttl)])
                return
            if self._native(lambda c: c.kv_set(key, value)) is NotImplemented:
                raise ValueError("a bytes value needs the native wire")
            self.expire(key, ttl)
            return
        r = self._native(lambda c: c.kv_set(key, value))
        if r is NotImplemented:
            if not isinstance(value, str):
                raise ValueError("a bytes value needs the native wire")
            self._sql_scalar("SELECT KV_SET($1, $2)", [key, value])

    def get(self, key: str) -> Optional[str]:
        """The value as text (UTF-8), or ``None`` when absent. Use
        :meth:`get_bytes` for binary values."""
        r = self._native(lambda c: c.kv_get(key))
        if r is NotImplemented:
            v = self._sql_scalar("SELECT KV_GET($1)", [key])
            return None if v is None else str(v)
        return None if r is None else r.decode("utf-8")

    def get_bytes(self, key: str) -> Optional[bytes]:
        """The value's bytes, or ``None`` (native wire only)."""
        r = self._native(lambda c: c.kv_get(key))
        if r is NotImplemented:
            raise ValueError("get_bytes needs the native wire")
        return r

    def delete(self, key: str) -> bool:
        """Delete ``key``; True when it existed."""
        r = self._native(lambda c: c.kv_delete(key))
        if r is NotImplemented:
            return _truthy(self._sql_scalar("SELECT KV_DEL($1)", [key]))
        return bool(r)

    def incr(self, key: str, amount: int = 1) -> int:
        """Atomically add ``amount`` (absent = 0) and return the new value."""
        if amount == 1:
            r = self._native(lambda c: c.kv_incr(key))
            if r is not NotImplemented:
                return int(r)
        return int(self._sql_scalar("SELECT KV_INCRBY($1, $2)", [key, int(amount)]))

    def decr(self, key: str, amount: int = 1) -> int:
        return int(self._sql_scalar("SELECT KV_INCRBY($1, $2)", [key, -int(amount)]))

    def mset(self, mapping: Dict[str, Value]) -> None:
        """Set several keys (each its own write, like RESP ``MSET`` on this
        server: not atomic across keys)."""
        if not mapping:
            return
        pairs = list(mapping.items())
        if self._native(lambda c: c.kv_mset(pairs)) is NotImplemented:
            args: List[Any] = []
            for k, v in pairs:
                args += [k, v]
            self._sql_scalar(f"SELECT KV_MSET({', '.join(f'${i + 1}' for i in range(len(args)))})", args)

    def mget(self, keys: Sequence[str]) -> Dict[str, Optional[str]]:
        """``{key: value or None}`` for every key (pipelined native GETs)."""
        if not keys:
            return {}
        keys = list(keys)
        r = self._native(lambda c: c.kv_mget(keys))
        if r is NotImplemented:
            cols = ", ".join(f"KV_GET(${i + 1}) AS v{i}" for i in range(len(keys)))
            rows = self._client.query(f"SELECT {cols}", keys)
            row = rows[0] if rows else {}
            return {k: (None if row.get(f"v{i}") is None else str(row.get(f"v{i}"))) for i, k in enumerate(keys)}
        return {k: (None if v is None else v.decode("utf-8")) for k, v in zip(keys, r)}

    # ── keys and TTLs ───────────────────────────────────────────────────

    def exists(self, key: str) -> bool:
        """True when ``key`` exists and has not expired, whatever its data type
        (string, hash, list, set, zset), as RESP ``EXISTS`` answers.
        ``KV_TTL`` is the function that answers for every type (``KV_EXISTS``
        looks at string keys only)."""
        return int(self._sql_scalar("SELECT KV_TTL($1)", [key])) != -2

    def expire(self, key: str, seconds: int) -> bool:
        """Set a TTL; True when the key exists."""
        return _truthy(self._sql_scalar("SELECT KV_EXPIRE($1, $2)", [key, int(seconds)]))

    def persist(self, key: str) -> bool:
        """Remove a TTL; True when one was removed."""
        return _truthy(self._sql_scalar("SELECT KV_PERSIST($1)", [key]))

    def ttl(self, key: str) -> int:
        """Seconds left; ``-1`` when the key has no TTL, ``-2`` when it does
        not exist (RESP ``TTL``)."""
        return int(self._sql_scalar("SELECT KV_TTL($1)", [key]))

    def keys(self, pattern: str = "*") -> List[str]:
        """Keys matching a glob ``pattern``."""
        raw = self._sql_scalar("SELECT KV_KEYS($1)", [pattern])
        if raw is None:
            return []
        if isinstance(raw, list):
            return [str(k) for k in raw]
        return [k for k in str(raw).split(",") if k]

    # ── any command (native wire only) ──────────────────────────────────

    def command(self, name: str, *args: Any) -> Any:
        """Run any RESP command the server implements over the native wire
        (``F_KV_CMD``): ``db.kv.command("HSET", "h", "f", "v")``. Same command
        implementations, durability and commit mode as RESP. Raises
        ``NotImplementedError`` on HTTP."""
        r = self._native(lambda c: c.kv_command(name, *args))
        if r is NotImplemented:
            raise NotImplementedError("kv.command needs the native wire")
        return r

    def transaction(self, cmds: Sequence[Sequence[Any]]) -> Any:
        """``[[name, *args], ...]`` as one MULTI/EXEC transaction (native wire)."""
        r = self._native(lambda c: c.kv_transaction(cmds))
        if r is NotImplemented:
            raise NotImplementedError("kv.transaction needs the native wire")
        return r

    def __repr__(self) -> str:
        return f"KV(client={self._client!r})"


def _truthy(val: Any) -> bool:
    """A KV_* function's integer (1/0), bool or text answer as a bool."""
    if isinstance(val, str):
        return val.strip().lower() in ("true", "t", "1", "ok")
    return bool(val)
