"""High-level ParticleDB client: the native wire by default, HTTP only when asked.

SQL (``query`` / ``query_one`` / ``query_result`` / ``execute`` /
``execute_many`` / ``bulk_insert`` / ``session`` / ``transaction`` /
``stream``) goes over the PDB native wire: protocol 2 (multiplexed), typed
server-side prepared statements for ``$n`` parameters, Arrow results decoded
WITHOUT pyarrow (pyarrow is only needed for ``query_arrow()`` tables).

Transports (``transport=``):

* ``"pdb_wire"`` (DEFAULT): the native wire, fail closed -- if it cannot be
  reached or does not negotiate protocol 2 the constructor RAISES; nothing
  silently switches to another protocol.
* ``"auto"``: the native wire, and HTTP (``POST /v1/sql`` on ``port``) only
  when the native port does not accept a connection at all; the fallback is
  logged at WARNING with the native error and kept in ``fallback_reason``. A
  native handshake, protocol or authentication failure is raised, never
  masked.
* ``"http"``: the HTTP API only (``tables()`` / ``table()`` are HTTP-only).

Pass ``tls=True`` (or an ``https://`` host) to talk to a TLS-fronted HTTP
server; Basic auth credentials are only sent over TLS.

Example::

    from particledb import ParticleDB

    db = ParticleDB()   # native wire, localhost:7173

    rows = db.query("SELECT name, price FROM products WHERE price > $1", [50])
    affected = db.execute(
        "INSERT INTO products (id, name, price) VALUES ($1, $2, $3)",
        [1, "Widget", 9.99],
    )
"""

from __future__ import annotations

import base64
import builtins
import json
import logging
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Sequence
from urllib.parse import unquote


log = logging.getLogger("particledb")


class _WireAdapter:
    """Stub adapter for PDB wire transport. Used by transport auto-detection tests."""
    pass


from .errors import ParticleDBError  # noqa: E402  (the SDK's one error base)
from ._dsn import parse_connection_url
from .connection import DEFAULT_PORT  # noqa: E402  (the database port: HTTP and native share it)


class ParticleDBConnectionError(ParticleDBError, builtins.ConnectionError):
    """The native wire could not be reached or did not negotiate protocol 2."""


class ParticleDBBatchError(ParticleDBError):
    """Raised by ``execute_many`` / ``bulk_insert`` when parameter sets failed.

    Outside a transaction the other sets were committed. ``errors`` is
    ``[(row_index, message), ...]``; ``affected`` counts rows changed by the
    sets that succeeded; ``attempted`` is how many sets the server answered.
    """

    def __init__(self, message: str, *, errors, affected: int, attempted: int,
                 sqlstate: Optional[str] = None, code: Optional[str] = None):
        super().__init__(message, sqlstate=sqlstate, code=code)
        self.errors = errors
        self.affected = affected
        self.attempted = attempted


def _wrap_wire_error(exc: Exception) -> ParticleDBError:
    """Map a native-wire exception to the client's ParticleDBError family."""
    from .pdb_wire import BatchExecuteError

    code = getattr(exc, "code", None)
    sqlstate = getattr(exc, "sqlstate", None)
    if isinstance(exc, BatchExecuteError):
        return ParticleDBBatchError(
            str(exc), errors=exc.errors, affected=exc.affected, attempted=exc.attempted,
            sqlstate=sqlstate, code=str(code) if code else None,
        )
    return ParticleDBError(str(exc), code=str(code) if code else None, sqlstate=sqlstate)


class _SessionProxy:
    """A native-wire Session whose errors surface as ParticleDBError."""

    def __init__(self, sess: Any):
        self._s = sess

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._s, name)
        if not callable(attr):
            return attr

        def call(*args: Any, **kwargs: Any) -> Any:
            from .pdb_wire import PdbError

            try:
                return attr(*args, **kwargs)
            except PdbError as exc:
                raise _wrap_wire_error(exc) from exc

        return call

    def __enter__(self) -> "_SessionProxy":
        return self

    def __exit__(self, *exc: Any) -> None:
        self._s.close()


@dataclass
class QueryResult:
    """Full result from a SQL query, including metadata."""

    columns: List[str] = field(default_factory=list)
    rows: List[Dict[str, Any]] = field(default_factory=list)
    row_count: int = 0
    elapsed_ms: float = 0.0


class ParticleDB:
    """ParticleDB REST client.

    Parameters
    ----------
    host : str
        Server hostname.  Default: ``"localhost"``.
    port : int
        HTTP API port.  Default: ``particledb.connection.DEFAULT_PORT``
        (7173, the database port, which serves HTTP and the native wire).
    user : str or None
        Username for Basic authentication.  Default: ``None``.
    password : str or None
        Password for Basic authentication.  Default: ``None``.
    database : str or None
        Database name (sent as a header hint).  Default: ``None``.
    timeout : float
        HTTP request timeout in seconds.  Default: ``30.0``.
    tls : bool or None
        Use HTTPS.  When ``None`` (default) it is inferred from the scheme in
        ``host`` (``https://…`` → ``True``), else ``False``.  Basic auth
        credentials are only transmitted when ``tls`` is ``True``.
    allow_insecure_auth : bool
        Escape hatch to permit Basic auth over cleartext HTTP for a trusted
        local/dev connection.  Default: ``False``.
    transport : str or None
        ``"pdb_wire"`` (default): native wire, raise if it cannot be reached
        or does not negotiate protocol 2.  ``"auto"``: native wire, HTTP only
        when the native port refuses the connection (logged, see
        ``fallback_reason``).  ``"http"``: HTTP API only.
    pdb_port : int or None
        Native wire port.  Default: ``particledb.connection.DEFAULT_PORT``
        (the server's own default).
    pool_size : int or None
        Max pooled native-wire connections.  Default:
        ``min(cpu_count, 16)``.
    prefer_unix_socket : bool
        For a local host (``localhost``, ``127.x``, ``::1``) the native wire
        connects over the server's Unix socket ``<unix_socket_dir>/.s.PDB.<pdb_port>``
        when it exists, else TCP.  ``False`` always uses TCP.  Default ``True``.
    unix_socket_dir : str or None
        The server's ``--unix-socket-dir``.  Default ``/tmp``.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = DEFAULT_PORT,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        timeout: float = 30.0,
        transport: Optional[str] = None,
        tls: Optional[bool] = None,
        allow_insecure_auth: bool = False,
        pdb_port: Optional[int] = None,
        pool_size: Optional[int] = None,
        prefer_unix_socket: bool = True,
        unix_socket_dir: Optional[str] = None,
    ):
        if transport is not None and transport not in ("pdb_wire", "http", "auto"):
            raise ValueError(f"Invalid transport: {transport}. Use 'pdb_wire', 'http', or 'auto'.")
        # Accept a full URL scheme in `host` (e.g. "https://db.example.com") so
        # callers can point at a TLS-fronted deployment directly. An explicit
        # `tls=` argument overrides the scheme parsed from `host`.
        scheme_from_host: Optional[str] = None
        if "://" in host:
            scheme_from_host, host = host.split("://", 1)
            # A URL host may include a :port suffix - honor it.
            if ":" in host and not host.rstrip("]").endswith("]"):
                host, _, host_port = host.partition(":")
                if host_port.isdigit():
                    port = int(host_port)
        if tls is None:
            tls = scheme_from_host == "https"
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.timeout = timeout
        self.tls = bool(tls)
        self.allow_insecure_auth = bool(allow_insecure_auth)
        self._transport = transport or "pdb_wire"
        scheme = "https" if self.tls else "http"
        self._base_url = f"{scheme}://{host}:{port}"
        # The native wire listens on its own port (server flag --pdb-addr),
        # independent of the HTTP API port; `pdb_port=` overrides it.
        from .connection import DEFAULT_PORT as _PDB_WIRE_PORT

        self.pdb_port = pdb_port if pdb_port is not None else _PDB_WIRE_PORT
        self.pool_size = pool_size
        self.fallback_reason: Optional[str] = None
        self._wire = None
        if self._transport in ("pdb_wire", "auto"):
            self._wire = self._init_wire(
                host=host,
                port=self.pdb_port,
                username=user,
                password=password,
                timeout=timeout,
                pool_size=pool_size,
                prefer_unix_socket=prefer_unix_socket,
                unix_socket_dir=unix_socket_dir,
                transport=self._transport,
            )
            if self._wire is None:
                if self._transport == "pdb_wire":
                    raise ParticleDBConnectionError(
                        f"PDB native wire not reachable at {host}:{self.pdb_port}", sqlstate="08001"
                    )
                self.fallback_reason = self.fallback_reason or "native wire not reachable"

        # Lazily-initialised sub-clients.
        self._kv: Optional["KV"] = None
        self._vector: Optional["Vector"] = None
        self._rag: Optional["RAG"] = None
        self._ai: Optional[Any] = None
        self._ops: Optional[Any] = None

    def _init_wire(
        self,
        host: str = "localhost",
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: float = 30.0,
        pool_size: Optional[int] = None,
        prefer_unix_socket: bool = True,
        unix_socket_dir: Optional[str] = None,
        transport: str = "pdb_wire",
    ):
        """Build the pooled native-wire client and open its first connection.

        A connection that cannot be OPENED (nothing listening) returns ``None``
        under ``transport="auto"`` -- logged at WARNING, the cause kept in
        ``fallback_reason`` -- and raises otherwise. Anything else (the server
        refused the handshake or the credentials, did not negotiate protocol
        2) always raises: that is a native-wire failure, not an absent one.
        """
        from .errors import PdbError, _ConnectFailed
        from .pdb_wire import DEFAULT_POOL_SIZE, DEFAULT_UNIX_SOCKET_DIR, PdbWireClient

        wire = PdbWireClient(
            host=host,
            port=port,
            username=username,
            password=password,
            timeout=timeout,
            pool_size=pool_size or DEFAULT_POOL_SIZE,
            prefer_unix_socket=prefer_unix_socket,
            unix_socket_dir=unix_socket_dir or DEFAULT_UNIX_SOCKET_DIR,
            max_retries=0,
        )
        try:
            conn = wire._pool.acquire(wait_timeout=timeout)
        except _ConnectFailed as exc:
            wire.close()
            if transport == "auto" and _is_absent(exc):
                self.fallback_reason = f"native wire at {host}:{port} not reachable: {exc}"
                log.warning("particledb: %s; using the HTTP API at %s (transport='auto')",
                            self.fallback_reason, self._base_url)
                return None
            raise ParticleDBConnectionError(
                f"PDB native wire at {host}:{port}: {exc}", sqlstate=getattr(exc, "sqlstate", "08001")
            ) from exc
        except PdbError as exc:
            wire.close()
            raise ParticleDBConnectionError(
                f"PDB native wire at {host}:{port}: {exc}", sqlstate=exc.sqlstate,
                code=str(exc.code) if exc.code else None,
            ) from exc
        wire._pool.release(conn)
        wire.max_retries = 3
        return wire

    @classmethod
    def connect(cls, dsn: str, **kwargs: Any) -> "ParticleDB":
        """Create a client from a URL.

        * ``pdb://[user[:password]@]host[:port]`` or
          ``particledb://...`` -- native wire (port default
          ``particledb.connection.DEFAULT_PORT``).
        * ``http://`` / ``https://host[:port]`` -- HTTP API only.

        ``postgres://`` / ``postgresql://`` URLs are refused: this SDK does
        not speak the PostgreSQL wire; use a PostgreSQL driver (e.g. psycopg)
        directly for that legacy path. Extra keyword arguments are passed to
        the constructor.
        """
        parts = parse_connection_url(dsn)
        scheme = parts.scheme.lower()
        user = unquote(parts.username) if parts.username else None
        password = unquote(parts.password) if parts.password else None
        if user is not None:
            kwargs.setdefault("user", user)
        if password is not None:
            kwargs.setdefault("password", password)
        host = parts.hostname or "localhost"
        if scheme in ("pdb", "particledb"):
            if parts.port:
                kwargs.setdefault("pdb_port", parts.port)
            kwargs.setdefault("transport", "pdb_wire")
            return cls(host=host, **kwargs)
        if scheme in ("http", "https"):
            if parts.port:
                kwargs.setdefault("port", parts.port)
            kwargs.setdefault("transport", "http")
            kwargs.setdefault("tls", scheme == "https")
            return cls(host=host, **kwargs)
        if scheme in ("postgres", "postgresql"):
            raise ValueError(
                "postgres:// URLs are not supported: this SDK speaks the "
                "ParticleDB native wire (pdb://host[:port]) and the HTTP API. "
                "For the PostgreSQL wire use a PostgreSQL driver directly."
            )
        raise ValueError("unsupported URL scheme (use pdb://, particledb://, http:// or https://)")

    def connection(self, **kwargs: Any):
        """A NEW dedicated multiplexed native-wire
        :class:`particledb.connection.Connection` to the same server (not
        pooled; many threads may share it). Close it when done."""
        self._require_wire("connection")
        from .connection import Connection

        kwargs.setdefault("user", self.user)
        kwargs.setdefault("password", self.password)
        kwargs.setdefault("connect_timeout", self.timeout)
        return Connection(self.host, self.pdb_port, **kwargs)

    def _wire_call(self, fn):
        """Run a native-wire call, mapping its errors to ParticleDBError."""
        from .pdb_wire import PdbError

        try:
            return fn(self._wire)
        except PdbError as exc:
            raise _wrap_wire_error(exc) from exc

    def _require_wire(self, what: str) -> None:
        if self._wire is None:
            raise ParticleDBError(
                f"{what} needs the native wire transport; this client is using "
                f"HTTP (native wire not reachable at {self.host}:{self.pdb_port} "
                f"or transport='http')"
            )

    def _active_transport(self) -> str:
        """Return the active transport name."""
        if self._wire is not None:
            return "pdb_wire"
        return "http"

    # ------------------------------------------------------------------
    # Core SQL methods
    # ------------------------------------------------------------------

    def query(
        self, sql: str, params: Optional[Sequence[Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a SQL query and return rows as a list of dicts.

        Parameters
        ----------
        sql : str
            SQL statement.  Use ``$1``, ``$2``, ... for parameter placeholders.
        params : list, optional
            Positional parameter values.

        Returns
        -------
        list of dict
            Each dict maps column name to its value.

        Raises
        ------
        ParticleDBError
            If the server returns an error response.

        Example
        -------
        >>> rows = db.query("SELECT id, name FROM users WHERE id = $1", [42])
        >>> rows[0]["name"]
        'alice'
        """
        if self._wire is not None:
            return self._wire_call(lambda w: w.query(sql, params))
        body = self._post_query(sql, params)
        return self._decode_rows(body)

    @staticmethod
    def _decode_rows(body: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Decode the ``rows`` field of a ``/v1/sql`` response into dicts.

        The live server returns each row as a JSON OBJECT keyed by column name:
        ``{"rows": [{"?column?": 1}], "columns": ["?column?"], ...}``. An object
        row is returned as-is (values preserved). For backward/robustness we
        also accept the legacy list-of-lists shape and zip it against
        ``columns``. (The original SDK unconditionally did
        ``{columns[i]: v for i, v in enumerate(row)}`` — which, on an OBJECT
        row, iterates the dict's KEYS, replacing every value with its own column
        name and returning ``[{'?column?': '?column?'}]`` instead of the real
        value. BUG-005.)
        """
        columns: List[str] = body.get("columns", [])
        raw_rows: List[Any] = body.get("rows", [])
        out: List[Dict[str, Any]] = []
        for row in raw_rows:
            if isinstance(row, dict):
                out.append(dict(row))
            else:
                # Legacy positional row: list/tuple aligned with `columns`.
                out.append({columns[i]: v for i, v in enumerate(row)})
        return out

    def query_result(
        self, sql: str, params: Optional[Sequence[Any]] = None
    ) -> QueryResult:
        """Execute a SQL query and return a full :class:`QueryResult`.

        Includes column names, row count, and elapsed time (server-side over
        HTTP; client-measured round trip over the native wire).
        """
        if self._wire is not None:
            import time as _time

            t0 = _time.monotonic()
            res = self._wire_call(lambda w: w.execute_result(sql, params))
            elapsed = (_time.monotonic() - t0) * 1000.0
            rows = res.dicts()
            return QueryResult(
                columns=list(res.columns),
                rows=rows,
                row_count=len(rows) if res.has_rows else res.rowcount,
                elapsed_ms=elapsed,
            )
        body = self._post_query(sql, params)

        columns: List[str] = body.get("columns", [])
        raw_rows: List[Any] = body.get("rows", [])
        row_count: int = body.get("row_count", len(raw_rows))
        elapsed: float = body.get("elapsed_ms", 0.0)

        rows = self._decode_rows(body)

        return QueryResult(
            columns=columns,
            rows=rows,
            row_count=row_count,
            elapsed_ms=elapsed,
        )

    def query_one(
        self, sql: str, params: Optional[Sequence[Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Execute a query and return the first row, or ``None``."""
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def execute(
        self, sql: str, params: Optional[Sequence[Any]] = None
    ) -> int:
        """Execute a DML/DDL statement and return the number of affected rows.

        Parameters
        ----------
        sql : str
            SQL statement (INSERT, UPDATE, DELETE, CREATE TABLE, ...).
        params : list, optional
            Positional parameter values.

        Returns
        -------
        int
            Number of rows affected by the statement.

        Example
        -------
        >>> db.execute("INSERT INTO users (id, name) VALUES ($1, $2)", [1, "alice"])
        1
        """
        if self._wire is not None:
            return self._wire_call(lambda w: w.execute(sql, params))
        body = self._post_query(sql, params)
        return body.get("rows_affected", 0)

    def execute_many(self, sql: str, rows: Sequence[Sequence[Any]]) -> int:
        """Run one prepared ``sql`` once per parameter set, in order, on one
        connection.

        Native wire only. Returns total rows affected. Raises
        :class:`ParticleDBBatchError` (``sqlstate`` = the first failure's) if
        any set failed; outside a transaction the other sets were committed.

        Example
        -------
        >>> db.execute_many("INSERT INTO t (id, v) VALUES ($1, $2)",
        ...                 [(i, f"v{i}") for i in range(10_000)])
        10000
        """
        self._require_wire("execute_many")
        return self._wire_call(lambda w: w.execute_many(sql, rows))

    # ------------------------------------------------------------------
    # Sessions, transactions, streaming (native wire)
    # ------------------------------------------------------------------

    def session(self):
        """Pin one native-wire connection; see :class:`particledb.pdb_wire.Session`."""
        self._require_wire("session")
        return _SessionProxy(self._wire_call(lambda w: w.session()))

    @contextmanager
    def transaction(self) -> Iterator[Any]:
        """Interactive transaction on one pinned connection.

        Yields a :class:`particledb.pdb_wire.Session` already inside
        ``BEGIN``: ``query`` / ``execute`` / ``execute_many`` run in the
        transaction and reads see its own writes. ``COMMIT`` on normal exit,
        ``ROLLBACK`` on exception. DDL inside a transaction is refused by the
        server (SQLSTATE 25001); run it through :meth:`execute`.

        Example::

            with db.transaction() as tx:
                tx.execute("UPDATE acct SET bal = bal - $1 WHERE id = $2", [10, 1])
                if tx.query("SELECT bal FROM acct WHERE id = $1", [1])[0]["bal"] < 0:
                    raise ValueError("overdraft")   # rolled back
        """
        from .pdb_wire import PdbError

        proxy = self.session()
        sess = proxy._s
        try:
            with sess.transaction():
                yield proxy
        except PdbError as exc:
            raise _wrap_wire_error(exc) from exc
        finally:
            sess.close()

    def stream(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
        batch_size: int = 1000,
    ) -> Iterator[Dict[str, Any]]:
        """Iterate a query's rows as dicts in bounded memory.

        Native wire only. Rows are decoded one Arrow record batch at a time as
        the server's chunks arrive (no pyarrow needed), so neither the result's
        bytes nor its rows are ever held whole. ``batch_size`` is accepted for
        compatibility; the server sizes the batches.
        """
        self._require_wire("stream")
        from .pdb_wire import PdbError

        try:
            yield from self._wire.stream(sql, params, batch_size=batch_size)
        except PdbError as exc:
            raise _wrap_wire_error(exc) from exc

    # ------------------------------------------------------------------
    # Bulk insert
    # ------------------------------------------------------------------

    def bulk_insert(
        self,
        table: str,
        columns: List[str],
        rows: List[List[Any]],
    ) -> int:
        """Insert multiple rows in one request.

        Native wire: ONE atomic native bulk ingest (``F_BULK``, Arrow over the
        wire): every row or none; inside a transaction it joins it.
        HTTP: ``POST /bulk``.

        Returns the number of rows inserted.
        """
        if self._wire is not None:
            if not rows:
                return 0
            return self._wire_call(lambda w: w.bulk_insert(table, columns, rows))
        payload = {"table": table, "columns": columns, "rows": rows}
        body = self._http_post("/bulk", payload)
        return body.get("rows_inserted", 0)

    # ------------------------------------------------------------------
    # Table information
    # ------------------------------------------------------------------

    def tables(self) -> List[Dict[str, Any]]:
        """List all tables.

        Returns
        -------
        list of dict
            Each dict has ``"name"``, ``"row_count"``, and ``"columns"`` keys.
        """
        body = self._http_get("/tables")
        return body.get("tables", [])

    def table(self, name: str) -> Dict[str, Any]:
        """Get schema details for a specific table.

        Parameters
        ----------
        name : str
            Table name.

        Returns
        -------
        dict
            Table metadata including column definitions.
        """
        return self._http_get(f"/tables/{name}")

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    def health(self) -> Dict[str, Any]:
        """Health check.

        Returns
        -------
        dict
            HTTP: server health with ``"status"``, ``"version"``,
            ``"uptime_seconds"``.  Native wire: the wire client's health
            (``connected``, ``latency_ms``, ``protocol`` (unix/tcp),
            ``wire_protocol``, ``capabilities``, pool counts).
        """
        if self._wire is not None:
            return self._wire.health()
        return self._http_get("/health")

    def close(self) -> None:
        """Close pooled native-wire connections (no-op over HTTP)."""
        if self._wire is not None:
            self._wire.close()

    def __enter__(self) -> "ParticleDB":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Sub-clients (KV, Vector, RAG)
    # ------------------------------------------------------------------

    @property
    def kv(self) -> "KV":
        """Access key-value operations.

        Example::

            db.kv.set("session:123", '{"user":"alice"}')
            val = db.kv.get("session:123")
        """
        if self._kv is None:
            from particledb.kv import KV

            self._kv = KV(self)
        return self._kv

    @property
    def vector(self) -> "Vector":
        """Access vector similarity search operations.

        Example::

            results = db.vector.search("docs", "embedding", [0.1, 0.2, 0.3])
        """
        if self._vector is None:
            from particledb.vector import Vector

            self._vector = Vector(self)
        return self._vector

    @property
    def rag(self) -> "RAG":
        """Access RAG (retrieval-augmented generation) operations.

        Example::

            answer = db.rag.query("What is ParticleDB?", pipeline="qa")
        """
        if self._rag is None:
            from particledb.rag import RAG

            self._rag = RAG(self)
        return self._rag

    @property
    def ai(self) -> Any:
        """Access AI / LLM operations (:class:`particledb.ai.AI`).

        Example::

            vec = db.ai.embed("hello")
            label = db.ai.classify("I love it", "positive", "negative")
        """
        if self._ai is None:
            from particledb.ai import AI

            self._ai = AI(self)
        return self._ai

    @property
    def ops(self) -> Any:
        """Access extended operations (:class:`particledb.ops.Ops`): time
        travel, materialized views, changefeeds, geospatial, lakehouse."""
        if self._ops is None:
            from particledb.ops import Ops

            self._ops = Ops(self)
        return self._ops

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _post_query(
        self, sql: str, params: Optional[Sequence[Any]] = None
    ) -> Dict[str, Any]:
        """Send ``POST /v1/sql`` and return the parsed JSON body.

        The live server's REST SQL route is ``/v1/sql`` (the older ``/query``
        route was removed). The response body is
        ``{"rows": [{col: value, ...}], "columns": [...], "row_count": N}``.
        """
        payload: Dict[str, Any] = {"sql": sql}
        if params:
            payload["params"] = list(params)
        return self._http_post("/v1/sql", payload)

    def _http_post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Low-level HTTP POST returning parsed JSON."""
        url = self._base_url + path
        data = json.dumps(payload).encode("utf-8")

        req = urllib.request.Request(
            url,
            data=data,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        self._apply_auth(req)

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            self._handle_http_error(exc)
            raise  # unreachable, but keeps mypy happy
        except urllib.error.URLError as exc:
            raise ParticleDBError(
                f"Connection failed: {exc.reason}", http_status=None
            ) from exc

        self._check_error(body)
        return body

    def _http_get(self, path: str) -> Dict[str, Any]:
        """Low-level HTTP GET returning parsed JSON."""
        url = self._base_url + path

        req = urllib.request.Request(
            url,
            method="GET",
            headers={"Accept": "application/json"},
        )
        self._apply_auth(req)

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            self._handle_http_error(exc)
            raise
        except urllib.error.URLError as exc:
            raise ParticleDBError(
                f"Connection failed: {exc.reason}", http_status=None
            ) from exc

        self._check_error(body)
        return body

    def _apply_auth(self, req: urllib.request.Request) -> None:
        """Add an ``Authorization: Basic`` header when credentials are set.

        SECURITY: Basic auth base64-encodes (does NOT encrypt) the credentials,
        so sending it over cleartext HTTP would leak the password to any network
        observer. We therefore refuse to attach credentials to a non-TLS request
        (``tls=False``). Construct the client with ``tls=True`` (or an
        ``https://`` host) to authenticate. A caller who explicitly wants the
        old cleartext behavior for a trusted localhost/dev loop can set
        ``allow_insecure_auth=True``.
        """
        if not (self.user and self.password):
            return
        if not self.tls and not getattr(self, "allow_insecure_auth", False):
            raise ParticleDBError(
                "refusing to send Basic auth credentials over cleartext HTTP; "
                "use tls=True (https) or set allow_insecure_auth=True for a "
                "trusted local connection"
            )
        credentials = base64.b64encode(
            f"{self.user}:{self.password}".encode("utf-8")
        ).decode("ascii")
        req.add_header("Authorization", f"Basic {credentials}")

    @staticmethod
    def _check_error(body: Any) -> None:
        """Raise :class:`ParticleDBError` if the JSON body contains an error."""
        if isinstance(body, dict) and "error" in body:
            raise ParticleDBError(
                body["error"],
                code=body.get("code"),
            )

    @staticmethod
    def _handle_http_error(exc: urllib.error.HTTPError) -> None:
        """Translate an ``HTTPError`` into a :class:`ParticleDBError`."""
        try:
            body = json.loads(exc.read().decode("utf-8"))
            msg = body.get("error", f"HTTP {exc.code}")
            code = body.get("code")
        except (json.JSONDecodeError, UnicodeDecodeError):
            msg = f"HTTP {exc.code}: {exc.reason}"
            code = None

        raise ParticleDBError(msg, code=code, http_status=exc.code) from exc

    def __repr__(self) -> str:
        return (
            f"ParticleDB(host={self.host!r}, port={self.port!r}, "
            f"database={self.database!r}, transport={self._active_transport()!r})"
        )


def _is_absent(exc: BaseException) -> bool:
    """True when the native port refused the connection outright (nothing
    listening): the one failure ``transport="auto"`` may fall back on."""
    c: Optional[BaseException] = exc
    while c is not None:
        if isinstance(c, (builtins.ConnectionRefusedError, FileNotFoundError, TimeoutError)):
            return True
        c = c.__cause__
    return False
