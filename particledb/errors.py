"""Exception hierarchy of the native-wire client.

Every error carries ``sqlstate`` (the PostgreSQL class, never ``None``) and
``code`` (the server's numeric wire code, ``0`` for a client-side failure).
``particledb.pdb_wire`` re-exports these names; ``particledb.client`` wraps
them in :class:`particledb.client.ParticleDBError` with the same attributes.
"""

from __future__ import annotations

from typing import Any, List, Optional, Sequence, Tuple

GENERAL_ERROR = "HY000"
CONNECTION_FAILURE = "08006"
CONNECTION_DOES_NOT_EXIST = "08003"
PROTOCOL_VIOLATION = "08P01"
QUERY_CANCELED = "57014"


class ParticleDBError(Exception):
    """Base of every error this SDK raises.

    ``sqlstate`` is the PostgreSQL class (native wire: always set), ``code``
    the server's error code, ``http_status`` the HTTP status (HTTP API only).
    """

    def __init__(self, message: str, *, code: Any = None, http_status: Optional[int] = None,
                 sqlstate: Optional[str] = None):
        super().__init__(message)
        self.code = code
        self.http_status = http_status
        self.sqlstate = sqlstate


class PdbError(ParticleDBError):
    """Base class for all native-wire errors (``code`` is the numeric wire
    code, ``0`` for a client-side failure; ``sqlstate`` never ``None``)."""

    def __init__(self, message: str, code: int = 0, sqlstate: Optional[str] = None):
        super().__init__(message, code=code, sqlstate=sqlstate or GENERAL_ERROR)


class ConnectionError(PdbError):  # noqa: A001 - historical public name
    """The connection is unusable: connect failure, loss, GOAWAY, protocol
    violation. SQLSTATE class 08."""

    def __init__(self, message: str, code: int = 0, sqlstate: Optional[str] = None):
        super().__init__(message, code, sqlstate or CONNECTION_FAILURE)


class _ConnectFailed(ConnectionError):
    """A NEW connection failed to connect or to complete HELLO/AUTH.

    No byte of any request was written on it, so the request that needed it
    can safely be retried. Every other transport failure is ambiguous.
    """


class ProtocolError(ConnectionError):
    """The server sent something this client cannot interpret (08P01)."""

    def __init__(self, message: str, code: int = 0):
        super().__init__(message, code, PROTOCOL_VIOLATION)


class QueryError(PdbError):
    """The server answered a request with a SQL-level error."""


class QueryCanceledError(QueryError):
    """The statement was cancelled (``cancel()``, a client timeout, or the
    server's ``statement_timeout``): SQLSTATE 57014.

    ``ambiguous`` is True only when the client gave up waiting and the server
    never answered the cancel: the statement may still run (and commit), so
    the connection was closed.
    """

    def __init__(self, message: str, code: int = 0, sqlstate: Optional[str] = None,
                 ambiguous: bool = False):
        super().__init__(message, code, sqlstate or QUERY_CANCELED)
        self.ambiguous = ambiguous


class TransactionRolledBackError(QueryError):
    """COMMIT was answered by rolling back a failed transaction: nothing was
    committed (a statement inside it failed and its error was caught)."""

    def __init__(self, message: str, code: int = 0, sqlstate: Optional[str] = None):
        super().__init__(message, code, sqlstate or "40000")


class DiskFullError(PdbError):
    """The server signalled that it ran out of disk space."""


class BatchExecuteError(QueryError):
    """One or more parameter sets of an ``execute_many`` failed.

    Outside an explicit transaction every parameter set autocommits on its own:
    the rows that succeeded ARE committed, the failed ones are not. Inside
    ``BEGIN``..``COMMIT`` the client stops at the first failure and the
    transaction is failed (ROLLBACK it).

    Attributes:
        errors:    ``[(row_index, server_message), ...]``.
        affected:  Rows affected by the parameter sets that succeeded.
        attempted: Parameter sets the server answered.
        sqlstate:  The FIRST failure's SQLSTATE.
    """

    def __init__(self, errors: List[Tuple[int, str]], affected: int, attempted: int, total: int,
                 sqlstate: Optional[str] = None, code: int = 0,
                 causes: Optional[Sequence[PdbError]] = None):
        first_idx, first_msg = errors[0]
        super().__init__(
            f"{len(errors)} of {total} parameter set(s) failed "
            f"({attempted} attempted, {affected} row(s) affected); "
            f"first: row {first_idx}: {first_msg}",
            code,
            sqlstate,
        )
        self.errors = errors
        self.affected = affected
        self.attempted = attempted
        self.total = total
        self.causes: List[PdbError] = list(causes or [])


class BatchError(QueryError):
    """A statement of a ``batch()`` (one ``BATCH`` frame) failed. The server
    stopped there; ``completed`` holds the affected-row counts of the
    statements before it (``-1`` for a row-returning one) and ``index`` the
    failed statement's position."""

    def __init__(self, message: str, code: int, sqlstate: str, index: int, completed: List[int]):
        super().__init__(message, code, sqlstate)
        self.index = index
        self.completed = completed

