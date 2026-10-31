"""PEP 249 exception hierarchy, mapped from SQLSTATE.

The tree is the one PEP 249 prescribes::

    Warning
    Error
        InterfaceError
        DatabaseError
            DataError
            OperationalError
            IntegrityError
            InternalError
            ProgrammingError
            NotSupportedError

``Error`` also derives from :class:`particledb.ParticleDBError`, so code that
already catches the SDK's base class keeps working. Every error carries
``sqlstate`` (PostgreSQL's five-character class; ``pgcode`` is an alias for
psycopg2-style code) and ``code`` (the server's numeric wire code, ``0`` for
a client-side failure). The original native-wire exception is ``__cause__``.

The class an SQLSTATE maps to follows PostgreSQL's class table the way
psycopg maps it (class ``23`` -> :class:`IntegrityError`, ``22`` ->
:class:`DataError`, ``42`` -> :class:`ProgrammingError`, ``40`` / ``08`` /
``53`` / ``57`` -> :class:`OperationalError`, ``0A`` ->
:class:`NotSupportedError`, ``XX`` / ``25`` -> :class:`InternalError`).
Frequent states also have their own subclass, named as psycopg names them
(``UniqueViolation`` 23505, ``SerializationFailure`` 40001, ...): ``except
errors.UniqueViolation`` and ``except IntegrityError`` both catch a duplicate
key. :func:`lookup` returns the class for any SQLSTATE.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Type

from ..errors import ParticleDBError as _SDKError


class Warning(Exception):  # noqa: A001 - PEP 249 name
    """Important warnings (PEP 249). Server notices are not raised: they are
    collected on ``Cursor.messages`` (see :class:`~particledb.Notice`)."""


class Error(_SDKError):
    """Base of every DB-API error."""

    sqlstate: Optional[str] = None

    def __init__(self, message: str = "", *, sqlstate: Optional[str] = None, code: Any = 0):
        super().__init__(message, code=code, sqlstate=sqlstate or type(self).sqlstate)

    @property
    def pgcode(self) -> Optional[str]:
        """psycopg2's name for :attr:`sqlstate`."""
        return self.sqlstate


class InterfaceError(Error):
    """An error of the client (a closed cursor or connection, misuse)."""


class DatabaseError(Error):
    """An error the database reported."""


class DataError(DatabaseError):
    """A problem with the processed data (division by zero, out of range, bad text)."""


class OperationalError(DatabaseError):
    """The database's operation failed: connection loss, cancel, serialization
    failure, deadlock, resource exhaustion."""


class IntegrityError(DatabaseError):
    """A constraint was violated (unique, foreign key, not null, check)."""


class InternalError(DatabaseError):
    """The database hit an internal error, or the transaction is in a state
    that forbids the operation (25xxx)."""


class ProgrammingError(DatabaseError):
    """The statement is wrong: syntax, unknown table or column, wrong number
    of parameters, an unadaptable parameter."""


class NotSupportedError(DatabaseError):
    """The server does not support the feature (0A000)."""


# ── SQLSTATE-specific classes (psycopg names) ───────────────────────────────

_BY_STATE: Dict[str, Type[Error]] = {}


def _state(state: str, name: str, base: Type[Error]) -> Type[Error]:
    cls = type(name, (base,), {"sqlstate": state, "__doc__": f"SQLSTATE {state}.",
                               "__module__": __name__})
    _BY_STATE[state] = cls
    return cls


UniqueViolation = _state("23505", "UniqueViolation", IntegrityError)
ForeignKeyViolation = _state("23503", "ForeignKeyViolation", IntegrityError)
NotNullViolation = _state("23502", "NotNullViolation", IntegrityError)
CheckViolation = _state("23514", "CheckViolation", IntegrityError)
ExclusionViolation = _state("23P01", "ExclusionViolation", IntegrityError)
RestrictViolation = _state("23001", "RestrictViolation", IntegrityError)
DivisionByZero = _state("22012", "DivisionByZero", DataError)
NumericValueOutOfRange = _state("22003", "NumericValueOutOfRange", DataError)
InvalidTextRepresentation = _state("22P02", "InvalidTextRepresentation", DataError)
InvalidDatetimeFormat = _state("22007", "InvalidDatetimeFormat", DataError)
DatetimeFieldOverflow = _state("22008", "DatetimeFieldOverflow", DataError)
StringDataRightTruncation = _state("22001", "StringDataRightTruncation", DataError)
InvalidParameterValue = _state("22023", "InvalidParameterValue", DataError)
CharacterNotInRepertoire = _state("22021", "CharacterNotInRepertoire", DataError)
SerializationFailure = _state("40001", "SerializationFailure", OperationalError)
DeadlockDetected = _state("40P01", "DeadlockDetected", OperationalError)
TransactionRollback = _state("40000", "TransactionRollback", OperationalError)
QueryCanceled = _state("57014", "QueryCanceled", OperationalError)
AdminShutdown = _state("57P01", "AdminShutdown", OperationalError)
LockNotAvailable = _state("55P03", "LockNotAvailable", OperationalError)
DiskFull = _state("53100", "DiskFull", OperationalError)
OutOfMemory = _state("53200", "OutOfMemory", OperationalError)
TooManyConnections = _state("53300", "TooManyConnections", OperationalError)
ConnectionFailure = _state("08006", "ConnectionFailure", OperationalError)
ConnectionDoesNotExist = _state("08003", "ConnectionDoesNotExist", OperationalError)
ProtocolViolation = _state("08P01", "ProtocolViolation", OperationalError)
InvalidPassword = _state("28P01", "InvalidPassword", OperationalError)
InvalidAuthorizationSpecification = _state("28000", "InvalidAuthorizationSpecification", OperationalError)
SyntaxError = _state("42601", "SyntaxError", ProgrammingError)  # noqa: A001 - psycopg name
UndefinedTable = _state("42P01", "UndefinedTable", ProgrammingError)
UndefinedColumn = _state("42703", "UndefinedColumn", ProgrammingError)
UndefinedFunction = _state("42883", "UndefinedFunction", ProgrammingError)
UndefinedObject = _state("42704", "UndefinedObject", ProgrammingError)
DuplicateTable = _state("42P07", "DuplicateTable", ProgrammingError)
DuplicateColumn = _state("42701", "DuplicateColumn", ProgrammingError)
DuplicateObject = _state("42710", "DuplicateObject", ProgrammingError)
DuplicatePreparedStatement = _state("42P05", "DuplicatePreparedStatement", ProgrammingError)
DatatypeMismatch = _state("42804", "DatatypeMismatch", ProgrammingError)
AmbiguousColumn = _state("42702", "AmbiguousColumn", ProgrammingError)
InsufficientPrivilege = _state("42501", "InsufficientPrivilege", ProgrammingError)
InvalidSqlStatementName = _state("26000", "InvalidSqlStatementName", ProgrammingError)
InvalidSchemaName = _state("3F000", "InvalidSchemaName", ProgrammingError)
InvalidCatalogName = _state("3D000", "InvalidCatalogName", ProgrammingError)
InFailedSqlTransaction = _state("25P02", "InFailedSqlTransaction", InternalError)
ActiveSqlTransaction = _state("25001", "ActiveSqlTransaction", InternalError)
NoActiveSqlTransaction = _state("25P01", "NoActiveSqlTransaction", InternalError)
ReadOnlySqlTransaction = _state("25006", "ReadOnlySqlTransaction", InternalError)
FeatureNotSupported = _state("0A000", "FeatureNotSupported", NotSupportedError)
RaiseException = _state("P0001", "RaiseException", InternalError)
InternalError_ = _state("XX000", "InternalError_", InternalError)

#: SQLSTATE class (first two characters) -> base class.
_BY_CLASS: Dict[str, Type[Error]] = {
    "01": DatabaseError, "02": DatabaseError, "03": DatabaseError,
    "08": OperationalError, "09": DatabaseError, "0A": NotSupportedError,
    "0B": DatabaseError, "0F": DatabaseError, "0L": ProgrammingError, "0P": ProgrammingError,
    "0Z": DatabaseError, "20": ProgrammingError, "21": ProgrammingError, "22": DataError,
    "23": IntegrityError, "24": InternalError, "25": InternalError, "26": ProgrammingError,
    "27": OperationalError, "28": OperationalError, "2B": InternalError, "2D": InternalError,
    "2F": OperationalError, "34": ProgrammingError, "38": OperationalError, "39": OperationalError,
    "3B": OperationalError, "3D": ProgrammingError, "3F": ProgrammingError, "40": OperationalError,
    "42": ProgrammingError, "44": ProgrammingError, "53": OperationalError, "54": OperationalError,
    "55": OperationalError, "57": OperationalError, "58": OperationalError, "72": DatabaseError,
    "F0": InternalError, "HV": OperationalError, "P0": InternalError, "XX": InternalError,
    "HY": DatabaseError,
}


def lookup(sqlstate: Optional[str]) -> Type[Error]:
    """The exception class for ``sqlstate``: its own subclass when it has one,
    else its class's PEP 249 base, else :class:`DatabaseError`."""
    if not sqlstate:
        return DatabaseError
    cls = _BY_STATE.get(sqlstate)
    if cls is not None:
        return cls
    return _BY_CLASS.get(sqlstate[:2], DatabaseError)


def translate(exc: BaseException) -> BaseException:
    """The DB-API exception for an exception of the native layer (or the
    encoder). Already-translated errors and unrelated exceptions (a
    ``KeyboardInterrupt``) are returned unchanged."""
    if isinstance(exc, Error):
        return exc
    from .. import errors as _native

    if isinstance(exc, _native.PdbError):
        state = exc.sqlstate or "HY000"
        cls = lookup(state)
        if isinstance(exc, _native.ProtocolError):
            cls = ProtocolViolation
        out = cls(str(exc), sqlstate=state, code=exc.code)
        for attr in ("ambiguous", "errors", "affected", "attempted", "index", "completed"):
            if hasattr(exc, attr):
                setattr(out, attr, getattr(exc, attr))
        return out
    if isinstance(exc, _SDKError):
        state = exc.sqlstate or "HY000"
        return lookup(state)(str(exc), sqlstate=state, code=exc.code)
    if isinstance(exc, TypeError):
        return ProgrammingError(str(exc), sqlstate="42804")
    if isinstance(exc, OverflowError):
        return NumericValueOutOfRange(str(exc))
    if isinstance(exc, ValueError):
        return DataError(str(exc), sqlstate="22023")
    return exc


__all__ = [
    "Warning", "Error", "InterfaceError", "DatabaseError", "DataError", "OperationalError",
    "IntegrityError", "InternalError", "ProgrammingError", "NotSupportedError", "lookup",
    "translate",
] + sorted(c.__name__ for c in _BY_STATE.values())
