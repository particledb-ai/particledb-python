"""gRPC client for ParticleDB.

Requires the ``grpc`` optional dependency::

    pip install particledb[grpc]

Uses the ``ParticleDB`` gRPC service defined in ``particledb.proto``.

Example::

    from particledb import GrpcClient

    client = GrpcClient(host="localhost", port=26257)

    # Simple query
    rows = client.query("SELECT * FROM users")

    # Streaming query
    for row in client.stream_query("SELECT * FROM large_table"):
        print(row)

    # Batch execute
    results = client.batch_execute([
        "INSERT INTO t (id) VALUES (1)",
        "INSERT INTO t (id) VALUES (2)",
    ])

    # Health check
    print(client.health())
"""

from __future__ import annotations

from typing import Any, Dict, Generator, List, Optional, Sequence

try:
    import grpc
    from google.protobuf import descriptor_pool, descriptor_pb2, symbol_database
except ImportError as exc:
    raise ImportError(
        "gRPC dependencies are not installed.  "
        "Install them with: pip install particledb[grpc]"
    ) from exc


# ---------------------------------------------------------------------------
# Lightweight protobuf message wrappers
#
# Instead of requiring generated _pb2 stubs, we build minimal request/response
# classes that match the proto wire format.  This keeps the SDK self-contained
# while still speaking valid protobuf over gRPC.
# ---------------------------------------------------------------------------

from google.protobuf import descriptor_pb2 as _dpb
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database


def _make_value(v: Any) -> Dict[str, Any]:
    """Convert a Python value into a dict matching the ``Value.kind`` oneof."""
    if v is None:
        return {"null_value": True}
    if isinstance(v, bool):
        return {"bool_value": v}
    if isinstance(v, int):
        return {"int_value": v}
    if isinstance(v, float):
        return {"float_value": v}
    if isinstance(v, bytes):
        return {"bytes_value": v}
    return {"string_value": str(v)}


def _extract_value(kind_field: str, raw: Any) -> Any:
    """Extract a Python value from a Value-like protobuf field."""
    if kind_field == "null_value":
        return None
    return raw


# ---------------------------------------------------------------------------
# Serialisation helpers (hand-rolled protobuf encoding/decoding)
#
# We encode just enough of the proto wire format to avoid depending on
# generated stubs while remaining compatible with the server.
# ---------------------------------------------------------------------------

import struct


def _encode_varint(value: int) -> bytes:
    """Encode an unsigned integer as a protobuf varint."""
    parts = []
    while value > 0x7F:
        parts.append((value & 0x7F) | 0x80)
        value >>= 7
    parts.append(value & 0x7F)
    return bytes(parts)


def _encode_signed_varint(value: int) -> bytes:
    """Encode a signed int64 as a protobuf varint (two's complement)."""
    if value < 0:
        value = value + (1 << 64)
    return _encode_varint(value)


def _encode_length_delimited(field_number: int, data: bytes) -> bytes:
    """Encode a length-delimited protobuf field."""
    tag = _encode_varint((field_number << 3) | 2)
    return tag + _encode_varint(len(data)) + data


def _encode_varint_field(field_number: int, value: int) -> bytes:
    """Encode a varint protobuf field."""
    tag = _encode_varint((field_number << 3) | 0)
    return tag + _encode_signed_varint(value)


def _encode_string_field(field_number: int, value: str) -> bytes:
    """Encode a string protobuf field."""
    return _encode_length_delimited(field_number, value.encode("utf-8"))


def _encode_double_field(field_number: int, value: float) -> bytes:
    """Encode a double (float64) protobuf field."""
    tag = _encode_varint((field_number << 3) | 1)
    return tag + struct.pack("<d", value)


def _encode_bool_field(field_number: int, value: bool) -> bytes:
    """Encode a bool protobuf field."""
    return _encode_varint_field(field_number, 1 if value else 0)


def _encode_bytes_field(field_number: int, value: bytes) -> bytes:
    """Encode a bytes protobuf field."""
    return _encode_length_delimited(field_number, value)


def _encode_value(v: Any) -> bytes:
    """Encode a Python value as a ``Value`` message (particledb.v1.Value)."""
    if v is None:
        # null_value = field 5, bool true
        return _encode_bool_field(5, True)
    if isinstance(v, bool):
        return _encode_bool_field(4, v)
    if isinstance(v, int):
        return _encode_varint_field(1, v)
    if isinstance(v, float):
        return _encode_double_field(2, v)
    if isinstance(v, bytes):
        return _encode_bytes_field(6, v)
    return _encode_string_field(3, str(v))


def _encode_execute_sql_request(sql: str, params: Sequence[Any]) -> bytes:
    """Encode an ``ExecuteSqlRequest`` message."""
    data = _encode_string_field(1, sql)
    for p in params:
        value_bytes = _encode_value(p)
        data += _encode_length_delimited(2, value_bytes)
    return data


def _encode_batch_execute_request(statements: Sequence[str]) -> bytes:
    """Encode a ``BatchExecuteRequest`` message."""
    data = b""
    for stmt in statements:
        data += _encode_string_field(1, stmt)
    return data


def _encode_subscribe_request(table: str, events: Sequence[str]) -> bytes:
    """Encode a ``SubscribeRequest`` message."""
    data = _encode_string_field(1, table)
    for event in events:
        data += _encode_string_field(2, event)
    return data


# ---------------------------------------------------------------------------
# Protobuf decoding helpers
# ---------------------------------------------------------------------------

def _decode_varint(buf: bytes, pos: int) -> tuple:
    """Decode a varint from buf starting at pos.  Returns (value, new_pos)."""
    result = 0
    shift = 0
    while pos < len(buf):
        b = buf[pos]
        result |= (b & 0x7F) << shift
        pos += 1
        if (b & 0x80) == 0:
            return result, pos
        shift += 7
    raise ValueError("Truncated varint")


def _decode_signed_varint(value: int) -> int:
    """Convert unsigned varint back to signed int64."""
    if value >= (1 << 63):
        value -= (1 << 64)
    return value


def _decode_fields(buf: bytes) -> List[tuple]:
    """Decode all fields from a protobuf message into (field_number, wire_type, data)."""
    fields = []
    pos = 0
    while pos < len(buf):
        tag, pos = _decode_varint(buf, pos)
        field_number = tag >> 3
        wire_type = tag & 0x07

        if wire_type == 0:  # varint
            value, pos = _decode_varint(buf, pos)
            fields.append((field_number, wire_type, value))
        elif wire_type == 1:  # 64-bit
            value = struct.unpack("<d", buf[pos : pos + 8])[0]
            pos += 8
            fields.append((field_number, wire_type, value))
        elif wire_type == 2:  # length-delimited
            length, pos = _decode_varint(buf, pos)
            data = buf[pos : pos + length]
            pos += length
            fields.append((field_number, wire_type, data))
        elif wire_type == 5:  # 32-bit
            pos += 4
        else:
            break

    return fields


def _decode_value(buf: bytes) -> Any:
    """Decode a ``Value`` message into a Python value."""
    fields = _decode_fields(buf)
    for field_number, wire_type, data in fields:
        if field_number == 1 and wire_type == 0:  # int_value
            return _decode_signed_varint(data)
        if field_number == 2 and wire_type == 1:  # float_value
            return data
        if field_number == 3 and wire_type == 2:  # string_value
            return data.decode("utf-8")
        if field_number == 4 and wire_type == 0:  # bool_value
            return bool(data)
        if field_number == 5 and wire_type == 0:  # null_value
            return None
        if field_number == 6 and wire_type == 2:  # bytes_value
            return data
    return None


def _decode_column(buf: bytes) -> Dict[str, str]:
    """Decode a ``Column`` message (name + type)."""
    result: Dict[str, str] = {"name": "", "type": ""}
    for field_number, wire_type, data in _decode_fields(buf):
        if field_number == 1 and wire_type == 2:
            result["name"] = data.decode("utf-8")
        elif field_number == 2 and wire_type == 2:
            result["type"] = data.decode("utf-8")
    return result


def _decode_row(buf: bytes) -> List[Any]:
    """Decode a ``Row`` message into a list of Python values."""
    values = []
    for field_number, wire_type, data in _decode_fields(buf):
        if field_number == 1 and wire_type == 2:
            values.append(_decode_value(data))
    return values


def _decode_execute_sql_response(buf: bytes) -> Dict[str, Any]:
    """Decode an ``ExecuteSqlResponse`` message."""
    columns: List[Dict[str, str]] = []
    rows: List[List[Any]] = []
    rows_affected = 0
    error = ""

    for field_number, wire_type, data in _decode_fields(buf):
        if field_number == 1 and wire_type == 2:
            columns.append(_decode_column(data))
        elif field_number == 2 and wire_type == 2:
            rows.append(_decode_row(data))
        elif field_number == 3 and wire_type == 0:
            rows_affected = _decode_signed_varint(data)
        elif field_number == 4 and wire_type == 2:
            error = data.decode("utf-8")

    return {
        "columns": columns,
        "rows": rows,
        "rows_affected": rows_affected,
        "error": error,
    }


def _decode_health_response(buf: bytes) -> Dict[str, str]:
    """Decode a ``HealthResponse`` message."""
    result: Dict[str, str] = {"status": "", "version": ""}
    for field_number, wire_type, data in _decode_fields(buf):
        if field_number == 1 and wire_type == 2:
            result["status"] = data.decode("utf-8")
        elif field_number == 2 and wire_type == 2:
            result["version"] = data.decode("utf-8")
    return result


def _decode_change_event(buf: bytes) -> Dict[str, Any]:
    """Decode a ``ChangeEvent`` message."""
    result: Dict[str, Any] = {"event": "", "table": "", "rows": 0, "payload": ""}
    for field_number, wire_type, data in _decode_fields(buf):
        if field_number == 1 and wire_type == 2:
            result["event"] = data.decode("utf-8")
        elif field_number == 2 and wire_type == 2:
            result["table"] = data.decode("utf-8")
        elif field_number == 3 and wire_type == 0:
            result["rows"] = _decode_signed_varint(data)
        elif field_number == 4 and wire_type == 2:
            result["payload"] = data.decode("utf-8")
    return result


# ---------------------------------------------------------------------------
# gRPC method descriptors
# ---------------------------------------------------------------------------

_SERVICE_NAME = "particledb.v1.ParticleDB"

# Method paths
_EXECUTE_SQL = f"/{_SERVICE_NAME}/ExecuteSQL"
_BATCH_EXECUTE = f"/{_SERVICE_NAME}/BatchExecute"
_SUBSCRIBE = f"/{_SERVICE_NAME}/Subscribe"
_HEALTH = f"/{_SERVICE_NAME}/Health"


class _Serializer:
    """Minimal gRPC (de)serializer that wraps raw bytes."""

    def __init__(self, encoder=None, decoder=None):
        self._encoder = encoder
        self._decoder = decoder

    def __call__(self, data):
        if self._decoder:
            return self._decoder(data)
        return data


# ---------------------------------------------------------------------------
# Public client
# ---------------------------------------------------------------------------

class GrpcClient:
    """ParticleDB gRPC client.

    Connects to the ParticleDB gRPC service (``particledb.v1.ParticleDB``)
    for SQL execution, batch operations, CDC subscriptions, and health checks.

    Parameters
    ----------
    host : str
        Server hostname.  Default: ``"localhost"``.
    port : int
        gRPC port.  Default: ``26257``.
    secure : bool
        Use TLS.  Default: ``False``.
    credentials : grpc.ChannelCredentials, optional
        Custom channel credentials for TLS.  Ignored when *secure* is ``False``.
    options : list of tuple, optional
        gRPC channel options (e.g. max message size).
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 26257,
        *,
        secure: bool = False,
        credentials: Optional[Any] = None,
        options: Optional[List[tuple]] = None,
    ):
        self.host = host
        self.port = port
        self._target = f"{host}:{port}"

        channel_options = options or []

        if secure:
            creds = credentials or grpc.ssl_channel_credentials()
            self._channel = grpc.secure_channel(
                self._target, creds, options=channel_options
            )
        else:
            self._channel = grpc.insecure_channel(
                self._target, options=channel_options
            )

    # ------------------------------------------------------------------
    # SQL execution
    # ------------------------------------------------------------------

    def execute_sql(
        self, sql: str, params: Optional[Sequence[Any]] = None
    ) -> Dict[str, Any]:
        """Execute a single SQL statement over gRPC.

        Parameters
        ----------
        sql : str
            SQL statement.
        params : list, optional
            Positional parameter values.

        Returns
        -------
        dict
            Parsed response with ``"columns"``, ``"rows"``, ``"rows_affected"``,
            and ``"error"`` keys.

        Raises
        ------
        grpc.RpcError
            On gRPC transport errors.
        ParticleDBError
            If the server response contains a non-empty ``error`` field.
        """
        request_bytes = _encode_execute_sql_request(sql, params or [])

        response_bytes = self._channel.unary_unary(
            _EXECUTE_SQL,
            request_serializer=lambda x: x,
            response_deserializer=lambda x: x,
        )(request_bytes)

        result = _decode_execute_sql_response(response_bytes)
        if result.get("error"):
            from particledb.client import ParticleDBError
            raise ParticleDBError(result["error"])
        return result

    def query(
        self, sql: str, params: Optional[Sequence[Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a SQL query and return rows as a list of dicts.

        Parameters
        ----------
        sql : str
            SQL statement.
        params : list, optional
            Positional parameter values.

        Returns
        -------
        list of dict
            Each dict maps column name to its value.
        """
        result = self.execute_sql(sql, params)
        col_names = [c["name"] for c in result.get("columns", [])]
        return [
            {col_names[i]: v for i, v in enumerate(row)}
            for row in result.get("rows", [])
        ]

    # ------------------------------------------------------------------
    # Streaming query (CDC subscribe)
    # ------------------------------------------------------------------

    def stream_query(
        self,
        table: str,
        events: Optional[Sequence[str]] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """Subscribe to CDC events on a table.

        Yields ``ChangeEvent`` dicts as the server streams them.

        Parameters
        ----------
        table : str
            Table to subscribe to.
        events : list of str, optional
            Event types to listen for (``"INSERT"``, ``"UPDATE"``, ``"DELETE"``).
            Default: all events.

        Yields
        ------
        dict
            Each dict has ``"event"``, ``"table"``, ``"rows"``, and ``"payload"``.
        """
        if events is None:
            events = ["INSERT", "UPDATE", "DELETE"]

        request_bytes = _encode_subscribe_request(table, events)

        response_iterator = self._channel.unary_stream(
            _SUBSCRIBE,
            request_serializer=lambda x: x,
            response_deserializer=lambda x: x,
        )(request_bytes)

        for response_bytes in response_iterator:
            yield _decode_change_event(response_bytes)

    # ------------------------------------------------------------------
    # Batch execute
    # ------------------------------------------------------------------

    def batch_execute(
        self, statements: Sequence[str]
    ) -> List[Dict[str, Any]]:
        """Execute multiple SQL statements in sequence.

        Parameters
        ----------
        statements : list of str
            SQL statements to execute.

        Returns
        -------
        list of dict
            One result dict per statement, each with ``"columns"``, ``"rows"``,
            ``"rows_affected"``, and ``"error"`` keys.
        """
        request_bytes = _encode_batch_execute_request(statements)

        response_bytes = self._channel.unary_unary(
            _BATCH_EXECUTE,
            request_serializer=lambda x: x,
            response_deserializer=lambda x: x,
        )(request_bytes)

        # Decode BatchExecuteResponse: field 1 is repeated ExecuteSqlResponse
        results = []
        for field_number, wire_type, data in _decode_fields(response_bytes):
            if field_number == 1 and wire_type == 2:
                results.append(_decode_execute_sql_response(data))
        return results

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    def health(self) -> Dict[str, str]:
        """Health check.

        Returns
        -------
        dict
            Server health with ``"status"`` and ``"version"`` keys.
        """
        # HealthRequest is an empty message
        request_bytes = b""

        response_bytes = self._channel.unary_unary(
            _HEALTH,
            request_serializer=lambda x: x,
            response_deserializer=lambda x: x,
        )(request_bytes)

        return _decode_health_response(response_bytes)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the gRPC channel."""
        self._channel.close()

    def __enter__(self) -> "GrpcClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"GrpcClient(host={self.host!r}, port={self.port!r})"
