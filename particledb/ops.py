"""Extended database operations for ParticleDB.

Provides methods for time travel queries, materialized views, changefeeds/CDC,
geospatial functions, time-series operations, multi-tenant namespace management,
and lakehouse file access -- all backed by ParticleDB SQL.

Example::

    from particledb import ParticleDB

    db = ParticleDB()

    # Time travel
    rows = db.ops.query_as_of("SELECT * FROM orders", "2024-01-01T00:00:00Z")

    # Materialized views
    db.ops.create_materialized_view("active_users", "SELECT * FROM users WHERE active")
    db.ops.refresh_materialized_view("active_users")

    # Lakehouse
    rows = db.ops.read_parquet("/data/events.parquet")
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from particledb.client import ParticleDBError

if TYPE_CHECKING:
    from particledb.client import ParticleDB

#: Message raised by the deprecated namespace methods. ``{verb}`` is the
#: SQL verb the caller was reaching for.
_NAMESPACE_UNSUPPORTED = (
    "{verb} is not implemented by any ParticleDB server: there is no namespace "
    "surface on any wire, so this call can never succeed. Use a table-name "
    "prefix, a tenant column filtered on every query, or a separate server per "
    "tenant. See https://particledb.ai/getting-started/databases-and-schemas/"
)


def _quote_ident(name: str) -> str:
    """Quote a SQL identifier by wrapping in double quotes with internal escaping."""
    return '"' + name.replace('"', '""') + '"'


def _escape_literal(value: str) -> str:
    """Escape a SQL string literal by doubling single quotes."""
    return value.replace("'", "''")


class Ops:
    """Extended database operations backed by ParticleDB SQL.

    Typically accessed via ``ParticleDB.ops`` rather than instantiated directly.

    Groups functionality into:

    - **Time travel** -- query_as_of
    - **Materialized views** -- create, refresh, drop
    - **Changefeeds / CDC** -- create, pause, resume, drop, list
    - **Geospatial** -- st_distance, st_within
    - **Time-series** -- retention policies, continuous aggregates
    - **Multi-tenant** -- namespace management
    - **Lakehouse** -- read_parquet, read_csv
    """

    def __init__(self, client: "ParticleDB"):
        self._client = client

    # ------------------------------------------------------------------
    # Time travel
    # ------------------------------------------------------------------

    def query_as_of(
        self, sql: str, timestamp: str, params: Optional[list] = None
    ) -> List[Dict[str, Any]]:
        """Execute a query as of a historical timestamp.

        Parameters
        ----------
        sql : str
            The SQL query to execute.
        timestamp : str
            ISO-8601 timestamp (e.g. ``"2024-01-01T00:00:00Z"``).
        params : list, optional
            Positional parameter values.

        Returns
        -------
        list of dict
            Query result rows.

        Example
        -------
        >>> db.ops.query_as_of("SELECT * FROM orders", "2024-01-01T00:00:00Z")
        [{'id': 1, 'total': 99.99}, ...]
        """
        as_of_sql = f"SET TIMESTAMP AS OF '{_escape_literal(timestamp)}'; {sql}"
        return self._client.query(as_of_sql, params)

    # ------------------------------------------------------------------
    # Materialized views
    # ------------------------------------------------------------------

    def create_materialized_view(
        self,
        name: str,
        query: str,
        refresh: str = "manual",
    ) -> int:
        """Create a materialized view.

        Parameters
        ----------
        name : str
            Name for the materialized view.
        query : str
            The SELECT query defining the view contents.
        refresh : str
            Refresh strategy: ``"manual"`` (default) or an interval like
            ``"1h"``, ``"5m"``.

        Returns
        -------
        int
            Number of rows affected (typically 0 for DDL).

        Example
        -------
        >>> db.ops.create_materialized_view("active_users",
        ...     "SELECT * FROM users WHERE active = true")
        0
        """
        if refresh == "manual":
            sql = f"CREATE MATERIALIZED VIEW {_quote_ident(name)} AS {query}"
        else:
            sql = (
                f"CREATE MATERIALIZED VIEW {_quote_ident(name)} "
                f"WITH (refresh_interval = '{_escape_literal(refresh)}') AS {query}"
            )
        return self._client.execute(sql)

    def refresh_materialized_view(self, name: str) -> int:
        """Trigger a refresh of a materialized view.

        Parameters
        ----------
        name : str
            Name of the materialized view to refresh.

        Returns
        -------
        int
            Number of rows affected.

        Example
        -------
        >>> db.ops.refresh_materialized_view("active_users")
        0
        """
        sql = f"REFRESH MATERIALIZED VIEW {_quote_ident(name)}"
        return self._client.execute(sql)

    def drop_materialized_view(self, name: str) -> int:
        """Drop a materialized view.

        Parameters
        ----------
        name : str
            Name of the materialized view to drop.

        Returns
        -------
        int
            Number of rows affected.

        Example
        -------
        >>> db.ops.drop_materialized_view("active_users")
        0
        """
        sql = f"DROP MATERIALIZED VIEW IF EXISTS {_quote_ident(name)}"
        return self._client.execute(sql)

    # ------------------------------------------------------------------
    # Changefeeds / CDC
    # ------------------------------------------------------------------

    def create_changefeed(
        self,
        name: str,
        table: str,
        sink_type: str,
        sink_url: str,
    ) -> int:
        """Create a changefeed for change data capture.

        Parameters
        ----------
        name : str
            Name for the changefeed.
        table : str
            Source table to watch for changes.
        sink_type : str
            Sink type (e.g. ``"kafka"``, ``"webhook"``, ``"s3"``).
        sink_url : str
            Sink destination URL.

        Returns
        -------
        int
            Number of rows affected.

        Example
        -------
        >>> db.ops.create_changefeed("orders_feed", "orders",
        ...     "kafka", "kafka://localhost:9092/orders-topic")
        0
        """
        sql = (
            f"CREATE CHANGEFEED {_quote_ident(name)} ON {_quote_ident(table)} "
            f"INTO '{_escape_literal(sink_type)}' WITH (sink_url = '{_escape_literal(sink_url)}')"
        )
        return self._client.execute(sql)

    def pause_changefeed(self, name: str) -> int:
        """Pause a running changefeed.

        Parameters
        ----------
        name : str
            Name of the changefeed to pause.

        Returns
        -------
        int
            Number of rows affected.
        """
        sql = f"ALTER CHANGEFEED {_quote_ident(name)} PAUSE"
        return self._client.execute(sql)

    def resume_changefeed(self, name: str) -> int:
        """Resume a paused changefeed.

        Parameters
        ----------
        name : str
            Name of the changefeed to resume.

        Returns
        -------
        int
            Number of rows affected.
        """
        sql = f"ALTER CHANGEFEED {_quote_ident(name)} RESUME"
        return self._client.execute(sql)

    def drop_changefeed(self, name: str) -> int:
        """Drop a changefeed.

        Parameters
        ----------
        name : str
            Name of the changefeed to drop.

        Returns
        -------
        int
            Number of rows affected.
        """
        sql = f"DROP CHANGEFEED IF EXISTS {_quote_ident(name)}"
        return self._client.execute(sql)

    def list_changefeeds(self) -> List[Dict[str, Any]]:
        """List all changefeeds.

        Returns
        -------
        list of dict
            Each dict contains changefeed metadata such as ``"name"``,
            ``"table"``, ``"status"``, and ``"sink_url"``.
        """
        return self._client.query("SHOW CHANGEFEEDS")

    # ------------------------------------------------------------------
    # Geospatial
    # ------------------------------------------------------------------

    def st_distance(
        self,
        p1_lon: float,
        p1_lat: float,
        p2_lon: float,
        p2_lat: float,
    ) -> float:
        """Compute the Haversine distance between two geographic points.

        Parameters
        ----------
        p1_lon, p1_lat : float
            Longitude and latitude of the first point.
        p2_lon, p2_lat : float
            Longitude and latitude of the second point.

        Returns
        -------
        float
            Distance in meters.

        Example
        -------
        >>> db.ops.st_distance(-73.9857, 40.7484, -0.1278, 51.5074)
        5570206.4
        """
        sql = (
            f"SELECT ST_DISTANCE("
            f"ST_POINT({float(p1_lon)}, {float(p1_lat)}), "
            f"ST_POINT({float(p2_lon)}, {float(p2_lat)})) AS distance"
        )
        rows = self._client.query(sql)
        if not rows or rows[0].get("distance") is None:
            return 0.0
        return float(rows[0]["distance"])

    def st_within(
        self,
        point_lon: float,
        point_lat: float,
        polygon_wkt: str,
    ) -> bool:
        """Test whether a point falls within a polygon.

        Parameters
        ----------
        point_lon, point_lat : float
            Longitude and latitude of the point.
        polygon_wkt : str
            Polygon in WKT format (e.g.
            ``"POLYGON((-74 40, -74 41, -73 41, -73 40, -74 40))"``).

        Returns
        -------
        bool
            ``True`` if the point is within the polygon.

        Example
        -------
        >>> db.ops.st_within(-73.9857, 40.7484,
        ...     "POLYGON((-74 40, -74 41, -73 41, -73 40, -74 40))")
        True
        """
        sql = (
            f"SELECT ST_WITHIN("
            f"ST_POINT({float(point_lon)}, {float(point_lat)}), "
            f"ST_GEOMFROMTEXT('{_escape_literal(polygon_wkt)}')) AS result"
        )
        rows = self._client.query(sql)
        if not rows:
            return False
        val = rows[0].get("result")
        return val is True or val == "true" or val == "t"

    # ------------------------------------------------------------------
    # Time-series
    # ------------------------------------------------------------------

    def create_retention_policy(
        self,
        name: str,
        table: str,
        column: str,
        retention: str,
    ) -> int:
        """Create a data retention policy for a time-series table.

        Parameters
        ----------
        name : str
            Name for the retention policy.
        table : str
            Target table.
        column : str
            Timestamp column used for age-based retention.
        retention : str
            Retention period (e.g. ``"30d"``, ``"1y"``, ``"90d"``).

        Returns
        -------
        int
            Number of rows affected.

        Example
        -------
        >>> db.ops.create_retention_policy("events_90d", "events",
        ...     "created_at", "90d")
        0
        """
        sql = (
            f"CREATE RETENTION POLICY {_quote_ident(name)} ON {_quote_ident(table)} "
            f"USING ({_quote_ident(column)}) RETAIN FOR '{_escape_literal(retention)}'"
        )
        return self._client.execute(sql)

    def create_continuous_aggregate(
        self,
        name: str,
        query: str,
        interval: str,
    ) -> int:
        """Create a continuous aggregate for time-series data.

        Parameters
        ----------
        name : str
            Name for the continuous aggregate.
        query : str
            The aggregation query.
        interval : str
            Refresh interval (e.g. ``"1m"``, ``"5m"``, ``"1h"``).

        Returns
        -------
        int
            Number of rows affected.

        Example
        -------
        >>> db.ops.create_continuous_aggregate("hourly_sales",
        ...     "SELECT date_trunc('hour', ts) AS hour, SUM(amount) FROM sales GROUP BY 1",
        ...     "1h")
        0
        """
        sql = (
            f"CREATE CONTINUOUS AGGREGATE {_quote_ident(name)} "
            f"WITH (refresh_interval = '{_escape_literal(interval)}') AS {query}"
        )
        return self._client.execute(sql)

    # ------------------------------------------------------------------
    # Multi-tenant / namespaces -- NOT IMPLEMENTED SERVER-SIDE
    #
    # DEPRECATED 2026-09-03. ParticleDB has no server-side namespace surface.
    # CREATE NAMESPACE, SET NAMESPACE and DROP NAMESPACE are refused with a
    # parse error on every wire (PG, native and MySQL), and the tenancy model
    # that would give them a meaning is still an open decision -- all three
    # candidate models leave these verbs refused. The three methods below used
    # to emit that SQL and document a ``0`` return, an outcome that could not
    # occur. They now raise ParticleDBError without contacting the server, and
    # are kept only so existing code keeps importing; they will be removed in
    # the next major version.
    #
    # For tenant isolation today: a table-name prefix, a tenant column filtered
    # on every query, or a separate server per tenant (its own --data-dir and
    # ports). See https://particledb.ai/getting-started/databases-and-schemas/
    # ------------------------------------------------------------------

    def create_namespace(
        self,
        name: str,
        quotas: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Always raise -- ``CREATE NAMESPACE`` is not implemented server-side.

        .. deprecated:: 2026-09-03
            This method has never been able to succeed: no ParticleDB server
            accepts ``CREATE NAMESPACE`` on any wire. It no longer sends SQL.
            See the section note above.

        Parameters
        ----------
        name : str
            Ignored -- no request is sent.
        quotas : dict, optional
            Ignored -- no request is sent.

        Raises
        ------
        ParticleDBError
            Always.
        """
        raise ParticleDBError(_NAMESPACE_UNSUPPORTED.format(verb="CREATE NAMESPACE"))

    def set_namespace(self, name: str) -> int:
        """Always raise -- ``SET NAMESPACE`` is not implemented server-side.

        .. deprecated:: 2026-09-03
            This method has never been able to succeed: no ParticleDB server
            accepts ``SET NAMESPACE`` on any wire. It no longer sends SQL.
            See the section note above.

        Parameters
        ----------
        name : str
            Ignored -- no request is sent.

        Raises
        ------
        ParticleDBError
            Always.
        """
        raise ParticleDBError(_NAMESPACE_UNSUPPORTED.format(verb="SET NAMESPACE"))

    def drop_namespace(self, name: str) -> int:
        """Always raise -- ``DROP NAMESPACE`` is not implemented server-side.

        .. deprecated:: 2026-09-03
            This method has never been able to succeed: no ParticleDB server
            accepts ``DROP NAMESPACE`` on any wire. It no longer sends SQL.
            See the section note above.

        Parameters
        ----------
        name : str
            Ignored -- no request is sent.

        Raises
        ------
        ParticleDBError
            Always.
        """
        raise ParticleDBError(_NAMESPACE_UNSUPPORTED.format(verb="DROP NAMESPACE"))

    # ------------------------------------------------------------------
    # Lakehouse
    # ------------------------------------------------------------------

    def read_parquet(
        self, path: str, params: Optional[list] = None
    ) -> List[Dict[str, Any]]:
        """Query a Parquet file directly.

        Parameters
        ----------
        path : str
            Path to the Parquet file (local or remote URI).
        params : list, optional
            Positional parameter values for additional filtering.

        Returns
        -------
        list of dict
            Query result rows.

        Example
        -------
        >>> rows = db.ops.read_parquet("/data/events.parquet")
        """
        sql = f"SELECT * FROM read_parquet('{_escape_literal(path)}')"
        return self._client.query(sql, params)

    def read_csv(
        self, path: str, params: Optional[list] = None
    ) -> List[Dict[str, Any]]:
        """Query a CSV file directly.

        Parameters
        ----------
        path : str
            Path to the CSV file (local or remote URI).
        params : list, optional
            Positional parameter values for additional filtering.

        Returns
        -------
        list of dict
            Query result rows.

        Example
        -------
        >>> rows = db.ops.read_csv("/data/events.csv")
        """
        sql = f"SELECT * FROM read_csv('{_escape_literal(path)}')"
        return self._client.query(sql, params)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return f"Ops(client={self._client!r})"
