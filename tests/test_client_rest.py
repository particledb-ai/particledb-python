"""Unit tests for the ParticleDB REST client (particledb.client).

Covers two REST-SDK-drift findings:

  * BUG-004: the SDK must POST to ``/v1/sql`` (not the removed ``/query``),
    support ``https://`` / ``tls=True``, and not send Basic auth over cleartext.
  * BUG-005: the SDK must decode the object-row ``/v1/sql`` response
    ``{"rows": [{"?column?": 1}], "columns": ["?column?"]}`` into
    ``[{"?column?": 1}]`` (values preserved), not
    ``[{"?column?": "?column?"}]`` (values replaced by column names).

All tests use unittest.mock — no real server is required.

Run::

    cd particledb-python
    python -m pytest tests/test_client_rest.py -v
"""

from __future__ import annotations

import io
import json
import unittest
from unittest.mock import patch

from particledb.client import ParticleDB, ParticleDBError


class _FakeResponse:
    """Minimal context-manager stand-in for urlopen()'s return value."""

    def __init__(self, body: dict):
        self._data = json.dumps(body).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self) -> bytes:
        return self._data


class _Capture:
    """Captures the urllib Request passed to urlopen and returns a canned body."""

    def __init__(self, body: dict):
        self.body = body
        self.request = None

    def __call__(self, req, timeout=None):
        self.request = req
        return _FakeResponse(self.body)


# The EXACT shape the live server emits for `SELECT 1` (server.rs builds each
# row as a JSON object keyed by column name).
LIVE_V1_SQL_BODY = {
    "rows": [{"?column?": 1}],
    "row_count": 1,
    "columns": ["?column?"],
}


class TestRoute(unittest.TestCase):
    def test_query_posts_to_v1_sql(self) -> None:
        cap = _Capture(LIVE_V1_SQL_BODY)
        db = ParticleDB(host="localhost", port=8080, transport="http")
        with patch("urllib.request.urlopen", cap):
            db.query("SELECT 1")
        self.assertIsNotNone(cap.request)
        self.assertEqual(cap.request.full_url, "http://localhost:8080/v1/sql")
        self.assertEqual(cap.request.method, "POST")
        sent = json.loads(cap.request.data.decode("utf-8"))
        self.assertEqual(sent["sql"], "SELECT 1")


class TestDecode(unittest.TestCase):
    def test_object_rows_preserve_values(self) -> None:
        """BUG-005: object-row response decodes with VALUES intact."""
        cap = _Capture(LIVE_V1_SQL_BODY)
        db = ParticleDB(host="localhost", port=8080, transport="http")
        with patch("urllib.request.urlopen", cap):
            rows = db.query("SELECT 1")
        self.assertEqual(rows, [{"?column?": 1}])
        # The pre-fix bug produced the column NAME as the value:
        self.assertNotEqual(rows, [{"?column?": "?column?"}])

    def test_multi_column_object_rows(self) -> None:
        body = {
            "rows": [
                {"id": 1, "name": "alice"},
                {"id": 2, "name": "bob"},
            ],
            "row_count": 2,
            "columns": ["id", "name"],
        }
        cap = _Capture(body)
        db = ParticleDB(host="localhost", port=8080, transport="http")
        with patch("urllib.request.urlopen", cap):
            rows = db.query("SELECT id, name FROM t")
        self.assertEqual(rows, [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}])

    def test_query_result_object_rows(self) -> None:
        cap = _Capture(LIVE_V1_SQL_BODY)
        db = ParticleDB(host="localhost", port=8080, transport="http")
        with patch("urllib.request.urlopen", cap):
            qr = db.query_result("SELECT 1")
        self.assertEqual(qr.rows, [{"?column?": 1}])
        self.assertEqual(qr.columns, ["?column?"])
        self.assertEqual(qr.row_count, 1)

    def test_legacy_list_rows_still_decode(self) -> None:
        """Backward-compat: a legacy list-of-lists row still zips to columns."""
        body = {"rows": [[1, "alice"]], "row_count": 1, "columns": ["id", "name"]}
        cap = _Capture(body)
        db = ParticleDB(host="localhost", port=8080, transport="http")
        with patch("urllib.request.urlopen", cap):
            rows = db.query("SELECT id, name FROM t")
        self.assertEqual(rows, [{"id": 1, "name": "alice"}])


class TestTls(unittest.TestCase):
    def test_https_host_uses_tls(self) -> None:
        db = ParticleDB(host="https://db.example.com", port=8080, transport="http")
        self.assertTrue(db.tls)
        self.assertEqual(db.host, "db.example.com")
        self.assertEqual(db._base_url, "https://db.example.com:8080")

    def test_https_host_with_port_in_url(self) -> None:
        db = ParticleDB(host="https://db.example.com:9443", transport="http")
        self.assertTrue(db.tls)
        self.assertEqual(db.host, "db.example.com")
        self.assertEqual(db.port, 9443)
        self.assertEqual(db._base_url, "https://db.example.com:9443")

    def test_explicit_tls_flag(self) -> None:
        db = ParticleDB(host="db.example.com", port=8080, tls=True, transport="http")
        self.assertEqual(db._base_url, "https://db.example.com:8080")

    def test_default_is_http(self) -> None:
        db = ParticleDB(host="localhost", port=8080, transport="http")
        self.assertFalse(db.tls)
        self.assertEqual(db._base_url, "http://localhost:8080")

    def test_https_request_url(self) -> None:
        cap = _Capture(LIVE_V1_SQL_BODY)
        db = ParticleDB(host="https://db.example.com", port=8443, transport="http")
        with patch("urllib.request.urlopen", cap):
            db.query("SELECT 1")
        self.assertEqual(cap.request.full_url, "https://db.example.com:8443/v1/sql")


class TestAuth(unittest.TestCase):
    def test_no_basic_auth_over_cleartext(self) -> None:
        """BUG-004: Basic auth credentials must NOT be sent over cleartext HTTP."""
        cap = _Capture(LIVE_V1_SQL_BODY)
        db = ParticleDB(host="localhost", port=8080, user="u", password="p", transport="http")
        with patch("urllib.request.urlopen", cap):
            with self.assertRaises(ParticleDBError):
                db.query("SELECT 1")

    def test_basic_auth_sent_over_tls(self) -> None:
        cap = _Capture(LIVE_V1_SQL_BODY)
        db = ParticleDB(host="https://db.example.com", port=8443, user="u", password="p", transport="http")
        with patch("urllib.request.urlopen", cap):
            db.query("SELECT 1")
        auth = cap.request.get_header("Authorization")
        self.assertIsNotNone(auth)
        self.assertTrue(auth.startswith("Basic "))

    def test_insecure_auth_escape_hatch(self) -> None:
        cap = _Capture(LIVE_V1_SQL_BODY)
        db = ParticleDB(
            host="localhost", port=8080, user="u", password="p",
            allow_insecure_auth=True, transport="http",
        )
        with patch("urllib.request.urlopen", cap):
            db.query("SELECT 1")
        self.assertTrue(cap.request.get_header("Authorization").startswith("Basic "))

    def test_no_credentials_no_auth_header(self) -> None:
        cap = _Capture(LIVE_V1_SQL_BODY)
        db = ParticleDB(host="localhost", port=8080, transport="http")
        with patch("urllib.request.urlopen", cap):
            db.query("SELECT 1")
        self.assertIsNone(cap.request.get_header("Authorization"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
