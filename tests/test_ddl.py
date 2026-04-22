"""Unit tests for goldlapel.ddl — the DDL API client + per-session cache."""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import patch

import pytest

from goldlapel import ddl


# ----------------------------------------------------------------------------
# Test HTTP server that captures requests and returns canned responses.
# ----------------------------------------------------------------------------

class _FakeHandler(BaseHTTPRequestHandler):
    # Per-test injection slots:
    responses = []  # list[(status_code, json_body_dict)]
    captured = []   # list[(path, headers_dict, body_dict)]

    def log_message(self, format, *args):  # silence test stderr spam
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b""
        try:
            body = json.loads(raw.decode("utf-8")) if raw else {}
        except json.JSONDecodeError:
            body = {"_raw": raw.decode("utf-8", errors="replace")}
        # email.message.Message is case-insensitive internally but .items()
        # preserves original case of the incoming header name. For test
        # assertions we want predictable key lookup — lowercase everything.
        headers = {k.lower(): v for k, v in self.headers.items()}
        _FakeHandler.captured.append((self.path, headers, body))

        if _FakeHandler.responses:
            status, resp = _FakeHandler.responses.pop(0)
        else:
            status, resp = 500, {"error": "no_response_queued"}
        payload = json.dumps(resp).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class _FakeServer:
    """Starts a BaseHTTPRequestHandler on 127.0.0.1:<port> in a bg thread."""

    def __init__(self):
        self.server = HTTPServer(("127.0.0.1", 0), _FakeHandler)
        self.port = self.server.server_port
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def stop(self):
        self.server.shutdown()
        self.thread.join(timeout=2)


@pytest.fixture
def fake_server():
    _FakeHandler.responses = []
    _FakeHandler.captured = []
    srv = _FakeServer()
    try:
        yield srv
    finally:
        srv.stop()


class FakeOwner:
    """Stand-in for a GoldLapel / AsyncGoldLapel instance — supports weakrefs."""
    pass


# ----------------------------------------------------------------------------
# token_from_env_or_file
# ----------------------------------------------------------------------------

class TestTokenResolution:
    def test_env_wins_over_file(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GOLDLAPEL_DASHBOARD_TOKEN", "env-token")
        # Write a file that should lose
        d = tmp_path / ".goldlapel"
        d.mkdir()
        (d / "dashboard-token").write_text("file-token\n")
        monkeypatch.setenv("HOME", str(tmp_path))
        assert ddl.token_from_env_or_file() == "env-token"

    def test_file_when_no_env(self, tmp_path, monkeypatch):
        monkeypatch.delenv("GOLDLAPEL_DASHBOARD_TOKEN", raising=False)
        d = tmp_path / ".goldlapel"
        d.mkdir()
        (d / "dashboard-token").write_text("  file-token  \n")
        monkeypatch.setenv("HOME", str(tmp_path))
        assert ddl.token_from_env_or_file() == "file-token"

    def test_none_when_nothing_set(self, tmp_path, monkeypatch):
        monkeypatch.delenv("GOLDLAPEL_DASHBOARD_TOKEN", raising=False)
        monkeypatch.setenv("HOME", str(tmp_path))
        assert ddl.token_from_env_or_file() is None

    def test_empty_env_ignored(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GOLDLAPEL_DASHBOARD_TOKEN", "")
        monkeypatch.setenv("HOME", str(tmp_path))
        assert ddl.token_from_env_or_file() is None


# ----------------------------------------------------------------------------
# supported_version
# ----------------------------------------------------------------------------

def test_supported_version_stream_is_v1():
    assert ddl.supported_version("stream") == "v1"


# ----------------------------------------------------------------------------
# fetch — HTTP round-trip
# ----------------------------------------------------------------------------

class TestFetch:
    def test_happy_path_hits_endpoint(self, fake_server):
        _FakeHandler.responses = [(
            200,
            {
                "accepted": True,
                "family": "stream",
                "schema_version": "v1",
                "tables": {"main": "_goldlapel.stream_events"},
                "query_patterns": {"insert": "INSERT ..."},
            },
        )]
        owner = FakeOwner()
        result = ddl.fetch(owner, "stream", "events", fake_server.port, "tok")
        assert result["tables"]["main"] == "_goldlapel.stream_events"
        assert result["query_patterns"]["insert"] == "INSERT ..."

        assert len(_FakeHandler.captured) == 1
        path, headers, body = _FakeHandler.captured[0]
        assert path == "/api/ddl/stream/create"
        assert headers.get("x-gl-dashboard") == "tok"
        assert body == {"name": "events", "schema_version": "v1"}

    def test_cache_hit_does_not_re_request(self, fake_server):
        _FakeHandler.responses = [(
            200,
            {
                "accepted": True,
                "family": "stream",
                "schema_version": "v1",
                "tables": {"main": "_goldlapel.stream_events"},
                "query_patterns": {"insert": "X"},
            },
        )]
        owner = FakeOwner()
        r1 = ddl.fetch(owner, "stream", "events", fake_server.port, "tok")
        r2 = ddl.fetch(owner, "stream", "events", fake_server.port, "tok")
        assert r1 is r2
        # Only one HTTP request — second call served from cache.
        assert len(_FakeHandler.captured) == 1

    def test_different_names_miss_cache(self, fake_server):
        for name in ("events", "orders"):
            _FakeHandler.responses.append((
                200,
                {
                    "accepted": True,
                    "family": "stream",
                    "schema_version": "v1",
                    "tables": {"main": f"_goldlapel.stream_{name}"},
                    "query_patterns": {"insert": f"INSERT {name}"},
                },
            ))
        owner = FakeOwner()
        ddl.fetch(owner, "stream", "events", fake_server.port, "tok")
        ddl.fetch(owner, "stream", "orders", fake_server.port, "tok")
        assert len(_FakeHandler.captured) == 2

    def test_different_owners_have_isolated_caches(self, fake_server):
        for _ in range(2):
            _FakeHandler.responses.append((
                200,
                {
                    "tables": {"main": "_goldlapel.stream_events"},
                    "query_patterns": {"insert": "X"},
                },
            ))
        a = FakeOwner()
        b = FakeOwner()
        ddl.fetch(a, "stream", "events", fake_server.port, "tok")
        ddl.fetch(b, "stream", "events", fake_server.port, "tok")
        assert len(_FakeHandler.captured) == 2

    def test_version_mismatch_raises_actionable_error(self, fake_server):
        _FakeHandler.responses = [(
            409,
            {
                "error": "version_mismatch",
                "detail": "wrapper requested v1; proxy speaks v2 — upgrade proxy",
                "requested": "v1",
                "canonical": "v2",
            },
        )]
        owner = FakeOwner()
        with pytest.raises(RuntimeError, match="schema version mismatch"):
            ddl.fetch(owner, "stream", "events", fake_server.port, "tok")

    def test_403_raises_token_error(self, fake_server):
        _FakeHandler.responses = [(403, {"error": "forbidden"})]
        owner = FakeOwner()
        with pytest.raises(RuntimeError, match="dashboard token"):
            ddl.fetch(owner, "stream", "events", fake_server.port, "tok")

    def test_missing_token_raises_before_http(self):
        owner = FakeOwner()
        with pytest.raises(RuntimeError, match="No dashboard token"):
            ddl.fetch(owner, "stream", "events", 9999, None)

    def test_missing_port_raises_before_http(self):
        owner = FakeOwner()
        with pytest.raises(RuntimeError, match="No dashboard port"):
            ddl.fetch(owner, "stream", "events", None, "tok")

    def test_server_unreachable_raises_actionable_error(self):
        owner = FakeOwner()
        # Bind nothing — port 1 is guaranteed-closed for this process.
        with pytest.raises(RuntimeError, match="Gold Lapel dashboard not reachable"):
            ddl.fetch(owner, "stream", "events", 1, "tok")

    def test_invalidate_drops_cache(self, fake_server):
        for _ in range(2):
            _FakeHandler.responses.append((
                200,
                {
                    "tables": {"main": "_goldlapel.stream_events"},
                    "query_patterns": {"insert": "X"},
                },
            ))
        owner = FakeOwner()
        ddl.fetch(owner, "stream", "events", fake_server.port, "tok")
        ddl.invalidate(owner)
        ddl.fetch(owner, "stream", "events", fake_server.port, "tok")
        # Two HTTP calls — cache was cleared between them.
        assert len(_FakeHandler.captured) == 2


# ----------------------------------------------------------------------------
# to_psycopg — placeholder translation
# ----------------------------------------------------------------------------

class TestToPsycopg:
    def test_substitutes_dollar_placeholders(self):
        sql = "INSERT INTO x (a,b) VALUES ($1,$2) RETURNING $3"
        assert ddl.to_psycopg(sql) == "INSERT INTO x (a,b) VALUES (%s,%s) RETURNING %s"

    def test_leaves_literal_dollar_untouched(self):
        # No digit after $ → not a placeholder; leave alone.
        sql = "SELECT 'foo$bar' FROM t"
        assert ddl.to_psycopg(sql) == "SELECT 'foo$bar' FROM t"

    def test_handles_double_digit_placeholders(self):
        sql = "SELECT $1, $2, $10, $11 FROM t"
        # $10 and $11 should both become single %s; no mis-split as $1 + '0'.
        assert ddl.to_psycopg(sql) == "SELECT %s, %s, %s, %s FROM t"
