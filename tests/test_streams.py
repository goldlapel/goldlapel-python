"""Unit tests for goldlapel.streams.StreamsAPI — the nested gl.streams
namespace introduced alongside Phase 4 of schema-to-core.

(Streams DDL ownership shipped earlier in Phase 1+2; the namespace nesting
restructure is the new piece here.)
"""

from unittest.mock import MagicMock, patch

import pytest

from goldlapel.proxy import GoldLapel
from goldlapel.streams import StreamsAPI
from goldlapel import utils as real_utils


@pytest.fixture
def gl():
    inst = GoldLapel("postgresql://localhost:5432/mydb")
    inst._conn = MagicMock(name="internal_conn")
    inst._dashboard_token = "test-token"
    return inst


@pytest.fixture
def fake_patterns():
    return {
        "tables": {"main": "_goldlapel.stream_events"},
        "query_patterns": {
            "insert": "INSERT INTO _goldlapel.stream_events (payload) VALUES ($1) RETURNING id",
        },
    }


class TestNamespaceShape:
    def test_streams_is_a_StreamsAPI(self, gl):
        assert isinstance(gl.streams, StreamsAPI)

    def test_streams_holds_back_reference_to_parent(self, gl):
        assert gl.streams._gl is gl

    def test_no_legacy_stream_methods_on_gl(self):
        # Hard cut — the flat stream_* methods are gone.
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        for legacy in ["stream_add", "stream_create_group", "stream_read",
                       "stream_ack", "stream_claim"]:
            assert not hasattr(gl, legacy), (
                f"Legacy flat method {legacy} should have been removed; "
                f"use gl.streams.<verb> instead."
            )


class TestVerbDispatch:
    @patch("goldlapel.ddl.fetch_patterns")
    def test_add_calls_utils_stream_add(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "stream_add", return_value=1) as m:
            result = gl.streams.add("events", {"type": "click"})
            assert result == 1
            m.assert_called_once_with(
                gl._conn, "events", {"type": "click"}, patterns=fake_patterns,
            )
            # The DDL fetch went out to family=stream + version=v1.
            args, kwargs = mock_fetch.call_args
            assert args[1] == "stream"
            assert args[2] == "events"

    @patch("goldlapel.ddl.fetch_patterns")
    def test_create_group_passes_group(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "stream_create_group") as m:
            gl.streams.create_group("events", "workers")
            m.assert_called_once_with(
                gl._conn, "events", "workers", patterns=fake_patterns,
            )

    @patch("goldlapel.ddl.fetch_patterns")
    def test_read_passes_count_kwarg(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "stream_read", return_value=[]) as m:
            gl.streams.read("events", "workers", "consumer-1", count=5)
            m.assert_called_once_with(
                gl._conn, "events", "workers", "consumer-1", 5,
                patterns=fake_patterns,
            )

    @patch("goldlapel.ddl.fetch_patterns")
    def test_ack_passes_message_id(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "stream_ack", return_value=True) as m:
            gl.streams.ack("events", "workers", 42)
            m.assert_called_once_with(
                gl._conn, "events", "workers", 42, patterns=fake_patterns,
            )

    @patch("goldlapel.ddl.fetch_patterns")
    def test_claim_passes_min_idle(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "stream_claim", return_value=[]) as m:
            gl.streams.claim("events", "workers", "c2", min_idle_ms=100)
            m.assert_called_once_with(
                gl._conn, "events", "workers", "c2", 100,
                patterns=fake_patterns,
            )
