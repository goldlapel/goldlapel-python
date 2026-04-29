"""Unit tests for goldlapel.counters.CountersAPI — the nested gl.counters
namespace introduced in Phase 5 of schema-to-core (counter / zset / hash /
queue / geo).

Tests cover:
  - gl.counters is a CountersAPI bound to the parent client.
  - Each verb fetches DDL patterns from the proxy then dispatches to the
    `goldlapel.utils.counter_*` helper with the right args.
  - The pattern cache is shared with the parent client (one HTTP call per
    (family, name) per session).
  - SQL builders use the proxy's canonical query patterns (no in-wrapper
    CREATE TABLE leaks).
  - Phase-5 counter `updated_at` parity: the canonical patterns reference
    `NOW()` on every UPDATE — wrappers don't paper over this.
"""

from unittest.mock import MagicMock, patch

import pytest

from goldlapel.proxy import GoldLapel
from goldlapel.counters import CountersAPI
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
        "tables": {"main": "_goldlapel.counter_pageviews"},
        "query_patterns": {
            "incr": "INSERT INTO _goldlapel.counter_pageviews (key, value, updated_at) VALUES ($1, $2, NOW()) ON CONFLICT (key) DO UPDATE SET value = _goldlapel.counter_pageviews.value + EXCLUDED.value, updated_at = NOW() RETURNING value",
            "set": "INSERT INTO _goldlapel.counter_pageviews (key, value, updated_at) VALUES ($1, $2, NOW()) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW() RETURNING value",
            "get": "SELECT value FROM _goldlapel.counter_pageviews WHERE key = $1",
            "delete": "DELETE FROM _goldlapel.counter_pageviews WHERE key = $1",
            "delete_all": "DELETE FROM _goldlapel.counter_pageviews",
            "count_keys": "SELECT COUNT(*) FROM _goldlapel.counter_pageviews",
        },
    }


class TestNamespaceShape:
    def test_counters_is_a_CountersAPI(self, gl):
        assert isinstance(gl.counters, CountersAPI)

    def test_counters_holds_back_reference_to_parent(self, gl):
        assert gl.counters._gl is gl

    def test_no_legacy_flat_methods(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        for legacy in ["incr", "get_counter"]:
            assert not hasattr(gl, legacy), (
                f"Phase 5 removed flat {legacy} — use gl.counters.<verb>."
            )


class TestVerbDispatch:
    @patch("goldlapel.ddl.fetch_patterns")
    def test_incr_calls_utils_counter_incr(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "counter_incr", return_value=42) as m:
            result = gl.counters.incr("pageviews", "home")
            assert result == 42
            m.assert_called_once_with(
                gl._conn, "pageviews", "home", 1, patterns=fake_patterns,
            )
            args, _ = mock_fetch.call_args
            assert args[1] == "counter"
            assert args[2] == "pageviews"

    @patch("goldlapel.ddl.fetch_patterns")
    def test_decr_negates_amount(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "counter_decr", return_value=-5) as m:
            gl.counters.decr("pageviews", "home", 3)
            m.assert_called_once_with(
                gl._conn, "pageviews", "home", 3, patterns=fake_patterns,
            )

    @patch("goldlapel.ddl.fetch_patterns")
    def test_set_passes_value(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "counter_set", return_value=100) as m:
            gl.counters.set("pageviews", "home", 100)
            m.assert_called_once_with(
                gl._conn, "pageviews", "home", 100, patterns=fake_patterns,
            )

    @patch("goldlapel.ddl.fetch_patterns")
    def test_get_passes_key(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "counter_get", return_value=42) as m:
            assert gl.counters.get("pageviews", "home") == 42
            m.assert_called_once_with(
                gl._conn, "pageviews", "home", patterns=fake_patterns,
            )

    @patch("goldlapel.ddl.fetch_patterns")
    def test_delete_returns_bool(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "counter_delete", return_value=True) as m:
            assert gl.counters.delete("pageviews", "home") is True
            m.assert_called_once_with(
                gl._conn, "pageviews", "home", patterns=fake_patterns,
            )

    @patch("goldlapel.ddl.fetch_patterns")
    def test_count_keys_no_args_after_name(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "counter_count_keys", return_value=5) as m:
            gl.counters.count_keys("pageviews")
            m.assert_called_once_with(
                gl._conn, "pageviews", patterns=fake_patterns,
            )

    @patch("goldlapel.ddl.fetch_patterns")
    def test_create_just_fetches_patterns(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        gl.counters.create("pageviews")
        mock_fetch.assert_called_once()
        args, _ = mock_fetch.call_args
        assert args[1] == "counter"


class _FakeConn:
    """Stand-in for a raw psycopg connection. Plain class (not a MagicMock)
    so `_get_raw_connection` doesn't recurse via the auto-`_conn` attribute
    that bare MagicMocks expose."""
    def __init__(self, cursor):
        self._cursor = cursor
        self.commit = MagicMock()

    def cursor(self):
        return self._cursor


def _make_cursor(*, fetchone=None, fetchall=None, rowcount=0, description=None):
    cur = MagicMock()
    cur.fetchone.return_value = fetchone
    cur.fetchall.return_value = fetchall or []
    cur.rowcount = rowcount
    if description is not None:
        cur.description = description
    return cur


class TestSqlBuilders:
    """Phase 5 contract: the wrapper executes the proxy's canonical patterns
    verbatim (after `$N → %s` translation). These tests exercise the actual
    SQL submitted to the cursor — the load-bearing piece is the binding
    order, since psycopg evaluates `%s` positionally.
    """

    def test_incr_translates_dollar_to_percent_s_and_returns_value(self, fake_patterns):
        cur = _make_cursor(fetchone=(7,))
        raw = _FakeConn(cur)
        result = real_utils.counter_incr(raw, "pageviews", "home", 5, patterns=fake_patterns)
        assert result == 7
        sql = cur.execute.call_args[0][0]
        assert "$" not in sql
        assert sql.count("%s") == 2  # ($1, $2) → two %s
        assert cur.execute.call_args[0][1] == ("home", 5)

    def test_get_returns_zero_for_unknown_key(self, fake_patterns):
        cur = _make_cursor(fetchone=None)
        raw = _FakeConn(cur)
        result = real_utils.counter_get(raw, "pageviews", "missing", patterns=fake_patterns)
        assert result == 0

    def test_decr_passes_negative_amount(self, fake_patterns):
        cur = _make_cursor(fetchone=(-2,))
        raw = _FakeConn(cur)
        result = real_utils.counter_decr(raw, "pageviews", "home", 3, patterns=fake_patterns)
        assert result == -2
        assert cur.execute.call_args[0][1] == ("home", -3)

    def test_missing_patterns_raises(self):
        with pytest.raises(RuntimeError, match="counter utils require"):
            real_utils.counter_incr(_FakeConn(MagicMock()), "x", "y", patterns=None)

    def test_phase5_incr_pattern_stamps_updated_at(self, fake_patterns):
        assert "updated_at = NOW()" in fake_patterns["query_patterns"]["incr"]
