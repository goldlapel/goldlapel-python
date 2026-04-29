"""Unit tests for goldlapel.zsets.ZsetsAPI.

Phase 5 introduced a `zset_key` column in the canonical schema so a single
namespace table holds many sorted sets. These tests verify:
  - `zset_key` threads through every method as the first positional arg
    after the namespace `name` (matching Redis ZADD signatures).
  - Pattern selection picks `zrange_asc` vs `zrange_desc` based on the
    `desc` kwarg.
  - Range/limit translation is Redis-inclusive (start..stop inclusive).
  - SQL builders bind in `(zset_key, member, score)` order matching the
    proxy's `$1, $2, $3` template.
"""

from unittest.mock import MagicMock, patch

import pytest

from goldlapel.proxy import GoldLapel
from goldlapel.zsets import ZsetsAPI
from goldlapel import utils as real_utils


@pytest.fixture
def gl():
    inst = GoldLapel("postgresql://localhost:5432/mydb")
    inst._conn = MagicMock(name="internal_conn")
    inst._dashboard_token = "test-token"
    return inst


@pytest.fixture
def fake_patterns():
    main = "_goldlapel.zset_leaderboard"
    return {
        "tables": {"main": main},
        "query_patterns": {
            "zadd": f"INSERT INTO {main} (zset_key, member, score) VALUES ($1, $2, $3) ON CONFLICT (zset_key, member) DO UPDATE SET score = EXCLUDED.score RETURNING score",
            "zincrby": f"INSERT INTO {main} (zset_key, member, score) VALUES ($1, $2, $3) ON CONFLICT (zset_key, member) DO UPDATE SET score = {main}.score + EXCLUDED.score RETURNING score",
            "zscore": f"SELECT score FROM {main} WHERE zset_key = $1 AND member = $2",
            "zrem": f"DELETE FROM {main} WHERE zset_key = $1 AND member = $2",
            "zrange_asc": f"SELECT member, score FROM {main} WHERE zset_key = $1 ORDER BY score ASC, member ASC LIMIT $2 OFFSET $3",
            "zrange_desc": f"SELECT member, score FROM {main} WHERE zset_key = $1 ORDER BY score DESC, member DESC LIMIT $2 OFFSET $3",
            "zrangebyscore": f"SELECT member, score FROM {main} WHERE zset_key = $1 AND score >= $2 AND score <= $3 ORDER BY score ASC, member ASC LIMIT $4 OFFSET $5",
            "zrank_asc": f"SELECT rank FROM ( SELECT member, ROW_NUMBER() OVER (ORDER BY score ASC, member ASC) - 1 AS rank FROM {main} WHERE zset_key = $1 ) ranked WHERE member = $2",
            "zrank_desc": f"SELECT rank FROM ( SELECT member, ROW_NUMBER() OVER (ORDER BY score DESC, member DESC) - 1 AS rank FROM {main} WHERE zset_key = $1 ) ranked WHERE member = $2",
            "zcard": f"SELECT COUNT(*) FROM {main} WHERE zset_key = $1",
            "delete_key": f"DELETE FROM {main} WHERE zset_key = $1",
            "delete_all": f"DELETE FROM {main}",
        },
    }


class TestNamespaceShape:
    def test_zsets_is_a_ZsetsAPI(self, gl):
        assert isinstance(gl.zsets, ZsetsAPI)

    def test_no_legacy_flat_methods(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        for legacy in ["zadd", "zincrby", "zrange", "zrank", "zscore", "zrem"]:
            assert not hasattr(gl, legacy)


class TestVerbDispatch:
    @patch("goldlapel.ddl.fetch_patterns")
    def test_add_threads_zset_key_first(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "zset_add", return_value=100.0) as m:
            gl.zsets.add("leaderboard", "global", "alice", 100)
            m.assert_called_once_with(
                gl._conn, "leaderboard", "global", "alice", 100,
                patterns=fake_patterns,
            )

    @patch("goldlapel.ddl.fetch_patterns")
    def test_incr_by_passes_delta(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "zset_incr_by", return_value=110.0) as m:
            gl.zsets.incr_by("leaderboard", "global", "alice", 10)
            m.assert_called_once_with(
                gl._conn, "leaderboard", "global", "alice", 10,
                patterns=fake_patterns,
            )

    @patch("goldlapel.ddl.fetch_patterns")
    def test_score_returns_value(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "zset_score", return_value=100.0) as m:
            gl.zsets.score("leaderboard", "global", "alice")
            m.assert_called_once_with(
                gl._conn, "leaderboard", "global", "alice",
                patterns=fake_patterns,
            )

    @patch("goldlapel.ddl.fetch_patterns")
    def test_rank_passes_desc_kwarg(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "zset_rank", return_value=0) as m:
            gl.zsets.rank("leaderboard", "global", "alice", desc=False)
            m.assert_called_once_with(
                gl._conn, "leaderboard", "global", "alice", desc=False,
                patterns=fake_patterns,
            )


class _FakeConn:
    """Plain stand-in for a raw psycopg connection — bypasses
    `_get_raw_connection`'s `_conn` autounwrap that bare MagicMocks trigger."""
    def __init__(self, cursor):
        self._cursor = cursor
        self.commit = MagicMock()

    def cursor(self):
        return self._cursor


def _cursor(*, fetchone=None, fetchall=None, rowcount=0):
    cur = MagicMock()
    cur.fetchone.return_value = fetchone
    cur.fetchall.return_value = fetchall or []
    cur.rowcount = rowcount
    return cur


class TestSqlBuilders:
    def test_zset_add_binds_in_zset_key_member_score_order(self, fake_patterns):
        cur = _cursor(fetchone=(100.0,))
        raw = _FakeConn(cur)
        result = real_utils.zset_add(raw, "leaderboard", "global", "alice", 100, patterns=fake_patterns)
        assert result == 100.0
        assert cur.execute.call_args[0][1] == ("global", "alice", 100.0)

    def test_zset_range_picks_desc_pattern(self, fake_patterns):
        cur = _cursor(fetchall=[("alice", 100.0), ("bob", 90.0)])
        raw = _FakeConn(cur)
        result = real_utils.zset_range(raw, "leaderboard", "global", 0, 1, desc=True, patterns=fake_patterns)
        assert result == [("alice", 100.0), ("bob", 90.0)]
        sql = cur.execute.call_args[0][0]
        assert "DESC" in sql

    def test_zset_range_picks_asc_pattern(self, fake_patterns):
        cur = _cursor(fetchall=[])
        raw = _FakeConn(cur)
        real_utils.zset_range(raw, "leaderboard", "global", 0, 5, desc=False, patterns=fake_patterns)
        sql = cur.execute.call_args[0][0]
        assert "ORDER BY score ASC" in sql

    def test_zset_range_translates_inclusive_stop_to_limit(self, fake_patterns):
        cur = _cursor(fetchall=[])
        raw = _FakeConn(cur)
        real_utils.zset_range(raw, "leaderboard", "global", 0, 9, patterns=fake_patterns)
        params = cur.execute.call_args[0][1]
        assert params == ("global", 10, 0)

    def test_zset_range_by_score_inclusive_bounds(self, fake_patterns):
        cur = _cursor(fetchall=[])
        raw = _FakeConn(cur)
        real_utils.zset_range_by_score(
            raw, "leaderboard", "global", 50, 200, limit=10, offset=2, patterns=fake_patterns,
        )
        params = cur.execute.call_args[0][1]
        assert params == ("global", 50.0, 200.0, 10, 2)

    def test_zset_remove_returns_true_on_rowcount_one(self, fake_patterns):
        cur = _cursor(rowcount=1)
        raw = _FakeConn(cur)
        assert real_utils.zset_remove(raw, "leaderboard", "global", "alice", patterns=fake_patterns) is True

    def test_zset_remove_returns_false_when_absent(self, fake_patterns):
        cur = _cursor(rowcount=0)
        raw = _FakeConn(cur)
        assert real_utils.zset_remove(raw, "leaderboard", "global", "alice", patterns=fake_patterns) is False

    def test_zset_card_returns_zero_for_unknown_key(self, fake_patterns):
        cur = _cursor(fetchone=None)
        raw = _FakeConn(cur)
        assert real_utils.zset_card(raw, "leaderboard", "missing", patterns=fake_patterns) == 0
