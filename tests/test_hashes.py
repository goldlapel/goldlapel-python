"""Unit tests for goldlapel.hashes.HashesAPI.

Phase 5 flipped the hash storage shape from "JSONB blob per key" to
"row per (hash_key, field)". These tests verify:
  - The wrapper executes single-row UPSERT for `set`, NOT load-merge-save.
  - `get_all` aggregates rows from the proxy into a Python dict.
  - `keys` / `values` return per-row sequences (not blob extraction).
  - `delete` returns True/False from rowcount, not from JSONB key probe.
"""

from unittest.mock import MagicMock, patch

import pytest

from goldlapel.proxy import GoldLapel
from goldlapel.hashes import HashesAPI
from goldlapel import utils as real_utils


@pytest.fixture
def gl():
    inst = GoldLapel("postgresql://localhost:5432/mydb")
    inst._conn = MagicMock(name="internal_conn")
    inst._dashboard_token = "test-token"
    return inst


@pytest.fixture
def fake_patterns():
    main = "_goldlapel.hash_sessions"
    return {
        "tables": {"main": main},
        "query_patterns": {
            "hset": f"INSERT INTO {main} (hash_key, field, value) VALUES ($1, $2, $3::jsonb) ON CONFLICT (hash_key, field) DO UPDATE SET value = EXCLUDED.value RETURNING value",
            "hget": f"SELECT value FROM {main} WHERE hash_key = $1 AND field = $2",
            "hgetall": f"SELECT field, value FROM {main} WHERE hash_key = $1 ORDER BY field",
            "hkeys": f"SELECT field FROM {main} WHERE hash_key = $1 ORDER BY field",
            "hvals": f"SELECT value FROM {main} WHERE hash_key = $1 ORDER BY field",
            "hexists": f"SELECT EXISTS (SELECT 1 FROM {main} WHERE hash_key = $1 AND field = $2)",
            "hdel": f"DELETE FROM {main} WHERE hash_key = $1 AND field = $2",
            "hlen": f"SELECT COUNT(*) FROM {main} WHERE hash_key = $1",
            "delete_key": f"DELETE FROM {main} WHERE hash_key = $1",
            "delete_all": f"DELETE FROM {main}",
        },
    }


class TestNamespaceShape:
    def test_hashes_is_a_HashesAPI(self, gl):
        assert isinstance(gl.hashes, HashesAPI)

    def test_no_legacy_flat_methods(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        for legacy in ["hset", "hget", "hgetall", "hdel"]:
            assert not hasattr(gl, legacy)


class TestVerbDispatch:
    @patch("goldlapel.ddl.fetch_patterns")
    def test_set_dispatches_to_hash_set(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "hash_set", return_value="alice") as m:
            gl.hashes.set("sessions", "user:1", "name", "alice")
            m.assert_called_once_with(
                gl._conn, "sessions", "user:1", "name", "alice",
                patterns=fake_patterns,
            )

    @patch("goldlapel.ddl.fetch_patterns")
    def test_get_all_aggregates_rows(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "hash_get_all",
                          return_value={"name": "alice", "email": "a@x"}) as m:
            result = gl.hashes.get_all("sessions", "user:1")
            assert result == {"name": "alice", "email": "a@x"}
            m.assert_called_once_with(
                gl._conn, "sessions", "user:1", patterns=fake_patterns,
            )


class _FakeConn:
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
    def test_hash_set_is_single_row_upsert_not_load_merge(self, fake_patterns):
        # Phase-5 contract: hash_set runs the proxy's INSERT/UPSERT directly.
        # No SELECT-then-merge-then-update sequence (the legacy JSONB-blob path).
        cur = _cursor(fetchone=("alice",))
        raw = _FakeConn(cur)
        real_utils.hash_set(raw, "sessions", "user:1", "name", "alice", patterns=fake_patterns)
        assert cur.execute.call_count == 1
        sql = cur.execute.call_args[0][0]
        assert "INSERT INTO" in sql
        assert "ON CONFLICT (hash_key, field)" in sql

    def test_hash_set_json_encodes_value(self, fake_patterns):
        cur = _cursor(fetchone=({"a": 1},))
        raw = _FakeConn(cur)
        real_utils.hash_set(raw, "sessions", "user:1", "data", {"a": 1}, patterns=fake_patterns)
        params = cur.execute.call_args[0][1]
        assert params[0] == "user:1"
        assert params[1] == "data"
        assert params[2] == '{"a": 1}'

    def test_hash_get_all_rebuilds_dict_from_rows(self, fake_patterns):
        cur = _cursor(fetchall=[
            ("email", "a@x"),
            ("name", "alice"),
        ])
        raw = _FakeConn(cur)
        result = real_utils.hash_get_all(raw, "sessions", "user:1", patterns=fake_patterns)
        assert result == {"email": "a@x", "name": "alice"}

    def test_hash_get_all_decodes_string_jsonb_payload(self, fake_patterns):
        cur = _cursor(fetchall=[("data", '{"k": 1}')])
        raw = _FakeConn(cur)
        result = real_utils.hash_get_all(raw, "sessions", "user:1", patterns=fake_patterns)
        assert result == {"data": {"k": 1}}

    def test_hash_get_returns_none_for_absent_field(self, fake_patterns):
        cur = _cursor(fetchone=None)
        raw = _FakeConn(cur)
        assert real_utils.hash_get(raw, "sessions", "user:1", "missing", patterns=fake_patterns) is None

    def test_hash_keys_returns_field_list(self, fake_patterns):
        cur = _cursor(fetchall=[("name",), ("email",)])
        raw = _FakeConn(cur)
        assert real_utils.hash_keys(raw, "sessions", "user:1", patterns=fake_patterns) == ["name", "email"]

    def test_hash_exists_returns_bool(self, fake_patterns):
        cur = _cursor(fetchone=(True,))
        raw = _FakeConn(cur)
        assert real_utils.hash_exists(raw, "sessions", "user:1", "name", patterns=fake_patterns) is True

    def test_hash_delete_returns_true_when_removed(self, fake_patterns):
        cur = _cursor(rowcount=1)
        raw = _FakeConn(cur)
        assert real_utils.hash_delete(raw, "sessions", "user:1", "name", patterns=fake_patterns) is True

    def test_hash_len_count_query(self, fake_patterns):
        cur = _cursor(fetchone=(3,))
        raw = _FakeConn(cur)
        assert real_utils.hash_len(raw, "sessions", "user:1", patterns=fake_patterns) == 3

    def test_canonical_pattern_is_row_per_field_not_blob(self, fake_patterns):
        sql = fake_patterns["query_patterns"]["hset"]
        assert "(hash_key, field, value)" in sql
        assert "jsonb_build_object" not in sql
