from unittest.mock import MagicMock, call
import json

import pytest

from goldlapel.utils import (
    doc_insert,
    doc_insert_many,
    doc_find,
    doc_find_one,
    doc_update,
    doc_update_one,
    doc_delete,
    doc_delete_one,
    doc_count,
    doc_create_index,
)


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commit = MagicMock()

    def cursor(self):
        return self._cursor


def capture_sql(fetchall_result=None, fetchone_result=None, description=None, rowcount=0):
    cursor = MagicMock()
    cursor.description = description or [("_id",), ("data",), ("created_at",)]
    cursor.fetchall.return_value = fetchall_result or []
    cursor.fetchone.return_value = fetchone_result
    cursor.rowcount = rowcount
    conn = FakeConn(cursor)
    return conn, cursor


# ---------------------------------------------------------------------------
# 1. doc_insert()
# ---------------------------------------------------------------------------

class TestDocInsert:
    def test_creates_table(self):
        conn, cur = capture_sql(
            fetchone_result=("abc-uuid", {"name": "alice"}, "2026-01-01"),
        )
        doc_insert(conn, "users", {"name": "alice"})
        calls = [c[0][0] for c in cur.execute.call_args_list]
        assert any("CREATE TABLE IF NOT EXISTS users" in c for c in calls)
        assert any("_id UUID PRIMARY KEY" in c for c in calls)

    def test_inserts_and_returns_dict(self):
        conn, cur = capture_sql(
            fetchone_result=("abc-uuid", {"name": "alice"}, "2026-01-01"),
        )
        result = doc_insert(conn, "users", {"name": "alice"})
        insert_call = [c for c in cur.execute.call_args_list if "INSERT INTO" in c[0][0]]
        assert len(insert_call) == 1
        sql = insert_call[0][0][0]
        params = insert_call[0][0][1]
        assert "INSERT INTO users (data) VALUES (%s::jsonb)" in sql
        assert "RETURNING _id, data, created_at" in sql
        assert params == (json.dumps({"name": "alice"}),)
        assert result == {"_id": "abc-uuid", "data": {"name": "alice"}, "created_at": "2026-01-01"}

    def test_commits(self):
        conn, cur = capture_sql(
            fetchone_result=("abc-uuid", {}, "2026-01-01"),
        )
        doc_insert(conn, "users", {})
        conn.commit.assert_called_once()

    def test_invalid_collection_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid identifier"):
            doc_insert(conn, "DROP TABLE; --", {"a": 1})


# ---------------------------------------------------------------------------
# 2. doc_insert_many()
# ---------------------------------------------------------------------------

class TestDocInsertMany:
    def test_creates_table(self):
        conn, cur = capture_sql(
            fetchall_result=[("id1", {"a": 1}, "ts"), ("id2", {"b": 2}, "ts")],
        )
        doc_insert_many(conn, "items", [{"a": 1}, {"b": 2}])
        calls = [c[0][0] for c in cur.execute.call_args_list]
        assert any("CREATE TABLE IF NOT EXISTS items" in c for c in calls)

    def test_batch_insert_and_returns_list(self):
        conn, cur = capture_sql(
            fetchall_result=[("id1", {"a": 1}, "ts"), ("id2", {"b": 2}, "ts")],
        )
        results = doc_insert_many(conn, "items", [{"a": 1}, {"b": 2}])
        insert_call = [c for c in cur.execute.call_args_list if "INSERT INTO" in c[0][0]]
        sql = insert_call[0][0][0]
        params = insert_call[0][0][1]
        assert "VALUES (%s::jsonb), (%s::jsonb)" in sql
        assert "RETURNING _id, data, created_at" in sql
        assert params == (json.dumps({"a": 1}), json.dumps({"b": 2}))
        assert len(results) == 2
        assert results[0]["_id"] == "id1"
        assert results[1]["data"] == {"b": 2}

    def test_commits(self):
        conn, cur = capture_sql(fetchall_result=[])
        doc_insert_many(conn, "items", [{"a": 1}])
        conn.commit.assert_called_once()

    def test_invalid_collection_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid identifier"):
            doc_insert_many(conn, "bad; name", [{"a": 1}])


# ---------------------------------------------------------------------------
# 3. doc_find()
# ---------------------------------------------------------------------------

class TestDocFind:
    def test_no_filter_no_where(self):
        conn, cur = capture_sql(
            fetchall_result=[("id1", {"name": "alice"}, "ts")],
        )
        results = doc_find(conn, "users")
        sql = cur.execute.call_args[0][0]
        assert "SELECT _id, data, created_at FROM users" in sql
        assert "WHERE" not in sql
        assert len(results) == 1

    def test_with_filter(self):
        conn, cur = capture_sql(
            fetchall_result=[("id1", {"status": "active"}, "ts")],
        )
        results = doc_find(conn, "users", filter={"status": "active"})
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "WHERE data @> %s::jsonb" in sql
        assert params[0] == json.dumps({"status": "active"})

    def test_with_sort(self):
        conn, cur = capture_sql(fetchall_result=[])
        doc_find(conn, "users", sort={"name": 1, "age": -1})
        sql = cur.execute.call_args[0][0]
        assert "ORDER BY data->>'name' ASC, data->>'age' DESC" in sql

    def test_with_limit_and_skip(self):
        conn, cur = capture_sql(fetchall_result=[])
        doc_find(conn, "users", limit=10, skip=20)
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "LIMIT %s" in sql
        assert "OFFSET %s" in sql
        assert params == (10, 20)

    def test_with_filter_limit_skip(self):
        conn, cur = capture_sql(fetchall_result=[])
        doc_find(conn, "users", filter={"active": True}, limit=5, skip=10)
        params = cur.execute.call_args[0][1]
        assert params == (json.dumps({"active": True}), 5, 10)

    def test_invalid_sort_key_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid sort key"):
            doc_find(conn, "users", sort={"name; DROP": 1})

    def test_invalid_collection_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid identifier"):
            doc_find(conn, "bad; table", filter={"a": 1})

    def test_returns_list_of_dicts(self):
        conn, cur = capture_sql(
            fetchall_result=[("id1", {"a": 1}, "ts1"), ("id2", {"b": 2}, "ts2")],
        )
        results = doc_find(conn, "users")
        assert isinstance(results, list)
        assert len(results) == 2
        assert results[0] == {"_id": "id1", "data": {"a": 1}, "created_at": "ts1"}


# ---------------------------------------------------------------------------
# 4. doc_find_one()
# ---------------------------------------------------------------------------

class TestDocFindOne:
    def test_no_filter(self):
        conn, cur = capture_sql(
            fetchone_result=("id1", {"name": "alice"}, "ts"),
        )
        result = doc_find_one(conn, "users")
        sql = cur.execute.call_args[0][0]
        assert "SELECT _id, data, created_at FROM users" in sql
        assert "LIMIT 1" in sql
        assert "WHERE" not in sql
        assert result == {"_id": "id1", "data": {"name": "alice"}, "created_at": "ts"}

    def test_with_filter(self):
        conn, cur = capture_sql(
            fetchone_result=("id1", {"name": "alice"}, "ts"),
        )
        doc_find_one(conn, "users", filter={"name": "alice"})
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "WHERE data @> %s::jsonb" in sql
        assert "LIMIT 1" in sql
        assert params == (json.dumps({"name": "alice"}),)

    def test_returns_none_when_not_found(self):
        conn, cur = capture_sql(fetchone_result=None)
        result = doc_find_one(conn, "users", filter={"name": "nobody"})
        assert result is None

    def test_invalid_collection_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid identifier"):
            doc_find_one(conn, "bad; name")


# ---------------------------------------------------------------------------
# 5. doc_update()
# ---------------------------------------------------------------------------

class TestDocUpdate:
    def test_sql_and_params(self):
        conn, cur = capture_sql(rowcount=3)
        result = doc_update(conn, "users", {"status": "old"}, {"status": "new"})
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "UPDATE users SET data = data || %s::jsonb WHERE data @> %s::jsonb" in sql
        assert params == (json.dumps({"status": "new"}), json.dumps({"status": "old"}))
        assert result == 3

    def test_returns_rowcount(self):
        conn, cur = capture_sql(rowcount=5)
        result = doc_update(conn, "users", {"a": 1}, {"a": 2})
        assert result == 5

    def test_commits(self):
        conn, cur = capture_sql(rowcount=1)
        doc_update(conn, "users", {"a": 1}, {"a": 2})
        conn.commit.assert_called_once()

    def test_invalid_collection_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid identifier"):
            doc_update(conn, "bad; name", {}, {})


# ---------------------------------------------------------------------------
# 6. doc_update_one()
# ---------------------------------------------------------------------------

class TestDocUpdateOne:
    def test_sql_and_params(self):
        conn, cur = capture_sql(rowcount=1)
        result = doc_update_one(conn, "users", {"name": "alice"}, {"age": 30})
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "WITH target AS" in sql
        assert "SELECT _id FROM users WHERE data @> %s::jsonb LIMIT 1" in sql
        assert "UPDATE users SET data = data || %s::jsonb FROM target WHERE users._id = target._id" in sql
        assert params == (json.dumps({"name": "alice"}), json.dumps({"age": 30}))

    def test_returns_rowcount(self):
        conn, cur = capture_sql(rowcount=1)
        result = doc_update_one(conn, "users", {"a": 1}, {"b": 2})
        assert result == 1

    def test_returns_zero_when_no_match(self):
        conn, cur = capture_sql(rowcount=0)
        result = doc_update_one(conn, "users", {"a": 1}, {"b": 2})
        assert result == 0

    def test_commits(self):
        conn, cur = capture_sql(rowcount=0)
        doc_update_one(conn, "users", {"a": 1}, {"b": 2})
        conn.commit.assert_called_once()

    def test_invalid_collection_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid identifier"):
            doc_update_one(conn, "bad; name", {}, {})


# ---------------------------------------------------------------------------
# 7. doc_delete()
# ---------------------------------------------------------------------------

class TestDocDelete:
    def test_sql_and_params(self):
        conn, cur = capture_sql(rowcount=2)
        result = doc_delete(conn, "users", {"status": "inactive"})
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "DELETE FROM users WHERE data @> %s::jsonb" in sql
        assert params == (json.dumps({"status": "inactive"}),)

    def test_returns_rowcount(self):
        conn, cur = capture_sql(rowcount=7)
        result = doc_delete(conn, "users", {"a": 1})
        assert result == 7

    def test_commits(self):
        conn, cur = capture_sql(rowcount=1)
        doc_delete(conn, "users", {"a": 1})
        conn.commit.assert_called_once()

    def test_invalid_collection_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid identifier"):
            doc_delete(conn, "bad; name", {"a": 1})


# ---------------------------------------------------------------------------
# 8. doc_delete_one()
# ---------------------------------------------------------------------------

class TestDocDeleteOne:
    def test_sql_and_params(self):
        conn, cur = capture_sql(rowcount=1)
        result = doc_delete_one(conn, "users", {"name": "alice"})
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "WITH target AS" in sql
        assert "SELECT _id FROM users WHERE data @> %s::jsonb LIMIT 1" in sql
        assert "DELETE FROM users USING target WHERE users._id = target._id" in sql
        assert params == (json.dumps({"name": "alice"}),)

    def test_returns_rowcount(self):
        conn, cur = capture_sql(rowcount=1)
        result = doc_delete_one(conn, "users", {"a": 1})
        assert result == 1

    def test_returns_zero_when_no_match(self):
        conn, cur = capture_sql(rowcount=0)
        result = doc_delete_one(conn, "users", {"a": 1})
        assert result == 0

    def test_commits(self):
        conn, cur = capture_sql(rowcount=0)
        doc_delete_one(conn, "users", {"a": 1})
        conn.commit.assert_called_once()

    def test_invalid_collection_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid identifier"):
            doc_delete_one(conn, "bad; name", {"a": 1})


# ---------------------------------------------------------------------------
# 9. doc_count()
# ---------------------------------------------------------------------------

class TestDocCount:
    def test_no_filter(self):
        conn, cur = capture_sql(fetchone_result=(42,))
        result = doc_count(conn, "users")
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "SELECT COUNT(*) FROM users" in sql
        assert "WHERE" not in sql
        assert params == ()
        assert result == 42

    def test_with_filter(self):
        conn, cur = capture_sql(fetchone_result=(5,))
        result = doc_count(conn, "users", filter={"status": "active"})
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "SELECT COUNT(*) FROM users" in sql
        assert "WHERE data @> %s::jsonb" in sql
        assert params == (json.dumps({"status": "active"}),)
        assert result == 5

    def test_invalid_collection_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid identifier"):
            doc_count(conn, "bad; name")


# ---------------------------------------------------------------------------
# 10. doc_create_index()
# ---------------------------------------------------------------------------

class TestDocCreateIndex:
    def test_gin_index_no_keys(self):
        conn, cur = capture_sql()
        doc_create_index(conn, "users")
        sql = cur.execute.call_args[0][0]
        assert "CREATE INDEX IF NOT EXISTS idx_users_gin ON users USING GIN (data)" in sql

    def test_btree_indexes_with_keys(self):
        conn, cur = capture_sql()
        doc_create_index(conn, "users", keys=["name", "email"])
        calls = [c[0][0] for c in cur.execute.call_args_list]
        assert any("idx_users_name" in c and "(data->>'name')" in c for c in calls)
        assert any("idx_users_email" in c and "(data->>'email')" in c for c in calls)

    def test_invalid_key_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid key"):
            doc_create_index(conn, "users", keys=["name; DROP"])

    def test_commits(self):
        conn, cur = capture_sql()
        doc_create_index(conn, "users")
        conn.commit.assert_called_once()

    def test_invalid_collection_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid identifier"):
            doc_create_index(conn, "bad; name")


# ---------------------------------------------------------------------------
# Parameter binding safety
# ---------------------------------------------------------------------------

class TestDocParameterBinding:
    def test_doc_insert_document_is_parameterized(self):
        conn, cur = capture_sql(
            fetchone_result=("id", {}, "ts"),
        )
        doc_insert(conn, "users", {"key": "'; DROP TABLE users; --"})
        insert_call = [c for c in cur.execute.call_args_list if "INSERT INTO" in c[0][0]]
        sql = insert_call[0][0][0]
        assert "DROP TABLE" not in sql

    def test_doc_find_filter_is_parameterized(self):
        conn, cur = capture_sql(fetchall_result=[])
        doc_find(conn, "users", filter={"key": "'; DROP TABLE users; --"})
        sql = cur.execute.call_args[0][0]
        assert "DROP TABLE" not in sql

    def test_doc_update_values_are_parameterized(self):
        conn, cur = capture_sql(rowcount=0)
        doc_update(conn, "users", {"key": "'; DROP TABLE users; --"}, {"a": 1})
        sql = cur.execute.call_args[0][0]
        assert "DROP TABLE" not in sql

    def test_doc_delete_filter_is_parameterized(self):
        conn, cur = capture_sql(rowcount=0)
        doc_delete(conn, "users", {"key": "'; DROP TABLE users; --"})
        sql = cur.execute.call_args[0][0]
        assert "DROP TABLE" not in sql
