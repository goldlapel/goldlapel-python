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
    doc_aggregate,
    _build_filter,
    _build_project,
    _field_path,
    _expand_dot_keys,
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


# ---------------------------------------------------------------------------
# 11. doc_aggregate()
# ---------------------------------------------------------------------------

class TestDocAggregate:
    def test_group_count(self):
        conn, cur = capture_sql(
            fetchall_result=[("electronics", 5)],
            description=[("_id",), ("count",)],
        )
        result = doc_aggregate(conn, "products", [
            {"$match": {"status": "active"}},
            {"$group": {"_id": "$category", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10},
        ])
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "GROUP BY" in sql
        assert "COUNT(*)" in sql
        assert "WHERE data @> %s::jsonb" in sql
        assert "ORDER BY count DESC" in sql
        assert "LIMIT %s" in sql
        assert params[0] == json.dumps({"status": "active"})
        assert params[1] == 10
        assert result == [{"_id": "electronics", "count": 5}]

    def test_group_avg(self):
        conn, cur = capture_sql(
            fetchall_result=[("electronics", 42.5)],
            description=[("_id",), ("avg_price",)],
        )
        doc_aggregate(conn, "products", [
            {"$group": {"_id": "$category", "avg_price": {"$avg": "$price"}}},
        ])
        sql = cur.execute.call_args[0][0]
        assert "AVG((data->>'price')::numeric)" in sql

    def test_group_null_id(self):
        conn, cur = capture_sql(
            fetchall_result=[(100,)],
            description=[("total",)],
        )
        doc_aggregate(conn, "orders", [
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
        ])
        sql = cur.execute.call_args[0][0]
        assert "GROUP BY" not in sql
        assert "SUM((data->>'amount')::numeric)" in sql

    def test_match_only(self):
        conn, cur = capture_sql(
            fetchall_result=[("id1", {"status": "active"}, "ts")],
        )
        doc_aggregate(conn, "users", [
            {"$match": {"status": "active"}},
        ])
        sql = cur.execute.call_args[0][0]
        assert "SELECT _id, data, created_at FROM users" in sql
        assert "WHERE data @> %s::jsonb" in sql

    def test_no_group_sort(self):
        conn, cur = capture_sql(fetchall_result=[])
        doc_aggregate(conn, "users", [
            {"$match": {"active": True}},
            {"$sort": {"name": 1}},
            {"$limit": 5},
        ])
        sql = cur.execute.call_args[0][0]
        assert "ORDER BY data->>'name' ASC" in sql
        assert "LIMIT %s" in sql

    def test_group_sort_uses_alias(self):
        conn, cur = capture_sql(
            fetchall_result=[],
            description=[("_id",), ("count",)],
        )
        doc_aggregate(conn, "products", [
            {"$group": {"_id": "$category", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ])
        sql = cur.execute.call_args[0][0]
        assert "ORDER BY count DESC" in sql
        assert "data->>'count'" not in sql

    def test_unsupported_stage(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Unsupported pipeline stage"):
            doc_aggregate(conn, "users", [{"$redact": {}}])

    def test_unsupported_accumulator(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Unsupported accumulator"):
            doc_aggregate(conn, "users", [
                {"$group": {"_id": "$category", "items": {"$first": "$name"}}},
            ])

    def test_invalid_field(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid field name"):
            doc_aggregate(conn, "users", [
                {"$group": {"_id": "$bad; field", "count": {"$sum": 1}}},
            ])

    def test_empty_pipeline(self):
        conn, cur = capture_sql(
            fetchall_result=[("id1", {"a": 1}, "ts")],
        )
        result = doc_aggregate(conn, "users", [])
        sql = cur.execute.call_args[0][0]
        assert "SELECT _id, data, created_at FROM users" in sql
        assert "WHERE" not in sql
        assert "GROUP BY" not in sql
        assert result == [{"_id": "id1", "data": {"a": 1}, "created_at": "ts"}]

    def test_composite_id(self):
        conn, cur = capture_sql(
            fetchall_result=[('{"region":"us","type":"pro"}', 3)],
            description=[("_id",), ("count",)],
        )
        doc_aggregate(conn, "orders", [
            {"$group": {
                "_id": {"region": "$region", "type": "$type"},
                "count": {"$sum": 1},
            }},
        ])
        sql = cur.execute.call_args[0][0]
        assert "json_build_object(" in sql
        assert "'region', data->>'region'" in sql
        assert "'type', data->>'type'" in sql
        assert "GROUP BY data->>'region', data->>'type'" in sql

    def test_composite_id_dot_notation(self):
        conn, cur = capture_sql(
            fetchall_result=[('{"city":"NY"}', 1)],
            description=[("_id",), ("count",)],
        )
        doc_aggregate(conn, "users", [
            {"$group": {
                "_id": {"city": "$addr.city"},
                "count": {"$sum": 1},
            }},
        ])
        sql = cur.execute.call_args[0][0]
        assert "json_build_object(" in sql
        assert "'city', data->'addr'->>'city'" in sql
        assert "GROUP BY data->'addr'->>'city'" in sql

    def test_composite_id_invalid_ref(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid field reference"):
            doc_aggregate(conn, "users", [
                {"$group": {"_id": {"x": "not_a_ref"}, "count": {"$sum": 1}}},
            ])

    def test_composite_id_invalid_alias(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid alias"):
            doc_aggregate(conn, "users", [
                {"$group": {"_id": {"bad;name": "$field"}, "count": {"$sum": 1}}},
            ])

    def test_push_accumulator(self):
        conn, cur = capture_sql(
            fetchall_result=[("electronics", ["tv", "phone"])],
            description=[("_id",), ("tags",)],
        )
        doc_aggregate(conn, "products", [
            {"$group": {"_id": "$category", "tags": {"$push": "$tag"}}},
        ])
        sql = cur.execute.call_args[0][0]
        assert "array_agg(data->>'tag') AS tags" in sql

    def test_addtoset_accumulator(self):
        conn, cur = capture_sql(
            fetchall_result=[("electronics", ["tv", "phone"])],
            description=[("_id",), ("cats",)],
        )
        doc_aggregate(conn, "products", [
            {"$group": {"_id": "$category", "cats": {"$addToSet": "$cat"}}},
        ])
        sql = cur.execute.call_args[0][0]
        assert "array_agg(DISTINCT data->>'cat') AS cats" in sql

    def test_single_id_unchanged(self):
        conn, cur = capture_sql(
            fetchall_result=[("electronics", 5)],
            description=[("_id",), ("count",)],
        )
        doc_aggregate(conn, "products", [
            {"$group": {"_id": "$category", "count": {"$sum": 1}}},
        ])
        sql = cur.execute.call_args[0][0]
        assert "data->>'category' AS _id" in sql
        assert "GROUP BY data->>'category'" in sql
        assert "json_build_object" not in sql

    def test_null_id_unchanged(self):
        conn, cur = capture_sql(
            fetchall_result=[(100,)],
            description=[("total",)],
        )
        doc_aggregate(conn, "orders", [
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
        ])
        sql = cur.execute.call_args[0][0]
        assert "GROUP BY" not in sql
        assert "json_build_object" not in sql


# ---------------------------------------------------------------------------
# 12. $project / $unwind / $lookup pipeline stages
# ---------------------------------------------------------------------------

class TestProjectStage:
    def test_project_include(self):
        conn, cur = capture_sql(
            fetchall_result=[("alice", "active")],
            description=[("name",), ("status",)],
        )
        doc_aggregate(conn, "users", [
            {"$project": {"name": 1, "status": 1}},
        ])
        sql = cur.execute.call_args[0][0]
        assert "data->>'name' AS name" in sql
        assert "data->>'status' AS status" in sql

    def test_project_exclude_id(self):
        conn, cur = capture_sql(
            fetchall_result=[("alice",)],
            description=[("name",)],
        )
        doc_aggregate(conn, "users", [
            {"$project": {"_id": 0, "name": 1}},
        ])
        sql = cur.execute.call_args[0][0]
        assert "_id" not in sql or "data->>'name' AS name" in sql
        assert "AS _id" not in sql

    def test_project_rename(self):
        conn, cur = capture_sql(
            fetchall_result=[("alice",)],
            description=[("fullName",)],
        )
        doc_aggregate(conn, "users", [
            {"$project": {"fullName": "$name"}},
        ])
        sql = cur.execute.call_args[0][0]
        assert "data->>'name' AS fullName" in sql

    def test_project_after_group(self):
        conn, cur = capture_sql(
            fetchall_result=[("electronics", 5)],
            description=[("_id",), ("count",)],
        )
        doc_aggregate(conn, "products", [
            {"$group": {"_id": "$category", "count": {"$sum": 1}}},
            {"$project": {"_id": 1, "count": 1}},
        ])
        sql = cur.execute.call_args[0][0]
        # $project after $group should pass through aliases, not data->>
        assert "data->>'_id'" not in sql
        assert "data->>'count'" not in sql
        # The select should reference the group aliases directly
        assert "_id" in sql
        assert "count" in sql
        # GROUP BY should still be present from $group
        assert "GROUP BY" in sql

    def test_project_dot_notation(self):
        conn, cur = capture_sql(
            fetchall_result=[("NY",)],
            description=[("city",)],
        )
        doc_aggregate(conn, "users", [
            {"$project": {"city": "$addr.city"}},
        ])
        sql = cur.execute.call_args[0][0]
        assert "data->'addr'->>'city' AS city" in sql


class TestUnwindStage:
    def test_unwind_basic(self):
        conn, cur = capture_sql(
            fetchall_result=[("id1", {"tags": ["a"]}, "ts")],
        )
        doc_aggregate(conn, "posts", [
            {"$unwind": "$tags"},
        ])
        sql = cur.execute.call_args[0][0]
        assert "jsonb_array_elements_text(data->'tags') AS _unwound_tags" in sql

    def test_unwind_then_group(self):
        conn, cur = capture_sql(
            fetchall_result=[("python", 3)],
            description=[("_id",), ("count",)],
        )
        doc_aggregate(conn, "posts", [
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
        ])
        sql = cur.execute.call_args[0][0]
        # GROUP BY should use the unwound alias, not data->>'tags'
        assert "_unwound_tags AS _id" in sql
        assert "GROUP BY _unwound_tags" in sql
        assert "data->>'tags'" not in sql

    def test_unwind_object_form(self):
        conn, cur = capture_sql(
            fetchall_result=[("id1", {"tags": ["a"]}, "ts")],
        )
        doc_aggregate(conn, "posts", [
            {"$unwind": {"path": "$tags"}},
        ])
        sql = cur.execute.call_args[0][0]
        assert "jsonb_array_elements_text(data->'tags') AS _unwound_tags" in sql

    def test_unwind_invalid(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="must be a string starting with"):
            doc_aggregate(conn, "posts", [{"$unwind": "no_dollar"}])


class TestLookupStage:
    def test_lookup_basic(self):
        conn, cur = capture_sql(
            fetchall_result=[("id1", {"name": "alice"}, "ts", "[]")],
            description=[("_id",), ("data",), ("created_at",), ("user_orders",)],
        )
        doc_aggregate(conn, "users", [
            {"$lookup": {
                "from": "orders",
                "localField": "uid",
                "foreignField": "uid",
                "as": "user_orders",
            }},
        ])
        sql = cur.execute.call_args[0][0]
        assert "COALESCE(" in sql
        assert "json_agg(b.data)" in sql
        assert "FROM orders b" in sql
        assert "b.data->>'uid'" in sql
        assert "users.data->>'uid'" in sql
        assert "AS user_orders" in sql

    def test_lookup_missing_field(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="missing required field"):
            doc_aggregate(conn, "users", [
                {"$lookup": {"localField": "uid", "foreignField": "uid", "as": "x"}},
            ])

    def test_lookup_validates_identifiers(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid identifier"):
            doc_aggregate(conn, "users", [
                {"$lookup": {
                    "from": "DROP TABLE; --",
                    "localField": "uid",
                    "foreignField": "uid",
                    "as": "x",
                }},
            ])


class TestFullPipeline:
    def test_full_pipeline(self):
        conn, cur = capture_sql(
            fetchall_result=[("python", 3)],
            description=[("_id",), ("count",)],
        )
        doc_aggregate(conn, "posts", [
            {"$match": {"status": "published"}},
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5},
        ])
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        # FROM has the unwind cross join
        assert "jsonb_array_elements_text(data->'tags')" in sql
        # GROUP BY uses the unwound alias
        assert "GROUP BY _unwound_tags" in sql
        # SELECT uses the unwound alias
        assert "_unwound_tags AS _id" in sql
        # WHERE from $match
        assert "WHERE data @> %s::jsonb" in sql
        # ORDER BY + LIMIT
        assert "ORDER BY count DESC" in sql
        assert "LIMIT %s" in sql
        assert params[1] == 5


# ---------------------------------------------------------------------------
# 13. Filter operators (_build_filter / _field_path)
# ---------------------------------------------------------------------------

class TestFilterOperators:
    def test_gt_numeric(self):
        clause, params = _build_filter({"age": {"$gt": 25}})
        assert "::numeric" in clause
        assert ">" in clause
        assert params == [25]

    def test_lte_string(self):
        clause, params = _build_filter({"name": {"$lte": "M"}})
        assert "::numeric" not in clause
        assert "<=" in clause
        assert params == ["M"]

    def test_in(self):
        clause, params = _build_filter({"status": {"$in": ["a", "b"]}})
        assert "IN (%s, %s)" in clause
        assert params == ["a", "b"]

    def test_nin(self):
        clause, params = _build_filter({"status": {"$nin": ["x"]}})
        assert "NOT IN (%s)" in clause
        assert params == ["x"]

    def test_exists_true(self):
        clause, params = _build_filter({"email": {"$exists": True}})
        assert "data ? %s" in clause
        assert "NOT" not in clause
        assert params == ["email"]

    def test_exists_false(self):
        clause, params = _build_filter({"email": {"$exists": False}})
        assert "NOT (data ? %s)" in clause
        assert params == ["email"]

    def test_regex(self):
        clause, params = _build_filter({"name": {"$regex": "^J"}})
        assert "~ %s" in clause
        assert params == ["^J"]

    def test_eq_ne(self):
        clause, params = _build_filter({"x": {"$eq": "a"}, "y": {"$ne": "b"}})
        assert "= %s" in clause
        assert "!= %s" in clause
        assert "a" in params
        assert "b" in params

    def test_mixed(self):
        clause, params = _build_filter({"active": True, "age": {"$gt": 18}})
        assert "data @> %s::jsonb" in clause
        assert "::numeric >" in clause
        assert params[0] == json.dumps({"active": True})
        assert params[1] == 18

    def test_dot_notation(self):
        clause, params = _build_filter({"addr.city": {"$eq": "NY"}})
        assert "data->'addr'->>'city'" in clause
        assert params == ["NY"]

    def test_range(self):
        clause, params = _build_filter({"age": {"$gte": 18, "$lt": 65}})
        assert ">=" in clause
        assert "<" in clause
        assert 18 in params
        assert 65 in params

    def test_plain_unchanged(self):
        clause, params = _build_filter({"status": "active"})
        assert clause == "data @> %s::jsonb"
        assert params == [json.dumps({"status": "active"})]

    def test_empty_unchanged(self):
        clause, params = _build_filter(None)
        assert clause == ""
        assert params == []

    def test_invalid_key(self):
        with pytest.raises(ValueError, match="Invalid filter key"):
            _build_filter({"bad;key": {"$gt": 1}})

    def test_unsupported_op(self):
        with pytest.raises(ValueError, match="Unsupported filter operator"):
            _build_filter({"x": {"$foo": 1}})


# ---------------------------------------------------------------------------
# Dot notation expansion in plain containment filters
# ---------------------------------------------------------------------------

class TestDotNotationExpansion:
    def test_dot_single_level(self):
        result = _expand_dot_keys({"addr.city": "NY"})
        assert result == {"addr": {"city": "NY"}}

    def test_dot_deep_nesting(self):
        result = _expand_dot_keys({"a.b.c": 1})
        assert result == {"a": {"b": {"c": 1}}}

    def test_dot_mixed_with_plain(self):
        result = _expand_dot_keys({"status": "active", "addr.city": "NY"})
        assert result == {"status": "active", "addr": {"city": "NY"}}

    def test_dot_merge_siblings(self):
        result = _expand_dot_keys({"a.b": 1, "a.c": 2})
        assert result == {"a": {"b": 1, "c": 2}}

    def test_no_dots_unchanged(self):
        result = _expand_dot_keys({"status": "active"})
        assert result == {"status": "active"}

    def test_dot_with_operators(self):
        clause, params = _build_filter({"addr.city": "NY", "age": {"$gt": 25}})
        assert "data @> %s::jsonb" in clause
        containment_json = params[0]
        assert json.loads(containment_json) == {"addr": {"city": "NY"}}
        assert "::numeric >" in clause
        assert 25 in params

    def test_dot_in_doc_find(self):
        conn, cur = capture_sql(fetchall_result=[])
        doc_find(conn, "users", filter={"addr.city": "NY"})
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "WHERE data @> %s::jsonb" in sql
        assert json.loads(params[0]) == {"addr": {"city": "NY"}}

    def test_dot_in_doc_count(self):
        conn, cur = capture_sql(fetchone_result=(3,))
        doc_count(conn, "users", filter={"addr.city": "NY"})
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "WHERE data @> %s::jsonb" in sql
        assert json.loads(params[0]) == {"addr": {"city": "NY"}}
