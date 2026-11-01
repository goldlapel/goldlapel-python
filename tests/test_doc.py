"""Mock-style unit tests for the goldlapel.utils.doc_* functions.

The doc_* utils are now driven by proxy-supplied query patterns (Phase 4 of
schema-to-core). Each test wraps the legacy `doc_<verb>(conn, "users", ...)`
call with a `patterns=` kwarg whose `tables.main` happens to be `"users"` —
this keeps the existing SQL-shape assertions valid while exercising the new
contract. Real callers go through `gl.documents.<verb>(...)` which fetches
patterns from the proxy with `_goldlapel.doc_<name>` as the canonical table.
"""

from unittest.mock import MagicMock, call
import json

import pytest

from goldlapel import utils
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
    doc_watch,
    doc_unwatch,
    doc_create_ttl_index,
    doc_remove_ttl_index,
    doc_create_capped,
    doc_remove_cap,
    doc_find_one_and_update,
    doc_find_one_and_delete,
    doc_distinct,
    doc_find_cursor,
    doc_create_collection,
    _build_filter,
    _build_update,
    _build_project,
    _jsonpath,
    _text_path,
)


def _patterns(name):
    """Build a fake patterns dict keyed off the user collection name.

    For the mock unit tests we use the bare name as the table — the proxy
    would actually return `_goldlapel.doc_<name>` but the wrapper code is
    table-name-agnostic, so this preserves the existing SQL-shape assertions
    (which were written against the legacy `CREATE TABLE <name>` flow).
    """
    return {
        "tables": {"main": name},
        "query_patterns": {
            "insert": f"INSERT INTO {name} (data) VALUES ($1::jsonb) RETURNING _id, data, created_at",
        },
    }


# Wrap every doc_* function with `patterns=_patterns(name)` automatically.
# This mirrors how `gl.documents.<verb>` would call them in production.
def _wrap(fn):
    def wrapped(conn, collection, *args, **kwargs):
        kwargs.setdefault("patterns", _patterns(collection))
        return fn(conn, collection, *args, **kwargs)
    wrapped.__name__ = fn.__name__
    wrapped.__wrapped__ = fn
    return wrapped


# Override module-level imports with the auto-patterns wrappers so existing
# tests keep using `doc_insert(conn, "users", ...)` without each call site
# threading the `patterns=` kwarg.
doc_insert = _wrap(doc_insert)
doc_insert_many = _wrap(doc_insert_many)
doc_find = _wrap(doc_find)
doc_find_one = _wrap(doc_find_one)
doc_update = _wrap(doc_update)
doc_update_one = _wrap(doc_update_one)
doc_delete = _wrap(doc_delete)
doc_delete_one = _wrap(doc_delete_one)
doc_count = _wrap(doc_count)
doc_create_index = _wrap(doc_create_index)
doc_aggregate = _wrap(doc_aggregate)
doc_watch = _wrap(doc_watch)
doc_unwatch = _wrap(doc_unwatch)
doc_create_ttl_index = _wrap(doc_create_ttl_index)
doc_remove_ttl_index = _wrap(doc_remove_ttl_index)
doc_create_capped = _wrap(doc_create_capped)
doc_remove_cap = _wrap(doc_remove_cap)
doc_find_one_and_update = _wrap(doc_find_one_and_update)
doc_find_one_and_delete = _wrap(doc_find_one_and_delete)
doc_distinct = _wrap(doc_distinct)
doc_find_cursor = _wrap(doc_find_cursor)
doc_create_collection = _wrap(doc_create_collection)


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commit = MagicMock()

    def cursor(self, name=None):
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
# 0. doc_create_collection()
# ---------------------------------------------------------------------------

class TestDocCreateCollection:
    """doc_create_collection is now a no-op on the wrapper side — DDL is
    executed by the proxy via /api/ddl/doc_store/create when the
    DocumentsAPI._patterns helper fires. The util just validates the
    identifier."""

    def test_no_ddl_emitted_on_wrapper_side(self):
        conn, cur = capture_sql()
        doc_create_collection(conn, "users")
        # Proxy owns DDL — wrapper does not run any SQL itself.
        assert cur.execute.call_count == 0

    def test_unlogged_kwarg_does_not_emit_ddl(self):
        # The unlogged flag flows through the DDL API options on the proxy
        # side (see DocumentsAPI._patterns); the wrapper doesn't run DDL.
        conn, cur = capture_sql()
        doc_create_collection(conn, "sessions", unlogged=True)
        assert cur.execute.call_count == 0

    def test_no_commit_when_no_ddl(self):
        # No DDL → no commit on the wrapper-side connection.
        conn, cur = capture_sql()
        doc_create_collection(conn, "test_col")
        conn.commit.assert_not_called()

    def test_rejects_invalid_identifier(self):
        conn, cur = capture_sql()
        with pytest.raises(ValueError):
            doc_create_collection(conn, "Robert'; DROP TABLE")

    def test_raises_when_patterns_missing(self):
        # The wrapped helper at the top of this file always supplies patterns.
        # Direct util calls without patterns should fail loud.
        conn, cur = capture_sql()
        with pytest.raises(RuntimeError, match="requires DDL patterns"):
            utils.doc_create_collection(conn, "users")


# ---------------------------------------------------------------------------
# 1. doc_insert()
# ---------------------------------------------------------------------------

class TestDocInsert:
    def test_no_create_table_emitted(self):
        # Proxy owns CREATE TABLE — wrapper just runs the INSERT now.
        conn, cur = capture_sql(
            fetchone_result=("abc-uuid", {"name": "alice"}, "2026-01-01"),
        )
        doc_insert(conn, "users", {"name": "alice"})
        calls = [c[0][0] for c in cur.execute.call_args_list]
        assert not any("CREATE TABLE" in c for c in calls), (
            "wrapper must not emit DDL — proxy owns it"
        )

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
    def test_no_create_table_emitted(self):
        # Proxy owns CREATE TABLE — wrapper just runs the bulk INSERT now.
        conn, cur = capture_sql(
            fetchall_result=[("id1", {"a": 1}, "ts"), ("id2", {"b": 2}, "ts")],
        )
        doc_insert_many(conn, "items", [{"a": 1}, {"b": 2}])
        calls = [c[0][0] for c in cur.execute.call_args_list]
        assert not any("CREATE TABLE" in c for c in calls), (
            "wrapper must not emit DDL — proxy owns it"
        )

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
        assert "WHERE data @? %s::text::jsonpath" in sql
        assert params[0] == '$."status" ? (@ == "active")'

    def test_with_sort(self):
        conn, cur = capture_sql(fetchall_result=[])
        doc_find(conn, "users", sort={"name": 1, "age": -1})
        sql = cur.execute.call_args[0][0]
        assert "ORDER BY (CASE jsonb_typeof(data #> '{name}')" in sql
        assert "(data #> '{name}') ASC, (CASE jsonb_typeof(data #> '{age}')" in sql
        assert sql.endswith("(data #> '{age}') DESC")

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
        assert params == ('$."active" ? (@ == true)', 5, 10)

    def test_invalid_sort_key_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid field key"):
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
        assert "WHERE data @? %s::text::jsonpath" in sql
        assert "LIMIT 1" in sql
        assert params == ('$."name" ? (@ == "alice")',)

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
        # No operators: the document is replaced, as by MongoDB's replace_one
        assert "UPDATE users SET data = %s::jsonb, updated_at = NOW() WHERE data @? %s::text::jsonpath" in sql
        assert params == (json.dumps({"status": "new"}), '$."status" ? (@ == "old")')
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
        assert "SELECT _id FROM users WHERE data @? %s::text::jsonpath LIMIT 1" in sql
        assert "UPDATE users SET data = %s::jsonb, updated_at = NOW() FROM target WHERE users._id = target._id" in sql
        assert params == ('$."name" ? (@ == "alice")', json.dumps({"age": 30}))

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
        assert "DELETE FROM users WHERE data @? %s::text::jsonpath" in sql
        assert params == ('$."status" ? (@ == "inactive")',)

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
        assert "SELECT _id FROM users WHERE data @? %s::text::jsonpath LIMIT 1" in sql
        assert "DELETE FROM users USING target WHERE users._id = target._id" in sql
        assert params == ('$."name" ? (@ == "alice")',)

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
        assert "WHERE data @? %s::text::jsonpath" in sql
        assert params == ('$."status" ? (@ == "active")',)
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
        # In the order a sort on the key uses
        assert any("idx_users_name" in c and "jsonb_typeof(data #> '{name}')" in c for c in calls)
        assert any("idx_users_email" in c and "jsonb_typeof(data #> '{email}')" in c for c in calls)

    def test_dotted_key_names_its_index(self):
        conn, cur = capture_sql()
        doc_create_index(conn, "users", keys=["addr.city"])
        sql = cur.execute.call_args[0][0]
        assert "idx_users_addr__city" in sql and "data #> '{addr,city}'" in sql

    def test_invalid_key_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid field key"):
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
        assert "WHERE data @? %s::text::jsonpath" in sql
        assert "ORDER BY count DESC" in sql
        assert "LIMIT %s" in sql
        assert params[0] == '$."status" ? (@ == "active")'
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
        assert "WHERE data @? %s::text::jsonpath" in sql

    def test_no_group_sort(self):
        conn, cur = capture_sql(fetchall_result=[])
        doc_aggregate(conn, "users", [
            {"$match": {"active": True}},
            {"$sort": {"name": 1}},
            {"$limit": 5},
        ])
        sql = cur.execute.call_args[0][0]
        assert "ORDER BY (CASE jsonb_typeof(data #> '{name}')" in sql
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
        assert "jsonb_array_elements_text(data #> '{tags}') AS _unwound_tags" in sql

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
        assert "jsonb_array_elements_text(data #> '{tags}') AS _unwound_tags" in sql

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
        assert "jsonb_array_elements_text(data #> '{tags}')" in sql
        # GROUP BY uses the unwound alias
        assert "GROUP BY _unwound_tags" in sql
        # SELECT uses the unwound alias
        assert "_unwound_tags AS _id" in sql
        # WHERE from $match
        assert "WHERE data @? %s::text::jsonpath" in sql
        # ORDER BY + LIMIT
        assert "ORDER BY count DESC" in sql
        assert "LIMIT %s" in sql
        assert params[1] == 5


# ---------------------------------------------------------------------------
# 13. Filter operators (_build_filter / _field_path)
# ---------------------------------------------------------------------------

class TestFilterOperators:
    """Each operator compiles to an SQL/JSON path predicate on `data`; what
    they match is tested against Postgres in test_doc_semantics.py."""

    def test_gt_numeric(self):
        clause, params = _build_filter({"age": {"$gt": 25}})
        assert clause == "data @? %s::text::jsonpath"
        assert params == ['$."age" ? (@ > 25)']

    def test_lte_string(self):
        clause, params = _build_filter({"name": {"$lte": "M"}})
        assert params == ['$."name" ? (@ <= "M")']

    def test_bool_is_a_boolean_not_a_number(self):
        _, params = _build_filter({"active": {"$eq": True}})
        assert params == ['$."active" ? (@ == true)']

    def test_in(self):
        clause, params = _build_filter({"status": {"$in": ["a", "b"]}})
        assert params == ['$."status" ? (@ == "a" || @ == "b")']

    def test_in_empty_matches_nothing(self):
        assert _build_filter({"status": {"$in": []}}) == ("FALSE", [])

    def test_in_with_null_matches_missing(self):
        clause, params = _build_filter({"status": {"$in": ["a", None]}})
        assert "NOT data @? %s::text::jsonpath" in clause
        assert params == ['$."status" ? (@ == "a")', '$."status"', '$."status" ? (@ == null)']

    def test_nin(self):
        clause, params = _build_filter({"status": {"$nin": ["x"]}})
        assert clause == "NOT (data @? %s::text::jsonpath)"
        assert params == ['$."status" ? (@ == "x")']

    def test_exists_true(self):
        clause, params = _build_filter({"email": {"$exists": True}})
        assert clause == "data @? %s::text::jsonpath"
        assert params == ['$."email"']

    def test_exists_false_follows_the_whole_path(self):
        clause, params = _build_filter({"addr.city": {"$exists": False}})
        assert clause == "NOT data @? %s::text::jsonpath"
        assert params == ['$."addr"."city"']

    def test_regex(self):
        _, params = _build_filter({"name": {"$regex": "^J"}})
        assert params == ['$."name" ? (@ like_regex "^J")']

    def test_regex_options(self):
        _, params = _build_filter({"name": {"$regex": "^j\\d", "$options": "i"}})
        assert params == ['$."name" ? (@ like_regex "^j\\\\d" flag "i")']

    def test_eq_ne(self):
        clause, params = _build_filter({"x": {"$eq": "a"}, "y": {"$ne": "b"}})
        assert clause == "data @? %s::text::jsonpath AND NOT data @? %s::text::jsonpath"
        assert params == ['$."x" ? (@ == "a")', '$."y" ? (@ == "b")']

    def test_eq_null_matches_missing(self):
        clause, params = _build_filter({"x": None})
        assert clause == "(NOT data @? %s::text::jsonpath OR data @? %s::text::jsonpath)"
        assert params == ['$."x"', '$."x" ? (@ == null)']

    def test_object_compares_whole(self):
        clause, params = _build_filter({"size": {"h": 10}})
        assert "data #> '{size}' = %s::jsonb" in clause
        assert params == [json.dumps({"h": 10}), json.dumps({"h": 10})]

    def test_mixed(self):
        clause, params = _build_filter({"active": True, "age": {"$gt": 18}})
        assert clause == "data @? %s::text::jsonpath AND data @? %s::text::jsonpath"
        assert params == ['$."active" ? (@ == true)', '$."age" ? (@ > 18)']

    def test_dot_notation(self):
        _, params = _build_filter({"addr.city": {"$eq": "NY"}})
        assert params == ['$."addr"."city" ? (@ == "NY")']

    def test_range(self):
        clause, params = _build_filter({"age": {"$gte": 18, "$lt": 65}})
        assert params == ['$."age" ? (@ >= 18)', '$."age" ? (@ < 65)']

    def test_size_and_all(self):
        clause, params = _build_filter({"tags": {"$size": 2, "$all": ["a", "b"]}})
        assert "jsonb_array_length(data #> '{tags}') = 2" in clause
        assert params == ['$."tags" ? (@ == "a")', '$."tags" ? (@ == "b")']

    def test_field_not(self):
        clause, params = _build_filter({"age": {"$not": {"$gt": 5}}})
        assert clause == "NOT (data @? %s::text::jsonpath)"
        assert params == ['$."age" ? (@ > 5)']

    def test_id_is_the_column(self):
        assert _build_filter({"_id": "abc"}) == ("_id = %s::text::uuid", ["abc"])

    def test_mixing_operators_and_fields_raises(self):
        with pytest.raises(ValueError, match="mixes operators"):
            _build_filter({"a": {"$gt": 1, "b": 2}})

    def test_options_without_regex_raises(self):
        with pytest.raises(ValueError, match="needs \\$regex"):
            _build_filter({"a": {"$options": "i"}})

    def test_empty_unchanged(self):
        clause, params = _build_filter(None)
        assert clause == ""
        assert params == []

    def test_invalid_key(self):
        with pytest.raises(ValueError, match="Invalid field key"):
            _build_filter({"bad;key": {"$gt": 1}})

    def test_unsupported_op(self):
        with pytest.raises(ValueError, match="Unsupported filter operator"):
            _build_filter({"x": {"$foo": 1}})


class TestDocWatch:
    def test_watch_creates_trigger(self):
        conn, cur = capture_sql()
        with pytest.raises(Exception):
            # _make_listen_connection will fail on the mock, but trigger SQL is already executed
            doc_watch(conn, "events", lambda e: None)
        calls = [c[0][0] for c in cur.execute.call_args_list]
        assert any("CREATE OR REPLACE FUNCTION _gl_watch_events()" in c for c in calls)
        assert any("pg_notify" in c for c in calls)
        # Atomic CREATE OR REPLACE TRIGGER (PG14+) — matches the Go wrapper.
        # Avoids the race where a DROP + CREATE pair could have two concurrent
        # doc_watch calls replace each other's triggers mid-flight.
        assert any("CREATE OR REPLACE TRIGGER _gl_watch_events_trigger" in c for c in calls)
        # Guard against the racy DROP + CREATE pair regressing.
        assert not any(
            "DROP TRIGGER IF EXISTS _gl_watch_events_trigger" in c for c in calls
        )

    @pytest.fixture
    def mock_listen(self, monkeypatch):
        import goldlapel.utils as utils_mod
        mock_listen_conn = MagicMock()
        mock_listen_conn.notifies = []
        mock_listen_conn.poll = MagicMock()
        mock_listen_cur = MagicMock()
        mock_listen_conn.cursor.return_value = mock_listen_cur
        mock_listen_conn.commit = MagicMock()
        monkeypatch.setattr(utils_mod, "_make_listen_connection", lambda c: mock_listen_conn)
        return mock_listen_conn, mock_listen_cur

    def test_watch_listens(self, mock_listen):
        mock_listen_conn, mock_listen_cur = mock_listen
        conn, cur = capture_sql()
        # select.select will raise StopIteration via side_effect to break the loop
        import select as select_mod
        original_select = select_mod.select

        call_count = [0]
        def fake_select(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] > 1:
                raise KeyboardInterrupt()
            return ([], [], [])

        import goldlapel.utils as utils_mod
        old_select = select_mod.select
        select_mod.select = fake_select
        try:
            with pytest.raises(KeyboardInterrupt):
                doc_watch(conn, "events", lambda e: None, blocking=True)
        finally:
            select_mod.select = old_select
        # Verify LISTEN was called on the listen connection
        listen_calls = [c[0][0] for c in mock_listen_cur.execute.call_args_list]
        assert any("LISTEN _gl_changes_events" in c for c in listen_calls)

    def test_unwatch_drops(self):
        conn, cur = capture_sql()
        doc_unwatch(conn, "events")
        calls = [c[0][0] for c in cur.execute.call_args_list]
        assert any("DROP TRIGGER IF EXISTS _gl_watch_events_trigger ON events" in c for c in calls)
        assert any("DROP FUNCTION IF EXISTS _gl_watch_events()" in c for c in calls)


# ---------------------------------------------------------------------------
# 15. TTL indexes (doc_create_ttl_index / doc_remove_ttl_index)
# ---------------------------------------------------------------------------

class TestDocTTL:
    def test_ttl_creates_index_and_trigger(self):
        conn, cur = capture_sql()
        doc_create_ttl_index(conn, "sessions", 3600)
        calls = [c[0][0] for c in cur.execute.call_args_list]
        assert any("CREATE INDEX IF NOT EXISTS idx_sessions_ttl" in c for c in calls)
        assert any("CREATE OR REPLACE FUNCTION _gl_ttl_sessions()" in c for c in calls)
        assert any("INTERVAL '3600 seconds'" in c for c in calls)
        # Atomic CREATE OR REPLACE TRIGGER (PG14+) — matches the Go wrapper.
        assert any("CREATE OR REPLACE TRIGGER _gl_ttl_sessions_trigger" in c for c in calls)
        # Guard against the racy DROP + CREATE pair regressing.
        assert not any(
            "DROP TRIGGER IF EXISTS _gl_ttl_sessions_trigger" in c for c in calls
        )

    def test_ttl_validates_seconds(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="expire_after_seconds must be an integer"):
            doc_create_ttl_index(conn, "sessions", "not_a_number")

    def test_remove_ttl(self):
        conn, cur = capture_sql()
        doc_remove_ttl_index(conn, "sessions")
        calls = [c[0][0] for c in cur.execute.call_args_list]
        assert any("DROP TRIGGER IF EXISTS _gl_ttl_sessions_trigger ON sessions" in c for c in calls)
        assert any("DROP FUNCTION IF EXISTS _gl_ttl_sessions()" in c for c in calls)
        assert any("DROP INDEX IF EXISTS idx_sessions_ttl" in c for c in calls)


# ---------------------------------------------------------------------------
# 16. Capped collections (doc_create_capped / doc_remove_cap)
# ---------------------------------------------------------------------------

class TestDocCapped:
    def test_capped_creates_trigger(self):
        conn, cur = capture_sql()
        doc_create_capped(conn, "logs", 1000)
        calls = [c[0][0] for c in cur.execute.call_args_list]
        assert any("CREATE OR REPLACE FUNCTION _gl_cap_logs()" in c for c in calls)
        assert any("COUNT(*) - 1000" in c for c in calls)
        assert any("DELETE" in c and "LIMIT excess" in c for c in calls)
        # Atomic CREATE OR REPLACE TRIGGER (PG14+) — matches the Go wrapper.
        assert any("CREATE OR REPLACE TRIGGER _gl_cap_logs_trigger" in c for c in calls)
        # Guard against the racy DROP + CREATE pair regressing.
        assert not any(
            "DROP TRIGGER IF EXISTS _gl_cap_logs_trigger" in c for c in calls
        )

    def test_capped_does_not_create_collection_table(self):
        # The collection table is materialized by the proxy via the DDL API.
        # doc_create_capped only adds the cap trigger + supporting index.
        conn, cur = capture_sql()
        doc_create_capped(conn, "logs", 500)
        calls = [c[0][0] for c in cur.execute.call_args_list]
        assert not any("CREATE TABLE" in c for c in calls)

    def test_capped_validates_max_documents(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="max_documents must be an integer"):
            doc_create_capped(conn, "logs", "not_a_number")

    def test_remove_cap(self):
        conn, cur = capture_sql()
        doc_remove_cap(conn, "logs")
        calls = [c[0][0] for c in cur.execute.call_args_list]
        assert any("DROP TRIGGER IF EXISTS _gl_cap_logs_trigger ON logs" in c for c in calls)
        assert any("DROP FUNCTION IF EXISTS _gl_cap_logs()" in c for c in calls)


# ---------------------------------------------------------------------------
# 17. Identifier validation across new methods
# ---------------------------------------------------------------------------

class TestOperationalIdentifierValidation:
    def test_invalid_collection_rejected(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid identifier"):
            doc_watch(conn, "bad; name", lambda e: None)
        with pytest.raises(ValueError, match="Invalid identifier"):
            doc_unwatch(conn, "bad; name")
        with pytest.raises(ValueError, match="Invalid identifier"):
            doc_create_ttl_index(conn, "bad; name", 3600)
        with pytest.raises(ValueError, match="Invalid identifier"):
            doc_remove_ttl_index(conn, "bad; name")
        with pytest.raises(ValueError, match="Invalid identifier"):
            doc_create_capped(conn, "bad; name", 1000)
        with pytest.raises(ValueError, match="Invalid identifier"):
            doc_remove_cap(conn, "bad; name")


# ---------------------------------------------------------------------------
# 18. Logical operators ($or, $and, $not)
# ---------------------------------------------------------------------------

class TestLogicalOperators:
    def test_or_simple(self):
        clause, params = _build_filter({"$or": [{"status": "active"}, {"status": "inactive"}]})
        assert "OR" in clause
        assert clause.startswith("(")
        assert params == ['$."status" ? (@ == "active")', '$."status" ? (@ == "inactive")']

    def test_and_explicit(self):
        clause, params = _build_filter({"$and": [{"age": {"$gt": 18}}, {"age": {"$lt": 65}}]})
        assert "AND" in clause
        assert clause.startswith("(")
        assert params == ['$."age" ? (@ > 18)', '$."age" ? (@ < 65)']

    def test_not(self):
        clause, params = _build_filter({"$not": {"status": "active"}})
        assert clause.startswith("NOT (")
        assert params == ['$."status" ? (@ == "active")']

    def test_nor(self):
        clause, params = _build_filter({"$nor": [{"a": 1}, {"b": 2}]})
        assert clause == "NOT ((data @? %s::text::jsonpath) OR (data @? %s::text::jsonpath))"
        assert params == ['$."a" ? (@ == 1)', '$."b" ? (@ == 2)']

    def test_or_with_operators(self):
        clause, params = _build_filter({
            "$or": [{"status": "active"}, {"age": {"$gt": 25}}]
        })
        assert "OR" in clause
        assert params == ['$."status" ? (@ == "active")', '$."age" ? (@ > 25)']

    def test_nested_or_and(self):
        clause, params = _build_filter({
            "$or": [
                {"$and": [{"a": 1}, {"b": 2}]},
                {"$not": {"c": 3}},
            ]
        })
        assert "OR" in clause
        assert "AND" in clause
        assert "NOT" in clause

    def test_mixed_logical_and_field(self):
        clause, params = _build_filter({
            "name": "alice",
            "$or": [{"status": "active"}, {"age": {"$gt": 25}}]
        })
        assert "AND" in clause
        assert "OR" in clause
        first_param = params[0]
        assert "alice" in first_param

    def test_or_empty_raises(self):
        with pytest.raises(ValueError, match="non-empty array"):
            _build_filter({"$or": []})

    def test_or_non_list_raises(self):
        with pytest.raises(ValueError, match="non-empty array"):
            _build_filter({"$or": {"a": 1}})

    def test_not_non_dict_raises(self):
        with pytest.raises(ValueError, match="filter object"):
            _build_filter({"$not": [{"a": 1}]})

    def test_or_in_find(self):
        conn, cur = capture_sql(fetchall_result=[])
        doc_find(conn, "users", filter={"$or": [{"status": "active"}, {"status": "inactive"}]})
        sql = cur.execute.call_args[0][0]
        assert "OR" in sql

    def test_not_in_count(self):
        conn, cur = capture_sql(fetchone_result=(5,))
        doc_count(conn, "users", filter={"$not": {"status": "suspended"}})
        sql = cur.execute.call_args[0][0]
        assert "NOT" in sql


# ---------------------------------------------------------------------------
# 19. Field update operators ($set, $inc, $unset, $mul, $rename)
# ---------------------------------------------------------------------------

class TestFieldUpdateOperators:
    """Each operator is a step reading the document so far (`e<n>`); what
    they do is tested against Postgres in test_doc_semantics.py."""

    def test_no_operators_replaces(self):
        expr, params = _build_update({"name": "new"})
        assert expr == "%s::jsonb"
        assert params == [json.dumps({"name": "new"})]

    def test_set(self):
        expr, params = _build_update({"$set": {"name": "new", "age": 30}})
        assert expr == (
            "(SELECT e2 FROM (SELECT data AS e0) AS s0"
            " CROSS JOIN LATERAL (SELECT jsonb_set(e0, '{name}', %s::jsonb) AS e1) AS s1"
            " CROSS JOIN LATERAL (SELECT jsonb_set(e1, '{age}', %s::jsonb) AS e2) AS s2)"
        )
        assert params == ['"new"', "30"]

    def test_set_nested_makes_parents(self):
        expr, _ = _build_update({"$set": {"a.b": 1}})
        assert "CASE WHEN jsonb_typeof(e0 #> '{a}') = 'object' THEN e0 #> '{a}' ELSE '{}'::jsonb END" in expr

    def test_inc(self):
        expr, params = _build_update({"$inc": {"count": 1}})
        assert "to_jsonb(COALESCE((e0 #> '{count}')::numeric, 0) + %s::text::numeric)" in expr
        assert params == ["1"]

    def test_inc_takes_a_number(self):
        for amount in ["1", True, None]:
            with pytest.raises(ValueError, match="takes a number"):
                _build_update({"$inc": {"count": amount}})

    def test_unset_nested(self):
        expr, params = _build_update({"$unset": {"nested.field": ""}})
        assert "(e0 #- '{nested,field}')" in expr
        assert params == []

    def test_mul(self):
        expr, params = _build_update({"$mul": {"price": 1.1}})
        assert "* %s::text::numeric" in expr
        assert params == ["1.1"]

    def test_rename_of_a_missing_field_is_a_no_op(self):
        expr, _ = _build_update({"$rename": {"old_name": "new_name"}})
        assert "CASE WHEN e0 #> '{old_name}' IS NULL THEN e0 ELSE" in expr

    def test_steps_build_on_each_other(self):
        expr, params = _build_update({"$set": {"name": "new"}, "$inc": {"count": 1}, "$unset": {"temp": ""}})
        assert "jsonb_set(e0, '{name}'" in expr and "(e1 #> '{count}')" in expr and "(e2 #- '{temp}')" in expr
        assert params == ['"new"', "1"]

    def test_set_in_doc_update(self):
        conn, cur = capture_sql(rowcount=1)
        doc_update(conn, "users", {"status": "old"}, {"$set": {"status": "new"}})
        sql = cur.execute.call_args[0][0]
        assert "UPDATE users SET data = (SELECT e1 FROM (SELECT data AS e0)" in sql
        assert "updated_at = NOW()" in sql

    def test_inc_in_doc_update_one(self):
        conn, cur = capture_sql(rowcount=1)
        doc_update_one(conn, "users", {"name": "alice"}, {"$inc": {"score": 10}})
        sql = cur.execute.call_args[0][0]
        assert "jsonb_set" in sql
        assert "COALESCE" in sql

    def test_invalid_field_key_raises(self):
        with pytest.raises(ValueError, match="Invalid field key"):
            _build_update({"$inc": {"bad;field": 1}})

    def test_operators_and_fields_together_raise(self):
        with pytest.raises(ValueError, match="not both"):
            _build_update({"$set": {"a": 1}, "b": 2})

    def test_unknown_operator_raises(self):
        with pytest.raises(ValueError, match="Unsupported update operator"):
            _build_update({"$min": {"a": 1}})


# ---------------------------------------------------------------------------
# 20. Array update operators ($push, $pull, $addToSet)
# ---------------------------------------------------------------------------

class TestArrayUpdateOperators:
    def test_push_one(self):
        expr, params = _build_update({"$push": {"tags": "new_tag"}})
        assert "jsonb_array_length(COALESCE(e0 #> '{tags}', '[]'::jsonb))" in expr
        assert params == [json.dumps(["new_tag"])]

    def test_push_a_list_is_one_element(self):
        _, params = _build_update({"$push": {"tags": ["a", "b"]}})
        assert params == [json.dumps([["a", "b"]])]

    def test_push_each(self):
        _, params = _build_update({"$push": {"tags": {"$each": ["a", "b"]}}})
        assert params == [json.dumps(["a", "b"])]

    def test_push_other_modifiers_raise(self):
        with pytest.raises(ValueError, match="\\$each"):
            _build_update({"$push": {"tags": {"$each": [1], "$slice": 2}}})

    def test_pull_value(self):
        expr, params = _build_update({"$pull": {"tags": "old_tag"}})
        assert "WHERE NOT (x = %s::jsonb)" in expr
        assert params == ['"old_tag"']

    def test_pull_condition(self):
        expr, params = _build_update({"$pull": {"scores": {"$gte": 6}}})
        assert "WHERE NOT (x @? %s::text::jsonpath)" in expr
        assert params == ["$ ? ((@ >= 6))"]

    def test_add_to_set_compares_exactly(self):
        expr, params = _build_update({"$addToSet": {"tags": "maybe"}})
        assert "WHERE x = d.v" in expr
        assert params == [json.dumps(["maybe"])]

    def test_push_in_doc_update(self):
        conn, cur = capture_sql(rowcount=1)
        doc_update(conn, "users", {"name": "alice"}, {"$push": {"tags": "python"}})
        sql = cur.execute.call_args[0][0]
        assert "jsonb_set" in sql
        assert "COALESCE" in sql


# ---------------------------------------------------------------------------
# 21. doc_find_one_and_update()
# ---------------------------------------------------------------------------

class TestDocFindOneAndUpdate:
    def test_returns_document(self):
        conn, cur = capture_sql(
            fetchone_result=("uuid-1", {"name": "alice", "score": 10}, "2026-01-01"),
        )
        result = doc_find_one_and_update(
            conn, "users", {"name": "alice"}, {"$inc": {"score": 5}}
        )
        sql = cur.execute.call_args[0][0]
        assert "WITH target AS" in sql
        assert "RETURNING" in sql
        assert "jsonb_set" in sql
        assert result is not None
        assert result["_id"] == "uuid-1"

    def test_returns_none_no_match(self):
        conn, cur = capture_sql(fetchone_result=None)
        result = doc_find_one_and_update(
            conn, "users", {"name": "nobody"}, {"status": "new"}
        )
        assert result is None

    def test_plain_update(self):
        conn, cur = capture_sql(
            fetchone_result=("uuid-1", {"name": "alice"}, "2026-01-01"),
        )
        doc_find_one_and_update(
            conn, "users", {"name": "alice"}, {"status": "updated"}
        )
        sql = cur.execute.call_args[0][0]
        assert "SET data = %s::jsonb, updated_at = NOW()" in sql
        assert "RETURNING" in sql

    def test_commits(self):
        conn, cur = capture_sql(fetchone_result=("uuid-1", {}, "ts"))
        doc_find_one_and_update(conn, "users", {}, {"a": 1})
        conn.commit.assert_called_once()

    def test_invalid_collection_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid identifier"):
            doc_find_one_and_update(conn, "bad; name", {}, {"a": 1})


# ---------------------------------------------------------------------------
# 22. doc_find_one_and_delete()
# ---------------------------------------------------------------------------

class TestDocFindOneAndDelete:
    def test_returns_document(self):
        conn, cur = capture_sql(
            fetchone_result=("uuid-1", {"name": "alice"}, "2026-01-01"),
        )
        result = doc_find_one_and_delete(conn, "users", {"name": "alice"})
        sql = cur.execute.call_args[0][0]
        assert "WITH target AS" in sql
        assert "DELETE FROM" in sql
        assert "RETURNING" in sql
        assert result is not None
        assert result["_id"] == "uuid-1"

    def test_returns_none_no_match(self):
        conn, cur = capture_sql(fetchone_result=None)
        result = doc_find_one_and_delete(conn, "users", {"name": "nobody"})
        assert result is None

    def test_commits(self):
        conn, cur = capture_sql(fetchone_result=("uuid-1", {}, "ts"))
        doc_find_one_and_delete(conn, "users", {"name": "alice"})
        conn.commit.assert_called_once()

    def test_invalid_collection_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid identifier"):
            doc_find_one_and_delete(conn, "bad; name", {})


# ---------------------------------------------------------------------------
# 23. doc_distinct()
# ---------------------------------------------------------------------------

class TestDocDistinct:
    def test_basic_distinct(self):
        conn, cur = capture_sql(fetchall_result=[('"active"',), ("3",)])
        result = doc_distinct(conn, "users", "status")
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert sql == "SELECT DISTINCT v::text FROM users, jsonb_path_query(data, %s::text::jsonpath) AS v"
        # An array's elements, each value as its type
        assert params == ('$."status"[*]',)
        assert result == ["active", 3]

    def test_dot_notation(self):
        conn, cur = capture_sql(fetchall_result=[])
        doc_distinct(conn, "users", "address.city")
        assert cur.execute.call_args[0][1] == ('$."address"."city"[*]',)

    def test_with_filter(self):
        conn, cur = capture_sql(fetchall_result=[])
        doc_distinct(conn, "users", "status", filter={"age": {"$gt": 25}})
        sql = cur.execute.call_args[0][0]
        assert sql.endswith(" WHERE data @? %s::text::jsonpath")
        assert cur.execute.call_args[0][1] == ('$."status"[*]', '$."age" ? (@ > 25)')

    def test_invalid_field_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid field key"):
            doc_distinct(conn, "users", "bad;field")

    def test_invalid_collection_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid identifier"):
            doc_distinct(conn, "bad; name", "status")


# ---------------------------------------------------------------------------
# 24. Helper function tests
# ---------------------------------------------------------------------------

class TestHelperFunctions:
    def test_jsonpath_single(self):
        assert _jsonpath("name") == '$."name"'

    def test_jsonpath_nested(self):
        assert _jsonpath("addr.city") == '$."addr"."city"'

    def test_jsonpath_invalid_raises(self):
        with pytest.raises(ValueError, match="Invalid field key"):
            _jsonpath("bad;key")

    def test_text_path_nested(self):
        assert _text_path("addr.city") == "'{addr,city}'"

    def test_text_path_invalid_raises(self):
        with pytest.raises(ValueError, match="Invalid field key"):
            _text_path("bad'key")


# ---------------------------------------------------------------------------
# 25. $elemMatch
# ---------------------------------------------------------------------------

class TestElemMatch:
    def test_numeric_range(self):
        clause, params = _build_filter({"scores": {"$elemMatch": {"$gt": 80, "$lt": 90}}})
        assert clause == "(jsonb_typeof(data #> '{scores}') = 'array' AND data @? %s::text::jsonpath)"
        # One element meets both
        assert params == ['$."scores"[*] ? ((@ > 80) && (@ < 90))']

    def test_string_regex(self):
        _, params = _build_filter({"tags": {"$elemMatch": {"$regex": "^py"}}})
        assert params == ['$."tags"[*] ? ((@ like_regex "^py"))']

    def test_fields_of_elements(self):
        _, params = _build_filter({"items": {"$elemMatch": {"sku": "p", "n": {"$gt": 5}}}})
        assert params == ['$."items"[*] ? (((@."sku" == "p")) && ((@."n" > 5)))']

    def test_invalid_operand_raises(self):
        with pytest.raises(ValueError, match="takes a filter"):
            _build_filter({"scores": {"$elemMatch": [1, 2]}})

    def test_unsupported_sub_op_raises(self):
        with pytest.raises(ValueError, match="Unsupported operator here"):
            _build_filter({"scores": {"$elemMatch": {"$foo": 1}}})

    def test_in_doc_find(self):
        conn, cur = capture_sql(fetchall_result=[])
        doc_find(conn, "users", filter={"scores": {"$elemMatch": {"$gt": 80}}})
        assert "jsonb_typeof(data #> '{scores}') = 'array'" in cur.execute.call_args[0][0]


# ---------------------------------------------------------------------------
# 26. $text in filters
# ---------------------------------------------------------------------------

class TestTextFilter:
    def test_top_level(self):
        clause, params = _build_filter({"$text": {"$search": "hello world"}})
        assert "to_tsvector" in clause
        assert "plainto_tsquery" in clause
        # The configuration is in the SQL, so GL's index on it serves the query
        assert "to_tsvector('english', data::text) @@ plainto_tsquery('english', %s)" in clause
        assert params == ["hello world"]

    def test_field_level(self):
        clause, params = _build_filter({"content": {"$text": {"$search": "hello"}}})
        assert "to_tsvector" in clause
        assert "plainto_tsquery" in clause
        assert "to_tsvector('english', data->>'content')" in clause
        assert params == ["hello"]

    def test_custom_language(self):
        clause, params = _build_filter({"$text": {"$search": "bonjour", "$language": "french"}})
        assert "to_tsvector" in clause
        assert "to_tsvector('french', data::text)" in clause
        assert params == ["bonjour"]

    def test_bad_language_raises(self):
        with pytest.raises(ValueError, match="text search configuration"):
            _build_filter({"$text": {"$search": "x", "$language": "english'); --"}})

    def test_missing_search_raises(self):
        with pytest.raises(ValueError, match="\\$text requires"):
            _build_filter({"$text": {"$language": "english"}})

    def test_non_dict_raises(self):
        with pytest.raises(ValueError, match="\\$text requires"):
            _build_filter({"$text": "hello"})

    def test_field_level_missing_search_raises(self):
        with pytest.raises(ValueError, match="\\$text requires"):
            _build_filter({"content": {"$text": {"$language": "english"}}})

    def test_in_doc_find(self):
        conn, cur = capture_sql(fetchall_result=[])
        doc_find(conn, "users", filter={"$text": {"$search": "hello"}})
        sql = cur.execute.call_args[0][0]
        assert "to_tsvector" in sql
        assert "@@" in sql

    def test_in_doc_count(self):
        conn, cur = capture_sql(fetchone_result=(3,))
        doc_count(conn, "users", filter={"bio": {"$text": {"$search": "python"}}})
        sql = cur.execute.call_args[0][0]
        assert "to_tsvector" in sql


# ---------------------------------------------------------------------------
# 27. doc_find_cursor()
# ---------------------------------------------------------------------------

class TestDocFindCursor:
    def _make_cursor_conn(self, fetchmany_side_effect=None):
        cursor = MagicMock()
        cursor.description = [("_id",), ("data",), ("created_at",)]
        if fetchmany_side_effect is not None:
            cursor.fetchmany.side_effect = fetchmany_side_effect
        else:
            cursor.fetchmany.return_value = []
        conn = FakeConn(cursor)
        return conn, cursor

    def test_returns_generator(self):
        conn, cur = self._make_cursor_conn(fetchmany_side_effect=[
            [("id1", {"a": 1}, "ts"), ("id2", {"b": 2}, "ts")],
            [],
        ])
        gen = doc_find_cursor(conn, "users")
        import types
        assert isinstance(gen, types.GeneratorType)

    def test_yields_dicts(self):
        conn, cur = self._make_cursor_conn(fetchmany_side_effect=[
            [("id1", {"a": 1}, "ts1")],
            [],
        ])
        results = list(doc_find_cursor(conn, "users"))
        assert len(results) == 1
        assert results[0]["_id"] == "id1"
        assert results[0]["data"] == {"a": 1}

    def test_with_filter(self):
        conn, cur = self._make_cursor_conn()
        list(doc_find_cursor(conn, "users", filter={"status": "active"}))
        sql = cur.execute.call_args[0][0]
        assert "WHERE" in sql

    def test_batch_size(self):
        conn, cur = self._make_cursor_conn()
        list(doc_find_cursor(conn, "users", batch_size=50))
        cur.fetchmany.assert_called_with(50)

    def test_invalid_collection_raises(self):
        conn, _ = self._make_cursor_conn()
        with pytest.raises(ValueError, match="Invalid identifier"):
            list(doc_find_cursor(conn, "bad; name"))

    def test_closes_cursor(self):
        conn, cur = self._make_cursor_conn()
        list(doc_find_cursor(conn, "users"))
        cur.close.assert_called_once()
