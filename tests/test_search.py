from unittest.mock import MagicMock, call
import json

import pytest

from goldlapel.utils import (
    search,
    search_fuzzy,
    search_phonetic,
    similar,
    suggest,
    facets,
    aggregate,
    create_search_config,
    percolate_add,
    percolate,
    percolate_delete,
    analyze,
    explain_score,
)


class FakeConn:
    """A mock connection that does NOT have _conn, so _get_raw_connection returns self."""
    def __init__(self, cursor):
        self._cursor = cursor
        self.commit = MagicMock()

    def cursor(self):
        return self._cursor


def capture_sql(fetchall_result=None, fetchone_result=None, description=None):
    cursor = MagicMock()
    cursor.description = description or [("id",), ("name",)]
    cursor.fetchall.return_value = fetchall_result or []
    cursor.fetchone.return_value = fetchone_result
    conn = FakeConn(cursor)
    return conn, cursor


# ---------------------------------------------------------------------------
# 1. search() — full-text search
# ---------------------------------------------------------------------------

class TestSearch:
    def test_basic_single_column(self):
        conn, cur = capture_sql(
            description=[("id",), ("title",), ("_score",)],
            fetchall_result=[(1, "hello world", 0.5)],
        )
        results = search(conn, "articles", "title", "hello")
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "to_tsvector" in sql
        assert "plainto_tsquery" in sql
        assert "ts_rank" in sql
        assert "FROM articles" in sql
        assert "ORDER BY _score DESC" in sql
        assert params == ("english", "english", "hello", "english", "english", "hello", 50)
        assert len(results) == 1
        assert results[0]["_score"] == 0.5

    def test_multi_column_coalesce(self):
        conn, cur = capture_sql(
            description=[("id",), ("_score",)],
            fetchall_result=[],
        )
        search(conn, "articles", ["title", "body"], "test")
        sql = cur.execute.call_args[0][0]
        assert "coalesce(title, '')" in sql
        assert "coalesce(body, '')" in sql
        assert "|| ' ' ||" in sql

    def test_custom_lang_and_limit(self):
        conn, cur = capture_sql()
        search(conn, "articles", "title", "hola", limit=10, lang="spanish")
        params = cur.execute.call_args[0][1]
        assert params[0] == "spanish"
        assert params[-1] == 10

    def test_highlight(self):
        conn, cur = capture_sql(
            description=[("id",), ("title",), ("_score",), ("_highlight",)],
            fetchall_result=[(1, "hello world", 0.5, "<mark>hello</mark> world")],
        )
        results = search(conn, "articles", "title", "hello", highlight=True)
        sql = cur.execute.call_args[0][0]
        assert "ts_headline" in sql
        assert "StartSel=<mark>" in sql
        assert "_highlight" in sql
        assert results[0]["_highlight"] == "<mark>hello</mark> world"

    def test_highlight_multi_column_uses_first(self):
        conn, cur = capture_sql(
            description=[("id",), ("_score",), ("_highlight",)],
            fetchall_result=[],
        )
        search(conn, "articles", ["title", "body"], "test", highlight=True)
        sql = cur.execute.call_args[0][0]
        # ts_headline should use the first column (title), not body
        assert "ts_headline(%s, title, plainto_tsquery" in sql

    def test_returns_list_of_dicts(self):
        conn, cur = capture_sql(
            description=[("id",), ("name",), ("_score",)],
            fetchall_result=[(1, "alice", 0.9), (2, "bob", 0.3)],
        )
        results = search(conn, "docs", "name", "alice")
        assert isinstance(results, list)
        assert len(results) == 2
        assert results[0] == {"id": 1, "name": "alice", "_score": 0.9}

    def test_invalid_table_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid identifier"):
            search(conn, "DROP TABLE; --", "title", "x")

    def test_invalid_column_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid identifier"):
            search(conn, "articles", "col; DROP", "x")


# ---------------------------------------------------------------------------
# 2. search_fuzzy() — trigram similarity
# ---------------------------------------------------------------------------

class TestSearchFuzzy:
    def test_basic(self):
        conn, cur = capture_sql(
            description=[("id",), ("name",), ("_score",)],
            fetchall_result=[(1, "john", 0.7)],
        )
        results = search_fuzzy(conn, "users", "name", "jon")
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "similarity(name, %s)" in sql
        assert "FROM users" in sql
        assert params == ("jon", "jon", 0.3, 50)
        assert results[0]["_score"] == 0.7

    def test_custom_threshold_and_limit(self):
        conn, cur = capture_sql()
        search_fuzzy(conn, "users", "name", "jon", limit=5, threshold=0.5)
        # Second execute call is the main query
        params = cur.execute.call_args[0][1]
        assert params == ("jon", "jon", 0.5, 5)

    def test_invalid_identifier_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid identifier"):
            search_fuzzy(conn, "users", "name; DROP", "x")


# ---------------------------------------------------------------------------
# 3. search_phonetic() — soundex/dmetaphone
# ---------------------------------------------------------------------------

class TestSearchPhonetic:
    def test_basic(self):
        conn, cur = capture_sql(
            description=[("id",), ("name",), ("_score",)],
            fetchall_result=[(1, "smith", 0.6)],
        )
        results = search_phonetic(conn, "users", "name", "smythe")
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "soundex(name) = soundex(%s)" in sql
        assert "similarity(name, %s)" in sql
        assert params == ("smythe", "smythe", 50)

    def test_custom_limit(self):
        conn, cur = capture_sql()
        search_phonetic(conn, "users", "name", "smythe", limit=5)
        params = cur.execute.call_args[0][1]
        assert params[-1] == 5

    def test_returns_results_ordered_by_score(self):
        conn, cur = capture_sql(
            description=[("id",), ("name",), ("_score",)],
            fetchall_result=[(1, "smith", 0.9), (2, "smyth", 0.7)],
        )
        results = search_phonetic(conn, "users", "name", "smith")
        sql = cur.execute.call_args[0][0]
        assert "ORDER BY _score DESC, name" in sql
        assert len(results) == 2


# ---------------------------------------------------------------------------
# 4. similar() — vector similarity
# ---------------------------------------------------------------------------

class TestSimilar:
    def test_basic(self):
        conn, cur = capture_sql(
            description=[("id",), ("embedding",), ("_score",)],
            fetchall_result=[(1, "[0.1,0.2]", 0.05)],
        )
        results = similar(conn, "docs", "embedding", [0.1, 0.2, 0.3])
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "<=>" in sql
        assert "::vector" in sql
        assert params[0] == "[0.1,0.2,0.3]"
        assert params[1] == 10

    def test_custom_limit(self):
        conn, cur = capture_sql()
        similar(conn, "docs", "embedding", [1.0, 2.0], limit=5)
        params = cur.execute.call_args[0][1]
        assert params[1] == 5

    def test_vector_literal_formatting(self):
        conn, cur = capture_sql()
        similar(conn, "docs", "embedding", [1, 2, 3])
        params = cur.execute.call_args[0][1]
        assert params[0] == "[1.0,2.0,3.0]"

    def test_empty_results(self):
        conn, cur = capture_sql(fetchall_result=[])
        results = similar(conn, "docs", "embedding", [0.1])
        assert results == []


# ---------------------------------------------------------------------------
# 5. suggest() — autocomplete
# ---------------------------------------------------------------------------

class TestSuggest:
    def test_basic(self):
        conn, cur = capture_sql(
            description=[("id",), ("name",), ("_score",)],
            fetchall_result=[(1, "new york", 0.8)],
        )
        results = suggest(conn, "cities", "name", "new")
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "ILIKE" in sql
        assert "similarity(name, %s)" in sql
        assert params == ("new", "new%", 10)

    def test_custom_limit(self):
        conn, cur = capture_sql()
        suggest(conn, "cities", "name", "new", limit=3)
        params = cur.execute.call_args[0][1]
        assert params[-1] == 3

    def test_ilike_pattern(self):
        conn, cur = capture_sql()
        suggest(conn, "cities", "name", "san fr")
        params = cur.execute.call_args[0][1]
        assert params[1] == "san fr%"


# ---------------------------------------------------------------------------
# 6. facets() — category counts
# ---------------------------------------------------------------------------

class TestFacets:
    def test_basic_no_query(self):
        conn, cur = capture_sql(
            description=[("value",), ("count",)],
            fetchall_result=[("electronics", 42), ("clothing", 15)],
        )
        results = facets(conn, "products", "category")
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "SELECT category AS value, COUNT(*) AS count" in sql
        assert "GROUP BY category" in sql
        assert "ORDER BY count DESC" in sql
        assert params == (50,)
        assert results == [
            {"value": "electronics", "count": 42},
            {"value": "clothing", "count": 15},
        ]

    def test_with_query_filter_single_column(self):
        conn, cur = capture_sql(
            description=[("value",), ("count",)],
            fetchall_result=[("electronics", 5)],
        )
        facets(conn, "products", "category", query="laptop", query_column="title")
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "to_tsvector" in sql
        assert "plainto_tsquery" in sql
        assert "coalesce(title, '')" in sql
        assert params == ("english", "english", "laptop", 50)

    def test_with_query_filter_multi_column(self):
        conn, cur = capture_sql(
            description=[("value",), ("count",)],
            fetchall_result=[],
        )
        facets(conn, "products", "category", query="laptop", query_column=["title", "description"])
        sql = cur.execute.call_args[0][0]
        assert "coalesce(title, '')" in sql
        assert "coalesce(description, '')" in sql
        assert "|| ' ' ||" in sql

    def test_custom_limit(self):
        conn, cur = capture_sql(
            description=[("value",), ("count",)],
            fetchall_result=[],
        )
        facets(conn, "products", "category", limit=5)
        params = cur.execute.call_args[0][1]
        assert params == (5,)

    def test_custom_lang(self):
        conn, cur = capture_sql(
            description=[("value",), ("count",)],
            fetchall_result=[],
        )
        facets(conn, "products", "category", query="laptop", query_column="title", lang="spanish")
        params = cur.execute.call_args[0][1]
        assert params[0] == "spanish"


# ---------------------------------------------------------------------------
# 7. aggregate() — metric aggregations
# ---------------------------------------------------------------------------

class TestAggregate:
    def test_sum(self):
        conn, cur = capture_sql(
            description=[("value",)],
            fetchall_result=[(1500,)],
        )
        results = aggregate(conn, "orders", "amount", "sum")
        sql = cur.execute.call_args[0][0]
        assert "SUM(amount) AS value" in sql
        assert results == [{"value": 1500}]

    def test_avg(self):
        conn, cur = capture_sql(
            description=[("value",)],
            fetchall_result=[(75.5,)],
        )
        aggregate(conn, "orders", "amount", "avg")
        sql = cur.execute.call_args[0][0]
        assert "AVG(amount) AS value" in sql

    def test_min(self):
        conn, cur = capture_sql(
            description=[("value",)],
            fetchall_result=[(10,)],
        )
        aggregate(conn, "orders", "amount", "min")
        sql = cur.execute.call_args[0][0]
        assert "MIN(amount) AS value" in sql

    def test_max(self):
        conn, cur = capture_sql(
            description=[("value",)],
            fetchall_result=[(999,)],
        )
        aggregate(conn, "orders", "amount", "max")
        sql = cur.execute.call_args[0][0]
        assert "MAX(amount) AS value" in sql

    def test_count_uses_star(self):
        conn, cur = capture_sql(
            description=[("value",)],
            fetchall_result=[(42,)],
        )
        aggregate(conn, "orders", "amount", "count")
        sql = cur.execute.call_args[0][0]
        assert "COUNT(*) AS value" in sql

    def test_group_by(self):
        conn, cur = capture_sql(
            description=[("category",), ("value",)],
            fetchall_result=[("electronics", 500), ("clothing", 300)],
        )
        results = aggregate(conn, "orders", "amount", "sum", group_by="category")
        sql = cur.execute.call_args[0][0]
        assert "GROUP BY category" in sql
        assert "SELECT category, SUM(amount) AS value" in sql
        assert "ORDER BY value DESC" in sql
        assert len(results) == 2

    def test_group_by_limit(self):
        conn, cur = capture_sql(
            description=[("category",), ("value",)],
            fetchall_result=[],
        )
        aggregate(conn, "orders", "amount", "sum", group_by="category", limit=3)
        params = cur.execute.call_args[0][1]
        assert params == (3,)

    def test_invalid_func_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="func must be one of"):
            aggregate(conn, "orders", "amount", "median")

    def test_no_group_by_no_limit(self):
        conn, cur = capture_sql(
            description=[("value",)],
            fetchall_result=[(100,)],
        )
        aggregate(conn, "orders", "amount", "sum")
        sql = cur.execute.call_args[0][0]
        assert "GROUP BY" not in sql
        assert "LIMIT" not in sql


# ---------------------------------------------------------------------------
# 8. create_search_config() — custom text search config
# ---------------------------------------------------------------------------

class TestCreateSearchConfig:
    def test_creates_config_when_not_exists(self):
        conn, cur = capture_sql(fetchone_result=None)
        create_search_config(conn, "my_config")
        calls = [c[0][0] for c in cur.execute.call_args_list]
        assert any("pg_ts_config" in c for c in calls)
        assert any("CREATE TEXT SEARCH CONFIGURATION my_config (COPY = english)" in c for c in calls)

    def test_skips_if_already_exists(self):
        conn, cur = capture_sql(fetchone_result=(1,))
        create_search_config(conn, "my_config")
        calls = [c[0][0] for c in cur.execute.call_args_list]
        assert not any("CREATE TEXT SEARCH CONFIGURATION" in c for c in calls)

    def test_custom_copy_from(self):
        conn, cur = capture_sql(fetchone_result=None)
        create_search_config(conn, "my_spanish", copy_from="spanish")
        calls = [c[0][0] for c in cur.execute.call_args_list]
        assert any("COPY = spanish" in c for c in calls)

    def test_invalid_name_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid identifier"):
            create_search_config(conn, "bad; name")

    def test_checks_pg_ts_config(self):
        conn, cur = capture_sql(fetchone_result=None)
        create_search_config(conn, "myconfig")
        check_call = cur.execute.call_args_list[0]
        assert "pg_ts_config" in check_call[0][0]
        assert check_call[0][1] == ("myconfig",)


# ---------------------------------------------------------------------------
# 9. percolate_add() — register percolator query
# ---------------------------------------------------------------------------

class TestPercolateAdd:
    def test_basic(self):
        conn, cur = capture_sql()
        percolate_add(conn, "alerts", "q1", "breaking news")
        calls = [c[0][0] for c in cur.execute.call_args_list]
        # Should create table, create index, then insert
        assert any("CREATE TABLE IF NOT EXISTS alerts" in c for c in calls)
        assert any("CREATE INDEX IF NOT EXISTS alerts_tsq_idx" in c for c in calls)
        insert_call = [c for c in cur.execute.call_args_list if "INSERT INTO alerts" in c[0][0]]
        assert len(insert_call) == 1
        params = insert_call[0][0][1]
        assert params == ("q1", "breaking news", "english", "breaking news", "english", None)

    def test_with_metadata(self):
        conn, cur = capture_sql()
        meta = {"priority": "high", "channel": "email"}
        percolate_add(conn, "alerts", "q1", "breaking news", metadata=meta)
        insert_call = [c for c in cur.execute.call_args_list if "INSERT INTO alerts" in c[0][0]]
        params = insert_call[0][0][1]
        assert params[-1] == json.dumps(meta)

    def test_custom_lang(self):
        conn, cur = capture_sql()
        percolate_add(conn, "alerts", "q1", "noticias", lang="spanish")
        insert_call = [c for c in cur.execute.call_args_list if "INSERT INTO alerts" in c[0][0]]
        params = insert_call[0][0][1]
        assert params[2] == "spanish"
        assert params[4] == "spanish"

    def test_upsert_on_conflict(self):
        conn, cur = capture_sql()
        percolate_add(conn, "alerts", "q1", "news")
        insert_call = [c for c in cur.execute.call_args_list if "INSERT INTO alerts" in c[0][0]]
        sql = insert_call[0][0][0]
        assert "ON CONFLICT (query_id) DO UPDATE" in sql

    def test_commits(self):
        conn, cur = capture_sql()
        percolate_add(conn, "alerts", "q1", "news")
        conn.commit.assert_called_once()


# ---------------------------------------------------------------------------
# 10. percolate() — match document against stored queries
# ---------------------------------------------------------------------------

class TestPercolate:
    def test_basic(self):
        conn, cur = capture_sql(
            description=[("query_id",), ("query_text",), ("metadata",), ("_score",)],
            fetchall_result=[("q1", "breaking news", None, 0.8)],
        )
        results = percolate(conn, "alerts", "Breaking news from around the world")
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "FROM alerts" in sql
        assert "to_tsvector(%s, %s) @@ tsquery" in sql
        assert "ts_rank" in sql
        assert params == ("english", "Breaking news from around the world",
                         "english", "Breaking news from around the world", 50)

    def test_custom_lang_and_limit(self):
        conn, cur = capture_sql(
            description=[("query_id",), ("query_text",), ("metadata",), ("_score",)],
            fetchall_result=[],
        )
        percolate(conn, "alerts", "noticias del mundo", lang="spanish", limit=5)
        params = cur.execute.call_args[0][1]
        assert params[0] == "spanish"
        assert params[-1] == 5

    def test_returns_list_of_dicts(self):
        conn, cur = capture_sql(
            description=[("query_id",), ("query_text",), ("metadata",), ("_score",)],
            fetchall_result=[
                ("q1", "news", {"channel": "email"}, 0.9),
                ("q2", "update", None, 0.3),
            ],
        )
        results = percolate(conn, "alerts", "some news update")
        assert len(results) == 2
        assert results[0]["query_id"] == "q1"
        assert results[0]["metadata"] == {"channel": "email"}
        assert results[1]["metadata"] is None


# ---------------------------------------------------------------------------
# 11. percolate_delete() — remove stored query
# ---------------------------------------------------------------------------

class TestPercolateDelete:
    def test_deletes_existing(self):
        conn, cur = capture_sql(fetchone_result=("q1",))
        result = percolate_delete(conn, "alerts", "q1")
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "DELETE FROM alerts WHERE query_id = %s" in sql
        assert "RETURNING query_id" in sql
        assert params == ("q1",)
        assert result is True

    def test_returns_false_when_not_found(self):
        conn, cur = capture_sql(fetchone_result=None)
        result = percolate_delete(conn, "alerts", "nonexistent")
        assert result is False

    def test_commits(self):
        conn, cur = capture_sql(fetchone_result=("q1",))
        percolate_delete(conn, "alerts", "q1")
        conn.commit.assert_called_once()

    def test_invalid_name_raises(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid identifier"):
            percolate_delete(conn, "bad; name", "q1")


# ---------------------------------------------------------------------------
# 12. analyze() — tokenization debug
# ---------------------------------------------------------------------------

class TestAnalyze:
    def test_basic(self):
        conn, cur = capture_sql(
            description=[("alias",), ("description",), ("token",), ("dictionaries",), ("dictionary",), ("lexemes",)],
            fetchall_result=[
                ("asciiword", "Word, all ASCII", "hello", "{english_stem}", "english_stem", "{hello}"),
                ("blank", "Space symbols", " ", "{}", None, "{}"),
                ("asciiword", "Word, all ASCII", "world", "{english_stem}", "english_stem", "{world}"),
            ],
        )
        results = analyze(conn, "hello world")
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "ts_debug" in sql
        assert params == ("english", "hello world")
        assert len(results) == 3
        assert results[0]["token"] == "hello"
        assert results[2]["token"] == "world"

    def test_custom_lang(self):
        conn, cur = capture_sql(
            description=[("alias",), ("description",), ("token",), ("dictionaries",), ("dictionary",), ("lexemes",)],
            fetchall_result=[],
        )
        analyze(conn, "hola mundo", lang="spanish")
        params = cur.execute.call_args[0][1]
        assert params == ("spanish", "hola mundo")

    def test_returns_list_of_dicts(self):
        conn, cur = capture_sql(
            description=[("alias",), ("description",), ("token",), ("dictionaries",), ("dictionary",), ("lexemes",)],
            fetchall_result=[("asciiword", "Word", "test", "{}", None, "{test}")],
        )
        results = analyze(conn, "test")
        assert isinstance(results, list)
        assert all(isinstance(r, dict) for r in results)
        assert set(results[0].keys()) == {"alias", "description", "token", "dictionaries", "dictionary", "lexemes"}


# ---------------------------------------------------------------------------
# 13. explain_score() — relevance debug
# ---------------------------------------------------------------------------

class TestExplainScore:
    def test_basic(self):
        conn, cur = capture_sql(
            description=[
                ("document_text",), ("document_tokens",), ("query_tokens",),
                ("matches",), ("score",), ("headline",),
            ],
            fetchone_result=(
                "hello world",
                "'hello':1 'world':2",
                "'hello'",
                True,
                0.0607927,
                "**hello** world",
            ),
        )
        result = explain_score(conn, "articles", "body", "hello", "id", 42)
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "to_tsvector(%s, body)::text AS document_tokens" in sql
        assert "plainto_tsquery(%s, %s)::text AS query_tokens" in sql
        assert "ts_rank" in sql
        assert "ts_headline" in sql
        assert "WHERE id = %s" in sql
        assert params == (
            "english", "english", "hello", "english", "english", "hello",
            "english", "english", "hello", "english", "english", "hello", 42,
        )
        assert result["matches"] is True
        assert result["score"] == 0.0607927

    def test_returns_none_when_row_not_found(self):
        conn, cur = capture_sql(fetchone_result=None)
        result = explain_score(conn, "articles", "body", "hello", "id", 999)
        assert result is None

    def test_custom_lang(self):
        conn, cur = capture_sql(
            description=[
                ("document_text",), ("document_tokens",), ("query_tokens",),
                ("matches",), ("score",), ("headline",),
            ],
            fetchone_result=("hola", "'hola':1", "'hola'", True, 0.06, "**hola**"),
        )
        explain_score(conn, "articles", "body", "hola", "id", 1, lang="spanish")
        params = cur.execute.call_args[0][1]
        assert params[0] == "spanish"

    def test_returns_dict_with_all_fields(self):
        conn, cur = capture_sql(
            description=[
                ("document_text",), ("document_tokens",), ("query_tokens",),
                ("matches",), ("score",), ("headline",),
            ],
            fetchone_result=("text", "tokens", "qtokens", True, 0.5, "hl"),
        )
        result = explain_score(conn, "articles", "body", "test", "id", 1)
        assert set(result.keys()) == {
            "document_text", "document_tokens", "query_tokens",
            "matches", "score", "headline",
        }

    def test_invalid_identifiers_raise(self):
        conn, _ = capture_sql()
        with pytest.raises(ValueError, match="Invalid identifier"):
            explain_score(conn, "bad; table", "body", "x", "id", 1)
        with pytest.raises(ValueError, match="Invalid identifier"):
            explain_score(conn, "articles", "bad; col", "x", "id", 1)
        with pytest.raises(ValueError, match="Invalid identifier"):
            explain_score(conn, "articles", "body", "x", "bad; id", 1)


# ---------------------------------------------------------------------------
# Parameter binding safety — verify no string interpolation of user values
# ---------------------------------------------------------------------------

class TestParameterBinding:
    def test_search_query_is_parameterized(self):
        conn, cur = capture_sql()
        search(conn, "articles", "title", "'; DROP TABLE articles; --")
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        # The malicious query string should be in params, not in the SQL
        assert "DROP TABLE" not in sql
        assert "'; DROP TABLE articles; --" in params

    def test_search_fuzzy_query_is_parameterized(self):
        conn, cur = capture_sql()
        search_fuzzy(conn, "users", "name", "'; DROP TABLE users; --")
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "DROP TABLE" not in sql
        assert "'; DROP TABLE users; --" in params

    def test_percolate_text_is_parameterized(self):
        conn, cur = capture_sql(
            description=[("query_id",), ("query_text",), ("metadata",), ("_score",)],
            fetchall_result=[],
        )
        percolate(conn, "alerts", "'; DROP TABLE alerts; --")
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "DROP TABLE" not in sql
        assert "'; DROP TABLE alerts; --" in params

    def test_explain_score_id_value_is_parameterized(self):
        conn, cur = capture_sql(fetchone_result=None)
        explain_score(conn, "articles", "body", "test", "id", "1; DROP TABLE articles")
        params = cur.execute.call_args[0][1]
        assert "1; DROP TABLE articles" in params

    def test_suggest_prefix_is_parameterized(self):
        conn, cur = capture_sql()
        suggest(conn, "cities", "name", "'; DROP TABLE cities; --")
        sql = cur.execute.call_args[0][0]
        assert "DROP TABLE" not in sql


# ---------------------------------------------------------------------------
# _get_raw_connection — wrapper unwrapping
# ---------------------------------------------------------------------------

class TestGetRawConnection:
    def test_unwraps_conn_attribute(self):
        inner_cursor = MagicMock()
        inner_cursor.description = [("id",)]
        inner_cursor.fetchall.return_value = []
        inner = MagicMock()
        inner.cursor.return_value = inner_cursor

        class WrappedConn:
            def __init__(self, raw):
                self._conn = raw
        outer = WrappedConn(inner)

        search(outer, "articles", "title", "test")
        inner.cursor.assert_called_once()

    def test_plain_connection_passes_through(self):
        conn, cur = capture_sql()
        search(conn, "articles", "title", "test")
        assert cur.execute.called
