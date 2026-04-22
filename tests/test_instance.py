from unittest.mock import MagicMock, patch, call

import pytest

from goldlapel.proxy import GoldLapel


class TestConnProperty:
    def test_raises_before_start(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        with pytest.raises(RuntimeError, match="Not connected. Call start"):
            gl.conn

    def test_returns_conn_when_set(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        fake_conn = MagicMock()
        gl._conn = fake_conn
        assert gl.conn is fake_conn


class TestStopClosesConnection:
    def test_stop_closes_connection(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        conn = MagicMock()
        gl._conn = conn
        gl.stop()
        conn.close.assert_called_once()
        assert gl._conn is None

    def test_stop_without_connection_is_safe(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        gl.stop()  # should not raise
        assert gl._conn is None

    def test_stop_swallows_close_exception(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        conn = MagicMock()
        conn.close.side_effect = Exception("already closed")
        gl._conn = conn
        gl.stop()  # should not raise
        assert gl._conn is None


class TestInstanceMethodDelegation:
    def setup_method(self):
        self.gl = GoldLapel("postgresql://localhost:5432/mydb")
        self.fake_conn = MagicMock()
        self.gl._conn = self.fake_conn

    @patch("goldlapel.proxy._utils")
    def test_doc_insert(self, mock_utils):
        mock_utils.return_value.doc_insert.return_value = {"_id": "abc"}
        result = self.gl.doc_insert("users", {"name": "alice"})
        mock_utils.return_value.doc_insert.assert_called_once_with(
            self.fake_conn, "users", {"name": "alice"}
        )
        assert result == {"_id": "abc"}

    @patch("goldlapel.proxy._utils")
    def test_doc_insert_many(self, mock_utils):
        mock_utils.return_value.doc_insert_many.return_value = [{"_id": "1"}, {"_id": "2"}]
        result = self.gl.doc_insert_many("items", [{"a": 1}, {"b": 2}])
        mock_utils.return_value.doc_insert_many.assert_called_once_with(
            self.fake_conn, "items", [{"a": 1}, {"b": 2}]
        )
        assert len(result) == 2

    @patch("goldlapel.proxy._utils")
    def test_search(self, mock_utils):
        mock_utils.return_value.search.return_value = [{"title": "hit"}]
        result = self.gl.search("articles", "body", "query", limit=10)
        mock_utils.return_value.search.assert_called_once_with(
            self.fake_conn, "articles", "body", "query", limit=10
        )
        assert result == [{"title": "hit"}]

    @patch("goldlapel.proxy._utils")
    def test_publish(self, mock_utils):
        self.gl.publish("orders", "new order")
        mock_utils.return_value.publish.assert_called_once_with(
            self.fake_conn, "orders", "new order"
        )

    @patch("goldlapel.proxy._utils")
    def test_incr(self, mock_utils):
        mock_utils.return_value.incr.return_value = 42
        result = self.gl.incr("counters", "page_views")
        mock_utils.return_value.incr.assert_called_once_with(
            self.fake_conn, "counters", "page_views"
        )
        assert result == 42

    @patch("goldlapel.proxy._utils")
    def test_hset(self, mock_utils):
        self.gl.hset("cache", "user:1", "name", "alice")
        mock_utils.return_value.hset.assert_called_once_with(
            self.fake_conn, "cache", "user:1", "name", "alice"
        )

    @patch("goldlapel.proxy._utils")
    def test_zadd(self, mock_utils):
        self.gl.zadd("leaderboard", "player1", 100)
        mock_utils.return_value.zadd.assert_called_once_with(
            self.fake_conn, "leaderboard", "player1", 100
        )

    @patch("goldlapel.ddl.fetch")
    @patch("goldlapel.proxy._utils")
    def test_stream_add(self, mock_utils, mock_ddl_fetch):
        # Stream DDL patterns now come from the proxy's /api/ddl/* endpoint.
        # The utils function gets the patterns via kwargs — tests mock both layers.
        fake_patterns = {
            "tables": {"main": "_goldlapel.stream_events"},
            "query_patterns": {"insert": "INSERT ..."},
        }
        mock_ddl_fetch.return_value = fake_patterns
        mock_utils.return_value.stream_add.return_value = 1
        # Provide a dashboard token so the ddl layer is willing to run.
        self.gl._dashboard_token = "test-token"
        result = self.gl.stream_add("events", {"type": "click"})
        mock_ddl_fetch.assert_called_once_with(
            self.gl, "stream", "events", self.gl._dashboard_port, "test-token",
        )
        mock_utils.return_value.stream_add.assert_called_once_with(
            self.fake_conn, "events", {"type": "click"}, patterns=fake_patterns,
        )
        assert result == 1

    @patch("goldlapel.proxy._utils")
    def test_percolate(self, mock_utils):
        mock_utils.return_value.percolate.return_value = [{"query_id": "q1"}]
        result = self.gl.percolate("alerts", "breaking news")
        mock_utils.return_value.percolate.assert_called_once_with(
            self.fake_conn, "alerts", "breaking news"
        )
        assert result == [{"query_id": "q1"}]

    @patch("goldlapel.proxy._utils")
    def test_analyze(self, mock_utils):
        mock_utils.return_value.analyze.return_value = [{"token": "hello"}]
        result = self.gl.analyze("hello world")
        mock_utils.return_value.analyze.assert_called_once_with(
            self.fake_conn, "hello world"
        )
        assert result == [{"token": "hello"}]


class TestMethodNotConnected:
    def test_method_raises_before_start(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        with pytest.raises(RuntimeError, match="Not connected"):
            gl.doc_insert("users", {"name": "alice"})

    def test_search_raises_before_start(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        with pytest.raises(RuntimeError, match="Not connected"):
            gl.search("articles", "body", "query")


class TestAllMethodsExist:
    def test_all_util_methods_present(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        expected = [
            "doc_insert", "doc_insert_many", "doc_find", "doc_find_one",
            "doc_update", "doc_update_one", "doc_delete", "doc_delete_one",
            "doc_count", "doc_create_index", "doc_aggregate",
            "search", "search_fuzzy", "search_phonetic", "similar", "suggest",
            "facets", "aggregate", "create_search_config",
            "publish", "subscribe", "enqueue", "dequeue",
            "incr", "get_counter",
            "hset", "hget", "hgetall", "hdel",
            "zadd", "zincrby", "zrange", "zrank", "zscore", "zrem",
            "georadius", "geoadd", "geodist",
            "count_distinct", "script",
            "stream_add", "stream_create_group", "stream_read",
            "stream_ack", "stream_claim",
            "percolate_add", "percolate", "percolate_delete",
            "analyze", "explain_score",
        ]
        for method_name in expected:
            assert hasattr(gl, method_name), f"Missing method: {method_name}"
            assert callable(getattr(gl, method_name)), f"Not callable: {method_name}"
