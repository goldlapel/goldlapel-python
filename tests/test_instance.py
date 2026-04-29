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

    @patch("goldlapel.ddl.fetch_patterns")
    @patch("goldlapel.documents.utils", create=True)  # _u in documents.py is a per-call import
    def test_doc_insert(self, mock_utils, mock_ddl_fetch):
        # Doc DDL patterns now come from the proxy's /api/ddl/doc_store/* endpoint.
        from goldlapel import utils as real_utils
        fake_patterns = {
            "tables": {"main": "_goldlapel.doc_users"},
            "query_patterns": {"insert": "INSERT ..."},
        }
        mock_ddl_fetch.return_value = fake_patterns
        # Patch the lazy `from goldlapel import utils as _u` inside DocumentsAPI
        # by replacing the function on the actual utils module instead.
        with patch.object(real_utils, "doc_insert", return_value={"_id": "abc"}) as m:
            self.gl._dashboard_token = "test-token"
            result = self.gl.documents.insert("users", {"name": "alice"})
            mock_ddl_fetch.assert_called_once_with(
                self.gl, "doc_store", "users", self.gl._dashboard_port, "test-token",
                options=None,
            )
            m.assert_called_once_with(
                self.fake_conn, "users", {"name": "alice"}, patterns=fake_patterns,
            )
            assert result == {"_id": "abc"}

    @patch("goldlapel.ddl.fetch_patterns")
    def test_doc_insert_many(self, mock_ddl_fetch):
        from goldlapel import utils as real_utils
        fake_patterns = {
            "tables": {"main": "_goldlapel.doc_items"},
            "query_patterns": {"insert": "INSERT ..."},
        }
        mock_ddl_fetch.return_value = fake_patterns
        with patch.object(
            real_utils, "doc_insert_many",
            return_value=[{"_id": "1"}, {"_id": "2"}],
        ) as m:
            self.gl._dashboard_token = "test-token"
            result = self.gl.documents.insert_many("items", [{"a": 1}, {"b": 2}])
            m.assert_called_once_with(
                self.fake_conn, "items", [{"a": 1}, {"b": 2}], patterns=fake_patterns,
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

    @patch("goldlapel.ddl.fetch_patterns")
    def test_counter_incr_via_namespace(self, mock_ddl_fetch):
        # Phase 5: counter DDL is proxy-owned; helper methods live on
        # gl.counters and dispatch through goldlapel.utils.counter_*.
        from goldlapel import utils as real_utils
        fake_patterns = {
            "tables": {"main": "_goldlapel.counter_pageviews"},
            "query_patterns": {
                "incr": "INSERT INTO _goldlapel.counter_pageviews ... RETURNING value",
            },
        }
        mock_ddl_fetch.return_value = fake_patterns
        with patch.object(real_utils, "counter_incr", return_value=42) as m:
            self.gl._dashboard_token = "test-token"
            result = self.gl.counters.incr("pageviews", "home")
            mock_ddl_fetch.assert_called_once_with(
                self.gl, "counter", "pageviews", self.gl._dashboard_port, "test-token",
            )
            m.assert_called_once_with(
                self.fake_conn, "pageviews", "home", 1, patterns=fake_patterns,
            )
            assert result == 42

    @patch("goldlapel.ddl.fetch_patterns")
    def test_hash_set_via_namespace(self, mock_ddl_fetch):
        from goldlapel import utils as real_utils
        fake_patterns = {
            "tables": {"main": "_goldlapel.hash_sessions"},
            "query_patterns": {"hset": "..."},
        }
        mock_ddl_fetch.return_value = fake_patterns
        with patch.object(real_utils, "hash_set", return_value="alice") as m:
            self.gl._dashboard_token = "test-token"
            self.gl.hashes.set("sessions", "user:1", "name", "alice")
            m.assert_called_once_with(
                self.fake_conn, "sessions", "user:1", "name", "alice",
                patterns=fake_patterns,
            )

    @patch("goldlapel.ddl.fetch_patterns")
    def test_zset_add_via_namespace(self, mock_ddl_fetch):
        from goldlapel import utils as real_utils
        fake_patterns = {
            "tables": {"main": "_goldlapel.zset_leaderboard"},
            "query_patterns": {"zadd": "..."},
        }
        mock_ddl_fetch.return_value = fake_patterns
        with patch.object(real_utils, "zset_add", return_value=100.0) as m:
            self.gl._dashboard_token = "test-token"
            # Phase 5: zset_key is the new first positional after the
            # namespace name (matches Redis ZADD semantics).
            self.gl.zsets.add("leaderboard", "global", "player1", 100)
            m.assert_called_once_with(
                self.fake_conn, "leaderboard", "global", "player1", 100,
                patterns=fake_patterns,
            )

    @patch("goldlapel.ddl.fetch_patterns")
    def test_stream_add(self, mock_ddl_fetch):
        from goldlapel import utils as real_utils
        # Stream DDL patterns now come from the proxy's /api/ddl/* endpoint.
        # The utils function gets the patterns via kwargs — tests mock both layers.
        fake_patterns = {
            "tables": {"main": "_goldlapel.stream_events"},
            "query_patterns": {"insert": "INSERT ..."},
        }
        mock_ddl_fetch.return_value = fake_patterns
        with patch.object(real_utils, "stream_add", return_value=1) as m:
            # Provide a dashboard token so the ddl layer is willing to run.
            self.gl._dashboard_token = "test-token"
            result = self.gl.streams.add("events", {"type": "click"})
            mock_ddl_fetch.assert_called_once_with(
                self.gl, "stream", "events", self.gl._dashboard_port, "test-token",
            )
            m.assert_called_once_with(
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
        # gl.documents.insert needs both a token (which it doesn't have)
        # and a connection (which it doesn't have). The token check fires
        # first now since DDL patterns are fetched before the conn is touched.
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        # Without a dashboard token, .insert raises before reaching the conn.
        with pytest.raises(RuntimeError):
            gl.documents.insert("users", {"name": "alice"})

    def test_search_raises_before_start(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        with pytest.raises(RuntimeError, match="Not connected"):
            gl.search("articles", "body", "query")


class TestAllMethodsExist:
    def test_all_flat_util_methods_present(self):
        # Flat namespaces (search / pub-sub / percolator / analysis) stay as
        # direct methods on GoldLapel until their own schema-to-core phase
        # nests them too. Phase 5 (counter / zset / hash / queue / geo) is
        # nested — see TestPhase5Namespaces below.
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        expected = [
            "search", "search_fuzzy", "search_phonetic", "similar", "suggest",
            "facets", "aggregate", "create_search_config",
            "publish", "subscribe",
            "count_distinct", "script",
            "percolate_add", "percolate", "percolate_delete",
            "analyze", "explain_score",
        ]
        for method_name in expected:
            assert hasattr(gl, method_name), f"Missing method: {method_name}"
            assert callable(getattr(gl, method_name)), f"Not callable: {method_name}"

    def test_legacy_phase5_flat_methods_are_gone(self):
        # Phase 5 (2026-04-30) removed every flat counter/zset/hash/queue/geo
        # method on the GoldLapel class. Hard cut, no aliases — callers
        # migrate once and use the namespaced sub-APIs from now on.
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        for legacy in [
            "incr", "get_counter",
            "hset", "hget", "hgetall", "hdel",
            "zadd", "zincrby", "zrange", "zrank", "zscore", "zrem",
            "geoadd", "geodist", "georadius",
            "enqueue", "dequeue",
        ]:
            assert not hasattr(gl, legacy), (
                f"Phase 5 removed flat method {legacy} — use the namespaced "
                f"sub-API (gl.counters / gl.zsets / gl.hashes / gl.queues / "
                f"gl.geos) instead."
            )

    def test_documents_namespace_is_nested(self):
        # gl.documents replaces gl.doc_*. See goldlapel/documents.py.
        from goldlapel.documents import DocumentsAPI
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        assert isinstance(gl.documents, DocumentsAPI)
        for verb in [
            "insert", "insert_many", "find", "find_one", "find_cursor",
            "update", "update_one", "delete", "delete_one",
            "find_one_and_update", "find_one_and_delete", "distinct",
            "count", "create_index", "aggregate",
            "watch", "unwatch",
            "create_ttl_index", "remove_ttl_index",
            "create_capped", "remove_cap",
            "create_collection",
        ]:
            assert hasattr(gl.documents, verb), f"Missing gl.documents.{verb}"
            assert callable(getattr(gl.documents, verb)), f"Not callable: gl.documents.{verb}"
        # The flat doc_* methods are gone — hard cut.
        for legacy in ["doc_insert", "doc_find", "doc_update", "doc_delete"]:
            assert not hasattr(gl, legacy), (
                f"Legacy flat method {legacy} should have been removed; "
                f"use gl.documents.<verb> instead."
            )

    def test_streams_namespace_is_nested(self):
        # gl.streams replaces gl.stream_*. See goldlapel/streams.py.
        from goldlapel.streams import StreamsAPI
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        assert isinstance(gl.streams, StreamsAPI)
        for verb in ["add", "create_group", "read", "ack", "claim"]:
            assert hasattr(gl.streams, verb), f"Missing gl.streams.{verb}"
            assert callable(getattr(gl.streams, verb)), f"Not callable: gl.streams.{verb}"
        # The flat stream_* methods are gone — hard cut.
        for legacy in ["stream_add", "stream_read", "stream_ack"]:
            assert not hasattr(gl, legacy), (
                f"Legacy flat method {legacy} should have been removed; "
                f"use gl.streams.<verb> instead."
            )


class TestPhase5Namespaces:
    """Phase 5 (2026-04-30): counter / zset / hash / queue / geo each get
    their own nested namespace. Verbs match the proxy's canonical handler
    surface; method shapes are documented in each module under
    src/goldlapel/{counters,zsets,hashes,queues,geos}.py.
    """

    def test_counters_namespace(self):
        from goldlapel.counters import CountersAPI
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        assert isinstance(gl.counters, CountersAPI)
        for verb in ["create", "incr", "decr", "set", "get", "delete", "count_keys"]:
            assert hasattr(gl.counters, verb), f"Missing gl.counters.{verb}"
            assert callable(getattr(gl.counters, verb))

    def test_zsets_namespace(self):
        from goldlapel.zsets import ZsetsAPI
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        assert isinstance(gl.zsets, ZsetsAPI)
        for verb in [
            "create", "add", "incr_by", "score", "rank", "range",
            "range_by_score", "remove", "card",
        ]:
            assert hasattr(gl.zsets, verb), f"Missing gl.zsets.{verb}"
            assert callable(getattr(gl.zsets, verb))

    def test_hashes_namespace(self):
        from goldlapel.hashes import HashesAPI
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        assert isinstance(gl.hashes, HashesAPI)
        for verb in [
            "create", "set", "get", "get_all", "keys", "values",
            "exists", "delete", "len",
        ]:
            assert hasattr(gl.hashes, verb), f"Missing gl.hashes.{verb}"
            assert callable(getattr(gl.hashes, verb))

    def test_queues_namespace(self):
        from goldlapel.queues import QueuesAPI
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        assert isinstance(gl.queues, QueuesAPI)
        for verb in [
            "create", "enqueue", "claim", "ack", "abandon", "extend",
            "peek", "count_ready", "count_claimed",
        ]:
            assert hasattr(gl.queues, verb), f"Missing gl.queues.{verb}"
            assert callable(getattr(gl.queues, verb))

    def test_geos_namespace(self):
        from goldlapel.geos import GeosAPI
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        assert isinstance(gl.geos, GeosAPI)
        for verb in [
            "create", "add", "pos", "dist", "radius", "radius_by_member",
            "remove", "count",
        ]:
            assert hasattr(gl.geos, verb), f"Missing gl.geos.{verb}"
            assert callable(getattr(gl.geos, verb))
