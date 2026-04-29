"""Tests for the v0.2.0 factory API:
- module-level start() returns a GoldLapel instance (not a wrapped conn)
- `with goldlapel.start(...) as gl:` context manager
- `gl.using(conn)` scoped override
- `gl.search(..., conn=...)` per-call override
"""

from unittest.mock import MagicMock, patch

import pytest

import goldlapel
from goldlapel.proxy import GoldLapel


class TestStartReturnsInstance:
    @patch("goldlapel.proxy._ensure_running")
    @patch("goldlapel.proxy._detect_sync_driver", return_value=("psycopg2", MagicMock()))
    def test_returns_goldlapel_instance(self, mock_detect, mock_ensure):
        fake = MagicMock(spec=GoldLapel)
        mock_ensure.return_value = fake
        result = goldlapel.start("postgresql://host/db")
        assert result is fake

    @patch("goldlapel.proxy._detect_sync_driver", return_value=(None, None))
    def test_raises_without_driver(self, mock_detect):
        with pytest.raises(ImportError, match="sync Postgres driver"):
            goldlapel.start("postgresql://host/db")


class TestContextManager:
    def test_stop_called_on_exit(self):
        gl = GoldLapel("postgresql://host/db")
        with patch.object(gl, "start") as mock_start, patch.object(gl, "stop") as mock_stop:
            # simulate running=False so __enter__ calls start
            with patch.object(GoldLapel, "running", new=False):
                with gl as entered:
                    assert entered is gl
                    mock_start.assert_called_once()
            mock_stop.assert_called_once()

    def test_enter_skips_start_when_already_running(self):
        gl = GoldLapel("postgresql://host/db")
        with patch.object(gl, "start") as mock_start, patch.object(gl, "stop") as mock_stop:
            with patch.object(GoldLapel, "running", new=True):
                with gl:
                    pass
            mock_start.assert_not_called()
            mock_stop.assert_called_once()

    def test_stop_called_even_on_exception(self):
        gl = GoldLapel("postgresql://host/db")
        with patch.object(gl, "start"), patch.object(gl, "stop") as mock_stop:
            with patch.object(GoldLapel, "running", new=True):
                with pytest.raises(ValueError):
                    with gl:
                        raise ValueError("bang")
            mock_stop.assert_called_once()


class TestUsingContextManager:
    def _mk_instance_with_internal_conn(self):
        gl = GoldLapel("postgresql://host/db")
        gl._conn = MagicMock(name="internal_conn")
        return gl

    def test_using_sets_scoped_conn(self):
        gl = self._mk_instance_with_internal_conn()
        user_conn = MagicMock(name="user_conn")
        with gl.using(user_conn):
            assert gl._effective_conn() is user_conn

    def test_using_reverts_after_scope(self):
        gl = self._mk_instance_with_internal_conn()
        user_conn = MagicMock(name="user_conn")
        with gl.using(user_conn):
            pass
        # outside the scope, effective_conn is the internal
        assert gl._effective_conn() is gl._conn

    def test_using_reverts_even_on_exception(self):
        gl = self._mk_instance_with_internal_conn()
        user_conn = MagicMock(name="user_conn")
        with pytest.raises(RuntimeError):
            with gl.using(user_conn):
                raise RuntimeError("bang")
        assert gl._effective_conn() is gl._conn

    def test_explicit_override_wins_over_using(self):
        gl = self._mk_instance_with_internal_conn()
        user_conn = MagicMock(name="user_conn")
        override = MagicMock(name="override")
        with gl.using(user_conn):
            assert gl._effective_conn(override) is override

    def test_each_instance_has_its_own_scope(self):
        gl1 = self._mk_instance_with_internal_conn()
        gl2 = self._mk_instance_with_internal_conn()
        conn1 = MagicMock(name="conn1")
        with gl1.using(conn1):
            # gl2 is untouched — its effective conn remains its own internal
            assert gl2._effective_conn() is gl2._conn
            assert gl1._effective_conn() is conn1


class TestConnKwargOnMethods:
    """The 54 wrapper methods accept an optional conn= kwarg that overrides the
    instance's internal connection for that call only."""

    def test_search_uses_kwarg_conn(self):
        gl = GoldLapel("postgresql://host/db")
        gl._conn = MagicMock(name="internal_conn")
        override = MagicMock(name="override_conn")
        with patch("goldlapel.utils.search") as mock_search:
            mock_search.return_value = "ok"
            result = gl.search("articles", "body", "query", conn=override)
            assert result == "ok"
            # First positional arg to utils.search should be the override
            call_conn = mock_search.call_args[0][0]
            assert call_conn is override

    def test_search_uses_internal_when_no_kwarg(self):
        gl = GoldLapel("postgresql://host/db")
        gl._conn = MagicMock(name="internal_conn")
        with patch("goldlapel.utils.search") as mock_search:
            mock_search.return_value = "ok"
            gl.search("articles", "body", "query")
            call_conn = mock_search.call_args[0][0]
            assert call_conn is gl._conn

    def test_doc_insert_uses_kwarg_conn(self):
        # gl.documents.insert replaces gl.doc_insert. The DocumentsAPI calls
        # _patterns first to fetch DDL — mock that out so we don't need a
        # dashboard.
        gl = GoldLapel("postgresql://host/db")
        gl._conn = MagicMock(name="internal_conn")
        override = MagicMock(name="override_conn")
        fake_patterns = {"tables": {"main": "events"}, "query_patterns": {}}
        with patch("goldlapel.ddl.fetch_patterns", return_value=fake_patterns), \
             patch("goldlapel.utils.doc_insert") as mock_fn:
            gl._dashboard_token = "test-token"
            gl.documents.insert("events", {"type": "x"}, conn=override)
            call_conn = mock_fn.call_args[0][0]
            assert call_conn is override

    def test_zsets_add_uses_scoped_using_conn(self):
        gl = GoldLapel("postgresql://host/db")
        gl._conn = MagicMock(name="internal_conn")
        gl._dashboard_token = "test-token"
        scoped = MagicMock(name="scoped_conn")
        fake_patterns = {
            "tables": {"main": "_goldlapel.zset_leaderboard"},
            "query_patterns": {"zadd": "..."},
        }
        with patch("goldlapel.ddl.fetch_patterns", return_value=fake_patterns), \
             patch("goldlapel.utils.zset_add") as mock_fn:
            with gl.using(scoped):
                gl.zsets.add("leaderboard", "global", "stephen", 100)
            call_conn = mock_fn.call_args[0][0]
            assert call_conn is scoped

    def test_kwarg_beats_using_scope(self):
        gl = GoldLapel("postgresql://host/db")
        gl._conn = MagicMock(name="internal_conn")
        scoped = MagicMock(name="scoped_conn")
        override = MagicMock(name="override_conn")
        fake_patterns = {"tables": {"main": "users"}, "query_patterns": {}}
        with patch("goldlapel.ddl.fetch_patterns", return_value=fake_patterns), \
             patch("goldlapel.utils.doc_update") as mock_fn:
            gl._dashboard_token = "test-token"
            with gl.using(scoped):
                gl.documents.update("users", {"id": 1}, {"$set": {"name": "s"}}, conn=override)
            call_conn = mock_fn.call_args[0][0]
            assert call_conn is override
