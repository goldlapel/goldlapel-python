from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from goldlapel.cache import NativeCache
from goldlapel.wrap import wrap, CachedConnection, CachedCursor


@pytest.fixture(autouse=True)
def reset_cache():
    NativeCache._reset()
    import goldlapel.wrap
    goldlapel.wrap._cache = None
    yield
    NativeCache._reset()
    goldlapel.wrap._cache = None


def make_connected_cache():
    cache = NativeCache()
    cache._invalidation_connected = True
    return cache


def mock_conn(description=None, fetchall_result=None):
    conn = MagicMock()
    cursor = MagicMock()
    cursor.description = description
    cursor.fetchall.return_value = fetchall_result or []
    cursor.name = None
    cursor.arraysize = 1
    conn.cursor.return_value = cursor
    return conn, cursor


# --- wrap() function ---

class TestWrap:
    def test_returns_cached_connection(self):
        conn = MagicMock()
        conn.fetch = None
        conn.fetchrow = None
        delattr(conn, "fetch")
        delattr(conn, "fetchrow")
        with patch("goldlapel.wrap.NativeCache") as MockCache:
            instance = MagicMock()
            instance._invalidation_thread = None
            MockCache.return_value = instance
            wrapped = wrap(conn, invalidation_port=9999)
        assert isinstance(wrapped, CachedConnection)

    def test_asyncpg_returns_async_wrapper(self):
        from goldlapel.wrap import AsyncCachedConnection
        conn = MagicMock()
        conn.fetch = MagicMock()
        conn.fetchrow = MagicMock()
        with patch("goldlapel.wrap.NativeCache") as MockCache:
            instance = MagicMock()
            instance._invalidation_thread = None
            MockCache.return_value = instance
            wrapped = wrap(conn, invalidation_port=9999)
        assert isinstance(wrapped, AsyncCachedConnection)

    def test_disable_native_cache_propagates_first_construction(self):
        conn = MagicMock()
        delattr(conn, "fetch")
        delattr(conn, "fetchrow")
        # Cache fixture wipes _cache to None, so this exercises the
        # first-construction branch.
        wrap(conn, invalidation_port=9999, disable_native_cache=True)
        from goldlapel.cache import NativeCache as _NC
        assert _NC._instance is not None
        assert _NC._instance._disabled is True

    def test_disable_native_cache_propagates_subsequent_construction(self):
        conn = MagicMock()
        delattr(conn, "fetch")
        delattr(conn, "fetchrow")
        # First wrap with default — disabled False.
        wrap(conn, invalidation_port=9999)
        from goldlapel.cache import NativeCache as _NC
        assert _NC._instance._disabled is False
        # Second wrap with disable_native_cache=True flips the flag on the
        # existing singleton. (wrap() short-circuits the singleton
        # construction but still re-passes the kwarg through
        # NativeCache.__init__.)
        wrap(conn, invalidation_port=9999, disable_native_cache=True)
        assert _NC._instance._disabled is True

    def test_disable_native_cache_default_false(self):
        conn = MagicMock()
        delattr(conn, "fetch")
        delattr(conn, "fetchrow")
        wrap(conn, invalidation_port=9999)
        from goldlapel.cache import NativeCache as _NC
        assert _NC._instance._disabled is False


# --- CachedConnection ---

class TestCachedConnection:
    def test_cursor_returns_cached_cursor(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        wrapped = CachedConnection(conn, cache)
        cur = wrapped.cursor()
        assert isinstance(cur, CachedCursor)

    def test_getattr_proxies(self):
        conn = MagicMock()
        conn.autocommit = True
        cache = make_connected_cache()
        wrapped = CachedConnection(conn, cache)
        assert wrapped.autocommit is True

    def test_close_delegates(self):
        conn = MagicMock()
        cache = make_connected_cache()
        wrapped = CachedConnection(conn, cache)
        wrapped.close()
        conn.close.assert_called_once()

    def test_context_manager(self):
        conn = MagicMock()
        cache = make_connected_cache()
        wrapped = CachedConnection(conn, cache)
        with wrapped as w:
            assert w is wrapped


# --- CachedCursor: cache hit ---

class TestCacheHit:
    def test_hit_skips_real_execute(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        sql = "SELECT * FROM orders"
        cache.put(sql, None, [(1, "widget")], (("id",), ("name",)))
        cc = CachedCursor(cursor, cache)
        cc.execute(sql)
        cursor.execute.assert_not_called()

    def test_hit_fetchall(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        sql = "SELECT * FROM orders"
        cache.put(sql, None, [(1, "a"), (2, "b")], None)
        cc = CachedCursor(cursor, cache)
        cc.execute(sql)
        assert cc.fetchall() == [(1, "a"), (2, "b")]

    def test_hit_fetchone(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        sql = "SELECT * FROM orders"
        cache.put(sql, None, [(1, "a"), (2, "b")], None)
        cc = CachedCursor(cursor, cache)
        cc.execute(sql)
        assert cc.fetchone() == (1, "a")
        assert cc.fetchone() == (2, "b")
        assert cc.fetchone() is None

    def test_hit_fetchmany(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        sql = "SELECT * FROM orders"
        cache.put(sql, None, [(1,), (2,), (3,)], None)
        cc = CachedCursor(cursor, cache)
        cc.execute(sql)
        assert cc.fetchmany(2) == [(1,), (2,)]
        assert cc.fetchmany(2) == [(3,)]

    def test_hit_description(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        desc = (("id", int), ("name", str))
        cache.put("SELECT * FROM orders", None, [(1,)], desc)
        cc = CachedCursor(cursor, cache)
        cc.execute("SELECT * FROM orders")
        assert cc.description == desc

    def test_hit_rowcount(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,), (2,)], None)
        cc = CachedCursor(cursor, cache)
        cc.execute("SELECT * FROM orders")
        assert cc.rowcount == 2

    def test_hit_iteration(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,), (2,)], None)
        cc = CachedCursor(cursor, cache)
        cc.execute("SELECT * FROM orders")
        rows = list(cc)
        assert rows == [(1,), (2,)]


# --- CachedCursor: cache miss ---

class TestCacheMiss:
    def test_miss_calls_real_execute(self):
        conn, cursor = mock_conn(
            description=(("id",),),
            fetchall_result=[(1,)],
        )
        cache = make_connected_cache()
        cc = CachedCursor(cursor, cache)
        cc.execute("SELECT * FROM orders")
        cursor.execute.assert_called_once_with("SELECT * FROM orders", None)

    def test_miss_caches_result(self):
        conn, cursor = mock_conn(
            description=(("id",),),
            fetchall_result=[(1,)],
        )
        cache = make_connected_cache()
        cc = CachedCursor(cursor, cache)
        cc.execute("SELECT * FROM orders")
        entry = cache.get("SELECT * FROM orders", None)
        assert entry is not None
        assert entry.rows == [(1,)]

    def test_miss_fetchone_returns_cached(self):
        conn, cursor = mock_conn(
            description=(("id",),),
            fetchall_result=[(1,), (2,)],
        )
        cache = make_connected_cache()
        cc = CachedCursor(cursor, cache)
        cc.execute("SELECT * FROM orders")
        assert cc.fetchone() == (1,)
        assert cc.fetchone() == (2,)


# --- CachedCursor: writes ---

class TestWrites:
    def test_write_invalidates_table(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        cc = CachedCursor(cursor, cache)
        cc.execute("INSERT INTO orders VALUES (2)")
        assert cache.get("SELECT * FROM orders", None) is None

    def test_write_delegates_to_real(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cc = CachedCursor(cursor, cache)
        cc.execute("INSERT INTO orders VALUES (2)")
        cursor.execute.assert_called_once()

    def test_ddl_invalidates_all(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        cache.put("SELECT * FROM users", None, [(2,)], None)
        cc = CachedCursor(cursor, cache)
        cc.execute("CREATE TABLE foo (id int)")
        assert cache.get("SELECT * FROM orders", None) is None
        assert cache.get("SELECT * FROM users", None) is None

    def test_executemany_invalidates(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        cc = CachedCursor(cursor, cache)
        cc.executemany("INSERT INTO orders VALUES (%s)", [(1,), (2,)])
        assert cache.get("SELECT * FROM orders", None) is None

    def test_callproc_invalidates_all(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        cc = CachedCursor(cursor, cache)
        cc.callproc("my_proc")
        assert cache.get("SELECT * FROM orders", None) is None


# --- CachedCursor: transactions ---

class MockCachedConn:
    _in_transaction = False


class TestTransactions:
    def test_begin_disables_cache(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        mock_cc = MockCachedConn()
        cc = CachedCursor(cursor, cache, mock_cc)
        cc.execute("BEGIN")
        cc.execute("SELECT * FROM orders")
        # Inside txn, should call real cursor even though cached
        cursor.execute.assert_any_call("SELECT * FROM orders", None)

    def test_commit_re_enables_cache(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        mock_cc = MockCachedConn()
        cc = CachedCursor(cursor, cache, mock_cc)
        cc.execute("BEGIN")
        cc.execute("COMMIT")
        cursor.reset_mock()
        cc.execute("SELECT * FROM orders")
        cursor.execute.assert_not_called()  # cache hit

    def test_rollback_re_enables_cache(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        mock_cc = MockCachedConn()
        cc = CachedCursor(cursor, cache, mock_cc)
        cc.execute("BEGIN")
        cc.execute("ROLLBACK")
        cursor.reset_mock()
        cc.execute("SELECT * FROM orders")
        cursor.execute.assert_not_called()  # cache hit

    def test_write_in_transaction_still_invalidates(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        mock_cc = MockCachedConn()
        cc = CachedCursor(cursor, cache, mock_cc)
        cc.execute("BEGIN")
        cc.execute("INSERT INTO orders VALUES (2)")
        assert cache.get("SELECT * FROM orders", None) is None

    def test_cross_cursor_transaction_tracking(self):
        """BEGIN on cursor1, read on cursor2 should bypass cache."""
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        mock_cc = MockCachedConn()
        cc1 = CachedCursor(cursor, cache, mock_cc)
        cc2 = CachedCursor(cursor, cache, mock_cc)
        cc1.execute("BEGIN")
        cc2.execute("SELECT * FROM orders")
        cursor.execute.assert_any_call("SELECT * FROM orders", None)


# --- Named cursor bypass ---

class TestNamedCursor:
    def test_named_cursor_bypassed(self):
        conn, cursor = mock_conn(
            description=(("id",),),
            fetchall_result=[(1,)],
        )
        cursor.name = "my_server_cursor"
        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(99,)], None)
        cc = CachedCursor(cursor, cache)
        cc.execute("SELECT * FROM orders")
        cursor.execute.assert_called_once()


# --- Edge cases ---

class TestEdgeCases:
    def test_execute_resets_cached_state(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cache.put("SELECT 1", None, [(1,)], None)
        cache.put("SELECT 2", None, [(2,)], None)
        cc = CachedCursor(cursor, cache)
        cc.execute("SELECT 1")
        assert cc.fetchone() == (1,)
        cc.execute("SELECT 2")
        assert cc.fetchone() == (2,)

    def test_write_after_cache_hit_resets_state(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1, "old")], None)
        cc = CachedCursor(cursor, cache)
        # First: cache hit
        cc.execute("SELECT * FROM orders")
        assert cc.fetchone() == (1, "old")
        # Then: write invalidates
        cc.execute("INSERT INTO orders VALUES (2, 'new')")
        # fetchone should NOT return stale cached data
        assert cc.fetchone() is None or cc._cached_rows is None

    def test_fetchone_after_fetchall_returns_none(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cache.put("SELECT 1", None, [(1,)], None)
        cc = CachedCursor(cursor, cache)
        cc.execute("SELECT 1")
        cc.fetchall()
        assert cc.fetchone() is None

    def test_getattr_proxies_to_real_cursor(self):
        conn, cursor = mock_conn()
        cursor.statusmessage = "SELECT 1"
        cache = make_connected_cache()
        cc = CachedCursor(cursor, cache)
        assert cc.statusmessage == "SELECT 1"

    def test_context_manager(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cc = CachedCursor(cursor, cache)
        with cc as c:
            assert c is cc
