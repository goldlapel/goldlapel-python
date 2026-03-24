import os
import socket
import threading
import time

import pytest

from goldlapel.cache import (
    NativeCache,
    CacheEntry,
    _detect_write,
    _extract_tables,
    _make_key,
    _DDL_SENTINEL,
    _TX_START,
    _TX_END,
)


@pytest.fixture(autouse=True)
def reset_cache():
    NativeCache._reset()
    yield
    NativeCache._reset()


def make_cache(max_entries=100, enabled=True, connected=True):
    if not enabled:
        os.environ["GOLDLAPEL_NATIVE_CACHE"] = "false"
    else:
        os.environ.pop("GOLDLAPEL_NATIVE_CACHE", None)
    os.environ["GOLDLAPEL_NATIVE_CACHE_SIZE"] = str(max_entries)
    cache = NativeCache()
    cache._invalidation_connected = connected
    os.environ.pop("GOLDLAPEL_NATIVE_CACHE", None)
    os.environ.pop("GOLDLAPEL_NATIVE_CACHE_SIZE", None)
    return cache


# --- Cache key ---

class TestMakeKey:
    def test_none_params(self):
        assert _make_key("SELECT 1", None) == ("SELECT 1", None)

    def test_tuple_params(self):
        assert _make_key("SELECT $1", (42,)) == ("SELECT $1", (42,))

    def test_list_params(self):
        assert _make_key("SELECT $1", [42]) == ("SELECT $1", (42,))

    def test_dict_params(self):
        key = _make_key("SELECT %(id)s", {"id": 42, "name": "test"})
        assert key == ("SELECT %(id)s", (("id", 42), ("name", "test")))

    def test_dict_params_order_independent(self):
        k1 = _make_key("SELECT 1", {"b": 2, "a": 1})
        k2 = _make_key("SELECT 1", {"a": 1, "b": 2})
        assert k1 == k2


# --- Write detection ---

class TestDetectWrite:
    def test_insert(self):
        assert _detect_write("INSERT INTO orders VALUES (1)") == "orders"

    def test_insert_schema(self):
        assert _detect_write("INSERT INTO public.orders VALUES (1)") == "orders"

    def test_update(self):
        assert _detect_write("UPDATE orders SET name = 'x'") == "orders"

    def test_delete(self):
        assert _detect_write("DELETE FROM orders WHERE id = 1") == "orders"

    def test_truncate(self):
        assert _detect_write("TRUNCATE orders") == "orders"

    def test_truncate_table(self):
        assert _detect_write("TRUNCATE TABLE orders") == "orders"

    def test_create_ddl(self):
        assert _detect_write("CREATE TABLE foo (id int)") == _DDL_SENTINEL

    def test_alter_ddl(self):
        assert _detect_write("ALTER TABLE foo ADD COLUMN bar int") == _DDL_SENTINEL

    def test_drop_ddl(self):
        assert _detect_write("DROP TABLE foo") == _DDL_SENTINEL

    def test_select_returns_none(self):
        assert _detect_write("SELECT * FROM orders") is None

    def test_case_insensitive(self):
        assert _detect_write("insert INTO Orders VALUES (1)") == "orders"

    def test_copy_from(self):
        assert _detect_write("COPY orders FROM '/tmp/data.csv'") == "orders"

    def test_copy_to_returns_none(self):
        assert _detect_write("COPY orders TO '/tmp/data.csv'") is None

    def test_copy_subquery_returns_none(self):
        assert _detect_write("COPY (SELECT * FROM orders) TO '/tmp/data.csv'") is None

    def test_with_cte_insert(self):
        assert _detect_write("WITH x AS (SELECT 1) INSERT INTO foo SELECT * FROM x") == _DDL_SENTINEL

    def test_with_cte_select(self):
        assert _detect_write("WITH x AS (SELECT 1) SELECT * FROM x") is None

    def test_empty_returns_none(self):
        assert _detect_write("") is None

    def test_whitespace_only_returns_none(self):
        assert _detect_write("   ") is None

    def test_copy_with_columns(self):
        assert _detect_write("COPY orders(id, name) FROM '/tmp/data.csv'") == "orders"


# --- Table extraction ---

class TestExtractTables:
    def test_simple_from(self):
        assert _extract_tables("SELECT * FROM orders") == {"orders"}

    def test_join(self):
        tables = _extract_tables("SELECT * FROM orders o JOIN customers c ON o.cid = c.id")
        assert tables == {"orders", "customers"}

    def test_schema_qualified(self):
        assert _extract_tables("SELECT * FROM public.orders") == {"orders"}

    def test_multiple_joins(self):
        sql = "SELECT * FROM orders JOIN items ON 1=1 JOIN products ON 1=1"
        assert _extract_tables(sql) == {"orders", "items", "products"}

    def test_case_insensitive(self):
        assert _extract_tables("SELECT * FROM ORDERS") == {"orders"}

    def test_no_tables(self):
        assert _extract_tables("SELECT 1") == set()

    def test_subquery(self):
        tables = _extract_tables("SELECT * FROM orders WHERE id IN (SELECT oid FROM users)")
        assert "orders" in tables
        assert "users" in tables


# --- Transaction detection ---

class TestTransactionDetection:
    def test_begin(self):
        assert _TX_START.match("BEGIN")

    def test_start_transaction(self):
        assert _TX_START.match("START TRANSACTION")

    def test_commit(self):
        assert _TX_END.match("COMMIT")

    def test_rollback(self):
        assert _TX_END.match("ROLLBACK")

    def test_end(self):
        assert _TX_END.match("END")

    def test_savepoint_not_start(self):
        assert not _TX_START.match("SAVEPOINT x")

    def test_set_transaction_not_start(self):
        assert not _TX_START.match("SET TRANSACTION ISOLATION LEVEL")

    def test_select_not_start(self):
        assert not _TX_START.match("SELECT 1")


# --- Cache operations ---

class TestCacheOps:
    def test_put_and_get(self):
        cache = make_cache()
        rows = [(1, "alice")]
        desc = (("id",), ("name",))
        cache.put("SELECT * FROM users", None, rows, desc)
        entry = cache.get("SELECT * FROM users", None)
        assert entry is not None
        assert entry.rows == [(1, "alice")]
        assert entry.description == (("id",), ("name",))

    def test_miss_returns_none(self):
        cache = make_cache()
        assert cache.get("SELECT 1", None) is None

    def test_disabled_returns_none(self):
        cache = make_cache(enabled=False)
        cache.put("SELECT 1", None, [(1,)], (("?column?",),))
        assert cache.get("SELECT 1", None) is None

    def test_not_connected_returns_none(self):
        cache = make_cache(connected=False)
        cache.put("SELECT 1", None, [(1,)], (("?column?",),))
        assert cache.get("SELECT 1", None) is None

    def test_params_differentiate_keys(self):
        cache = make_cache()
        cache.put("SELECT * FROM users WHERE id = %s", (1,), [(1, "alice")], None)
        cache.put("SELECT * FROM users WHERE id = %s", (2,), [(2, "bob")], None)
        e1 = cache.get("SELECT * FROM users WHERE id = %s", (1,))
        e2 = cache.get("SELECT * FROM users WHERE id = %s", (2,))
        assert e1.rows == [(1, "alice")]
        assert e2.rows == [(2, "bob")]

    def test_unhashable_params_bypassed(self):
        cache = make_cache()
        cache.put("SELECT 1", ([1, 2],), [(1,)], None)
        assert cache.get("SELECT 1", ([1, 2],)) is None

    def test_stats_tracking(self):
        cache = make_cache()
        cache.put("SELECT 1", None, [(1,)], None)
        cache.get("SELECT 1", None)
        cache.get("SELECT 2", None)
        assert cache.stats_hits == 1
        assert cache.stats_misses == 1


# --- LRU eviction ---

class TestLRU:
    def test_eviction_at_capacity(self):
        cache = make_cache(max_entries=3)
        cache.put("SELECT 1", None, [(1,)], None)
        cache.put("SELECT 2", None, [(2,)], None)
        cache.put("SELECT 3", None, [(3,)], None)
        cache.put("SELECT 4", None, [(4,)], None)
        assert cache.get("SELECT 1", None) is None
        assert cache.get("SELECT 4", None) is not None

    def test_access_refreshes_lru(self):
        cache = make_cache(max_entries=3)
        cache.put("SELECT 1", None, [(1,)], None)
        cache.put("SELECT 2", None, [(2,)], None)
        cache.put("SELECT 3", None, [(3,)], None)
        cache.get("SELECT 1", None)  # refresh SELECT 1
        cache.put("SELECT 4", None, [(4,)], None)  # evicts SELECT 2 (oldest)
        assert cache.get("SELECT 1", None) is not None
        assert cache.get("SELECT 2", None) is None

    def test_eviction_cleans_table_index(self):
        cache = make_cache(max_entries=2)
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        cache.put("SELECT * FROM users", None, [(2,)], None)
        cache.put("SELECT * FROM products", None, [(3,)], None)
        assert "orders" not in cache._table_index or len(cache._table_index.get("orders", set())) == 0


# --- Invalidation ---

class TestInvalidation:
    def test_invalidate_table(self):
        cache = make_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        cache.put("SELECT * FROM users", None, [(2,)], None)
        cache.invalidate_table("orders")
        assert cache.get("SELECT * FROM orders", None) is None
        assert cache.get("SELECT * FROM users", None) is not None

    def test_invalidate_all(self):
        cache = make_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        cache.put("SELECT * FROM users", None, [(2,)], None)
        cache.invalidate_all()
        assert cache.get("SELECT * FROM orders", None) is None
        assert cache.get("SELECT * FROM users", None) is None

    def test_invalidate_cross_referenced(self):
        cache = make_cache()
        cache.put("SELECT * FROM orders JOIN users ON 1=1", None, [(1,)], None)
        cache.invalidate_table("orders")
        assert cache.get("SELECT * FROM orders JOIN users ON 1=1", None) is None
        assert "users" not in cache._table_index or not cache._table_index["users"]

    def test_invalidate_stats(self):
        cache = make_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        cache.invalidate_table("orders")
        assert cache.stats_invalidations == 1


# --- Signal processing ---

class TestSignalProcessing:
    def test_table_signal_invalidates(self):
        cache = make_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        cache._process_signal("I:orders")
        assert cache.get("SELECT * FROM orders", None) is None

    def test_wildcard_signal_invalidates_all(self):
        cache = make_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        cache._process_signal("I:*")
        assert cache.get("SELECT * FROM orders", None) is None

    def test_keepalive_preserves_cache(self):
        cache = make_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        cache._process_signal("P:")
        assert cache.get("SELECT * FROM orders", None) is not None

    def test_unknown_signal_preserves_cache(self):
        cache = make_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        cache._process_signal("X:something")
        assert cache.get("SELECT * FROM orders", None) is not None


# --- Push invalidation ---

class TestPushInvalidation:
    def test_remote_signal_clears_cache(self):
        cache = make_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", 0))
        port = server.getsockname()[1]
        server.listen(1)

        cache.connect_invalidation(port)
        conn, _ = server.accept()
        time.sleep(0.1)

        assert cache._invalidation_connected
        conn.sendall(b"I:orders\n")
        time.sleep(0.2)

        assert cache.get("SELECT * FROM orders", None) is None

        conn.close()
        server.close()
        cache.stop_invalidation()

    def test_connection_drop_clears_cache(self):
        cache = make_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", 0))
        port = server.getsockname()[1]
        server.listen(1)

        cache.connect_invalidation(port)
        conn, _ = server.accept()
        time.sleep(0.1)

        assert cache._invalidation_connected

        conn.close()
        server.close()
        time.sleep(0.5)

        assert not cache._invalidation_connected
        assert len(cache._cache) == 0

        cache.stop_invalidation()


# --- Thread safety ---

class TestThreadSafety:
    def test_concurrent_put_and_get(self):
        cache = make_cache(max_entries=1000)
        errors = []

        def writer(start, count):
            try:
                for i in range(start, start + count):
                    cache.put(f"SELECT {i}", None, [(i,)], None)
            except Exception as e:
                errors.append(e)

        def reader(start, count):
            try:
                for i in range(start, start + count):
                    cache.get(f"SELECT {i}", None)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=writer, args=(0, 200)),
            threading.Thread(target=writer, args=(200, 200)),
            threading.Thread(target=reader, args=(0, 200)),
            threading.Thread(target=reader, args=(200, 200)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors

    def test_concurrent_invalidation(self):
        cache = make_cache(max_entries=1000)
        for i in range(100):
            cache.put(f"SELECT * FROM t{i % 10}", (i,), [(i,)], None)

        errors = []
        def invalidator():
            try:
                for i in range(10):
                    cache.invalidate_table(f"t{i}")
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                for i in range(100):
                    cache.get(f"SELECT * FROM t{i % 10}", (i,))
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=invalidator),
            threading.Thread(target=reader),
            threading.Thread(target=reader),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
