import json
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
    _EVICT_RATE_HIGH,
    _EVICT_RATE_LOW,
    _EVICT_RATE_WINDOW,
    _HIT_RATE_HIGH_PCT,
    _HIT_RATE_LOW_PCT,
    _HIT_RATE_WARMUP,
    _HIT_RATE_WINDOW,
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

    def test_re_put_refreshes_lru(self):
        cache = make_cache(max_entries=3)
        cache.put("SELECT 1", None, [(1,)], None)
        cache.put("SELECT 2", None, [(2,)], None)
        cache.put("SELECT 3", None, [(3,)], None)
        cache.put("SELECT 1", None, [(10,)], None)  # re-put refreshes SELECT 1
        cache.put("SELECT 4", None, [(4,)], None)  # evicts SELECT 2 (oldest)
        assert cache.get("SELECT 1", None) is not None
        assert cache.get("SELECT 1", None).rows == [(10,)]
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


# --- L1 telemetry: counters + snapshot ---

class TestEvictionsCounter:
    def test_evictions_counter_starts_zero(self):
        cache = make_cache(max_entries=4)
        assert cache.stats_evictions == 0

    def test_evictions_counter_bumps_on_overflow(self):
        cache = make_cache(max_entries=4)
        for i in range(8):
            cache.put(f"SELECT {i}", None, [(i,)], None)
        # 8 puts, capacity 4 → 4 evictions.
        assert cache.stats_evictions == 4

    def test_evictions_counter_no_bump_within_capacity(self):
        cache = make_cache(max_entries=8)
        for i in range(4):
            cache.put(f"SELECT {i}", None, [(i,)], None)
        assert cache.stats_evictions == 0


class TestSnapshotShape:
    def test_snapshot_carries_required_fields(self):
        cache = make_cache(max_entries=64)
        cache.put("SELECT 1", None, [(1,)], None)
        cache.get("SELECT 1", None)
        cache.get("SELECT MISS", None)
        snap = cache._build_snapshot()
        assert snap["wrapper_id"] == cache._wrapper_id
        assert snap["lang"] == "python"
        assert "version" in snap
        assert snap["hits"] == 1
        assert snap["misses"] == 1
        assert snap["evictions"] == 0
        assert snap["current_size_entries"] == 1
        assert snap["capacity_entries"] == 64

    def test_wrapper_id_is_uuid(self):
        cache = make_cache()
        # UUID4 string format check
        import uuid as _uuid
        parsed = _uuid.UUID(cache._wrapper_id)
        assert parsed.version == 4

    def test_wrapper_id_stable_across_calls(self):
        cache = make_cache()
        a = cache._build_snapshot()["wrapper_id"]
        b = cache._build_snapshot()["wrapper_id"]
        assert a == b


# --- L1 telemetry: state-change emission via real socket ---

def _wait_for(predicate, timeout=2.0, interval=0.02):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def _accept_with_buf(server):
    """Accept a connection and start a buffered reader. Returns (conn, lines_list, stop_fn)."""
    conn, _ = server.accept()
    conn.settimeout(0.5)
    lines = []
    stop = threading.Event()

    def reader():
        buf = b""
        while not stop.is_set():
            try:
                data = conn.recv(4096)
                if not data:
                    return
                buf += data
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    lines.append(line.decode("utf-8", errors="replace"))
            except socket.timeout:
                continue
            except OSError:
                return

    t = threading.Thread(target=reader, daemon=True)
    t.start()

    def stop_fn():
        stop.set()
        try:
            conn.close()
        except OSError:
            pass

    return conn, lines, stop_fn


class TestStateChangeEmission:
    def _spawn_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", 0))
        port = server.getsockname()[1]
        server.listen(1)
        return server, port

    def test_wrapper_connected_emitted_on_socket_connect(self):
        cache = make_cache()
        server, port = self._spawn_server()
        try:
            cache.connect_invalidation(port)
            conn, lines, stop_fn = _accept_with_buf(server)
            try:
                _wait_for(lambda: any(l.startswith("S:") for l in lines))
                state_lines = [l for l in lines if l.startswith("S:")]
                assert state_lines, f"expected S: line, got {lines}"
                payload = json.loads(state_lines[0][2:])
                assert payload["state"] == "wrapper_connected"
                assert payload["wrapper_id"] == cache._wrapper_id
                assert payload["lang"] == "python"
            finally:
                stop_fn()
        finally:
            cache.stop_invalidation()
            server.close()

    def test_snapshot_request_returns_response(self):
        cache = make_cache()
        cache.put("SELECT 1", None, [(1,)], None)
        cache.get("SELECT 1", None)
        server, port = self._spawn_server()
        try:
            cache.connect_invalidation(port)
            conn, lines, stop_fn = _accept_with_buf(server)
            try:
                # Wait for wrapper_connected first so we know the socket
                # is wired.
                _wait_for(lambda: any(l.startswith("S:") for l in lines))
                # Send the snapshot request from the "proxy" side.
                conn.sendall(b"?:snapshot\n")
                _wait_for(lambda: any(l.startswith("R:") for l in lines))
                r_lines = [l for l in lines if l.startswith("R:")]
                assert r_lines, f"expected R: line, got {lines}"
                payload = json.loads(r_lines[0][2:])
                assert payload["wrapper_id"] == cache._wrapper_id
                assert payload["hits"] == 1
                assert payload["current_size_entries"] == 1
            finally:
                stop_fn()
        finally:
            cache.stop_invalidation()
            server.close()

    def test_report_stats_disabled_suppresses_emissions(self):
        os.environ["GOLDLAPEL_REPORT_STATS"] = "false"
        try:
            NativeCache._reset()
            cache = make_cache()
        finally:
            os.environ.pop("GOLDLAPEL_REPORT_STATS", None)
        assert cache._report_stats is False
        server, port = self._spawn_server()
        try:
            cache.connect_invalidation(port)
            conn, lines, stop_fn = _accept_with_buf(server)
            try:
                # Give it a moment then check no S: was sent.
                time.sleep(0.2)
                conn.sendall(b"?:snapshot\n")
                time.sleep(0.2)
                state_lines = [l for l in lines if l.startswith("S:") or l.startswith("R:")]
                assert state_lines == [], f"expected no S/R lines, got {state_lines}"
            finally:
                stop_fn()
        finally:
            cache.stop_invalidation()
            server.close()

    def test_unknown_proxy_prefix_silently_ignored(self):
        # Backwards-compat: the wrapper must not crash when a future
        # proxy sends an unknown prefix.
        cache = make_cache()
        # No exception raised.
        cache._process_signal("Z:future-prefix")
        cache._process_signal("$:bogus")


# --- L1 telemetry: hit-rate sliding window state changes ---

class TestHitRateStateChange:
    def test_hit_rate_dropped_fires_below_threshold(self):
        cache = make_cache()
        # Mock send_line to capture emissions instead of needing a socket.
        emissions = []
        cache._send_line = lambda line: emissions.append(line)
        # Warmup with mostly misses → hit rate well below LOW threshold.
        for i in range(_HIT_RATE_WARMUP + 50):
            cache.get(f"NEVERCACHED-{i}", None)
        # Hit rate should be 0%; expect a hit_rate_dropped emission.
        s_lines = [e for e in emissions if e.startswith("S:")]
        assert any("hit_rate_dropped" in s for s in s_lines), \
            f"expected hit_rate_dropped, got {s_lines}"

    def test_hit_rate_dropped_only_fires_once_until_recovered(self):
        cache = make_cache()
        emissions = []
        cache._send_line = lambda line: emissions.append(line)
        for i in range(_HIT_RATE_WARMUP + 50):
            cache.get(f"NEVERCACHED-{i}", None)
        first_count = sum(1 for e in emissions if "hit_rate_dropped" in e)
        # More misses — must NOT re-emit while latched.
        for i in range(50):
            cache.get(f"STILL-NOT-CACHED-{i}", None)
        second_count = sum(1 for e in emissions if "hit_rate_dropped" in e)
        assert first_count == second_count == 1


# --- L1 telemetry: eviction-rate state changes ---

class TestEvictionRateStateChange:
    def test_cache_full_fires_when_evictions_dominate(self):
        # Capacity 4 — every put past the 4th evicts. Window = 200 puts.
        cache = make_cache(max_entries=4)
        emissions = []
        cache._send_line = lambda line: emissions.append(line)
        # Need to fill the window before any state-change can fire.
        for i in range(_EVICT_RATE_WINDOW + 10):
            cache.put(f"SELECT {i}", None, [(i,)], None)
        s_lines = [e for e in emissions if "cache_full" in e]
        assert s_lines, "expected at least one cache_full emission"

    def test_cache_full_does_not_fire_below_window(self):
        # With fewer puts than the window, no state-change fires —
        # warmup gate.
        cache = make_cache(max_entries=2)
        emissions = []
        cache._send_line = lambda line: emissions.append(line)
        for i in range(_EVICT_RATE_WINDOW - 1):
            cache.put(f"SELECT {i}", None, [(i,)], None)
        # No cache_full yet — window not full.
        assert not any("cache_full" in e for e in emissions)


# --- L1 telemetry: process_request ---

class TestProcessRequest:
    def test_request_snapshot_emits_response(self):
        cache = make_cache()
        emissions = []
        cache._send_line = lambda line: emissions.append(line)
        cache._process_request("snapshot")
        r_lines = [e for e in emissions if e.startswith("R:")]
        assert len(r_lines) == 1
        payload = json.loads(r_lines[0][2:])
        assert payload["wrapper_id"] == cache._wrapper_id

    def test_request_empty_body_treated_as_snapshot(self):
        cache = make_cache()
        emissions = []
        cache._send_line = lambda line: emissions.append(line)
        cache._process_request("")
        r_lines = [e for e in emissions if e.startswith("R:")]
        assert len(r_lines) == 1

    def test_request_unknown_body_silently_dropped(self):
        cache = make_cache()
        emissions = []
        cache._send_line = lambda line: emissions.append(line)
        cache._process_request("future_request_type")
        r_lines = [e for e in emissions if e.startswith("R:")]
        assert r_lines == []
