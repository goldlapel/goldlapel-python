import json
import os
import socket
import threading
import time

import pytest

from goldlapel.cache import (
    NativeCache,
    CacheEntry,
    ConnectionGucState,
    _detect_write,
    _detect_writes_multi,
    _extract_tables,
    _make_key,
    _DDL_SENTINEL,
    _EVICT_RATE_HIGH,
    _EVICT_RATE_LOW,
    _EVICT_RATE_WINDOW,
    _TX_START,
    _TX_END,
    is_top_level_function_call,
    is_unsafe_guc,
    parse_set_command,
    split_statements,
    update_tx_state,
    SET_KIND_SET,
    SET_KIND_SET_LOCAL,
    SET_KIND_RESET,
    SET_KIND_RESET_ALL,
    SET_KIND_DISCARD_OTHER,
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
    # Cache keys are 3-tuples: (sql, params, state_hash). The state_hash
    # defaults to 0 (empty / fresh-connection state) for back-compat with
    # call sites that haven't yet observed any unsafe SET commands.
    def test_none_params(self):
        assert _make_key("SELECT 1", None) == ("SELECT 1", None, 0)

    def test_tuple_params(self):
        assert _make_key("SELECT $1", (42,)) == ("SELECT $1", (42,), 0)

    def test_list_params(self):
        assert _make_key("SELECT $1", [42]) == ("SELECT $1", (42,), 0)

    def test_dict_params(self):
        key = _make_key("SELECT %(id)s", {"id": 42, "name": "test"})
        assert key == ("SELECT %(id)s", (("id", 42), ("name", "test")), 0)

    def test_dict_params_order_independent(self):
        k1 = _make_key("SELECT 1", {"b": 2, "a": 1})
        k2 = _make_key("SELECT 1", {"a": 1, "b": 2})
        assert k1 == k2

    def test_state_hash_changes_key(self):
        k0 = _make_key("SELECT 1", None, 0)
        k1 = _make_key("SELECT 1", None, 12345)
        assert k0 != k1, "Different state_hash must produce a different cache key"

    def test_state_hash_default_is_zero(self):
        # When omitted, state_hash is 0 — preserves identical keys for
        # callers that haven't been updated to pass it through.
        k_default = _make_key("SELECT 1", (1,))
        k_explicit_zero = _make_key("SELECT 1", (1,), 0)
        assert k_default == k_explicit_zero


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

    # Regression: SELECT with `INTO` inside a string literal must not be
    # classified as SELECT-INTO DDL. The pre-fix tokenizer split on
    # whitespace only, so a literal like `'INSERT INTO orders'` smuggled
    # the bare word INTO into the scan and tripped the DDL sentinel,
    # forcing a full cache invalidation on a plain read.
    def test_select_with_into_in_single_quoted_literal_is_not_ddl(self):
        assert _detect_write(
            "SELECT 'INSERT INTO orders;' FROM audit_log"
        ) is None

    def test_select_with_into_in_double_quoted_identifier_is_not_ddl(self):
        assert _detect_write('SELECT * FROM "into_table"') is None

    def test_select_with_into_in_like_pattern_literal_is_not_ddl(self):
        assert _detect_write(
            "SELECT message FROM logs WHERE message LIKE '%INTO%'"
        ) is None

    def test_select_with_into_inside_doubled_quote_escape_is_not_ddl(self):
        assert _detect_write(
            "SELECT 'It''s INTO trouble' FROM notes"
        ) is None

    def test_real_select_into_new_table_is_still_ddl(self):
        # Regression guard: real SELECT-INTO DDL must still be flagged.
        assert _detect_write(
            "SELECT * INTO new_table FROM source"
        ) is _DDL_SENTINEL

    def test_real_select_into_temp_table_is_still_ddl(self):
        assert _detect_write(
            "SELECT id INTO TEMP scratch FROM source"
        ) is _DDL_SENTINEL


# --- Multi-statement write detection ---

class TestDetectWritesMulti:
    """Single-statement bodies short-circuit to `_detect_write`'s shape;
    multi-statement bodies split and union per-segment results."""

    def test_single_select_returns_none(self):
        assert _detect_writes_multi("SELECT * FROM orders") is None

    def test_single_insert_returns_set(self):
        assert _detect_writes_multi("INSERT INTO orders VALUES (1)") == {"orders"}

    def test_single_ddl_returns_sentinel(self):
        assert _detect_writes_multi("CREATE TABLE foo (id int)") is _DDL_SENTINEL

    def test_set_then_insert_unions(self):
        # The original bug: `SET ...; INSERT ...` looked like a SET to
        # `_detect_write` and was misclassified as a read.
        result = _detect_writes_multi(
            "SET app.user_id = '42'; INSERT INTO orders VALUES (1)"
        )
        assert result == {"orders"}

    def test_two_writes_two_tables(self):
        result = _detect_writes_multi(
            "INSERT INTO orders VALUES (1); UPDATE users SET v = 1"
        )
        assert result == {"orders", "users"}

    def test_ddl_anywhere_short_circuits_to_sentinel(self):
        # DDL as a later segment still trips global invalidation.
        assert _detect_writes_multi(
            "INSERT INTO orders VALUES (1); CREATE TABLE foo (id int)"
        ) is _DDL_SENTINEL

    def test_two_selects_returns_none(self):
        assert _detect_writes_multi("SELECT 1; SELECT 2") is None

    def test_set_then_select_returns_none(self):
        # `SET ...; SELECT ...` — the SET observation runs separately
        # (via `observe_sql`), and there's no actual write here.
        assert _detect_writes_multi("SET app.user_id = '42'; SELECT 1") is None

    def test_empty_returns_none(self):
        assert _detect_writes_multi("") is None

    def test_quoted_semicolon_does_not_split(self):
        # A `;` inside a string literal must not be treated as a statement
        # boundary — single-token detection still sees the INSERT.
        assert _detect_writes_multi("INSERT INTO orders VALUES ('a;b')") == {"orders"}

    def test_trailing_semicolon_treated_as_single_statement(self):
        # Matches the `observe_sql` fast-path heuristic.
        assert _detect_writes_multi("INSERT INTO orders VALUES (1);") == {"orders"}


# --- Multi-statement tx-state bookkeeping ---


class TestUpdateTxState:
    """A multi-statement Q like `BEGIN; INSERT INTO t VALUES (1); COMMIT`
    flips wrapper-side `_in_transaction` based on first token only (`BEGIN`)
    in the pre-fix code, but the COMMIT at the end means the server ends
    out-of-tx. The wrapper's tx state diverges from the server's, and
    subsequent reads bypass the cache forever (until a fresh BEGIN/COMMIT
    cycle resets it). Walking segments converges the wrapper's view to the
    server's end-state.
    """

    def test_begin_insert_commit_ends_out_of_tx(self):
        new_tx, had_marker = update_tx_state(
            False, "BEGIN; INSERT INTO t VALUES (1); COMMIT"
        )
        assert new_tx is False
        assert had_marker is True

    def test_begin_insert_no_commit_stays_in_tx(self):
        new_tx, had_marker = update_tx_state(
            False, "BEGIN; INSERT INTO t VALUES (1)"
        )
        assert new_tx is True
        assert had_marker is True

    def test_insert_commit_no_begin_ends_out_of_tx(self):
        # Already in tx (from a prior BEGIN), now COMMITting at end of body.
        new_tx, had_marker = update_tx_state(
            True, "INSERT INTO t VALUES (1); COMMIT"
        )
        assert new_tx is False
        assert had_marker is True

    def test_savepoint_release_inside_tx_stays_in_tx(self):
        # SAVEPOINT/RELEASE are intra-transaction markers — neither ends
        # the outer transaction. `RELEASE SAVEPOINT` commits a nested
        # savepoint but leaves the enclosing tx open server-side. The
        # wrapper must agree, otherwise subsequent in-tx reads would
        # route through the cache while the server is still in-tx
        # (stale-reads / read-your-own-writes violation).
        new_tx, had_marker = update_tx_state(
            True, "SAVEPOINT s1; INSERT INTO t VALUES (1); RELEASE s1"
        )
        assert new_tx is True
        assert had_marker is False

    def test_savepoint_alone_no_change(self):
        # Standalone `SAVEPOINT` — server errors if not already in a tx,
        # so wrapper state is already True when this fires. Either way:
        # don't flip, don't mark as a tx-boundary.
        new_tx, had_marker = update_tx_state(True, "SAVEPOINT s1")
        assert new_tx is True
        assert had_marker is False
        new_tx, had_marker = update_tx_state(False, "SAVEPOINT s1")
        assert new_tx is False
        assert had_marker is False

    def test_release_alone_no_change(self):
        # `RELEASE SAVEPOINT name` does NOT end the outer tx. Wrapper
        # must stay in_transaction=True so subsequent reads still bypass
        # the cache (server is still in-tx).
        new_tx, had_marker = update_tx_state(True, "RELEASE s1")
        assert new_tx is True
        assert had_marker is False
        new_tx, had_marker = update_tx_state(True, "RELEASE SAVEPOINT s1")
        assert new_tx is True
        assert had_marker is False

    def test_begin_savepoint_release_commit_full_cycle(self):
        # Full lifecycle: BEGIN; SAVEPOINT; SELECT; RELEASE; SELECT;
        # COMMIT. Only BEGIN and COMMIT are real boundaries — the
        # SAVEPOINT and RELEASE in the middle leave state alone, and
        # the trailing COMMIT closes the tx.
        new_tx, had_marker = update_tx_state(
            False,
            "BEGIN; SAVEPOINT s; SELECT 1; RELEASE s; SELECT 2; COMMIT",
        )
        assert new_tx is False
        assert had_marker is True

    def test_plain_select_no_change(self):
        new_tx_false, had_false = update_tx_state(False, "SELECT * FROM t")
        assert new_tx_false is False
        assert had_false is False
        new_tx_true, had_true = update_tx_state(True, "SELECT * FROM t")
        assert new_tx_true is True
        assert had_true is False

    def test_begin_alone(self):
        new_tx, had_marker = update_tx_state(False, "BEGIN")
        assert new_tx is True
        assert had_marker is True

    def test_commit_alone(self):
        new_tx, had_marker = update_tx_state(True, "COMMIT")
        assert new_tx is False
        assert had_marker is True

    def test_rollback_alone(self):
        new_tx, had_marker = update_tx_state(True, "ROLLBACK")
        assert new_tx is False
        assert had_marker is True

    def test_start_transaction(self):
        new_tx, had_marker = update_tx_state(False, "START TRANSACTION")
        assert new_tx is True
        assert had_marker is True

    def test_balanced_begin_rollback_nets_out_of_tx_with_marker(self):
        # `BEGIN; ROLLBACK` net state == prior state (False), but the body
        # had tx markers — callers must dispatch to the real connection
        # rather than treat as cacheable.
        new_tx, had_marker = update_tx_state(False, "BEGIN; ROLLBACK")
        assert new_tx is False
        assert had_marker is True

    def test_case_insensitive(self):
        new_tx, had_marker = update_tx_state(
            False, "begin; insert into t values (1); commit"
        )
        assert new_tx is False
        assert had_marker is True

    def test_trailing_semicolon_does_not_split(self):
        # `BEGIN;` is a single-statement body in our splitter. Still flips.
        new_tx, had_marker = update_tx_state(False, "BEGIN;")
        assert new_tx is True
        assert had_marker is True

    def test_quoted_semicolon_not_a_split(self):
        # `;` inside a string literal must not produce an extra segment.
        # The body is a single-statement INSERT; no tx marker.
        new_tx, had_marker = update_tx_state(
            False, "INSERT INTO t VALUES ('a;b')"
        )
        assert new_tx is False
        assert had_marker is False

    def test_empty_sql(self):
        new_tx, had_marker = update_tx_state(False, "")
        assert new_tx is False
        assert had_marker is False
        new_tx, had_marker = update_tx_state(True, "")
        assert new_tx is True
        assert had_marker is False


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


# --- native cache telemetry: counters + snapshot ---

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


# --- native cache telemetry: state-change emission via real socket ---

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


# --- native cache telemetry: eviction-rate state changes ---

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


# --- native cache telemetry: process_request ---

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


# --- native cache explicit-disable knob (`disable_native_cache`) ---

class TestDisableNativeCache:
    """`disable_native_cache=True` makes NativeCache a no-op pass-through:
    get() always returns None, put() is silent. Counters still tick so the
    dashboard sees a connected wrapper with "0 hits, N misses" — clear
    "native cache off" signal — and the snapshot carries `disabled: true`."""

    def test_disabled_default_false(self):
        cache = make_cache()
        assert cache._disabled is False

    def test_disabled_get_returns_none_and_bumps_misses(self):
        # Seed the cache before flipping the flag — proves the disabled
        # get() path doesn't peek at stored entries.
        cache = make_cache()
        cache.put("SELECT 1", None, [(1,)], None)
        # Sanity: hit works pre-flip.
        assert cache.get("SELECT 1", None) is not None
        cache.stats_hits = 0
        cache.stats_misses = 0
        # Flip and re-query.
        cache._disabled = True
        assert cache.get("SELECT 1", None) is None
        assert cache.get("SELECT NEW", None) is None
        assert cache.stats_hits == 0
        assert cache.stats_misses == 2

    def test_disabled_put_is_no_op(self):
        cache = make_cache()
        cache._disabled = True
        cache.put("SELECT 1", None, [(1,)], None)
        # Nothing stored — re-flipping the flag and re-getting still
        # misses (no entry was ever inserted).
        assert len(cache._cache) == 0
        cache._disabled = False
        assert cache.get("SELECT 1", None) is None

    def test_disabled_put_does_not_evict(self):
        # With disable_native_cache on we never store — so we never evict
        # either. stats_evictions stays 0, which the dashboard reads as
        # "no churn" (true: there's nothing to churn).
        cache = make_cache(max_entries=2)
        cache._disabled = True
        for i in range(50):
            cache.put(f"SELECT {i}", None, [(i,)], None)
        assert cache.stats_evictions == 0
        assert len(cache._cache) == 0

    def test_disabled_unhashable_params_still_count_as_miss(self):
        # In the normal path, unhashable params return None without
        # bumping misses (we never reached the cache lookup). In the
        # disabled path, we want every get() call to count as a miss
        # so the dashboard's "wrapper alive, querying" signal is
        # accurate regardless of param shape.
        cache = make_cache()
        cache._disabled = True
        cache.get("SELECT $1", [{"unhashable": [1, 2]}])
        assert cache.stats_misses == 1

    def test_disabled_invalidate_table_still_works(self):
        # Even with disable_native_cache on, invalidate_table is a no-op
        # in practice (cache is empty) — but should not crash if the
        # proxy sends an `I:<table>` signal. Counter does not bump
        # because no keys were affected.
        cache = make_cache()
        cache._disabled = True
        cache.invalidate_table("orders")
        assert cache.stats_invalidations == 0

    def test_snapshot_includes_disabled_true(self):
        cache = make_cache()
        cache._disabled = True
        snap = cache._build_snapshot()
        assert snap["disabled"] is True

    def test_snapshot_includes_disabled_false(self):
        cache = make_cache()
        snap = cache._build_snapshot()
        assert snap["disabled"] is False

    def test_disabled_snapshot_counters_reflect_misses_only(self):
        cache = make_cache()
        cache._disabled = True
        for i in range(5):
            cache.get(f"SELECT {i}", None)
        for i in range(3):
            cache.put(f"SELECT {i}", None, [(i,)], None)
        snap = cache._build_snapshot()
        assert snap["hits"] == 0
        assert snap["misses"] == 5
        assert snap["evictions"] == 0
        assert snap["current_size_entries"] == 0
        assert snap["disabled"] is True

    def test_construct_with_disabled_kwarg(self):
        # Singleton: first construction wins on most state, but `disabled`
        # is intentionally late-binding so wrap() can flip it.
        cache = NativeCache(disabled=True)
        cache._invalidation_connected = True
        assert cache._disabled is True
        # Re-constructing with disabled=False should flip the flag back.
        cache2 = NativeCache(disabled=False)
        assert cache2 is cache  # singleton
        assert cache._disabled is False


# --- L1 state-hash: unsafe-GUC classifier ---
#
# Mirrors the proxy's `src/guc_state.rs::tests::is_unsafe_guc` shape. Custom
# RLS state (anything namespaced via `.`) is unsafe by default; a short
# hardcoded list covers the well-known core GUCs that change query results
# without changing the SQL text. Match is case-insensitive everywhere.


class TestIsUnsafeGuc:
    def test_short_list_search_path(self):
        assert is_unsafe_guc("search_path")

    def test_short_list_role(self):
        assert is_unsafe_guc("role")

    def test_short_list_session_authorization(self):
        assert is_unsafe_guc("session_authorization")

    def test_short_list_default_transaction_isolation(self):
        assert is_unsafe_guc("default_transaction_isolation")

    def test_short_list_default_transaction_read_only(self):
        assert is_unsafe_guc("default_transaction_read_only")

    def test_short_list_transaction_isolation(self):
        assert is_unsafe_guc("transaction_isolation")

    def test_short_list_row_security(self):
        assert is_unsafe_guc("row_security")

    def test_short_list_case_insensitive(self):
        assert is_unsafe_guc("ROLE")
        assert is_unsafe_guc("Search_Path")
        assert is_unsafe_guc("SEARCH_PATH")

    def test_namespaced_unsafe(self):
        # Any GUC with a `.` in the name is treated as unsafe — the canonical
        # custom-RLS pattern (`SET app.user_id = '42'` / read via
        # current_setting('app.user_id')).
        assert is_unsafe_guc("app.user_id")
        assert is_unsafe_guc("myapp.tenant")
        assert is_unsafe_guc("rls.account")
        assert is_unsafe_guc("a.b.c")  # arbitrarily nested
        assert is_unsafe_guc("APP.USER")

    def test_safe_gucs(self):
        # Harmless GUCs — they don't change query results AND they don't
        # change how results are textually rendered on the wire. Safe to
        # leave out of the cache key.
        assert not is_unsafe_guc("application_name")
        assert not is_unsafe_guc("statement_timeout")
        assert not is_unsafe_guc("work_mem")
        assert not is_unsafe_guc("client_encoding")

    # -- output-format / locale GUCs are unsafe --
    #
    # These don't change which rows the server returns, but they DO change
    # how those rows are rendered on the wire. Two connections sharing a
    # cache slot under the same SQL would observe each other's rendering;
    # treat them as state-affecting. Same posture as the proxy's L2
    # classifier (`src/guc_state.rs`).

    def test_datestyle_unsafe(self):
        assert is_unsafe_guc("DateStyle")
        assert is_unsafe_guc("datestyle")
        assert is_unsafe_guc("DATESTYLE")

    def test_intervalstyle_unsafe(self):
        assert is_unsafe_guc("IntervalStyle")
        assert is_unsafe_guc("intervalstyle")

    def test_timezone_unsafe(self):
        # `timezone` (one word, lower) — tracked. The legacy two-word form
        # `SET TIME ZONE 'UTC'` is unparsable by `parse_set_command` and
        # returns None there (see test_rejects_set_time_zone_two_word_form),
        # but the classifier itself flags `timezone` as unsafe.
        assert is_unsafe_guc("timezone")
        assert is_unsafe_guc("TimeZone")
        assert is_unsafe_guc("TIMEZONE")

    def test_bytea_output_unsafe(self):
        assert is_unsafe_guc("bytea_output")
        assert is_unsafe_guc("BYTEA_OUTPUT")

    def test_lc_messages_unsafe(self):
        assert is_unsafe_guc("lc_messages")
        assert is_unsafe_guc("LC_MESSAGES")

    def test_lc_monetary_unsafe(self):
        assert is_unsafe_guc("lc_monetary")

    def test_lc_numeric_unsafe(self):
        assert is_unsafe_guc("lc_numeric")

    def test_lc_time_unsafe(self):
        assert is_unsafe_guc("lc_time")


# --- L1 state-hash: SET / RESET parser ---


class TestParseSetCommand:
    # -- shapes --

    def test_set_eq_quoted(self):
        assert parse_set_command("SET foo = 'bar'") == (SET_KIND_SET, "foo", "bar")

    def test_set_to_quoted(self):
        assert parse_set_command("SET foo TO 'bar'") == (SET_KIND_SET, "foo", "bar")

    def test_set_unquoted(self):
        assert parse_set_command("SET foo = 42") == (SET_KIND_SET, "foo", "42")

    def test_set_session_modifier(self):
        # SESSION is the default — same effect as bare SET.
        assert parse_set_command("SET SESSION foo = 'bar'") == (SET_KIND_SET, "foo", "bar")

    def test_set_local_modifier(self):
        assert parse_set_command("SET LOCAL foo = 'bar'") == (SET_KIND_SET_LOCAL, "foo", "bar")

    def test_reset_named(self):
        assert parse_set_command("RESET foo") == (SET_KIND_RESET, "foo", None)

    def test_reset_all(self):
        assert parse_set_command("RESET ALL") == (SET_KIND_RESET_ALL, None, None)

    # -- case + whitespace + semicolon --

    def test_case_insensitive_keywords(self):
        assert parse_set_command("set foo = 'bar'") == (SET_KIND_SET, "foo", "bar")
        assert parse_set_command("Set Local foo To 'bar'") == (SET_KIND_SET_LOCAL, "foo", "bar")
        assert parse_set_command("reset all") == (SET_KIND_RESET_ALL, None, None)

    def test_lowercases_guc_name(self):
        # GUC names are stored lowercased so `SET App.User_ID` and
        # `SET app.user_id` collapse onto the same state slot.
        assert parse_set_command("SET App.User_ID = '42'") == (SET_KIND_SET, "app.user_id", "42")

    def test_tolerates_trailing_semicolon(self):
        assert parse_set_command("SET foo = 'bar';") == (SET_KIND_SET, "foo", "bar")
        assert parse_set_command("RESET foo ;") == (SET_KIND_RESET, "foo", None)

    def test_tolerates_extra_whitespace(self):
        assert parse_set_command("   SET    foo   =   'bar'   ") == (SET_KIND_SET, "foo", "bar")

    def test_glued_equals(self):
        # Some clients send `SET name=value` with no spaces.
        assert parse_set_command("SET app.user_id='42'") == (SET_KIND_SET, "app.user_id", "42")

    def test_double_quoted_value(self):
        assert parse_set_command('SET foo = "bar"') == (SET_KIND_SET, "foo", "bar")

    def test_double_quoted_name(self):
        # `"app.user_id"` is a quoted identifier — same value as bare.
        assert parse_set_command('SET "app.user_id" = \'42\'') == (SET_KIND_SET, "app.user_id", "42")

    # -- rejects --

    def test_rejects_non_set_statements(self):
        assert parse_set_command("SELECT 1") is None
        assert parse_set_command("BEGIN") is None
        assert parse_set_command("UPDATE t SET x = 1") is None

    def test_rejects_empty(self):
        assert parse_set_command("") is None
        assert parse_set_command("   ") is None
        assert parse_set_command(";") is None

    def test_rejects_set_without_value(self):
        assert parse_set_command("SET foo =") is None
        assert parse_set_command("SET foo TO") is None
        assert parse_set_command("SET foo") is None

    def test_rejects_reset_with_garbage(self):
        # `RESET foo bar` — second token after RESET is unexpected.
        assert parse_set_command("RESET foo bar") is None

    def test_rejects_set_time_zone_two_word_form(self):
        # The legacy `SET TIME ZONE 'UTC'` form is not modelled — timezone
        # is harmless and the unusual two-word GUC name doesn't fit. We
        # return None so the caller treats it as not-a-trackable-SET (i.e.
        # cache-safe), which is correct.
        assert parse_set_command("SET TIME ZONE 'UTC'") is None


# --- L1 state-hash: top-level statement splitter ---


class TestSplitStatements:
    def test_simple_two_statements(self):
        assert split_statements("SET foo = '42'; SELECT 1") == ["SET foo = '42'", "SELECT 1"]

    def test_drops_empty_segments(self):
        # Trailing `;`, leading `;`, doubled `;;` all produce empty
        # segments which we drop.
        assert split_statements("; SET foo = '42';;SELECT 1;") == [
            "SET foo = '42'", "SELECT 1",
        ]

    def test_respects_single_quotes(self):
        # The `;` inside the literal must NOT split the statement.
        assert split_statements("SET foo = 'a;b'; SELECT 1") == [
            "SET foo = 'a;b'", "SELECT 1",
        ]

    def test_respects_double_quotes(self):
        assert split_statements('SET "app;guc" = \'x\'; SELECT 1') == [
            'SET "app;guc" = \'x\'', "SELECT 1",
        ]

    def test_handles_doubled_quote_escape(self):
        # PG escapes a literal `'` inside a string by doubling: `''`.
        assert split_statements("SET foo = 'it''s; ok'; SELECT 1") == [
            "SET foo = 'it''s; ok'", "SELECT 1",
        ]

    def test_single_statement_pass_through(self):
        assert split_statements("SET foo = '42'") == ["SET foo = '42'"]

    def test_empty(self):
        assert split_statements("") == []
        assert split_statements("   ") == []
        assert split_statements(";;;") == []


# --- L1 state-hash: ConnectionGucState ---


class TestConnectionGucState:
    def test_empty_state_hash_is_zero(self):
        s = ConnectionGucState()
        assert s.hash == 0

    def test_safe_set_does_not_change_hash(self):
        s = ConnectionGucState()
        s.observe_sql("SET application_name = 'foo'")
        assert s.hash == 0
        s.observe_sql("SET statement_timeout = 5000")
        assert s.hash == 0
        s.observe_sql("SET work_mem = '64MB'")
        assert s.hash == 0

    def test_unsafe_set_changes_hash(self):
        s = ConnectionGucState()
        h0 = s.hash
        s.observe_sql("SET app.user_id = '42'")
        assert s.hash != h0

    def test_same_unsafe_set_yields_same_hash_on_two_states(self):
        # Two independent connections that have applied the same unsafe
        # SET converge on the same hash → cache slot can be safely shared.
        a = ConnectionGucState()
        b = ConnectionGucState()
        a.observe_sql("SET app.user_id = '42'")
        b.observe_sql("SET app.user_id = '42'")
        assert a.hash == b.hash

    def test_different_unsafe_values_yield_different_hashes(self):
        # The whole point — two connections with different `app.user_id`
        # values must NOT share a cache slot.
        a = ConnectionGucState()
        b = ConnectionGucState()
        a.observe_sql("SET app.user_id = '42'")
        b.observe_sql("SET app.user_id = '43'")
        assert a.hash != b.hash

    def test_insertion_order_does_not_matter(self):
        # State hash must be order-independent (sorted dict / map iteration).
        a = ConnectionGucState()
        a.observe_sql("SET app.user_id = '42'")
        a.observe_sql("SET app.tenant = 'alpha'")

        b = ConnectionGucState()
        b.observe_sql("SET app.tenant = 'alpha'")
        b.observe_sql("SET app.user_id = '42'")

        assert a.hash == b.hash

    def test_reset_returns_hash_to_baseline(self):
        s = ConnectionGucState()
        baseline = s.hash
        s.observe_sql("SET app.user_id = '42'")
        assert s.hash != baseline
        s.observe_sql("RESET app.user_id")
        assert s.hash == baseline

    def test_reset_all_clears_all_unsafe_state(self):
        s = ConnectionGucState()
        s.observe_sql("SET app.user_id = '42'")
        s.observe_sql("SET search_path TO 'tenant_a'")
        s.observe_sql("SET role = 'app_user'")
        assert s.hash != 0
        s.observe_sql("RESET ALL")
        assert s.hash == 0

    def test_set_local_does_not_change_hash(self):
        # SET LOCAL is intentionally ignored for state-hash purposes —
        # the wrapper bypasses the cache for in-transaction reads anyway.
        s = ConnectionGucState()
        s.observe_sql("SET LOCAL app.user_id = '42'")
        assert s.hash == 0

    def test_observe_sql_returns_change_flag(self):
        s = ConnectionGucState()
        assert s.observe_sql("SET app.user_id = '42'") is True
        assert s.observe_sql("SELECT 1") is False
        assert s.observe_sql("SET application_name = 'foo'") is False
        assert s.observe_sql("RESET app.user_id") is True

    def test_reset_safe_guc_is_noop(self):
        s = ConnectionGucState()
        s.observe_sql("SET app.user_id = '42'")
        h = s.hash
        s.observe_sql("RESET work_mem")  # safe — should not perturb.
        assert s.hash == h

    def test_overwrite_unsafe_value_changes_hash(self):
        s = ConnectionGucState()
        s.observe_sql("SET app.user_id = '42'")
        h1 = s.hash
        s.observe_sql("SET app.user_id = '43'")
        assert s.hash != h1

    def test_reapply_same_value_does_not_re_emit(self):
        # Setting the same value twice should be observably stable —
        # `observe_sql` returns False on the second application because
        # the hash didn't move.
        s = ConnectionGucState()
        assert s.observe_sql("SET app.user_id = '42'") is True
        assert s.observe_sql("SET app.user_id = '42'") is False

    def test_observe_multi_statement_applies_all_sets(self):
        # Real-world pattern: client batches a SET with the query.
        s = ConnectionGucState()
        s.observe_sql("SET app.user_id = '42'; SELECT * FROM accounts")
        assert s.hash != 0

    def test_observe_multi_statement_two_unsafe_sets_match_separate(self):
        a = ConnectionGucState()
        a.observe_sql("SET app.user_id = '42'")
        a.observe_sql("SET app.tenant = 'alpha'")

        b = ConnectionGucState()
        b.observe_sql("SET app.user_id = '42'; SET app.tenant = 'alpha'")

        assert a.hash == b.hash

    def test_observe_multi_statement_with_quoted_semicolon(self):
        # The `;` inside the value must not be treated as a statement
        # separator.
        s = ConnectionGucState()
        s.observe_sql("SET app.tenant = 'has;semicolon'; SELECT 1")
        assert s.hash != 0

    def test_reset_unset_unsafe_guc_is_noop(self):
        # `RESET app.user_id` on a fresh state (the GUC was never SET) is
        # a no-op — hash stays at baseline, observe_sql returns False.
        s = ConnectionGucState()
        assert s.hash == 0
        assert s.observe_sql("RESET app.user_id") is False
        assert s.hash == 0

    def test_reset_all_on_empty_state_is_noop(self):
        # `RESET ALL` on a fresh state with no unsafe GUCs ever set is a
        # no-op — we don't recompute the hash, and observe_sql signals no
        # change.
        s = ConnectionGucState()
        assert s.hash == 0
        assert s.observe_sql("RESET ALL") is False
        assert s.hash == 0

    def test_first_set_then_separate_select_keys_under_post_set_hash(self):
        # Sequenced (not multi-statement) form: SET first, then a separate
        # SELECT. Proves the SELECT issued AFTER the SET sees the post-SET
        # state hash — equivalent end state to the single-shot
        # `SET ...; SELECT ...` form already covered.
        s = ConnectionGucState()
        s.observe_sql("SET app.user_id = '42'")
        post_set_hash = s.hash
        assert post_set_hash != 0
        # Subsequent non-SET statement does not change the state.
        assert s.observe_sql("SELECT name FROM accounts") is False
        assert s.hash == post_set_hash


# --- L1 state-hash: cache key correctness ---
#
# These exercise the contract end-to-end on the cache. The key invariant:
# two cache writes with identical SQL+params but different state_hash
# values must map to different slots, so reads from the wrong-hash side
# never see the other slot's rows. Mirrors the security guarantee the
# proxy gives at L2.


class TestL1CacheStateHash:
    def test_different_state_hash_different_slot(self):
        cache = make_cache()
        # User A writes their row under hash 111.
        cache.put("SELECT * FROM accounts", None, [("alice",)], None, 111)
        # User B reads with their own hash 222 — must miss.
        assert cache.get("SELECT * FROM accounts", None, 222) is None

    def test_same_state_hash_hits(self):
        cache = make_cache()
        cache.put("SELECT * FROM accounts", None, [("alice",)], None, 111)
        entry = cache.get("SELECT * FROM accounts", None, 111)
        assert entry is not None
        assert entry.rows == [("alice",)]

    def test_default_state_hash_zero_isolated_from_nonzero(self):
        cache = make_cache()
        # Caller that hasn't passed state_hash uses default (0).
        cache.put("SELECT 1", None, [(1,)], None)
        # Caller with a non-zero hash reading the same SQL must miss —
        # they're a different security context.
        assert cache.get("SELECT 1", None, 999) is None

    def test_two_states_two_slots_no_cross_contamination(self):
        cache = make_cache()
        cache.put("SELECT * FROM accounts", None, [("alice",)], None, 111)
        cache.put("SELECT * FROM accounts", None, [("bob",)], None, 222)
        # Each side reads only its own row.
        assert cache.get("SELECT * FROM accounts", None, 111).rows == [("alice",)]
        assert cache.get("SELECT * FROM accounts", None, 222).rows == [("bob",)]

    def test_state_hash_zero_is_shared_baseline(self):
        # Two fresh-state callers (both hash=0) share their cache slots —
        # this is correct: a connection that has never set an unsafe GUC
        # has the same security context as any other such connection.
        cache = make_cache()
        cache.put("SELECT 1", None, [(1,)], None, 0)
        assert cache.get("SELECT 1", None, 0).rows == [(1,)]
        # Default arg path also lands on the same slot.
        assert cache.get("SELECT 1", None).rows == [(1,)]

    def test_invalidate_table_clears_across_state_hashes(self):
        # Table-level invalidation walks the table_index, which doesn't
        # carry the state_hash — so it correctly flushes ALL state-hash
        # variants of a table when the data underneath changes (single
        # source of truth: a write to `accounts` invalidates every
        # cached SELECT on accounts regardless of which RLS context
        # populated it).
        cache = make_cache()
        cache.put("SELECT * FROM accounts", None, [("alice",)], None, 111)
        cache.put("SELECT * FROM accounts", None, [("bob",)], None, 222)
        cache.invalidate_table("accounts")
        assert cache.get("SELECT * FROM accounts", None, 111) is None
        assert cache.get("SELECT * FROM accounts", None, 222) is None


# --- DISCARD parsing + state effects ---
#
# DISCARD is the protocol-level "reset this connection" surface PG offers.
# `DISCARD ALL` is the canonical pool-on-release statement (asyncpg /
# psycopg / SQLAlchemy default). Recognizing it inline lets the wrapper
# clear its state map without an extra round-trip.


class TestDiscardParsing:
    def test_discard_all_parses_as_reset_all(self):
        # Same kind as `RESET ALL` — both clear the entire session state.
        assert parse_set_command("DISCARD ALL") == (SET_KIND_RESET_ALL, None, None)

    def test_discard_all_case_insensitive(self):
        assert parse_set_command("discard all") == (SET_KIND_RESET_ALL, None, None)
        assert parse_set_command("Discard ALL") == (SET_KIND_RESET_ALL, None, None)

    def test_discard_all_trailing_semicolon(self):
        assert parse_set_command("DISCARD ALL;") == (SET_KIND_RESET_ALL, None, None)

    def test_discard_plans_no_op_for_state(self):
        # Recognized as a "DISCARD other" command — distinct kind so the
        # state machine can clear the dirty flag, but doesn't touch the
        # state map (DISCARD PLANS only clears the prepared-statement
        # cache server-side, no GUC reset).
        assert parse_set_command("DISCARD PLANS") == (SET_KIND_DISCARD_OTHER, None, None)

    def test_discard_sequences_no_op_for_state(self):
        assert parse_set_command("DISCARD SEQUENCES") == (SET_KIND_DISCARD_OTHER, None, None)

    def test_discard_temp_no_op_for_state(self):
        assert parse_set_command("DISCARD TEMP") == (SET_KIND_DISCARD_OTHER, None, None)

    def test_discard_temporary_no_op_for_state(self):
        # `TEMPORARY` is the long-form alias of `TEMP`.
        assert parse_set_command("DISCARD TEMPORARY") == (SET_KIND_DISCARD_OTHER, None, None)

    def test_discard_unknown_subcommand_returns_none(self):
        # `DISCARD FOO` is a syntax error server-side. Parser is
        # conservative: returns None so the wrapper treats it as
        # "not a trackable command" and the dirty flag persists.
        assert parse_set_command("DISCARD FOO") is None

    def test_discard_no_subcommand_returns_none(self):
        assert parse_set_command("DISCARD") is None


class TestDiscardStateEffects:
    def test_discard_all_clears_state_map(self):
        # Mirrors the RESET ALL behaviour — state map empties, hash
        # returns to 0. Plus: dirty flag clears.
        s = ConnectionGucState()
        s.observe_sql("SET app.user_id = '42'")
        s.observe_sql("SET app.tenant = 'alpha'")
        assert s.hash != 0
        assert s.dirty is True
        s.observe_sql("DISCARD ALL")
        assert s.hash == 0
        assert s.dirty is False

    def test_discard_other_does_not_change_hash(self):
        # DISCARD PLANS / SEQUENCES / TEMP — touch nothing in pg_settings,
        # so the state map is unchanged.
        s = ConnectionGucState()
        s.observe_sql("SET app.user_id = '42'")
        h_before = s.hash
        for cmd in ("DISCARD PLANS", "DISCARD SEQUENCES", "DISCARD TEMP", "DISCARD TEMPORARY"):
            s.observe_sql(cmd)
            assert s.hash == h_before, f"{cmd} must not perturb the state hash"

    def test_discard_other_clears_dirty_flag(self):
        # DISCARD PLANS / SEQUENCES / TEMP are still strong "the pool
        # reset this connection" signals, so observing one clears the
        # dirty flag — even though the GUC state map is unchanged.
        s = ConnectionGucState()
        s.observe_sql("SET app.user_id = '42'")
        assert s.dirty is True
        s.observe_sql("DISCARD PLANS")
        assert s.dirty is False
        # Re-mark and try the others.
        s.observe_sql("SET app.user_id = '42'")
        assert s.dirty is True
        s.observe_sql("DISCARD SEQUENCES")
        assert s.dirty is False

    def test_discard_other_increments_observed_counter(self):
        # The discards-observed counter is monotonic across all DISCARD
        # forms — pool integrations / verify paths can read it to confirm
        # at least one DISCARD was seen since the last dirty mark.
        s = ConnectionGucState()
        assert s.discards_observed == 0
        s.observe_sql("DISCARD PLANS")
        assert s.discards_observed == 1
        s.observe_sql("DISCARD ALL")
        assert s.discards_observed == 2

    def test_set_then_discard_then_set_keys_under_post_discard_hash(self):
        # Lifecycle: SET (hash=h1, dirty=True) → DISCARD ALL (hash=0,
        # dirty=False) → SET different value (hash=h2, dirty=True). Each
        # phase produces an independent slot.
        s = ConnectionGucState()
        s.observe_sql("SET app.user_id = '42'")
        h1 = s.hash
        s.observe_sql("DISCARD ALL")
        assert s.hash == 0
        s.observe_sql("SET app.user_id = '99'")
        h2 = s.hash
        assert h2 != 0
        assert h2 != h1


# --- set_config() function form ---
#
# `SELECT set_config('app.user_id', '42', false)` is the Supabase /
# PostgREST canonical RLS-context shape (and PG's docs document it as
# directly equivalent to SET / SET LOCAL). The wrapper must fold it
# into the state hash inline so the very-next SELECT keys correctly.


class TestSetConfigParsing:
    def test_set_config_session_level(self):
        # is_local=false → behaves like a regular SET.
        assert parse_set_command(
            "SELECT set_config('app.user_id', '42', false)"
        ) == (SET_KIND_SET, "app.user_id", "42")

    def test_set_config_local(self):
        # is_local=true → behaves like SET LOCAL (ignored for state hash).
        assert parse_set_command(
            "SELECT set_config('app.user_id', '42', true)"
        ) == (SET_KIND_SET_LOCAL, "app.user_id", "42")

    def test_set_config_pg_catalog_qualifier(self):
        # The schema-qualified form is what asyncpg sometimes emits
        # under-the-hood for safety.
        assert parse_set_command(
            "SELECT pg_catalog.set_config('app.user_id', '42', false)"
        ) == (SET_KIND_SET, "app.user_id", "42")

    def test_set_config_pg_catalog_with_whitespace(self):
        assert parse_set_command(
            "SELECT pg_catalog . set_config('app.user_id', '42', false)"
        ) == (SET_KIND_SET, "app.user_id", "42")

    def test_set_config_case_insensitive(self):
        assert parse_set_command(
            "select SET_CONFIG('app.user_id', '42', FALSE)"
        ) == (SET_KIND_SET, "app.user_id", "42")

    def test_set_config_lowercases_name(self):
        # Same as the SET path — `App.User_ID` → `app.user_id`.
        assert parse_set_command(
            "SELECT set_config('App.User_ID', '42', false)"
        ) == (SET_KIND_SET, "app.user_id", "42")

    def test_set_config_value_with_apostrophe_doubled_escape(self):
        # PG escapes `'` inside a single-quoted literal by doubling.
        cmd = parse_set_command(
            "SELECT set_config('app.greeting', 'it''s ok', false)"
        )
        assert cmd == (SET_KIND_SET, "app.greeting", "it's ok")

    def test_set_config_quoted_bool_true(self):
        # `'true'` quoted form is also accepted — PG coerces.
        assert parse_set_command(
            "SELECT set_config('app.user_id', '42', 'true')"
        ) == (SET_KIND_SET_LOCAL, "app.user_id", "42")

    def test_set_config_quoted_bool_false(self):
        assert parse_set_command(
            "SELECT set_config('app.user_id', '42', 'false')"
        ) == (SET_KIND_SET, "app.user_id", "42")

    def test_set_config_t_alias(self):
        # PG accepts `t` / `f` / `on` / `off` / `1` / `0`.
        assert parse_set_command(
            "SELECT set_config('app.user_id', '42', t)"
        ) == (SET_KIND_SET_LOCAL, "app.user_id", "42")

    def test_set_config_param_placeholders_returns_none(self):
        # We deliberately don't try to parse `$1`-style positional params
        # — the connection gets marked dirty by the post-call verify
        # heuristic instead, which is correct (a verify will reconcile
        # state).
        assert parse_set_command(
            "SELECT set_config($1, $2, $3)"
        ) is None

    def test_set_config_with_trailing_semicolon(self):
        assert parse_set_command(
            "SELECT set_config('app.user_id', '42', false);"
        ) == (SET_KIND_SET, "app.user_id", "42")

    def test_other_select_function_returns_none(self):
        # `SELECT now()` — a stateless builtin — must NOT parse as a
        # set_config call. The post-call verify path handles unknown
        # functions separately.
        assert parse_set_command("SELECT now()") is None

    def test_set_config_double_quoted_name(self):
        # PG accepts `"app.user_id"` as a quoted identifier-as-string;
        # set_config just takes the literal text.
        assert parse_set_command(
            'SELECT set_config("app.user_id", \'42\', false)'
        ) == (SET_KIND_SET, "app.user_id", "42")


class TestSetConfigStateEffects:
    def test_set_config_session_changes_state_hash(self):
        s = ConnectionGucState()
        baseline = s.hash
        s.observe_sql("SELECT set_config('app.user_id', '42', false)")
        assert s.hash != baseline

    def test_set_config_local_does_not_change_state_hash(self):
        s = ConnectionGucState()
        s.observe_sql("SELECT set_config('app.user_id', '42', true)")
        assert s.hash == 0

    def test_set_config_matches_set_hash(self):
        # Two paths to the same end state must produce the SAME hash —
        # otherwise a SET and a set_config of the same value would key
        # under different cache slots, fragmenting the cache for no
        # security reason.
        a = ConnectionGucState()
        b = ConnectionGucState()
        a.observe_sql("SET app.user_id = '42'")
        b.observe_sql("SELECT set_config('app.user_id', '42', false)")
        assert a.hash == b.hash

    def test_pg_catalog_set_config_matches_bare_set(self):
        a = ConnectionGucState()
        b = ConnectionGucState()
        a.observe_sql("SET app.user_id = '42'")
        b.observe_sql("SELECT pg_catalog.set_config('app.user_id', '42', false)")
        assert a.hash == b.hash

    def test_set_config_session_marks_dirty(self):
        s = ConnectionGucState()
        s.observe_sql("SELECT set_config('app.user_id', '42', false)")
        assert s.dirty is True


# --- Top-level function-call detection (post-call verify trigger) ---


class TestIsTopLevelFunctionCall:
    def test_simple_function_call(self):
        # A bare `SELECT my_func()` with no FROM / WHERE / etc. is a
        # statement-level function call — the candidate shape for
        # post-call verify.
        assert is_top_level_function_call("SELECT my_func()") is True

    def test_call_with_args(self):
        assert is_top_level_function_call("SELECT my_func(1, 2, 'three')") is True

    def test_schema_qualified_call(self):
        assert is_top_level_function_call("SELECT public.my_func()") is True

    def test_call_with_trailing_semicolon(self):
        assert is_top_level_function_call("SELECT my_func();") is True

    def test_select_with_from_returns_false(self):
        # A SELECT that has a FROM clause is a normal query, not a
        # statement-level function call.
        assert is_top_level_function_call("SELECT my_func() FROM tbl") is False

    def test_select_column_returns_false(self):
        assert is_top_level_function_call("SELECT name FROM accounts") is False

    def test_select_constant_returns_false(self):
        assert is_top_level_function_call("SELECT 1") is False

    def test_safe_builtin_returns_false(self):
        # Stateless builtins skip the verify (no possible state mutation).
        assert is_top_level_function_call("SELECT now()") is False
        assert is_top_level_function_call("SELECT current_timestamp()") is False
        assert is_top_level_function_call("SELECT version()") is False

    def test_set_config_returns_false(self):
        # `set_config` is in the safe-builtins list — the inline parser
        # already captured the mutation, no verify needed.
        assert is_top_level_function_call(
            "SELECT set_config('app.user_id', '42', false)"
        ) is False

    def test_pg_catalog_set_config_returns_false(self):
        assert is_top_level_function_call(
            "SELECT pg_catalog.set_config('app.user_id', '42', false)"
        ) is False

    def test_unbalanced_parens_returns_false(self):
        # Defensive — a malformed call shouldn't trip the verify path.
        assert is_top_level_function_call("SELECT my_func(") is False

    def test_function_with_string_containing_paren(self):
        # Balanced-paren scanner respects string literals so a `)` inside
        # a string doesn't close the call early.
        assert is_top_level_function_call(
            "SELECT my_func('text with ) inside')"
        ) is True

    def test_case_insensitive_select(self):
        assert is_top_level_function_call("select my_func()") is True

    def test_leading_whitespace(self):
        assert is_top_level_function_call("   \n SELECT my_func()") is True

    def test_empty_returns_false(self):
        assert is_top_level_function_call("") is False
        assert is_top_level_function_call("   ") is False

    def test_not_select_returns_false(self):
        assert is_top_level_function_call("CALL my_proc()") is False
        assert is_top_level_function_call("UPDATE t SET x = 1") is False


# --- Verify-on-checkout: pg_settings reconciliation ---


class TestMaybeVerifySync:
    """Sync verify reconstructs the unsafe-GUC state map from a
    `SELECT name, setting FROM pg_settings WHERE source='session'`
    round-trip. Used by pool integrations whose drivers don't issue
    DISCARD ALL on release."""

    def _fake_conn(self, rows):
        """Mock a sync conn whose `cursor().fetchall()` returns `rows`."""
        from unittest.mock import MagicMock
        cursor = MagicMock()
        cursor.fetchall.return_value = rows
        conn = MagicMock()
        conn.cursor.return_value = cursor
        return conn, cursor

    def test_no_op_when_not_dirty(self):
        s = ConnectionGucState()
        conn, cursor = self._fake_conn([("app.user_id", "42")])
        # Not dirty — verify should skip the round-trip.
        ran = s.maybe_verify(conn)
        assert ran is False
        cursor.execute.assert_not_called()

    def test_runs_when_dirty(self):
        s = ConnectionGucState()
        s.observe_sql("SET app.user_id = '42'")
        assert s.dirty is True
        conn, cursor = self._fake_conn([("app.user_id", "42")])
        ran = s.maybe_verify(conn)
        assert ran is True
        # Verify cleared dirty flag.
        assert s.dirty is False
        # Verify hit pg_settings.
        cursor.execute.assert_called_once()
        sql_used = cursor.execute.call_args[0][0]
        assert "pg_settings" in sql_used
        assert "source = 'session'" in sql_used

    def test_overwrites_state_with_pg_settings_view(self):
        # Server says `app.tenant=alpha` is set; wrapper had
        # `app.user_id=42` in its map. After verify, the server view wins.
        s = ConnectionGucState()
        s.observe_sql("SET app.user_id = '42'")
        conn, _ = self._fake_conn([("app.tenant", "alpha")])
        s.maybe_verify(conn)
        # Hash should reflect the new (verified) state — not the old one.
        peer = ConnectionGucState()
        peer.observe_sql("SET app.tenant = 'alpha'")
        assert s.hash == peer.hash

    def test_skips_safe_gucs_in_pg_settings_rows(self):
        # pg_settings returns ALL session-set GUCs, including harmless
        # ones we explicitly don't track. Filter them out to keep the
        # state map tightly scoped.
        s = ConnectionGucState()
        s.observe_sql("SET app.user_id = '42'")
        conn, _ = self._fake_conn([
            ("app.user_id", "42"),
            ("application_name", "myapp"),  # safe — must be ignored
            ("statement_timeout", "1000"),  # safe — must be ignored
            ("work_mem", "64MB"),           # safe — must be ignored
        ])
        s.maybe_verify(conn)
        # Hash should match a peer connection that ONLY set app.user_id=42.
        peer = ConnectionGucState()
        peer.observe_sql("SET app.user_id = '42'")
        assert s.hash == peer.hash

    def test_swallows_errors_and_keeps_dirty(self):
        # If verify itself errors, the dirty flag persists for a
        # future retry. The user's query must NOT see the verify error.
        from unittest.mock import MagicMock
        conn = MagicMock()
        conn.cursor.side_effect = RuntimeError("boom")
        s = ConnectionGucState()
        s.observe_sql("SET app.user_id = '42'")
        ran = s.maybe_verify(conn)
        assert ran is False
        assert s.dirty is True  # still dirty, retry on next checkout

    def test_no_discard_observed_means_verify_runs(self):
        # The `discards_observed` counter is part of the `dirty` story —
        # if a DISCARD lands between the SET and the verify, the dirty
        # flag would already be cleared. Direct test that verify only
        # fires when there's been no DISCARD since dirty was set.
        s = ConnectionGucState()
        s.observe_sql("SET app.user_id = '42'")
        s.observe_sql("DISCARD ALL")  # clears dirty
        conn, cursor = self._fake_conn([])
        ran = s.maybe_verify(conn)
        assert ran is False
        cursor.execute.assert_not_called()


# --- Async post-call verify (item #6) ---


class TestPostCallVerifyAsync:
    """The async post-call verify fires after the user's response is
    dispatched, never blocking the user's hot path. State map is
    reconciled from pg_settings; failures mark the connection dirty
    without surfacing to the user."""

    @pytest.mark.asyncio
    async def test_async_verify_reconciles_state(self):
        from unittest.mock import AsyncMock
        # Simulate a connection whose `fetch()` returns the verify rows
        # in the asyncpg shape (Record-like; ours just accepts any
        # tuple-like).
        conn = AsyncMock()
        conn.fetch.return_value = [
            ("app.user_id", "999"),  # post-call value (function set this)
        ]
        s = ConnectionGucState()
        s.mark_dirty()
        ran = await s.maybe_verify_async(conn)
        assert ran is True
        # Hash now reflects the post-call value.
        peer = ConnectionGucState()
        peer.observe_sql("SET app.user_id = '999'")
        assert s.hash == peer.hash
        assert s.dirty is False

    @pytest.mark.asyncio
    async def test_async_verify_errors_swallowed(self):
        from unittest.mock import AsyncMock
        conn = AsyncMock()
        conn.fetch.side_effect = RuntimeError("connection blew up")
        s = ConnectionGucState()
        s.mark_dirty()
        # Must not raise.
        ran = await s.maybe_verify_async(conn)
        assert ran is False
        assert s.dirty is True  # ready to retry

    @pytest.mark.asyncio
    async def test_async_verify_skipped_when_not_dirty(self):
        from unittest.mock import AsyncMock
        conn = AsyncMock()
        s = ConnectionGucState()
        ran = await s.maybe_verify_async(conn)
        assert ran is False
        conn.fetch.assert_not_called()


# --- mark_dirty (post-call function-call dirty signal) ---


class TestMarkDirty:
    def test_mark_dirty_idempotent(self):
        s = ConnectionGucState()
        assert s.dirty is False
        s.mark_dirty()
        assert s.dirty is True
        s.mark_dirty()
        assert s.dirty is True

    def test_mark_dirty_does_not_change_hash(self):
        s = ConnectionGucState()
        s.observe_sql("SET app.user_id = '42'")
        h = s.hash
        s.mark_dirty()
        assert s.hash == h

    def test_mark_dirty_cleared_by_discard(self):
        s = ConnectionGucState()
        s.mark_dirty()
        assert s.dirty is True
        s.observe_sql("DISCARD ALL")
        assert s.dirty is False

