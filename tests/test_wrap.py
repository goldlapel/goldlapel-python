from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from goldlapel.cache import NativeCache
from goldlapel.wrap import wrap, CachedConnection, CachedCursor


@pytest.fixture(autouse=True)
def reset_cache():
    NativeCache._reset()
    # `goldlapel.wrap` is shadowed at module import time by `from
    # goldlapel.wrap import wrap` — so `goldlapel.wrap` resolves to the
    # function, not the module. Use the sys.modules lookup instead.
    import sys as _sys
    _wrap_mod = _sys.modules["goldlapel.wrap"]
    _wrap_mod._cache = None
    # Reset the trigger-detection cache so each test sees a fresh start.
    from goldlapel.cache import _trigger_detection_reset
    _trigger_detection_reset()
    yield
    NativeCache._reset()
    _wrap_mod._cache = None
    _trigger_detection_reset()


@pytest.fixture(autouse=True)
def _no_aggressive_verify_by_default(monkeypatch):
    """Most unit tests in this module construct `CachedConnection` /
    `AsyncCachedConnection` directly with MagicMock-backed conns. The
    smart aggressive-verify auto-detection runs a `pg_trigger`
    `cursor.execute(...)` round-trip on the first wire op — which lands
    on the same MagicMock cursor the test is asserting against,
    polluting `cursor.execute.call_args_list` and breaking assertions
    that pre-date the feature.

    Production callers go through `goldlapel.start(...)`, which always
    wires `aggressive_verify="auto"`; this fixture only flips the
    *default* on direct `CachedConnection(conn, cache)` calls so legacy
    tests stay clean. Tests that exercise the detection feature
    explicitly pass `aggressive_verify=` (or seed the module-level
    detection cache) to override.
    """
    import sys as _sys
    _wrap_mod = _sys.modules["goldlapel.wrap"]
    original_sync = _wrap_mod.CachedConnection.__init__
    original_async = _wrap_mod.AsyncCachedConnection.__init__

    def patched_sync(self, real_conn, cache, **kwargs):
        kwargs.setdefault("aggressive_verify", _wrap_mod.AGGRESSIVE_VERIFY_OFF)
        return original_sync(self, real_conn, cache, **kwargs)

    def patched_async(self, real_conn, cache, **kwargs):
        kwargs.setdefault("aggressive_verify", _wrap_mod.AGGRESSIVE_VERIFY_OFF)
        return original_async(self, real_conn, cache, **kwargs)

    monkeypatch.setattr(_wrap_mod.CachedConnection, "__init__", patched_sync)
    monkeypatch.setattr(_wrap_mod.AsyncCachedConnection, "__init__", patched_async)


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
    # Smart aggressive-verify defaults: "off" mode, pre-resolved as
    # inactive. Mirrors the autouse `_no_aggressive_verify_by_default`
    # fixture's intent — tests using `MockCachedConn` directly don't
    # exercise the detection round-trip, so resolution is a no-op.
    _aggressive_verify_mode = "off"
    _aggressive_verify_active = False
    _db_key = None

    def __init__(self):
        # Per-connection unsafe-GUC state — required by CachedCursor.execute
        # to compute the L1 cache key. The real ConnectionGucState is cheap
        # and stateless until a SET is observed, so we use it directly.
        from goldlapel.cache import ConnectionGucState
        self._guc_state = ConnectionGucState()


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


# --- L1 state-hash: end-to-end via CachedCursor + CachedConnection ---
#
# Proves the wrapper's wire path actually folds the per-connection unsafe-GUC
# state into the cache key. Without this, `SET app.user_id` on connection A
# could leak rows to connection B when they execute identical SQL+params.


class TestStateHashWiring:
    def test_cached_connection_has_per_instance_guc_state(self):
        # Each CachedConnection gets its own ConnectionGucState — process-
        # wide state would defeat the point.
        from goldlapel.cache import ConnectionGucState
        a_real = MagicMock()
        b_real = MagicMock()
        cache = make_connected_cache()
        a = CachedConnection(a_real, cache)
        b = CachedConnection(b_real, cache)
        assert isinstance(a._guc_state, ConnectionGucState)
        assert isinstance(b._guc_state, ConnectionGucState)
        assert a._guc_state is not b._guc_state

    def test_set_observation_updates_connection_state(self):
        # `SET app.user_id` on a CachedCursor must mutate the parent
        # CachedConnection's state hash.
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cc_conn = CachedConnection(conn, cache)
        cursor_real = conn.cursor()
        cc_cursor = CachedCursor(cursor_real, cache, cc_conn)
        baseline = cc_conn._guc_state.hash
        cc_cursor.execute("SET app.user_id = '42'")
        assert cc_conn._guc_state.hash != baseline

    def test_safe_set_does_not_change_state_hash(self):
        # Harmless GUC — observable on the cursor but state hash stays
        # at its baseline.
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cc_conn = CachedConnection(conn, cache)
        cursor_real = conn.cursor()
        cc_cursor = CachedCursor(cursor_real, cache, cc_conn)
        baseline = cc_conn._guc_state.hash
        cc_cursor.execute("SET application_name = 'foo'")
        assert cc_conn._guc_state.hash == baseline

    def test_two_connections_different_set_get_different_cache_slots(self):
        # The full security guarantee: connection A sets app.user_id=42,
        # connection B sets app.user_id=43, both run the SAME SELECT.
        # Each must get its own cache slot — B reading after A's put must
        # miss.
        # Use a single shared NativeCache (singleton) — that's the real
        # production layout — but two distinct CachedConnections.
        conn_a, cursor_a = mock_conn(
            description=(("name",),),
            fetchall_result=[("alice",)],
        )
        conn_b, cursor_b = mock_conn(
            description=(("name",),),
            fetchall_result=[("bob",)],
        )
        cache = make_connected_cache()
        cc_a = CachedConnection(conn_a, cache)
        cc_b = CachedConnection(conn_b, cache)
        # Pretend the mock conns are recognized-driver instances so the
        # verify-on-checkout fallback (which fires only for "unknown"
        # drivers) doesn't add an extra cursor.execute(pg_settings...)
        # to either path. The test is about cache-slot isolation, which
        # is independent of the verify path.
        from goldlapel.wrap import _DRIVER_PSYCOPG_SYNC
        object.__setattr__(cc_a, "_driver", _DRIVER_PSYCOPG_SYNC)
        object.__setattr__(cc_b, "_driver", _DRIVER_PSYCOPG_SYNC)

        cur_a = CachedCursor(cursor_a, cache, cc_a)
        cur_b = CachedCursor(cursor_b, cache, cc_b)

        # Connection A: set its RLS context, run the SELECT — populates
        # the cache slot under hash_a.
        cur_a.execute("SET app.user_id = '42'")
        cur_a.execute("SELECT name FROM accounts")
        # Connection A reads alice from its slot.
        assert cur_a.fetchall() == [("alice",)]
        # Reset cursor mock so we can detect whether B re-executes for real.
        cursor_b.execute.reset_mock()

        # Connection B: different RLS context. Identical SQL but the
        # state hash differs → must MISS the cache and call into the
        # real cursor.
        cur_b.execute("SET app.user_id = '43'")
        cur_b.execute("SELECT name FROM accounts")
        # B must have re-executed (cache miss for its hash).
        assert cursor_b.execute.call_args_list[-1][0][0] == "SELECT name FROM accounts"

    def test_same_state_hash_hits_cache(self):
        # Sanity check: when two connections have identical RLS context
        # (both empty / both same), they share the cache slot — by
        # design.
        conn_a, cursor_a = mock_conn(
            description=(("v",),),
            fetchall_result=[(1,)],
        )
        conn_b, cursor_b = mock_conn(
            description=(("v",),),
            fetchall_result=[(2,)],  # would only be returned on cache miss
        )
        cache = make_connected_cache()
        cc_a = CachedConnection(conn_a, cache)
        cc_b = CachedConnection(conn_b, cache)

        cur_a = CachedCursor(cursor_a, cache, cc_a)
        cur_b = CachedCursor(cursor_b, cache, cc_b)

        # A populates the cache.
        cur_a.execute("SELECT v")
        assert cur_a.fetchall() == [(1,)]

        cursor_b.execute.reset_mock()
        # B (no SETs, hash = 0 = same as A) must HIT the cache.
        cur_b.execute("SELECT v")
        assert cur_b.fetchall() == [(1,)]
        # Cache hit: real cursor was never invoked on B's side.
        cursor_b.execute.assert_not_called()

    def test_multi_statement_set_then_select_keys_under_new_hash(self):
        # Single-shot Q: `SET app.user_id='42'; SELECT ...`. The state
        # update must take effect BEFORE the SELECT is evaluated, so the
        # cache key reflects the new RLS context.
        conn, cursor = mock_conn(
            description=(("name",),),
            fetchall_result=[("alice",)],
        )
        cache = make_connected_cache()
        cc_conn = CachedConnection(conn, cache)
        cur = CachedCursor(cursor, cache, cc_conn)
        cur.execute("SET app.user_id = '42'; SELECT name FROM accounts")
        # State hash moved.
        assert cc_conn._guc_state.hash != 0

    def test_reset_returns_state_to_baseline_so_cache_can_re_share(self):
        # After `RESET app.user_id`, the connection's hash returns to 0.
        # Subsequent reads can share slots with peer connections that
        # never set the GUC — by design (correct security context).
        conn, cursor = mock_conn(
            description=(("v",),),
            fetchall_result=[(1,)],
        )
        cache = make_connected_cache()
        cc_conn = CachedConnection(conn, cache)
        cur = CachedCursor(cursor, cache, cc_conn)
        cur.execute("SET app.user_id = '42'")
        assert cc_conn._guc_state.hash != 0
        cur.execute("RESET app.user_id")
        assert cc_conn._guc_state.hash == 0


# --- L1 state-hash: AsyncCachedConnection wiring ---


class TestAsyncStateHashWiring:
    @pytest.mark.asyncio
    async def test_async_connection_has_per_instance_guc_state(self):
        from goldlapel.wrap import AsyncCachedConnection
        from goldlapel.cache import ConnectionGucState
        a_real = MagicMock()
        a_real.fetch = MagicMock()
        a_real.fetchrow = MagicMock()
        cache = make_connected_cache()
        a = AsyncCachedConnection(a_real, cache)
        b = AsyncCachedConnection(MagicMock(), cache)
        assert isinstance(a._guc_state, ConnectionGucState)
        assert a._guc_state is not b._guc_state

    @pytest.mark.asyncio
    async def test_async_execute_observes_set(self):
        # asyncpg's `execute` returns status strings, not rows — so the
        # path doesn't read the cache, but it MUST still observe SETs so
        # subsequent fetch* calls key under the new hash.
        from goldlapel.wrap import AsyncCachedConnection

        class FakeAsyncpgConn:
            async def execute(self, sql, *args):
                return "SET"

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        baseline = a._guc_state.hash
        await a.execute("SET app.user_id = '42'")
        assert a._guc_state.hash != baseline

    @pytest.mark.asyncio
    async def test_async_fetch_after_set_keys_under_new_hash(self):
        from goldlapel.wrap import AsyncCachedConnection

        # Track every (sql, params, state_hash) the cache sees. We also
        # need fetch to actually return rows so .put gets called.
        seen_puts = []
        original_put = NativeCache.put

        def spy_put(self, sql, params, rows, description, state_hash=0):
            seen_puts.append((sql, params, state_hash))
            return original_put(self, sql, params, rows, description, state_hash)

        class FakeAsyncpgConn:
            async def fetch(self, sql, *args):
                return [("alice",)]

            async def execute(self, sql, *args):
                return "SET"

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)

        with patch.object(NativeCache, "put", spy_put):
            await a.execute("SET app.user_id = '42'")
            await a.fetch("SELECT name FROM accounts")

        # The fetch's put landed on the cache with the post-SET hash.
        assert any(
            sql == "SELECT name FROM accounts" and state_hash != 0
            for sql, _params, state_hash in seen_puts
        ), f"Expected non-zero state_hash on fetch put, saw {seen_puts}"


# --- Multi-statement write detection (Bug fix 2026-05-04) ---


class TestMultiStatementWriteDetection:
    """A single Q message can carry multiple statements separated by `;`.
    `_detect_write` looks at the first token only, so `SET ...; INSERT ...`
    used to slip past the write path and leak a stale cache entry. The
    multi-statement detector splits the body and unions per-segment results.
    """

    def test_set_then_insert_invalidates_table(self):
        # `SET ...; INSERT INTO orders ...` — first token is SET, but the
        # INSERT must still trigger invalidation of `orders`.
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        mock_cc = MockCachedConn()
        cc = CachedCursor(cursor, cache, mock_cc)
        cc.execute("SET app.user_id = '42'; INSERT INTO orders VALUES (1)")
        assert cache.get("SELECT * FROM orders", None) is None

    def test_set_then_insert_delegates_to_real_cursor(self):
        # The whole multi-statement body goes through to the real cursor —
        # the wrapper neither short-circuits nor splits the wire message.
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        mock_cc = MockCachedConn()
        cc = CachedCursor(cursor, cache, mock_cc)
        sql = "SET app.user_id = '42'; INSERT INTO orders VALUES (1)"
        cc.execute(sql)
        cursor.execute.assert_called_once_with(sql, None)

    def test_set_then_ddl_invalidates_all(self):
        # Any DDL anywhere in the multi-statement body trips the global
        # invalidation, even if other statements only touch known tables.
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        cache.put("SELECT * FROM users", None, [(2,)], None)
        mock_cc = MockCachedConn()
        cc = CachedCursor(cursor, cache, mock_cc)
        cc.execute("SET app.user_id = '42'; CREATE TABLE foo (id int)")
        assert cache.get("SELECT * FROM orders", None) is None
        assert cache.get("SELECT * FROM users", None) is None

    def test_multiple_writes_invalidate_all_tables(self):
        # Two writes, two distinct tables → both invalidated, none missed.
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        cache.put("SELECT * FROM users", None, [(2,)], None)
        mock_cc = MockCachedConn()
        cc = CachedCursor(cursor, cache, mock_cc)
        cc.execute("INSERT INTO orders VALUES (1); UPDATE users SET v = 1")
        assert cache.get("SELECT * FROM orders", None) is None
        assert cache.get("SELECT * FROM users", None) is None

    def test_select_only_multi_statement_not_a_write(self):
        # Two SELECTs separated by `;` — neither is a write, so the read
        # path is taken (we still go through the cache path, not the
        # write-invalidation path).
        conn, cursor = mock_conn(
            description=(("v",),),
            fetchall_result=[(1,)],
        )
        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        mock_cc = MockCachedConn()
        cc = CachedCursor(cursor, cache, mock_cc)
        cc.execute("SELECT 1; SELECT 2")
        # No invalidation happened; the pre-existing entry survives.
        assert cache.get("SELECT * FROM orders", None) is not None

    def test_write_with_semicolons_in_string_literal(self):
        # The splitter respects quoted literals — a `;` inside a string
        # must not look like a statement terminator.
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        mock_cc = MockCachedConn()
        cc = CachedCursor(cursor, cache, mock_cc)
        cc.execute("INSERT INTO orders VALUES ('a;b;c')")
        assert cache.get("SELECT * FROM orders", None) is None

    def test_executemany_multi_statement_write(self):
        # `executemany` shares the same multi-statement detection path.
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        mock_cc = MockCachedConn()
        cc = CachedCursor(cursor, cache, mock_cc)
        cc.executemany(
            "SET app.user_id = '42'; INSERT INTO orders VALUES (%s)",
            [(1,), (2,)],
        )
        assert cache.get("SELECT * FROM orders", None) is None


# --- Multi-statement tx-flag bookkeeping (Bug fix 2026-05-04) ---


class TestMultiStatementTxBookkeeping:
    """A multi-statement Q like `BEGIN; INSERT INTO t VALUES (1); COMMIT`
    used to flip wrapper-side `_in_transaction` based on first token only,
    leaving the wrapper believing it's in a tx after the COMMIT closed it
    server-side. Subsequent reads bypass the cache forever (until a fresh
    BEGIN/COMMIT cycle resets it). Walking segments converges the wrapper's
    view to the server's actual end-state.
    """

    def test_begin_insert_commit_ends_out_of_tx(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        mock_cc = MockCachedConn()
        cc = CachedCursor(cursor, cache, mock_cc)
        cc.execute("BEGIN; INSERT INTO orders VALUES (1); COMMIT")
        assert mock_cc._in_transaction is False

    def test_begin_insert_no_commit_stays_in_tx(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        mock_cc = MockCachedConn()
        cc = CachedCursor(cursor, cache, mock_cc)
        cc.execute("BEGIN; INSERT INTO orders VALUES (1)")
        assert mock_cc._in_transaction is True

    def test_insert_commit_no_begin_ends_out_of_tx(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        mock_cc = MockCachedConn()
        cc = CachedCursor(cursor, cache, mock_cc)
        # Pre-condition: already in a tx from an earlier BEGIN.
        cc.execute("BEGIN")
        assert mock_cc._in_transaction is True
        cc.execute("INSERT INTO orders VALUES (1); COMMIT")
        assert mock_cc._in_transaction is False

    def test_savepoint_release_round_trip_stays_in_tx(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        mock_cc = MockCachedConn()
        cc = CachedCursor(cursor, cache, mock_cc)
        cc.execute("BEGIN")
        # SAVEPOINT/RELEASE are intra-transaction markers — RELEASE
        # SAVEPOINT does NOT end the outer transaction, it just commits
        # a nested savepoint. The wrapper must stay in_transaction=True
        # so subsequent reads still bypass the cache (server is still
        # in-tx). Flipping to False here would let stale reads slip
        # through (read-your-own-writes violation).
        cc.execute("SAVEPOINT s1; INSERT INTO orders VALUES (1); RELEASE s1")
        assert mock_cc._in_transaction is True
        # Only an explicit COMMIT/ROLLBACK ends the tx.
        cc.execute("COMMIT")
        assert mock_cc._in_transaction is False

    def test_plain_select_no_tx_change(self):
        conn, cursor = mock_conn(
            description=(("v",),),
            fetchall_result=[(1,)],
        )
        cache = make_connected_cache()
        mock_cc = MockCachedConn()
        cc = CachedCursor(cursor, cache, mock_cc)
        cc.execute("SELECT * FROM orders")
        assert mock_cc._in_transaction is False

    def test_multi_statement_tx_body_still_invalidates_writes(self):
        # A `BEGIN; INSERT INTO orders ...; COMMIT` body must invalidate
        # `orders` (the original code's TX_START regex returned early
        # before write detection ran, leaking stale entries).
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        mock_cc = MockCachedConn()
        cc = CachedCursor(cursor, cache, mock_cc)
        cc.execute("BEGIN; INSERT INTO orders VALUES (1); COMMIT")
        assert cache.get("SELECT * FROM orders", None) is None


class TestAsyncMultiStatementTxBookkeeping:
    """Same bookkeeping shape for the asyncpg path."""

    @pytest.mark.asyncio
    async def test_async_execute_begin_insert_commit_ends_out_of_tx(self):
        from goldlapel.wrap import AsyncCachedConnection

        class FakeAsyncpgConn:
            async def execute(self, sql, *args):
                return "INSERT 0 1"

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        await a.execute("BEGIN; INSERT INTO orders VALUES (1); COMMIT")
        assert a._in_transaction is False

    @pytest.mark.asyncio
    async def test_async_execute_begin_alone_starts_tx(self):
        from goldlapel.wrap import AsyncCachedConnection

        class FakeAsyncpgConn:
            async def execute(self, sql, *args):
                return "BEGIN"

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        await a.execute("BEGIN")
        assert a._in_transaction is True

    @pytest.mark.asyncio
    async def test_async_fetch_with_tx_markers_dispatches_to_real(self):
        from goldlapel.wrap import AsyncCachedConnection

        class FakeAsyncpgConn:
            def __init__(self):
                self.calls = 0

            async def fetch(self, sql, *args):
                self.calls += 1
                return [(42,)]

        cache = make_connected_cache()
        # Pre-populate a cache entry — the body has a tx marker so the
        # wrapper must dispatch to real, not serve from cache.
        cache.put("BEGIN; SELECT 1; COMMIT", None, [(99,)], None)
        real = FakeAsyncpgConn()
        a = AsyncCachedConnection(real, cache)
        await a.fetch("BEGIN; SELECT 1; COMMIT")
        assert real.calls == 1
        assert a._in_transaction is False

    @pytest.mark.asyncio
    async def test_async_execute_balanced_begin_rollback_ends_out_of_tx(self):
        from goldlapel.wrap import AsyncCachedConnection

        class FakeAsyncpgConn:
            async def execute(self, sql, *args):
                return "ROLLBACK"

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        await a.execute("BEGIN; ROLLBACK")
        assert a._in_transaction is False


class TestAsyncMultiStatementWriteDetection:
    @pytest.mark.asyncio
    async def test_async_fetch_set_then_insert_invalidates(self):
        from goldlapel.wrap import AsyncCachedConnection

        class FakeAsyncpgConn:
            async def fetch(self, sql, *args):
                return []

        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        await a.fetch("SET app.user_id = '42'; INSERT INTO orders VALUES (1)")
        assert cache.get("SELECT * FROM orders", None) is None

    @pytest.mark.asyncio
    async def test_async_execute_set_then_insert_invalidates(self):
        from goldlapel.wrap import AsyncCachedConnection

        class FakeAsyncpgConn:
            async def execute(self, sql, *args):
                return "INSERT 0 1"

        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        await a.execute("SET app.user_id = '42'; INSERT INTO orders VALUES (1)")
        assert cache.get("SELECT * FROM orders", None) is None

    @pytest.mark.asyncio
    async def test_async_fetchrow_set_then_ddl_invalidates_all(self):
        from goldlapel.wrap import AsyncCachedConnection

        class FakeAsyncpgConn:
            async def fetchrow(self, sql, *args):
                return None

        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        cache.put("SELECT * FROM users", None, [(2,)], None)
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        await a.fetchrow("SET app.user_id = '42'; CREATE TABLE foo (id int)")
        assert cache.get("SELECT * FROM orders", None) is None
        assert cache.get("SELECT * FROM users", None) is None

    @pytest.mark.asyncio
    async def test_async_fetchval_multi_writes(self):
        from goldlapel.wrap import AsyncCachedConnection

        class FakeAsyncpgConn:
            async def fetchval(self, sql, *args, column=0):
                return None

        cache = make_connected_cache()
        cache.put("SELECT * FROM orders", None, [(1,)], None)
        cache.put("SELECT * FROM users", None, [(2,)], None)
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        await a.fetchval(
            "INSERT INTO orders VALUES (1); UPDATE users SET v = 1"
        )
        assert cache.get("SELECT * FROM orders", None) is None
        assert cache.get("SELECT * FROM users", None) is None


# --- SET / RESET responses are not cached (Bug fix 2026-05-04) ---


class TestSetResponseNotCached:
    """psycopg's sync path sets `cursor.description = None` on SET / RESET
    statements, so the existing `if self._real.description is not None`
    guard already skips the put. Confirm that empirically — the test
    documents the contract so a future refactor that gates on something
    else (e.g. `command in {SET, RESET}`) doesn't regress.
    """

    def test_set_response_not_cached(self):
        # description=None mirrors psycopg's SET behaviour. With the
        # existing `if self._real.description is not None` guard the
        # wrapper must not call cache.put.
        conn, cursor = mock_conn(description=None)
        cache = make_connected_cache()
        mock_cc = MockCachedConn()
        cc = CachedCursor(cursor, cache, mock_cc)
        # Pick a SET that doesn't change the GUC state hash, so we don't
        # also have to reason about state_hash.
        before = cache.stats_invalidations
        cc.execute("SET timezone = 'UTC'")
        # No cache entry under any state hash.
        assert cache.get("SET timezone = 'UTC'", None) is None
        # Sanity: the SET went down the read path (cache miss + put-skip),
        # not the write path — no invalidations fired.
        assert cache.stats_invalidations == before

    def test_reset_response_not_cached(self):
        conn, cursor = mock_conn(description=None)
        cache = make_connected_cache()
        mock_cc = MockCachedConn()
        cc = CachedCursor(cursor, cache, mock_cc)
        cc.execute("RESET timezone")
        assert cache.get("RESET timezone", None) is None


class TestAsyncSetResponseNotCached:
    """asyncpg's `fetch` returns `[]` on a SET (it doesn't surface a
    description / command tag the way psycopg does), so the old code
    path would happily cache a useless empty entry. The fix gates on
    `if rows:` — empty result sets are skipped.
    """

    @pytest.mark.asyncio
    async def test_async_fetch_empty_result_not_cached(self):
        from goldlapel.wrap import AsyncCachedConnection

        seen_puts = []
        original_put = NativeCache.put

        def spy_put(self, sql, params, rows, description, state_hash=0):
            seen_puts.append(sql)
            return original_put(self, sql, params, rows, description, state_hash)

        class FakeAsyncpgConn:
            async def fetch(self, sql, *args):
                return []  # SET / RESET / LISTEN all look like this

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)

        with patch.object(NativeCache, "put", spy_put):
            await a.fetch("SET timezone = 'UTC'")
            await a.fetch("RESET timezone")
            await a.fetch("LISTEN channel")

        assert seen_puts == [], (
            f"Empty fetch results must not be cached; saw puts for {seen_puts}"
        )

    @pytest.mark.asyncio
    async def test_async_fetch_non_empty_result_still_cached(self):
        # Sanity: the empty-rows guard only short-circuits empty results.
        # Real SELECT rows still land in the cache.
        from goldlapel.wrap import AsyncCachedConnection

        class FakeAsyncpgConn:
            async def fetch(self, sql, *args):
                return [("alice",)]

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        await a.fetch("SELECT name FROM accounts")
        assert cache.get("SELECT name FROM accounts", None) is not None


# --- Driver detection ---
#
# `_detect_driver` recognizes asyncpg / psycopg / psycopg2 connection
# classes by their qualified name. Mocks land on "unknown" — which is
# the safe answer (we don't assume default DISCARD-on-release behaviour).


class TestDriverDetection:
    def test_unknown_for_magicmock(self):
        from goldlapel.wrap import _detect_driver, _DRIVER_UNKNOWN
        assert _detect_driver(MagicMock()) == _DRIVER_UNKNOWN

    def test_asyncpg_detection(self):
        from goldlapel.wrap import _detect_driver, _DRIVER_ASYNCPG

        # Build a class whose qualified name matches asyncpg.connection.Connection.
        class Connection:
            pass

        Connection.__module__ = "asyncpg.connection"
        assert _detect_driver(Connection()) == _DRIVER_ASYNCPG

    def test_psycopg_async_detection(self):
        from goldlapel.wrap import _detect_driver, _DRIVER_PSYCOPG_ASYNC

        class AsyncConnection:
            pass

        AsyncConnection.__module__ = "psycopg"
        assert _detect_driver(AsyncConnection()) == _DRIVER_PSYCOPG_ASYNC

    def test_psycopg_sync_detection(self):
        from goldlapel.wrap import _detect_driver, _DRIVER_PSYCOPG_SYNC

        class Connection:
            pass

        Connection.__module__ = "psycopg"
        assert _detect_driver(Connection()) == _DRIVER_PSYCOPG_SYNC

    def test_psycopg2_detection(self):
        from goldlapel.wrap import _detect_driver, _DRIVER_PSYCOPG_SYNC

        class connection:
            pass

        connection.__module__ = "psycopg2.extensions"
        assert _detect_driver(connection()) == _DRIVER_PSYCOPG_SYNC


# --- Verify-on-checkout: sync pre-call hook ---


class TestSyncVerifyOnCheckout:
    """The sync pre-call hook fires `maybe_verify` ONLY when the driver
    is "unknown" AND the connection is dirty. For known drivers we trust
    the pool's default DISCARD-on-release; the parser observes the
    DISCARD on the wire and clears state automatically."""

    def _seed_dirty_with_unknown_driver(self):
        from goldlapel.wrap import _DRIVER_UNKNOWN
        conn, cursor = mock_conn(description=None, fetchall_result=[])
        cache = make_connected_cache()
        cc_conn = CachedConnection(conn, cache)
        # MagicMock already classifies as unknown, but be explicit.
        object.__setattr__(cc_conn, "_driver", _DRIVER_UNKNOWN)
        cur = CachedCursor(cursor, cache, cc_conn)
        # Initial SET marks the state dirty.
        cur.execute("SET app.user_id = '42'")
        # Reset call counter so subsequent assertions can see ONLY the
        # next execute()'s wire activity.
        cursor.execute.reset_mock()
        cursor.fetchall.reset_mock()
        return conn, cursor, cc_conn, cur

    def test_dirty_unknown_driver_triggers_verify(self):
        conn, cursor, cc_conn, cur = self._seed_dirty_with_unknown_driver()
        # Mock the verify response — pg_settings shows app.user_id=42.
        cursor.fetchall.return_value = [("app.user_id", "42")]
        cur.execute("SELECT name FROM accounts")
        # First call must have been the verify SQL.
        first_sql = cursor.execute.call_args_list[0][0][0]
        assert "pg_settings" in first_sql
        # Verify cleared dirty.
        assert cc_conn._guc_state.dirty is False

    def test_known_driver_skips_verify(self):
        # If the driver is one we trust (asyncpg / psycopg / psycopg2),
        # we don't pay the round-trip — the wire-side DISCARD parser
        # handles the common case.
        from goldlapel.wrap import _DRIVER_PSYCOPG_SYNC
        conn, cursor = mock_conn(description=None, fetchall_result=[])
        cache = make_connected_cache()
        cc_conn = CachedConnection(conn, cache)
        object.__setattr__(cc_conn, "_driver", _DRIVER_PSYCOPG_SYNC)
        cur = CachedCursor(cursor, cache, cc_conn)
        cur.execute("SET app.user_id = '42'")
        cursor.execute.reset_mock()
        cur.execute("SELECT name FROM accounts")
        # No call should have been the verify SQL — we trust the pool.
        for call in cursor.execute.call_args_list:
            sql = call[0][0]
            assert "pg_settings" not in sql, f"Unexpected verify call: {sql}"

    def test_clean_state_skips_verify(self):
        # Even on an "unknown" driver, if dirty=False, no verify fires.
        conn, cursor = mock_conn(description=None, fetchall_result=[])
        cache = make_connected_cache()
        cc_conn = CachedConnection(conn, cache)  # default driver classification
        cur = CachedCursor(cursor, cache, cc_conn)
        # No prior SET — clean state.
        cur.execute("SELECT 1")
        for call in cursor.execute.call_args_list:
            sql = call[0][0]
            assert "pg_settings" not in sql

    def test_discard_clears_dirty_skipping_next_verify(self):
        # SET → DISCARD ALL → SELECT. The DISCARD-on-the-wire clears the
        # dirty flag, so the SELECT's pre-call verify is a no-op.
        from goldlapel.wrap import _DRIVER_UNKNOWN
        conn, cursor = mock_conn(description=None, fetchall_result=[])
        cache = make_connected_cache()
        cc_conn = CachedConnection(conn, cache)
        object.__setattr__(cc_conn, "_driver", _DRIVER_UNKNOWN)
        cur = CachedCursor(cursor, cache, cc_conn)
        cur.execute("SET app.user_id = '42'")
        cur.execute("DISCARD ALL")
        cursor.execute.reset_mock()
        cur.execute("SELECT 1")
        for call in cursor.execute.call_args_list:
            sql = call[0][0]
            assert "pg_settings" not in sql


# --- Sync mark-dirty for top-level function calls ---


class TestSyncMarkDirtyOnFunctionCall:
    """`SELECT my_func()` at statement-level marks the connection dirty
    so the next pre-call verify reconciles state. The function body
    might have done a server-side `SET` we couldn't see on the wire."""

    def test_user_function_call_marks_dirty(self):
        conn, cursor = mock_conn(
            description=(("v",),),
            fetchall_result=[(1,)],
        )
        cache = make_connected_cache()
        cc_conn = CachedConnection(conn, cache)
        cur = CachedCursor(cursor, cache, cc_conn)
        assert cc_conn._guc_state.dirty is False
        cur.execute("SELECT my_func()")
        assert cc_conn._guc_state.dirty is True

    def test_safe_builtin_does_not_mark_dirty(self):
        conn, cursor = mock_conn(
            description=(("v",),),
            fetchall_result=[(1,)],
        )
        cache = make_connected_cache()
        cc_conn = CachedConnection(conn, cache)
        cur = CachedCursor(cursor, cache, cc_conn)
        cur.execute("SELECT now()")
        assert cc_conn._guc_state.dirty is False

    def test_normal_select_does_not_mark_dirty(self):
        conn, cursor = mock_conn(
            description=(("name",),),
            fetchall_result=[("alice",)],
        )
        cache = make_connected_cache()
        cc_conn = CachedConnection(conn, cache)
        cur = CachedCursor(cursor, cache, cc_conn)
        cur.execute("SELECT name FROM accounts")
        assert cc_conn._guc_state.dirty is False


# --- Async post-call verify (item #6) ---


class TestAsyncPostCallVerify:
    """The async post-call verify schedules a background task to read
    pg_settings AFTER the user's response is dispatched. The user's
    fetch() returns immediately; the verify runs concurrently."""

    @pytest.mark.asyncio
    async def test_user_function_fetch_schedules_verify(self):
        from goldlapel.wrap import AsyncCachedConnection
        import asyncio

        class FakeAsyncpgConn:
            def __init__(self):
                self.calls = []

            async def fetch(self, sql, *args):
                self.calls.append(sql)
                if "pg_settings" in sql:
                    # Verify fetches the post-call session view.
                    return [("app.user_id", "999")]
                return [(1,)]

        cache = make_connected_cache()
        conn = FakeAsyncpgConn()
        a = AsyncCachedConnection(conn, cache)
        # User's call lands first; verify is scheduled afterwards.
        rows = await a.fetch("SELECT my_func()")
        assert rows == [(1,)]
        # Drain the verify task — it should have been scheduled.
        assert len(a._pending_verify_tasks) >= 0  # may have already run
        if a._pending_verify_tasks:
            await asyncio.gather(*a._pending_verify_tasks, return_exceptions=True)
        # State map reflects the post-call value.
        peer = type(a._guc_state)()
        peer.observe_sql("SET app.user_id = '999'")
        assert a._guc_state.hash == peer.hash
        # `pg_settings` query landed.
        assert any("pg_settings" in c for c in conn.calls)

    @pytest.mark.asyncio
    async def test_verify_failure_does_not_error_user_query(self):
        # If pg_settings query fails, the user's call must STILL succeed.
        from goldlapel.wrap import AsyncCachedConnection
        import asyncio

        class FakeAsyncpgConn:
            async def fetch(self, sql, *args):
                if "pg_settings" in sql:
                    raise RuntimeError("verify blew up")
                return [(1,)]

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        # User's call must NOT raise.
        rows = await a.fetch("SELECT my_func()")
        assert rows == [(1,)]
        # Drain any pending verify tasks (without raising).
        if a._pending_verify_tasks:
            await asyncio.gather(*a._pending_verify_tasks, return_exceptions=True)
        # Connection stays dirty so a future verify can retry.
        assert a._guc_state.dirty is True

    @pytest.mark.asyncio
    async def test_user_function_fetchrow_schedules_verify(self):
        from goldlapel.wrap import AsyncCachedConnection
        import asyncio

        verify_calls = []

        class FakeAsyncpgConn:
            async def fetchrow(self, sql, *args):
                return ("ok",)

            async def fetch(self, sql, *args):
                verify_calls.append(sql)
                return [("app.user_id", "55")]

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        await a.fetchrow("SELECT my_func()")
        if a._pending_verify_tasks:
            await asyncio.gather(*a._pending_verify_tasks, return_exceptions=True)
        # Verify hit pg_settings.
        assert any("pg_settings" in c for c in verify_calls)

    @pytest.mark.asyncio
    async def test_user_function_execute_schedules_verify(self):
        from goldlapel.wrap import AsyncCachedConnection
        import asyncio

        verify_calls = []

        class FakeAsyncpgConn:
            async def execute(self, sql, *args):
                return "OK"

            async def fetch(self, sql, *args):
                verify_calls.append(sql)
                return []

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        await a.execute("SELECT my_func()")
        if a._pending_verify_tasks:
            await asyncio.gather(*a._pending_verify_tasks, return_exceptions=True)
        assert any("pg_settings" in c for c in verify_calls)

    @pytest.mark.asyncio
    async def test_safe_builtin_does_not_schedule_verify(self):
        # `SELECT now()` is in the safe-builtins list — no verify needed.
        from goldlapel.wrap import AsyncCachedConnection

        class FakeAsyncpgConn:
            async def fetch(self, sql, *args):
                return [("2026-01-01",)]

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        await a.fetch("SELECT now()")
        assert not a._pending_verify_tasks
        assert a._guc_state.dirty is False

    @pytest.mark.asyncio
    async def test_normal_select_does_not_schedule_verify(self):
        from goldlapel.wrap import AsyncCachedConnection

        class FakeAsyncpgConn:
            async def fetch(self, sql, *args):
                return [("alice",)]

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        await a.fetch("SELECT name FROM accounts")
        assert not a._pending_verify_tasks

    @pytest.mark.asyncio
    async def test_close_cancels_pending_verifies(self):
        # If `close()` is called while a verify is in flight, the task
        # must be cancelled cleanly — no lingering coroutines.
        from goldlapel.wrap import AsyncCachedConnection
        import asyncio

        verify_started = asyncio.Event()
        verify_proceed = asyncio.Event()
        close_finished = False

        class FakeAsyncpgConn:
            async def fetch(self, sql, *args):
                if "pg_settings" in sql:
                    verify_started.set()
                    # Block until told to proceed (or cancelled).
                    await verify_proceed.wait()
                    return []
                return [(1,)]

            async def close(self):
                pass

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        await a.fetch("SELECT my_func()")
        # Wait for the verify to actually start before closing.
        await asyncio.wait_for(verify_started.wait(), timeout=1.0)
        # Close should cancel the in-flight verify and return promptly.
        await a.close()
        # All tasks drained (task set is empty post-close).
        assert not a._pending_verify_tasks

    @pytest.mark.asyncio
    async def test_verify_serializes_with_user_calls(self):
        # asyncpg / psycopg-async are single-op per connection. The
        # post-call verify task and the user's NEXT call would race
        # without explicit serialization. This test simulates that
        # race: the verify task starts first (and blocks on a fake
        # await), the user's next call must wait until verify finishes
        # rather than racing the connection's protocol lock.
        from goldlapel.wrap import AsyncCachedConnection
        import asyncio

        verify_started = asyncio.Event()
        verify_proceed = asyncio.Event()
        order_seen = []

        class FakeAsyncpgConn:
            async def fetch(self, sql, *args):
                if "pg_settings" in sql:
                    order_seen.append("verify-start")
                    verify_started.set()
                    await verify_proceed.wait()
                    order_seen.append("verify-end")
                    return []
                order_seen.append(f"user:{sql}")
                return [(1,)]

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        # First call schedules a verify.
        await a.fetch("SELECT my_func()")
        # Wait for the verify task to actually be holding the lock.
        await asyncio.wait_for(verify_started.wait(), timeout=1.0)
        # User's next call — must wait behind the verify under the lock.
        next_call_task = asyncio.create_task(a.fetch("SELECT 1"))
        # Give the next-call task a chance to advance to the lock acquire.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        # While verify is still in flight, the user's next call must
        # NOT have hit the wire — the lock is held by verify.
        assert "user:SELECT 1" not in order_seen, (
            f"User call ran before verify finished: {order_seen}"
        )
        # Release verify. Now the user's call should proceed.
        verify_proceed.set()
        await next_call_task
        # Sequence: user's first call, then verify, then user's next call.
        assert order_seen[0] == "user:SELECT my_func()"
        assert "verify-end" in order_seen
        assert "user:SELECT 1" in order_seen
        # Verify must complete BEFORE the user's next call lands —
        # confirming the lock serialized them.
        verify_end_idx = order_seen.index("verify-end")
        user_call_idx = order_seen.index("user:SELECT 1")
        assert verify_end_idx < user_call_idx

    @pytest.mark.asyncio
    async def test_cache_hit_does_not_schedule_post_call_verify(self):
        # A pure cache hit doesn't run the user's function on the wire,
        # so no NEW post-call verify task is scheduled. (The pre-call
        # verify path is independent and may still fire if the prior
        # call left the connection dirty.)
        from goldlapel.wrap import AsyncCachedConnection, _DRIVER_PSYCOPG_ASYNC
        import asyncio

        post_call_marker = []
        captured = []

        class FakeAsyncpgConn:
            async def fetch(self, sql, *args):
                captured.append(sql)
                if "pg_settings" in sql:
                    return []
                return [(1,)]

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        # Pretend the driver is recognized so pre-call verify is gated off.
        # The post-call verify path uses `mark_dirty` + create_task and is
        # independent of driver detection.
        object.__setattr__(a, "_driver", _DRIVER_PSYCOPG_ASYNC)
        # First call populates the cache and schedules a post-call verify.
        await a.fetch("SELECT my_func()")
        if a._pending_verify_tasks:
            await asyncio.gather(*a._pending_verify_tasks, return_exceptions=True)
        captured.clear()
        # Second call hits the cache — no new wire activity at all.
        rows = await a.fetch("SELECT my_func()")
        assert rows == [(1,)]
        # No new post-call verify scheduled (the cache path returns before
        # `_schedule_post_call_verify` runs).
        assert not a._pending_verify_tasks
        # And no new wire calls landed (driver was trusted, so pre-call
        # verify was skipped too).
        assert captured == []


# --- SET-actually-applied (Wave 2): wrap.py defers state mutation ---


class TestSetActuallyAppliedSync:
    """Wave 2 fix: the sync wrapper defers per-connection GUC state
    mutation until AFTER the dispatch resolves. A server-side error on
    the SET (e.g. invalid role, unknown GUC namespace) must NOT leave
    wrapper state diverged from server reality.

    Tests use a real CachedConnection so the apply_pending plumbing is
    end-to-end exercised (not just the cache.py-level apply_pending tests).
    """

    def _make_failing_cursor(self):
        # A real-cursor mock whose execute() raises on the first call,
        # mimicking psycopg's behaviour when the server returns an
        # ErrorResponse on a SET (e.g. `SET role = 'nonexistent'`).
        from unittest.mock import MagicMock
        cursor = MagicMock()
        cursor.description = None
        cursor.name = None
        cursor.arraysize = 1
        cursor.execute.side_effect = RuntimeError(
            'role "nonexistent_role" does not exist'
        )
        return cursor

    def test_set_success_applies_state(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cc_conn = CachedConnection(conn, cache)
        cur = CachedCursor(cursor, cache, cc_conn)
        baseline = cc_conn._guc_state.hash
        cur.execute("SET app.user_id = '42'")
        assert cc_conn._guc_state.hash != baseline
        assert cc_conn._guc_state.dirty is True

    def test_set_error_does_not_apply_state(self):
        # The bug we're fixing: pre-Wave 2 the SET applied optimistically.
        # After Wave 2, a raised execute leaves state at baseline.
        conn = mock_conn()[0]
        cache = make_connected_cache()
        cursor = self._make_failing_cursor()
        cc_conn = CachedConnection(conn, cache)
        cur = CachedCursor(cursor, cache, cc_conn)
        baseline_hash = cc_conn._guc_state.hash
        with pytest.raises(RuntimeError):
            cur.execute("SET role = 'nonexistent_role'")
        # State unchanged — no optimistic mutation.
        assert cc_conn._guc_state.hash == baseline_hash
        # Dirty flag also stays False — no actual unsafe SET landed.
        assert cc_conn._guc_state.dirty is False

    def test_multi_statement_set_then_failing_query_applies_set(self):
        # `SET app.user_id='42'; SELECT bad_query` — execute raises on the
        # SELECT, but the leading SET landed before the trailing statement
        # erred. Wrapper state should reflect the SET.
        conn = mock_conn()[0]
        cache = make_connected_cache()
        cursor = self._make_failing_cursor()
        cc_conn = CachedConnection(conn, cache)
        cur = CachedCursor(cursor, cache, cc_conn)
        baseline = cc_conn._guc_state.hash
        with pytest.raises(RuntimeError):
            cur.execute(
                "SET app.user_id = '42'; SELECT bad_query"
            )
        assert cc_conn._guc_state.hash != baseline

    def test_discard_all_success_clears_state(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cc_conn = CachedConnection(conn, cache)
        cur = CachedCursor(cursor, cache, cc_conn)
        cur.execute("SET app.user_id = '42'")
        assert cc_conn._guc_state.hash != 0
        cur.execute("DISCARD ALL")
        assert cc_conn._guc_state.hash == 0

    def test_discard_all_error_preserves_state(self):
        # DISCARD ALL inside an explicit transaction errors server-side.
        # Wrapper state must NOT clear on the error. Pre-set state
        # directly on the GucState (bypassing the verify-on-checkout
        # path that an unknown-driver MagicMock would trigger off the
        # dirty flag) so the test isolates the apply_pending(False)
        # behavior for DISCARD ALL.
        from goldlapel.wrap import _DRIVER_PSYCOPG_SYNC
        conn = mock_conn()[0]
        cache = make_connected_cache()
        cc_conn = CachedConnection(conn, cache)
        # Pretend the driver is recognized so pre-call verify is gated off.
        object.__setattr__(cc_conn, "_driver", _DRIVER_PSYCOPG_SYNC)
        bad_cursor = self._make_failing_cursor()
        bad = CachedCursor(bad_cursor, cache, cc_conn)
        # Prime state with an unsafe SET (separate cursor that succeeds).
        ok_cursor = mock_conn()[1]
        ok = CachedCursor(ok_cursor, cache, cc_conn)
        ok.execute("SET app.user_id = '42'")
        post_set = cc_conn._guc_state.hash
        assert post_set != 0
        # Now dispatch DISCARD ALL through the failing cursor.
        with pytest.raises(RuntimeError):
            bad.execute("DISCARD ALL")
        # State unchanged — DISCARD ALL did not land on the server.
        assert cc_conn._guc_state.hash == post_set

    def test_begin_set_rollback_reverts_state(self):
        # Multi-statement BEGIN; SET; ROLLBACK — succeeds end-to-end, but
        # the SET was rolled back server-side. Wrapper state must NOT
        # carry the SET past this call.
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cc_conn = CachedConnection(conn, cache)
        cur = CachedCursor(cursor, cache, cc_conn)
        cur.execute("BEGIN; SET app.user_id = '42'; ROLLBACK")
        assert cc_conn._guc_state.hash == 0
        # Tx state also converged out of tx.
        assert cc_conn._in_transaction is False

    def test_begin_set_commit_persists_state(self):
        conn, cursor = mock_conn()
        cache = make_connected_cache()
        cc_conn = CachedConnection(conn, cache)
        cur = CachedCursor(cursor, cache, cc_conn)
        cur.execute("BEGIN; SET app.user_id = '42'; COMMIT")
        assert cc_conn._guc_state.hash != 0
        assert cc_conn._in_transaction is False

    def test_executemany_set_error_does_not_apply(self):
        from unittest.mock import MagicMock
        cursor = MagicMock()
        cursor.executemany.side_effect = RuntimeError("boom")
        cursor.description = None
        cursor.name = None
        cursor.arraysize = 1
        conn = mock_conn()[0]
        cache = make_connected_cache()
        cc_conn = CachedConnection(conn, cache)
        cur = CachedCursor(cursor, cache, cc_conn)
        with pytest.raises(RuntimeError):
            cur.executemany("SET app.user_id = '42'", [()])
        assert cc_conn._guc_state.hash == 0


class TestSetActuallyAppliedAsync:
    """Async parity for the Wave 2 SET-actually-applied fix. Mirrors the
    sync coverage across fetch / fetchrow / fetchval / execute paths."""

    @pytest.mark.asyncio
    async def test_async_execute_set_success_applies(self):
        from goldlapel.wrap import AsyncCachedConnection

        class FakeAsyncpgConn:
            async def execute(self, sql, *args):
                return "SET"

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        baseline = a._guc_state.hash
        await a.execute("SET app.user_id = '42'")
        assert a._guc_state.hash != baseline

    @pytest.mark.asyncio
    async def test_async_execute_set_error_does_not_apply(self):
        from goldlapel.wrap import AsyncCachedConnection

        class FakeAsyncpgConn:
            async def execute(self, sql, *args):
                raise RuntimeError(
                    'role "nonexistent_role" does not exist'
                )

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        baseline = a._guc_state.hash
        with pytest.raises(RuntimeError):
            await a.execute("SET role = 'nonexistent_role'")
        assert a._guc_state.hash == baseline
        assert a._guc_state.dirty is False

    @pytest.mark.asyncio
    async def test_async_fetch_set_error_does_not_apply(self):
        from goldlapel.wrap import AsyncCachedConnection

        class FakeAsyncpgConn:
            async def fetch(self, sql, *args):
                raise RuntimeError("server-side SET error")

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        baseline = a._guc_state.hash
        with pytest.raises(RuntimeError):
            await a.fetch("SET role = 'nonexistent_role'")
        assert a._guc_state.hash == baseline

    @pytest.mark.asyncio
    async def test_async_fetchrow_set_error_does_not_apply(self):
        from goldlapel.wrap import AsyncCachedConnection

        class FakeAsyncpgConn:
            async def fetchrow(self, sql, *args):
                raise RuntimeError("server-side SET error")

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        baseline = a._guc_state.hash
        with pytest.raises(RuntimeError):
            await a.fetchrow("SET role = 'nonexistent_role'")
        assert a._guc_state.hash == baseline

    @pytest.mark.asyncio
    async def test_async_fetchval_set_error_does_not_apply(self):
        from goldlapel.wrap import AsyncCachedConnection

        class FakeAsyncpgConn:
            async def fetchval(self, sql, *args, column=0):
                raise RuntimeError("server-side SET error")

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        baseline = a._guc_state.hash
        with pytest.raises(RuntimeError):
            await a.fetchval("SET role = 'nonexistent_role'")
        assert a._guc_state.hash == baseline

    @pytest.mark.asyncio
    async def test_async_multi_statement_set_then_failing_query_applies_set(self):
        from goldlapel.wrap import AsyncCachedConnection

        class FakeAsyncpgConn:
            async def execute(self, sql, *args):
                # Server processed the SET, then errored on the SELECT.
                raise RuntimeError("syntax error at or near \"bad_query\"")

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        baseline = a._guc_state.hash
        with pytest.raises(RuntimeError):
            await a.execute(
                "SET app.user_id = '42'; SELECT bad_query"
            )
        # SET landed before the trailing query; wrapper state mirrors that.
        assert a._guc_state.hash != baseline

    @pytest.mark.asyncio
    async def test_async_begin_set_rollback_reverts_state(self):
        from goldlapel.wrap import AsyncCachedConnection

        class FakeAsyncpgConn:
            async def execute(self, sql, *args):
                return "ROLLBACK"

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        await a.execute("BEGIN; SET app.user_id = '42'; ROLLBACK")
        assert a._guc_state.hash == 0
        assert a._in_transaction is False

    @pytest.mark.asyncio
    async def test_async_begin_set_commit_persists_state(self):
        from goldlapel.wrap import AsyncCachedConnection

        class FakeAsyncpgConn:
            async def execute(self, sql, *args):
                return "COMMIT"

        cache = make_connected_cache()
        a = AsyncCachedConnection(FakeAsyncpgConn(), cache)
        await a.execute("BEGIN; SET app.user_id = '42'; COMMIT")
        assert a._guc_state.hash != 0
        assert a._in_transaction is False
