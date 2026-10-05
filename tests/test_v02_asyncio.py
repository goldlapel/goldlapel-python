"""Tests for the v0.2.x async factory API in goldlapel.asyncio.

Native asyncpg path: wrapper methods call goldlapel.asyncio._utils functions
directly with a plain asyncpg.Connection — no thread-pool bridge. These tests mock asyncpg.connect + subprocess spawn to
verify wiring, lifecycle, `using()` semantics, and the startup banner
without touching a real Postgres.
"""

import asyncio
import inspect
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import goldlapel.asyncio as gl_async
from goldlapel.asyncio._proxy import AsyncGoldLapel, _ASYNC_SKIPPED
from goldlapel.proxy import GoldLapel


# Auto-derived public method surface on AsyncGoldLapel — names of methods
# attached by _derive_async_methods at module load. Identified by the
# `__wrapped__` attribute set by functools.wraps inside _make_async_wrapper /
# _make_async_gen_wrapper. Hand-written async-native methods (start/stop/
# using/stream_*) don't have __wrapped__ pointing at a sync GoldLapel method,
# so they're excluded — they have their own coverage in this file.
_AUTO_DERIVED_NAMES = tuple(sorted(
    name for name in vars(AsyncGoldLapel)
    if not name.startswith("_")
    and getattr(getattr(AsyncGoldLapel, name), "__wrapped__", None)
       is getattr(GoldLapel, name, None)
    and getattr(GoldLapel, name, None) is not None
))


class TestAsyncStart:
    @pytest.mark.asyncio
    @patch("goldlapel.asyncio._proxy._detect_asyncpg")
    async def test_returns_async_goldlapel(self, mock_detect):
        # Fake asyncpg module: connect returns a fake raw conn.
        fake_asyncpg = MagicMock()
        fake_raw = MagicMock()
        fake_raw.set_type_codec = AsyncMock()
        fake_asyncpg.connect = AsyncMock(return_value=fake_raw)
        mock_detect.return_value = fake_asyncpg

        # Mock subprocess spawn + port wait so no real binary is needed.
        with patch("goldlapel.asyncio._proxy._find_binary", return_value="/usr/bin/goldlapel"), \
             patch("goldlapel.asyncio._proxy._wait_for_port", return_value=True), \
             patch("goldlapel.asyncio._proxy._kill_orphan_on_port"), \
             patch("goldlapel.asyncio._proxy._make_proxy_url", return_value="postgresql://localhost:7932/db"):
            import subprocess as sp_mod
            with patch("subprocess.Popen") as mock_popen:
                proc = MagicMock()
                proc.poll.return_value = None
                proc.stderr = MagicMock()
                mock_popen.return_value = proc
                try:
                    _reset_proxy_state()
                    result = await gl_async.start("postgresql://host/db")
                    assert isinstance(result, AsyncGoldLapel)
                    # Internal conn is the plain asyncpg conn.
                    assert result._conn is fake_raw
                    # Sync struct exists for using() + subprocess bookkeeping.
                    assert result._sync is not None
                finally:
                    _reset_proxy_state()

    @pytest.mark.asyncio
    @patch("goldlapel.asyncio._proxy._detect_asyncpg", return_value=None)
    async def test_raises_without_asyncpg(self, mock_detect):
        with pytest.raises(ImportError, match="asyncpg"):
            await gl_async.start("postgresql://host/db")


class TestStartHandleDoubleUse:
    """Regression for v0.2 review finding (MEDIUM, Option B):
    _StartHandle is single-use. A second use (await then async-with, or
    vice versa) would spawn a second subprocess while orphaning the first —
    we raise RuntimeError instead.
    """

    @pytest.mark.asyncio
    @patch("goldlapel.asyncio._proxy._actual_start", new_callable=AsyncMock)
    async def test_await_then_async_with_raises(self, mock_actual):
        mock_actual.return_value = MagicMock(spec=AsyncGoldLapel, name="fake_gl")
        handle = gl_async.start("postgresql://host/db")
        await handle  # first use — consumes the handle
        with pytest.raises(RuntimeError, match="already consumed"):
            async with handle:
                pass

    @pytest.mark.asyncio
    @patch("goldlapel.asyncio._proxy._actual_start", new_callable=AsyncMock)
    async def test_async_with_then_await_raises(self, mock_actual):
        mock_actual.return_value = MagicMock(spec=AsyncGoldLapel, name="fake_gl")
        handle = gl_async.start("postgresql://host/db")
        async with handle:
            pass  # first use — consumes the handle
        with pytest.raises(RuntimeError, match="already consumed"):
            await handle

    @pytest.mark.asyncio
    @patch("goldlapel.asyncio._proxy._actual_start", new_callable=AsyncMock)
    async def test_double_await_raises(self, mock_actual):
        mock_actual.return_value = MagicMock(spec=AsyncGoldLapel, name="fake_gl")
        handle = gl_async.start("postgresql://host/db")
        await handle
        with pytest.raises(RuntimeError, match="already consumed"):
            await handle

    @pytest.mark.asyncio
    @patch("goldlapel.asyncio._proxy._actual_start", new_callable=AsyncMock)
    async def test_fresh_handle_per_call(self, mock_actual):
        # Calling start() again yields a fresh, usable handle — the
        # single-use guard is per-handle, not global.
        fake_gl = MagicMock(spec=AsyncGoldLapel, name="fake_gl")
        mock_actual.return_value = fake_gl
        gl1 = await gl_async.start("postgresql://host/db")
        gl2 = await gl_async.start("postgresql://host/db")
        assert gl1 is fake_gl
        assert gl2 is fake_gl


class TestAsyncContextManager:
    @pytest.mark.asyncio
    async def test_aenter_calls_start_if_not_running(self):
        inst = AsyncGoldLapel.__new__(AsyncGoldLapel)
        inst._sync = MagicMock()
        inst._sync.running = False
        inst._conn = None
        with patch.object(inst, "start", new=AsyncMock()) as mock_start, \
             patch.object(inst, "stop", new=AsyncMock()) as mock_stop:
            async with inst as entered:
                assert entered is inst
                mock_start.assert_awaited_once()
            mock_stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_aenter_skips_start_when_running(self):
        inst = AsyncGoldLapel.__new__(AsyncGoldLapel)
        inst._sync = MagicMock()
        inst._sync.running = True
        inst._conn = MagicMock()
        with patch.object(inst, "start", new=AsyncMock()) as mock_start, \
             patch.object(inst, "stop", new=AsyncMock()) as mock_stop:
            async with inst:
                pass
            mock_start.assert_not_called()
            mock_stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_aexit_runs_even_on_exception(self):
        inst = AsyncGoldLapel.__new__(AsyncGoldLapel)
        inst._sync = MagicMock()
        inst._sync.running = True
        inst._conn = MagicMock()
        with patch.object(inst, "start", new=AsyncMock()), \
             patch.object(inst, "stop", new=AsyncMock()) as mock_stop:
            with pytest.raises(ValueError):
                async with inst:
                    raise ValueError("bang")
            mock_stop.assert_awaited_once()


class TestAsyncUsing:
    @pytest.mark.asyncio
    async def test_using_sets_and_reverts_scoped_conn(self):
        inst = AsyncGoldLapel.__new__(AsyncGoldLapel)
        sync = MagicMock()
        sync._using_conn.set.return_value = "token"
        inst._sync = sync

        user_conn = MagicMock(name="user_conn")
        async with inst.using(user_conn):
            sync._using_conn.set.assert_called_once_with(user_conn)
        sync._using_conn.reset.assert_called_once_with("token")

    @pytest.mark.asyncio
    async def test_using_reverts_on_exception(self):
        inst = AsyncGoldLapel.__new__(AsyncGoldLapel)
        sync = MagicMock()
        sync._using_conn.set.return_value = "token"
        inst._sync = sync

        user_conn = MagicMock(name="user_conn")
        with pytest.raises(RuntimeError):
            async with inst.using(user_conn):
                raise RuntimeError("bang")
        sync._using_conn.reset.assert_called_once_with("token")


class TestAsyncUsingScopeIsolation:
    """Regression: Task A's `using(conn)` scope must not leak to a sibling
    Task B running concurrently via `asyncio.gather`.

    Analogue of Ruby's test_async_native.rb::test_using_scope_under_async_reactor.
    Ruby once had a real bug in this class where fiber-local scope leaked
    across sibling fibers; this test codifies the expected Python-asyncio
    behavior so a future refactor from ContextVar to instance state
    (e.g. `self._scope_conn = conn`) would be caught.

    Why this works with ContextVar: `asyncio.gather` wraps each coroutine
    via `ensure_future`, and `Task.__init__` copies the *current* context
    at task-creation time. Task A's `using()` set() runs inside Task A's
    context copy and is never visible to Task B's copy. If scope were
    stored in shared instance state, both tasks would see the same slot
    and B would observe A's conn.

    Synchronization is deterministic (asyncio.Event), not timing-based.
    """

    @pytest.mark.asyncio
    async def test_using_scope_does_not_leak_to_sibling_task(self):
        # Real AsyncGoldLapel instance wired to a real GoldLapel (real
        # ContextVar) — no subprocess spawned because we never call start().
        # The internal _conn is a distinctive mock so we can assert B's
        # effective conn against it.
        inst = AsyncGoldLapel.__new__(AsyncGoldLapel)
        inst._sync = GoldLapel("postgresql://fake/db", proxy_port=0)
        default_conn = MagicMock(name="default_internal_conn")
        inst._conn = default_conn

        conn_a = MagicMock(name="conn_a_scoped_by_task_a")

        enter_using = asyncio.Event()     # A → B: scope is now active on A
        b_observed = asyncio.Event()      # B → A: I've recorded my effective
        observations = {}

        async def task_a():
            # A enters using(conn_a); while inside, hand control to B and
            # wait until B has observed its effective conn.
            async with inst.using(conn_a):
                # Sanity: inside A's context, A's effective conn is conn_a.
                observations["a_effective_inside"] = inst._effective_conn(None)
                enter_using.set()
                await b_observed.wait()
            # After the block exits, A's context is restored to default.
            observations["a_effective_after"] = inst._effective_conn(None)

        async def task_b():
            # B waits until A is inside the using block, then samples the
            # effective conn. A real scope leak (shared instance state)
            # would surface here as `conn_a`.
            await enter_using.wait()
            observations["b_effective_while_a_scoped"] = inst._effective_conn(None)
            b_observed.set()

        await asyncio.gather(task_a(), task_b())

        assert observations["a_effective_inside"] is conn_a, (
            "Task A should see its own scoped conn_a inside using()"
        )
        assert observations["b_effective_while_a_scoped"] is default_conn, (
            "SCOPE LEAK: Task B observed Task A's scoped conn_a. "
            "The async using() block is storing scope in shared state "
            "instead of a ContextVar, so sibling tasks corrupt each other."
        )
        assert observations["a_effective_after"] is default_conn, (
            "Task A's scope should unwind to the default conn after using()"
        )

    @pytest.mark.asyncio
    async def test_two_sibling_using_blocks_do_not_cross_contaminate(self):
        """Stronger form: two sibling tasks each inside their own using()
        block concurrently. Each must see its own scoped conn, never the
        other's. Covers the symmetric leak case.

        Rendezvous via a pair of Events so both tasks are guaranteed to be
        inside their respective using() blocks before either samples —
        otherwise a one-sided leak from the earlier-entering task could
        slip past.
        """
        inst = AsyncGoldLapel.__new__(AsyncGoldLapel)
        inst._sync = GoldLapel("postgresql://fake/db", proxy_port=0)
        inst._conn = MagicMock(name="default_internal_conn")

        conn_a = MagicMock(name="conn_a")
        conn_b = MagicMock(name="conn_b")

        a_entered = asyncio.Event()
        b_entered = asyncio.Event()
        observations = {}

        async def task_a():
            async with inst.using(conn_a):
                a_entered.set()
                await b_entered.wait()
                observations["a"] = inst._effective_conn(None)

        async def task_b():
            async with inst.using(conn_b):
                b_entered.set()
                await a_entered.wait()
                observations["b"] = inst._effective_conn(None)

        await asyncio.gather(task_a(), task_b())

        assert observations["a"] is conn_a, (
            "Task A should see conn_a; got something else — cross-contamination"
        )
        assert observations["b"] is conn_b, (
            "Task B should see conn_b; got something else — cross-contamination"
        )


class TestEffectiveConn:
    """`_effective_conn` precedence: explicit kwarg > using() scope > internal."""

    def test_explicit_kwarg_wins(self):
        inst = AsyncGoldLapel.__new__(AsyncGoldLapel)
        inst._sync = MagicMock()
        inst._sync._using_conn.get.return_value = MagicMock(name="scoped")
        inst._conn = MagicMock(name="internal")
        explicit = MagicMock(name="explicit")
        assert inst._effective_conn(explicit) is explicit

    def test_scoped_beats_internal(self):
        inst = AsyncGoldLapel.__new__(AsyncGoldLapel)
        inst._sync = MagicMock()
        scoped = MagicMock(name="scoped")
        inst._sync._using_conn.get.return_value = scoped
        inst._conn = MagicMock(name="internal")
        assert inst._effective_conn(None) is scoped

    def test_internal_when_no_override(self):
        inst = AsyncGoldLapel.__new__(AsyncGoldLapel)
        inst._sync = MagicMock()
        inst._sync._using_conn.get.return_value = None
        internal = MagicMock(name="internal")
        inst._conn = internal
        assert inst._effective_conn(None) is internal


class TestAllMethodsAreAsync:
    """Every auto-derived wrapper method on AsyncGoldLapel should be async
    (coroutine or async gen). Generator-vs-coroutine is auto-detected from
    the underlying util — see _is_async_generator_util in _proxy.py."""
    @pytest.mark.parametrize("method_name", _AUTO_DERIVED_NAMES)
    def test_method_is_async(self, method_name):
        from goldlapel.asyncio import _utils as autils
        method = getattr(AsyncGoldLapel, method_name)
        util_fn = getattr(autils, method_name, None)
        if util_fn is not None and inspect.isasyncgenfunction(util_fn):
            # Async generators like doc_find_cursor aren't coroutine functions;
            # they're async generator functions. Either is acceptable "async".
            assert inspect.isasyncgenfunction(method), \
                f"AsyncGoldLapel.{method_name} should be an async generator"
        else:
            assert inspect.iscoroutinefunction(method), \
                f"AsyncGoldLapel.{method_name} should be async def"


class TestAsyncMethodDelegation:
    """Wrapper methods should delegate to the corresponding function in
    goldlapel.asyncio._utils, passing the effective conn first."""

    @pytest.mark.asyncio
    async def test_search_delegates_to_utils(self):
        inst = AsyncGoldLapel.__new__(AsyncGoldLapel)
        inst._sync = MagicMock()
        inst._sync._using_conn.get.return_value = None
        fake_conn = MagicMock(name="internal_conn")
        inst._conn = fake_conn

        with patch(
            "goldlapel.asyncio._utils.search",
            new=AsyncMock(return_value="ok"),
        ) as mock_search:
            result = await inst.search("articles", "body", "query")
            assert result == "ok"
            mock_search.assert_awaited_once_with(
                fake_conn, "articles", "body", "query",
            )

    @pytest.mark.asyncio
    async def test_method_passes_explicit_conn_kwarg(self):
        # Wraps a non-doc auto-derived method (search) since doc_* methods
        # are nested under gl.documents and need a DDL-pattern fetch.
        inst = AsyncGoldLapel.__new__(AsyncGoldLapel)
        inst._sync = MagicMock()
        inst._sync._using_conn.get.return_value = None
        inst._conn = MagicMock()
        override = MagicMock(name="explicit")

        with patch(
            "goldlapel.asyncio._utils.search",
            new=AsyncMock(return_value=[{"id": 1}]),
        ) as mock_search:
            result = await inst.search("articles", "body", "query", conn=override)
            assert result == [{"id": 1}]
            mock_search.assert_awaited_once_with(
                override, "articles", "body", "query",
            )


class TestAllMethodsCount:
    def test_async_class_has_same_surface_as_sync(self):
        # Strict parity check (sync vs async public methods, modulo skip list)
        # lives in tests/test_async_parity.py — that's the single source of
        # truth for surface drift detection. This test is kept as a coarse
        # smoke check that the auto-derive at module load actually populated
        # AsyncGoldLapel with the expected method count.
        sync_methods = {
            name for name in dir(GoldLapel)
            if not name.startswith("_") and callable(getattr(GoldLapel, name))
        }
        async_methods = {
            name for name in dir(AsyncGoldLapel)
            if not name.startswith("_") and callable(getattr(AsyncGoldLapel, name))
        }
        expected = sync_methods - _ASYNC_SKIPPED
        missing = expected - async_methods
        assert not missing, f"async wrapper missing: {missing}"


# -- Helpers for subprocess-mocking startup-banner tests ---------------------

def _reset_proxy_state():
    from goldlapel import proxy as proxy_mod
    proxy_mod._instances.clear()


def _mock_popen_instance():
    proc = MagicMock()
    proc.poll.return_value = None
    proc.stderr = MagicMock()
    return proc


class TestAsyncPortAllocation:
    """Async starts share the sync registry and port rule: each proxy claims
    its proxy port and its dashboard port, so auto-assigned pairs never
    overlap — across async, sync, or a mix of both."""

    @staticmethod
    def _fake_asyncpg():
        fake_asyncpg = MagicMock()
        fake_raw = MagicMock()
        fake_raw.set_type_codec = AsyncMock()
        fake_raw.close = AsyncMock()
        fake_asyncpg.connect = AsyncMock(return_value=fake_raw)
        return fake_asyncpg

    def _patches(self):
        from contextlib import ExitStack
        stack = ExitStack()
        for target, kwargs in (
            ("goldlapel.asyncio._proxy._detect_asyncpg", {"return_value": self._fake_asyncpg()}),
            ("goldlapel.asyncio._proxy._find_binary", {"return_value": "/usr/bin/goldlapel"}),
            ("goldlapel.asyncio._proxy._wait_for_port", {"return_value": True}),
            ("goldlapel.asyncio._proxy._kill_orphan_on_port", {}),
            ("subprocess.Popen", {"side_effect": lambda *a, **kw: _mock_popen_instance()}),
        ):
            stack.enter_context(patch(target, **kwargs))
        return stack

    @pytest.mark.asyncio
    async def test_two_upstreams_get_non_overlapping_pairs(self):
        _reset_proxy_state()
        try:
            with self._patches():
                a = await gl_async.start("postgresql://host-a:5432/db_a", silent=True)
                b = await gl_async.start("postgresql://host-b:5432/db_b", silent=True)
            assert a._sync._proxy_port == 7932
            assert b._sync._proxy_port == 7934
            assert b._sync._dashboard_port == 7935
        finally:
            _reset_proxy_state()

    @pytest.mark.asyncio
    async def test_explicit_dashboard_port_is_skipped(self):
        _reset_proxy_state()
        try:
            with self._patches():
                await gl_async.start(
                    "postgresql://host-a:5432/db_a", dashboard_port=7934, silent=True,
                )
                b = await gl_async.start("postgresql://host-b:5432/db_b", silent=True)
            assert b._sync._proxy_port == 7935
        finally:
            _reset_proxy_state()

    @pytest.mark.asyncio
    async def test_sync_and_async_share_claims(self):
        from goldlapel import proxy as proxy_mod
        _reset_proxy_state()
        try:
            with self._patches(), \
                 patch("goldlapel.proxy._detect_sync_driver",
                       return_value=("psycopg3", MagicMock())), \
                 patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel"), \
                 patch("goldlapel.proxy._wait_for_port", return_value=True), \
                 patch("goldlapel.proxy._kill_orphan_on_port"):
                sync_gl = proxy_mod.start("postgresql://host-a:5432/db_a", silent=True)
                b = await gl_async.start("postgresql://host-b:5432/db_b", silent=True)
            assert sync_gl._proxy_port == 7932
            assert b._sync._proxy_port == 7934
        finally:
            _reset_proxy_state()

    @pytest.mark.asyncio
    async def test_stop_releases_ports(self):
        from goldlapel import proxy as proxy_mod
        _reset_proxy_state()
        try:
            with self._patches():
                a = await gl_async.start("postgresql://host-a:5432/db_a", silent=True)
                await a.stop()
                assert "postgresql://host-a:5432/db_a" not in proxy_mod._instances
                b = await gl_async.start("postgresql://host-b:5432/db_b", silent=True)
            assert b._sync._proxy_port == 7932
        finally:
            _reset_proxy_state()


class TestAsyncStartupBanner:
    """Native-asyncpg async start() writes the same stderr banner as sync —
    suppressed by config={'silent': True}."""

    def _base_patches(self):
        """Return a list of patches we apply in each banner test."""
        fake_asyncpg = MagicMock()
        fake_raw = MagicMock()
        fake_raw.set_type_codec = AsyncMock()
        fake_raw.close = AsyncMock()
        fake_asyncpg.connect = AsyncMock(return_value=fake_raw)
        return fake_asyncpg

    @pytest.mark.asyncio
    async def test_async_banner_writes_to_stderr(self, capsys):
        _reset_proxy_state()
        try:
            fake_asyncpg = self._base_patches()
            with patch("goldlapel.asyncio._proxy._detect_asyncpg", return_value=fake_asyncpg), \
                 patch("goldlapel.asyncio._proxy._find_binary", return_value="/usr/bin/goldlapel"), \
                 patch("goldlapel.asyncio._proxy._wait_for_port", return_value=True), \
                 patch("goldlapel.asyncio._proxy._kill_orphan_on_port"), \
                 patch("goldlapel.asyncio._proxy._make_proxy_url", return_value="postgresql://localhost:7932/db"), \
                 patch("subprocess.Popen", side_effect=lambda *a, **kw: _mock_popen_instance()):
                await gl_async.start("postgresql://host:5432/mydb")
                captured = capsys.readouterr()
                assert "goldlapel →" not in captured.out
                assert "goldlapel →" in captured.err
        finally:
            _reset_proxy_state()

    @pytest.mark.asyncio
    async def test_async_banner_omits_disabled_dashboard(self, capsys):
        _reset_proxy_state()
        try:
            fake_asyncpg = self._base_patches()
            with patch("goldlapel.asyncio._proxy._detect_asyncpg", return_value=fake_asyncpg), \
                 patch("goldlapel.asyncio._proxy._find_binary", return_value="/usr/bin/goldlapel"), \
                 patch("goldlapel.asyncio._proxy._wait_for_port", return_value=True), \
                 patch("goldlapel.asyncio._proxy._kill_orphan_on_port"), \
                 patch("subprocess.Popen", side_effect=lambda *a, **kw: _mock_popen_instance()):
                await gl_async.start("postgresql://host:5432/mydb", dashboard_port=0)
                captured = capsys.readouterr()
                assert "goldlapel → :7932 (proxy)" in captured.err
                assert "(dashboard)" not in captured.err
        finally:
            _reset_proxy_state()

    @pytest.mark.asyncio
    async def test_async_silent_suppresses_banner(self, capsys):
        _reset_proxy_state()
        try:
            fake_asyncpg = self._base_patches()
            with patch("goldlapel.asyncio._proxy._detect_asyncpg", return_value=fake_asyncpg), \
                 patch("goldlapel.asyncio._proxy._find_binary", return_value="/usr/bin/goldlapel"), \
                 patch("goldlapel.asyncio._proxy._wait_for_port", return_value=True), \
                 patch("goldlapel.asyncio._proxy._kill_orphan_on_port"), \
                 patch("goldlapel.asyncio._proxy._make_proxy_url", return_value="postgresql://localhost:7932/db"), \
                 patch("subprocess.Popen", side_effect=lambda *a, **kw: _mock_popen_instance()):
                await gl_async.start("postgresql://host:5432/mydb", silent=True)
                captured = capsys.readouterr()
                assert "goldlapel →" not in captured.out
                assert "goldlapel →" not in captured.err
        finally:
            _reset_proxy_state()


class TestAsyncPromotedDisableKwargs:
    """The 3 promoted disable flags (proxy_cache / sqloptimize /
    auto_indexes) must reach the proxy CLI on the async path the same way
    they do on sync. The async constructor stores them on the underlying
    GoldLapel; AsyncGoldLapel.start() emits the matching `--disable-X`
    flags into the spawn argv."""

    def _base_patches(self):
        fake_asyncpg = MagicMock()
        fake_raw = MagicMock()
        fake_raw.set_type_codec = AsyncMock()
        fake_raw.close = AsyncMock()
        fake_asyncpg.connect = AsyncMock(return_value=fake_raw)
        return fake_asyncpg

    # -- Stored attribute defaults / mutability ------------------------

    def test_async_disable_proxy_cache_defaults_false(self):
        gl = AsyncGoldLapel("postgresql://localhost:5432/mydb")
        assert gl._sync._disable_proxy_cache is False

    def test_async_disable_proxy_cache_true_stored(self):
        gl = AsyncGoldLapel(
            "postgresql://localhost:5432/mydb", disable_proxy_cache=True,
        )
        assert gl._sync._disable_proxy_cache is True

    def test_async_disable_sqloptimize_true_stored(self):
        gl = AsyncGoldLapel(
            "postgresql://localhost:5432/mydb", disable_sqloptimize=True,
        )
        assert gl._sync._disable_sqloptimize is True

    def test_async_disable_auto_indexes_true_stored(self):
        gl = AsyncGoldLapel(
            "postgresql://localhost:5432/mydb", disable_auto_indexes=True,
        )
        assert gl._sync._disable_auto_indexes is True

    @pytest.mark.parametrize("kwarg", [
        "invalidation_port", "disable_native_cache", "aggressive_verify",
        "disable_matviews",
    ])
    def test_async_removed_cache_kwargs_raise(self, kwarg):
        # The in-process cache and matviews are gone — no aliases.
        with pytest.raises(TypeError):
            gl_async.start("postgresql://localhost:5432/mydb", **{kwarg: True})

    # -- argv emission --------------------------------------------------

    @pytest.mark.asyncio
    async def test_async_all_three_flags_emit(self):
        # All 3 flags set → all 3 --disable-* CLI args present in spawn argv.
        _reset_proxy_state()
        try:
            fake_asyncpg = self._base_patches()
            captured_cmd = []

            def capture_popen(*args, **kwargs):
                captured_cmd.append(args[0])
                return _mock_popen_instance()

            with patch("goldlapel.asyncio._proxy._detect_asyncpg", return_value=fake_asyncpg), \
                 patch("goldlapel.asyncio._proxy._find_binary", return_value="/usr/bin/goldlapel"), \
                 patch("goldlapel.asyncio._proxy._wait_for_port", return_value=True), \
                 patch("goldlapel.asyncio._proxy._kill_orphan_on_port"), \
                 patch("goldlapel.asyncio._proxy._make_proxy_url", return_value="postgresql://localhost:7932/db"), \
                 patch("subprocess.Popen", side_effect=capture_popen):
                await gl_async.start(
                    "postgresql://host:5432/mydb",
                    disable_proxy_cache=True,
                    disable_sqloptimize=True,
                    disable_auto_indexes=True,
                    silent=True,
                )
            assert captured_cmd, "Popen was not called"
            cmd = captured_cmd[0]
            for flag in (
                "--disable-proxy-cache",
                "--disable-sqloptimize", "--disable-auto-indexes",
            ):
                assert flag in cmd, f"{flag} missing from async spawn argv"
        finally:
            _reset_proxy_state()

    @pytest.mark.asyncio
    async def test_async_no_disable_flags_in_default_argv(self):
        # Default state: none of the 3 promoted flags should appear.
        _reset_proxy_state()
        try:
            fake_asyncpg = self._base_patches()
            captured_cmd = []

            def capture_popen(*args, **kwargs):
                captured_cmd.append(args[0])
                return _mock_popen_instance()

            with patch("goldlapel.asyncio._proxy._detect_asyncpg", return_value=fake_asyncpg), \
                 patch("goldlapel.asyncio._proxy._find_binary", return_value="/usr/bin/goldlapel"), \
                 patch("goldlapel.asyncio._proxy._wait_for_port", return_value=True), \
                 patch("goldlapel.asyncio._proxy._kill_orphan_on_port"), \
                 patch("goldlapel.asyncio._proxy._make_proxy_url", return_value="postgresql://localhost:7932/db"), \
                 patch("subprocess.Popen", side_effect=capture_popen):
                await gl_async.start("postgresql://host:5432/mydb", silent=True)
            assert captured_cmd, "Popen was not called"
            cmd = captured_cmd[0]
            for flag in (
                "--disable-proxy-cache",
                "--disable-sqloptimize", "--disable-auto-indexes",
            ):
                assert flag not in cmd, (
                    f"{flag} unexpectedly present in default async spawn argv"
                )
        finally:
            _reset_proxy_state()


class TestAsyncReuse:
    """A second async start for an upstream whose proxy is already running
    reuses that proxy but returns a fully initialised AsyncGoldLapel: every
    namespace helper works, bound to the returned object."""

    @staticmethod
    def _patches():
        return TestAsyncPortAllocation()._patches()

    @pytest.mark.asyncio
    async def test_reused_proxy_returns_complete_instance(self):
        _reset_proxy_state()
        try:
            with self._patches():
                url = "postgresql://host-a:5432/db_a"
                a = await gl_async.start(url, silent=True)
                b = await gl_async.start(url, silent=True)
            assert b is not a
            assert b._sync is a._sync  # same proxy subprocess
            for family in ("documents", "streams", "counters", "zsets",
                           "hashes", "queues", "geos"):
                assert getattr(b, family)._gl is b
            assert b._conn is not None
        finally:
            _reset_proxy_state()

    @pytest.mark.asyncio
    async def test_reuses_proxy_started_by_sync(self):
        from goldlapel import proxy as proxy_mod
        _reset_proxy_state()
        try:
            with self._patches(), \
                 patch("goldlapel.proxy._detect_sync_driver",
                       return_value=("psycopg3", MagicMock())), \
                 patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel"), \
                 patch("goldlapel.proxy._wait_for_port", return_value=True), \
                 patch("goldlapel.proxy._kill_orphan_on_port"):
                url = "postgresql://host-a:5432/db_a"
                sync_gl = proxy_mod.start(url, silent=True)
                b = await gl_async.start(url, silent=True)
            assert b._sync is sync_gl
            assert b.documents._gl is b
        finally:
            _reset_proxy_state()


class TestAsyncExplicitPortCollision:
    @staticmethod
    def _patches():
        return TestAsyncPortAllocation()._patches()

    @pytest.mark.asyncio
    async def test_explicit_port_held_by_other_upstream_raises(self):
        from goldlapel import proxy as proxy_mod
        _reset_proxy_state()
        try:
            with self._patches():
                await gl_async.start("postgresql://u:secret@host-a:5432/db_a", silent=True)
                with patch("goldlapel.asyncio._proxy._kill_orphan_on_port") as orphan:
                    with pytest.raises(RuntimeError) as exc:
                        await gl_async.start(
                            "postgresql://host-b:5432/db_b", proxy_port=7933, silent=True,
                        )
                    orphan.assert_not_called()
            msg = str(exc.value)
            assert "7933" in msg
            assert "host-a:5432/db_a" in msg
            assert "secret" not in msg
            assert "postgresql://host-b:5432/db_b" not in proxy_mod._instances
        finally:
            _reset_proxy_state()


class TestAsyncApiKey:
    @pytest.mark.asyncio
    async def test_api_key_reaches_subprocess_env(self, monkeypatch):
        monkeypatch.delenv("GOLDLAPEL_API_KEY", raising=False)
        _reset_proxy_state()
        captured = {}

        def fake_popen(cmd, **kwargs):
            captured["cmd"] = cmd
            captured["env"] = kwargs.get("env", {})
            return _mock_popen_instance()

        try:
            with TestAsyncPortAllocation()._patches(), \
                 patch("subprocess.Popen", side_effect=fake_popen):
                await gl_async.start(
                    "postgresql://host:5432/db", api_key="gl_test_async", silent=True,
                )
            assert captured["env"].get("GOLDLAPEL_API_KEY") == "gl_test_async"
            assert "gl_test_async" not in captured["cmd"]
        finally:
            _reset_proxy_state()
