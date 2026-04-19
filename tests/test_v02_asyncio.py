"""Tests for the v0.2.0 async factory API in goldlapel.asyncio."""

from unittest.mock import MagicMock, patch

import pytest

import goldlapel.asyncio as gl_async
from goldlapel.asyncio._proxy import AsyncGoldLapel, _WRAPPED_METHODS


class TestAsyncStart:
    @pytest.mark.asyncio
    @patch("goldlapel.asyncio._proxy._ensure_running")
    @patch("goldlapel.asyncio._proxy._detect_sync_driver", return_value=("psycopg2", MagicMock()))
    async def test_returns_async_goldlapel(self, mock_detect, mock_ensure):
        fake_sync = MagicMock(name="fake_sync_instance")
        mock_ensure.return_value = fake_sync
        result = await gl_async.start("postgresql://host/db")
        assert isinstance(result, AsyncGoldLapel)
        assert result._sync is fake_sync

    @pytest.mark.asyncio
    @patch("goldlapel.asyncio._proxy._detect_sync_driver", return_value=(None, None))
    async def test_raises_without_driver(self, mock_detect):
        with pytest.raises(ImportError, match="Postgres driver"):
            await gl_async.start("postgresql://host/db")


class TestStartHandleDoubleUse:
    """Regression for v0.2 review finding (MEDIUM, Option B):
    _StartHandle is single-use. A second use (await then async-with, or
    vice versa) would spawn a second subprocess while orphaning the first —
    we raise RuntimeError instead.
    """

    @pytest.mark.asyncio
    @patch("goldlapel.asyncio._proxy._ensure_running")
    @patch("goldlapel.asyncio._proxy._detect_sync_driver", return_value=("psycopg2", MagicMock()))
    async def test_await_then_async_with_raises(self, mock_detect, mock_ensure):
        mock_ensure.return_value = MagicMock(name="fake_sync_instance")
        handle = gl_async.start("postgresql://host/db")
        await handle  # first use — consumes the handle
        with pytest.raises(RuntimeError, match="already consumed"):
            async with handle:
                pass

    @pytest.mark.asyncio
    @patch("goldlapel.asyncio._proxy._ensure_running")
    @patch("goldlapel.asyncio._proxy._detect_sync_driver", return_value=("psycopg2", MagicMock()))
    async def test_async_with_then_await_raises(self, mock_detect, mock_ensure):
        mock_ensure.return_value = MagicMock(name="fake_sync_instance")
        handle = gl_async.start("postgresql://host/db")
        async with handle:
            pass  # first use — consumes the handle
        with pytest.raises(RuntimeError, match="already consumed"):
            await handle

    @pytest.mark.asyncio
    @patch("goldlapel.asyncio._proxy._ensure_running")
    @patch("goldlapel.asyncio._proxy._detect_sync_driver", return_value=("psycopg2", MagicMock()))
    async def test_double_await_raises(self, mock_detect, mock_ensure):
        mock_ensure.return_value = MagicMock(name="fake_sync_instance")
        handle = gl_async.start("postgresql://host/db")
        await handle
        with pytest.raises(RuntimeError, match="already consumed"):
            await handle

    @pytest.mark.asyncio
    @patch("goldlapel.asyncio._proxy._ensure_running")
    @patch("goldlapel.asyncio._proxy._detect_sync_driver", return_value=("psycopg2", MagicMock()))
    async def test_fresh_handle_per_call(self, mock_detect, mock_ensure):
        # Calling start() again yields a fresh, usable handle — the
        # single-use guard is per-handle, not global.
        mock_ensure.return_value = MagicMock(name="fake_sync_instance")
        gl1 = await gl_async.start("postgresql://host/db")
        gl2 = await gl_async.start("postgresql://host/db")
        assert isinstance(gl1, AsyncGoldLapel)
        assert isinstance(gl2, AsyncGoldLapel)


class TestAsyncContextManager:
    @pytest.mark.asyncio
    async def test_aenter_calls_start_if_not_running(self):
        inst = AsyncGoldLapel.__new__(AsyncGoldLapel)
        inst._sync = MagicMock()
        inst._sync.running = False
        with patch.object(inst, "start") as mock_start, patch.object(inst, "stop") as mock_stop:
            async with inst as entered:
                assert entered is inst
                mock_start.assert_awaited_once()
            mock_stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_aenter_skips_start_when_running(self):
        inst = AsyncGoldLapel.__new__(AsyncGoldLapel)
        inst._sync = MagicMock()
        inst._sync.running = True
        with patch.object(inst, "start") as mock_start, patch.object(inst, "stop") as mock_stop:
            async with inst:
                pass
            mock_start.assert_not_called()
            mock_stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_aexit_runs_even_on_exception(self):
        inst = AsyncGoldLapel.__new__(AsyncGoldLapel)
        inst._sync = MagicMock()
        inst._sync.running = True
        with patch.object(inst, "start"), patch.object(inst, "stop") as mock_stop:
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


class TestAllMethodsAreAsync:
    """Every wrapper method on AsyncGoldLapel should be an async def."""
    @pytest.mark.parametrize("method_name", _WRAPPED_METHODS)
    def test_method_is_coroutine_function(self, method_name):
        import inspect
        method = getattr(AsyncGoldLapel, method_name)
        assert inspect.iscoroutinefunction(method), \
            f"AsyncGoldLapel.{method_name} should be async def"


class TestAsyncMethodDelegation:
    @pytest.mark.asyncio
    async def test_search_delegates_to_sync(self):
        inst = AsyncGoldLapel.__new__(AsyncGoldLapel)
        inst._sync = MagicMock()
        inst._sync.search = MagicMock(return_value="ok")

        result = await inst.search("articles", "body", "query")
        assert result == "ok"
        inst._sync.search.assert_called_once_with("articles", "body", "query", conn=None)

    @pytest.mark.asyncio
    async def test_search_passes_conn_kwarg(self):
        inst = AsyncGoldLapel.__new__(AsyncGoldLapel)
        inst._sync = MagicMock()
        inst._sync.doc_insert = MagicMock(return_value={"id": 1})
        override = MagicMock()

        result = await inst.doc_insert("events", {"type": "x"}, conn=override)
        assert result == {"id": 1}
        inst._sync.doc_insert.assert_called_once_with("events", {"type": "x"}, conn=override)


class TestAllMethodsCount:
    def test_async_class_has_same_surface_as_sync(self):
        from goldlapel.proxy import GoldLapel
        sync_methods = {
            name for name in dir(GoldLapel)
            if not name.startswith("_") and callable(getattr(GoldLapel, name))
            and name not in {"start", "stop", "using"}
        }
        async_wrapped = set(_WRAPPED_METHODS)
        missing = sync_methods - async_wrapped
        # any sync method not wrapped asynchronously is a gap worth surfacing
        assert not missing, f"async wrapper missing: {missing}"


class TestAsyncStartupBanner:
    """Regression tests for async start()'s banner — it goes through the sync
    path under asyncio.to_thread, so stream/silent behavior must match sync.
    """

    def _mock_popen(self):
        proc = MagicMock()
        proc.poll.return_value = None
        proc.stderr = MagicMock()
        return proc

    def _reset(self):
        from goldlapel import proxy as proxy_mod
        from goldlapel.proxy import DEFAULT_PORT
        proxy_mod._instances.clear()
        proxy_mod._next_port = DEFAULT_PORT

    @pytest.mark.asyncio
    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", return_value=("psycopg3", MagicMock(**{"connect.return_value": MagicMock()})))
    @patch("goldlapel.asyncio._proxy._detect_sync_driver", return_value=("psycopg3", MagicMock()))
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    async def test_async_banner_writes_to_stderr(
        self, mock_find, mock_popen, mock_wait,
        mock_async_detect, mock_sync_detect, mock_wrap, capsys,
    ):
        self._reset()
        try:
            mock_popen.side_effect = lambda *a, **kw: self._mock_popen()

            await gl_async.start("postgresql://host:5432/mydb")
            captured = capsys.readouterr()
            assert "goldlapel →" not in captured.out
            assert "goldlapel →" in captured.err
        finally:
            self._reset()

    @pytest.mark.asyncio
    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", return_value=("psycopg3", MagicMock(**{"connect.return_value": MagicMock()})))
    @patch("goldlapel.asyncio._proxy._detect_sync_driver", return_value=("psycopg3", MagicMock()))
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    async def test_async_silent_suppresses_banner(
        self, mock_find, mock_popen, mock_wait,
        mock_async_detect, mock_sync_detect, mock_wrap, capsys,
    ):
        self._reset()
        try:
            mock_popen.side_effect = lambda *a, **kw: self._mock_popen()

            await gl_async.start("postgresql://host:5432/mydb", config={"silent": True})
            captured = capsys.readouterr()
            assert "goldlapel →" not in captured.out
            assert "goldlapel →" not in captured.err
        finally:
            self._reset()
