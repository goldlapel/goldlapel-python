"""AsyncGoldLapel — native-asyncpg async façade over the Gold Lapel proxy.

Spawns the proxy subprocess (via the sync helpers in goldlapel.proxy), opens
an asyncpg.Connection wrapped with AsyncCachedConnection, and exposes the same
54 wrapper methods as sync GoldLapel, implemented as native `async def` that
calls into goldlapel.asyncio._utils.

Public API (unchanged from v0.2.0):
  - goldlapel.asyncio.start(url) → AsyncGoldLapel (awaitable or async CM)
  - every wrapper method identical signature
  - gl.using(conn) scoped override — ContextVar semantics
  - conn= per-call kwarg with precedence: explicit > using > internal

Internal conn is an AsyncCachedConnection wrapping asyncpg.Connection, so
cache invalidation / read caching behaves identically to the sync path.

When `asyncpg` is not importable, `start()` raises ImportError with the
install hint. The sync fallback (psycopg3 async) is not implemented here —
asyncpg is the canonical async driver and is declared a dev dependency.
"""

import asyncio
import sys
from contextlib import asynccontextmanager

from goldlapel.proxy import (
    _config_to_args,
    _find_binary,
    _kill_orphan_on_port,
    _make_proxy_url,
    _set_pdeathsig,
    _wait_for_port,
    _STARTUP_TIMEOUT,
    _instances,
    _lock,
    _cleanup_registered,
    _next_port,
    DEFAULT_PORT,
    GoldLapel,
)
from goldlapel.asyncio import _utils as autils


# All 54 wrapper-method names that exist on the sync GoldLapel class.
_WRAPPED_METHODS = (
    # -- Document store --
    "doc_create_collection", "doc_insert", "doc_insert_many", "doc_find",
    "doc_find_one", "doc_update", "doc_update_one", "doc_delete",
    "doc_delete_one", "doc_find_one_and_update", "doc_find_one_and_delete",
    "doc_distinct", "doc_find_cursor", "doc_count", "doc_create_index",
    "doc_aggregate", "doc_watch", "doc_unwatch", "doc_create_ttl_index",
    "doc_remove_ttl_index", "doc_create_capped", "doc_remove_cap",
    # -- Search --
    "search", "search_fuzzy", "search_phonetic", "similar", "suggest",
    "facets", "aggregate", "create_search_config",
    # -- Pub/sub & queues --
    "publish", "subscribe", "enqueue", "dequeue",
    # -- Counters --
    "incr", "get_counter",
    # -- Hashes --
    "hset", "hget", "hgetall", "hdel",
    # -- Sorted sets --
    "zadd", "zincrby", "zrange", "zrank", "zscore", "zrem",
    # -- Geo --
    "georadius", "geoadd", "geodist",
    # -- Misc --
    "count_distinct", "script",
    # -- Streams --
    "stream_add", "stream_create_group", "stream_read", "stream_ack",
    "stream_claim",
    # -- Percolator --
    "percolate_add", "percolate", "percolate_delete",
    # -- Analysis --
    "analyze", "explain_score",
)

# Methods that return async generators (can't be `await`ed, only `async for`).
_GENERATOR_METHODS = frozenset({"doc_find_cursor"})

# Methods that are already implemented above the dispatcher as explicit
# `async def` (e.g. subscribe/doc_watch block forever, doc_find_cursor is a
# generator). These names are SKIPPED when auto-generating wrappers.
_EXPLICITLY_IMPLEMENTED = frozenset({"doc_find_cursor"})


def _detect_asyncpg():
    try:
        import asyncpg
        return asyncpg
    except ImportError:
        return None


async def _open_asyncpg_conn(proxy_url):
    """Open an asyncpg connection to `proxy_url` and return the raw conn.

    `statement_cache_size=0` disables asyncpg's prepared-statement cache.
    The Gold Lapel proxy has a known CloseComplete-framing interaction with
    persistent prepared statements (see docs/wrapper-v0.2/03-proxy-closecomplete-framing.md
    in the main repo — the .NET wrapper hit the same thing). Disabling the
    cache sidesteps it; asyncpg parses on every call, which is fine for the
    wrapper-utility workload (short queries, many different SQL shapes).
    """
    asyncpg = _detect_asyncpg()
    conn = await asyncpg.connect(proxy_url, statement_cache_size=0)
    await autils._register_jsonb_codec(conn)
    return conn


class AsyncGoldLapel:
    """Native-asyncpg async façade over Gold Lapel.

    Spawns and owns the proxy subprocess (reusing the sync spawn helpers in
    goldlapel.proxy) and opens an asyncpg connection wrapped with the same
    AsyncCachedConnection used by user-supplied conns.
    """

    def __init__(self, upstream, config=None, port=None, extra_args=None):
        # Piggyback on the sync GoldLapel for subprocess/lifecycle state so
        # `using(conn)` / ContextVar semantics and stop-on-exit are identical.
        self._sync = GoldLapel(upstream, config=config, port=port, extra_args=extra_args)
        self._conn = None  # AsyncCachedConnection (wraps asyncpg.Connection)

    # -- Properties (sync access, no await) ---------------------------------

    @property
    def url(self):
        return self._sync.url

    @property
    def dashboard_url(self):
        return self._sync.dashboard_url

    @property
    def running(self):
        return self._sync.running

    @property
    def conn(self):
        if self._conn is None:
            raise RuntimeError("Not connected. Call start() first.")
        return self._conn

    # -- Lifecycle ----------------------------------------------------------

    async def start(self):
        """Spawn the proxy subprocess and open the internal asyncpg connection.

        If opening the connection fails after the subprocess is up, we tear
        down the subprocess before re-raising — same pattern as the sync
        GoldLapel.start() to avoid leaking orphaned binaries.
        """
        if self._sync.running and self._conn is not None:
            return self._sync.url

        # -- Subprocess spawn (mirrors GoldLapel.start without its driver path) --
        if self._sync._process and self._sync._process.poll() is None:
            # Subprocess already up (e.g. restarting after conn close) — just
            # need to reopen the asyncpg conn.
            pass
        else:
            import os
            import subprocess
            binary = _find_binary()
            cmd = [
                binary,
                "--upstream", self._sync._upstream,
                "--proxy-port", str(self._sync._port),
            ] + _config_to_args(self._sync._config) + self._sync._extra_args

            _kill_orphan_on_port(self._sync._port)

            env = os.environ.copy()
            env.setdefault("GOLDLAPEL_CLIENT", "python")
            popen_kwargs = dict(
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            if sys.platform == "linux":
                popen_kwargs["preexec_fn"] = _set_pdeathsig
            self._sync._process = subprocess.Popen(cmd, **popen_kwargs)

            if not _wait_for_port("127.0.0.1", self._sync._port, _STARTUP_TIMEOUT):
                self._sync._process.kill()
                stderr = self._sync._process.stderr.read().decode(errors="replace")
                self._sync._process.stderr.close()
                raise RuntimeError(
                    f"Gold Lapel failed to start on port {self._sync._port} "
                    f"within {_STARTUP_TIMEOUT}s.\nstderr: {stderr}"
                )
            self._sync._process.stderr.close()
            self._sync._proxy_url = _make_proxy_url(
                self._sync._upstream, self._sync._port,
            )

        # -- asyncpg connect + cache wrap, with cleanup on failure --
        asyncpg = _detect_asyncpg()
        if asyncpg is None:
            # Should not reach here — `start()` factory pre-checks — but guard
            # direct AsyncGoldLapel().start() calls too.
            self._tear_down_subprocess()
            raise ImportError(
                "Gold Lapel async wrapper needs asyncpg. "
                "Install with: pip install asyncpg"
            )

        try:
            raw = await _open_asyncpg_conn(self._sync._proxy_url)
            from goldlapel.wrap import wrap
            inv_port = int(
                (self._sync._config or {}).get("invalidation_port", self._sync._port + 2),
            )
            self._conn = wrap(raw, invalidation_port=inv_port)
        except BaseException:
            # Kill subprocess + close any half-open asyncpg conn before raising.
            await self._teardown_async()
            raise

        # Startup banner — matches the sync path's stderr banner.
        if not (self._sync._config or {}).get("silent", False):
            banner = (
                f"goldlapel → :{self._sync._port} (proxy) | "
                f"http://127.0.0.1:{self._sync._dashboard_port} (dashboard)"
            )
            print(banner, file=sys.stderr)

        return self._sync._proxy_url

    def _tear_down_subprocess(self):
        """Terminate the sync _process synchronously. Used from init-failure
        paths where we cannot `await`."""
        import subprocess
        proc = self._sync._process
        if proc and proc.poll() is None:
            try:
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
            except Exception:
                pass
        self._sync._process = None
        self._sync._proxy_url = None

    async def _teardown_async(self):
        """Close the asyncpg conn (if any) and terminate the subprocess."""
        if self._conn is not None:
            try:
                await self._conn.close()
            except Exception:
                pass
            self._conn = None
        self._tear_down_subprocess()

    async def stop(self):
        if self._conn is not None:
            try:
                await self._conn.close()
            except Exception:
                pass
            self._conn = None
        # Delegate subprocess shutdown to the sync helper (synchronous OS work —
        # no benefit from threading it).
        if self._sync._process and self._sync._process.poll() is None:
            import subprocess
            try:
                self._sync._process.terminate()
                try:
                    self._sync._process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self._sync._process.kill()
                    self._sync._process.wait()
            except Exception:
                pass
        self._sync._process = None
        self._sync._proxy_url = None

    # -- Async context manager ---------------------------------------------

    async def __aenter__(self):
        if not self.running:
            await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()
        return False

    # -- Scoped using() ----------------------------------------------------

    @asynccontextmanager
    async def using(self, conn):
        """Scoped override: all wrapper methods called inside this `async with`
        block will use `conn` (typically a caller-provided asyncpg Connection
        inside their own transaction) instead of the internal connection.

        Users may pass either a raw asyncpg.Connection or an AsyncCachedConnection
        wrapper. The utils layer handles both via _get_raw_connection.
        """
        token = self._sync._using_conn.set(conn)
        try:
            yield self
        finally:
            self._sync._using_conn.reset(token)

    def _effective_conn(self, override=None):
        if override is not None:
            return override
        scoped = self._sync._using_conn.get()
        if scoped is not None:
            return scoped
        return self.conn  # raises if not started


# -- Auto-generate async wrappers for each utility function ---------------

def _make_async_wrapper(name):
    async def method(self, *args, conn=None, **kwargs):
        # Look up the util fn lazily on the module, so test-time `patch(
        # "goldlapel.asyncio._utils.search", ...)` replaces it for us.
        util_fn = getattr(autils, name)
        return await util_fn(self._effective_conn(conn), *args, **kwargs)

    method.__name__ = name
    method.__qualname__ = f"AsyncGoldLapel.{name}"
    method.__doc__ = (
        f"Async wrapper for {name}. See goldlapel.utils.{name} for signature "
        f"(native asyncpg impl in goldlapel.asyncio._utils)."
    )
    return method


def _make_async_gen_wrapper(name):
    async def method(self, *args, conn=None, **kwargs):
        util_fn = getattr(autils, name)
        async for row in util_fn(self._effective_conn(conn), *args, **kwargs):
            yield row

    method.__name__ = name
    method.__qualname__ = f"AsyncGoldLapel.{name}"
    method.__doc__ = (
        f"Async generator wrapper for {name}. See goldlapel.utils.{name}. "
        f"Use `async for row in gl.{name}(...)` — not `await`."
    )
    return method


for _name in _WRAPPED_METHODS:
    if _name in _GENERATOR_METHODS:
        setattr(AsyncGoldLapel, _name, _make_async_gen_wrapper(_name))
    else:
        setattr(AsyncGoldLapel, _name, _make_async_wrapper(_name))


# -- Module-level factory -------------------------------------------------

def _register_cleanup():
    """Register an atexit handler (once) to stop leftover instances."""
    import atexit
    from goldlapel import proxy as proxy_mod
    if not proxy_mod._cleanup_registered:
        atexit.register(proxy_mod._cleanup)
        proxy_mod._cleanup_registered = True


async def _actual_start(upstream, config=None, port=None, extra_args=None):
    """Spawn (or reuse) a proxy instance + open the internal asyncpg conn.

    Mirrors goldlapel.proxy._ensure_running but for AsyncGoldLapel and with
    async connect inlined so we don't block the loop via threadpool bounces.
    """
    asyncpg = _detect_asyncpg()
    if asyncpg is None:
        raise ImportError(
            "Gold Lapel async wrapper needs asyncpg. "
            "Install with: pip install asyncpg"
        )

    global _next_port
    from goldlapel import proxy as proxy_mod
    with proxy_mod._lock:
        # If an instance already exists for this upstream and is running, reuse
        # its subprocess but still open a *new* asyncpg conn for this caller —
        # asyncpg connections are not thread/coro-shared freely.
        existing = proxy_mod._instances.get(upstream)
        if existing and existing.running:
            # Wrap the already-running subprocess in an AsyncGoldLapel.
            inst = AsyncGoldLapel.__new__(AsyncGoldLapel)
            inst._sync = existing
            inst._conn = None
            # Fall through to connect below — don't re-spawn.
            need_spawn = False
        else:
            # Fresh instance.
            if existing:
                del proxy_mod._instances[existing._upstream]
            if port is None:
                port = proxy_mod._next_port
            if port >= proxy_mod._next_port:
                proxy_mod._next_port = port + 1
            inst = AsyncGoldLapel(
                upstream, port=port, config=config, extra_args=extra_args,
            )
            proxy_mod._instances[upstream] = inst._sync
            need_spawn = True
        _register_cleanup()

    try:
        if need_spawn:
            await inst.start()
        else:
            # Reusing existing subprocess — just open the conn.
            raw = await _open_asyncpg_conn(inst._sync._proxy_url)
            from goldlapel.wrap import wrap
            inv_port = int(
                (inst._sync._config or {}).get(
                    "invalidation_port", inst._sync._port + 2,
                ),
            )
            inst._conn = wrap(raw, invalidation_port=inv_port)
        return inst
    except Exception:
        if need_spawn:
            with proxy_mod._lock:
                proxy_mod._instances.pop(upstream, None)
        raise


class _StartHandle:
    """Dual-interface object returned by `start()` — awaitable and async CM.

    Mirrors asyncpg.create_pool()'s pattern.
    """

    def __init__(self, upstream, config=None, port=None, extra_args=None):
        self._args = (upstream, config, port, extra_args)
        self._inst = None

    def __await__(self):
        return _actual_start(*self._args).__await__()

    async def __aenter__(self):
        self._inst = await _actual_start(*self._args)
        return self._inst

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._inst is not None:
            await self._inst.stop()
        return False


def start(upstream, config=None, port=None, extra_args=None):
    """Factory: spawn a Gold Lapel proxy and return an AsyncGoldLapel instance.

    Usable both as an awaitable and as an async context manager.

    Requires `asyncpg` installed — raises ImportError otherwise. The public
    API is identical to v0.2.0; only the underlying driver changed from
    psycopg-via-thread to native asyncpg.

    Usage:
        from goldlapel.asyncio import start

        # await form
        gl = await start("postgresql://user:pass@db/mydb")
        hits = await gl.search("articles", "body", "postgres")
        await gl.stop()

        # async context manager form
        async with start("postgresql://...") as gl:
            hits = await gl.search(...)
    """
    return _StartHandle(upstream, config=config, port=port, extra_args=extra_args)
