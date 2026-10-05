"""AsyncGoldLapel — native-asyncpg async façade over the Gold Lapel proxy.

Spawns the proxy subprocess (via the sync helpers in goldlapel.proxy), opens
a plain asyncpg.Connection to it, and exposes the same wrapper-method
surface as sync GoldLapel, implemented as native `async def` that calls
into goldlapel.asyncio._utils.

The wrapper-method surface is auto-derived at import time by walking the
public methods on GoldLapel — see _derive_async_methods at the bottom of
this module. Hand-written async-native methods (start, stop, using,
stream_*) win over auto-derive; everything else falls through to a
generated wrapper that dispatches to goldlapel.asyncio._utils.<name>.

Public API (unchanged from v0.2.0):
  - goldlapel.asyncio.start(url) → AsyncGoldLapel (awaitable or async CM)
  - every wrapper method identical signature
  - gl.using(conn) scoped override — ContextVar semantics
  - conn= per-call kwarg with precedence: explicit > using > internal

When `asyncpg` is not importable, `start()` raises ImportError with the
install hint. The sync fallback (psycopg3 async) is not implemented here —
asyncpg is the canonical async driver and is declared a dev dependency.
"""

import asyncio
import inspect
from contextlib import asynccontextmanager
from functools import wraps

from goldlapel.proxy import GoldLapel, _lock, _reject_unknown_options
from goldlapel.asyncio import _utils as autils


# Sync-class methods that intentionally do NOT belong on AsyncGoldLapel as
# auto-derived wrappers. Empty today — every public sync method has an async
# equivalent (either auto-derived from goldlapel.asyncio._utils, or an
# async-native method defined directly on AsyncGoldLapel below, e.g. `start`,
# `stop`, `using`, `stream_*`).
#
# Note: methods already defined on AsyncGoldLapel win over auto-derive (the
# loop at the bottom of this module checks `name in target_cls.__dict__`).
# That's how lifecycle and stream methods stay async-native without needing
# entries here. This skip list exists for the case where a sync method should
# NOT appear on the async surface at all — add an entry with a comment
# explaining why if that ever happens.
_ASYNC_SKIPPED = frozenset({
    # (none)
})


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
    goldlapel.proxy) and opens an asyncpg connection to it.
    """

    def __init__(
        self,
        upstream,
        *,
        proxy_port=None,
        dashboard_port=None,
        log_level=None,
        mode=None,
        license=None,
        api_key=None,
        client=None,
        config_file=None,
        config=None,
        extra_args=None,
        silent=False,
        mesh=False,
        mesh_tag=None,
        disable_proxy_cache=False,
        disable_sqloptimize=False,
        disable_auto_indexes=False,
        **unknown,
    ):
        _reject_unknown_options(unknown)
        # Piggyback on the sync GoldLapel for subprocess/lifecycle state so
        # `using(conn)` / ContextVar semantics and stop-on-exit are identical.
        self._init_state(GoldLapel(
            upstream,
            proxy_port=proxy_port,
            dashboard_port=dashboard_port,
            log_level=log_level,
            mode=mode,
            license=license,
            api_key=api_key,
            client=client,
            config_file=config_file,
            config=config,
            extra_args=extra_args,
            silent=silent,
            mesh=mesh,
            mesh_tag=mesh_tag,
            disable_proxy_cache=disable_proxy_cache,
            disable_sqloptimize=disable_sqloptimize,
            disable_auto_indexes=disable_auto_indexes,
        ))

    @classmethod
    def _wrapping(cls, sync):
        """An AsyncGoldLapel over `sync`, a proxy that is already running —
        the reuse path of `start()`. Same state as a fresh instance, so every
        helper works either way."""
        inst = cls.__new__(cls)
        inst._init_state(sync)
        return inst

    def _init_state(self, sync):
        self._sync = sync
        self._conn = None  # asyncpg.Connection
        # True while this handle holds the proxy (see GoldLapel.stop).
        self._held = False

        # Nested namespaces — mirror the sync GoldLapel but with async sub-API
        # classes. State is shared via the parent reference held in each
        # sub-API's `self._gl`. The sync GoldLapel also constructed a
        # DocumentsAPI / StreamsAPI / etc. bound to itself, but we never call
        # those — users access the async surface via `gl.<family>` below,
        # where `gl` is the AsyncGoldLapel. This avoids accidentally calling
        # sync code through an async client.
        from goldlapel.asyncio._documents import AsyncDocumentsAPI
        from goldlapel.asyncio._streams import AsyncStreamsAPI
        from goldlapel.asyncio._counters import AsyncCountersAPI
        from goldlapel.asyncio._zsets import AsyncZsetsAPI
        from goldlapel.asyncio._hashes import AsyncHashesAPI
        from goldlapel.asyncio._queues import AsyncQueuesAPI
        from goldlapel.asyncio._geos import AsyncGeosAPI
        self.documents = AsyncDocumentsAPI(self)
        self.streams = AsyncStreamsAPI(self)
        self.counters = AsyncCountersAPI(self)
        self.zsets = AsyncZsetsAPI(self)
        self.hashes = AsyncHashesAPI(self)
        self.queues = AsyncQueuesAPI(self)
        self.geos = AsyncGeosAPI(self)

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

        If opening the connection fails (or is cancelled) after the
        subprocess is up, this handle's hold is given up before re-raising,
        which stops the proxy unless another caller shares it.
        """
        if self._sync.running and self._conn is not None:
            return self._sync.url
        if _detect_asyncpg() is None:
            raise ImportError(
                "Gold Lapel async wrapper needs asyncpg. "
                "Install with: pip install asyncpg"
            )
        spawned = not self._sync.running
        if spawned:
            self._sync._spawn()
        elif not self._held:
            with _lock:
                self._sync._holders += 1
        self._held = True
        await self._connect()
        if spawned:
            self._sync._print_banner()
        return self._sync._proxy_url

    async def _connect(self):
        """Open this handle's asyncpg conn; on failure (cancellation
        included) give up this handle's hold on the proxy."""
        try:
            self._conn = await _open_asyncpg_conn(self._sync._proxy_url)
        except BaseException:
            self._release()
            raise

    def _release(self):
        if self._held:
            self._held = False
            self._sync.stop()

    async def stop(self):
        """Close this handle's connection and give up its hold on the proxy:
        the proxy stops when no other caller (sync or async) holds it."""
        # Drop any cached DDL patterns tied to this instance (see sync stop).
        try:
            from goldlapel import ddl as _ddl
            _ddl.invalidate(self)
        except Exception:
            pass
        if self._conn is not None:
            try:
                await self._conn.close()
            except Exception:
                pass
            self._conn = None
        self._release()

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

    # -- Streams: gl.streams.<verb>(...). See goldlapel/asyncio/_streams.py.
    # -- Documents: gl.documents.<verb>(...). See goldlapel/asyncio/_documents.py.


# -- Auto-derive async wrappers from the sync GoldLapel surface ----------
#
# Pattern modeled on Motor (PyMongo's async driver) and aioboto3: introspect
# the sync class at import time, attach an async wrapper to AsyncGoldLapel
# for every public sync method that doesn't already have a hand-written
# async-native version. Adding a new method to GoldLapel automatically
# exposes it on AsyncGoldLapel — no second list to maintain, no drift.
#
# A parity test (tests/test_async_parity.py) asserts that every public sync
# method has an async counterpart (modulo _ASYNC_SKIPPED), so a future change
# that breaks this invariant fails CI loudly.


def _make_async_wrapper(name, sync_method):
    """Build an async wrapper that dispatches to goldlapel.asyncio._utils.<name>.

    The util is looked up lazily on the module so test-time patches like
    `patch("goldlapel.asyncio._utils.search", ...)` replace it for us.
    """
    @wraps(sync_method)
    async def method(self, *args, conn=None, **kwargs):
        util_fn = getattr(autils, name)
        return await util_fn(self._effective_conn(conn), *args, **kwargs)

    # @wraps copies __doc__ from the sync method (typically None for these
    # thin dispatch methods); fall back to a hand-written line so help() and
    # IDEs show something useful.
    if not method.__doc__:
        method.__doc__ = (
            f"Async wrapper for {name}. See goldlapel.utils.{name} for signature "
            f"(native asyncpg impl in goldlapel.asyncio._utils)."
        )
    method.__qualname__ = f"AsyncGoldLapel.{name}"
    return method


def _make_async_gen_wrapper(name, sync_method):
    @wraps(sync_method)
    async def method(self, *args, conn=None, **kwargs):
        util_fn = getattr(autils, name)
        async for row in util_fn(self._effective_conn(conn), *args, **kwargs):
            yield row

    if not method.__doc__:
        method.__doc__ = (
            f"Async generator wrapper for {name}. See goldlapel.utils.{name}. "
            f"Use `async for row in gl.{name}(...)` — not `await`."
        )
    method.__qualname__ = f"AsyncGoldLapel.{name}"
    return method


def _is_async_generator_util(name):
    """True if goldlapel.asyncio._utils.<name> is `async def ... yield`.

    Used to pick async-generator wrapper vs coroutine wrapper at import
    time. Falls back to False (coroutine wrapper) when the util doesn't
    exist — that case will fail loudly at first invocation, which is more
    useful than silently picking the wrong wrapper.
    """
    fn = getattr(autils, name, None)
    return fn is not None and inspect.isasyncgenfunction(fn)


def _derive_async_methods(target_cls, sync_cls):
    """Walk sync_cls's public methods; attach an async wrapper to target_cls
    for each one. Hand-written methods on target_cls win — they're skipped.
    Methods listed in _ASYNC_SKIPPED are also skipped."""
    for name, sync_method in inspect.getmembers(sync_cls, predicate=inspect.isfunction):
        if name.startswith("_"):
            continue
        if name in _ASYNC_SKIPPED:
            continue
        if name in target_cls.__dict__:
            # Already hand-written on the async class (e.g. start, stop,
            # using, stream_*). Don't overwrite. Use __dict__ rather than
            # hasattr() so we only check methods defined on this class
            # itself, not anything inherited from object.
            continue
        if _is_async_generator_util(name):
            wrapper = _make_async_gen_wrapper(name, sync_method)
        else:
            wrapper = _make_async_wrapper(name, sync_method)
        setattr(target_cls, name, wrapper)


_derive_async_methods(AsyncGoldLapel, GoldLapel)


# -- Module-level factory -------------------------------------------------

async def _actual_start(upstream, **kwargs):
    """Spawn (or share) the proxy for `upstream` and open this caller's
    asyncpg conn — the async twin of goldlapel.proxy._ensure_running, on
    the same registry, port claims and holder count."""
    asyncpg = _detect_asyncpg()
    if asyncpg is None:
        raise ImportError(
            "Gold Lapel async wrapper needs asyncpg. "
            "Install with: pip install asyncpg"
        )

    from goldlapel import proxy as proxy_mod
    while True:
        with proxy_mod._lock:
            sync = proxy_mod._instances.get(upstream)
            if sync is not None and sync._ready.is_set():
                if sync.running:
                    sync._holders += 1
                    spawn = False
                    break
                del proxy_mod._instances[upstream]
                sync = None
            if sync is None:
                # Option errors raise here, before anything is registered.
                sync = GoldLapel(upstream, **kwargs)
                proxy_mod._instances[upstream] = sync
                spawn = True
                break
        # Another caller is starting this upstream: wait without blocking
        # the event loop.
        await asyncio.to_thread(sync._ready.wait)

    if spawn:
        # Synchronous — nothing else runs on this loop until the proxy is
        # up or the start has failed and been cleaned up.
        try:
            sync._spawn()
        except BaseException:
            with proxy_mod._lock:
                if proxy_mod._instances.get(upstream) is sync:
                    del proxy_mod._instances[upstream]
            raise
        finally:
            sync._ready.set()

    inst = AsyncGoldLapel._wrapping(sync)
    inst._held = True
    await inst._connect()
    if spawn:
        sync._print_banner()
    return inst


class _StartHandle:
    """Dual-interface object returned by `start()` — awaitable and async CM.

      - Awaitable: `gl = await start(url)`
      - Async context manager: `async with start(url) as gl: ...`

    Mirrors the pattern used by asyncpg.create_pool().

    A handle is single-use: `await`ing it OR entering it as a context manager
    consumes it. A second use would spawn a second subprocess while orphaning
    the first — Option B from the v0.2 review findings raises loudly instead.
    """

    _CONSUMED_MSG = (
        "Gold Lapel start handle already consumed — "
        "call goldlapel.asyncio.start(...) again for a new handle"
    )

    def __init__(self, upstream, **kwargs):
        self._upstream = upstream
        self._kwargs = kwargs
        self._inst = None
        self._consumed = False

    def __await__(self):
        # Enable `gl = await start(url)` — just run the underlying coroutine.
        if self._consumed:
            raise RuntimeError(self._CONSUMED_MSG)
        self._consumed = True
        return _actual_start(self._upstream, **self._kwargs).__await__()

    async def __aenter__(self):
        # Enable `async with start(url) as gl:`.
        if self._consumed:
            raise RuntimeError(self._CONSUMED_MSG)
        self._consumed = True
        self._inst = await _actual_start(self._upstream, **self._kwargs)
        return self._inst

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._inst is not None:
            await self._inst.stop()
        return False


def start(
    upstream,
    *,
    proxy_port=None,
    dashboard_port=None,
    log_level=None,
    mode=None,
    license=None,
    api_key=None,
    client=None,
    config_file=None,
    config=None,
    extra_args=None,
    silent=False,
    mesh=False,
    mesh_tag=None,
    disable_proxy_cache=False,
    disable_sqloptimize=False,
    disable_auto_indexes=False,
    **unknown,
):
    """Factory: spawn a Gold Lapel proxy and return an AsyncGoldLapel instance.

    Usable both as an awaitable and as an async context manager.

    Requires `asyncpg` installed — raises ImportError otherwise. Canonical
    top-level kwargs match the sync `goldlapel.start` factory — see its
    docstring for the full list.

    Usage:
        from goldlapel.asyncio import start

        # await form
        gl = await start("postgresql://user:pass@db/mydb")
        hits = await gl.search("articles", "body", "postgres")
        await gl.stop()

        # async context manager form
        async with start("postgresql://...") as gl:
            hits = await gl.search(...)

    A proxy already running for `upstream` in this process is shared: each
    handle gets its own connection, and stopping one stops the proxy only
    when no other handle (sync or async) still holds it.
    """
    _reject_unknown_options(unknown)
    return _StartHandle(
        upstream,
        proxy_port=proxy_port,
        dashboard_port=dashboard_port,
        log_level=log_level,
        mode=mode,
        license=license,
        api_key=api_key,
        client=client,
        config_file=config_file,
        config=config,
        extra_args=extra_args,
        silent=silent,
        mesh=mesh,
        mesh_tag=mesh_tag,
        disable_proxy_cache=disable_proxy_cache,
        disable_sqloptimize=disable_sqloptimize,
        disable_auto_indexes=disable_auto_indexes,
    )
