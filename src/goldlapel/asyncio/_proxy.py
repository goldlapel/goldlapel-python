"""AsyncGoldLapel — async façade over goldlapel.proxy.GoldLapel.

All 54 wrapper methods, plus using() and context manager, exposed as async.
Current impl bridges to the sync methods via asyncio.to_thread; the shape
and API are stable even when we swap to native asyncpg in a future release.
"""

import asyncio
from contextlib import asynccontextmanager

from goldlapel.proxy import GoldLapel, _detect_sync_driver, _ensure_running


# All 54 wrapper-method names that exist on the sync GoldLapel class.
# Mirrored here so the async façade exposes the same surface as async def.
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


class AsyncGoldLapel:
    """Async façade over GoldLapel. Wraps each sync wrapper method via
    asyncio.to_thread so users can `await gl.search(...)` naturally.
    """

    def __init__(self, upstream, config=None, port=None, extra_args=None):
        self._sync = GoldLapel(upstream, config=config, port=port, extra_args=extra_args)

    # -- Lifecycle -----------------------------------------------------------

    async def start(self):
        return await asyncio.to_thread(self._sync.start)

    async def stop(self):
        return await asyncio.to_thread(self._sync.stop)

    # -- Properties (sync access, no await needed) ---------------------------

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
        return self._sync.conn

    # -- Async context manager -----------------------------------------------

    async def __aenter__(self):
        if not self.running:
            await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()
        return False

    # -- Scoped using() — async context manager ------------------------------

    @asynccontextmanager
    async def using(self, conn):
        """Scoped override: all wrapper methods called inside this `async with`
        block will use `conn` (typically a caller-provided connection inside
        their own transaction) instead of the instance's internal connection.
        """
        token = self._sync._using_conn.set(conn)
        try:
            yield self
        finally:
            self._sync._using_conn.reset(token)


# -- Generate async versions of all 54 wrapper methods ------------------------

def _make_async_wrapper(name):
    sync_method_name = name

    async def method(self, *args, conn=None, **kwargs):
        sync_method = getattr(self._sync, sync_method_name)
        return await asyncio.to_thread(sync_method, *args, conn=conn, **kwargs)

    method.__name__ = name
    method.__qualname__ = f"AsyncGoldLapel.{name}"
    method.__doc__ = f"Async wrapper for {name}. See goldlapel.GoldLapel.{name} for signature."
    return method


for _name in _WRAPPED_METHODS:
    setattr(AsyncGoldLapel, _name, _make_async_wrapper(_name))


# -- Module-level factory -----------------------------------------------------

async def start(upstream, config=None, port=None, extra_args=None):
    """Factory: spawn a Gold Lapel proxy in front of `upstream` and return an
    AsyncGoldLapel instance. All wrapper methods on the returned instance are
    awaitable.

    Eager: starts the subprocess and opens the internal DB connection before
    returning. Requires a sync Postgres driver (psycopg2 or psycopg3) —
    raises ImportError otherwise. (Native asyncpg will become the default
    driver in a future release; API is stable.)

    Usage:
        from goldlapel.asyncio import start
        gl = await start("postgresql://user:pass@db/mydb")
        hits = await gl.search("articles", "body", "postgres")
        await gl.stop()

    Async context manager:
        async with start("postgresql://...") as gl:
            hits = await gl.search(...)
    """
    driver_name, driver = _detect_sync_driver()
    if driver is None:
        raise ImportError(
            "Gold Lapel async wrapper methods need a Postgres driver. "
            "Install one: `pip install psycopg2-binary` or `pip install psycopg`. "
            "(Native asyncpg support will land in a future release.)"
        )
    # Reuse the sync _ensure_running — it manages the subprocess + singleton-per-URL
    sync_inst = await asyncio.to_thread(
        _ensure_running, upstream, config=config, port=port, extra_args=extra_args,
    )
    async_inst = AsyncGoldLapel.__new__(AsyncGoldLapel)
    async_inst._sync = sync_inst
    return async_inst
