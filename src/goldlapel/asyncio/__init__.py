"""Async flavor of the Gold Lapel Python wrapper.

Usage:
    from goldlapel.asyncio import start

    gl = await start("postgresql://user:pass@db/mydb")
    hits = await gl.search("articles", "body", "postgres tuning")
    await gl.stop()

    # Or as an async context manager
    async with start("postgresql://...") as gl:
        results = await gl.search(...)

API parity with the sync wrapper (`goldlapel.start`) — all 54 wrapper methods
exist here as `async def`, plus `gl.using(conn)` scoped override (async
context manager) and per-call `conn=` kwarg.

Native asyncpg under the hood: wrapper methods issue `await conn.fetch(...)`
/ `await conn.execute(...)` directly with no thread-pool bounce. Requires
asyncpg (pip install asyncpg). Public API identical to v0.2.0.
"""

from goldlapel.asyncio._proxy import start, AsyncGoldLapel

__all__ = ["start", "AsyncGoldLapel"]
