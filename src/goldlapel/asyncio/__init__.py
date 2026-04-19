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

Implementation note (v0.2.0): under the hood, wrapper methods bridge to the
sync implementation via `asyncio.to_thread()`. This preserves the full method
surface without a second implementation. Future releases will swap the bridge
for native asyncpg calls transparently — the public API is stable.
"""

from goldlapel.asyncio._proxy import start, AsyncGoldLapel

__all__ = ["start", "AsyncGoldLapel"]
