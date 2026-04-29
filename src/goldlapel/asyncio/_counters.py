"""Async counters namespace API — `gl.counters.<verb>(...)` on AsyncGoldLapel.

Mirrors goldlapel.counters.CountersAPI but with async methods. State
(dashboard token, dashboard port, internal asyncpg connection, DDL pattern
cache) is shared via the parent AsyncGoldLapel reference held in `self._gl`.
"""

import asyncio as _asyncio

from goldlapel import ddl as _ddl
from goldlapel.asyncio import _utils as autils


class AsyncCountersAPI:
    def __init__(self, gl):
        self._gl = gl

    async def _patterns(self, name):
        autils._validate_identifier(name)
        gl = self._gl
        token = gl._sync._dashboard_token or _ddl.token_from_env_or_file()
        port = gl._sync._dashboard_port
        loop = _asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: _ddl.fetch_patterns(gl, "counter", name, port, token),
        )

    async def create(self, name):
        await self._patterns(name)

    async def incr(self, name, key, amount=1, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.counter_incr(
            self._gl._effective_conn(conn), name, key, amount, patterns=patterns,
        )

    async def decr(self, name, key, amount=1, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.counter_decr(
            self._gl._effective_conn(conn), name, key, amount, patterns=patterns,
        )

    async def set(self, name, key, value, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.counter_set(
            self._gl._effective_conn(conn), name, key, value, patterns=patterns,
        )

    async def get(self, name, key, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.counter_get(
            self._gl._effective_conn(conn), name, key, patterns=patterns,
        )

    async def delete(self, name, key, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.counter_delete(
            self._gl._effective_conn(conn), name, key, patterns=patterns,
        )

    async def count_keys(self, name, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.counter_count_keys(
            self._gl._effective_conn(conn), name, patterns=patterns,
        )
