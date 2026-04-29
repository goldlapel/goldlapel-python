"""Async sorted-set namespace API — `gl.zsets.<verb>(...)` on AsyncGoldLapel.

Mirrors goldlapel.zsets.ZsetsAPI; threads `zset_key` as the first arg
after the namespace `name` so a single namespace table holds many
sorted sets.
"""

import asyncio as _asyncio

from goldlapel import ddl as _ddl
from goldlapel.asyncio import _utils as autils


class AsyncZsetsAPI:
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
            lambda: _ddl.fetch_patterns(gl, "zset", name, port, token),
        )

    async def create(self, name):
        await self._patterns(name)

    async def add(self, name, zset_key, member, score, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.zset_add(
            self._gl._effective_conn(conn), name, zset_key, member, score,
            patterns=patterns,
        )

    async def incr_by(self, name, zset_key, member, delta=1, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.zset_incr_by(
            self._gl._effective_conn(conn), name, zset_key, member, delta,
            patterns=patterns,
        )

    async def score(self, name, zset_key, member, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.zset_score(
            self._gl._effective_conn(conn), name, zset_key, member,
            patterns=patterns,
        )

    async def rank(self, name, zset_key, member, desc=True, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.zset_rank(
            self._gl._effective_conn(conn), name, zset_key, member, desc=desc,
            patterns=patterns,
        )

    async def range(self, name, zset_key, start=0, stop=-1, desc=True, *, conn=None):
        if stop is None or stop == -1:
            stop = 9999
        patterns = await self._patterns(name)
        return await autils.zset_range(
            self._gl._effective_conn(conn), name, zset_key, start, stop, desc,
            patterns=patterns,
        )

    async def range_by_score(self, name, zset_key, min_score, max_score, limit=100, offset=0, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.zset_range_by_score(
            self._gl._effective_conn(conn), name, zset_key,
            min_score, max_score, limit, offset, patterns=patterns,
        )

    async def remove(self, name, zset_key, member, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.zset_remove(
            self._gl._effective_conn(conn), name, zset_key, member,
            patterns=patterns,
        )

    async def card(self, name, zset_key, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.zset_card(
            self._gl._effective_conn(conn), name, zset_key, patterns=patterns,
        )
