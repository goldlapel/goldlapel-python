"""Async hash namespace API — `gl.hashes.<verb>(...)` on AsyncGoldLapel.

Mirrors goldlapel.hashes.HashesAPI. Storage is row-per-field
(`hash_key`, `field`, `value`) — the v1 schema flip.
"""

import asyncio as _asyncio

from goldlapel import ddl as _ddl
from goldlapel.asyncio import _utils as autils


class AsyncHashesAPI:
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
            lambda: _ddl.fetch_patterns(gl, "hash", name, port, token),
        )

    async def create(self, name):
        await self._patterns(name)

    async def set(self, name, hash_key, field, value, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.hash_set(
            self._gl._effective_conn(conn), name, hash_key, field, value,
            patterns=patterns,
        )

    async def get(self, name, hash_key, field, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.hash_get(
            self._gl._effective_conn(conn), name, hash_key, field,
            patterns=patterns,
        )

    async def get_all(self, name, hash_key, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.hash_get_all(
            self._gl._effective_conn(conn), name, hash_key, patterns=patterns,
        )

    async def keys(self, name, hash_key, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.hash_keys(
            self._gl._effective_conn(conn), name, hash_key, patterns=patterns,
        )

    async def values(self, name, hash_key, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.hash_values(
            self._gl._effective_conn(conn), name, hash_key, patterns=patterns,
        )

    async def exists(self, name, hash_key, field, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.hash_exists(
            self._gl._effective_conn(conn), name, hash_key, field,
            patterns=patterns,
        )

    async def delete(self, name, hash_key, field, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.hash_delete(
            self._gl._effective_conn(conn), name, hash_key, field,
            patterns=patterns,
        )

    async def len(self, name, hash_key, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.hash_len(
            self._gl._effective_conn(conn), name, hash_key, patterns=patterns,
        )
