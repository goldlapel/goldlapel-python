"""Async queue namespace API — `gl.queues.<verb>(...)` on AsyncGoldLapel.

Mirrors goldlapel.queues.QueuesAPI. The v1 schema is at-least-once with
visibility-timeout — `claim` / `ack` / `abandon` rather than the legacy
fire-and-forget `dequeue`.
"""

import asyncio as _asyncio

from goldlapel import ddl as _ddl
from goldlapel.asyncio import _utils as autils


class AsyncQueuesAPI:
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
            lambda: _ddl.fetch_patterns(gl, "queue", name, port, token),
        )

    async def create(self, name):
        await self._patterns(name)

    async def enqueue(self, name, payload, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.queue_enqueue(
            self._gl._effective_conn(conn), name, payload, patterns=patterns,
        )

    async def claim(self, name, visibility_timeout_ms=30000, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.queue_claim(
            self._gl._effective_conn(conn), name,
            visibility_timeout_ms=visibility_timeout_ms,
            patterns=patterns,
        )

    async def ack(self, name, message_id, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.queue_ack(
            self._gl._effective_conn(conn), name, message_id, patterns=patterns,
        )

    async def abandon(self, name, message_id, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.queue_abandon(
            self._gl._effective_conn(conn), name, message_id, patterns=patterns,
        )

    async def extend(self, name, message_id, additional_ms, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.queue_extend(
            self._gl._effective_conn(conn), name, message_id, additional_ms,
            patterns=patterns,
        )

    async def peek(self, name, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.queue_peek(
            self._gl._effective_conn(conn), name, patterns=patterns,
        )

    async def count_ready(self, name, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.queue_count_ready(
            self._gl._effective_conn(conn), name, patterns=patterns,
        )

    async def count_claimed(self, name, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.queue_count_claimed(
            self._gl._effective_conn(conn), name, patterns=patterns,
        )
