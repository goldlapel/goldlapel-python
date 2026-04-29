"""Async streams namespace API — `gl.streams.<verb>(...)` on AsyncGoldLapel.

Mirrors goldlapel.streams.StreamsAPI but with async methods. State (dashboard
token, dashboard port, internal asyncpg connection, DDL pattern cache) is
shared via the parent AsyncGoldLapel reference held in `self._gl`.
"""

import asyncio as _asyncio

from goldlapel import ddl as _ddl
from goldlapel.asyncio import _utils as autils


class AsyncStreamsAPI:
    """Async streams sub-API — accessible as `gl.streams` on AsyncGoldLapel."""

    def __init__(self, gl):
        self._gl = gl

    async def _patterns(self, stream):
        autils._validate_identifier(stream)
        gl = self._gl
        token = gl._sync._dashboard_token or _ddl.token_from_env_or_file()
        port = gl._sync._dashboard_port
        # Cache owner is the parent AsyncGoldLapel — describe-once-per-session.
        # urllib is blocking; bounce to a thread executor so we don't block the
        # event loop. One round-trip per (family, name) per session — not hot.
        loop = _asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, _ddl.fetch_patterns, gl, "stream", stream, port, token,
        )

    async def add(self, stream, payload, *, conn=None):
        patterns = await self._patterns(stream)
        return await autils.stream_add(
            self._gl._effective_conn(conn), stream, payload, patterns=patterns,
        )

    async def create_group(self, stream, group, *, conn=None):
        patterns = await self._patterns(stream)
        return await autils.stream_create_group(
            self._gl._effective_conn(conn), stream, group, patterns=patterns,
        )

    async def read(self, stream, group, consumer, count=1, *, conn=None):
        patterns = await self._patterns(stream)
        return await autils.stream_read(
            self._gl._effective_conn(conn), stream, group, consumer, count,
            patterns=patterns,
        )

    async def ack(self, stream, group, message_id, *, conn=None):
        patterns = await self._patterns(stream)
        return await autils.stream_ack(
            self._gl._effective_conn(conn), stream, group, message_id,
            patterns=patterns,
        )

    async def claim(self, stream, group, consumer, min_idle_ms=60000, *, conn=None):
        patterns = await self._patterns(stream)
        return await autils.stream_claim(
            self._gl._effective_conn(conn), stream, group, consumer, min_idle_ms,
            patterns=patterns,
        )
