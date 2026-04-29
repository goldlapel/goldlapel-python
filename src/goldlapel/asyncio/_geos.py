"""Async geo namespace API — `gl.geos.<verb>(...)` on AsyncGoldLapel.

Mirrors goldlapel.geos.GeosAPI. PostGIS GEOGRAPHY-native; member name is
the primary key (idempotent re-add).
"""

import asyncio as _asyncio

from goldlapel import ddl as _ddl
from goldlapel.asyncio import _utils as autils


class AsyncGeosAPI:
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
            lambda: _ddl.fetch_patterns(gl, "geo", name, port, token),
        )

    async def create(self, name):
        await self._patterns(name)

    async def add(self, name, member, lon, lat, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.geo_add(
            self._gl._effective_conn(conn), name, member, lon, lat,
            patterns=patterns,
        )

    async def pos(self, name, member, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.geo_pos(
            self._gl._effective_conn(conn), name, member, patterns=patterns,
        )

    async def dist(self, name, member_a, member_b, unit="m", *, conn=None):
        patterns = await self._patterns(name)
        return await autils.geo_dist(
            self._gl._effective_conn(conn), name, member_a, member_b, unit=unit,
            patterns=patterns,
        )

    async def radius(self, name, lon, lat, radius, unit="m", limit=50, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.geo_radius(
            self._gl._effective_conn(conn), name, lon, lat, radius,
            unit=unit, limit=limit, patterns=patterns,
        )

    async def radius_by_member(self, name, member, radius, unit="m", limit=50, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.geo_radius_by_member(
            self._gl._effective_conn(conn), name, member, radius,
            unit=unit, limit=limit, patterns=patterns,
        )

    async def remove(self, name, member, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.geo_remove(
            self._gl._effective_conn(conn), name, member, patterns=patterns,
        )

    async def count(self, name, *, conn=None):
        patterns = await self._patterns(name)
        return await autils.geo_count(
            self._gl._effective_conn(conn), name, patterns=patterns,
        )
