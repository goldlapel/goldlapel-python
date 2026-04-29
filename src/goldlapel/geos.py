"""Geo namespace API — `gl.geos.<verb>(...)`.

Phase 5 of schema-to-core. The proxy's v1 geo schema uses GEOGRAPHY (not
GEOMETRY), `member TEXT PRIMARY KEY` (not `BIGSERIAL` + `name`), and a
GIST index on the location column. `geo.add` is idempotent on the member
name — re-adding a member updates its location.

Distance unit: methods accept `unit='m' | 'km' | 'mi' | 'ft'`. The proxy
column is meters-native (GEOGRAPHY default); wrappers convert at the edge.
"""

from goldlapel import ddl as _ddl
from goldlapel.utils import _validate_identifier


class GeosAPI:
    """The geos sub-API — accessible as `gl.geos`."""

    def __init__(self, gl):
        self._gl = gl

    def _patterns(self, name):
        _validate_identifier(name)
        gl = self._gl
        token = gl._dashboard_token or _ddl.token_from_env_or_file()
        return _ddl.fetch_patterns(gl, "geo", name, gl._dashboard_port, token)

    def create(self, name):
        self._patterns(name)

    def add(self, name, member, lon, lat, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.geo_add(
            self._gl._effective_conn(conn), name, member, lon, lat,
            patterns=patterns,
        )

    def pos(self, name, member, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.geo_pos(
            self._gl._effective_conn(conn), name, member, patterns=patterns,
        )

    def dist(self, name, member_a, member_b, unit="m", *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.geo_dist(
            self._gl._effective_conn(conn), name, member_a, member_b, unit=unit,
            patterns=patterns,
        )

    def radius(self, name, lon, lat, radius, unit="m", limit=50, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.geo_radius(
            self._gl._effective_conn(conn), name, lon, lat, radius,
            unit=unit, limit=limit, patterns=patterns,
        )

    def radius_by_member(self, name, member, radius, unit="m", limit=50, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.geo_radius_by_member(
            self._gl._effective_conn(conn), name, member, radius,
            unit=unit, limit=limit, patterns=patterns,
        )

    def remove(self, name, member, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.geo_remove(
            self._gl._effective_conn(conn), name, member, patterns=patterns,
        )

    def count(self, name, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.geo_count(
            self._gl._effective_conn(conn), name, patterns=patterns,
        )
