"""Sorted-set (zset) namespace API — `gl.zsets.<verb>(...)`.

Phase 5 of schema-to-core. The proxy's v1 zset schema introduces a
`zset_key` column so a single namespace table holds many sorted sets —
matching Redis's mental model. Every method below threads `zset_key` as
the first positional arg after the namespace `name`.
"""

from goldlapel import ddl as _ddl
from goldlapel.utils import _validate_identifier


class ZsetsAPI:
    """The zsets sub-API — accessible as `gl.zsets`.

    Method shape: `gl.zsets.<verb>(name, zset_key, ...)`. `name` is the
    namespace (one Postgres table); `zset_key` partitions multiple sorted
    sets within that namespace.
    """

    def __init__(self, gl):
        self._gl = gl

    def _patterns(self, name):
        _validate_identifier(name)
        gl = self._gl
        token = gl._dashboard_token or _ddl.token_from_env_or_file()
        return _ddl.fetch_patterns(gl, "zset", name, gl._dashboard_port, token)

    def create(self, name):
        self._patterns(name)

    def add(self, name, zset_key, member, score, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.zset_add(
            self._gl._effective_conn(conn), name, zset_key, member, score,
            patterns=patterns,
        )

    def incr_by(self, name, zset_key, member, delta=1, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.zset_incr_by(
            self._gl._effective_conn(conn), name, zset_key, member, delta,
            patterns=patterns,
        )

    def score(self, name, zset_key, member, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.zset_score(
            self._gl._effective_conn(conn), name, zset_key, member,
            patterns=patterns,
        )

    def rank(self, name, zset_key, member, desc=True, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.zset_rank(
            self._gl._effective_conn(conn), name, zset_key, member, desc=desc,
            patterns=patterns,
        )

    def range(self, name, zset_key, start=0, stop=-1, desc=True, *, conn=None):
        """Members by rank within `zset_key`. Inclusive `start`/`stop`
        Redis-style; `stop=-1` is a sentinel meaning "to the end" — we map
        it to a large limit (10000) since the proxy's pattern is
        LIMIT/OFFSET-based. Callers wanting the entire set should page
        explicitly via `range_by_score`.
        """
        if stop is None or stop == -1:
            stop = 9999
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.zset_range(
            self._gl._effective_conn(conn), name, zset_key, start, stop, desc,
            patterns=patterns,
        )

    def range_by_score(self, name, zset_key, min_score, max_score, limit=100, offset=0, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.zset_range_by_score(
            self._gl._effective_conn(conn), name, zset_key,
            min_score, max_score, limit, offset, patterns=patterns,
        )

    def remove(self, name, zset_key, member, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.zset_remove(
            self._gl._effective_conn(conn), name, zset_key, member,
            patterns=patterns,
        )

    def card(self, name, zset_key, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.zset_card(
            self._gl._effective_conn(conn), name, zset_key, patterns=patterns,
        )
