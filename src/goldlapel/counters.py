"""Counters namespace API — `gl.counters.<verb>(...)`.

Phase 5 of schema-to-core: the proxy owns counter DDL. Each call here:

  1. Calls `/api/ddl/counter/create` (idempotent) to materialize the canonical
     `_goldlapel.counter_<name>` table and pull its query patterns.
  2. Caches `(tables, query_patterns)` on the parent GoldLapel instance for
     the session's lifetime (one HTTP round-trip per (family, name)).
  3. Hands the patterns off to `goldlapel.utils.counter_*` helpers, which
     execute against the canonical table name.

Mirrors `goldlapel.documents.DocumentsAPI` exactly — the canonical
schema-to-core sub-API shape.
"""

from goldlapel import ddl as _ddl
from goldlapel.utils import _validate_identifier


class CountersAPI:
    """The counters sub-API — accessible as `gl.counters`.

    Each method takes the counter namespace name as the first positional
    argument; the per-key value-mutation surface follows. State (dashboard
    token, dashboard port, internal connection, DDL pattern cache) is
    shared via the parent GoldLapel reference held in `self._gl`.
    """

    def __init__(self, gl):
        self._gl = gl

    def _patterns(self, name):
        _validate_identifier(name)
        gl = self._gl
        token = gl._dashboard_token or _ddl.token_from_env_or_file()
        return _ddl.fetch_patterns(gl, "counter", name, gl._dashboard_port, token)

    # -- Lifecycle -----------------------------------------------------------

    def create(self, name):
        """Eagerly materialize the counter table. Other methods will also
        materialize on first use, so calling this is optional — provided for
        callers that want explicit setup at startup time."""
        self._patterns(name)

    # -- Per-key ops ---------------------------------------------------------

    def incr(self, name, key, amount=1, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.counter_incr(
            self._gl._effective_conn(conn), name, key, amount, patterns=patterns,
        )

    def decr(self, name, key, amount=1, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.counter_decr(
            self._gl._effective_conn(conn), name, key, amount, patterns=patterns,
        )

    def set(self, name, key, value, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.counter_set(
            self._gl._effective_conn(conn), name, key, value, patterns=patterns,
        )

    def get(self, name, key, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.counter_get(
            self._gl._effective_conn(conn), name, key, patterns=patterns,
        )

    def delete(self, name, key, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.counter_delete(
            self._gl._effective_conn(conn), name, key, patterns=patterns,
        )

    def count_keys(self, name, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.counter_count_keys(
            self._gl._effective_conn(conn), name, patterns=patterns,
        )
