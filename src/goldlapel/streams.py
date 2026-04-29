"""Streams namespace API — `gl.streams.<verb>(...)`.

Wraps the wire-level stream methods in a sub-API instance held on the parent
GoldLapel client. The instance shares all state (license, dashboard token, http
session, conn) by reference back to the parent — no duplication.

This is the canonical sub-API shape for the schema-to-core wrapper rollout.
Other namespaces (cache, search, queues, counters, hashes, zsets, geo, auth,
…) stay flat for now; they migrate to nested form one-at-a-time as their own
schema-to-core phase fires.
"""

from goldlapel import ddl as _ddl
from goldlapel.utils import _validate_identifier


class StreamsAPI:
    """The streams sub-API — accessible as `gl.streams`.

    All methods take the stream name as the first positional argument; remaining
    args mirror the legacy `gl.stream_<verb>` signatures. State (dashboard
    token, dashboard port, internal connection, DDL pattern cache) is shared
    via the parent GoldLapel reference held in `self._gl`.
    """

    def __init__(self, gl):
        # Hold a back-reference to the parent client. We never copy lifecycle
        # state (token, port, conn) onto this instance — always read through
        # `self._gl` so a config change on the parent (e.g. proxy restart with
        # a new dashboard token) is reflected immediately on the next call.
        self._gl = gl

    def _patterns(self, stream):
        """Fetch (and cache) canonical stream DDL + query patterns from the
        proxy. Cache lives on the parent GoldLapel instance — see ddl.py."""
        _validate_identifier(stream)
        gl = self._gl
        token = gl._dashboard_token or _ddl.token_from_env_or_file()
        # Cache owner is the parent client so describe-once-per-session works
        # even if the user holds onto a `gl.streams` reference across calls.
        return _ddl.fetch_patterns(gl, "stream", stream, gl._dashboard_port, token)

    def add(self, stream, payload, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(stream)
        return _u.stream_add(
            self._gl._effective_conn(conn), stream, payload, patterns=patterns,
        )

    def create_group(self, stream, group, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(stream)
        return _u.stream_create_group(
            self._gl._effective_conn(conn), stream, group, patterns=patterns,
        )

    def read(self, stream, group, consumer, count=1, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(stream)
        return _u.stream_read(
            self._gl._effective_conn(conn), stream, group, consumer, count,
            patterns=patterns,
        )

    def ack(self, stream, group, message_id, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(stream)
        return _u.stream_ack(
            self._gl._effective_conn(conn), stream, group, message_id,
            patterns=patterns,
        )

    def claim(self, stream, group, consumer, min_idle_ms=60000, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(stream)
        return _u.stream_claim(
            self._gl._effective_conn(conn), stream, group, consumer, min_idle_ms,
            patterns=patterns,
        )
