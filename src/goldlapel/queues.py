"""Queue namespace API — `gl.queues.<verb>(...)`.

Phase 5 of schema-to-core. The proxy's v1 queue schema is at-least-once
with visibility-timeout — NOT the legacy fire-and-forget shape. The
breaking change:

  Before:  payload = gl.dequeue("jobs")        # delete-on-fetch, may lose work
  After :  msg = gl.queues.claim("jobs")       # lease the row
           id_, payload = msg                  # unpack
           # ... handle the work ...
           gl.queues.ack("jobs", id_)          # commit; missing ack → redelivery

`claim` returns `(id, payload)` or `None`. The caller MUST `ack(id)` to
commit, or `abandon(id)` to release the lease immediately. A consumer
that crashes leaves the lease standing; the message becomes ready again
after `visibility_timeout_ms` and is redelivered to the next claim.
"""

from goldlapel import ddl as _ddl
from goldlapel.utils import _validate_identifier


class QueuesAPI:
    """The queues sub-API — accessible as `gl.queues`."""

    def __init__(self, gl):
        self._gl = gl

    def _patterns(self, name):
        _validate_identifier(name)
        gl = self._gl
        token = gl._dashboard_token or _ddl.token_from_env_or_file()
        return _ddl.fetch_patterns(gl, "queue", name, gl._dashboard_port, token)

    def create(self, name):
        self._patterns(name)

    def enqueue(self, name, payload, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.queue_enqueue(
            self._gl._effective_conn(conn), name, payload, patterns=patterns,
        )

    def claim(self, name, visibility_timeout_ms=30000, *, conn=None):
        """Claim the next ready message; returns `(id, payload)` or `None`."""
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.queue_claim(
            self._gl._effective_conn(conn), name,
            visibility_timeout_ms=visibility_timeout_ms,
            patterns=patterns,
        )

    def ack(self, name, message_id, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.queue_ack(
            self._gl._effective_conn(conn), name, message_id, patterns=patterns,
        )

    def abandon(self, name, message_id, *, conn=None):
        """Release a claim immediately so the message is redelivered without
        waiting for the visibility timeout. Equivalent to a queue NACK."""
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.queue_abandon(
            self._gl._effective_conn(conn), name, message_id, patterns=patterns,
        )

    def extend(self, name, message_id, additional_ms, *, conn=None):
        """Push the visibility deadline forward by `additional_ms`."""
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.queue_extend(
            self._gl._effective_conn(conn), name, message_id, additional_ms,
            patterns=patterns,
        )

    def peek(self, name, *, conn=None):
        """Look at the next-ready message without claiming."""
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.queue_peek(
            self._gl._effective_conn(conn), name, patterns=patterns,
        )

    def count_ready(self, name, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.queue_count_ready(
            self._gl._effective_conn(conn), name, patterns=patterns,
        )

    def count_claimed(self, name, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.queue_count_claimed(
            self._gl._effective_conn(conn), name, patterns=patterns,
        )
