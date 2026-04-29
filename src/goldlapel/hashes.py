"""Hash namespace API — `gl.hashes.<verb>(...)`.

Phase 5 of schema-to-core. The proxy's v1 hash schema is row-per-field
(`hash_key`, `field`, `value`) — NOT the legacy JSONB-blob-per-key shape.
Every method threads `hash_key` as the first positional arg after the
namespace `name`. `value` is JSON-encoded so callers can store arbitrary
structured payloads.
"""

from goldlapel import ddl as _ddl
from goldlapel.utils import _validate_identifier


class HashesAPI:
    """The hashes sub-API — accessible as `gl.hashes`."""

    def __init__(self, gl):
        self._gl = gl

    def _patterns(self, name):
        _validate_identifier(name)
        gl = self._gl
        token = gl._dashboard_token or _ddl.token_from_env_or_file()
        return _ddl.fetch_patterns(gl, "hash", name, gl._dashboard_port, token)

    def create(self, name):
        self._patterns(name)

    def set(self, name, hash_key, field, value, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.hash_set(
            self._gl._effective_conn(conn), name, hash_key, field, value,
            patterns=patterns,
        )

    def get(self, name, hash_key, field, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.hash_get(
            self._gl._effective_conn(conn), name, hash_key, field,
            patterns=patterns,
        )

    def get_all(self, name, hash_key, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.hash_get_all(
            self._gl._effective_conn(conn), name, hash_key, patterns=patterns,
        )

    def keys(self, name, hash_key, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.hash_keys(
            self._gl._effective_conn(conn), name, hash_key, patterns=patterns,
        )

    def values(self, name, hash_key, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.hash_values(
            self._gl._effective_conn(conn), name, hash_key, patterns=patterns,
        )

    def exists(self, name, hash_key, field, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.hash_exists(
            self._gl._effective_conn(conn), name, hash_key, field,
            patterns=patterns,
        )

    def delete(self, name, hash_key, field, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.hash_delete(
            self._gl._effective_conn(conn), name, hash_key, field,
            patterns=patterns,
        )

    def len(self, name, hash_key, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(name)
        return _u.hash_len(
            self._gl._effective_conn(conn), name, hash_key, patterns=patterns,
        )
