"""Documents namespace API — `gl.documents.<verb>(...)`.

Wraps the doc-store methods in a sub-API instance held on the parent GoldLapel
client. The instance shares all state (license, dashboard token, http session,
conn) by reference back to the parent — no duplication.

The proxy owns doc-store DDL (Phase 4 of schema-to-core). Each call here:

  1. Calls `/api/ddl/doc_store/create` (idempotent) to materialize the canonical
     `_goldlapel.doc_<name>` table and pull its query patterns.
  2. Caches `(tables, query_patterns)` on the parent GoldLapel instance for the
     session's lifetime (one HTTP round-trip per (family, name) per session).
  3. Hands the patterns off to the existing `goldlapel.utils.doc_*` functions
     so they execute against the canonical table name instead of CREATE-ing
     their own.

Sub-API class shape mirrors `goldlapel.streams.StreamsAPI` — this is the
canonical pattern for the wrapper rollout. Other namespaces (cache, search,
queues, counters, hashes, zsets, geo, auth, …) stay flat for now; they
migrate to nested form one-at-a-time as their own schema-to-core phase fires.
"""

from goldlapel import ddl as _ddl
from goldlapel.utils import _validate_identifier


class DocumentsAPI:
    """The documents sub-API — accessible as `gl.documents`.

    All methods take the collection name as the first positional argument;
    remaining args mirror the legacy `gl.doc_<verb>` signatures. State
    (dashboard token, dashboard port, internal connection, DDL pattern
    cache) is shared via the parent GoldLapel reference held in `self._gl`.
    """

    def __init__(self, gl):
        # Hold a back-reference to the parent client. Never copy lifecycle
        # state (token, port, conn) onto this instance — always read through
        # `self._gl` so a config change on the parent (e.g. proxy restart
        # with a new dashboard token) is reflected immediately on the next
        # call.
        self._gl = gl

    def _patterns(self, collection, *, unlogged=False):
        """Fetch (and cache) canonical doc-store DDL + query patterns from
        the proxy. Cache lives on the parent GoldLapel instance.

        `unlogged` is a creation-time option; passed only on the first call
        for a given (family, name) since proxy `CREATE TABLE IF NOT EXISTS`
        makes subsequent calls no-op DDL-wise. If a caller flips `unlogged`
        across calls in the same session, the table's storage type is
        whatever it was on first create — wrappers don't migrate it.
        """
        _validate_identifier(collection)
        gl = self._gl
        token = gl._dashboard_token or _ddl.token_from_env_or_file()
        options = {"unlogged": True} if unlogged else None
        return _ddl.fetch_patterns(
            gl, "doc_store", collection, gl._dashboard_port, token,
            options=options,
        )

    # -- Collection lifecycle ------------------------------------------------

    def create_collection(self, collection, unlogged=False):
        """Eagerly materialize the doc-store table. Other methods will also
        materialize on first use, so calling this is optional — provided for
        callers that want explicit setup at startup time."""
        self._patterns(collection, unlogged=unlogged)

    # -- CRUD ----------------------------------------------------------------

    def insert(self, collection, document, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(collection)
        return _u.doc_insert(
            self._gl._effective_conn(conn), collection, document, patterns=patterns,
        )

    def insert_many(self, collection, documents, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(collection)
        return _u.doc_insert_many(
            self._gl._effective_conn(conn), collection, documents, patterns=patterns,
        )

    def find(self, collection, filter=None, sort=None, limit=None, skip=None, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(collection)
        return _u.doc_find(
            self._gl._effective_conn(conn), collection,
            filter=filter, sort=sort, limit=limit, skip=skip,
            patterns=patterns,
        )

    def find_cursor(self, collection, filter=None, sort=None, limit=None, skip=None, batch_size=100, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(collection)
        return _u.doc_find_cursor(
            self._gl._effective_conn(conn), collection,
            filter=filter, sort=sort, limit=limit, skip=skip, batch_size=batch_size,
            patterns=patterns,
        )

    def find_one(self, collection, filter=None, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(collection)
        return _u.doc_find_one(
            self._gl._effective_conn(conn), collection, filter=filter, patterns=patterns,
        )

    def update(self, collection, filter, update, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(collection)
        return _u.doc_update(
            self._gl._effective_conn(conn), collection, filter, update, patterns=patterns,
        )

    def update_one(self, collection, filter, update, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(collection)
        return _u.doc_update_one(
            self._gl._effective_conn(conn), collection, filter, update, patterns=patterns,
        )

    def delete(self, collection, filter, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(collection)
        return _u.doc_delete(
            self._gl._effective_conn(conn), collection, filter, patterns=patterns,
        )

    def delete_one(self, collection, filter, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(collection)
        return _u.doc_delete_one(
            self._gl._effective_conn(conn), collection, filter, patterns=patterns,
        )

    def find_one_and_update(self, collection, filter, update, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(collection)
        return _u.doc_find_one_and_update(
            self._gl._effective_conn(conn), collection, filter, update, patterns=patterns,
        )

    def find_one_and_delete(self, collection, filter, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(collection)
        return _u.doc_find_one_and_delete(
            self._gl._effective_conn(conn), collection, filter, patterns=patterns,
        )

    def distinct(self, collection, field, filter=None, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(collection)
        return _u.doc_distinct(
            self._gl._effective_conn(conn), collection, field, filter=filter, patterns=patterns,
        )

    def count(self, collection, filter=None, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(collection)
        return _u.doc_count(
            self._gl._effective_conn(conn), collection, filter=filter, patterns=patterns,
        )

    def create_index(self, collection, keys=None, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(collection)
        return _u.doc_create_index(
            self._gl._effective_conn(conn), collection, keys=keys, patterns=patterns,
        )

    def aggregate(self, collection, pipeline, *, conn=None):
        """Run a Mongo-style aggregation pipeline.

        $lookup.from references are resolved to their canonical proxy tables
        (`_goldlapel.doc_<name>`) — each unique `from` collection triggers an
        idempotent describe/create against the proxy and is cached for the
        session.
        """
        from goldlapel import utils as _u
        patterns = self._patterns(collection)
        # Walk the pipeline once to find every $lookup.from collection, fetch
        # patterns for each (cached after first call), and pass the resolved
        # map down to doc_aggregate.
        lookup_tables = {}
        for stage in pipeline:
            if isinstance(stage, dict) and "$lookup" in stage:
                spec = stage["$lookup"]
                if isinstance(spec, dict) and "from" in spec:
                    from_name = spec["from"]
                    if from_name not in lookup_tables:
                        lp = self._patterns(from_name)
                        lookup_tables[from_name] = lp["tables"]["main"]
        return _u.doc_aggregate(
            self._gl._effective_conn(conn), collection, pipeline,
            patterns=patterns, lookup_tables=lookup_tables,
        )

    # -- Watch / TTL / capped ------------------------------------------------

    def watch(self, collection, callback, blocking=True, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(collection)
        return _u.doc_watch(
            self._gl._effective_conn(conn), collection, callback,
            blocking=blocking, patterns=patterns,
        )

    def unwatch(self, collection, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(collection)
        return _u.doc_unwatch(
            self._gl._effective_conn(conn), collection, patterns=patterns,
        )

    def create_ttl_index(self, collection, expire_after_seconds, field="created_at", *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(collection)
        return _u.doc_create_ttl_index(
            self._gl._effective_conn(conn), collection, expire_after_seconds,
            field=field, patterns=patterns,
        )

    def remove_ttl_index(self, collection, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(collection)
        return _u.doc_remove_ttl_index(
            self._gl._effective_conn(conn), collection, patterns=patterns,
        )

    def create_capped(self, collection, max_documents, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(collection)
        return _u.doc_create_capped(
            self._gl._effective_conn(conn), collection, max_documents, patterns=patterns,
        )

    def remove_cap(self, collection, *, conn=None):
        from goldlapel import utils as _u
        patterns = self._patterns(collection)
        return _u.doc_remove_cap(
            self._gl._effective_conn(conn), collection, patterns=patterns,
        )
