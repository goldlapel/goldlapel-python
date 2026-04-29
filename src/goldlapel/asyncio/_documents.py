"""Async documents namespace API — `gl.documents.<verb>(...)` on AsyncGoldLapel.

Mirrors goldlapel.documents.DocumentsAPI but with async methods. State
(dashboard token, dashboard port, internal asyncpg connection, DDL pattern
cache) is shared via the parent AsyncGoldLapel reference held in `self._gl`.
"""

import asyncio as _asyncio

from goldlapel import ddl as _ddl
from goldlapel.asyncio import _utils as autils


class AsyncDocumentsAPI:
    """Async documents sub-API — accessible as `gl.documents` on AsyncGoldLapel."""

    def __init__(self, gl):
        self._gl = gl

    async def _patterns(self, collection, *, unlogged=False):
        autils._validate_identifier(collection)
        gl = self._gl
        token = gl._sync._dashboard_token or _ddl.token_from_env_or_file()
        port = gl._sync._dashboard_port
        options = {"unlogged": True} if unlogged else None
        # urllib is blocking; bounce to a thread executor — one round-trip
        # per (family, name) per session, not on a hot path.
        loop = _asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: _ddl.fetch_patterns(
                gl, "doc_store", collection, port, token, options=options,
            ),
        )

    # -- Collection lifecycle -----------------------------------------------

    async def create_collection(self, collection, unlogged=False):
        await self._patterns(collection, unlogged=unlogged)

    # -- CRUD ----------------------------------------------------------------

    async def insert(self, collection, document, *, conn=None):
        patterns = await self._patterns(collection)
        return await autils.doc_insert(
            self._gl._effective_conn(conn), collection, document, patterns=patterns,
        )

    async def insert_many(self, collection, documents, *, conn=None):
        patterns = await self._patterns(collection)
        return await autils.doc_insert_many(
            self._gl._effective_conn(conn), collection, documents, patterns=patterns,
        )

    async def find(self, collection, filter=None, sort=None, limit=None, skip=None, *, conn=None):
        patterns = await self._patterns(collection)
        return await autils.doc_find(
            self._gl._effective_conn(conn), collection,
            filter=filter, sort=sort, limit=limit, skip=skip,
            patterns=patterns,
        )

    async def find_cursor(self, collection, filter=None, sort=None, limit=None, skip=None, batch_size=100, *, conn=None):
        # find_cursor is an async generator — fetch patterns first, then
        # delegate. We can't `async for` here without becoming a generator
        # ourselves, so do that.
        patterns = await self._patterns(collection)
        async for row in autils.doc_find_cursor(
            self._gl._effective_conn(conn), collection,
            filter=filter, sort=sort, limit=limit, skip=skip, batch_size=batch_size,
            patterns=patterns,
        ):
            yield row

    async def find_one(self, collection, filter=None, *, conn=None):
        patterns = await self._patterns(collection)
        return await autils.doc_find_one(
            self._gl._effective_conn(conn), collection, filter=filter, patterns=patterns,
        )

    async def update(self, collection, filter, update, *, conn=None):
        patterns = await self._patterns(collection)
        return await autils.doc_update(
            self._gl._effective_conn(conn), collection, filter, update, patterns=patterns,
        )

    async def update_one(self, collection, filter, update, *, conn=None):
        patterns = await self._patterns(collection)
        return await autils.doc_update_one(
            self._gl._effective_conn(conn), collection, filter, update, patterns=patterns,
        )

    async def delete(self, collection, filter, *, conn=None):
        patterns = await self._patterns(collection)
        return await autils.doc_delete(
            self._gl._effective_conn(conn), collection, filter, patterns=patterns,
        )

    async def delete_one(self, collection, filter, *, conn=None):
        patterns = await self._patterns(collection)
        return await autils.doc_delete_one(
            self._gl._effective_conn(conn), collection, filter, patterns=patterns,
        )

    async def find_one_and_update(self, collection, filter, update, *, conn=None):
        patterns = await self._patterns(collection)
        return await autils.doc_find_one_and_update(
            self._gl._effective_conn(conn), collection, filter, update, patterns=patterns,
        )

    async def find_one_and_delete(self, collection, filter, *, conn=None):
        patterns = await self._patterns(collection)
        return await autils.doc_find_one_and_delete(
            self._gl._effective_conn(conn), collection, filter, patterns=patterns,
        )

    async def distinct(self, collection, field, filter=None, *, conn=None):
        patterns = await self._patterns(collection)
        return await autils.doc_distinct(
            self._gl._effective_conn(conn), collection, field, filter=filter, patterns=patterns,
        )

    async def count(self, collection, filter=None, *, conn=None):
        patterns = await self._patterns(collection)
        return await autils.doc_count(
            self._gl._effective_conn(conn), collection, filter=filter, patterns=patterns,
        )

    async def create_index(self, collection, keys=None, *, conn=None):
        patterns = await self._patterns(collection)
        return await autils.doc_create_index(
            self._gl._effective_conn(conn), collection, keys=keys, patterns=patterns,
        )

    async def aggregate(self, collection, pipeline, *, conn=None):
        patterns = await self._patterns(collection)
        # Pre-resolve $lookup.from collections to their canonical proxy tables.
        lookup_tables = {}
        for stage in pipeline:
            if isinstance(stage, dict) and "$lookup" in stage:
                spec = stage["$lookup"]
                if isinstance(spec, dict) and "from" in spec:
                    from_name = spec["from"]
                    if from_name not in lookup_tables:
                        lp = await self._patterns(from_name)
                        lookup_tables[from_name] = lp["tables"]["main"]
        return await autils.doc_aggregate(
            self._gl._effective_conn(conn), collection, pipeline,
            patterns=patterns, lookup_tables=lookup_tables,
        )

    # -- Watch / TTL / capped ------------------------------------------------

    async def watch(self, collection, callback, blocking=True, *, conn=None):
        patterns = await self._patterns(collection)
        return await autils.doc_watch(
            self._gl._effective_conn(conn), collection, callback,
            blocking=blocking, patterns=patterns,
        )

    async def unwatch(self, collection, *, conn=None):
        patterns = await self._patterns(collection)
        return await autils.doc_unwatch(
            self._gl._effective_conn(conn), collection, patterns=patterns,
        )

    async def create_ttl_index(self, collection, expire_after_seconds, field="created_at", *, conn=None):
        patterns = await self._patterns(collection)
        return await autils.doc_create_ttl_index(
            self._gl._effective_conn(conn), collection, expire_after_seconds,
            field=field, patterns=patterns,
        )

    async def remove_ttl_index(self, collection, *, conn=None):
        patterns = await self._patterns(collection)
        return await autils.doc_remove_ttl_index(
            self._gl._effective_conn(conn), collection, patterns=patterns,
        )

    async def create_capped(self, collection, max_documents, *, conn=None):
        patterns = await self._patterns(collection)
        return await autils.doc_create_capped(
            self._gl._effective_conn(conn), collection, max_documents, patterns=patterns,
        )

    async def remove_cap(self, collection, *, conn=None):
        patterns = await self._patterns(collection)
        return await autils.doc_remove_cap(
            self._gl._effective_conn(conn), collection, patterns=patterns,
        )
