"""Native-asyncpg integration tests.

Verify the async path (native asyncpg under the hood) produces the same
results as the sync path for the canonical wrapper methods. Skipped
automatically when asyncpg isn't installed or Postgres isn't reachable.

Covers the acceptance criteria spelled out in the native-asyncpg-driver
plan: search, doc_insert, doc_find, doc_count, doc_update, and a
gl.using() transactional scope using a caller-supplied asyncpg
connection + asyncpg transaction.
"""

import time

import pytest

from _integration_gate import require_integration_upstream

pytestmark = pytest.mark.integration

asyncpg = pytest.importorskip("asyncpg")


@pytest.fixture(scope="module")
def pg_url():
    url = require_integration_upstream()
    try:
        import psycopg2
        conn = psycopg2.connect(url, connect_timeout=2)
        conn.close()
    except Exception as e:
        pytest.skip(f"Postgres not reachable at {url}: {e}")
    return url


@pytest.fixture
def collection_name():
    return f"gl_asyncpg_it_{int(time.time() * 1000)}"


@pytest.fixture
def search_table(pg_url):
    """A plain table with text + pre-seeded rows. Fresh per test."""
    table = f"gl_asyncpg_search_{int(time.time() * 1000)}"
    import psycopg2
    conn = psycopg2.connect(pg_url)
    cur = conn.cursor()
    cur.execute(f"CREATE UNLOGGED TABLE {table} (id SERIAL PRIMARY KEY, body TEXT)")
    cur.executemany(
        f"INSERT INTO {table} (body) VALUES (%s)",
        [
            ("postgres tuning guide",),
            ("how to index a table",),
            ("asyncpg is fast",),
            ("python async programming patterns",),
            ("postgres async queries with asyncpg",),
        ],
    )
    conn.commit()
    cur.close()
    conn.close()
    yield table
    # Cleanup
    conn = psycopg2.connect(pg_url)
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()
    cur.close()
    conn.close()


# -- Canonical native-async tests -------------------------------------------

@pytest.mark.asyncio
class TestNativeAsyncpgPath:
    """End-to-end tests: AsyncGoldLapel's internal conn is an asyncpg
    connection (wrapped in AsyncCachedConnection), no thread-pool bounce."""

    async def test_internal_conn_is_asyncpg(self, pg_url):
        from goldlapel.asyncio import start
        async with start(pg_url, port=7840) as gl:
            # The internal conn is AsyncCachedConnection wrapping asyncpg.
            raw = gl.conn._real
            assert isinstance(raw, asyncpg.Connection)

    async def test_doc_insert_and_find(self, pg_url, collection_name):
        from goldlapel.asyncio import start
        async with start(pg_url, port=7841) as gl:
            await gl.documents.create_collection(collection_name, unlogged=True)
            inserted = await gl.documents.insert(
                collection_name, {"hello": "asyncpg", "count": 1},
            )
            assert inserted is not None
            assert "_id" in inserted
            # JSONB codec registered → native dict, not string.
            assert isinstance(inserted["data"], dict)
            assert inserted["data"]["hello"] == "asyncpg"

            found = await gl.documents.find(collection_name, {"hello": "asyncpg"})
            assert len(found) == 1
            assert found[0]["data"]["count"] == 1

            one = await gl.documents.find_one(collection_name, {"hello": "asyncpg"})
            assert one["data"]["hello"] == "asyncpg"

            n = await gl.documents.count(collection_name)
            assert n == 1

    async def test_doc_update_and_delete(self, pg_url, collection_name):
        from goldlapel.asyncio import start
        async with start(pg_url, port=7842) as gl:
            await gl.documents.create_collection(collection_name, unlogged=True)
            await gl.documents.insert(collection_name, {"k": "a", "n": 1})
            await gl.documents.insert(collection_name, {"k": "b", "n": 2})
            updated = await gl.documents.update(
                collection_name, {"k": "a"}, {"$set": {"n": 42}},
            )
            assert updated == 1
            one = await gl.documents.find_one(collection_name, {"k": "a"})
            assert one["data"]["n"] == 42

            deleted = await gl.documents.delete(collection_name, {"k": "b"})
            assert deleted == 1
            n = await gl.documents.count(collection_name)
            assert n == 1

    async def test_search_native_async(self, pg_url, search_table):
        from goldlapel.asyncio import start
        async with start(pg_url, port=7843) as gl:
            results = await gl.search(search_table, "body", "postgres")
            # Two docs mention 'postgres' in our seed data.
            assert len(results) >= 2
            # Rows are dicts with the expected columns.
            assert all("body" in r for r in results)
            assert all("_score" in r for r in results)

    async def test_using_with_user_asyncpg_transaction(
        self, pg_url, collection_name,
    ):
        """gl.using(user_conn) must route wrapper calls through the
        user-supplied asyncpg connection — matching sync semantics."""
        from goldlapel.asyncio import start
        async with start(pg_url, port=7844) as gl:
            # Pre-create the collection using the internal conn.
            await gl.documents.create_collection(collection_name, unlogged=True)

            # Open a user-owned asyncpg connection to the proxy URL and open
            # a tx on it; all inserts inside the `using` block go through it.
            # Match the internal conn's settings: disable statement cache to
            # avoid the Gold Lapel proxy's CloseComplete-framing interaction
            # with persistent prepared statements.
            user_conn = await asyncpg.connect(gl.url, statement_cache_size=0)
            # Register JSONB codec so the user-supplied conn returns dicts too
            # (the util layer handles text decoding defensively, but aligning
            # matches the internal-conn behavior).
            from goldlapel.asyncio._utils import _register_jsonb_codec
            await _register_jsonb_codec(user_conn)
            try:
                async with user_conn.transaction():
                    async with gl.using(user_conn):
                        await gl.documents.insert(
                            collection_name, {"who": "user-tx", "n": 1},
                        )
                        await gl.documents.insert(
                            collection_name, {"who": "user-tx", "n": 2},
                        )
                # After commit, the rows are visible via the internal conn.
                found = await gl.documents.find(collection_name, {"who": "user-tx"})
                assert len(found) == 2
            finally:
                await user_conn.close()

    async def test_using_rollback_discards_writes(self, pg_url, collection_name):
        """If the user-supplied transaction rolls back, writes must be gone."""
        from goldlapel.asyncio import start
        async with start(pg_url, port=7845) as gl:
            await gl.documents.create_collection(collection_name, unlogged=True)

            # Match the internal conn's settings: disable statement cache to
            # avoid the Gold Lapel proxy's CloseComplete-framing interaction
            # with persistent prepared statements.
            user_conn = await asyncpg.connect(gl.url, statement_cache_size=0)
            from goldlapel.asyncio._utils import _register_jsonb_codec
            await _register_jsonb_codec(user_conn)
            try:
                class _Boom(RuntimeError):
                    pass
                with pytest.raises(_Boom):
                    async with user_conn.transaction():
                        async with gl.using(user_conn):
                            await gl.documents.insert(
                                collection_name,
                                {"who": "rollback", "n": 1},
                            )
                        raise _Boom("force rollback")
                # Row should not be visible — tx rolled back.
                count = await gl.documents.count(
                    collection_name, {"who": "rollback"},
                )
                assert count == 0
            finally:
                await user_conn.close()

    async def test_conn_kwarg_overrides_internal(
        self, pg_url, collection_name,
    ):
        from goldlapel.asyncio import start
        async with start(pg_url, port=7846) as gl:
            await gl.documents.create_collection(collection_name, unlogged=True)
            # Match the internal conn's settings: disable statement cache to
            # avoid the Gold Lapel proxy's CloseComplete-framing interaction
            # with persistent prepared statements.
            user_conn = await asyncpg.connect(gl.url, statement_cache_size=0)
            from goldlapel.asyncio._utils import _register_jsonb_codec
            await _register_jsonb_codec(user_conn)
            try:
                await gl.documents.insert(
                    collection_name, {"k": "kwarg"}, conn=user_conn,
                )
                # Visible via the internal conn too (auto-commit on asyncpg
                # outside a transaction).
                found = await gl.documents.find_one(collection_name, {"k": "kwarg"})
                assert found is not None
            finally:
                await user_conn.close()


# -- Sync vs async result-shape parity --------------------------------------

@pytest.mark.asyncio
class TestSyncAsyncParity:
    """The native-async path should produce results that match the sync path
    in shape and content for the same input."""

    async def test_search_results_match_sync(self, pg_url, search_table):
        import goldlapel
        from goldlapel.asyncio import start

        with goldlapel.start(pg_url, port=7850) as sync_gl:
            sync_results = sync_gl.search(search_table, "body", "postgres")

        async with start(pg_url, port=7851) as async_gl:
            async_results = await async_gl.search(
                search_table, "body", "postgres",
            )

        assert len(sync_results) == len(async_results)
        # Order by score should match, and the bodies must match 1:1.
        sync_bodies = [r["body"] for r in sync_results]
        async_bodies = [r["body"] for r in async_results]
        assert sync_bodies == async_bodies

    async def test_doc_insert_and_find_match_sync(
        self, pg_url, collection_name,
    ):
        import goldlapel
        from goldlapel.asyncio import start

        # Sync path: insert + find
        with goldlapel.start(pg_url, port=7852) as sync_gl:
            sync_gl.documents.create_collection(collection_name, unlogged=True)
            sync_gl.documents.insert(collection_name, {"tag": "a", "n": 1})
            sync_gl.documents.insert(collection_name, {"tag": "a", "n": 2})
            sync_gl.documents.insert(collection_name, {"tag": "b", "n": 3})
            sync_results = sync_gl.documents.find(
                collection_name, {"tag": "a"}, sort={"n": 1},
            )

        # Async path on the same table
        async with start(pg_url, port=7853) as async_gl:
            async_results = await async_gl.documents.find(
                collection_name, {"tag": "a"}, sort={"n": 1},
            )

        assert len(sync_results) == len(async_results)
        for s, a in zip(sync_results, async_results):
            # Shape parity: both are dicts with the same keys.
            assert set(s.keys()) == set(a.keys())
            # Data round-trip: both are dicts (JSONB decoded natively).
            assert s["data"]["tag"] == a["data"]["tag"]
            assert s["data"]["n"] == a["data"]["n"]
