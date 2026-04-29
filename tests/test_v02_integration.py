"""End-to-end integration tests for the v0.2 factory API.

Gated on GOLDLAPEL_INTEGRATION=1 + GOLDLAPEL_TEST_UPSTREAM (the
standardized integration-test convention — see tests/conftest.py). The
goldlapel binary is resolved from GOLDLAPEL_BINARY (preferred — the
default shutil.which("goldlapel") may resolve to the wrapper's own CLI
script in dev installs) or PATH.
"""

import time

import pytest

from _integration_gate import require_integration_upstream

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def pg_url():
    url = require_integration_upstream()
    # best-effort reachability probe
    try:
        import psycopg2
        conn = psycopg2.connect(url, connect_timeout=2)
        conn.close()
    except Exception as e:
        pytest.skip(f"Postgres not reachable at {url}: {e}")
    return url


@pytest.fixture
def collection_name():
    return f"gl_v02_smoke_{int(time.time() * 1000)}"


@pytest.fixture
def gl(pg_url):
    """Spawn Gold Lapel proxy for this test, tear down on exit."""
    import goldlapel
    # Use a high port to avoid conflicts with default installs
    port = 7900 + (int(time.time()) % 50)
    inst = goldlapel.start(pg_url, port=port)
    yield inst
    inst.stop()


class TestFactoryEndToEnd:
    def test_start_returns_instance(self, gl):
        from goldlapel.proxy import GoldLapel
        assert isinstance(gl, GoldLapel)
        assert gl.running
        assert gl.url.startswith("postgresql://")

    def test_raw_sql_via_url(self, gl):
        import psycopg2
        conn = psycopg2.connect(gl.url)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)
        conn.close()

    def test_wrapper_methods(self, gl, collection_name):
        gl.documents.create_collection(collection_name, unlogged=True)
        gl.documents.insert(collection_name, {"hello": "world", "n": 1})
        hit = gl.documents.find_one(collection_name, {"hello": "world"})
        assert hit is not None
        assert hit["data"]["hello"] == "world"
        assert gl.documents.count(collection_name) == 1

    def test_using_scope_with_user_conn(self, gl, collection_name):
        import psycopg2
        gl.documents.create_collection(collection_name, unlogged=True)

        conn = psycopg2.connect(gl.url)
        with gl.using(conn):
            gl.documents.insert(collection_name, {"from": "using-scope"})
        conn.close()

        hit = gl.documents.find_one(collection_name, {"from": "using-scope"})
        assert hit is not None
        assert hit["data"]["from"] == "using-scope"

    def test_conn_kwarg_on_method(self, gl, collection_name):
        import psycopg2
        gl.documents.create_collection(collection_name, unlogged=True)

        conn = psycopg2.connect(gl.url)
        gl.documents.insert(collection_name, {"from": "kwarg"}, conn=conn)
        conn.close()

        hit = gl.documents.find_one(collection_name, {"from": "kwarg"})
        assert hit is not None


class TestContextManager:
    def test_with_statement_starts_and_stops(self, pg_url):
        import goldlapel
        with goldlapel.start(pg_url, port=7949) as gl:
            assert gl.running
        assert not gl.running


@pytest.mark.asyncio
class TestAsyncEndToEnd:
    async def test_async_factory_and_method(self, pg_url):
        from goldlapel.asyncio import start
        gl = await start(pg_url, port=7948)
        assert gl.running
        coll = f"gl_v02_smoke_async_{int(time.time() * 1000)}"
        await gl.documents.create_collection(coll, unlogged=True)
        await gl.documents.insert(coll, {"async": True})
        hit = await gl.documents.find_one(coll, {"async": True})
        assert hit is not None
        await gl.stop()

    async def test_async_context_manager(self, pg_url):
        from goldlapel.asyncio import start
        async with start(pg_url, port=7947) as gl:
            assert gl.running
            coll = f"gl_v02_smoke_async_ctx_{int(time.time() * 1000)}"
            await gl.documents.create_collection(coll, unlogged=True)
        assert not gl.running
