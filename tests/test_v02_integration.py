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
    inst = goldlapel.start(pg_url, proxy_port=port)
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
        with goldlapel.start(pg_url, proxy_port=7949) as gl:
            assert gl.running
        assert not gl.running


@pytest.mark.asyncio
class TestAsyncEndToEnd:
    async def test_async_factory_and_method(self, pg_url):
        from goldlapel.asyncio import start
        gl = await start(pg_url, proxy_port=7948)
        assert gl.running
        coll = f"gl_v02_smoke_async_{int(time.time() * 1000)}"
        await gl.documents.create_collection(coll, unlogged=True)
        await gl.documents.insert(coll, {"async": True})
        hit = await gl.documents.find_one(coll, {"async": True})
        assert hit is not None
        await gl.stop()

    async def test_async_context_manager(self, pg_url):
        from goldlapel.asyncio import start
        async with start(pg_url, proxy_port=7947) as gl:
            assert gl.running
            coll = f"gl_v02_smoke_async_ctx_{int(time.time() * 1000)}"
            await gl.documents.create_collection(coll, unlogged=True)
        assert not gl.running


def _listener():
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("0.0.0.0", 0))
    sock.listen()
    return sock


class TestPortsAgainstRealProxy:
    """The proxy refuses a port something else holds; the wrapper must
    surface that, and auto-assignment must step over such ports."""

    def test_explicit_busy_port_surfaces_the_proxys_refusal(self, pg_url):
        import goldlapel
        sock = _listener()
        port = sock.getsockname()[1]
        try:
            with pytest.raises(RuntimeError) as exc:
                goldlapel.start(pg_url, proxy_port=port, dashboard_port=0, silent=True)
        finally:
            sock.close()
        assert f"port {port}, for the proxy, is already in use" in str(exc.value)
        assert "exited with status 1" in str(exc.value)

    def test_auto_assignment_steps_over_a_busy_port(self, pg_url):
        import goldlapel
        import goldlapel.proxy as proxy_mod
        import socket
        first = proxy_mod._pick_proxy_port(None, {})
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("0.0.0.0", first))
        sock.listen()
        try:
            gl = goldlapel.start(pg_url, silent=True)
            try:
                assert first not in (gl.proxy_port, gl.dashboard_port)
                import psycopg2
                conn = psycopg2.connect(gl.url)
                cur = conn.cursor()
                cur.execute("SELECT 1")
                assert cur.fetchone() == (1,)
                conn.close()
            finally:
                gl.stop()
        finally:
            sock.close()

    def test_upstream_tls_params_stay_upstream(self, pg_url):
        import goldlapel
        sep = "&" if "?" in pg_url else "?"
        upstream = f"{pg_url}{sep}sslmode=prefer&channel_binding=prefer"
        gl = goldlapel.start(upstream, silent=True)
        try:
            assert "sslmode" not in gl.url and "channel_binding" not in gl.url
            cur = gl.conn.cursor()
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)
        finally:
            gl.stop()

    def test_two_upstreams_each_reach_their_own_database(self, pg_url):
        import goldlapel
        sep = "&" if "?" in pg_url else "?"
        a = goldlapel.start(f"{pg_url}{sep}application_name=gl-a", silent=True)
        b = goldlapel.start(f"{pg_url}{sep}application_name=gl-b", silent=True)
        try:
            assert {a.proxy_port, a.dashboard_port}.isdisjoint({b.proxy_port, b.dashboard_port})
            for gl, name in ((a, "gl-a"), (b, "gl-b")):
                cur = gl.conn.cursor()
                cur.execute("SHOW application_name")
                assert cur.fetchone()[0] == name
        finally:
            a.stop()
            b.stop()
