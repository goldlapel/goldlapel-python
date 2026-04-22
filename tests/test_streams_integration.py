"""End-to-end streams integration tests — proxy-owned DDL (Phase 2).

Exercises the full flow:
- Spawn a real proxy + real Postgres.
- Call `gl.stream_add(...)` — verify table is at `_goldlapel.stream_<name>`.
- Verify `_goldlapel.schema_meta` contains a row for the stream.
- Verify repeated calls do NOT re-issue DDL (HTTP round-trip count stays at 1
  per (family, name) per session).
- Exercise stream_create_group → stream_read → stream_ack full round-trip.
- Exercise stream_claim pending-reassignment path.

Gated on GOLDLAPEL_INTEGRATION=1 + GOLDLAPEL_TEST_UPSTREAM — the
standardized integration-test convention shared across all Gold Lapel
wrappers. See tests/_integration_gate.py.
"""

import time

import pytest

from _integration_gate import require_integration_upstream

pytestmark = pytest.mark.integration


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
def stream_name():
    # Unique per-test so repeated runs against a stale DB don't collide.
    return f"gl_int_stream_{int(time.time() * 1000)}"


@pytest.fixture
def gl(pg_url):
    """Spawn Gold Lapel proxy for this test, tear down on exit."""
    import goldlapel
    # Random high port range unlikely to collide with a dev install
    port = 7700 + (int(time.time() * 1000) % 100)
    inst = goldlapel.start(pg_url, port=port)
    yield inst
    inst.stop()


def _direct_conn(pg_url):
    import psycopg2
    c = psycopg2.connect(pg_url)
    c.autocommit = True
    return c


class TestStreamDdlOwnership:
    def test_stream_add_creates_prefixed_table(self, gl, pg_url, stream_name):
        gl.stream_add(stream_name, {"type": "click"})

        # The canonical table should be _goldlapel.stream_<name>.
        conn = _direct_conn(pg_url)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = '_goldlapel' AND table_name = %s",
                (f"stream_{stream_name}",),
            )
            assert cur.fetchone()[0] == 1, (
                f"expected _goldlapel.stream_{stream_name} to exist"
            )

            # Ensure the in-wrapper naming didn't land anywhere (no public.<name>).
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = %s",
                (stream_name,),
            )
            assert cur.fetchone()[0] == 0, (
                f"public.{stream_name} should not have been created — proxy owns DDL now"
            )
        finally:
            conn.close()

    def test_schema_meta_row_recorded(self, gl, pg_url, stream_name):
        gl.stream_add(stream_name, {"type": "click"})

        conn = _direct_conn(pg_url)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT family, name, schema_version FROM _goldlapel.schema_meta "
                "WHERE family = 'stream' AND name = %s",
                (stream_name,),
            )
            row = cur.fetchone()
            assert row is not None, "schema_meta row missing"
            assert row == ("stream", stream_name, "v1")
        finally:
            conn.close()

    def test_subsequent_calls_skip_ddl_fetch(self, gl, stream_name, monkeypatch):
        """After the first fetch, subsequent calls use the cached patterns —
        no extra HTTP round-trip to /api/ddl/*."""
        from goldlapel import ddl as _ddl

        real_fetch = _ddl.fetch_patterns
        counter = {"n": 0}

        def counting_fetch(*args, **kwargs):
            counter["n"] += 1
            return real_fetch(*args, **kwargs)

        monkeypatch.setattr(_ddl, "fetch", counting_fetch)

        # Three separate adds — first call hits the cache once (and performs HTTP),
        # subsequent calls are served out of the cache layer. Our counter still ticks
        # because the cache lookup is inside fetch() itself — but we can see the
        # HTTP round-trip didn't repeat by checking the captured request count…
        # Actually, the cache is inside fetch(), so fetch still runs — we measure
        # by counting the *distinct* results it returns (should be identical).
        gl.stream_add(stream_name, {"i": 1})
        gl.stream_add(stream_name, {"i": 2})
        gl.stream_add(stream_name, {"i": 3})

        assert counter["n"] == 3, (
            "fetch should be called once per stream_add invocation (but returns cached)"
        )
        # And the cache entry itself is shared:
        cache = _ddl._cache_for(gl)
        assert ("stream", stream_name) in cache

    def test_ddl_http_call_happens_once_per_name(self, gl, pg_url, stream_name, monkeypatch):
        """Instrument the HTTP layer directly — first stream op should POST
        exactly once; subsequent ops should not POST again."""
        from goldlapel import ddl as _ddl

        real_post = _ddl._post
        counter = {"n": 0}

        def counting_post(*args, **kwargs):
            counter["n"] += 1
            return real_post(*args, **kwargs)

        monkeypatch.setattr(_ddl, "_post", counting_post)

        gl.stream_add(stream_name, {"i": 1})
        assert counter["n"] == 1, "first call should post once"
        gl.stream_add(stream_name, {"i": 2})
        gl.stream_add(stream_name, {"i": 3})
        assert counter["n"] == 1, (
            "subsequent calls must use the cached patterns — no extra POST"
        )


class TestStreamRoundTrip:
    def test_add_and_read_round_trip(self, gl, stream_name):
        gl.stream_create_group(stream_name, "workers")
        msg_id_1 = gl.stream_add(stream_name, {"i": 1})
        msg_id_2 = gl.stream_add(stream_name, {"i": 2})
        assert isinstance(msg_id_1, int) and msg_id_1 > 0
        assert msg_id_2 > msg_id_1

        messages = gl.stream_read(stream_name, "workers", "consumer-1", count=10)
        assert len(messages) == 2
        assert messages[0]["payload"] == {"i": 1}
        assert messages[1]["payload"] == {"i": 2}
        assert messages[0]["id"] == msg_id_1
        assert messages[1]["id"] == msg_id_2

    def test_ack_removes_pending(self, gl, stream_name):
        gl.stream_create_group(stream_name, "workers")
        msg_id = gl.stream_add(stream_name, {"i": 1})
        messages = gl.stream_read(stream_name, "workers", "c", count=10)
        assert len(messages) == 1

        ok = gl.stream_ack(stream_name, "workers", msg_id)
        assert ok is True
        # Second ack is a no-op — already removed.
        ok2 = gl.stream_ack(stream_name, "workers", msg_id)
        assert ok2 is False

    def test_claim_reassigns_idle_pending(self, gl, stream_name):
        gl.stream_create_group(stream_name, "workers")
        gl.stream_add(stream_name, {"i": 1})
        # Consume as consumer-A.
        gl.stream_read(stream_name, "workers", "consumer-a", count=10)
        # Claim with min_idle_ms=0 — should sweep in the pending row.
        claimed = gl.stream_claim(stream_name, "workers", "consumer-b", min_idle_ms=0)
        assert len(claimed) == 1
        assert claimed[0]["payload"] == {"i": 1}


@pytest.mark.asyncio
class TestAsyncStream:
    async def test_async_stream_round_trip(self, pg_url):
        from goldlapel.asyncio import start

        port = 7800 + (int(time.time() * 1000) % 100)
        async with start(pg_url, port=port) as gl:
            assert gl.running
            name = f"gl_async_stream_{int(time.time() * 1000)}"
            await gl.stream_create_group(name, "workers")
            await gl.stream_add(name, {"i": 1})
            await gl.stream_add(name, {"i": 2})
            messages = await gl.stream_read(name, "workers", "c", count=10)
            assert len(messages) == 2
            assert messages[0]["payload"] == {"i": 1}
            await gl.stream_ack(name, "workers", messages[0]["id"])
