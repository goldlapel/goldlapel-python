"""Search helpers against real data, through the proxy: each returns the
right rows, and the SQL it sends is SQL Gold Lapel indexes — once the
indexer has run, Postgres plans the same query with GL's index.
"""

import time

import pytest

from _integration_gate import require_integration_upstream

pytestmark = pytest.mark.integration

TABLE = "gl_search_articles"
NAMES = ["Smith", "Smyth", "Smithe", "Jones", "Johnson", "Brown", "Browne", "Green"]


@pytest.fixture(scope="module")
def database():
    """A database of its own: GL starts a fresh trial in it (one that has
    GL's schema without a trial of this HOME's reads as tampered with, and
    optimizations stay paused), so its indexer runs."""
    import psycopg2
    from urllib.parse import urlsplit, urlunsplit
    upstream = require_integration_upstream()
    name = f"gl_search_{int(time.time() * 1000) % 10**9}"
    admin = psycopg2.connect(upstream)
    admin.autocommit = True
    admin.cursor().execute(f"CREATE DATABASE {name}")
    parts = urlsplit(upstream)
    yield urlunsplit(parts._replace(path="/" + name))
    admin.cursor().execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
    admin.close()


@pytest.fixture(scope="module")
def direct(database):
    import psycopg2
    conn = psycopg2.connect(database)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {TABLE} (id serial PRIMARY KEY, title text, body text, author text, category text)")
    # Enough rows that an index is worth planning
    cur.execute(
        f"INSERT INTO {TABLE} (title, body, author, category) "
        "SELECT 'Report ' || n, 'filler words about nothing ' || n, (%s::text[])[1 + n %% 8], 'c' || (n %% 5) "
        "FROM generate_series(1, 5000) n",
        (NAMES,),
    )
    cur.execute(
        f"INSERT INTO {TABLE} (title, body, author, category) VALUES "
        "('Postgres caching', 'A proxy that caches query results', 'Smith', 'db'), "
        "('Gardening', 'Tomatoes and the proxy war of the aphids', 'Green', 'home'), "
        "('Lapel styles', 'Gold buttons on a dinner jacket', 'Brown', 'style')"
    )
    cur.execute(f"ANALYZE {TABLE}")
    yield cur
    conn.close()


@pytest.fixture(scope="module")
def gl(database, direct, tmp_path_factory):
    import os
    import goldlapel
    home = os.environ.get("HOME")
    os.environ["HOME"] = str(tmp_path_factory.mktemp("home"))
    try:
        inst = goldlapel.start(database, proxy_port=7790 + int(time.time()) % 40, extra_args=["--report-interval-secs", "1"])
    finally:
        os.environ["HOME"] = home
    yield inst
    inst.stop()


def gl_index(direct, fragment, timeout=40):
    """GL's index on the table whose definition has `fragment`, once built."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        direct.execute("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = %s", (TABLE,))
        for name, definition in direct.fetchall():
            if fragment in definition:
                return name
        time.sleep(0.5)
    direct.execute("SELECT indexdef FROM pg_indexes WHERE tablename = %s", (TABLE,))
    pytest.fail(f"no index with {fragment!r}: {[r[0] for r in direct.fetchall()]}")


def uses(direct, index, sql, params=()):
    """Whether Postgres plans `sql` with `index` (sequential scans off, so
    the plan shows the index if it can serve the query at all)."""
    direct.execute("SET enable_seqscan = off")
    direct.execute("EXPLAIN " + sql, params)
    plan = "\n".join(r[0] for r in direct.fetchall())
    direct.execute("RESET enable_seqscan")
    return index in plan, plan


def repeat(call, times=4):
    for _ in range(times):
        result = call()
    return result


def test_search_several_columns(gl, direct):
    found = repeat(lambda: gl.search(TABLE, ["title", "body"], "proxy caching"))
    assert [r["title"] for r in found] == ["Postgres caching"]
    index = gl_index(direct, "COALESCE(title, ''::text)")
    ok, plan = uses(
        direct, index,
        f"SELECT * FROM {TABLE} WHERE to_tsvector('english', coalesce(title, '') || ' ' || coalesce(body, '')) "
        "@@ plainto_tsquery('english', %s)",
        ("proxy",),
    )
    assert ok, plan


def test_search_highlight(gl):
    found = gl.search(TABLE, "body", "aphids", highlight=True)
    assert [r["title"] for r in found] == ["Gardening"]
    assert "<mark>aphids</mark>" in found[0]["_highlight"]


def test_search_rejects_a_bad_config(gl):
    with pytest.raises(ValueError, match="text search configuration"):
        gl.search(TABLE, "body", "x", lang="english'); DROP TABLE x; --")


def test_facets_with_a_query(gl, direct):
    found = repeat(lambda: gl.facets(TABLE, "category", query="proxy", query_column="body"))
    assert sorted((r["value"], r["count"]) for r in found) == [("db", 1), ("home", 1)]
    index = gl_index(direct, "to_tsvector('english'::regconfig, COALESCE(body, ''::text))")
    ok, plan = uses(
        direct, index,
        f"SELECT category FROM {TABLE} WHERE to_tsvector('english', coalesce(body, '')) @@ plainto_tsquery('english', %s)",
        ("proxy",),
    )
    assert ok, plan


def test_search_fuzzy(gl, direct):
    found = repeat(lambda: gl.search_fuzzy(TABLE, "title", "Postgress cachng", threshold=0.4))
    assert found[0]["title"] == "Postgres caching"
    index = gl_index(direct, "title gin_trgm_ops")
    ok, plan = uses(direct, index, f"SELECT * FROM {TABLE} WHERE title %% %s AND similarity(title, %s) > 0.4", ("Postgress", "Postgress"))
    assert ok, plan


def test_search_phonetic_finds_sound_alikes(gl, direct):
    found = repeat(lambda: gl.search_phonetic(TABLE, "author", "Smyth", limit=6000))
    authors = {r["author"] for r in found}
    # soundex: S530 for each
    assert {"Smith", "Smyth", "Smithe"} <= authors and "Jones" not in authors
    index = gl_index(direct, "soundex(author)")
    ok, plan = uses(direct, index, f"SELECT * FROM {TABLE} WHERE soundex(author) = soundex(%s)", ("Smyth",))
    assert ok, plan


def test_suggest(gl, direct):
    found = repeat(lambda: gl.suggest(TABLE, "title", "Lap"))
    assert [r["title"] for r in found] == ["Lapel styles"]
    index = gl_index(direct, "title gin_trgm_ops")
    ok, plan = uses(direct, index, f"SELECT * FROM {TABLE} WHERE title ILIKE %s", ("Lap%",))
    assert ok, plan


def test_documents_text_search_is_indexed(gl, direct):
    coll = f"srch_{int(time.time() * 1000) % 10**9}"
    gl.documents.create_collection(coll)
    gl.documents.insert_many(coll, [{"text": f"note {n}"} for n in range(2000)] + [{"text": "the gold lapel"}])
    found = repeat(lambda: gl.documents.find(coll, {"text": {"$text": {"$search": "lapel"}}}))
    assert [d["data"]["text"] for d in found] == ["the gold lapel"]
    deadline = time.time() + 40
    while time.time() < deadline:
        direct.execute("SELECT indexdef FROM pg_indexes WHERE tablename = %s AND indexdef LIKE '%%to_tsvector%%'", (f"doc_{coll}",))
        if direct.fetchall():
            break
        time.sleep(0.5)
    else:
        pytest.fail("no full-text index on the collection")
    gl.documents.delete(coll, {})
