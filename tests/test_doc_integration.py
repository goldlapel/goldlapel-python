"""Real-Postgres integration tests for a focused set of high-risk doc_* operations.

These are promotions of mock-heavy tests from ``tests/test_doc.py`` whose SQL-shape
assertions can't catch semantically-wrong-but-syntactically-valid SQL. The mock
tests are KEPT (they still guard SQL-generation regression); these are additional
coverage that prove the generated SQL actually does what Postgres expects.

Scope (v0.2 Tests Q8): jsonb merge/update semantics (``$inc``/``$push``/``$unset``),
CTE-based ``doc_update_one`` roundtrips, filter-operator AST correctness (``$in``,
nested ``$or``/``$and``), pagination ordering stability, capped-collection trigger
firing, and ``doc_find_one_and_update`` RETURNING correctness.

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
def collection_name():
    # millisecond-unique + nanosecond tail keeps names distinct even when two
    # tests in the same file start in the same millisecond.
    return f"gl_doc_it_{int(time.time() * 1000)}_{time.perf_counter_ns() & 0xFFFF}"


@pytest.fixture
def conn(pg_url):
    """Raw psycopg2 connection against Postgres directly.

    We bypass the proxy here because these tests assert on the SQL that the
    wrapper emits, not on proxy behavior. ``tests/test_v02_integration.py``
    already covers the proxy end-to-end path.
    """
    import psycopg2
    c = psycopg2.connect(pg_url)
    yield c
    c.close()


def _drop(conn, *collections):
    """Best-effort cleanup. Each test creates ephemeral tables; drop on teardown."""
    with conn.cursor() as cur:
        for name in collections:
            cur.execute(f"DROP TABLE IF EXISTS {name} CASCADE")
    conn.commit()


# ---------------------------------------------------------------------------
# 1. $inc via doc_update_one — CTE + jsonb_set + COALESCE semantics
#    Mock test: TestFieldUpdateOperators::test_inc_in_doc_update_one
#    Risk: CTE scope ("WHERE {coll}._id = target._id") + jsonb_set path array
#          cast + COALESCE(NULL, 0) arithmetic. Shape-only assertion can't
#          prove the update actually lands on the matched row, or that the
#          increment applies to a missing field (starts at 0).
# ---------------------------------------------------------------------------

def test_inc_update_one_applies_to_matched_row(conn, collection_name):
    from goldlapel.utils import doc_insert, doc_update_one, doc_find_one
    try:
        doc_insert(conn, collection_name, {"name": "alice", "score": 10})
        doc_insert(conn, collection_name, {"name": "bob", "score": 10})

        matched = doc_update_one(conn, collection_name, {"name": "alice"}, {"$inc": {"score": 5}})
        assert matched == 1

        alice = doc_find_one(conn, collection_name, {"name": "alice"})
        bob = doc_find_one(conn, collection_name, {"name": "bob"})
        assert alice["data"]["score"] == 15
        assert bob["data"]["score"] == 10, "inc must not leak to sibling rows"
    finally:
        _drop(conn, collection_name)


def test_inc_update_one_creates_missing_field_from_zero(conn, collection_name):
    from goldlapel.utils import doc_insert, doc_update_one, doc_find_one
    try:
        # Insert a row with no "counter" field at all; $inc should COALESCE-default
        # it to 0 and then add 3 — i.e. land at 3, not NULL or an error.
        doc_insert(conn, collection_name, {"name": "alice"})
        doc_update_one(conn, collection_name, {"name": "alice"}, {"$inc": {"counter": 3}})
        alice = doc_find_one(conn, collection_name, {"name": "alice"})
        assert alice["data"]["counter"] == 3
    finally:
        _drop(conn, collection_name)


# ---------------------------------------------------------------------------
# 2. $push via doc_update — jsonb_set + COALESCE('[]'::jsonb) + array concat
#    Mock test: TestArrayUpdateOperators::test_push_in_doc_update
#    Risk: the generated SQL is
#          jsonb_set(data, '{tags}', COALESCE(data->'tags','[]'::jsonb) || to_jsonb(%s::text))
#          — three operators with precedence that has to be right. Shape-only
#          assertion says "jsonb_set is in the string", which can silently pass
#          even if Postgres would reject the expression or produce [..., "x"]
#          vs "x" in the wrong place.
# ---------------------------------------------------------------------------

def test_push_appends_to_existing_array_and_creates_when_missing(conn, collection_name):
    from goldlapel.utils import doc_insert, doc_update, doc_find_one
    try:
        # Row 1: tags already exists — push should append.
        doc_insert(conn, collection_name, {"name": "alice", "tags": ["python"]})
        # Row 2: tags missing — push should create the array.
        doc_insert(conn, collection_name, {"name": "bob"})

        doc_update(conn, collection_name, {}, {"$push": {"tags": "rust"}})

        alice = doc_find_one(conn, collection_name, {"name": "alice"})
        bob = doc_find_one(conn, collection_name, {"name": "bob"})
        assert alice["data"]["tags"] == ["python", "rust"]
        assert bob["data"]["tags"] == ["rust"]
    finally:
        _drop(conn, collection_name)


# ---------------------------------------------------------------------------
# 3. $unset nested via doc_update_one — #- operator with text[] path cast
#    Mock test: TestFieldUpdateOperators::test_unset_nested
#    Risk: the "data #- %s::text[]" expression with param "{a,b}" is easy to
#          get wrong (e.g. quoting, brace syntax, cast placement). Only a real
#          DB proves the path-delete operator accepts it.
# ---------------------------------------------------------------------------

def test_unset_nested_removes_deep_key_only(conn, collection_name):
    from goldlapel.utils import doc_insert, doc_update_one, doc_find_one
    try:
        doc_insert(
            conn,
            collection_name,
            {"name": "alice", "profile": {"city": "NYC", "zip": "10001"}},
        )
        doc_update_one(
            conn,
            collection_name,
            {"name": "alice"},
            {"$unset": {"profile.zip": ""}},
        )
        alice = doc_find_one(conn, collection_name, {"name": "alice"})
        assert alice["data"]["profile"] == {"city": "NYC"}
        assert "zip" not in alice["data"]["profile"]
    finally:
        _drop(conn, collection_name)


# ---------------------------------------------------------------------------
# 4. $in filter — "data->>'field' IN (%s, %s)" text-cast semantics
#    Mock test: TestFilterOperators::test_in
#    Risk: Mock asserts "IN (%s, %s)" is in the string. But the left side is
#          "data->>'field'" (text) and the params are whatever Python type the
#          caller passed. If the wrapper ever coerces wrong, Postgres would
#          reject the comparison. Also proves ANY/ALL semantics match $in
#          intent when the target field is missing (must NOT match).
# ---------------------------------------------------------------------------

def test_find_in_matches_listed_values_only(conn, collection_name):
    from goldlapel.utils import doc_insert, doc_find
    try:
        doc_insert(conn, collection_name, {"name": "alice", "status": "active"})
        doc_insert(conn, collection_name, {"name": "bob", "status": "pending"})
        doc_insert(conn, collection_name, {"name": "carol", "status": "suspended"})
        doc_insert(conn, collection_name, {"name": "dave"})  # no status field

        results = doc_find(
            conn,
            collection_name,
            filter={"status": {"$in": ["active", "pending"]}},
        )
        names = sorted(r["data"]["name"] for r in results)
        assert names == ["alice", "bob"]
    finally:
        _drop(conn, collection_name)


# ---------------------------------------------------------------------------
# 5. Nested $or + $and — parenthesization semantics
#    Mock test: TestLogicalOperators::test_nested_or_and
#    Risk: "OR/AND/NOT are in the string" is a very weak assertion. Wrong
#          parenthesization (e.g. "a AND b OR c" vs "a AND (b OR c)") would
#          pass the mock and return wrong rows against Postgres.
# ---------------------------------------------------------------------------

def test_find_nested_or_and_respects_grouping(conn, collection_name):
    from goldlapel.utils import doc_insert, doc_find
    try:
        # We want to prove ($or [ $and[a,b], $not(c) ]) groups correctly.
        # Design rows so mis-grouping would yield a different set:
        doc_insert(conn, collection_name, {"name": "r1", "a": 1, "b": 2, "c": 3})   # AND arm matches (a=1 AND b=2)
        doc_insert(conn, collection_name, {"name": "r2", "a": 1, "b": 99, "c": 3})  # neither arm matches: AND fails (b!=2), NOT fails (c=3)
        doc_insert(conn, collection_name, {"name": "r3", "a": 1, "b": 99, "c": 9})  # NOT arm matches (c!=3)
        doc_insert(conn, collection_name, {"name": "r4", "a": 9, "b": 9, "c": 3})   # neither arm matches

        results = doc_find(
            conn,
            collection_name,
            filter={
                "$or": [
                    {"$and": [{"a": 1}, {"b": 2}]},
                    {"$not": {"c": 3}},
                ]
            },
        )
        names = sorted(r["data"]["name"] for r in results)
        assert names == ["r1", "r3"], f"expected grouping OR($and, $not), got {names}"
    finally:
        _drop(conn, collection_name)


# ---------------------------------------------------------------------------
# 6. sort + limit + skip pagination — stable ordering under real Postgres
#    Mock test: TestDocFind::test_with_sort, test_with_limit_and_skip
#    Risk: Mock asserts the SQL has ORDER BY/LIMIT/OFFSET. A real test proves
#          the ordering is actually what the caller asked for, and that
#          LIMIT + OFFSET compose correctly (not reversed, not double-applied).
# ---------------------------------------------------------------------------

def test_find_sort_limit_skip_returns_correct_window(conn, collection_name):
    from goldlapel.utils import doc_insert, doc_find
    try:
        for i in [5, 3, 1, 4, 2]:
            doc_insert(conn, collection_name, {"rank": i, "label": f"item-{i}"})

        page_1 = doc_find(conn, collection_name, sort={"rank": 1}, limit=2, skip=0)
        page_2 = doc_find(conn, collection_name, sort={"rank": 1}, limit=2, skip=2)
        page_3 = doc_find(conn, collection_name, sort={"rank": 1}, limit=2, skip=4)

        assert [r["data"]["rank"] for r in page_1] == [1, 2]
        assert [r["data"]["rank"] for r in page_2] == [3, 4]
        assert [r["data"]["rank"] for r in page_3] == [5]

        # Descending sort on the same data proves ASC/DESC is honored.
        desc_top = doc_find(conn, collection_name, sort={"rank": -1}, limit=3)
        assert [r["data"]["rank"] for r in desc_top] == [5, 4, 3]
    finally:
        _drop(conn, collection_name)


# ---------------------------------------------------------------------------
# 7. doc_find_one_and_update with $inc — CTE + jsonb_set + RETURNING
#    Mock test: TestDocFindOneAndUpdate::test_returns_document
#    Risk: Fragile multi-clause SQL. RETURNING must come from the UPDATE,
#          not the CTE. The returned row must reflect the POST-update state.
#          Mock-asserted strings can't catch "RETURNING returns pre-update
#          data" or "RETURNING returns the wrong row".
# ---------------------------------------------------------------------------

def test_find_one_and_update_returns_post_update_row(conn, collection_name):
    from goldlapel.utils import doc_insert, doc_find_one_and_update
    try:
        doc_insert(conn, collection_name, {"name": "alice", "score": 10})
        doc_insert(conn, collection_name, {"name": "alice", "score": 20})  # second alice — LIMIT 1 must pick exactly one

        returned = doc_find_one_and_update(
            conn,
            collection_name,
            {"name": "alice"},
            {"$inc": {"score": 100}},
        )
        assert returned is not None
        assert returned["data"]["name"] == "alice"
        # The returned score must be post-update (old + 100), not pre-update.
        assert returned["data"]["score"] in (110, 120), (
            f"expected post-update score (110 or 120), got {returned['data']['score']}"
        )
    finally:
        _drop(conn, collection_name)


# ---------------------------------------------------------------------------
# 8. doc_create_capped — trigger must actually fire and hold the cap
#    Mock test: TestDocCapped::test_capped_creates_trigger
#    Risk: The trigger function body is an f-string interpolating {collection}
#          and {max_int} into PL/pgSQL. A subtle error (wrong identifier
#          quoting, wrong column reference, wrong schema) would pass the
#          mock's substring match but raise at CREATE FUNCTION time or fail
#          silently at INSERT time. The only way to prove it is to insert
#          past the cap and observe the row count.
# ---------------------------------------------------------------------------

def test_capped_collection_trigger_enforces_cap(conn, collection_name):
    from goldlapel.utils import doc_create_capped, doc_insert, doc_count
    try:
        doc_create_capped(conn, collection_name, max_documents=3)

        for i in range(7):
            doc_insert(conn, collection_name, {"seq": i})
            # Ensure distinct created_at timestamps so the oldest-first DELETE
            # inside the trigger is deterministic. psycopg2 commits per call,
            # but NOW() can have microsecond resolution — add a tiny sleep.
            time.sleep(0.002)

        # Cap is 3; after 7 inserts, the trigger should have pruned back to <=3.
        assert doc_count(conn, collection_name) == 3

        # The surviving rows must be the 3 most recent (seq 4, 5, 6).
        from goldlapel.utils import doc_find
        survivors = sorted(r["data"]["seq"] for r in doc_find(conn, collection_name))
        assert survivors == [4, 5, 6]
    finally:
        _drop(conn, collection_name)


# ---------------------------------------------------------------------------
# 9. doc_distinct with filter — numeric-cast filter + DISTINCT projection
#    Mock test: TestDocDistinct::test_with_filter
#    Risk: The generated SQL is
#          SELECT DISTINCT data->>'status' FROM ... WHERE data->>'status' IS NOT NULL
#            AND (data->>'age')::numeric > %s
#          Mock asserts substrings. A real test proves:
#            (a) NULL/missing-field rows are excluded,
#            (b) the numeric cast in the filter doesn't crash on rows where
#                age is missing or non-numeric (it would in naive SQL),
#            (c) the distinct values returned are correct.
# ---------------------------------------------------------------------------

def test_distinct_with_numeric_filter_returns_unique_values(conn, collection_name):
    from goldlapel.utils import doc_insert, doc_distinct
    try:
        doc_insert(conn, collection_name, {"status": "active", "age": 30})
        doc_insert(conn, collection_name, {"status": "active", "age": 40})   # duplicate "active"
        doc_insert(conn, collection_name, {"status": "pending", "age": 50})
        doc_insert(conn, collection_name, {"status": "inactive", "age": 20}) # below filter threshold
        doc_insert(conn, collection_name, {"age": 99})                       # missing status — must be excluded by IS NOT NULL
        doc_insert(conn, collection_name, {"status": "ghost"})               # missing age — must not crash filter

        values = sorted(
            doc_distinct(
                conn,
                collection_name,
                "status",
                filter={"age": {"$gt": 25}},
            )
        )
        assert values == ["active", "pending"]
    finally:
        _drop(conn, collection_name)
