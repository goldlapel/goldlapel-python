"""Documents behave as MongoDB documents them, through the proxy with its
cache on, in both write-detection modes (WAL, and write triggers).

Each expectation is MongoDB's documented behaviour:
- query operators: https://www.mongodb.com/docs/manual/reference/operator/query/
- arrays: https://www.mongodb.com/docs/manual/tutorial/query-arrays/
- null and missing fields: https://www.mongodb.com/docs/manual/tutorial/query-for-null-fields/
- sort order across types: https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/
- update operators: https://www.mongodb.com/docs/manual/reference/operator/update/

Known differences, by design (see `goldlapel.utils`): an array nested in an
array is searched too; objects compare without regard to key order.
"""

import time

import pytest

from _integration_gate import require_integration_upstream

pytestmark = pytest.mark.integration

DOCS = [
    {"name": "a", "qty": 5, "tags": ["x", "y"], "size": {"h": 10, "w": 2}, "active": True, "note": None},
    {"name": "b", "qty": 15, "tags": ["y"], "size": {"h": 20}, "active": False},
    {"name": "c", "qty": "15", "tags": [], "items": [{"sku": "p", "n": 2}, {"sku": "q", "n": 7}]},
    {"name": "d", "items": [{"sku": "p", "n": 9}], "size": {"h": 10, "w": 2}},
    {"name": "E", "qty": 2.5, "tags": ["z"], "active": True},
]


@pytest.fixture(scope="module", params=["wal", "triggers"])
def gl(request):
    import goldlapel
    url = require_integration_upstream()
    extra = [] if request.param == "wal" else ["--logical-decoding=false"]
    port = 7860 + (int(time.time()) % 40) + (0 if request.param == "wal" else 50)
    inst = goldlapel.start(url, proxy_port=port, extra_args=extra)
    yield inst
    inst.stop()


@pytest.fixture
def coll(gl):
    name = f"sem_{int(time.time() * 1000000) % 10**12}"
    gl.documents.create_collection(name)
    gl.documents.insert_many(name, DOCS)
    yield name
    gl.documents.delete(name, {})


def names(gl, coll, filter=None, **kw):
    """The names of the documents found, each find run twice (the second is
    served from cache once it's watched)."""
    first = [d["data"]["name"] for d in gl.documents.find(coll, filter, **kw)]
    again = [d["data"]["name"] for d in gl.documents.find(coll, filter, **kw)]
    assert first == again
    return first if "sort" in kw else sorted(first)


QUERIES = [
    # A number matches numbers only, not the string "15"
    ({"qty": 15}, ["b"]),
    ({"qty": {"$gt": 4}}, ["a", "b"]),
    ({"qty": {"$gte": 2.5, "$lt": 6}}, ["E", "a"]),
    ({"qty": {"$gt": "1"}}, ["c"]),
    # An array field matches an element, or the whole array exactly
    ({"tags": "x"}, ["a"]),
    ({"tags": ["y"]}, ["b"]),
    ({"tags": ["y", "x"]}, []),
    ({"active": True}, ["E", "a"]),
    # $ne includes documents without the field
    ({"active": {"$ne": True}}, ["b", "c", "d"]),
    # Null matches null and missing
    ({"note": None}, ["E", "a", "b", "c", "d"]),
    ({"note": {"$ne": None}}, []),
    ({"note": {"$exists": True}}, ["a"]),
    ({"size.w": {"$exists": True}}, ["a", "d"]),
    ({"size.w": {"$exists": False}}, ["E", "b", "c"]),
    ({"size.h": 10}, ["a", "d"]),
    # An embedded document matches exactly, not as a subset
    ({"size": {"h": 10, "w": 2}}, ["a", "d"]),
    ({"size": {"h": 20}}, ["b"]),
    ({"size": {"h": 10}}, []),
    # A path through an array searches its elements
    ({"items.sku": "q"}, ["c"]),
    ({"items.n": {"$gt": 5}}, ["c", "d"]),
    # $elemMatch: one element meets every condition
    ({"items": {"$elemMatch": {"sku": "p", "n": {"$gt": 5}}}}, ["d"]),
    ({"qty": {"$in": [5, "15"]}}, ["a", "c"]),
    ({"qty": {"$in": []}}, []),
    ({"qty": {"$nin": [5, 15]}}, ["E", "c", "d"]),
    ({"active": {"$in": [False, None]}}, ["b", "c", "d"]),
    ({"tags": {"$size": 2}}, ["a"]),
    ({"tags": {"$all": ["y", "x"]}}, ["a"]),
    ({"name": {"$regex": "^e$", "$options": "i"}}, ["E"]),
    ({"name": {"$not": {"$regex": "^[a-c]$"}}}, ["E", "d"]),
    ({"$or": [{"qty": 5}, {"name": "d"}]}, ["a", "d"]),
    ({"$nor": [{"qty": 5}, {"name": "d"}]}, ["E", "b", "c"]),
    ({"$and": [{"tags": "y"}, {"qty": {"$lt": 10}}]}, ["a"]),
]


@pytest.mark.parametrize("filter,expected", QUERIES, ids=[str(q) for q, _ in QUERIES])
def test_query(gl, coll, filter, expected):
    assert names(gl, coll, filter) == expected


def test_by_id(gl, coll):
    a = gl.documents.find_one(coll, {"name": "a"})
    assert names(gl, coll, {"_id": str(a["_id"])}) == ["a"]
    assert names(gl, coll, {"_id": {"$nin": [str(a["_id"])]}}) == ["E", "b", "c", "d"]


def test_sort_follows_mongodbs_type_order(gl, coll):
    # Missing (as null), then numbers by value, then strings
    assert names(gl, coll, sort={"qty": 1}) == ["d", "E", "a", "b", "c"]
    assert names(gl, coll, sort={"qty": -1}) == ["c", "b", "a", "E", "d"]
    # Strings by code point: "E" before "a"
    assert names(gl, coll, sort={"name": 1}) == ["E", "a", "b", "c", "d"]
    assert names(gl, coll, sort={"size.h": -1, "name": 1}) == ["b", "a", "d", "E", "c"]


def test_distinct_unwinds_arrays_and_keeps_types(gl, coll):
    assert sorted(gl.documents.distinct(coll, "tags")) == ["x", "y", "z"]
    assert sorted(map(repr, gl.documents.distinct(coll, "qty"))) == sorted(map(repr, [5, 15, "15", 2.5]))


def test_create_index_on_a_dotted_key(gl, coll):
    gl.documents.create_index(coll, ["size.h"])
    assert names(gl, coll, sort={"size.h": 1, "name": 1}) == ["E", "c", "a", "d", "b"]


def doc(gl, coll, name):
    return gl.documents.find_one(coll, {"name": name})["data"]


def test_set_on_a_dotted_path_makes_its_parents(gl, coll):
    gl.documents.update_one(coll, {"name": "b"}, {"$set": {"size.d": 3, "new.deep.x": 1}})
    b = doc(gl, coll, "b")
    assert b["size"] == {"h": 20, "d": 3}
    assert b["new"] == {"deep": {"x": 1}}


def test_operators_build_on_each_other(gl, coll):
    gl.documents.update_one(coll, {"name": "b"}, {"$inc": {"qty": 1}, "$mul": {"qty": 2}})
    assert doc(gl, coll, "b")["qty"] == 32
    gl.documents.update_one(coll, {"name": "b"}, {"$inc": {"stats.views": 1}})
    assert doc(gl, coll, "b")["stats"] == {"views": 1}


def test_rename_of_a_missing_field_changes_nothing(gl, coll):
    before = doc(gl, coll, "a")
    gl.documents.update_one(coll, {"name": "a"}, {"$rename": {"nope": "other"}})
    assert doc(gl, coll, "a") == before
    gl.documents.update_one(coll, {"name": "a"}, {"$rename": {"size.h": "height"}})
    a = doc(gl, coll, "a")
    assert a["height"] == 10 and a["size"] == {"w": 2}


def test_push_adds_one_element_or_each(gl, coll):
    gl.documents.update_one(coll, {"name": "b"}, {"$push": {"tags": ["p", "q"]}})
    assert doc(gl, coll, "b")["tags"] == ["y", ["p", "q"]]
    gl.documents.update_one(coll, {"name": "b"}, {"$push": {"tags": {"$each": ["r", "s"]}}})
    assert doc(gl, coll, "b")["tags"] == ["y", ["p", "q"], "r", "s"]
    gl.documents.update_one(coll, {"name": "d"}, {"$push": {"log": 1}})
    assert doc(gl, coll, "d")["log"] == [1]


def test_add_to_set_never_duplicates(gl, coll):
    for _ in range(2):
        gl.documents.update_one(coll, {"name": "d"}, {"$addToSet": {"items": {"sku": "p", "n": 9}}})
        gl.documents.update_one(coll, {"name": "d"}, {"$addToSet": {"items": {"$each": [{"sku": "z"}, {"sku": "z"}]}}})
    assert doc(gl, coll, "d")["items"] == [{"sku": "p", "n": 9}, {"sku": "z"}]


def test_pull(gl, coll):
    gl.documents.update_one(coll, {"name": "a"}, {"$pull": {"tags": "y"}})
    assert doc(gl, coll, "a")["tags"] == ["x"]
    gl.documents.update_one(coll, {"name": "c"}, {"$pull": {"items": {"n": {"$gt": 5}}}})
    assert doc(gl, coll, "c")["items"] == [{"sku": "p", "n": 2}]
    gl.documents.update_one(coll, {"name": "c"}, {"$pull": {"items": {"sku": "p"}}})
    assert doc(gl, coll, "c")["items"] == []
    gl.documents.update_one(coll, {"name": "b"}, {"$pull": {"missing": 1}})
    assert "missing" not in doc(gl, coll, "b")


def test_unset(gl, coll):
    gl.documents.update_one(coll, {"name": "a"}, {"$unset": {"size.w": "", "note": ""}})
    a = doc(gl, coll, "a")
    assert a["size"] == {"h": 10} and "note" not in a


def test_a_document_without_operators_replaces(gl, coll):
    gl.documents.update_one(coll, {"name": "a"}, {"name": "a", "only": 1})
    assert doc(gl, coll, "a") == {"name": "a", "only": 1}


def test_updates_set_updated_at(gl, coll):
    before = gl.documents.find_one(coll, {"name": "b"})
    conn = gl.conn
    cur = conn.cursor()
    table = f"_goldlapel.doc_{coll}"
    cur.execute(f"SELECT updated_at FROM {table} WHERE _id = %s", (before["_id"],))
    t0 = cur.fetchone()[0]
    conn.commit()
    gl.documents.update_one(coll, {"name": "b"}, {"$set": {"x": 1}})
    cur.execute(f"SELECT updated_at FROM {table} WHERE _id = %s", (before["_id"],))
    assert cur.fetchone()[0] > t0
    conn.commit()
    cur.close()


@pytest.mark.parametrize("update", [
    {"$set": {"x": 1}, "y": 2},
    {"$setOnWhatever": {"x": 1}},
    {},
    {"$inc": {"qty": "1"}},
])
def test_bad_updates_raise(gl, coll, update):
    with pytest.raises(ValueError):
        gl.documents.update_one(coll, {"name": "a"}, update)


def test_a_write_made_elsewhere_is_seen(gl, coll):
    import psycopg2
    assert names(gl, coll, {"qty": 5}) == ["a"]
    direct = psycopg2.connect(require_integration_upstream())
    cur = direct.cursor()
    cur.execute(f"UPDATE _goldlapel.doc_{coll} SET data = jsonb_set(data, '{{qty}}', '6') WHERE data->>'name' = 'a'")
    direct.commit()
    direct.close()
    assert names(gl, coll, {"qty": 5}) == []
    assert names(gl, coll, {"qty": 6}) == ["a"]


def test_pipeline_stages_out_of_order_raise(gl, coll):
    with pytest.raises(ValueError):
        gl.documents.aggregate(coll, [{"$group": {"_id": "$name"}}, {"$match": {"qty": 5}}])
    with pytest.raises(ValueError):
        gl.documents.aggregate(coll, [{"$limit": 5}, {"$skip": 2}])


@pytest.mark.asyncio
async def test_the_same_through_asyncpg():
    """Every query above, an update of each kind and a sorted find, through
    the asyncpg path: its parameters are typed differently."""
    pytest.importorskip("asyncpg")
    from goldlapel.asyncio import start
    url = require_integration_upstream()
    async with start(url, proxy_port=7830 + int(time.time()) % 20) as gl:
        coll = f"sema_{int(time.time() * 1000000) % 10**12}"
        await gl.documents.create_collection(coll)
        await gl.documents.insert_many(coll, DOCS)

        async def found(filter=None, **kw):
            docs = await gl.documents.find(coll, filter, **kw)
            got = [d["data"]["name"] for d in docs]
            return got if "sort" in kw else sorted(got)

        for filter, expected in QUERIES:
            assert await found(filter) == expected, filter
        a = await gl.documents.find_one(coll, {"name": "a"})
        assert await found({"_id": {"$in": [str(a["_id"])]}}) == ["a"]
        assert await found(sort={"qty": 1}) == ["d", "E", "a", "b", "c"]
        assert sorted(await gl.documents.distinct(coll, "tags")) == ["x", "y", "z"]
        await gl.documents.update_one(coll, {"name": "b"}, {
            "$set": {"size.d": 3}, "$inc": {"qty": 1}, "$push": {"tags": {"$each": ["p"]}},
            "$addToSet": {"tags": "y"}, "$pull": {"missing": 1}, "$rename": {"active": "on"},
        })
        b = (await gl.documents.find_one(coll, {"name": "b"}))["data"]
        assert b["size"] == {"h": 20, "d": 3} and b["qty"] == 16 and b["tags"] == ["y", "p"] and b["on"] is False
        await gl.documents.update_one(coll, {"name": "c"}, {"$pull": {"items": {"n": {"$gt": 5}}}})
        assert (await gl.documents.find_one(coll, {"name": "c"}))["data"]["items"] == [{"sku": "p", "n": 2}]
        await gl.documents.create_index(coll, ["size.h"])
        await gl.documents.delete(coll, {})
