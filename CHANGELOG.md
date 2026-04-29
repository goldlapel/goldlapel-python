# Changelog

## Unreleased

### Breaking changes (Phase 5 — counter / zset / hash / queue / geo)

**The five Redis-compat helper families moved to nested namespaces, and the
proxy now owns their DDL.** The flat `gl.incr`, `gl.zadd`, `gl.hset`,
`gl.enqueue`, `gl.geoadd`, etc. methods are gone. Operations now live under:

| Old (flat)                                  | New (nested)                                 |
| ------------------------------------------- | -------------------------------------------- |
| `gl.incr(name, key)`                        | `gl.counters.incr(name, key)`                |
| `gl.get_counter(name, key)`                 | `gl.counters.get(name, key)`                 |
| `gl.hset(name, key, field, value)`          | `gl.hashes.set(name, key, field, value)`     |
| `gl.hget(name, key, field)`                 | `gl.hashes.get(name, key, field)`            |
| `gl.hgetall(name, key)`                     | `gl.hashes.get_all(name, key)`               |
| `gl.hdel(name, key, field)`                 | `gl.hashes.delete(name, key, field)`         |
| `gl.zadd(name, member, score)`              | `gl.zsets.add(name, zset_key, member, score)`|
| `gl.zincrby(name, member, amount)`          | `gl.zsets.incr_by(name, zset_key, member, d)`|
| `gl.zrange(name, start, stop, desc)`        | `gl.zsets.range(name, zset_key, start, stop)`|
| `gl.zscore(name, member)`                   | `gl.zsets.score(name, zset_key, member)`     |
| `gl.zrank(name, member, desc)`              | `gl.zsets.rank(name, zset_key, member, desc)`|
| `gl.zrem(name, member)`                     | `gl.zsets.remove(name, zset_key, member)`    |
| `gl.enqueue(table, payload)`                | `gl.queues.enqueue(name, payload)`           |
| `gl.dequeue(table)` — DELETED, NO ALIAS     | `gl.queues.claim(name)` then `.ack(id)`      |
| `gl.geoadd(table, name_col, geom_col, ...)` | `gl.geos.add(name, member, lon, lat)`        |
| `gl.geodist(table, geom_col, ...)`          | `gl.geos.dist(name, m1, m2, unit='m')`       |
| `gl.georadius(table, geom_col, lon, lat, r)`| `gl.geos.radius(name, lon, lat, r, unit='m')`|

**Schema breaking changes (canonical v1 schemas owned by the proxy):**

- **counter**: gains `updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()` —
  stamped on every UPDATE/UPSERT. Operators see "when did this counter last
  move?" via `\d+`.
- **zset**: NEW `zset_key TEXT` column makes the table a *namespace*; many
  sorted sets live under one table. Every `gl.zsets.<verb>` call takes
  `zset_key` as the first arg after `name`. Matches Redis ZADD semantics.
- **hash**: storage flipped from "JSONB blob per key" to row-per-field
  (`hash_key`, `field`, `value`). Concurrent HSET on different fields no
  longer contends on the same row; `HDEL` is a single-row DELETE; `HKEYS` /
  `HLEN` are direct queries (no JSONB extraction).
- **queue**: at-least-once with visibility timeout, NOT fire-and-forget.
  `enqueue` + `claim` + `ack` is the new contract. **No `dequeue` compat
  shim** — that was a deliberate decision; see `gl.queues.abandon` for
  explicit retry. A consumer that crashes leaves its lease pending; the
  message becomes visible again at `visible_at` and is redelivered.
- **geo**: column type is `GEOGRAPHY(POINT, 4326)` (was `GEOMETRY(Point,
  4326)`); member is the primary key (was `BIGSERIAL` + free-form `name`);
  re-adding a member is idempotent (Redis GEOADD semantics). Distance
  returns are meters-native — `gl.geos.dist(unit='km'|'mi'|'ft')` converts
  at the wrapper edge.

**Wrapper now uses proxy-owned DDL.** Wrappers no longer emit `CREATE TABLE
IF NOT EXISTS` for any of these families. Each call to `gl.<family>.<verb>`
fetches `(tables, query_patterns)` from `POST /api/ddl/<family>/create`
(idempotent), caches per-session, and executes the proxy's canonical
patterns. One HTTP round-trip per (family, name) per session.

### Breaking changes (Phase 4 — doc-store and streams)

**Doc-store and stream methods moved under nested namespaces.** The flat
`gl.doc_*` and `gl.stream_*` methods are gone; document and stream operations
now live under `gl.documents.<verb>` and `gl.streams.<verb>`. No
backwards-compat aliases — search and replace once.

Migration map:

| Old (flat)                           | New (nested)                              |
| ------------------------------------ | ----------------------------------------- |
| `gl.doc_insert(name, doc)`           | `gl.documents.insert(name, doc)`          |
| `gl.doc_insert_many(name, docs)`     | `gl.documents.insert_many(name, docs)`    |
| `gl.doc_find(name, filter)`          | `gl.documents.find(name, filter)`         |
| `gl.doc_find_one(name, filter)`      | `gl.documents.find_one(name, filter)`     |
| `gl.doc_find_cursor(name, ...)`      | `gl.documents.find_cursor(name, ...)`     |
| `gl.doc_update(name, f, u)`          | `gl.documents.update(name, f, u)`         |
| `gl.doc_update_one(name, f, u)`      | `gl.documents.update_one(name, f, u)`     |
| `gl.doc_delete(name, f)`             | `gl.documents.delete(name, f)`            |
| `gl.doc_delete_one(name, f)`         | `gl.documents.delete_one(name, f)`        |
| `gl.doc_find_one_and_update(...)`    | `gl.documents.find_one_and_update(...)`   |
| `gl.doc_find_one_and_delete(...)`    | `gl.documents.find_one_and_delete(...)`   |
| `gl.doc_distinct(name, field, f)`    | `gl.documents.distinct(name, field, f)`   |
| `gl.doc_count(name, filter)`         | `gl.documents.count(name, filter)`        |
| `gl.doc_create_index(name, keys)`    | `gl.documents.create_index(name, keys)`   |
| `gl.doc_aggregate(name, pipeline)`   | `gl.documents.aggregate(name, pipeline)`  |
| `gl.doc_watch(name, cb)`             | `gl.documents.watch(name, cb)`            |
| `gl.doc_unwatch(name)`               | `gl.documents.unwatch(name)`              |
| `gl.doc_create_ttl_index(name, n)`   | `gl.documents.create_ttl_index(name, n)`  |
| `gl.doc_remove_ttl_index(name)`      | `gl.documents.remove_ttl_index(name)`     |
| `gl.doc_create_capped(name, max)`    | `gl.documents.create_capped(name, max)`   |
| `gl.doc_remove_cap(name)`            | `gl.documents.remove_cap(name)`           |
| `gl.doc_create_collection(name, ...)`| `gl.documents.create_collection(name, ...)` |
| `gl.stream_add(name, payload)`       | `gl.streams.add(name, payload)`           |
| `gl.stream_create_group(name, group)`| `gl.streams.create_group(name, group)`    |
| `gl.stream_read(name, g, c, count)`  | `gl.streams.read(name, g, c, count)`      |
| `gl.stream_ack(name, group, id)`     | `gl.streams.ack(name, group, id)`         |
| `gl.stream_claim(name, g, c, ...)`   | `gl.streams.claim(name, g, c, ...)`       |

As of Phase 5, the seven helper-table families are all nested:
`gl.documents`, `gl.streams`, `gl.counters`, `gl.zsets`, `gl.hashes`,
`gl.queues`, `gl.geos`. Search, cache, and pub/sub remain flat (they don't
own helper tables — they read/write user-managed schema or use
`pg_notify`). They'll migrate to nested form if/when their own
schema-to-core phase fires.

**Doc-store DDL is now owned by the proxy.** The wrapper no longer emits
`CREATE TABLE _goldlapel.doc_<name>` SQL when a collection is first used.
Instead, `gl.documents.<verb>` calls `POST /api/ddl/doc_store/create`
against the proxy's dashboard port; the proxy runs the canonical DDL on its
management connection and returns the table reference + query patterns. The
wrapper caches `(tables, query_patterns)` per session — one HTTP round-trip
per (family, name) per session.

Canonical doc-store schema (v1) standardizes the column shape across every
Gold Lapel wrapper:

```
_id        UUID PRIMARY KEY DEFAULT gen_random_uuid()
data       JSONB NOT NULL
created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
```

Both timestamps are `NOT NULL` — kills the `created_at NOT NULL` /
`updated_at` drift surfaced in the v0.2 cross-wrapper compat audit. Any
wrapper (Python, JS, Ruby, Java, PHP, Go, .NET) writing to a doc-store
collection now produces the same table.

**Upgrade path for dev databases:** wipe and recreate. There is no
in-place migration. Pre-1.0, dev databases get rebuilt freely.

```bash
goldlapel clean   # drops _goldlapel.* tables
# ...drop/recreate your DB if needed...
```

If you have a v0.2-pre wrapper running against a v0.2-post proxy, the
wrapper's first `gl.documents.<verb>` call surfaces a clear `version_mismatch`
error pointing to this CHANGELOG.
