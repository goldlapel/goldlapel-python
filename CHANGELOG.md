# Changelog

## Unreleased

### Fixed — multiple upstreams no longer collide on ports

Each proxy uses two ports, proxy (`P`) and dashboard (`P + 1`), but
auto-assignment stepped by one, so a second upstream got 7933 — the first
proxy's dashboard port. Auto-assigned ports now skip every port a live proxy
in this process holds (its proxy port, plus its dashboard port unless that is
disabled with `0`): two upstreams get 7932/7933 and 7934/7935. The first proxy
still gets 7932, an explicit `proxy_port` is used as given, and stopping a
proxy frees its ports for the next one. Sync and `goldlapel.asyncio` share the
same allocation.

- Django: the backend only passes `proxy_port` when `OPTIONS["goldlapel"]`
  sets one, and points Django at the port the proxy actually got — two
  `DATABASES` entries without a port previously both asked for 7932.
- `goldlapel.asyncio`: `gl.stop()` now releases the proxy's registry entry
  (and ports), as the sync `stop()` already did. The startup banner no
  longer prints a dashboard URL when `dashboard_port=0`.
- The stale-proxy cleanup before spawn only signals a `goldlapel` process
  listening on the target port. It passed `lsof` selectors without `-a`,
  which ORs them: it also matched a non-Gold-Lapel process holding that port
  and every `goldlapel` process on the machine, including this process's
  other proxies.

### Fixed — several databases from frameworks, async reuse, explicit ports

- SQLAlchemy: a second `create_engine` / `create_async_engine` for another
  database raised "Multiple Gold Lapel instances are running". Each engine
  now connects to the URL of the proxy started for its own database.
- Django and SQLAlchemy accept every option `goldlapel.start` takes —
  newly `api_key`, `client`, `mesh`, `mesh_tag`, `disable_proxy_cache`,
  `disable_sqloptimize` and `disable_auto_indexes` (plus `license`,
  `config_file` and `silent` for SQLAlchemy) — under the same names: in
  `OPTIONS["goldlapel"]` for Django, as `goldlapel_<name>` engine kwargs or
  plain `init()` kwargs for SQLAlchemy. Only options you set are passed on.
- `goldlapel.asyncio.start` on an upstream whose proxy is already running
  returned an object without `documents`, `streams`, `counters`, `zsets`,
  `hashes`, `queues` or `geos`. It now returns a complete `AsyncGoldLapel`
  on the running proxy, the same as a fresh start.
- `goldlapel.asyncio.start(api_key=...)` never handed the key to the proxy;
  it now sets `GOLDLAPEL_API_KEY` for it, as the sync path does.
- An explicit `proxy_port` or `dashboard_port` that one of this process's
  running proxies already holds for a different upstream now raises a
  `RuntimeError` naming the port and that upstream (password masked).
  Previously the stale-proxy cleanup could kill that proxy, or the new
  upstream's connections went to the other upstream's proxy. In Django the
  error is logged and the connection falls back to the database directly,
  like any other proxy start failure.

### Breaking changes — the in-process cache (L1) is gone

**The wrapper no longer caches anything itself.** The proxy's result cache
now caches every connection the same way — the wrapper's, your own
driver's, or an ORM's — so the wrapper-side cache, and the invalidation
socket that kept it fresh, have been removed. The proxy no longer serves
the invalidation port; it uses two ports: proxy (`proxy_port`) and
dashboard (`proxy_port + 1`).

- `gl.conn`, `goldlapel.connect()` and the `goldlapel.asyncio` internal
  connection are now the plain driver connection (psycopg / psycopg2 /
  asyncpg), not a caching wrapper around it.
- Removed: `goldlapel.wrap()`, `goldlapel.NativeCache`, the
  `CachedConnection` / `CachedCursor` / `AsyncCachedConnection` classes,
  and `goldlapel.sqlalchemy.wrap` / `goldlapel.sqlalchemy.NativeCache`.
- Removed options from `goldlapel.start`, `goldlapel.asyncio.start` and
  `GoldLapel(...)`: `invalidation_port`, `disable_native_cache`,
  `aggressive_verify`, `disable_matviews` (the proxy no longer builds
  materialized views). The `invalidation_port` property is gone too.
- Removed keys from the `config` map, all materialized-view tuning the
  proxy no longer has: `refresh_interval_secs`, `pattern_ttl_secs`,
  `max_tables_per_view`, `max_columns_per_view`, `disable_consolidation`,
  `disable_rewrite`, `disable_shadow_mode`. `enable_coalescing` (never a
  proxy flag) is replaced by `disable_coalescing`, which is.
- Removed env vars: `GOLDLAPEL_NATIVE_CACHE`, `GOLDLAPEL_NATIVE_CACHE_SIZE`,
  `GOLDLAPEL_REPORT_STATS`, `GOLDLAPEL_INVALIDATION_PORT`.
- Django: the `invalidation_port` and `aggressive_verify` keys in
  `OPTIONS["goldlapel"]` are no longer accepted; the backend still starts
  the proxy and points Django's connection at it.
- SQLAlchemy: `goldlapel_invalidation_port`, `goldlapel_native_cache` and
  `goldlapel_aggressive_verify` engine kwargs are removed, and
  `create_engine` no longer installs its own connection `creator` —
  SQLAlchemy connects to the proxy URL with the driver named in your URL.
  `init()` drops its `invalidation_port` argument and no longer sets
  `GOLDLAPEL_INVALIDATION_PORT`.

No aliases: passing any removed option raises `TypeError` (or `ValueError`
for removed `config` keys).

Connections are still tagged `application_name=goldlapel:python:<version>`
so they're recognisable in `pg_stat_activity`; the proxy doesn't treat them
differently.

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
