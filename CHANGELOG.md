# Changelog

## Unreleased

### Breaking changes

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

Other namespaces (`gl.search`, `gl.cache`, `gl.publish` / `gl.subscribe`,
`gl.incr`, `gl.zadd`, `gl.hset`, `gl.geoadd`, …) remain flat and will
migrate to nested form in subsequent releases (one namespace per
schema-to-core phase).

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
