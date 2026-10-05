# goldlapel

[![Tests](https://github.com/goldlapel/goldlapel-python/actions/workflows/test.yml/badge.svg)](https://github.com/goldlapel/goldlapel-python/actions/workflows/test.yml)

The Python wrapper for [Gold Lapel](https://goldlapel.com) — a self-optimizing Postgres proxy that caches query results and creates indexes automatically. Zero code changes beyond the connection string.

The wrapper itself holds no cache. It:

- runs the proxy as a managed subprocess — finds the binary, starts and stops it with your app, turns your options into proxy flags, and hands back a driver-ready URL;
- adds Postgres-backed helpers (search, documents, streams, counters, sorted sets, hashes, queues, geo, pub/sub);
- plugs into Django and SQLAlchemy, pointing their connection settings at the proxy.

Every connection to the proxy — from the wrapper, your own driver, or an ORM — is cached the same way, by the proxy.

## Install

```bash
pip install goldlapel

# Plus any Postgres driver you like:
pip install psycopg2-binary   # sync, most common
pip install psycopg            # psycopg3 (sync or async)
pip install asyncpg            # async-only
```

## Quickstart

```python
import goldlapel
import psycopg2

# Spawn the proxy in front of your upstream DB
gl = goldlapel.start("postgresql://user:pass@localhost:5432/mydb")

# Point any Postgres driver at gl.url
conn = psycopg2.connect(gl.url)
cur = conn.cursor()
cur.execute("SELECT * FROM users WHERE id = %s", (42,))

gl.stop()  # (also cleaned up automatically on process exit)
```

Point your Postgres driver at `gl.url`. Gold Lapel sits between your app and your DB, caching query results (invalidated as writes land) and creating indexes for the query patterns it sees. `gl.conn` is a plain psycopg/psycopg2 connection to the proxy (an asyncpg connection under `goldlapel.asyncio`).

The proxy listens on two ports: the proxy itself (`proxy_port`, default 7932) and the dashboard (`dashboard_port`, default `proxy_port + 1`; `0` disables it).

Each `start()` for a different upstream spawns its own proxy. Without an explicit `proxy_port`, it takes the first pair that no other proxy in the process holds and nothing else on the machine is listening on: usually 7932 (dashboard 7933), then 7934 (dashboard 7935), and so on. An explicit `proxy_port` is used as given. If something else already holds it, the proxy refuses to start and the error says which port. Stopping a proxy frees its ports.

Calling `start()` again for an upstream that's already running (sync or `goldlapel.asyncio`, from any thread) shares that proxy. It keeps running until every `start()` has been matched by its `stop()`, so leaving one `with` block doesn't pull the proxy out from under other code. `goldlapel.stop(url)` stops it regardless.

`gl.url` leaves out the upstream's TLS settings (`sslmode`, `sslrootcert`, `channel_binding`, …). The proxy still uses them to reach your database, but your app talks to the proxy on localhost, which only accepts TLS when you give it `tls_cert` / `tls_key`. Unknown or removed options raise `TypeError`.

Async usage (`goldlapel.asyncio.start`), context managers, transactional coordination via `gl.using(conn)`, and framework integrations are in the docs.

## Documents and streams

Document store and stream operations live under nested namespaces:

```python
gl = goldlapel.start("postgresql://...")

# Documents — Mongo-style API over JSONB-backed tables.
gl.documents.insert("users", {"name": "alice", "age": 30})
alice = gl.documents.find_one("users", {"name": "alice"})
gl.documents.update("users", {"age": {"$gte": 30}}, {"$set": {"adult": True}})
count = gl.documents.count("users", {"adult": True})

# Streams — Kafka/Redis-streams-style append-only log with consumer groups.
gl.streams.add("events", {"type": "click", "user": "alice"})
gl.streams.create_group("events", "workers")
messages = gl.streams.read("events", "workers", "consumer-1", count=10)
for msg in messages:
    gl.streams.ack("events", "workers", msg["id"])
```

Tables are materialized server-side at `_goldlapel.doc_<name>` / `_goldlapel.stream_<name>` — Gold Lapel owns the schema so every wrapper produces byte-identical tables. You don't run `CREATE TABLE` for these helpers anymore; the proxy does, idempotently, on first use.

Counters, sorted sets, hashes, queues and geo live under `gl.counters`, `gl.zsets`, `gl.hashes`, `gl.queues` and `gl.geos`. Search, percolator and pub/sub (`gl.search`, `gl.percolate`, `gl.publish` / `gl.subscribe`, …) remain at the top level for now.

## Authentication

For paid customers, paste your API key once and Gold Lapel handles the rest — fetching and auto-renewing the underlying license against entitlement changes:

```python
gl = goldlapel.start(
    "postgresql://user:pass@localhost:5432/mydb",
    api_key="gl_live_...",   # from https://manor.goldlapel.com/account
)
```

You can also set the env var `GOLDLAPEL_API_KEY` and skip the kwarg.

If you'd rather hand-place a license PEM (e.g., for fully offline hosts), `license="/path/to/license.key"` still works and serves as the offline fallback when both are set.

Trial customers don't need anything — Gold Lapel registers an anonymous trial automatically on first run.

## Dashboard

Gold Lapel exposes a live dashboard at `gl.dashboard_url`:

```python
print(gl.dashboard_url)
# -> http://127.0.0.1:7933
```

## Documentation

Full API reference, async usage, configuration, framework integrations (Django, SQLAlchemy, FastAPI), upgrading from v0.1, and production deployment: https://goldlapel.com/docs/python

## Uninstalling

Before removing the package, drop Gold Lapel's helper schema and the indexes it created from your Postgres:

```bash
goldlapel clean
```

Then remove the package and any local state:

```bash
pip uninstall goldlapel
rm -rf ~/.goldlapel
rm -f goldlapel.toml     # only if you wrote one
```

Cancelling your subscription does not delete your data — only Gold Lapel's helper schema and indexes go away.

## License

MIT. See `LICENSE`.
