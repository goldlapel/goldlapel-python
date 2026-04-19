# Gold Lapel

Self-optimizing Postgres proxy — automatic materialized views and indexes, with an L1 native cache that serves repeated reads in microseconds. Zero code changes required.

Gold Lapel sits between your app and Postgres, watches query patterns, and automatically creates materialized views and indexes to make your database faster. Port 7932 (79 = atomic number for gold, 32 from Postgres).

## Install

```bash
pip install goldlapel

# You also need a Postgres driver — any of these works:
pip install psycopg2-binary   # most common
pip install psycopg            # psycopg3 (newer)
pip install asyncpg            # async Python apps (used alongside one of the above)
```

## Quick start — sync

```python
import goldlapel
import psycopg2

# Spawn the proxy in front of your upstream DB, get back a GoldLapel instance
gl = goldlapel.start("postgresql://user:pass@localhost:5432/mydb")

# Use gl.url with any Postgres driver for raw SQL
conn = psycopg2.connect(gl.url)
cursor = conn.cursor()
cursor.execute("SELECT * FROM users WHERE id = %s", (42,))

# Or use Gold Lapel's wrapper methods directly — no conn arg needed
hits = gl.search("articles", "body", "postgres tuning")
gl.doc_insert("events", {"type": "signup", "user": "steve"})

# Clean up (happens automatically on process exit too)
gl.stop()
```

### Context manager

```python
with goldlapel.start("postgresql://...") as gl:
    results = gl.search("articles", "body", "query")
# proxy stopped automatically on exit
```

## Quick start — async

```python
from goldlapel.asyncio import start
import asyncpg

gl = await start("postgresql://user:pass@localhost:5432/mydb")

conn = await asyncpg.connect(gl.url)
rows = await conn.fetch("SELECT * FROM users WHERE id = $1", 42)

hits = await gl.search("articles", "body", "postgres tuning")
await gl.doc_insert("events", {"type": "signup"})

await gl.stop()
```

### Async context manager

```python
from goldlapel.asyncio import start

async with start("postgresql://...") as gl:
    hits = await gl.search("articles", "body", "query")
```

## Transactional coordination

When you want wrapper methods to run inside your own transaction, pass your
connection via `gl.using(conn)` (scoped) or the `conn=` kwarg (per call):

```python
import psycopg2
gl = goldlapel.start("postgresql://...")
conn = psycopg2.connect(gl.url)
conn.autocommit = False
cur = conn.cursor()

# Scoped: all wrapper methods inside this block use `conn`
with gl.using(conn):
    cur.execute("INSERT INTO orders (total) VALUES (%s)", (99,))
    gl.doc_insert("events", {"type": "order.created"})
    cur.execute("UPDATE inventory SET qty = qty - 1")
    conn.commit()

# Or per-call
gl.doc_insert("events", {"type": "x"}, conn=conn)
```

Async has the same shape with `async with gl.using(conn): ...` and `conn=` kwarg.

## Multiple proxies in one process

`goldlapel.start()` is a factory — each URL gets its own instance:

```python
gl_primary = goldlapel.start("postgresql://primary/mydb")
gl_replica = goldlapel.start("postgresql://replica/mydb")

gl_primary.search(...)  # hits primary
gl_replica.search(...)  # hits replica
```

## Configuration

Pass a config dict at `start()`:

```python
gl = goldlapel.start("postgresql://user:pass@localhost/mydb", config={
    "mode": "waiter",
    "pool_size": 50,
    "disable_matviews": True,
    "replica": ["postgresql://user:pass@replica1/mydb"],
    "log_level": "info",  # trace | debug | info | warn | error
})
```

Keys use `snake_case` and map directly to CLI flags. To see all valid keys:

```python
import goldlapel
print(goldlapel.config_keys())
```

Full configuration reference in the [main documentation](https://github.com/goldlapel/goldlapel#setting-reference).

You can also set environment variables (`GOLDLAPEL_PROXY_PORT`, `GOLDLAPEL_UPSTREAM`, etc.) — the binary reads them automatically.

## Framework / ORM integrations

- **Django** — integration code is included in the `goldlapel` package. Install Django separately: `pip install django`.
- **SQLAlchemy** — integration code is included. Install separately: `pip install sqlalchemy`.
- **FastAPI / Starlette / etc.** — use `goldlapel.asyncio.start()` and any async driver.

## How it works

This package bundles the Gold Lapel Rust binary for your platform. When you call `start()`, it:

1. Locates the binary (bundled, on PATH, or via `GOLDLAPEL_BINARY` env var)
2. Spawns it as a subprocess listening on localhost
3. Waits for the port to be ready
4. Opens the wrapper's internal DB connection (eagerly, so wrapper methods are fast)
5. Cleans up automatically on process exit

The binary does all the optimization work — this wrapper just manages its lifecycle and exposes convenience methods.

## Upgrading from v0.1.x

v0.2.0 is a breaking change. Summary:

- `goldlapel.start(url)` now returns a `GoldLapel` instance (previously returned a wrapped connection).
  - For raw SQL: use `psycopg2.connect(gl.url)` (or equivalent).
  - For wrapper methods: call `gl.search(...)`, `gl.doc_insert(...)`, etc. directly on the instance.
- `goldlapel.start_async` moved to `goldlapel.asyncio.start`.
- Optional dependencies `[django]` and `[sqlalchemy]` removed — install those packages directly.
- All wrapper methods now accept an optional `conn=` kwarg, and `gl.using(conn)` provides scoped override.

## Links

- [Website](https://goldlapel.com)
- [Documentation](https://github.com/goldlapel/goldlapel)
