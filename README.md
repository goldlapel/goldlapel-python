# Gold Lapel

Self-optimizing Postgres proxy — automatic materialized views and indexes. Zero code changes required.

Gold Lapel sits between your app and Postgres, watches query patterns, and automatically creates materialized views and indexes to make your database faster. Port 7932 (79 = atomic number for gold, 32 from Postgres).

## Install

```bash
pip install goldlapel
# or
uv pip install goldlapel
```

## Quick Start

```python
import goldlapel

# Start the proxy — returns a connection string pointing at Gold Lapel
url = goldlapel.start("postgresql://user:pass@localhost:5432/mydb")

# Use the URL with any Postgres driver
import asyncpg
conn = await asyncpg.connect(url)

# Or psycopg2, SQLAlchemy, Django — anything that speaks Postgres
```

Gold Lapel is driver-agnostic. `start()` returns a connection string (`postgresql://...@localhost:7932/...`) that works with any Postgres driver or ORM.

## API

### `goldlapel.start(upstream, config=None, port=None, extra_args=None)`

Starts the Gold Lapel proxy and returns the proxy connection string.

- `upstream` — your Postgres connection string (e.g. `postgresql://user:pass@localhost:5432/mydb`)
- `config` — dict of configuration options (see [Configuration](#configuration))
- `port` — proxy port (default: 7932)
- `extra_args` — additional CLI flags passed to the binary (e.g. `["--threshold-impact", "5000"]`)

### `goldlapel.stop()`

Stops the proxy. Also called automatically on process exit.

### `goldlapel.proxy_url()`

Returns the current proxy URL, or `None` if not running.

### `goldlapel.config_keys()`

Returns the set of all valid config key names.

### `goldlapel.GoldLapel(upstream, config=None, port=None, extra_args=None)`

Class interface for managing multiple instances:

```python
proxy = goldlapel.GoldLapel("postgresql://user:pass@localhost:5432/mydb", port=7932)
url = proxy.start()
# ...
proxy.stop()
```

## Configuration

Pass a config dict as the second argument to `start()` to configure the proxy:

```python
import goldlapel

url = goldlapel.start("postgresql://user:pass@localhost/mydb", {
    "mode": "butler",
    "pool_size": 50,
    "disable_matviews": True,
    "replica": ["postgresql://user:pass@replica1/mydb"],
})
```

Keys use `snake_case` and map directly to CLI flags (`pool_size` → `--pool-size`). Boolean keys like `disable_matviews` are flags — `True` enables them, `False` (or omitting) leaves them off. List keys like `replica` accept arrays and produce repeated flags.

Unknown keys raise `ValueError` immediately. To see all valid keys:

```python
import goldlapel
print(goldlapel.config_keys())
```

For the full configuration reference, see the [main documentation](https://github.com/goldlapel/goldlapel#setting-reference).

You can also set environment variables (`GOLDLAPEL_PORT`, `GOLDLAPEL_UPSTREAM`, etc.) — the binary reads them automatically.

## How It Works

This package bundles the Gold Lapel Rust binary for your platform. When you call `start()`, it:

1. Locates the binary (bundled in package, on PATH, or via `GOLDLAPEL_BINARY` env var)
2. Spawns it as a subprocess listening on localhost
3. Waits for the port to be ready
4. Returns a connection string pointing at the proxy
5. Cleans up automatically on process exit

The binary does all the work — this wrapper just manages its lifecycle.

## Links

- [Website](https://goldlapel.com)
- [Documentation](https://github.com/goldlapel/goldlapel)
