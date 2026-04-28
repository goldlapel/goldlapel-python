# goldlapel

[![Tests](https://github.com/goldlapel/goldlapel-python/actions/workflows/test.yml/badge.svg)](https://github.com/goldlapel/goldlapel-python/actions/workflows/test.yml)

The Python wrapper for [Gold Lapel](https://goldlapel.com) — a self-optimizing Postgres proxy that watches query patterns and creates materialized views + indexes automatically. Zero code changes beyond the connection string.

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

Point your Postgres driver at `gl.url`. Gold Lapel sits between your app and your DB, watching query patterns and creating materialized views + indexes automatically. Zero code changes beyond the connection string.

Async usage (`goldlapel.asyncio.start`), context managers, transactional coordination via `gl.using(conn)`, and framework integrations are in the docs.

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

Before removing the package, drop Gold Lapel's helper schema and cached matviews from your Postgres:

```bash
goldlapel clean
```

Then remove the package and any local state:

```bash
pip uninstall goldlapel
rm -rf ~/.goldlapel
rm -f goldlapel.toml     # only if you wrote one
```

Cancelling your subscription does not delete your data — only Gold Lapel's helper schema and cached matviews go away.

## License

MIT. See `LICENSE`.
