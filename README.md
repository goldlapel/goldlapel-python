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

## Dashboard

Gold Lapel exposes a live dashboard at `gl.dashboard_url`:

```python
print(gl.dashboard_url)
# -> http://127.0.0.1:7933
```

## Documentation

Full API reference, async usage, configuration, framework integrations (Django, SQLAlchemy, FastAPI), upgrading from v0.1, and production deployment: https://goldlapel.com/docs/python

## License

MIT. See `LICENSE`.
