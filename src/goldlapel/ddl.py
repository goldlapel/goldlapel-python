"""
DDL API client — fetches canonical helper-table DDL + query patterns from the
Rust proxy's dashboard port so the wrapper never hand-writes CREATE TABLE for
helper families (streams, docs, counters, ...).

Architecture: see docs/wrapper-v0.2/SCHEMA-TO-CORE-PLAN.md in the goldlapel repo.

- One HTTP call per (family, name) per session (cached).
- Cache key: (family, name). Value: {"tables": {...}, "query_patterns": {...}}.
- Cache lives on the conn-carrying object (wrapper instance or raw conn).
- Errors: HTTP failures surface as RuntimeError with actionable text.

Token + port resolution:
- `GoldLapel` passes `dashboard_port` + `dashboard_token` explicitly when it
  spawned the proxy subprocess (the happy path).
- For externally-launched proxies, wrapper reads
  `GOLDLAPEL_DASHBOARD_TOKEN` env or `~/.goldlapel/dashboard-token` file.
"""

import json
import os
import urllib.error
import urllib.request
import weakref
from pathlib import Path


_SUPPORTED_VERSIONS = {
    "stream": "v1",
}

# Per-instance cache keyed on `id(owner)`: {owner_id: {(family, name): patterns}}.
# We key on id() so each GoldLapel instance has its own cache even when the
# connection is shared. Entries are cleared when the owning instance is GC'd
# via WeakKeyDictionary.
_CACHE: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()


def token_from_env_or_file():
    """Resolve dashboard token for externally-launched proxies.

    Priority: GOLDLAPEL_DASHBOARD_TOKEN env > ~/.goldlapel/dashboard-token file.
    Returns None if neither is set — caller should raise a clear error.
    """
    env = os.environ.get("GOLDLAPEL_DASHBOARD_TOKEN")
    if env:
        return env.strip()
    home = Path.home() / ".goldlapel" / "dashboard-token"
    if home.exists():
        try:
            return home.read_text(encoding="utf-8").strip() or None
        except OSError:
            return None
    return None


def supported_version(family):
    """Return the schema version the wrapper pins for `family`."""
    return _SUPPORTED_VERSIONS[family]


def _cache_for(owner):
    """Return the per-owner cache dict, creating it lazily."""
    # weakref.WeakKeyDictionary requires its keys to support weak references.
    # Most simple Python objects do; `GoldLapel` and raw psycopg connections both do.
    try:
        bucket = _CACHE.get(owner)
    except TypeError:
        # Owner isn't hashable for weakref — fall back to a dedicated attr.
        bucket = getattr(owner, "_gl_ddl_cache", None)
        if bucket is None:
            bucket = {}
            try:
                setattr(owner, "_gl_ddl_cache", bucket)
            except (AttributeError, TypeError):
                # Can't attach an attribute — create a per-call bucket and
                # live with an extra round-trip per call.
                return {}
        return bucket
    if bucket is None:
        bucket = {}
        try:
            _CACHE[owner] = bucket
        except TypeError:
            bucket = getattr(owner, "_gl_ddl_cache", None)
            if bucket is None:
                bucket = {}
                try:
                    setattr(owner, "_gl_ddl_cache", bucket)
                except (AttributeError, TypeError):
                    return {}
    return bucket


def _post(url, token, body, timeout=10.0):
    """POST JSON, return (status_code, parsed_json or raw text)."""
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "X-GL-Dashboard": token,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            text = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        status = e.code
        text = e.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as e:
        raise RuntimeError(
            f"Gold Lapel dashboard not reachable at {url}: {e.reason}. "
            "Is `goldlapel` running? The dashboard port must be open for helper "
            "families (streams, docs, …) to work."
        ) from e
    try:
        parsed = json.loads(text) if text else {}
    except json.JSONDecodeError:
        parsed = {"_raw": text}
    return status, parsed


def fetch(owner, family, name, dashboard_port, dashboard_token):
    """Fetch (and cache) the canonical {tables, query_patterns} for a helper.

    Per-session cache: one HTTP call on the first call for a given (family, name);
    cached result for every subsequent call in the same session.
    """
    cache = _cache_for(owner)
    key = (family, name)
    if key in cache:
        return cache[key]

    if dashboard_token is None:
        raise RuntimeError(
            "No dashboard token available. Set GOLDLAPEL_DASHBOARD_TOKEN or let "
            "GoldLapel spawn the proxy (which provisions a token automatically)."
        )
    if not dashboard_port:
        raise RuntimeError(
            "No dashboard port available. Gold Lapel's helper families "
            f"({family}, ...) require the proxy's dashboard to be reachable."
        )

    url = f"http://127.0.0.1:{dashboard_port}/api/ddl/{family}/create"
    body = {"name": name, "schema_version": supported_version(family)}
    status, resp = _post(url, dashboard_token, body)
    if status != 200:
        error = resp.get("error", "unknown") if isinstance(resp, dict) else "unknown"
        detail = resp.get("detail", str(resp)) if isinstance(resp, dict) else str(resp)
        if status == 409 and error == "version_mismatch":
            raise RuntimeError(
                f"Gold Lapel schema version mismatch for {family} '{name}': {detail}. "
                "Upgrade the proxy or the wrapper so versions agree."
            )
        if status == 403:
            raise RuntimeError(
                "Gold Lapel dashboard rejected the DDL request (403). "
                "The dashboard token is missing or incorrect — check "
                "GOLDLAPEL_DASHBOARD_TOKEN or ~/.goldlapel/dashboard-token."
            )
        raise RuntimeError(
            f"Gold Lapel DDL API {family}/{name} failed with {status} {error}: {detail}"
        )

    entry = {
        "tables": resp["tables"],
        "query_patterns": resp["query_patterns"],
    }
    cache[key] = entry
    return entry


def invalidate(owner):
    """Clear all cached patterns for `owner` — used on `gl.stop()`."""
    try:
        _CACHE.pop(owner, None)
    except TypeError:
        pass
    try:
        if hasattr(owner, "_gl_ddl_cache"):
            delattr(owner, "_gl_ddl_cache")
    except (AttributeError, TypeError):
        pass


def _pct_s(pattern):
    """Convert Postgres `$1`, `$2`, … to psycopg's `%s` binding syntax.

    Simple substitution — proxy only emits numbered placeholders of form $N,
    which never collide with `%s` inside payloads (JSONB literals travel
    through parameters).
    """
    # Replace $N with %s. Do it in descending N order so $10 isn't mis-handled
    # as $1 followed by 0 — though we only use $1..$9 today, this is defensive.
    import re
    return re.sub(r"\$\d+", "%s", pattern)


def to_psycopg(pattern):
    """Public wrapper around `_pct_s` used by streams code."""
    return _pct_s(pattern)
