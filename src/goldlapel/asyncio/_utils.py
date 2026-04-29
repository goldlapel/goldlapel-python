"""Native-asyncpg versions of the utility functions in goldlapel.utils.

Every sync util in goldlapel.utils that the GoldLapel wrapper methods call has
an async sibling here. The SQL strings, identifier validation, filter/update
builders, and JSON encoding logic are shared with the sync module — we reuse
them directly. Only the driver calls (cursor/execute/fetch/commit) change.

asyncpg differences handled here:

  - Placeholder translation: asyncpg uses `$1, $2, ...` not psycopg's `%s`.
    The sync SQL strings are copied verbatim and `_to_asyncpg(sql, params)`
    translates them on the way out. This keeps the two implementations in
    sync without maintaining two copies of every SQL string.

  - No explicit commit: asyncpg connections are auto-committed by default
    (every `execute()` is its own transaction). The sync path calls
    `raw.commit()` after writes; the async path omits that, since writes
    are visible immediately. Inside `async with conn.transaction():` (opened
    by asyncpg or by `gl.using()` with a user-supplied transaction), writes
    are deferred as expected.

  - JSONB result shape: we do NOT register an asyncpg jsonb codec — it
    double-encodes on the `data @> %s::jsonb` filter path that the sync
    `_build_filter` emits. Instead, doc_* functions that return rows with
    a `data` column run the values through `_maybe_json_decode` at the
    util-return boundary, so async results match the sync path's dict/list
    shape regardless of which asyncpg internal codec ran. See
    `_register_jsonb_codec` docstring for the full rationale.

  - Record → dict: asyncpg fetch returns asyncpg.Record objects. The sync
    path returns dict(zip(cols, row)). We convert here with `dict(record)`
    which is O(n) but consistent with the sync return shape.

Notes on specific utilities:

  - `subscribe`, `doc_watch`: LISTEN/NOTIFY via asyncpg uses conn.add_listener
    on a fresh connection (asyncpg doesn't expose select() on notify queues).
  - `doc_find_cursor`: implemented as an async generator (asyncpg cursors are
    scoped to a transaction — we open one, iterate, close).
  - `stream_read`: opens an explicit transaction for `FOR UPDATE` semantics.
  - `script`: unchanged shape vs sync; works because pllua functions are a
    one-shot DDL+SELECT sequence.
"""

import json
import re

from goldlapel.utils import (
    _validate_identifier,
    _build_filter,
    _build_update,
    _field_path,
    _FIELD_PART_RE,
    _build_project,
    _build_group,
)


# -- Placeholder translation -------------------------------------------------

_PSYCOPG_PLACEHOLDER = re.compile(r"%s")


def _to_asyncpg(sql, params):
    """Translate `%s` placeholders to `$N`. Returns (sql, params_tuple).

    Preserves `%%` (literal percent) untouched — we split on `%s` only.
    Mismatched counts raise ValueError; asyncpg would fail anyway but the
    error here is clearer.
    """
    if not params:
        return sql, ()
    expected = len(params) if not isinstance(params, dict) else len(params)
    # Count %s occurrences (not inside %%)
    parts = _PSYCOPG_PLACEHOLDER.split(sql)
    count = len(parts) - 1
    if count != expected:
        raise ValueError(
            f"asyncpg placeholder translation: SQL has {count} %s but {expected} params"
        )
    out = []
    for i, chunk in enumerate(parts):
        out.append(chunk)
        if i < count:
            out.append(f"${i + 1}")
    return "".join(out), tuple(params)


def _get_raw_connection(conn):
    """Extract the raw asyncpg.Connection from an AsyncCachedConnection wrapper."""
    # AsyncCachedConnection stores the real asyncpg conn as `_real`.
    if hasattr(conn, "_real") and hasattr(conn._real, "fetch"):
        return conn._real
    return conn


async def _execute(conn, sql, params=()):
    sql2, params2 = _to_asyncpg(sql, params)
    raw = _get_raw_connection(conn)
    return await raw.execute(sql2, *params2)


async def _fetch(conn, sql, params=()):
    sql2, params2 = _to_asyncpg(sql, params)
    raw = _get_raw_connection(conn)
    return await raw.fetch(sql2, *params2)


async def _fetchrow(conn, sql, params=()):
    sql2, params2 = _to_asyncpg(sql, params)
    raw = _get_raw_connection(conn)
    return await raw.fetchrow(sql2, *params2)


async def _fetchval(conn, sql, params=()):
    sql2, params2 = _to_asyncpg(sql, params)
    raw = _get_raw_connection(conn)
    return await raw.fetchval(sql2, *params2)


def _rowcount_from_status(status):
    """asyncpg's execute() returns a status string like 'UPDATE 3' or 'DELETE 0'.
    Extract the trailing integer (rows affected)."""
    if not status:
        return 0
    parts = status.rsplit(" ", 1)
    try:
        return int(parts[-1])
    except (ValueError, IndexError):
        return 0


def _row_to_dict(record):
    return dict(record) if record is not None else None


def _rows_to_dicts(records):
    return [dict(r) for r in records]


# -- Pub/sub & queues -------------------------------------------------------

async def publish(conn, channel, message):
    _validate_identifier(channel)
    await _execute(conn, "SELECT pg_notify(%s, %s)", (channel, str(message)))


async def subscribe(conn, channel, callback, blocking=True):
    """Subscribe to a channel using asyncpg's add_listener.

    Opens a fresh asyncpg connection for LISTEN (listener is tied to a specific
    conn). If blocking=True, waits indefinitely for notifications. If False,
    returns the listener connection so the caller can close it later.

    Callback is invoked as callback(channel, payload) — same signature as sync.
    """
    import asyncio

    _validate_identifier(channel)
    raw = _get_raw_connection(conn)
    dsn = _dsn_for_listen(raw)

    import asyncpg
    listen_conn = await asyncpg.connect(dsn)
    await _register_jsonb_codec(listen_conn)

    def _cb(conn_arg, pid, chan, payload):
        # asyncpg's listener callback is sync; match the sync wrapper signature.
        callback(chan, payload)

    await listen_conn.add_listener(channel, _cb)

    if not blocking:
        # Caller holds the listen_conn to close later.
        return listen_conn

    try:
        # Block forever on a never-completing future — the listener fires in
        # the event loop independently.
        await asyncio.Event().wait()
    finally:
        await listen_conn.remove_listener(channel, _cb)
        await listen_conn.close()


# --
# Phase 5 Redis-compat families: counter, zset, hash, queue, geo.
#
# Each family's helpers consume `patterns` returned from the proxy's
# `/api/ddl/<family>/create` endpoint. The proxy emits SQL with `$N`
# placeholders — asyncpg's native bind style — so we use them as-is
# (no `_to_asyncpg` translation needed for these utils).
# --


def _family_pattern(patterns, key, family):
    """Pull a query pattern from the proxy's response (asyncpg-native $N)."""
    if patterns is None:
        raise RuntimeError(
            f"{family} utils require DDL patterns from the proxy — call via "
            f"`gl.{family}s.<verb>(...)` rather than the utils function directly."
        )
    return patterns["query_patterns"][key]


async def _family_execute(conn, sql, *params):
    """Execute SQL with native `$N` placeholders, no `%s → $N` translation."""
    raw = _get_raw_connection(conn)
    return await raw.execute(sql, *params)


async def _family_fetchrow(conn, sql, *params):
    raw = _get_raw_connection(conn)
    return await raw.fetchrow(sql, *params)


async def _family_fetch(conn, sql, *params):
    raw = _get_raw_connection(conn)
    return await raw.fetch(sql, *params)


# ---------------------------------------------------------------------------
# Counter family
# ---------------------------------------------------------------------------


async def counter_incr(conn, name, key, amount=1, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "incr", "counter")
    row = await _family_fetchrow(conn, sql, key, int(amount))
    return row[0]


async def counter_decr(conn, name, key, amount=1, *, patterns=None):
    return await counter_incr(conn, name, key, -int(amount), patterns=patterns)


async def counter_set(conn, name, key, value, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "set", "counter")
    row = await _family_fetchrow(conn, sql, key, int(value))
    return row[0]


async def counter_get(conn, name, key, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "get", "counter")
    row = await _family_fetchrow(conn, sql, key)
    return row[0] if row else 0


async def counter_delete(conn, name, key, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "delete", "counter")
    status = await _family_execute(conn, sql, key)
    return _rowcount_from_status(status) > 0


async def counter_count_keys(conn, name, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "count_keys", "counter")
    row = await _family_fetchrow(conn, sql)
    return row[0] if row else 0


# ---------------------------------------------------------------------------
# Sorted-set (zset) family
# ---------------------------------------------------------------------------


async def zset_add(conn, name, zset_key, member, score, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "zadd", "zset")
    row = await _family_fetchrow(
        conn, sql, str(zset_key), str(member), float(score),
    )
    return row[0]


async def zset_incr_by(conn, name, zset_key, member, delta=1, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "zincrby", "zset")
    row = await _family_fetchrow(
        conn, sql, str(zset_key), str(member), float(delta),
    )
    return row[0]


async def zset_score(conn, name, zset_key, member, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "zscore", "zset")
    row = await _family_fetchrow(conn, sql, str(zset_key), str(member))
    return row[0] if row else None


async def zset_remove(conn, name, zset_key, member, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "zrem", "zset")
    status = await _family_execute(conn, sql, str(zset_key), str(member))
    return _rowcount_from_status(status) > 0


async def zset_range(conn, name, zset_key, start=0, stop=10, desc=True, *, patterns=None):
    _validate_identifier(name)
    key = "zrange_desc" if desc else "zrange_asc"
    sql = _family_pattern(patterns, key, "zset")
    limit = max(0, int(stop) - int(start) + 1)
    rows = await _family_fetch(conn, sql, str(zset_key), limit, int(start))
    return [(r[0], r[1]) for r in rows]


async def zset_range_by_score(conn, name, zset_key, min_score, max_score, limit=100, offset=0, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "zrangebyscore", "zset")
    rows = await _family_fetch(
        conn, sql, str(zset_key), float(min_score), float(max_score),
        int(limit), int(offset),
    )
    return [(r[0], r[1]) for r in rows]


async def zset_rank(conn, name, zset_key, member, desc=True, *, patterns=None):
    _validate_identifier(name)
    key = "zrank_desc" if desc else "zrank_asc"
    sql = _family_pattern(patterns, key, "zset")
    row = await _family_fetchrow(conn, sql, str(zset_key), str(member))
    return row[0] if row else None


async def zset_card(conn, name, zset_key, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "zcard", "zset")
    row = await _family_fetchrow(conn, sql, str(zset_key))
    return row[0] if row else 0


# ---------------------------------------------------------------------------
# Hash family
# ---------------------------------------------------------------------------


def _decode_jsonb(value):
    """Best-effort decode for JSONB payloads. asyncpg's default codec hands
    us decoded objects; if a config returned the raw text, decode it.
    Tolerate non-JSON strings (might be pre-existing data)."""
    if not isinstance(value, (str, bytes)):
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


async def hash_set(conn, name, hash_key, field, value, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "hset", "hash")
    row = await _family_fetchrow(
        conn, sql, str(hash_key), str(field), json.dumps(value),
    )
    if row and row[0] is not None:
        return _decode_jsonb(row[0])
    return None


async def hash_get(conn, name, hash_key, field, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "hget", "hash")
    row = await _family_fetchrow(conn, sql, str(hash_key), str(field))
    if row and row[0] is not None:
        return _decode_jsonb(row[0])
    return None


async def hash_get_all(conn, name, hash_key, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "hgetall", "hash")
    rows = await _family_fetch(conn, sql, str(hash_key))
    return {r[0]: _decode_jsonb(r[1]) for r in rows}


async def hash_keys(conn, name, hash_key, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "hkeys", "hash")
    rows = await _family_fetch(conn, sql, str(hash_key))
    return [r[0] for r in rows]


async def hash_values(conn, name, hash_key, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "hvals", "hash")
    rows = await _family_fetch(conn, sql, str(hash_key))
    return [_decode_jsonb(r[0]) for r in rows]


async def hash_exists(conn, name, hash_key, field, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "hexists", "hash")
    row = await _family_fetchrow(conn, sql, str(hash_key), str(field))
    return bool(row[0]) if row else False


async def hash_delete(conn, name, hash_key, field, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "hdel", "hash")
    status = await _family_execute(conn, sql, str(hash_key), str(field))
    return _rowcount_from_status(status) > 0


async def hash_len(conn, name, hash_key, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "hlen", "hash")
    row = await _family_fetchrow(conn, sql, str(hash_key))
    return row[0] if row else 0


# ---------------------------------------------------------------------------
# Queue family (at-least-once with visibility timeout)
# ---------------------------------------------------------------------------


async def queue_enqueue(conn, name, payload, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "enqueue", "queue")
    row = await _family_fetchrow(conn, sql, json.dumps(payload))
    return row[0] if row else None


async def queue_claim(conn, name, visibility_timeout_ms=30000, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "claim", "queue")
    row = await _family_fetchrow(conn, sql, int(visibility_timeout_ms))
    if not row:
        return None
    return (row[0], _decode_jsonb(row[1]))


async def queue_ack(conn, name, message_id, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "ack", "queue")
    status = await _family_execute(conn, sql, int(message_id))
    return _rowcount_from_status(status) > 0


async def queue_abandon(conn, name, message_id, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "nack", "queue")
    row = await _family_fetchrow(conn, sql, int(message_id))
    return row is not None


async def queue_extend(conn, name, message_id, additional_ms, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "extend", "queue")
    row = await _family_fetchrow(
        conn, sql, int(message_id), int(additional_ms),
    )
    return row[0] if row else None


async def queue_peek(conn, name, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "peek", "queue")
    row = await _family_fetchrow(conn, sql)
    if not row:
        return None
    msg_id, payload, visible_at, status, created_at = row
    return {
        "id": msg_id,
        "payload": _decode_jsonb(payload),
        "visible_at": visible_at,
        "status": status,
        "created_at": created_at,
    }


async def queue_count_ready(conn, name, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "count_ready", "queue")
    row = await _family_fetchrow(conn, sql)
    return row[0] if row else 0


async def queue_count_claimed(conn, name, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "count_claimed", "queue")
    row = await _family_fetchrow(conn, sql)
    return row[0] if row else 0


# ---------------------------------------------------------------------------
# Geo family (PostGIS GEOGRAPHY-native)
# ---------------------------------------------------------------------------

# Distance unit conversion (meters-native — matches the proxy column type).
_GEO_UNITS = {"m": 1.0, "km": 1000.0, "mi": 1609.344, "ft": 0.3048}


def _to_meters(value, unit):
    factor = _GEO_UNITS.get(unit)
    if factor is None:
        raise ValueError(f"Unknown distance unit: {unit!r} (choose m/km/mi/ft)")
    return float(value) * factor


def _convert_distance_meters(meters, unit):
    factor = _GEO_UNITS.get(unit)
    if factor is None:
        raise ValueError(f"Unknown distance unit: {unit!r} (choose m/km/mi/ft)")
    return float(meters) / factor


async def geo_add(conn, name, member, lon, lat, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "geoadd", "geo")
    row = await _family_fetchrow(
        conn, sql, str(member), float(lon), float(lat),
    )
    return (row[0], row[1]) if row else None


async def geo_pos(conn, name, member, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "geopos", "geo")
    row = await _family_fetchrow(conn, sql, str(member))
    return (row[0], row[1]) if row else None


async def geo_dist(conn, name, member_a, member_b, unit="m", *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "geodist", "geo")
    row = await _family_fetchrow(conn, sql, str(member_a), str(member_b))
    if not row or row[0] is None:
        return None
    return _convert_distance_meters(row[0], unit)


async def geo_radius(conn, name, lon, lat, radius, unit="m", limit=50, *, patterns=None):
    """Members within `radius` of (lon, lat) with per-row distance.

    Proxy contract: $1=lon, $2=lat, $3=radius_m, $4=limit. The proxy
    computes the anchor geography once via CTE so each $N appears exactly
    once — asyncpg binds the 4-tuple positionally, no translation needed.
    """
    _validate_identifier(name)
    sql = _family_pattern(patterns, "georadius_with_dist", "geo")
    radius_m = _to_meters(radius, unit)
    rows = await _family_fetch(
        conn, sql, float(lon), float(lat), float(radius_m), int(limit),
    )
    return [dict(r) for r in rows]


async def geo_radius_by_member(conn, name, member, radius, unit="m", limit=50, *, patterns=None):
    """Members within `radius` of `member`'s location.

    Proxy contract: $1 and $2 are both the anchor member name (one for the
    join, one for the self-exclusion); $3=radius_m, $4=limit. The proxy
    emits the WHERE clauses in source-text order matching $N indices, so
    `(member, member, radius_m, limit)` works for both native-$N (asyncpg
    here) AND psycopg %s translation in the sync path.
    """
    _validate_identifier(name)
    sql = _family_pattern(patterns, "geosearch_member", "geo")
    radius_m = _to_meters(radius, unit)
    rows = await _family_fetch(
        conn, sql, str(member), str(member), float(radius_m), int(limit),
    )
    return [dict(r) for r in rows]


async def geo_remove(conn, name, member, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "geo_remove", "geo")
    status = await _family_execute(conn, sql, str(member))
    return _rowcount_from_status(status) > 0


async def geo_count(conn, name, *, patterns=None):
    _validate_identifier(name)
    sql = _family_pattern(patterns, "geo_count", "geo")
    row = await _family_fetchrow(conn, sql)
    return row[0] if row else 0


# -- Misc -------------------------------------------------------------------

async def script(conn, lua_code, *args):
    import hashlib
    await _execute(conn, "CREATE EXTENSION IF NOT EXISTS pllua")
    func_name = "_gl_lua_" + format(abs(hash(lua_code)), "x")[:8]
    tag = f"$_gl_{hashlib.md5(lua_code.encode()).hexdigest()[:8]}$"
    n = len(args)
    params = ", ".join([f"p{i + 1} text" for i in range(n)])
    await _execute(conn, f"""
        CREATE OR REPLACE FUNCTION pg_temp.{func_name}({params})
        RETURNS text LANGUAGE pllua AS {tag}
        {lua_code}
        {tag}
    """)
    if n > 0:
        placeholders = ", ".join(["%s"] * n)
        row = await _fetchrow(conn, f"SELECT pg_temp.{func_name}({placeholders})", args)
    else:
        row = await _fetchrow(conn, f"SELECT pg_temp.{func_name}()")
    return row[0] if row else None


async def count_distinct(conn, table, column):
    _validate_identifier(table)
    _validate_identifier(column)
    val = await _fetchval(conn, f"SELECT COUNT(DISTINCT {column}) FROM {table}")
    return val


# -- Streams ----------------------------------------------------------------

def _stream_pattern(patterns, key):
    """The proxy hands us SQL with $1/$2 placeholders — asyncpg's native
    binding style — so no translation is needed. Just look it up."""
    return patterns["query_patterns"][key]


async def stream_add(conn, stream, payload, *, patterns=None):
    if patterns is None:
        raise RuntimeError(
            "stream_add requires DDL patterns from the proxy — call via "
            "`gl.stream_add(...)` rather than the utils function directly."
        )
    _validate_identifier(stream)
    raw = _get_raw_connection(conn)
    sql = _stream_pattern(patterns, "insert")
    row = await raw.fetchrow(sql, json.dumps(payload))
    return row[0]


async def stream_create_group(conn, stream, group, *, patterns=None):
    if patterns is None:
        raise RuntimeError(
            "stream_create_group requires DDL patterns from the proxy — call via "
            "`gl.stream_create_group(...)` rather than the utils function directly."
        )
    _validate_identifier(stream)
    raw = _get_raw_connection(conn)
    await raw.execute(_stream_pattern(patterns, "create_group"), group)


async def stream_read(conn, stream, group, consumer, count=1, *, patterns=None):
    # Sync path uses `FOR UPDATE` inside an implicit txn; asyncpg needs an
    # explicit transaction for FOR UPDATE to be meaningful. We open a tx here.
    if patterns is None:
        raise RuntimeError(
            "stream_read requires DDL patterns from the proxy — call via "
            "`gl.stream_read(...)` rather than the utils function directly."
        )
    _validate_identifier(stream)
    raw = _get_raw_connection(conn)
    async with raw.transaction():
        row = await raw.fetchrow(_stream_pattern(patterns, "group_get_cursor"), group)
        if not row:
            return []
        last_id = row[0]
        rows = await raw.fetch(_stream_pattern(patterns, "read_since"), last_id, count)
        messages = []
        for r in rows:
            msg_id, payload, created_at = r[0], r[1], r[2]
            messages.append({
                "id": msg_id,
                "payload": payload if isinstance(payload, (dict, list)) else json.loads(payload),
                "created_at": str(created_at),
            })
        if messages:
            new_last = messages[-1]["id"]
            await raw.execute(
                _stream_pattern(patterns, "group_advance_cursor"),
                new_last, group,
            )
            pending_insert = _stream_pattern(patterns, "pending_insert")
            for msg in messages:
                await raw.execute(pending_insert, msg["id"], group, consumer)
        return messages


async def stream_ack(conn, stream, group, message_id, *, patterns=None):
    if patterns is None:
        raise RuntimeError(
            "stream_ack requires DDL patterns from the proxy — call via "
            "`gl.stream_ack(...)` rather than the utils function directly."
        )
    _validate_identifier(stream)
    raw = _get_raw_connection(conn)
    status = await raw.execute(_stream_pattern(patterns, "ack"), group, message_id)
    return _rowcount_from_status(status) > 0


async def stream_claim(conn, stream, group, consumer, min_idle_ms=60000, *, patterns=None):
    if patterns is None:
        raise RuntimeError(
            "stream_claim requires DDL patterns from the proxy — call via "
            "`gl.stream_claim(...)` rather than the utils function directly."
        )
    _validate_identifier(stream)
    raw = _get_raw_connection(conn)
    rows = await raw.fetch(
        _stream_pattern(patterns, "claim"),
        consumer, group, min_idle_ms,
    )
    claimed_ids = [r[0] for r in rows]
    messages = []
    if claimed_ids:
        read_by_id = _stream_pattern(patterns, "read_by_id")
        for msg_id in claimed_ids:
            r = await raw.fetchrow(read_by_id, msg_id)
            if r:
                payload = r[1]
                messages.append({
                    "id": r[0],
                    "payload": payload if isinstance(payload, (dict, list)) else json.loads(payload),
                    "created_at": str(r[2]),
                })
    return messages


# -- Search -----------------------------------------------------------------

async def search(conn, table, column, query, limit=50, lang="english", highlight=False):
    _validate_identifier(table)
    if isinstance(column, str):
        columns = [column]
    else:
        columns = list(column)
    for col in columns:
        _validate_identifier(col)
    tsvec = " || ' ' || ".join(f"coalesce({col}, '')" for col in columns)
    if highlight:
        hl_col = columns[0]
        rows = await _fetch(conn, f"""
            SELECT *,
                ts_rank(to_tsvector(%s, {tsvec}), plainto_tsquery(%s, %s)) AS _score,
                ts_headline(%s, {hl_col}, plainto_tsquery(%s, %s),
                    'StartSel=<mark>, StopSel=</mark>, MaxWords=35, MinWords=15') AS _highlight
            FROM {table}
            WHERE to_tsvector(%s, {tsvec}) @@ plainto_tsquery(%s, %s)
            ORDER BY _score DESC LIMIT %s
        """, (lang, lang, query, lang, lang, query, lang, lang, query, limit))
    else:
        rows = await _fetch(conn, f"""
            SELECT *,
                ts_rank(to_tsvector(%s, {tsvec}), plainto_tsquery(%s, %s)) AS _score
            FROM {table}
            WHERE to_tsvector(%s, {tsvec}) @@ plainto_tsquery(%s, %s)
            ORDER BY _score DESC LIMIT %s
        """, (lang, lang, query, lang, lang, query, limit))
    return _rows_to_dicts(rows)


async def search_fuzzy(conn, table, column, query, limit=50, threshold=0.3):
    _validate_identifier(table)
    _validate_identifier(column)
    rows = await _fetch(conn, f"""
        SELECT *, similarity({column}, %s) AS _score
        FROM {table}
        WHERE similarity({column}, %s) > %s
        ORDER BY _score DESC LIMIT %s
    """, (query, query, float(threshold), limit))
    return _rows_to_dicts(rows)


async def search_phonetic(conn, table, column, query, limit=50):
    _validate_identifier(table)
    _validate_identifier(column)
    rows = await _fetch(conn, f"""
        SELECT *, similarity({column}, %s) AS _score
        FROM {table}
        WHERE soundex({column}) = soundex(%s)
        ORDER BY _score DESC, {column} LIMIT %s
    """, (query, query, limit))
    return _rows_to_dicts(rows)


async def similar(conn, table, column, vector, limit=10):
    _validate_identifier(table)
    _validate_identifier(column)
    vec_literal = "[" + ",".join(str(float(v)) for v in vector) + "]"
    rows = await _fetch(conn, f"""
        SELECT *, ({column} <=> %s::vector) AS _score
        FROM {table}
        ORDER BY _score LIMIT %s
    """, (vec_literal, limit))
    return _rows_to_dicts(rows)


async def suggest(conn, table, column, prefix, limit=10):
    _validate_identifier(table)
    _validate_identifier(column)
    pattern = prefix + "%"
    rows = await _fetch(conn, f"""
        SELECT *, similarity({column}, %s) AS _score
        FROM {table}
        WHERE {column} ILIKE %s
        ORDER BY _score DESC, {column} LIMIT %s
    """, (prefix, pattern, limit))
    return _rows_to_dicts(rows)


async def facets(conn, table, column, limit=50, query=None, query_column=None, lang="english"):
    _validate_identifier(table)
    _validate_identifier(column)
    if query and query_column:
        if isinstance(query_column, str):
            query_columns = [query_column]
        else:
            query_columns = list(query_column)
        for qc in query_columns:
            _validate_identifier(qc)
        tsvec = " || ' ' || ".join(f"coalesce({qc}, '')" for qc in query_columns)
        rows = await _fetch(conn, f"""
            SELECT {column} AS value, COUNT(*) AS count
            FROM {table}
            WHERE to_tsvector(%s, {tsvec}) @@ plainto_tsquery(%s, %s)
            GROUP BY {column}
            ORDER BY count DESC, {column} LIMIT %s
        """, (lang, lang, query, limit))
    else:
        rows = await _fetch(conn, f"""
            SELECT {column} AS value, COUNT(*) AS count
            FROM {table}
            GROUP BY {column}
            ORDER BY count DESC, {column} LIMIT %s
        """, (limit,))
    return _rows_to_dicts(rows)


async def aggregate(conn, table, column, func, group_by=None, limit=50):
    _validate_identifier(table)
    _validate_identifier(column)
    allowed = {"count", "sum", "avg", "min", "max"}
    if func not in allowed:
        raise ValueError(f"func must be one of {allowed}")
    agg_expr = "COUNT(*)" if func == "count" else f"{func.upper()}({column})"
    if group_by:
        _validate_identifier(group_by)
        rows = await _fetch(conn, f"""
            SELECT {group_by}, {agg_expr} AS value
            FROM {table}
            GROUP BY {group_by}
            ORDER BY value DESC LIMIT %s
        """, (limit,))
    else:
        rows = await _fetch(conn, f"""
            SELECT {agg_expr} AS value
            FROM {table}
        """)
    return _rows_to_dicts(rows)


async def create_search_config(conn, name, copy_from="english"):
    _validate_identifier(name)
    _validate_identifier(copy_from)
    row = await _fetchrow(conn, "SELECT 1 FROM pg_ts_config WHERE cfgname = %s", (name,))
    if not row:
        await _execute(conn, f"CREATE TEXT SEARCH CONFIGURATION {name} (COPY = {copy_from})")


# -- Percolator -------------------------------------------------------------

async def percolate_add(conn, name, query_id, query, lang="english", metadata=None):
    _validate_identifier(name)
    await _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {name} (
            query_id TEXT PRIMARY KEY,
            query_text TEXT NOT NULL,
            tsquery TSQUERY NOT NULL,
            lang TEXT NOT NULL DEFAULT 'english',
            metadata JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    await _execute(conn, f"CREATE INDEX IF NOT EXISTS {name}_tsq_idx ON {name} USING GIST (tsquery)")
    metadata_json = json.dumps(metadata) if metadata is not None else None
    await _execute(conn, f"""
        INSERT INTO {name} (query_id, query_text, tsquery, lang, metadata)
        VALUES (%s, %s, plainto_tsquery(%s, %s), %s, %s::jsonb)
        ON CONFLICT (query_id) DO UPDATE SET
            query_text = EXCLUDED.query_text,
            tsquery = EXCLUDED.tsquery,
            lang = EXCLUDED.lang,
            metadata = EXCLUDED.metadata
    """, (query_id, query, lang, query, lang, metadata_json))


async def percolate(conn, name, text, lang="english", limit=50):
    _validate_identifier(name)
    rows = await _fetch(conn, f"""
        SELECT query_id, query_text, metadata,
            ts_rank(to_tsvector(%s, %s), tsquery) AS _score
        FROM {name}
        WHERE to_tsvector(%s, %s) @@ tsquery
        ORDER BY _score DESC LIMIT %s
    """, (lang, text, lang, text, limit))
    return _rows_to_dicts(rows)


async def percolate_delete(conn, name, query_id):
    _validate_identifier(name)
    row = await _fetchrow(
        conn, f"DELETE FROM {name} WHERE query_id = %s RETURNING query_id", (query_id,),
    )
    return row is not None


# -- Analysis ---------------------------------------------------------------

async def analyze(conn, text, lang="english"):
    rows = await _fetch(
        conn,
        "SELECT alias, description, token, dictionaries, dictionary, lexemes FROM ts_debug(%s, %s)",
        (lang, text),
    )
    return _rows_to_dicts(rows)


async def explain_score(conn, table, column, query, id_column, id_value, lang="english"):
    _validate_identifier(table)
    _validate_identifier(column)
    _validate_identifier(id_column)
    row = await _fetchrow(conn, f"""
        SELECT
            {column} AS document_text,
            to_tsvector(%s, {column})::text AS document_tokens,
            plainto_tsquery(%s, %s)::text AS query_tokens,
            to_tsvector(%s, {column}) @@ plainto_tsquery(%s, %s) AS matches,
            ts_rank(to_tsvector(%s, {column}), plainto_tsquery(%s, %s)) AS score,
            ts_headline(%s, {column}, plainto_tsquery(%s, %s),
                'StartSel=**, StopSel=**, MaxWords=50, MinWords=20') AS headline
        FROM {table}
        WHERE {id_column} = %s
    """, (lang, lang, query, lang, lang, query, lang, lang, query, lang, lang, query, id_value))
    return _row_to_dict(row)


# -- Document store ---------------------------------------------------------

def _doc_table(patterns):
    if patterns is None:
        raise RuntimeError(
            "doc_* utils now require DDL patterns from the proxy — call via "
            "`gl.documents.<verb>(...)` rather than the utils function directly."
        )
    return patterns["tables"]["main"]


def _doc_index_name(table, suffix):
    bare = table.rsplit(".", 1)[-1]
    return f"idx_{bare}_{suffix}"


async def doc_create_collection(conn, collection, unlogged=False, *, patterns=None):
    _validate_identifier(collection)
    if patterns is None:
        raise RuntimeError(
            "doc_create_collection requires DDL patterns from the proxy — call via "
            "`gl.documents.create_collection(...)` rather than the utils function directly."
        )
    # The proxy already executed the DDL on its mgmt connection. Nothing left
    # for the wrapper to do.


async def doc_insert(conn, collection, document, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    row = await _fetchrow(
        conn,
        f"INSERT INTO {table} (data) VALUES (%s::jsonb) RETURNING _id, data, created_at",
        (json.dumps(document),),
    )
    return _decode_doc_row(_row_to_dict(row))


async def doc_insert_many(conn, collection, documents, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    placeholders = ", ".join(["(%s::jsonb)"] * len(documents))
    params = tuple(json.dumps(d) for d in documents)
    rows = await _fetch(
        conn,
        f"INSERT INTO {table} (data) VALUES {placeholders} RETURNING _id, data, created_at",
        params,
    )
    return _decode_doc_rows(_rows_to_dicts(rows))


def _build_doc_find_sql(table, filter=None, sort=None, limit=None, skip=None):
    sql = f"SELECT _id, data, created_at FROM {table}"
    params = []
    where_clause, filter_params = _build_filter(filter)
    if where_clause:
        sql += " WHERE " + where_clause
        params.extend(filter_params)
    if sort:
        order_parts = []
        for key, direction in sort.items():
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", key):
                raise ValueError(f"Invalid sort key: {key}")
            order_parts.append(f"data->>'{key}' {'ASC' if direction == 1 else 'DESC'}")
        sql += " ORDER BY " + ", ".join(order_parts)
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
    if skip is not None:
        sql += " OFFSET %s"
        params.append(skip)
    return sql, params


async def doc_find(conn, collection, filter=None, sort=None, limit=None, skip=None, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    sql, params = _build_doc_find_sql(table, filter, sort, limit, skip)
    rows = await _fetch(conn, sql, tuple(params))
    return _decode_doc_rows(_rows_to_dicts(rows))


async def doc_find_cursor(
    conn, collection, filter=None, sort=None, limit=None, skip=None, batch_size=100, *, patterns=None,
):
    """Async generator version of doc_find_cursor.

    asyncpg cursors are scoped to a transaction — we open one, iterate, yield,
    and close in a finally. Matches psycopg2's server-side cursor semantics.
    """
    _validate_identifier(collection)
    table = _doc_table(patterns)
    sql, params = _build_doc_find_sql(table, filter, sort, limit, skip)
    sql2, params2 = _to_asyncpg(sql, tuple(params))
    raw = _get_raw_connection(conn)
    async with raw.transaction():
        cur = await raw.cursor(sql2, *params2)
        while True:
            rows = await cur.fetch(batch_size)
            if not rows:
                break
            for row in rows:
                yield _decode_doc_row(dict(row))


async def doc_find_one(conn, collection, filter=None, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    sql = f"SELECT _id, data, created_at FROM {table}"
    params = []
    where_clause, filter_params = _build_filter(filter)
    if where_clause:
        sql += " WHERE " + where_clause
        params.extend(filter_params)
    sql += " LIMIT 1"
    row = await _fetchrow(conn, sql, tuple(params))
    return _decode_doc_row(_row_to_dict(row))


async def doc_update(conn, collection, filter, update, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    where_clause, filter_params = _build_filter(filter)
    update_expr, update_params = _build_update(update)
    sql = f"UPDATE {table} SET data = {update_expr}"
    params = list(update_params)
    if where_clause:
        sql += " WHERE " + where_clause
        params.extend(filter_params)
    status = await _execute(conn, sql, tuple(params))
    return _rowcount_from_status(status)


async def doc_update_one(conn, collection, filter, update, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    where_clause, filter_params = _build_filter(filter)
    update_expr, update_params = _build_update(update)
    cte_where = " WHERE " + where_clause if where_clause else ""
    sql = (
        f"WITH target AS (SELECT _id FROM {table}{cte_where} LIMIT 1) "
        f"UPDATE {table} SET data = {update_expr} FROM target WHERE {table}._id = target._id"
    )
    params = list(filter_params) + list(update_params)
    status = await _execute(conn, sql, tuple(params))
    return _rowcount_from_status(status)


async def doc_delete(conn, collection, filter, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    where_clause, filter_params = _build_filter(filter)
    sql = f"DELETE FROM {table}"
    if where_clause:
        sql += " WHERE " + where_clause
    status = await _execute(conn, sql, tuple(filter_params))
    return _rowcount_from_status(status)


async def doc_delete_one(conn, collection, filter, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    where_clause, filter_params = _build_filter(filter)
    cte_where = " WHERE " + where_clause if where_clause else ""
    status = await _execute(
        conn,
        f"WITH target AS (SELECT _id FROM {table}{cte_where} LIMIT 1) "
        f"DELETE FROM {table} USING target WHERE {table}._id = target._id",
        tuple(filter_params),
    )
    return _rowcount_from_status(status)


async def doc_count(conn, collection, filter=None, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    sql = f"SELECT COUNT(*) FROM {table}"
    params = []
    where_clause, filter_params = _build_filter(filter)
    if where_clause:
        sql += " WHERE " + where_clause
        params.extend(filter_params)
    val = await _fetchval(conn, sql, tuple(params))
    return val


async def doc_find_one_and_update(conn, collection, filter, update, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    where_clause, filter_params = _build_filter(filter)
    update_expr, update_params = _build_update(update)
    cte_where = " WHERE " + where_clause if where_clause else ""
    sql = (
        f"WITH target AS (SELECT _id FROM {table}{cte_where} LIMIT 1) "
        f"UPDATE {table} SET data = {update_expr} FROM target "
        f"WHERE {table}._id = target._id "
        f"RETURNING {table}._id, {table}.data, {table}.created_at"
    )
    params = list(filter_params) + list(update_params)
    row = await _fetchrow(conn, sql, tuple(params))
    return _decode_doc_row(_row_to_dict(row))


async def doc_find_one_and_delete(conn, collection, filter, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    where_clause, filter_params = _build_filter(filter)
    cte_where = " WHERE " + where_clause if where_clause else ""
    sql = (
        f"WITH target AS (SELECT _id FROM {table}{cte_where} LIMIT 1) "
        f"DELETE FROM {table} USING target "
        f"WHERE {table}._id = target._id "
        f"RETURNING {table}._id, {table}.data, {table}.created_at"
    )
    row = await _fetchrow(conn, sql, tuple(filter_params))
    return _decode_doc_row(_row_to_dict(row))


async def doc_distinct(conn, collection, field, filter=None, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    field_expr = _field_path(field)
    sql = f"SELECT DISTINCT {field_expr} FROM {table}"
    params = []
    where_parts = [f"{field_expr} IS NOT NULL"]
    where_clause, filter_params = _build_filter(filter)
    if where_clause:
        where_parts.append(where_clause)
        params.extend(filter_params)
    sql += " WHERE " + " AND ".join(where_parts)
    rows = await _fetch(conn, sql, tuple(params))
    return [r[0] for r in rows]


async def doc_create_index(conn, collection, keys=None, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    if keys is None:
        idx = _doc_index_name(table, "gin")
        await _execute(
            conn,
            f"CREATE INDEX IF NOT EXISTS {idx} ON {table} USING GIN (data)",
        )
    else:
        for key in keys:
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", key):
                raise ValueError(f"Invalid key: {key}")
            idx = _doc_index_name(table, key)
            await _execute(
                conn,
                f"CREATE INDEX IF NOT EXISTS {idx} "
                f"ON {table} ((data->>'{key}'))",
            )


# -- Aggregation pipeline ---------------------------------------------------

async def doc_aggregate(conn, collection, pipeline, *, patterns=None, lookup_tables=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    _SUPPORTED_STAGES = {
        "$match", "$group", "$sort", "$limit", "$skip",
        "$project", "$unwind", "$lookup",
    }
    match_filter = None
    group_stage = None
    sort_stage = None
    limit_val = None
    skip_val = None
    project_stage = None
    unwind_stages = []
    lookup_stages = []
    for stage in pipeline:
        key = next(iter(stage))
        if key not in _SUPPORTED_STAGES:
            raise ValueError(f"Unsupported pipeline stage: {key}")
        if key == "$match":
            match_filter = stage[key]
        elif key == "$group":
            group_stage = stage[key]
        elif key == "$sort":
            sort_stage = stage[key]
        elif key == "$limit":
            limit_val = stage[key]
        elif key == "$skip":
            skip_val = stage[key]
        elif key == "$project":
            project_stage = stage[key]
        elif key == "$unwind":
            spec = stage[key]
            if isinstance(spec, str):
                path = spec
            elif isinstance(spec, dict):
                path = spec.get("path", "")
            else:
                raise ValueError(f"Invalid $unwind value: {spec}")
            if not isinstance(path, str) or not path.startswith("$"):
                raise ValueError(f"$unwind path must be a string starting with '$': {path}")
            field = path[1:]
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", field):
                raise ValueError(f"Invalid field name: {field}")
            unwind_stages.append(field)
        elif key == "$lookup":
            spec = stage[key]
            for required in ("from", "localField", "foreignField", "as"):
                if required not in spec:
                    raise ValueError(f"$lookup missing required field: {required}")
            from_table = spec["from"]
            _validate_identifier(from_table)
            local_field = spec["localField"]
            foreign_field = spec["foreignField"]
            as_name = spec["as"]
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", local_field):
                raise ValueError(f"Invalid field name: {local_field}")
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", foreign_field):
                raise ValueError(f"Invalid field name: {foreign_field}")
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", as_name):
                raise ValueError(f"Invalid identifier: {as_name}")
            lookup_stages.append({
                "from": from_table,
                "localField": local_field,
                "foreignField": foreign_field,
                "as": as_name,
            })

    unwind_map = {}
    from_extras = []
    for field in unwind_stages:
        alias = f"_unwound_{field}"
        unwind_map[field] = alias
        from_extras.append(f"jsonb_array_elements_text(data->'{field}') AS {alias}")

    params = []
    group_by = None
    if group_stage:
        _, group_by = _build_group(group_stage, unwind_map)
    if project_stage:
        group_aliases = None
        if group_stage:
            group_aliases = set()
            for k in group_stage:
                if k == "_id":
                    group_aliases.add("_id")
                else:
                    group_aliases.add(k)
        select_parts = _build_project(project_stage, group_aliases)
    elif group_stage:
        select_parts, group_by = _build_group(group_stage, unwind_map)
    else:
        select_parts = ["_id", "data", "created_at"]

    for lookup in lookup_stages:
        local_expr = _field_path(lookup["localField"])
        foreign_parts = lookup["foreignField"].split(".")
        for part in foreign_parts:
            if not _FIELD_PART_RE.match(part):
                raise ValueError(f"Invalid filter key: {lookup['foreignField']}")
        if len(foreign_parts) == 1:
            foreign_expr = f"b.data->>'{foreign_parts[0]}'"
        else:
            foreign_expr = "b.data"
            for part in foreign_parts[:-1]:
                foreign_expr += f"->'{part}'"
            foreign_expr += f"->>'{foreign_parts[-1]}'"
        from_table = lookup_tables.get(lookup["from"], lookup["from"]) if lookup_tables else lookup["from"]
        subquery = (
            f"COALESCE((SELECT json_agg(b.data) FROM {from_table} b "
            f"WHERE {foreign_expr} = {table}.{local_expr}), '[]'::json) "
            f"AS {lookup['as']}"
        )
        select_parts.append(subquery)

    from_clause = table
    for extra in from_extras:
        from_clause += f", {extra}"

    sql = f"SELECT {', '.join(select_parts)} FROM {from_clause}"

    where_clause, filter_params = _build_filter(match_filter)
    if where_clause:
        sql += " WHERE " + where_clause
        params.extend(filter_params)
    if group_by:
        sql += f" GROUP BY {group_by}"
    if sort_stage:
        order_parts = []
        for key, direction in sort_stage.items():
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", key):
                raise ValueError(f"Invalid sort key: {key}")
            dir_str = "ASC" if direction == 1 else "DESC"
            if group_stage or project_stage:
                order_parts.append(f"{key} {dir_str}")
            else:
                order_parts.append(f"data->>'{key}' {dir_str}")
        sql += " ORDER BY " + ", ".join(order_parts)
    if limit_val is not None:
        sql += " LIMIT %s"
        params.append(limit_val)
    if skip_val is not None:
        sql += " OFFSET %s"
        params.append(skip_val)

    rows = await _fetch(conn, sql, tuple(params))
    # When the pipeline has no $project/$group, the bare docs flow through
    # with `data` as jsonb — decode defensively. For projected / grouped
    # pipelines, there's no `data` field, so _decode_doc_rows is a no-op.
    return _decode_doc_rows(_rows_to_dicts(rows))


# -- Watch / TTL / capped ---------------------------------------------------

def _dsn_for_listen(asyncpg_conn):
    """Reconstruct a DSN for opening a fresh LISTEN connection.

    asyncpg stores connect params on the connection. We use `_params` (private
    but stable across releases) to round-trip DSN. Falls back to any `_dsn`
    attribute a wrapper might have stashed.
    """
    if hasattr(asyncpg_conn, "_dsn") and asyncpg_conn._dsn:
        return asyncpg_conn._dsn
    # asyncpg's private _params is a ConnectionParameters namedtuple-ish.
    # Fallback: reconstruct from _addr + _params.user/password/database.
    addr = getattr(asyncpg_conn, "_addr", None)
    params = getattr(asyncpg_conn, "_params", None)
    if addr and params:
        host, port = addr
        user = getattr(params, "user", None) or ""
        password = getattr(params, "password", None) or ""
        database = getattr(params, "database", None) or ""
        auth = f"{user}:{password}@" if password else (f"{user}@" if user else "")
        return f"postgresql://{auth}{host}:{port}/{database}"
    raise RuntimeError("Cannot determine DSN for LISTEN connection")


async def _register_jsonb_codec(conn):
    """No-op placeholder — kept for API compat with callers.

    We intentionally do NOT register an asyncpg JSONB codec. Registering
    `encoder=json.dumps, decoder=json.loads` interferes with the sync-style
    SQL the util layer generates: filters emit `data @> %s::jsonb` with a
    `json.dumps(...)` text parameter, and asyncpg's codec machinery would
    double-encode on that path (match returns zero rows). Instead, the
    doc_* utils post-process results with `_maybe_json_decode`, which gives
    a predictable dict/list shape regardless of which asyncpg internal
    codepath is taken. See test_v02_asyncio_integration.TestSyncAsyncParity
    for the parity contract this preserves.
    """
    # Intentionally empty — see docstring.
    return None


def _maybe_json_decode(value):
    """If `value` is a JSON-formatted string, return the decoded dict/list.
    Otherwise (already a dict/list/None), return it unchanged.

    Used on the `data` field of doc_* results to guarantee a consistent
    return shape whether asyncpg's jsonb codec ran or not.
    """
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    return value


def _decode_doc_row(d):
    """Decode the `data` field of a doc_* result dict if it came back as a
    string instead of a native dict/list."""
    if d is None:
        return None
    if "data" in d:
        d["data"] = _maybe_json_decode(d["data"])
    return d


def _decode_doc_rows(rows):
    return [_decode_doc_row(r) for r in rows]


async def doc_watch(conn, collection, callback, blocking=True, *, patterns=None):
    """Watch a collection for changes via triggers + pg_notify.

    Async version uses asyncpg's add_listener on a fresh connection.
    Callback is invoked as callback(event_dict) — same signature as sync.
    """
    import asyncio

    _validate_identifier(collection)
    table = _doc_table(patterns)
    await _execute(conn, f"""
        CREATE OR REPLACE FUNCTION _gl_watch_{collection}() RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify('_gl_changes_{collection}', json_build_object(
                'operationType', lower(TG_OP),
                '_id', COALESCE(NEW._id, OLD._id)::text,
                'fullDocument', CASE WHEN TG_OP = 'DELETE' THEN NULL ELSE NEW.data END
            )::text);
            RETURN COALESCE(NEW, OLD);
        END;
        $$ LANGUAGE plpgsql
    """)
    # CREATE OR REPLACE TRIGGER (Postgres 14+) is atomic — no window where
    # the trigger is missing, and a redefinition cleanly replaces the old
    # one instead of being swallowed by `EXCEPTION WHEN duplicate_object`.
    # GL targets PG14+, so this is safe and matches the Go wrapper.
    await _execute(conn, f"""
        CREATE OR REPLACE TRIGGER _gl_watch_{collection}_trigger
            AFTER INSERT OR UPDATE OR DELETE ON {table}
            FOR EACH ROW EXECUTE FUNCTION _gl_watch_{collection}()
    """)

    channel = f"_gl_changes_{collection}"
    raw = _get_raw_connection(conn)
    dsn = _dsn_for_listen(raw)

    import asyncpg
    listen_conn = await asyncpg.connect(dsn)

    def _cb(_c, _pid, _chan, payload):
        event = json.loads(payload)
        callback(event)

    await listen_conn.add_listener(channel, _cb)

    if not blocking:
        return listen_conn

    try:
        await asyncio.Event().wait()
    finally:
        await listen_conn.remove_listener(channel, _cb)
        await listen_conn.close()


async def doc_unwatch(conn, collection, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    await _execute(conn, f"DROP TRIGGER IF EXISTS _gl_watch_{collection}_trigger ON {table}")
    await _execute(conn, f"DROP FUNCTION IF EXISTS _gl_watch_{collection}()")


async def doc_create_ttl_index(conn, collection, expire_after_seconds, field="created_at", *, patterns=None):
    _validate_identifier(collection)
    _validate_identifier(field)
    if not isinstance(expire_after_seconds, int):
        raise ValueError("expire_after_seconds must be an integer")
    table = _doc_table(patterns)
    idx_ttl = _doc_index_name(table, "ttl")
    await _execute(
        conn, f"CREATE INDEX IF NOT EXISTS {idx_ttl} ON {table} ({field})",
    )
    expire_int = int(expire_after_seconds)
    await _execute(conn, f"""
        CREATE OR REPLACE FUNCTION _gl_ttl_{collection}() RETURNS TRIGGER AS $$
        BEGIN
            DELETE FROM {table} WHERE {field} < NOW() - INTERVAL '{expire_int} seconds';
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
    """)
    # CREATE OR REPLACE TRIGGER (Postgres 14+): atomic and redefinable.
    # See doc_watch for rationale.
    await _execute(conn, f"""
        CREATE OR REPLACE TRIGGER _gl_ttl_{collection}_trigger
            BEFORE INSERT ON {table}
            FOR EACH STATEMENT EXECUTE FUNCTION _gl_ttl_{collection}()
    """)


async def doc_remove_ttl_index(conn, collection, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    idx_ttl = _doc_index_name(table, "ttl")
    await _execute(conn, f"DROP TRIGGER IF EXISTS _gl_ttl_{collection}_trigger ON {table}")
    await _execute(conn, f"DROP FUNCTION IF EXISTS _gl_ttl_{collection}()")
    await _execute(conn, f"DROP INDEX IF EXISTS {idx_ttl}")


async def doc_create_capped(conn, collection, max_documents, *, patterns=None):
    _validate_identifier(collection)
    if not isinstance(max_documents, int):
        raise ValueError("max_documents must be an integer")
    table = _doc_table(patterns)
    idx_created = _doc_index_name(table, "created_at")
    await _execute(
        conn,
        f"CREATE INDEX IF NOT EXISTS {idx_created} "
        f"ON {table} (created_at ASC)",
    )
    max_int = int(max_documents)
    await _execute(conn, f"""
        CREATE OR REPLACE FUNCTION _gl_cap_{collection}() RETURNS TRIGGER AS $$
        DECLARE excess INTEGER;
        BEGIN
            SELECT COUNT(*) - {max_int} INTO excess FROM {table};
            IF excess > 0 THEN
                DELETE FROM {table} WHERE _id IN (
                    SELECT _id FROM {table} ORDER BY created_at ASC LIMIT excess
                );
            END IF;
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql
    """)
    # CREATE OR REPLACE TRIGGER (Postgres 14+): atomic and redefinable.
    # See doc_watch for rationale.
    await _execute(conn, f"""
        CREATE OR REPLACE TRIGGER _gl_cap_{collection}_trigger
            AFTER INSERT ON {table}
            FOR EACH STATEMENT EXECUTE FUNCTION _gl_cap_{collection}()
    """)


async def doc_remove_cap(conn, collection, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    await _execute(conn, f"DROP TRIGGER IF EXISTS _gl_cap_{collection}_trigger ON {table}")
    await _execute(conn, f"DROP FUNCTION IF EXISTS _gl_cap_{collection}()")
