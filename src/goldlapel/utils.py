"""
Redis-compatible convenience methods backed by PostgreSQL.

These methods provide a Redis-like API using native PostgreSQL features.
No Redis server needed — everything runs through your existing Postgres connection.

Usage:
    import goldlapel
    conn = goldlapel.start("postgresql://localhost/mydb")

    # Pub/sub
    goldlapel.publish(conn, "orders", "new order received")
    goldlapel.subscribe(conn, "orders", lambda channel, payload: print(payload))

    # Queues
    goldlapel.enqueue(conn, "jobs", {"task": "send_email", "to": "user@example.com"})
    job = goldlapel.dequeue(conn, "jobs")

    # Counters
    goldlapel.incr(conn, "page_views", "home")
"""

import hashlib
import json
import re
import select
import threading


def _validate_identifier(name):
    # Bound to 63 chars (Postgres NAMEDATALEN-1) so identifiers match the
    # proxy's server-side regex exactly: `^[A-Za-z_][A-Za-z0-9_]{0,62}$`.
    # Prevents client-side-only clients from slipping oversized names past
    # the wrapper into queries that would only fail on the proxy.
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]{0,62}$', name):
        raise ValueError(f"Invalid identifier: {name}")


def publish(conn, channel, message):
    """Publish a message to a channel. Like redis.publish()."""
    _validate_identifier(channel)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute("SELECT pg_notify(%s, %s)", (channel, str(message)))
    raw.commit()
    cur.close()


def subscribe(conn, channel, callback, blocking=True):
    """Subscribe to a channel and call callback(channel, payload) on each message.

    If blocking=False, runs in a background thread and returns the thread.
    Like redis.subscribe().
    """
    _validate_identifier(channel)
    raw = _get_raw_connection(conn)

    def _listen():
        listen_conn = _make_listen_connection(raw)
        cur = listen_conn.cursor()
        cur.execute(f"LISTEN {channel}")
        listen_conn.commit()
        cur.close()
        while True:
            if select.select([listen_conn], [], [], 5.0) != ([], [], []):
                listen_conn.poll()
                while listen_conn.notifies:
                    notify = listen_conn.notifies.pop(0)
                    callback(notify.channel, notify.payload)

    if blocking:
        _listen()
    else:
        t = threading.Thread(target=_listen, daemon=True)
        t.start()
        return t


# --
# Phase 5 Redis-compat families: counter, zset, hash, queue, geo.
#
# Each family's helpers consume `patterns` returned from the proxy's
# `/api/ddl/<family>/create` endpoint and translate `$1`-style placeholders
# to psycopg's `%s` via `_pattern_sql`. The proxy owns DDL — these helpers
# never CREATE TABLE.
# --


def _pattern_sql(patterns, key, family):
    """Pull a query pattern from the proxy's response and convert $N → %s.

    Family is only used to make the error message helpful when callers
    forget to pass `patterns=` (i.e. they invoked the util directly rather
    than via the namespaced API).
    """
    if patterns is None:
        raise RuntimeError(
            f"{family} utils require DDL patterns from the proxy — call via "
            f"`gl.{family}s.<verb>(...)` rather than the utils function directly."
        )
    from goldlapel.ddl import to_psycopg
    return to_psycopg(patterns["query_patterns"][key])


# ---------------------------------------------------------------------------
# Counter family
# ---------------------------------------------------------------------------


def counter_incr(conn, name, key, amount=1, *, patterns=None):
    """Increment-or-insert a counter; returns the new value."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(_pattern_sql(patterns, "incr", "counter"), (key, int(amount)))
    result = cur.fetchone()[0]
    raw.commit()
    cur.close()
    return result


def counter_decr(conn, name, key, amount=1, *, patterns=None):
    """Decrement is incr with a negative amount. Provided as a separate
    method so callers don't need to remember the sign convention."""
    return counter_incr(conn, name, key, -int(amount), patterns=patterns)


def counter_set(conn, name, key, value, *, patterns=None):
    """Idempotent set-key; returns the value just stored."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(_pattern_sql(patterns, "set", "counter"), (key, int(value)))
    result = cur.fetchone()[0]
    raw.commit()
    cur.close()
    return result


def counter_get(conn, name, key, *, patterns=None):
    """Get a counter's current value. Returns 0 for unknown keys (matches
    the Redis convention — no NULL surprise on cold cache)."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(_pattern_sql(patterns, "get", "counter"), (key,))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else 0


def counter_delete(conn, name, key, *, patterns=None):
    """Delete a counter row. Returns True if a row was deleted, False if
    the key was already absent."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(_pattern_sql(patterns, "delete", "counter"), (key,))
    removed = cur.rowcount > 0
    raw.commit()
    cur.close()
    return removed


def counter_count_keys(conn, name, *, patterns=None):
    """Total distinct keys in the counter namespace."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(_pattern_sql(patterns, "count_keys", "counter"))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else 0


# ---------------------------------------------------------------------------
# Sorted-set (zset) family
# ---------------------------------------------------------------------------


def zset_add(conn, name, zset_key, member, score, *, patterns=None):
    """Set-or-update a member's score under `zset_key`; returns the new score."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(
        _pattern_sql(patterns, "zadd", "zset"),
        (str(zset_key), str(member), float(score)),
    )
    result = cur.fetchone()[0]
    raw.commit()
    cur.close()
    return result


def zset_incr_by(conn, name, zset_key, member, delta=1, *, patterns=None):
    """Atomic increment-or-insert; returns the new score."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(
        _pattern_sql(patterns, "zincrby", "zset"),
        (str(zset_key), str(member), float(delta)),
    )
    result = cur.fetchone()[0]
    raw.commit()
    cur.close()
    return result


def zset_score(conn, name, zset_key, member, *, patterns=None):
    """Get a member's score, or None if absent."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(
        _pattern_sql(patterns, "zscore", "zset"),
        (str(zset_key), str(member)),
    )
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def zset_remove(conn, name, zset_key, member, *, patterns=None):
    """Remove a member; True if removed, False if absent."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(
        _pattern_sql(patterns, "zrem", "zset"),
        (str(zset_key), str(member)),
    )
    removed = cur.rowcount > 0
    raw.commit()
    cur.close()
    return removed


def zset_range(conn, name, zset_key, start=0, stop=10, desc=True, *, patterns=None):
    """Get members by rank within `zset_key`.

    Returns a list of (member, score) tuples. `desc=True` orders highest
    score first (leaderboard order). `start`/`stop` are 0-based inclusive
    bounds Redis-style; the SQL converts to LIMIT/OFFSET.
    """
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    key = "zrange_desc" if desc else "zrange_asc"
    limit = max(0, int(stop) - int(start) + 1)
    cur.execute(
        _pattern_sql(patterns, key, "zset"),
        (str(zset_key), limit, int(start)),
    )
    results = [(row[0], row[1]) for row in cur.fetchall()]
    cur.close()
    return results


def zset_range_by_score(conn, name, zset_key, min_score, max_score, limit=100, offset=0, *, patterns=None):
    """Get members whose score is between min and max (inclusive)."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(
        _pattern_sql(patterns, "zrangebyscore", "zset"),
        (str(zset_key), float(min_score), float(max_score), int(limit), int(offset)),
    )
    results = [(row[0], row[1]) for row in cur.fetchall()]
    cur.close()
    return results


def zset_rank(conn, name, zset_key, member, desc=True, *, patterns=None):
    """0-based rank within `zset_key`, or None if member absent."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    key = "zrank_desc" if desc else "zrank_asc"
    cur.execute(
        _pattern_sql(patterns, key, "zset"),
        (str(zset_key), str(member)),
    )
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def zset_card(conn, name, zset_key, *, patterns=None):
    """Cardinality of one zset_key namespace."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(_pattern_sql(patterns, "zcard", "zset"), (str(zset_key),))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else 0


# ---------------------------------------------------------------------------
# Hash family
# ---------------------------------------------------------------------------


def _decode_jsonb(value):
    """Coerce a JSONB column value into the user's Python object.

    psycopg/asyncpg with the default JSONB codec hand us dicts/lists/scalars
    already decoded. Other configurations hand back the raw text of the
    JSON document (str or bytes). Decode only in the second case; if the
    text isn't valid JSON, return it as-is rather than raising — the
    underlying column may have been written by something other than this
    helper, and surfacing a JSONDecodeError on `hash_get` would be
    user-hostile.
    """
    if not isinstance(value, (str, bytes)):
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


def hash_set(conn, name, hash_key, field, value, *, patterns=None):
    """Set a field's value (single-row UPSERT). Value is JSON-encoded."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(
        _pattern_sql(patterns, "hset", "hash"),
        (str(hash_key), str(field), json.dumps(value)),
    )
    row = cur.fetchone()
    raw.commit()
    cur.close()
    if row and row[0] is not None:
        return _decode_jsonb(row[0])
    return None


def hash_get(conn, name, hash_key, field, *, patterns=None):
    """Get a field's value, or None if (key, field) absent."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(
        _pattern_sql(patterns, "hget", "hash"),
        (str(hash_key), str(field)),
    )
    row = cur.fetchone()
    cur.close()
    if row and row[0] is not None:
        return _decode_jsonb(row[0])
    return None


def hash_get_all(conn, name, hash_key, *, patterns=None):
    """Reassemble every (field, value) under `hash_key` into a Python dict.
    Empty dict if the key has no fields."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(_pattern_sql(patterns, "hgetall", "hash"), (str(hash_key),))
    out = {}
    for row in cur.fetchall():
        out[row[0]] = _decode_jsonb(row[1])
    cur.close()
    return out


def hash_keys(conn, name, hash_key, *, patterns=None):
    """List every field name under `hash_key`."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(_pattern_sql(patterns, "hkeys", "hash"), (str(hash_key),))
    result = [row[0] for row in cur.fetchall()]
    cur.close()
    return result


def hash_values(conn, name, hash_key, *, patterns=None):
    """List every value under `hash_key` (in field-name order)."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(_pattern_sql(patterns, "hvals", "hash"), (str(hash_key),))
    result = [_decode_jsonb(row[0]) for row in cur.fetchall()]
    cur.close()
    return result


def hash_exists(conn, name, hash_key, field, *, patterns=None):
    """Does (hash_key, field) exist?"""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(
        _pattern_sql(patterns, "hexists", "hash"),
        (str(hash_key), str(field)),
    )
    row = cur.fetchone()
    cur.close()
    return bool(row[0]) if row else False


def hash_delete(conn, name, hash_key, field, *, patterns=None):
    """Delete a field; True if deleted, False if absent."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(
        _pattern_sql(patterns, "hdel", "hash"),
        (str(hash_key), str(field)),
    )
    removed = cur.rowcount > 0
    raw.commit()
    cur.close()
    return removed


def hash_len(conn, name, hash_key, *, patterns=None):
    """Number of fields under `hash_key`."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(_pattern_sql(patterns, "hlen", "hash"), (str(hash_key),))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else 0


# ---------------------------------------------------------------------------
# Queue family (at-least-once with visibility timeout)
# ---------------------------------------------------------------------------


def queue_enqueue(conn, name, payload, *, patterns=None):
    """Add a message; returns its assigned `id`."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(
        _pattern_sql(patterns, "enqueue", "queue"),
        (json.dumps(payload),),
    )
    row = cur.fetchone()
    raw.commit()
    cur.close()
    return row[0] if row else None


def queue_claim(conn, name, visibility_timeout_ms=30000, *, patterns=None):
    """Lease the next ready message. Returns `(id, payload)` or None if the
    queue is empty. Caller MUST `ack` or `abandon` (alias for `nack`) the id
    or the message becomes visible again after `visibility_timeout_ms`."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(
        _pattern_sql(patterns, "claim", "queue"),
        (int(visibility_timeout_ms),),
    )
    row = cur.fetchone()
    raw.commit()
    cur.close()
    if not row:
        return None
    return (row[0], _decode_jsonb(row[1]))


def queue_ack(conn, name, message_id, *, patterns=None):
    """Mark a claimed message done (DELETEs the row). Returns True if the
    message existed and was removed."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(_pattern_sql(patterns, "ack", "queue"), (int(message_id),))
    removed = cur.rowcount > 0
    raw.commit()
    cur.close()
    return removed


def queue_abandon(conn, name, message_id, *, patterns=None):
    """Release a claimed message back to ready immediately. Returns True if
    the message existed and was a claim. Equivalent to a NACK in queue
    parlance — the message stays in the queue and is redelivered to the
    next claim."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(_pattern_sql(patterns, "nack", "queue"), (int(message_id),))
    row = cur.fetchone()
    raw.commit()
    cur.close()
    return row is not None


def queue_extend(conn, name, message_id, additional_ms, *, patterns=None):
    """Extend a claimed message's visibility deadline by `additional_ms`
    milliseconds. Returns the new `visible_at`, or None if the id wasn't
    a claimed message."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(
        _pattern_sql(patterns, "extend", "queue"),
        (int(message_id), int(additional_ms)),
    )
    row = cur.fetchone()
    raw.commit()
    cur.close()
    return row[0] if row else None


def queue_peek(conn, name, *, patterns=None):
    """Look at the next-visible message without claiming it. Returns a dict
    with `id`, `payload`, `visible_at`, `status`, `created_at`, or None
    when nothing is ready."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(_pattern_sql(patterns, "peek", "queue"))
    row = cur.fetchone()
    cur.close()
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


def queue_count_ready(conn, name, *, patterns=None):
    """Count of messages currently ready (status='ready' and visible)."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(_pattern_sql(patterns, "count_ready", "queue"))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else 0


def queue_count_claimed(conn, name, *, patterns=None):
    """Count of currently-claimed messages (in flight)."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(_pattern_sql(patterns, "count_claimed", "queue"))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else 0


# ---------------------------------------------------------------------------
# Geo family (PostGIS GEOGRAPHY-native)
# ---------------------------------------------------------------------------


def geo_add(conn, name, member, lon, lat, *, patterns=None):
    """Set-or-update a member's lon/lat. Idempotent on the member name (PK).
    Returns the just-stored (lon, lat) tuple."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(
        _pattern_sql(patterns, "geoadd", "geo"),
        (str(member), float(lon), float(lat)),
    )
    row = cur.fetchone()
    raw.commit()
    cur.close()
    return (row[0], row[1]) if row else None


def geo_pos(conn, name, member, *, patterns=None):
    """Fetch a member's (lon, lat) tuple, or None if absent."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(_pattern_sql(patterns, "geopos", "geo"), (str(member),))
    row = cur.fetchone()
    cur.close()
    return (row[0], row[1]) if row else None


def geo_dist(conn, name, member_a, member_b, unit="m", *, patterns=None):
    """Distance between two members. `unit` accepts m / km / mi / ft.
    Returns float in the requested unit, or None if either member is absent."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(
        _pattern_sql(patterns, "geodist", "geo"),
        (str(member_a), str(member_b)),
    )
    row = cur.fetchone()
    cur.close()
    if not row or row[0] is None:
        return None
    return _convert_distance_meters(row[0], unit)


def geo_radius(conn, name, lon, lat, radius, unit="m", limit=50, *, patterns=None):
    """Members within `radius` of (lon, lat). Returns a list of dicts with
    `member`, `lon`, `lat`, `distance_m`.

    Proxy contract: $1=lon, $2=lat, $3=radius_m, $4=limit. The proxy
    computes the anchor geography once via CTE so each $N appears exactly
    once — same param tuple works for both psycopg %s translation and
    native-$N drivers.
    """
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    radius_m = _to_meters(radius, unit)
    cur.execute(
        _pattern_sql(patterns, "georadius_with_dist", "geo"),
        (float(lon), float(lat), float(radius_m), int(limit)),
    )
    cols = [desc[0] for desc in cur.description]
    results = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    return results


def geo_radius_by_member(conn, name, member, radius, unit="m", limit=50, *, patterns=None):
    """Members within `radius` of `member`'s location.

    Proxy contract: $1 and $2 are both the anchor member name (one for the
    join, one for the self-exclusion); $3=radius_m, $4=limit. After psycopg's
    $N → %s translation the markers appear in source order, so we pass
    `(member, radius_m, member, limit)` to match the in-SQL order
    `a.member=$1 ... ST_DWithin(..., $3) ... b.member<>$2 ... LIMIT $4`.
    """
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    radius_m = _to_meters(radius, unit)
    cur.execute(
        _pattern_sql(patterns, "geosearch_member", "geo"),
        (str(member), float(radius_m), str(member), int(limit)),
    )
    cols = [desc[0] for desc in cur.description]
    results = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    return results


def geo_remove(conn, name, member, *, patterns=None):
    """Delete a member; True if removed, False if absent."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(_pattern_sql(patterns, "geo_remove", "geo"), (str(member),))
    removed = cur.rowcount > 0
    raw.commit()
    cur.close()
    return removed


def geo_count(conn, name, *, patterns=None):
    """Total members in the namespace."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(_pattern_sql(patterns, "geo_count", "geo"))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else 0


# Distance unit conversion helpers — proxy returns meters always (GEOGRAPHY
# default); wrappers translate at the edge so callers can ask in km/mi/ft.
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


def script(conn, lua_code, *args):
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS pllua")
    raw.commit()
    func_name = "_gl_lua_" + format(abs(hash(lua_code)), 'x')[:8]
    tag = f"$_gl_{hashlib.md5(lua_code.encode()).hexdigest()[:8]}$"
    n = len(args)
    params = ", ".join([f"p{i+1} text" for i in range(n)])
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION pg_temp.{func_name}({params})
        RETURNS text LANGUAGE pllua AS {tag}
        {lua_code}
        {tag}
    """)
    if n > 0:
        placeholders = ", ".join(["%s"] * n)
        cur.execute(f"SELECT pg_temp.{func_name}({placeholders})", args)
    else:
        cur.execute(f"SELECT pg_temp.{func_name}()")
    result = cur.fetchone()
    cur.close()
    return result[0] if result else None


def count_distinct(conn, table, column):
    _validate_identifier(table)
    _validate_identifier(column)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"SELECT COUNT(DISTINCT {column}) FROM {table}")
    result = cur.fetchone()[0]
    cur.close()
    return result


def _stream_sql(patterns, key):
    """Translate the proxy's $N placeholders to psycopg's %s syntax."""
    from goldlapel.ddl import to_psycopg
    return to_psycopg(patterns["query_patterns"][key])


def stream_add(conn, stream, payload, *, patterns=None):
    if patterns is None:
        raise RuntimeError(
            "stream_add requires DDL patterns from the proxy — call via "
            "`gl.stream_add(...)` rather than the utils function directly."
        )
    _validate_identifier(stream)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(
        _stream_sql(patterns, "insert"),
        (json.dumps(payload),),
    )
    msg_id = cur.fetchone()[0]
    raw.commit()
    cur.close()
    return msg_id


def stream_create_group(conn, stream, group, *, patterns=None):
    if patterns is None:
        raise RuntimeError(
            "stream_create_group requires DDL patterns from the proxy — call via "
            "`gl.stream_create_group(...)` rather than the utils function directly."
        )
    _validate_identifier(stream)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(_stream_sql(patterns, "create_group"), (group,))
    raw.commit()
    cur.close()


def stream_read(conn, stream, group, consumer, count=1, *, patterns=None):
    if patterns is None:
        raise RuntimeError(
            "stream_read requires DDL patterns from the proxy — call via "
            "`gl.stream_read(...)` rather than the utils function directly."
        )
    _validate_identifier(stream)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(_stream_sql(patterns, "group_get_cursor"), (group,))
    row = cur.fetchone()
    if not row:
        cur.close()
        return []
    last_id = row[0]
    cur.execute(_stream_sql(patterns, "read_since"), (last_id, count))
    messages = []
    for r in cur.fetchall():
        msg_id, payload, created_at = r
        messages.append({
            "id": msg_id,
            "payload": payload if isinstance(payload, dict) else json.loads(payload),
            "created_at": str(created_at),
        })
    if messages:
        new_last = messages[-1]["id"]
        cur.execute(
            _stream_sql(patterns, "group_advance_cursor"),
            (new_last, group),
        )
        pending_insert = _stream_sql(patterns, "pending_insert")
        for msg in messages:
            cur.execute(pending_insert, (msg["id"], group, consumer))
    raw.commit()
    cur.close()
    return messages


def stream_ack(conn, stream, group, message_id, *, patterns=None):
    if patterns is None:
        raise RuntimeError(
            "stream_ack requires DDL patterns from the proxy — call via "
            "`gl.stream_ack(...)` rather than the utils function directly."
        )
    _validate_identifier(stream)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(_stream_sql(patterns, "ack"), (group, message_id))
    removed = cur.rowcount > 0
    raw.commit()
    cur.close()
    return removed


def stream_claim(conn, stream, group, consumer, min_idle_ms=60000, *, patterns=None):
    if patterns is None:
        raise RuntimeError(
            "stream_claim requires DDL patterns from the proxy — call via "
            "`gl.stream_claim(...)` rather than the utils function directly."
        )
    _validate_identifier(stream)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(
        _stream_sql(patterns, "claim"),
        (consumer, group, min_idle_ms),
    )
    claimed_ids = [r[0] for r in cur.fetchall()]
    messages = []
    if claimed_ids:
        read_by_id = _stream_sql(patterns, "read_by_id")
        for msg_id in claimed_ids:
            cur.execute(read_by_id, (msg_id,))
            r = cur.fetchone()
            if r:
                messages.append({
                    "id": r[0],
                    "payload": r[1] if isinstance(r[1], dict) else json.loads(r[1]),
                    "created_at": str(r[2]),
                })
    raw.commit()
    cur.close()
    return messages


def search(conn, table, column, query, limit=50, lang='english', highlight=False):
    """Full-text search with ranking. Like Elasticsearch match query."""
    _validate_identifier(table)
    if isinstance(column, str):
        columns = [column]
    else:
        columns = list(column)
    for col in columns:
        _validate_identifier(col)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    tsvec = " || ' ' || ".join(f"coalesce({col}, '')" for col in columns)
    if highlight:
        hl_col = columns[0]
        cur.execute(f"""
            SELECT *,
                ts_rank(to_tsvector(%s, {tsvec}), plainto_tsquery(%s, %s)) AS _score,
                ts_headline(%s, {hl_col}, plainto_tsquery(%s, %s),
                    'StartSel=<mark>, StopSel=</mark>, MaxWords=35, MinWords=15') AS _highlight
            FROM {table}
            WHERE to_tsvector(%s, {tsvec}) @@ plainto_tsquery(%s, %s)
            ORDER BY _score DESC LIMIT %s
        """, (lang, lang, query, lang, lang, query, lang, lang, query, limit))
    else:
        cur.execute(f"""
            SELECT *,
                ts_rank(to_tsvector(%s, {tsvec}), plainto_tsquery(%s, %s)) AS _score
            FROM {table}
            WHERE to_tsvector(%s, {tsvec}) @@ plainto_tsquery(%s, %s)
            ORDER BY _score DESC LIMIT %s
        """, (lang, lang, query, lang, lang, query, limit))
    cols = [desc[0] for desc in cur.description]
    results = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    return results


def search_fuzzy(conn, table, column, query, limit=50, threshold=0.3):
    """Typo-tolerant search. Like Elasticsearch fuzzy query."""
    _validate_identifier(table)
    _validate_identifier(column)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"""
        SELECT *, similarity({column}, %s) AS _score
        FROM {table}
        WHERE similarity({column}, %s) > %s
        ORDER BY _score DESC LIMIT %s
    """, (query, query, float(threshold), limit))
    cols = [desc[0] for desc in cur.description]
    results = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    return results


def search_phonetic(conn, table, column, query, limit=50):
    """Sound-alike search. Like Elasticsearch phonetic plugin."""
    _validate_identifier(table)
    _validate_identifier(column)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"""
        SELECT *, similarity({column}, %s) AS _score
        FROM {table}
        WHERE soundex({column}) = soundex(%s)
        ORDER BY _score DESC, {column} LIMIT %s
    """, (query, query, limit))
    cols = [desc[0] for desc in cur.description]
    results = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    return results


def similar(conn, table, column, vector, limit=10):
    """Vector similarity search. Like Elasticsearch kNN."""
    _validate_identifier(table)
    _validate_identifier(column)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    vec_literal = "[" + ",".join(str(float(v)) for v in vector) + "]"
    cur.execute(f"""
        SELECT *, ({column} <=> %s::vector) AS _score
        FROM {table}
        ORDER BY _score LIMIT %s
    """, (vec_literal, limit))
    cols = [desc[0] for desc in cur.description]
    results = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    return results


def suggest(conn, table, column, prefix, limit=10):
    """Autocomplete/typeahead. Like Elasticsearch completion suggester."""
    _validate_identifier(table)
    _validate_identifier(column)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    pattern = prefix + "%"
    cur.execute(f"""
        SELECT *, similarity({column}, %s) AS _score
        FROM {table}
        WHERE {column} ILIKE %s
        ORDER BY _score DESC, {column} LIMIT %s
    """, (prefix, pattern, limit))
    cols = [desc[0] for desc in cur.description]
    results = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    return results


def facets(conn, table, column, limit=50, query=None, query_column=None, lang='english'):
    """Get value counts for a column. Like Elasticsearch terms aggregation."""
    _validate_identifier(table)
    _validate_identifier(column)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    if query and query_column:
        if isinstance(query_column, str):
            query_columns = [query_column]
        else:
            query_columns = list(query_column)
        for qc in query_columns:
            _validate_identifier(qc)
        tsvec = " || ' ' || ".join(f"coalesce({qc}, '')" for qc in query_columns)
        cur.execute(f"""
            SELECT {column} AS value, COUNT(*) AS count
            FROM {table}
            WHERE to_tsvector(%s, {tsvec}) @@ plainto_tsquery(%s, %s)
            GROUP BY {column}
            ORDER BY count DESC, {column} LIMIT %s
        """, (lang, lang, query, limit))
    else:
        cur.execute(f"""
            SELECT {column} AS value, COUNT(*) AS count
            FROM {table}
            GROUP BY {column}
            ORDER BY count DESC, {column} LIMIT %s
        """, (limit,))
    cols = [desc[0] for desc in cur.description]
    results = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    return results


def aggregate(conn, table, column, func, group_by=None, limit=50):
    """Compute an aggregate over a column. Like Elasticsearch metric aggregations."""
    _validate_identifier(table)
    _validate_identifier(column)
    allowed = {'count', 'sum', 'avg', 'min', 'max'}
    if func not in allowed:
        raise ValueError(f"func must be one of {allowed}")
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    agg_expr = "COUNT(*)" if func == 'count' else f"{func.upper()}({column})"
    if group_by:
        _validate_identifier(group_by)
        cur.execute(f"""
            SELECT {group_by}, {agg_expr} AS value
            FROM {table}
            GROUP BY {group_by}
            ORDER BY value DESC LIMIT %s
        """, (limit,))
    else:
        cur.execute(f"""
            SELECT {agg_expr} AS value
            FROM {table}
        """)
    cols = [desc[0] for desc in cur.description]
    results = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    return results


def create_search_config(conn, name, copy_from='english'):
    """Create a custom text search configuration. Like Elasticsearch custom analyzer.

    Pass the config name as the lang parameter to search().
    """
    _validate_identifier(name)
    _validate_identifier(copy_from)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute("SELECT 1 FROM pg_ts_config WHERE cfgname = %s", (name,))
    if not cur.fetchone():
        cur.execute(f"CREATE TEXT SEARCH CONFIGURATION {name} (COPY = {copy_from})")
        raw.commit()
    cur.close()


def percolate_add(conn, name, query_id, query, lang='english', metadata=None):
    """Register a named query for reverse matching. Like Elasticsearch percolator."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {name} (
            query_id TEXT PRIMARY KEY,
            query_text TEXT NOT NULL,
            tsquery TSQUERY NOT NULL,
            lang TEXT NOT NULL DEFAULT 'english',
            metadata JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    cur.execute(f"CREATE INDEX IF NOT EXISTS {name}_tsq_idx ON {name} USING GIST (tsquery)")
    metadata_json = json.dumps(metadata) if metadata is not None else None
    cur.execute(f"""
        INSERT INTO {name} (query_id, query_text, tsquery, lang, metadata)
        VALUES (%s, %s, plainto_tsquery(%s, %s), %s, %s)
        ON CONFLICT (query_id) DO UPDATE SET
            query_text = EXCLUDED.query_text,
            tsquery = EXCLUDED.tsquery,
            lang = EXCLUDED.lang,
            metadata = EXCLUDED.metadata
    """, (query_id, query, lang, query, lang, metadata_json))
    raw.commit()
    cur.close()


def percolate(conn, name, text, lang='english', limit=50):
    """Match a document against stored queries. Like Elasticsearch percolate API."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"""
        SELECT query_id, query_text, metadata,
            ts_rank(to_tsvector(%s, %s), tsquery) AS _score
        FROM {name}
        WHERE to_tsvector(%s, %s) @@ tsquery
        ORDER BY _score DESC LIMIT %s
    """, (lang, text, lang, text, limit))
    cols = [desc[0] for desc in cur.description]
    results = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    return results


def percolate_delete(conn, name, query_id):
    """Remove a stored query from a percolator index."""
    _validate_identifier(name)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"DELETE FROM {name} WHERE query_id = %s RETURNING query_id", (query_id,))
    deleted = cur.fetchone() is not None
    raw.commit()
    cur.close()
    return deleted


def analyze(conn, text, lang='english'):
    """Show how text is tokenized. Like Elasticsearch _analyze API."""
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute("SELECT alias, description, token, dictionaries, dictionary, lexemes FROM ts_debug(%s, %s)", (lang, text))
    cols = [desc[0] for desc in cur.description]
    results = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    return results


def explain_score(conn, table, column, query, id_column, id_value, lang='english'):
    """Explain why a document scored what it did. Like Elasticsearch _explain API."""
    _validate_identifier(table)
    _validate_identifier(column)
    _validate_identifier(id_column)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"""
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
    cols = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    cur.close()
    if row is None:
        return None
    return dict(zip(cols, row))


_FIELD_PART_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

_COMPARISON_OPS = {
    "$gt": ">", "$gte": ">=", "$lt": "<", "$lte": "<=",
    "$eq": "=", "$ne": "!=",
}
_SUPPORTED_FILTER_OPS = set(_COMPARISON_OPS) | {"$in", "$nin", "$exists", "$regex", "$elemMatch", "$text"}
_LOGICAL_OPS = {"$or", "$and", "$not"}
_UPDATE_OPS = {"$set", "$unset", "$inc", "$mul", "$rename", "$push", "$pull", "$addToSet"}


def _field_path(key):
    parts = key.split(".")
    for part in parts:
        if not _FIELD_PART_RE.match(part):
            raise ValueError(f"Invalid filter key: {key}")
    if len(parts) == 1:
        return f"data->>'{parts[0]}'"
    arrow_chain = "data"
    for part in parts[:-1]:
        arrow_chain += f"->'{part}'"
    arrow_chain += f"->>'{parts[-1]}'"
    return arrow_chain


def _expand_dot_keys(d):
    result = {}
    for key, value in d.items():
        parts = key.split('.')
        current = result
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value
    return result


def _field_path_json(key):
    parts = key.split(".")
    for part in parts:
        if not _FIELD_PART_RE.match(part):
            raise ValueError(f"Invalid field key: {key}")
    chain = "data"
    for part in parts:
        chain += f"->'{part}'"
    return chain


def _jsonb_path(key):
    parts = key.split(".")
    for part in parts:
        if not _FIELD_PART_RE.match(part):
            raise ValueError(f"Invalid field key: {key}")
    return "{" + ",".join(parts) + "}"


def _to_jsonb_expr(value):
    if isinstance(value, bool):
        return "to_jsonb(%s::boolean)", value
    elif isinstance(value, (int, float)):
        return "to_jsonb(%s::numeric)", value
    elif isinstance(value, str):
        return "to_jsonb(%s::text)", value
    else:
        return "%s::jsonb", json.dumps(value)


def _build_update(update):
    if not any(k.startswith("$") for k in update):
        return "data || %s::jsonb", [json.dumps(update)]

    expr = "data"
    params = []

    if "$set" in update:
        expr = f"({expr} || %s::jsonb)"
        params.append(json.dumps(update["$set"]))

    if "$unset" in update:
        for field in update["$unset"]:
            parts = field.split(".")
            for part in parts:
                if not _FIELD_PART_RE.match(part):
                    raise ValueError(f"Invalid field key: {field}")
            if len(parts) == 1:
                expr = f"({expr} - %s)"
                params.append(field)
            else:
                path = "{" + ",".join(parts) + "}"
                expr = f"({expr} #- %s::text[])"
                params.append(path)

    if "$inc" in update:
        for field, amount in update["$inc"].items():
            jp = _jsonb_path(field)
            fp = _field_path(field)
            expr = f"jsonb_set({expr}, %s::text[], to_jsonb(COALESCE(({fp})::numeric, 0) + %s))"
            params.extend([jp, amount])

    if "$mul" in update:
        for field, factor in update["$mul"].items():
            jp = _jsonb_path(field)
            fp = _field_path(field)
            expr = f"jsonb_set({expr}, %s::text[], to_jsonb(COALESCE(({fp})::numeric, 0) * %s))"
            params.extend([jp, factor])

    if "$rename" in update:
        for old_name, new_name in update["$rename"].items():
            for part in old_name.split("."):
                if not _FIELD_PART_RE.match(part):
                    raise ValueError(f"Invalid field key: {old_name}")
            for part in new_name.split("."):
                if not _FIELD_PART_RE.match(part):
                    raise ValueError(f"Invalid field key: {new_name}")
            old_json = _field_path_json(old_name)
            new_jp = _jsonb_path(new_name)
            if "." in old_name:
                old_path = "{" + ",".join(old_name.split(".")) + "}"
                expr = f"jsonb_set(({expr} #- %s::text[]), %s::text[], {old_json})"
                params.extend([old_path, new_jp])
            else:
                expr = f"jsonb_set(({expr} - %s), %s::text[], {old_json})"
                params.extend([old_name, new_jp])

    if "$push" in update:
        for field, value in update["$push"].items():
            jp = _jsonb_path(field)
            fj = _field_path_json(field)
            val_expr, val_param = _to_jsonb_expr(value)
            expr = f"jsonb_set({expr}, %s::text[], COALESCE({fj}, '[]'::jsonb) || {val_expr})"
            params.extend([jp, val_param])

    if "$pull" in update:
        for field, value in update["$pull"].items():
            jp = _jsonb_path(field)
            fj = _field_path_json(field)
            val_expr, val_param = _to_jsonb_expr(value)
            expr = (
                f"jsonb_set({expr}, %s::text[], "
                f"COALESCE((SELECT jsonb_agg(elem) FROM jsonb_array_elements({fj}) AS elem "
                f"WHERE elem != {val_expr}), '[]'::jsonb))"
            )
            params.extend([jp, val_param])

    if "$addToSet" in update:
        for field, value in update["$addToSet"].items():
            jp = _jsonb_path(field)
            fj = _field_path_json(field)
            val_expr, val_param = _to_jsonb_expr(value)
            expr = (
                f"jsonb_set({expr}, %s::text[], "
                f"CASE WHEN COALESCE({fj}, '[]'::jsonb) @> {val_expr} "
                f"THEN {fj} "
                f"ELSE COALESCE({fj}, '[]'::jsonb) || {val_expr} END)"
            )
            params.extend([jp, val_param, val_param])

    return expr, params


def _build_filter(filter_dict):
    if not filter_dict:
        return "", []
    containment = {}
    clauses = []
    params = []
    for key, value in filter_dict.items():
        if key == "$text":
            if not isinstance(value, dict) or "$search" not in value:
                raise ValueError("$text requires {$search: 'query'}")
            lang = value.get("$language", "english")
            clauses.append("to_tsvector(%s, data::text) @@ plainto_tsquery(%s, %s)")
            params.extend([lang, lang, value["$search"]])
        elif key in _LOGICAL_OPS:
            if key == "$not":
                if not isinstance(value, dict):
                    raise ValueError("$not value must be a filter object")
                sub_clause, sub_params = _build_filter(value)
                if sub_clause:
                    clauses.append(f"NOT ({sub_clause})")
                    params.extend(sub_params)
            else:
                if not isinstance(value, list) or len(value) == 0:
                    raise ValueError(f"{key} value must be a non-empty array")
                joiner = " OR " if key == "$or" else " AND "
                sub_clauses = []
                for sub_filter in value:
                    sc, sp = _build_filter(sub_filter)
                    if sc:
                        sub_clauses.append(sc)
                        params.extend(sp)
                if sub_clauses:
                    clauses.append("(" + joiner.join(sub_clauses) + ")")
        elif isinstance(value, dict) and any(k.startswith("$") for k in value):
            field_expr = _field_path(key)
            for op, operand in value.items():
                if op in _COMPARISON_OPS:
                    sql_op = _COMPARISON_OPS[op]
                    if isinstance(operand, (int, float)):
                        clauses.append(f"({field_expr})::numeric {sql_op} %s")
                        params.append(operand)
                    else:
                        clauses.append(f"{field_expr} {sql_op} %s")
                        params.append(str(operand))
                elif op == "$in":
                    placeholders = ", ".join(["%s"] * len(operand))
                    clauses.append(f"{field_expr} IN ({placeholders})")
                    params.extend(str(v) for v in operand)
                elif op == "$nin":
                    placeholders = ", ".join(["%s"] * len(operand))
                    clauses.append(f"{field_expr} NOT IN ({placeholders})")
                    params.extend(str(v) for v in operand)
                elif op == "$exists":
                    parts = key.split(".")
                    top_key = parts[0]
                    if operand:
                        clauses.append("data ? %s")
                    else:
                        clauses.append("NOT (data ? %s)")
                    params.append(top_key)
                elif op == "$regex":
                    clauses.append(f"{field_expr} ~ %s")
                    params.append(operand)
                elif op == "$elemMatch":
                    if not isinstance(operand, dict):
                        raise ValueError("$elemMatch value must be an object")
                    field_json = _field_path_json(key)
                    elem_clauses = []
                    for sub_op, sub_val in operand.items():
                        if sub_op in _COMPARISON_OPS:
                            sql_op = _COMPARISON_OPS[sub_op]
                            if isinstance(sub_val, (int, float)):
                                elem_clauses.append(f"(elem#>>'{{}}')::numeric {sql_op} %s")
                                params.append(sub_val)
                            else:
                                elem_clauses.append(f"elem#>>'{{}}' {sql_op} %s")
                                params.append(str(sub_val))
                        elif sub_op == "$regex":
                            elem_clauses.append(f"elem#>>'{{}}' ~ %s")
                            params.append(sub_val)
                        else:
                            raise ValueError(f"Unsupported $elemMatch operator: {sub_op}")
                    if elem_clauses:
                        clauses.append(
                            f"EXISTS (SELECT 1 FROM jsonb_array_elements({field_json}) AS elem "
                            f"WHERE {' AND '.join(elem_clauses)})"
                        )
                elif op == "$text":
                    if not isinstance(operand, dict) or "$search" not in operand:
                        raise ValueError("$text requires {$search: 'query'}")
                    lang = operand.get("$language", "english")
                    clauses.append(f"to_tsvector(%s, {field_expr}) @@ plainto_tsquery(%s, %s)")
                    params.extend([lang, lang, operand["$search"]])
                else:
                    raise ValueError(f"Unsupported filter operator: {op}")
        else:
            containment[key] = value
    all_clauses = []
    all_params = []
    if containment:
        all_clauses.append("data @> %s::jsonb")
        all_params.append(json.dumps(_expand_dot_keys(containment)))
    all_clauses.extend(clauses)
    all_params.extend(params)
    return " AND ".join(all_clauses), all_params


def _doc_table(patterns):
    """Resolve the canonical doc-store table name from proxy-fetched patterns.

    Returns the FQ table name (e.g. `_goldlapel.doc_users`). Raises if
    `patterns` is None — the wrapper API never builds DDL itself anymore;
    `gl.documents.<verb>(...)` always supplies patterns.
    """
    if patterns is None:
        raise RuntimeError(
            "doc_* utils now require DDL patterns from the proxy — call via "
            "`gl.documents.<verb>(...)` rather than the utils function directly."
        )
    return patterns["tables"]["main"]


def _doc_index_name(table, suffix):
    """Build a deterministic index name from a (possibly schema-qualified)
    table reference. Strips any `schema.` prefix so the index name doesn't
    contain a dot — Postgres rejects those without quoting."""
    bare = table.rsplit(".", 1)[-1]
    return f"idx_{bare}_{suffix}"


def doc_create_collection(conn, collection, unlogged=False, *, patterns=None):
    """Eagerly materialize the doc-store table via the proxy.

    Idempotent: subsequent calls are no-ops on the proxy side (CREATE TABLE IF
    NOT EXISTS + ON CONFLICT DO NOTHING in schema_meta). The `unlogged` flag
    only takes effect on the first create — the proxy doesn't migrate storage
    type on subsequent calls.
    """
    _validate_identifier(collection)
    # Calling _patterns on DocumentsAPI already issued the create — nothing
    # left to do on the wrapper side. We accept `patterns=None` so direct
    # `doc_create_collection(conn, "users")` calls fail loud instead of
    # silently doing nothing.
    if patterns is None:
        raise RuntimeError(
            "doc_create_collection requires DDL patterns from the proxy — call via "
            "`gl.documents.create_collection(...)` rather than the utils function directly."
        )
    # No commit: the proxy already executed the DDL on its mgmt connection.


def doc_insert(conn, collection, document, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(
        f"INSERT INTO {table} (data) VALUES (%s::jsonb) RETURNING _id, data, created_at",
        (json.dumps(document),),
    )
    cols = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    raw.commit()
    cur.close()
    return dict(zip(cols, row))


def doc_insert_many(conn, collection, documents, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    placeholders = ", ".join(["(%s::jsonb)"] * len(documents))
    params = tuple(json.dumps(d) for d in documents)
    cur.execute(
        f"INSERT INTO {table} (data) VALUES {placeholders} RETURNING _id, data, created_at",
        params,
    )
    cols = [desc[0] for desc in cur.description]
    results = [dict(zip(cols, row)) for row in cur.fetchall()]
    raw.commit()
    cur.close()
    return results


def doc_find(conn, collection, filter=None, sort=None, limit=None, skip=None, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    sql = f"SELECT _id, data, created_at FROM {table}"
    params = []
    where_clause, filter_params = _build_filter(filter)
    if where_clause:
        sql += " WHERE " + where_clause
        params.extend(filter_params)
    if sort:
        order_parts = []
        for key, direction in sort.items():
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', key):
                raise ValueError(f"Invalid sort key: {key}")
            order_parts.append(f"data->>'{key}' {'ASC' if direction == 1 else 'DESC'}")
        sql += " ORDER BY " + ", ".join(order_parts)
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
    if skip is not None:
        sql += " OFFSET %s"
        params.append(skip)
    cur.execute(sql, tuple(params))
    cols = [desc[0] for desc in cur.description]
    results = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    return results


def doc_find_cursor(conn, collection, filter=None, sort=None, limit=None, skip=None, batch_size=100, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    raw = _get_raw_connection(conn)
    cur = raw.cursor(name=f"gl_cursor_{id(raw)}")
    sql = f"SELECT _id, data, created_at FROM {table}"
    params = []
    where_clause, filter_params = _build_filter(filter)
    if where_clause:
        sql += " WHERE " + where_clause
        params.extend(filter_params)
    if sort:
        order_parts = []
        for key, direction in sort.items():
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', key):
                raise ValueError(f"Invalid sort key: {key}")
            order_parts.append(f"data->>'{key}' {'ASC' if direction == 1 else 'DESC'}")
        sql += " ORDER BY " + ", ".join(order_parts)
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
    if skip is not None:
        sql += " OFFSET %s"
        params.append(skip)
    cur.execute(sql, tuple(params))
    cols = [desc[0] for desc in cur.description]
    try:
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            for row in rows:
                yield dict(zip(cols, row))
    finally:
        cur.close()


def doc_find_one(conn, collection, filter=None, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    sql = f"SELECT _id, data, created_at FROM {table}"
    params = []
    where_clause, filter_params = _build_filter(filter)
    if where_clause:
        sql += " WHERE " + where_clause
        params.extend(filter_params)
    sql += " LIMIT 1"
    cur.execute(sql, tuple(params))
    cols = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    cur.close()
    if row is None:
        return None
    return dict(zip(cols, row))


def doc_update(conn, collection, filter, update, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    where_clause, filter_params = _build_filter(filter)
    update_expr, update_params = _build_update(update)
    sql = f"UPDATE {table} SET data = {update_expr}"
    params = list(update_params)
    if where_clause:
        sql += " WHERE " + where_clause
        params.extend(filter_params)
    cur.execute(sql, tuple(params))
    rowcount = cur.rowcount
    raw.commit()
    cur.close()
    return rowcount


def doc_update_one(conn, collection, filter, update, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    where_clause, filter_params = _build_filter(filter)
    update_expr, update_params = _build_update(update)
    if where_clause:
        cte_where = " WHERE " + where_clause
    else:
        cte_where = ""
    sql = (
        f"WITH target AS (SELECT _id FROM {table}{cte_where} LIMIT 1) "
        f"UPDATE {table} SET data = {update_expr} FROM target WHERE {table}._id = target._id"
    )
    params = list(filter_params) + list(update_params)
    cur.execute(sql, tuple(params))
    rowcount = cur.rowcount
    raw.commit()
    cur.close()
    return rowcount


def doc_delete(conn, collection, filter, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    where_clause, filter_params = _build_filter(filter)
    sql = f"DELETE FROM {table}"
    if where_clause:
        sql += " WHERE " + where_clause
    cur.execute(sql, tuple(filter_params))
    rowcount = cur.rowcount
    raw.commit()
    cur.close()
    return rowcount


def doc_delete_one(conn, collection, filter, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    where_clause, filter_params = _build_filter(filter)
    if where_clause:
        cte_where = " WHERE " + where_clause
    else:
        cte_where = ""
    cur.execute(
        f"WITH target AS (SELECT _id FROM {table}{cte_where} LIMIT 1) "
        f"DELETE FROM {table} USING target WHERE {table}._id = target._id",
        tuple(filter_params),
    )
    rowcount = cur.rowcount
    raw.commit()
    cur.close()
    return rowcount


def doc_count(conn, collection, filter=None, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    sql = f"SELECT COUNT(*) FROM {table}"
    params = []
    where_clause, filter_params = _build_filter(filter)
    if where_clause:
        sql += " WHERE " + where_clause
        params.extend(filter_params)
    cur.execute(sql, tuple(params))
    result = cur.fetchone()[0]
    cur.close()
    return result


def doc_find_one_and_update(conn, collection, filter, update, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    where_clause, filter_params = _build_filter(filter)
    update_expr, update_params = _build_update(update)
    if where_clause:
        cte_where = " WHERE " + where_clause
    else:
        cte_where = ""
    sql = (
        f"WITH target AS (SELECT _id FROM {table}{cte_where} LIMIT 1) "
        f"UPDATE {table} SET data = {update_expr} FROM target "
        f"WHERE {table}._id = target._id "
        f"RETURNING {table}._id, {table}.data, {table}.created_at"
    )
    params = list(filter_params) + list(update_params)
    cur.execute(sql, tuple(params))
    cols = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    raw.commit()
    cur.close()
    if row is None:
        return None
    return dict(zip(cols, row))


def doc_find_one_and_delete(conn, collection, filter, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    where_clause, filter_params = _build_filter(filter)
    if where_clause:
        cte_where = " WHERE " + where_clause
    else:
        cte_where = ""
    sql = (
        f"WITH target AS (SELECT _id FROM {table}{cte_where} LIMIT 1) "
        f"DELETE FROM {table} USING target "
        f"WHERE {table}._id = target._id "
        f"RETURNING {table}._id, {table}.data, {table}.created_at"
    )
    cur.execute(sql, tuple(filter_params))
    cols = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    raw.commit()
    cur.close()
    if row is None:
        return None
    return dict(zip(cols, row))


def doc_distinct(conn, collection, field, filter=None, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    field_expr = _field_path(field)
    sql = f"SELECT DISTINCT {field_expr} FROM {table}"
    params = []
    where_parts = [f"{field_expr} IS NOT NULL"]
    where_clause, filter_params = _build_filter(filter)
    if where_clause:
        where_parts.append(where_clause)
        params.extend(filter_params)
    sql += " WHERE " + " AND ".join(where_parts)
    cur.execute(sql, tuple(params))
    result = [row[0] for row in cur.fetchall()]
    cur.close()
    return result


def doc_create_index(conn, collection, keys=None, *, patterns=None):
    _validate_identifier(collection)
    table = _doc_table(patterns)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    if keys is None:
        idx = _doc_index_name(table, "gin")
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS {idx} ON {table} USING GIN (data)"
        )
    else:
        for key in keys:
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', key):
                raise ValueError(f"Invalid key: {key}")
            idx = _doc_index_name(table, key)
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS {idx} ON {table} ((data->>'{key}'))"
            )
    raw.commit()
    cur.close()


def _resolve_field(field, unwind_map=None):
    if unwind_map and field in unwind_map:
        return unwind_map[field]
    return _field_path(field)


def _build_project(project, group_aliases=None):
    select_parts = []
    for key, val in project.items():
        if key == "_id" and val == 0:
            continue
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', key):
            raise ValueError(f"Invalid field name: {key}")
        if val == 1:
            if group_aliases and key in group_aliases:
                select_parts.append(key)
            else:
                select_parts.append(f"data->>'{key}' AS {key}")
        elif isinstance(val, str) and val.startswith("$"):
            field = val[1:]
            if group_aliases and field in group_aliases:
                select_parts.append(f"{field} AS {key}")
            else:
                expr = _field_path(field)
                select_parts.append(f"{expr} AS {key}")
        else:
            raise ValueError(f"Invalid $project value for {key}: {val}")
    return select_parts


def _build_group(group, unwind_map=None):
    _CAST_ACCUMULATORS = {"$avg", "$min", "$max", "$sum"}
    _SUPPORTED_ACCUMULATORS = _CAST_ACCUMULATORS | {"$count", "$push", "$addToSet"}
    select_parts = []
    group_by = None
    group_id = group.get("_id")
    if isinstance(group_id, str) and group_id.startswith("$"):
        field = group_id[1:]
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', field):
            raise ValueError(f"Invalid field name: {field}")
        resolved = _resolve_field(field, unwind_map)
        select_parts.append(f"{resolved} AS _id")
        group_by = resolved
    elif isinstance(group_id, dict):
        if not group_id:
            raise ValueError("Composite _id must have at least one field")
        parts_select = []
        parts_group = []
        for alias, ref in group_id.items():
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', alias):
                raise ValueError(f"Invalid alias: {alias}")
            if not isinstance(ref, str) or not ref.startswith("$"):
                raise ValueError(f"Invalid field reference: {ref}")
            field = ref[1:]
            expr = _resolve_field(field, unwind_map)
            parts_select.append(f"'{alias}', {expr}")
            parts_group.append(expr)
        select_parts.append(f"json_build_object({', '.join(parts_select)}) AS _id")
        group_by = ", ".join(parts_group)
    elif group_id is None:
        pass
    for alias, expr in group.items():
        if alias == "_id":
            continue
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', alias):
            raise ValueError(f"Invalid field name: {alias}")
        if not isinstance(expr, dict) or len(expr) != 1:
            raise ValueError(f"Invalid accumulator for {alias}")
        op = next(iter(expr))
        if op not in _SUPPORTED_ACCUMULATORS:
            raise ValueError(f"Unsupported accumulator: {op}")
        val = expr[op]
        if op == "$count":
            select_parts.append(f"COUNT(*) AS {alias}")
        elif op == "$sum":
            if val == 1:
                select_parts.append(f"COUNT(*) AS {alias}")
            elif isinstance(val, str) and val.startswith("$"):
                field = val[1:]
                if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', field):
                    raise ValueError(f"Invalid field name: {field}")
                resolved = _resolve_field(field, unwind_map)
                if unwind_map and field in unwind_map:
                    select_parts.append(f"SUM({resolved}::numeric) AS {alias}")
                else:
                    select_parts.append(f"SUM(({resolved})::numeric) AS {alias}")
            elif isinstance(val, (int, float)):
                select_parts.append(f"SUM({val}) AS {alias}")
            else:
                raise ValueError(f"Invalid $sum value: {val}")
        elif op == "$avg":
            if isinstance(val, str) and val.startswith("$"):
                field = val[1:]
                if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', field):
                    raise ValueError(f"Invalid field name: {field}")
                resolved = _resolve_field(field, unwind_map)
                if unwind_map and field in unwind_map:
                    select_parts.append(f"AVG({resolved}::numeric) AS {alias}")
                else:
                    select_parts.append(f"AVG(({resolved})::numeric) AS {alias}")
        elif op == "$min":
            if isinstance(val, str) and val.startswith("$"):
                field = val[1:]
                if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', field):
                    raise ValueError(f"Invalid field name: {field}")
                resolved = _resolve_field(field, unwind_map)
                if unwind_map and field in unwind_map:
                    select_parts.append(f"MIN({resolved}::numeric) AS {alias}")
                else:
                    select_parts.append(f"MIN(({resolved})::numeric) AS {alias}")
        elif op == "$max":
            if isinstance(val, str) and val.startswith("$"):
                field = val[1:]
                if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', field):
                    raise ValueError(f"Invalid field name: {field}")
                resolved = _resolve_field(field, unwind_map)
                if unwind_map and field in unwind_map:
                    select_parts.append(f"MAX({resolved}::numeric) AS {alias}")
                else:
                    select_parts.append(f"MAX(({resolved})::numeric) AS {alias}")
        elif op == "$push":
            if isinstance(val, str) and val.startswith("$"):
                field = val[1:]
                if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', field):
                    raise ValueError(f"Invalid field name: {field}")
                resolved = _resolve_field(field, unwind_map)
                select_parts.append(f"array_agg({resolved}) AS {alias}")
            else:
                raise ValueError(f"Invalid $push value: {val}")
        elif op == "$addToSet":
            if isinstance(val, str) and val.startswith("$"):
                field = val[1:]
                if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', field):
                    raise ValueError(f"Invalid field name: {field}")
                resolved = _resolve_field(field, unwind_map)
                select_parts.append(f"array_agg(DISTINCT {resolved}) AS {alias}")
            else:
                raise ValueError(f"Invalid $addToSet value: {val}")
    return select_parts, group_by


def doc_aggregate(conn, collection, pipeline, *, patterns=None, lookup_tables=None):
    """Run a Mongo-style aggregation pipeline.

    `lookup_tables` is a `{user_collection_name: canonical_proxy_table}` map
    used to rewrite `$lookup.from` references to their proxy-canonical FQ
    tables (`_goldlapel.doc_<name>`). Supplied by `gl.documents.aggregate` —
    direct util callers may omit it (in which case `from` is used verbatim).
    """
    _validate_identifier(collection)
    table = _doc_table(patterns)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
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
                raise ValueError(
                    f"$unwind path must be a string starting with '$': {path}"
                )
            field = path[1:]
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', field):
                raise ValueError(f"Invalid field name: {field}")
            unwind_stages.append(field)
        elif key == "$lookup":
            spec = stage[key]
            for required in ("from", "localField", "foreignField", "as"):
                if required not in spec:
                    raise ValueError(
                        f"$lookup missing required field: {required}"
                    )
            from_table = spec["from"]
            _validate_identifier(from_table)
            local_field = spec["localField"]
            foreign_field = spec["foreignField"]
            as_name = spec["as"]
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', local_field):
                raise ValueError(f"Invalid field name: {local_field}")
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', foreign_field):
                raise ValueError(f"Invalid field name: {foreign_field}")
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', as_name):
                raise ValueError(f"Invalid identifier: {as_name}")
            lookup_stages.append({
                "from": from_table,
                "localField": local_field,
                "foreignField": foreign_field,
                "as": as_name,
            })

    # Build unwind_map: field -> alias
    unwind_map = {}
    from_extras = []
    for field in unwind_stages:
        alias = f"_unwound_{field}"
        unwind_map[field] = alias
        from_extras.append(
            f"jsonb_array_elements_text(data->'{field}') AS {alias}"
        )

    # Build SELECT
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

    # Append $lookup subqueries to SELECT
    #
    # `lookup['from']` is the user-supplied collection name. The
    # `lookup_tables` map (when present) is supplied by `gl.documents.aggregate`
    # and resolves each `from` to its canonical proxy table (e.g. `users` →
    # `_goldlapel.doc_users`). When `aggregate` is called via the legacy code
    # path with no resolution map, the original collection name is used
    # verbatim — direct util callers are responsible for using fully-qualified
    # names if they want anything other than `public.<from>`.
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

    # Build FROM clause
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
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', key):
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
    cur.execute(sql, tuple(params))
    cols = [desc[0] for desc in cur.description]
    results = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    return results


def _get_raw_connection(conn):
    """Extract the raw psycopg/psycopg2 connection from a wrapped connection."""
    if hasattr(conn, '_conn'):
        return conn._conn
    return conn


def _make_listen_connection(conn):
    """Create a separate connection for LISTEN (reuses the same DSN)."""
    dsn = conn.info.dsn if hasattr(conn, 'info') else conn.dsn
    import psycopg2
    listen_conn = psycopg2.connect(dsn)
    listen_conn.set_isolation_level(0)  # autocommit
    return listen_conn


def doc_watch(conn, collection, callback, blocking=True, *, patterns=None):
    """Watch a collection for changes via triggers + pg_notify. Like MongoDB change streams."""
    _validate_identifier(collection)
    table = _doc_table(patterns)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()

    # Trigger / function / channel names are keyed off the user's collection
    # name (validated identifier — safe to interpolate). The trigger fires on
    # the canonical proxy table.
    cur.execute(f"""
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
    # the trigger is missing between drop and create, and a redefinition
    # cleanly replaces the old one instead of being silently swallowed by
    # `EXCEPTION WHEN duplicate_object`. GL targets PG14+ across the
    # product, so this is safe and matches the Go wrapper.
    cur.execute(f"""
        CREATE OR REPLACE TRIGGER _gl_watch_{collection}_trigger
            AFTER INSERT OR UPDATE OR DELETE ON {table}
            FOR EACH ROW EXECUTE FUNCTION _gl_watch_{collection}()
    """)
    raw.commit()
    cur.close()

    channel = f"_gl_changes_{collection}"

    def _listen():
        listen_conn = _make_listen_connection(raw)
        lcur = listen_conn.cursor()
        lcur.execute(f"LISTEN {channel}")
        listen_conn.commit()
        lcur.close()
        while True:
            if select.select([listen_conn], [], [], 5.0) != ([], [], []):
                listen_conn.poll()
                while listen_conn.notifies:
                    notify = listen_conn.notifies.pop(0)
                    event = json.loads(notify.payload)
                    callback(event)

    if blocking:
        _listen()
    else:
        t = threading.Thread(target=_listen, daemon=True)
        t.start()
        return t


def doc_unwatch(conn, collection, *, patterns=None):
    """Remove change stream trigger from a collection."""
    _validate_identifier(collection)
    table = _doc_table(patterns)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"DROP TRIGGER IF EXISTS _gl_watch_{collection}_trigger ON {table}")
    cur.execute(f"DROP FUNCTION IF EXISTS _gl_watch_{collection}()")
    raw.commit()
    cur.close()


def doc_create_ttl_index(conn, collection, expire_after_seconds, field="created_at", *, patterns=None):
    """Create a TTL index that deletes expired rows on each INSERT. Like MongoDB TTL indexes."""
    _validate_identifier(collection)
    _validate_identifier(field)
    if not isinstance(expire_after_seconds, int):
        raise ValueError("expire_after_seconds must be an integer")
    table = _doc_table(patterns)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()

    idx_ttl = _doc_index_name(table, "ttl")
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {idx_ttl} ON {table} ({field})"
    )

    expire_int = int(expire_after_seconds)
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION _gl_ttl_{collection}() RETURNS TRIGGER AS $$
        BEGIN
            DELETE FROM {table} WHERE {field} < NOW() - INTERVAL '{expire_int} seconds';
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
    """)

    # CREATE OR REPLACE TRIGGER (Postgres 14+): atomic and redefinable.
    # See doc_watch for rationale.
    cur.execute(f"""
        CREATE OR REPLACE TRIGGER _gl_ttl_{collection}_trigger
            BEFORE INSERT ON {table}
            FOR EACH STATEMENT EXECUTE FUNCTION _gl_ttl_{collection}()
    """)
    raw.commit()
    cur.close()


def doc_remove_ttl_index(conn, collection, *, patterns=None):
    """Remove TTL trigger, function, and index from a collection."""
    _validate_identifier(collection)
    table = _doc_table(patterns)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    idx_ttl = _doc_index_name(table, "ttl")
    cur.execute(f"DROP TRIGGER IF EXISTS _gl_ttl_{collection}_trigger ON {table}")
    cur.execute(f"DROP FUNCTION IF EXISTS _gl_ttl_{collection}()")
    cur.execute(f"DROP INDEX IF EXISTS {idx_ttl}")
    raw.commit()
    cur.close()


def doc_create_capped(conn, collection, max_documents, *, patterns=None):
    """Create a capped collection that auto-deletes oldest rows. Like MongoDB capped collections.

    The underlying doc-store table is already materialized by the proxy
    (DocumentsAPI._patterns issues create on first call). This call only adds
    the cap trigger + supporting index.
    """
    _validate_identifier(collection)
    if not isinstance(max_documents, int):
        raise ValueError("max_documents must be an integer")
    table = _doc_table(patterns)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()

    idx_created = _doc_index_name(table, "created_at")
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {idx_created} ON {table} (created_at ASC)"
    )

    max_int = int(max_documents)
    cur.execute(f"""
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
    cur.execute(f"""
        CREATE OR REPLACE TRIGGER _gl_cap_{collection}_trigger
            AFTER INSERT ON {table}
            FOR EACH STATEMENT EXECUTE FUNCTION _gl_cap_{collection}()
    """)
    raw.commit()
    cur.close()


def doc_remove_cap(conn, collection, *, patterns=None):
    """Remove capped collection trigger and function."""
    _validate_identifier(collection)
    table = _doc_table(patterns)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"DROP TRIGGER IF EXISTS _gl_cap_{collection}_trigger ON {table}")
    cur.execute(f"DROP FUNCTION IF EXISTS _gl_cap_{collection}()")
    raw.commit()
    cur.close()
