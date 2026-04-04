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

import json
import select
import threading


def publish(conn, channel, message):
    """Publish a message to a channel. Like redis.publish()."""
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


def enqueue(conn, queue_table, payload):
    """Add a job to a queue table. Like redis.lpush().

    Creates the queue table if it doesn't exist.
    Payload is stored as JSONB.
    """
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {queue_table} (
            id BIGSERIAL PRIMARY KEY,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    cur.execute(
        f"INSERT INTO {queue_table} (payload) VALUES (%s)",
        (json.dumps(payload),),
    )
    raw.commit()
    cur.close()


def dequeue(conn, queue_table):
    """Pop the next job from a queue table. Like redis.brpop() (non-blocking).

    Uses FOR UPDATE SKIP LOCKED for safe concurrent access.
    Returns the payload dict, or None if the queue is empty.
    """
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"""
        DELETE FROM {queue_table}
        WHERE id = (
            SELECT id FROM {queue_table}
            ORDER BY id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING payload
    """)
    row = cur.fetchone()
    raw.commit()
    cur.close()
    if row:
        return row[0] if isinstance(row[0], dict) else json.loads(row[0])
    return None


def incr(conn, table, key, amount=1):
    """Increment a counter. Like redis.incr().

    Creates the counter table if it doesn't exist.
    Returns the new value.
    """
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            key TEXT PRIMARY KEY,
            value BIGINT NOT NULL DEFAULT 0
        )
    """)
    cur.execute(f"""
        INSERT INTO {table} (key, value) VALUES (%s, %s)
        ON CONFLICT (key) DO UPDATE SET value = {table}.value + %s
        RETURNING value
    """, (key, amount, amount))
    result = cur.fetchone()[0]
    raw.commit()
    cur.close()
    return result


def get_counter(conn, table, key):
    """Get a counter value. Like redis.get() for a counter."""
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"SELECT value FROM {table} WHERE key = %s", (key,))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else 0


def hset(conn, table, key, field, value):
    """Set a field in a hash. Like redis.hset().

    Creates the hash table if it doesn't exist. Uses JSONB for storage.
    """
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            key TEXT PRIMARY KEY,
            data JSONB NOT NULL DEFAULT '{{}}'::jsonb
        )
    """)
    cur.execute(f"""
        INSERT INTO {table} (key, data) VALUES (%s, jsonb_build_object(%s, %s::jsonb))
        ON CONFLICT (key) DO UPDATE SET data = {table}.data || jsonb_build_object(%s, %s::jsonb)
    """, (key, field, json.dumps(value), field, json.dumps(value)))
    raw.commit()
    cur.close()


def hget(conn, table, key, field):
    """Get a field from a hash. Like redis.hget().

    Returns the value, or None if key or field doesn't exist.
    """
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"SELECT data->>%s FROM {table} WHERE key = %s", (field, key))
    row = cur.fetchone()
    cur.close()
    if row and row[0] is not None:
        try:
            return json.loads(row[0])
        except (json.JSONDecodeError, TypeError):
            return row[0]
    return None


def hgetall(conn, table, key):
    """Get all fields from a hash. Like redis.hgetall().

    Returns a dict, or empty dict if key doesn't exist.
    """
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"SELECT data FROM {table} WHERE key = %s", (key,))
    row = cur.fetchone()
    cur.close()
    if row and row[0]:
        return row[0] if isinstance(row[0], dict) else json.loads(row[0])
    return {}


def hdel(conn, table, key, field):
    """Remove a field from a hash. Like redis.hdel().

    Returns True if the field existed, False otherwise.
    """
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"SELECT data ? %s FROM {table} WHERE key = %s", (field, key))
    row = cur.fetchone()
    existed = row and row[0]
    if existed:
        cur.execute(f"UPDATE {table} SET data = data - %s WHERE key = %s", (field, key))
        raw.commit()
    cur.close()
    return bool(existed)


def zadd(conn, table, member, score):
    """Add a member with a score to a sorted set. Like redis.zadd().

    Creates the sorted set table if it doesn't exist.
    If the member already exists, updates the score.
    """
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            member TEXT PRIMARY KEY,
            score DOUBLE PRECISION NOT NULL
        )
    """)
    cur.execute(f"""
        INSERT INTO {table} (member, score) VALUES (%s, %s)
        ON CONFLICT (member) DO UPDATE SET score = EXCLUDED.score
    """, (str(member), float(score)))
    raw.commit()
    cur.close()


def zincrby(conn, table, member, amount=1):
    """Increment a member's score. Like redis.zincrby().

    Creates the member with the given amount if it doesn't exist.
    Returns the new score.
    """
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            member TEXT PRIMARY KEY,
            score DOUBLE PRECISION NOT NULL
        )
    """)
    cur.execute(f"""
        INSERT INTO {table} (member, score) VALUES (%s, %s)
        ON CONFLICT (member) DO UPDATE SET score = {table}.score + %s
        RETURNING score
    """, (str(member), float(amount), float(amount)))
    result = cur.fetchone()[0]
    raw.commit()
    cur.close()
    return result


def zrange(conn, table, start=0, stop=10, desc=True):
    """Get members by score rank. Like redis.zrange().

    Returns a list of (member, score) tuples.
    desc=True returns highest scores first (leaderboard order).
    """
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    order = "DESC" if desc else "ASC"
    limit = stop - start
    cur.execute(f"""
        SELECT member, score FROM {table}
        ORDER BY score {order}
        LIMIT %s OFFSET %s
    """, (limit, start))
    results = [(row[0], row[1]) for row in cur.fetchall()]
    cur.close()
    return results


def zrank(conn, table, member, desc=True):
    """Get the rank of a member. Like redis.zrank().

    Returns 0-based rank, or None if member doesn't exist.
    desc=True ranks by highest score first.
    """
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    order = "DESC" if desc else "ASC"
    cur.execute(f"""
        SELECT rank FROM (
            SELECT member, ROW_NUMBER() OVER (ORDER BY score {order}) - 1 AS rank
            FROM {table}
        ) ranked
        WHERE member = %s
    """, (str(member),))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def zscore(conn, table, member):
    """Get the score of a member. Like redis.zscore().

    Returns the score, or None if member doesn't exist.
    """
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"SELECT score FROM {table} WHERE member = %s", (str(member),))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def zrem(conn, table, member):
    """Remove a member from a sorted set. Like redis.zrem().

    Returns True if the member was removed, False if it didn't exist.
    """
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"DELETE FROM {table} WHERE member = %s", (str(member),))
    removed = cur.rowcount > 0
    raw.commit()
    cur.close()
    return removed


def georadius(conn, table, geom_column, lon, lat, radius_meters, limit=50):
    """Find rows within a radius of a point. Like redis.georadius().

    Requires PostGIS extension. Uses ST_DWithin with geography type
    for accurate distance on the Earth's surface.

    Returns a list of dicts with all columns plus a 'distance_m' field.
    """
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"""
        SELECT *, ST_Distance(
            {geom_column}::geography,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
        ) AS distance_m
        FROM {table}
        WHERE ST_DWithin(
            {geom_column}::geography,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
            %s
        )
        ORDER BY distance_m
        LIMIT %s
    """, (lon, lat, lon, lat, radius_meters, limit))
    columns = [desc[0] for desc in cur.description]
    results = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()
    return results


def geoadd(conn, table, name_column, geom_column, name, lon, lat):
    """Add a location to a geo table. Like redis.geoadd().

    Creates the table with PostGIS geometry column if it doesn't exist.
    Requires PostGIS extension.
    """
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id BIGSERIAL PRIMARY KEY,
            {name_column} TEXT NOT NULL,
            {geom_column} GEOMETRY(Point, 4326) NOT NULL
        )
    """)
    cur.execute(f"""
        INSERT INTO {table} ({name_column}, {geom_column})
        VALUES (%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
    """, (name, lon, lat))
    raw.commit()
    cur.close()


def geodist(conn, table, geom_column, name_column, name_a, name_b):
    """Get distance between two members in meters. Like redis.geodist()."""
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"""
        SELECT ST_Distance(a.{geom_column}::geography, b.{geom_column}::geography)
        FROM {table} a, {table} b
        WHERE a.{name_column} = %s AND b.{name_column} = %s
    """, (name_a, name_b))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def script(conn, lua_code, *args):
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS pllua")
    raw.commit()
    func_name = "_gl_lua_" + format(abs(hash(lua_code)), 'x')[:8]
    n = len(args)
    params = ", ".join([f"p{i+1} text" for i in range(n)])
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION pg_temp.{func_name}({params})
        RETURNS text LANGUAGE pllua AS $pllua$
        {lua_code}
        $pllua$
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
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"SELECT COUNT(DISTINCT {column}) FROM {table}")
    result = cur.fetchone()[0]
    cur.close()
    return result


def stream_add(conn, stream, payload):
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {stream} (
            id BIGSERIAL PRIMARY KEY,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    cur.execute(
        f"INSERT INTO {stream} (payload) VALUES (%s) RETURNING id",
        (json.dumps(payload),),
    )
    msg_id = cur.fetchone()[0]
    raw.commit()
    cur.close()
    return msg_id


def stream_create_group(conn, stream, group):
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {stream}_groups (
            group_name TEXT PRIMARY KEY,
            last_delivered_id BIGINT NOT NULL DEFAULT 0
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {stream}_pending (
            message_id BIGINT NOT NULL,
            group_name TEXT NOT NULL,
            consumer TEXT NOT NULL,
            claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            delivery_count INT NOT NULL DEFAULT 1,
            PRIMARY KEY (group_name, message_id)
        )
    """)
    cur.execute(
        f"INSERT INTO {stream}_groups (group_name) VALUES (%s) ON CONFLICT DO NOTHING",
        (group,),
    )
    raw.commit()
    cur.close()


def stream_read(conn, stream, group, consumer, count=1):
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(
        f"SELECT last_delivered_id FROM {stream}_groups WHERE group_name = %s FOR UPDATE",
        (group,),
    )
    row = cur.fetchone()
    if not row:
        cur.close()
        return []
    last_id = row[0]
    cur.execute(
        f"SELECT id, payload, created_at FROM {stream} WHERE id > %s ORDER BY id LIMIT %s",
        (last_id, count),
    )
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
            f"UPDATE {stream}_groups SET last_delivered_id = %s WHERE group_name = %s",
            (new_last, group),
        )
        for msg in messages:
            cur.execute(
                f"""INSERT INTO {stream}_pending (message_id, group_name, consumer)
                    VALUES (%s, %s, %s) ON CONFLICT (group_name, message_id) DO NOTHING""",
                (msg["id"], group, consumer),
            )
    raw.commit()
    cur.close()
    return messages


def stream_ack(conn, stream, group, message_id):
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(
        f"DELETE FROM {stream}_pending WHERE group_name = %s AND message_id = %s",
        (group, message_id),
    )
    removed = cur.rowcount > 0
    raw.commit()
    cur.close()
    return removed


def stream_claim(conn, stream, group, consumer, min_idle_ms=60000):
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"""
        UPDATE {stream}_pending
        SET consumer = %s, claimed_at = NOW(), delivery_count = delivery_count + 1
        WHERE group_name = %s AND claimed_at < NOW() - INTERVAL '%s milliseconds'
        RETURNING message_id
    """, (consumer, group, min_idle_ms))
    claimed_ids = [r[0] for r in cur.fetchall()]
    messages = []
    for msg_id in claimed_ids:
        cur.execute(
            f"SELECT id, payload, created_at FROM {stream} WHERE id = %s",
            (msg_id,),
        )
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
