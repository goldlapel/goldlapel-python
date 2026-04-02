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
