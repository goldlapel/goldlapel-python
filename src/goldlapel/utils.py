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
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
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


def enqueue(conn, queue_table, payload):
    """Add a job to a queue table. Like redis.lpush().

    Creates the queue table if it doesn't exist.
    Payload is stored as JSONB.
    """
    _validate_identifier(queue_table)
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
    _validate_identifier(queue_table)
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
    _validate_identifier(table)
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
    _validate_identifier(table)
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
    _validate_identifier(table)
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
    _validate_identifier(table)
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
    _validate_identifier(table)
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
    _validate_identifier(table)
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
    _validate_identifier(table)
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
    _validate_identifier(table)
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
    _validate_identifier(table)
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
    _validate_identifier(table)
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
    _validate_identifier(table)
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
    _validate_identifier(table)
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
    _validate_identifier(table)
    _validate_identifier(geom_column)
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
    _validate_identifier(table)
    _validate_identifier(name_column)
    _validate_identifier(geom_column)
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
    _validate_identifier(table)
    _validate_identifier(geom_column)
    _validate_identifier(name_column)
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


def stream_add(conn, stream, payload):
    _validate_identifier(stream)
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
    _validate_identifier(stream)
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
    _validate_identifier(stream)
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
    _validate_identifier(stream)
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
    _validate_identifier(stream)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"""
        UPDATE {stream}_pending
        SET consumer = %s, claimed_at = NOW(), delivery_count = delivery_count + 1
        WHERE group_name = %s AND claimed_at < NOW() - INTERVAL '1 millisecond' * %s
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


def _ensure_collection(cur, collection, unlogged=False):
    prefix = "CREATE UNLOGGED TABLE" if unlogged else "CREATE TABLE"
    cur.execute(
        f"{prefix} IF NOT EXISTS {collection} ("
        "_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), "
        "data JSONB NOT NULL, "
        "created_at TIMESTAMPTZ DEFAULT NOW())"
    )


def doc_create_collection(conn, collection, unlogged=False):
    _validate_identifier(collection)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    _ensure_collection(cur, collection, unlogged=unlogged)
    raw.commit()


def doc_insert(conn, collection, document):
    _validate_identifier(collection)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    _ensure_collection(cur, collection)
    cur.execute(
        f"INSERT INTO {collection} (data) VALUES (%s::jsonb) RETURNING _id, data, created_at",
        (json.dumps(document),),
    )
    cols = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    raw.commit()
    cur.close()
    return dict(zip(cols, row))


def doc_insert_many(conn, collection, documents):
    _validate_identifier(collection)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    _ensure_collection(cur, collection)
    placeholders = ", ".join(["(%s::jsonb)"] * len(documents))
    params = tuple(json.dumps(d) for d in documents)
    cur.execute(
        f"INSERT INTO {collection} (data) VALUES {placeholders} RETURNING _id, data, created_at",
        params,
    )
    cols = [desc[0] for desc in cur.description]
    results = [dict(zip(cols, row)) for row in cur.fetchall()]
    raw.commit()
    cur.close()
    return results


def doc_find(conn, collection, filter=None, sort=None, limit=None, skip=None):
    _validate_identifier(collection)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    sql = f"SELECT _id, data, created_at FROM {collection}"
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


def doc_find_cursor(conn, collection, filter=None, sort=None, limit=None, skip=None, batch_size=100):
    _validate_identifier(collection)
    raw = _get_raw_connection(conn)
    cur = raw.cursor(name=f"gl_cursor_{id(raw)}")
    sql = f"SELECT _id, data, created_at FROM {collection}"
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


def doc_find_one(conn, collection, filter=None):
    _validate_identifier(collection)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    sql = f"SELECT _id, data, created_at FROM {collection}"
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


def doc_update(conn, collection, filter, update):
    _validate_identifier(collection)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    where_clause, filter_params = _build_filter(filter)
    update_expr, update_params = _build_update(update)
    sql = f"UPDATE {collection} SET data = {update_expr}"
    params = list(update_params)
    if where_clause:
        sql += " WHERE " + where_clause
        params.extend(filter_params)
    cur.execute(sql, tuple(params))
    rowcount = cur.rowcount
    raw.commit()
    cur.close()
    return rowcount


def doc_update_one(conn, collection, filter, update):
    _validate_identifier(collection)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    where_clause, filter_params = _build_filter(filter)
    update_expr, update_params = _build_update(update)
    if where_clause:
        cte_where = " WHERE " + where_clause
    else:
        cte_where = ""
    sql = (
        f"WITH target AS (SELECT _id FROM {collection}{cte_where} LIMIT 1) "
        f"UPDATE {collection} SET data = {update_expr} FROM target WHERE {collection}._id = target._id"
    )
    params = list(filter_params) + list(update_params)
    cur.execute(sql, tuple(params))
    rowcount = cur.rowcount
    raw.commit()
    cur.close()
    return rowcount


def doc_delete(conn, collection, filter):
    _validate_identifier(collection)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    where_clause, filter_params = _build_filter(filter)
    sql = f"DELETE FROM {collection}"
    if where_clause:
        sql += " WHERE " + where_clause
    cur.execute(sql, tuple(filter_params))
    rowcount = cur.rowcount
    raw.commit()
    cur.close()
    return rowcount


def doc_delete_one(conn, collection, filter):
    _validate_identifier(collection)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    where_clause, filter_params = _build_filter(filter)
    if where_clause:
        cte_where = " WHERE " + where_clause
    else:
        cte_where = ""
    cur.execute(
        f"WITH target AS (SELECT _id FROM {collection}{cte_where} LIMIT 1) "
        f"DELETE FROM {collection} USING target WHERE {collection}._id = target._id",
        tuple(filter_params),
    )
    rowcount = cur.rowcount
    raw.commit()
    cur.close()
    return rowcount


def doc_count(conn, collection, filter=None):
    _validate_identifier(collection)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    sql = f"SELECT COUNT(*) FROM {collection}"
    params = []
    where_clause, filter_params = _build_filter(filter)
    if where_clause:
        sql += " WHERE " + where_clause
        params.extend(filter_params)
    cur.execute(sql, tuple(params))
    result = cur.fetchone()[0]
    cur.close()
    return result


def doc_find_one_and_update(conn, collection, filter, update):
    _validate_identifier(collection)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    where_clause, filter_params = _build_filter(filter)
    update_expr, update_params = _build_update(update)
    if where_clause:
        cte_where = " WHERE " + where_clause
    else:
        cte_where = ""
    sql = (
        f"WITH target AS (SELECT _id FROM {collection}{cte_where} LIMIT 1) "
        f"UPDATE {collection} SET data = {update_expr} FROM target "
        f"WHERE {collection}._id = target._id "
        f"RETURNING {collection}._id, {collection}.data, {collection}.created_at"
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


def doc_find_one_and_delete(conn, collection, filter):
    _validate_identifier(collection)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    where_clause, filter_params = _build_filter(filter)
    if where_clause:
        cte_where = " WHERE " + where_clause
    else:
        cte_where = ""
    sql = (
        f"WITH target AS (SELECT _id FROM {collection}{cte_where} LIMIT 1) "
        f"DELETE FROM {collection} USING target "
        f"WHERE {collection}._id = target._id "
        f"RETURNING {collection}._id, {collection}.data, {collection}.created_at"
    )
    cur.execute(sql, tuple(filter_params))
    cols = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    raw.commit()
    cur.close()
    if row is None:
        return None
    return dict(zip(cols, row))


def doc_distinct(conn, collection, field, filter=None):
    _validate_identifier(collection)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    field_expr = _field_path(field)
    sql = f"SELECT DISTINCT {field_expr} FROM {collection}"
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


def doc_create_index(conn, collection, keys=None):
    _validate_identifier(collection)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    if keys is None:
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{collection}_gin ON {collection} USING GIN (data)"
        )
    else:
        for key in keys:
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', key):
                raise ValueError(f"Invalid key: {key}")
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{collection}_{key} ON {collection} ((data->>'{key}'))"
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


def doc_aggregate(conn, collection, pipeline):
    _validate_identifier(collection)
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
        subquery = (
            f"COALESCE((SELECT json_agg(b.data) FROM {lookup['from']} b "
            f"WHERE {foreign_expr} = {collection}.{local_expr}), '[]'::json) "
            f"AS {lookup['as']}"
        )
        select_parts.append(subquery)

    # Build FROM clause
    from_clause = collection
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


def doc_watch(conn, collection, callback, blocking=True):
    """Watch a collection for changes via triggers + pg_notify. Like MongoDB change streams."""
    _validate_identifier(collection)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()

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

    cur.execute(f"""
        DO $$ BEGIN
            CREATE TRIGGER _gl_watch_{collection}_trigger
                AFTER INSERT OR UPDATE OR DELETE ON {collection}
                FOR EACH ROW EXECUTE FUNCTION _gl_watch_{collection}();
        EXCEPTION WHEN duplicate_object THEN NULL;
        END $$
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


def doc_unwatch(conn, collection):
    """Remove change stream trigger from a collection."""
    _validate_identifier(collection)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"DROP TRIGGER IF EXISTS _gl_watch_{collection}_trigger ON {collection}")
    cur.execute(f"DROP FUNCTION IF EXISTS _gl_watch_{collection}()")
    raw.commit()
    cur.close()


def doc_create_ttl_index(conn, collection, expire_after_seconds, field="created_at"):
    """Create a TTL index that deletes expired rows on each INSERT. Like MongoDB TTL indexes."""
    _validate_identifier(collection)
    _validate_identifier(field)
    if not isinstance(expire_after_seconds, int):
        raise ValueError("expire_after_seconds must be an integer")
    raw = _get_raw_connection(conn)
    cur = raw.cursor()

    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{collection}_ttl ON {collection} ({field})"
    )

    expire_int = int(expire_after_seconds)
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION _gl_ttl_{collection}() RETURNS TRIGGER AS $$
        BEGIN
            DELETE FROM {collection} WHERE {field} < NOW() - INTERVAL '{expire_int} seconds';
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
    """)

    cur.execute(f"""
        DO $$ BEGIN
            CREATE TRIGGER _gl_ttl_{collection}_trigger
                BEFORE INSERT ON {collection}
                FOR EACH STATEMENT EXECUTE FUNCTION _gl_ttl_{collection}();
        EXCEPTION WHEN duplicate_object THEN NULL;
        END $$
    """)
    raw.commit()
    cur.close()


def doc_remove_ttl_index(conn, collection):
    """Remove TTL trigger, function, and index from a collection."""
    _validate_identifier(collection)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"DROP TRIGGER IF EXISTS _gl_ttl_{collection}_trigger ON {collection}")
    cur.execute(f"DROP FUNCTION IF EXISTS _gl_ttl_{collection}()")
    cur.execute(f"DROP INDEX IF EXISTS idx_{collection}_ttl")
    raw.commit()
    cur.close()


def doc_create_capped(conn, collection, max_documents):
    """Create a capped collection that auto-deletes oldest rows. Like MongoDB capped collections."""
    _validate_identifier(collection)
    if not isinstance(max_documents, int):
        raise ValueError("max_documents must be an integer")
    raw = _get_raw_connection(conn)
    cur = raw.cursor()

    _ensure_collection(cur, collection)

    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{collection}_created_at ON {collection} (created_at ASC)"
    )

    max_int = int(max_documents)
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION _gl_cap_{collection}() RETURNS TRIGGER AS $$
        DECLARE excess INTEGER;
        BEGIN
            SELECT COUNT(*) - {max_int} INTO excess FROM {collection};
            IF excess > 0 THEN
                DELETE FROM {collection} WHERE _id IN (
                    SELECT _id FROM {collection} ORDER BY created_at ASC LIMIT excess
                );
            END IF;
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql
    """)

    cur.execute(f"""
        DO $$ BEGIN
            CREATE TRIGGER _gl_cap_{collection}_trigger
                AFTER INSERT ON {collection}
                FOR EACH STATEMENT EXECUTE FUNCTION _gl_cap_{collection}();
        EXCEPTION WHEN duplicate_object THEN NULL;
        END $$
    """)
    raw.commit()
    cur.close()


def doc_remove_cap(conn, collection):
    """Remove capped collection trigger and function."""
    _validate_identifier(collection)
    raw = _get_raw_connection(conn)
    cur = raw.cursor()
    cur.execute(f"DROP TRIGGER IF EXISTS _gl_cap_{collection}_trigger ON {collection}")
    cur.execute(f"DROP FUNCTION IF EXISTS _gl_cap_{collection}()")
    raw.commit()
    cur.close()
