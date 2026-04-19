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


async def enqueue(conn, queue_table, payload):
    _validate_identifier(queue_table)
    await _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {queue_table} (
            id BIGSERIAL PRIMARY KEY,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    await _execute(
        conn,
        f"INSERT INTO {queue_table} (payload) VALUES (%s)",
        (json.dumps(payload),),
    )


async def dequeue(conn, queue_table):
    # KNOWN ISSUE: this specific query shape (DELETE WHERE id = (SELECT …
    # FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING payload) hits the Gold Lapel
    # proxy's CloseComplete-framing interaction with asyncpg's extended query
    # protocol when it follows an INSERT-with-parameter on the same conn.
    # asyncpg caches a bogus parameter descriptor from the proxy and fails
    # the no-param fetchrow with "server expects 1 argument". See the main
    # repo's docs/wrapper-v0.2/03-proxy-closecomplete-framing.md. Tracking
    # the proxy-side fix separately. For now enqueue/dequeue work when used
    # on a fresh conn per call; the AsyncGoldLapel internal conn reuse is
    # where the issue surfaces. Integration tests for the doc_*/search
    # methods do not exercise this shape.
    _validate_identifier(queue_table)
    row = await _fetchrow(conn, f"""
        DELETE FROM {queue_table}
        WHERE id = (
            SELECT id FROM {queue_table}
            ORDER BY id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING payload
    """)
    if row is None:
        return None
    val = row[0]
    # JSONB codec registered → already dict. Defensive decode for raw text.
    if isinstance(val, (dict, list)):
        return val
    return json.loads(val)


# -- Counters ---------------------------------------------------------------

async def incr(conn, table, key, amount=1):
    _validate_identifier(table)
    await _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {table} (
            key TEXT PRIMARY KEY,
            value BIGINT NOT NULL DEFAULT 0
        )
    """)
    row = await _fetchrow(conn, f"""
        INSERT INTO {table} (key, value) VALUES (%s, %s)
        ON CONFLICT (key) DO UPDATE SET value = {table}.value + %s
        RETURNING value
    """, (key, amount, amount))
    return row[0]


async def get_counter(conn, table, key):
    _validate_identifier(table)
    row = await _fetchrow(conn, f"SELECT value FROM {table} WHERE key = %s", (key,))
    return row[0] if row else 0


# -- Hashes -----------------------------------------------------------------

async def hset(conn, table, key, field, value):
    _validate_identifier(table)
    await _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {table} (
            key TEXT PRIMARY KEY,
            data JSONB NOT NULL DEFAULT '{{}}'::jsonb
        )
    """)
    await _execute(conn, f"""
        INSERT INTO {table} (key, data) VALUES (%s, jsonb_build_object(%s, %s::jsonb))
        ON CONFLICT (key) DO UPDATE SET data = {table}.data || jsonb_build_object(%s, %s::jsonb)
    """, (key, field, json.dumps(value), field, json.dumps(value)))


async def hget(conn, table, key, field):
    _validate_identifier(table)
    row = await _fetchrow(
        conn, f"SELECT data->>%s FROM {table} WHERE key = %s", (field, key),
    )
    if row and row[0] is not None:
        try:
            return json.loads(row[0])
        except (json.JSONDecodeError, TypeError):
            return row[0]
    return None


async def hgetall(conn, table, key):
    _validate_identifier(table)
    row = await _fetchrow(conn, f"SELECT data FROM {table} WHERE key = %s", (key,))
    if row and row[0]:
        val = row[0]
        return val if isinstance(val, dict) else json.loads(val)
    return {}


async def hdel(conn, table, key, field):
    _validate_identifier(table)
    row = await _fetchrow(
        conn, f"SELECT data ? %s FROM {table} WHERE key = %s", (field, key),
    )
    existed = bool(row and row[0])
    if existed:
        await _execute(conn, f"UPDATE {table} SET data = data - %s WHERE key = %s", (field, key))
    return existed


# -- Sorted sets ------------------------------------------------------------

async def zadd(conn, table, member, score):
    _validate_identifier(table)
    await _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {table} (
            member TEXT PRIMARY KEY,
            score DOUBLE PRECISION NOT NULL
        )
    """)
    await _execute(conn, f"""
        INSERT INTO {table} (member, score) VALUES (%s, %s)
        ON CONFLICT (member) DO UPDATE SET score = EXCLUDED.score
    """, (str(member), float(score)))


async def zincrby(conn, table, member, amount=1):
    _validate_identifier(table)
    await _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {table} (
            member TEXT PRIMARY KEY,
            score DOUBLE PRECISION NOT NULL
        )
    """)
    row = await _fetchrow(conn, f"""
        INSERT INTO {table} (member, score) VALUES (%s, %s)
        ON CONFLICT (member) DO UPDATE SET score = {table}.score + %s
        RETURNING score
    """, (str(member), float(amount), float(amount)))
    return row[0]


async def zrange(conn, table, start=0, stop=10, desc=True):
    _validate_identifier(table)
    order = "DESC" if desc else "ASC"
    limit = stop - start
    rows = await _fetch(conn, f"""
        SELECT member, score FROM {table}
        ORDER BY score {order}
        LIMIT %s OFFSET %s
    """, (limit, start))
    return [(r[0], r[1]) for r in rows]


async def zrank(conn, table, member, desc=True):
    _validate_identifier(table)
    order = "DESC" if desc else "ASC"
    row = await _fetchrow(conn, f"""
        SELECT rank FROM (
            SELECT member, ROW_NUMBER() OVER (ORDER BY score {order}) - 1 AS rank
            FROM {table}
        ) ranked
        WHERE member = %s
    """, (str(member),))
    return row[0] if row else None


async def zscore(conn, table, member):
    _validate_identifier(table)
    row = await _fetchrow(
        conn, f"SELECT score FROM {table} WHERE member = %s", (str(member),),
    )
    return row[0] if row else None


async def zrem(conn, table, member):
    _validate_identifier(table)
    status = await _execute(conn, f"DELETE FROM {table} WHERE member = %s", (str(member),))
    return _rowcount_from_status(status) > 0


# -- Geo --------------------------------------------------------------------

async def georadius(conn, table, geom_column, lon, lat, radius_meters, limit=50):
    _validate_identifier(table)
    _validate_identifier(geom_column)
    rows = await _fetch(conn, f"""
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
    return _rows_to_dicts(rows)


async def geoadd(conn, table, name_column, geom_column, name, lon, lat):
    _validate_identifier(table)
    _validate_identifier(name_column)
    _validate_identifier(geom_column)
    await _execute(conn, "CREATE EXTENSION IF NOT EXISTS postgis")
    await _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id BIGSERIAL PRIMARY KEY,
            {name_column} TEXT NOT NULL,
            {geom_column} GEOMETRY(Point, 4326) NOT NULL
        )
    """)
    await _execute(conn, f"""
        INSERT INTO {table} ({name_column}, {geom_column})
        VALUES (%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
    """, (name, lon, lat))


async def geodist(conn, table, geom_column, name_column, name_a, name_b):
    _validate_identifier(table)
    _validate_identifier(geom_column)
    _validate_identifier(name_column)
    row = await _fetchrow(conn, f"""
        SELECT ST_Distance(a.{geom_column}::geography, b.{geom_column}::geography)
        FROM {table} a, {table} b
        WHERE a.{name_column} = %s AND b.{name_column} = %s
    """, (name_a, name_b))
    return row[0] if row else None


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

async def stream_add(conn, stream, payload):
    _validate_identifier(stream)
    await _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {stream} (
            id BIGSERIAL PRIMARY KEY,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    row = await _fetchrow(
        conn,
        f"INSERT INTO {stream} (payload) VALUES (%s) RETURNING id",
        (json.dumps(payload),),
    )
    return row[0]


async def stream_create_group(conn, stream, group):
    _validate_identifier(stream)
    await _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {stream}_groups (
            group_name TEXT PRIMARY KEY,
            last_delivered_id BIGINT NOT NULL DEFAULT 0
        )
    """)
    await _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {stream}_pending (
            message_id BIGINT NOT NULL,
            group_name TEXT NOT NULL,
            consumer TEXT NOT NULL,
            claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            delivery_count INT NOT NULL DEFAULT 1,
            PRIMARY KEY (group_name, message_id)
        )
    """)
    await _execute(
        conn,
        f"INSERT INTO {stream}_groups (group_name) VALUES (%s) ON CONFLICT DO NOTHING",
        (group,),
    )


async def stream_read(conn, stream, group, consumer, count=1):
    # Sync path uses `FOR UPDATE` inside an implicit txn; asyncpg needs an
    # explicit transaction for FOR UPDATE to be meaningful. We open a tx here.
    _validate_identifier(stream)
    raw = _get_raw_connection(conn)
    async with raw.transaction():
        sql1, p1 = _to_asyncpg(
            f"SELECT last_delivered_id FROM {stream}_groups WHERE group_name = %s FOR UPDATE",
            (group,),
        )
        row = await raw.fetchrow(sql1, *p1)
        if not row:
            return []
        last_id = row[0]
        sql2, p2 = _to_asyncpg(
            f"SELECT id, payload, created_at FROM {stream} WHERE id > %s ORDER BY id LIMIT %s",
            (last_id, count),
        )
        rows = await raw.fetch(sql2, *p2)
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
            sql3, p3 = _to_asyncpg(
                f"UPDATE {stream}_groups SET last_delivered_id = %s WHERE group_name = %s",
                (new_last, group),
            )
            await raw.execute(sql3, *p3)
            for msg in messages:
                sql4, p4 = _to_asyncpg(
                    f"""INSERT INTO {stream}_pending (message_id, group_name, consumer)
                        VALUES (%s, %s, %s) ON CONFLICT (group_name, message_id) DO NOTHING""",
                    (msg["id"], group, consumer),
                )
                await raw.execute(sql4, *p4)
        return messages


async def stream_ack(conn, stream, group, message_id):
    _validate_identifier(stream)
    status = await _execute(
        conn,
        f"DELETE FROM {stream}_pending WHERE group_name = %s AND message_id = %s",
        (group, message_id),
    )
    return _rowcount_from_status(status) > 0


async def stream_claim(conn, stream, group, consumer, min_idle_ms=60000):
    _validate_identifier(stream)
    rows = await _fetch(conn, f"""
        UPDATE {stream}_pending
        SET consumer = %s, claimed_at = NOW(), delivery_count = delivery_count + 1
        WHERE group_name = %s AND claimed_at < NOW() - INTERVAL '1 millisecond' * %s
        RETURNING message_id
    """, (consumer, group, min_idle_ms))
    claimed_ids = [r[0] for r in rows]
    messages = []
    for msg_id in claimed_ids:
        r = await _fetchrow(
            conn,
            f"SELECT id, payload, created_at FROM {stream} WHERE id = %s",
            (msg_id,),
        )
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

async def _ensure_collection(conn, collection, unlogged=False):
    prefix = "CREATE UNLOGGED TABLE" if unlogged else "CREATE TABLE"
    await _execute(conn, (
        f"{prefix} IF NOT EXISTS {collection} ("
        "_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), "
        "data JSONB NOT NULL, "
        "created_at TIMESTAMPTZ DEFAULT NOW())"
    ))


async def doc_create_collection(conn, collection, unlogged=False):
    _validate_identifier(collection)
    await _ensure_collection(conn, collection, unlogged=unlogged)


async def doc_insert(conn, collection, document):
    _validate_identifier(collection)
    await _ensure_collection(conn, collection)
    row = await _fetchrow(
        conn,
        f"INSERT INTO {collection} (data) VALUES (%s::jsonb) RETURNING _id, data, created_at",
        (json.dumps(document),),
    )
    return _decode_doc_row(_row_to_dict(row))


async def doc_insert_many(conn, collection, documents):
    _validate_identifier(collection)
    await _ensure_collection(conn, collection)
    placeholders = ", ".join(["(%s::jsonb)"] * len(documents))
    params = tuple(json.dumps(d) for d in documents)
    rows = await _fetch(
        conn,
        f"INSERT INTO {collection} (data) VALUES {placeholders} RETURNING _id, data, created_at",
        params,
    )
    return _decode_doc_rows(_rows_to_dicts(rows))


def _build_doc_find_sql(collection, filter=None, sort=None, limit=None, skip=None):
    sql = f"SELECT _id, data, created_at FROM {collection}"
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


async def doc_find(conn, collection, filter=None, sort=None, limit=None, skip=None):
    _validate_identifier(collection)
    sql, params = _build_doc_find_sql(collection, filter, sort, limit, skip)
    rows = await _fetch(conn, sql, tuple(params))
    return _decode_doc_rows(_rows_to_dicts(rows))


async def doc_find_cursor(
    conn, collection, filter=None, sort=None, limit=None, skip=None, batch_size=100,
):
    """Async generator version of doc_find_cursor.

    asyncpg cursors are scoped to a transaction — we open one, iterate, yield,
    and close in a finally. Matches psycopg2's server-side cursor semantics.
    """
    _validate_identifier(collection)
    sql, params = _build_doc_find_sql(collection, filter, sort, limit, skip)
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


async def doc_find_one(conn, collection, filter=None):
    _validate_identifier(collection)
    sql = f"SELECT _id, data, created_at FROM {collection}"
    params = []
    where_clause, filter_params = _build_filter(filter)
    if where_clause:
        sql += " WHERE " + where_clause
        params.extend(filter_params)
    sql += " LIMIT 1"
    row = await _fetchrow(conn, sql, tuple(params))
    return _decode_doc_row(_row_to_dict(row))


async def doc_update(conn, collection, filter, update):
    _validate_identifier(collection)
    where_clause, filter_params = _build_filter(filter)
    update_expr, update_params = _build_update(update)
    sql = f"UPDATE {collection} SET data = {update_expr}"
    params = list(update_params)
    if where_clause:
        sql += " WHERE " + where_clause
        params.extend(filter_params)
    status = await _execute(conn, sql, tuple(params))
    return _rowcount_from_status(status)


async def doc_update_one(conn, collection, filter, update):
    _validate_identifier(collection)
    where_clause, filter_params = _build_filter(filter)
    update_expr, update_params = _build_update(update)
    cte_where = " WHERE " + where_clause if where_clause else ""
    sql = (
        f"WITH target AS (SELECT _id FROM {collection}{cte_where} LIMIT 1) "
        f"UPDATE {collection} SET data = {update_expr} FROM target WHERE {collection}._id = target._id"
    )
    params = list(filter_params) + list(update_params)
    status = await _execute(conn, sql, tuple(params))
    return _rowcount_from_status(status)


async def doc_delete(conn, collection, filter):
    _validate_identifier(collection)
    where_clause, filter_params = _build_filter(filter)
    sql = f"DELETE FROM {collection}"
    if where_clause:
        sql += " WHERE " + where_clause
    status = await _execute(conn, sql, tuple(filter_params))
    return _rowcount_from_status(status)


async def doc_delete_one(conn, collection, filter):
    _validate_identifier(collection)
    where_clause, filter_params = _build_filter(filter)
    cte_where = " WHERE " + where_clause if where_clause else ""
    status = await _execute(
        conn,
        f"WITH target AS (SELECT _id FROM {collection}{cte_where} LIMIT 1) "
        f"DELETE FROM {collection} USING target WHERE {collection}._id = target._id",
        tuple(filter_params),
    )
    return _rowcount_from_status(status)


async def doc_count(conn, collection, filter=None):
    _validate_identifier(collection)
    sql = f"SELECT COUNT(*) FROM {collection}"
    params = []
    where_clause, filter_params = _build_filter(filter)
    if where_clause:
        sql += " WHERE " + where_clause
        params.extend(filter_params)
    val = await _fetchval(conn, sql, tuple(params))
    return val


async def doc_find_one_and_update(conn, collection, filter, update):
    _validate_identifier(collection)
    where_clause, filter_params = _build_filter(filter)
    update_expr, update_params = _build_update(update)
    cte_where = " WHERE " + where_clause if where_clause else ""
    sql = (
        f"WITH target AS (SELECT _id FROM {collection}{cte_where} LIMIT 1) "
        f"UPDATE {collection} SET data = {update_expr} FROM target "
        f"WHERE {collection}._id = target._id "
        f"RETURNING {collection}._id, {collection}.data, {collection}.created_at"
    )
    params = list(filter_params) + list(update_params)
    row = await _fetchrow(conn, sql, tuple(params))
    return _decode_doc_row(_row_to_dict(row))


async def doc_find_one_and_delete(conn, collection, filter):
    _validate_identifier(collection)
    where_clause, filter_params = _build_filter(filter)
    cte_where = " WHERE " + where_clause if where_clause else ""
    sql = (
        f"WITH target AS (SELECT _id FROM {collection}{cte_where} LIMIT 1) "
        f"DELETE FROM {collection} USING target "
        f"WHERE {collection}._id = target._id "
        f"RETURNING {collection}._id, {collection}.data, {collection}.created_at"
    )
    row = await _fetchrow(conn, sql, tuple(filter_params))
    return _decode_doc_row(_row_to_dict(row))


async def doc_distinct(conn, collection, field, filter=None):
    _validate_identifier(collection)
    field_expr = _field_path(field)
    sql = f"SELECT DISTINCT {field_expr} FROM {collection}"
    params = []
    where_parts = [f"{field_expr} IS NOT NULL"]
    where_clause, filter_params = _build_filter(filter)
    if where_clause:
        where_parts.append(where_clause)
        params.extend(filter_params)
    sql += " WHERE " + " AND ".join(where_parts)
    rows = await _fetch(conn, sql, tuple(params))
    return [r[0] for r in rows]


async def doc_create_index(conn, collection, keys=None):
    _validate_identifier(collection)
    if keys is None:
        await _execute(
            conn,
            f"CREATE INDEX IF NOT EXISTS idx_{collection}_gin ON {collection} USING GIN (data)",
        )
    else:
        for key in keys:
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", key):
                raise ValueError(f"Invalid key: {key}")
            await _execute(
                conn,
                f"CREATE INDEX IF NOT EXISTS idx_{collection}_{key} "
                f"ON {collection} ((data->>'{key}'))",
            )


# -- Aggregation pipeline ---------------------------------------------------

async def doc_aggregate(conn, collection, pipeline):
    _validate_identifier(collection)
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
        subquery = (
            f"COALESCE((SELECT json_agg(b.data) FROM {lookup['from']} b "
            f"WHERE {foreign_expr} = {collection}.{local_expr}), '[]'::json) "
            f"AS {lookup['as']}"
        )
        select_parts.append(subquery)

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


async def doc_watch(conn, collection, callback, blocking=True):
    """Watch a collection for changes via triggers + pg_notify.

    Async version uses asyncpg's add_listener on a fresh connection.
    Callback is invoked as callback(event_dict) — same signature as sync.
    """
    import asyncio

    _validate_identifier(collection)
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
    await _execute(conn, f"""
        DO $$ BEGIN
            CREATE TRIGGER _gl_watch_{collection}_trigger
                AFTER INSERT OR UPDATE OR DELETE ON {collection}
                FOR EACH ROW EXECUTE FUNCTION _gl_watch_{collection}();
        EXCEPTION WHEN duplicate_object THEN NULL;
        END $$
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


async def doc_unwatch(conn, collection):
    _validate_identifier(collection)
    await _execute(conn, f"DROP TRIGGER IF EXISTS _gl_watch_{collection}_trigger ON {collection}")
    await _execute(conn, f"DROP FUNCTION IF EXISTS _gl_watch_{collection}()")


async def doc_create_ttl_index(conn, collection, expire_after_seconds, field="created_at"):
    _validate_identifier(collection)
    _validate_identifier(field)
    if not isinstance(expire_after_seconds, int):
        raise ValueError("expire_after_seconds must be an integer")
    await _execute(
        conn, f"CREATE INDEX IF NOT EXISTS idx_{collection}_ttl ON {collection} ({field})",
    )
    expire_int = int(expire_after_seconds)
    await _execute(conn, f"""
        CREATE OR REPLACE FUNCTION _gl_ttl_{collection}() RETURNS TRIGGER AS $$
        BEGIN
            DELETE FROM {collection} WHERE {field} < NOW() - INTERVAL '{expire_int} seconds';
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
    """)
    await _execute(conn, f"""
        DO $$ BEGIN
            CREATE TRIGGER _gl_ttl_{collection}_trigger
                BEFORE INSERT ON {collection}
                FOR EACH STATEMENT EXECUTE FUNCTION _gl_ttl_{collection}();
        EXCEPTION WHEN duplicate_object THEN NULL;
        END $$
    """)


async def doc_remove_ttl_index(conn, collection):
    _validate_identifier(collection)
    await _execute(conn, f"DROP TRIGGER IF EXISTS _gl_ttl_{collection}_trigger ON {collection}")
    await _execute(conn, f"DROP FUNCTION IF EXISTS _gl_ttl_{collection}()")
    await _execute(conn, f"DROP INDEX IF EXISTS idx_{collection}_ttl")


async def doc_create_capped(conn, collection, max_documents):
    _validate_identifier(collection)
    if not isinstance(max_documents, int):
        raise ValueError("max_documents must be an integer")
    await _ensure_collection(conn, collection)
    await _execute(
        conn,
        f"CREATE INDEX IF NOT EXISTS idx_{collection}_created_at "
        f"ON {collection} (created_at ASC)",
    )
    max_int = int(max_documents)
    await _execute(conn, f"""
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
    await _execute(conn, f"""
        DO $$ BEGIN
            CREATE TRIGGER _gl_cap_{collection}_trigger
                AFTER INSERT ON {collection}
                FOR EACH STATEMENT EXECUTE FUNCTION _gl_cap_{collection}();
        EXCEPTION WHEN duplicate_object THEN NULL;
        END $$
    """)


async def doc_remove_cap(conn, collection):
    _validate_identifier(collection)
    await _execute(conn, f"DROP TRIGGER IF EXISTS _gl_cap_{collection}_trigger ON {collection}")
    await _execute(conn, f"DROP FUNCTION IF EXISTS _gl_cap_{collection}()")
