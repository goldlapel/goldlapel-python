import atexit

from goldlapel.cache import (
    NativeCache,
    ConnectionGucState,
    _detect_write,
    _DDL_SENTINEL,
    _TX_START,
    _TX_END,
)

_cache = None
_atexit_registered = False


def _shutdown_cache():
    """atexit handler — emit a final wrapper_disconnected snapshot
    before the process exits so the proxy's per-wrapper aggregate
    flips to "gone" promptly. Best-effort; the socket may already be
    torn down by other shutdown paths."""
    global _cache
    if _cache is not None:
        try:
            _cache.emit_wrapper_disconnected()
        except Exception:
            pass


def _detect_invalidation_port():
    try:
        from goldlapel.proxy import _instances, _lock, DEFAULT_PROXY_PORT
        with _lock:
            if _instances:
                inst = next(iter(_instances.values()))
                # invalidation_port is resolved at GoldLapel construction.
                return inst.invalidation_port
        return DEFAULT_PROXY_PORT + 2
    except Exception:
        return 7934


def wrap(conn, invalidation_port=None, disable_native_cache=False):
    global _cache, _atexit_registered
    # Pass `disable_native_cache` through on every wrap() call. NativeCache
    # is a singleton; on first construction it stores the flag, and on every
    # later call (a second start(), or this same start() opening a new
    # conn) the __init__ short-circuit re-binds `_disabled` so the most
    # recent caller's intent wins.
    _cache = NativeCache(disabled=disable_native_cache)
    if invalidation_port is None:
        invalidation_port = _detect_invalidation_port()
    if not _cache._invalidation_thread or not _cache._invalidation_thread.is_alive():
        _cache.connect_invalidation(invalidation_port)
    if not _atexit_registered:
        atexit.register(_shutdown_cache)
        _atexit_registered = True

    if hasattr(conn, "fetch") and hasattr(conn, "fetchrow"):
        return AsyncCachedConnection(conn, _cache)

    return CachedConnection(conn, _cache)


class CachedConnection:
    def __init__(self, real_conn, cache):
        object.__setattr__(self, "_real", real_conn)
        object.__setattr__(self, "_cache", cache)
        object.__setattr__(self, "_in_transaction", False)
        # Per-connection unsafe-GUC state. Folded into the L1 cache key so
        # two connections that have set different `app.user_id` (or any
        # other unsafe GUC) never share a cache slot. See
        # goldlapel.cache.ConnectionGucState for the full classifier rule.
        object.__setattr__(self, "_guc_state", ConnectionGucState())

    def cursor(self, *args, **kwargs):
        real_cursor = self._real.cursor(*args, **kwargs)
        return CachedCursor(real_cursor, self._cache, self)

    def close(self):
        return self._real.close()

    @property
    def closed(self):
        return self._real.closed

    def commit(self):
        return self._real.commit()

    def rollback(self):
        return self._real.rollback()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return self._real.__exit__(*args)

    def __getattr__(self, name):
        return getattr(self._real, name)

    def __setattr__(self, name, value):
        setattr(self._real, name, value)


class CachedCursor:
    def __init__(self, real_cursor, cache, conn=None):
        object.__setattr__(self, "_real", real_cursor)
        object.__setattr__(self, "_cache", cache)
        object.__setattr__(self, "_conn", conn)
        object.__setattr__(self, "_cached_rows", None)
        object.__setattr__(self, "_cached_description", None)
        object.__setattr__(self, "_fetch_index", 0)

    def execute(self, sql, params=None):
        object.__setattr__(self, "_cached_rows", None)
        object.__setattr__(self, "_cached_description", None)
        object.__setattr__(self, "_fetch_index", 0)

        # Observe SET / RESET commands BEFORE any cache decision so a
        # multi-statement Q like `SET app.user_id='42'; SELECT ...`
        # updates state and then keys the SELECT under the new hash.
        # observe_sql is fast-pathed for non-SET statements (no allocation
        # when there's no `;` and no "SET"/"RESET" prefix to match).
        if self._conn is not None:
            self._conn._guc_state.observe_sql(sql)
        state_hash = self._conn._guc_state.hash if self._conn is not None else 0

        # Transaction tracking (state on connection, not cursor)
        if _TX_START.match(sql):
            if self._conn is not None:
                object.__setattr__(self._conn, "_in_transaction", True)
            return self._real.execute(sql, params)
        if _TX_END.match(sql):
            if self._conn is not None:
                object.__setattr__(self._conn, "_in_transaction", False)
            return self._real.execute(sql, params)

        # Write detection + self-invalidation (always, even in transactions)
        write_table = _detect_write(sql)
        if write_table:
            if write_table == _DDL_SENTINEL:
                self._cache.invalidate_all()
            else:
                self._cache.invalidate_table(write_table)
            return self._real.execute(sql, params)

        # Inside transaction: bypass cache for reads
        if self._conn is not None and self._conn._in_transaction:
            return self._real.execute(sql, params)

        # Bypass cache for server-side/named cursors
        if getattr(self._real, "name", None):
            return self._real.execute(sql, params)

        # Read path: check native cache (state_hash-keyed)
        entry = self._cache.get(sql, params, state_hash)
        if entry is not None:
            object.__setattr__(self, "_cached_rows", entry.rows)
            object.__setattr__(self, "_cached_description", entry.description)
            object.__setattr__(self, "_fetch_index", 0)
            return None

        # Cache miss: execute for real
        result = self._real.execute(sql, params)

        # Cache the result if the query returns rows
        if self._real.description is not None:
            try:
                rows = self._real.fetchall()
                desc = self._real.description
            except Exception:
                return result  # fetchall failed, cursor state is gone, nothing we can do
            # Cache the result (best effort)
            try:
                self._cache.put(sql, params, rows, desc, state_hash)
            except Exception:
                pass
            # Always serve from our copy since we consumed the cursor
            object.__setattr__(self, "_cached_rows", rows)
            object.__setattr__(self, "_cached_description", desc)
            object.__setattr__(self, "_fetch_index", 0)

        return result

    def fetchone(self):
        if self._cached_rows is not None:
            idx = self._fetch_index
            if idx < len(self._cached_rows):
                object.__setattr__(self, "_fetch_index", idx + 1)
                return self._cached_rows[idx]
            return None
        return self._real.fetchone()

    def fetchall(self):
        if self._cached_rows is not None:
            remaining = self._cached_rows[self._fetch_index:]
            object.__setattr__(self, "_fetch_index", len(self._cached_rows))
            return remaining
        return self._real.fetchall()

    def fetchmany(self, size=None):
        if self._cached_rows is not None:
            if size is None:
                size = getattr(self._real, "arraysize", 1)
            end = min(self._fetch_index + size, len(self._cached_rows))
            rows = self._cached_rows[self._fetch_index:end]
            object.__setattr__(self, "_fetch_index", end)
            return rows
        return self._real.fetchmany(size) if size is not None else self._real.fetchmany()

    @property
    def description(self):
        if self._cached_description is not None:
            return self._cached_description
        return self._real.description

    @property
    def rowcount(self):
        if self._cached_rows is not None:
            return len(self._cached_rows)
        return self._real.rowcount

    def executemany(self, sql, params_list):
        # Observe in case the caller `executemany`s a `SET ...` (unusual
        # but cheap to track and avoids stale cache slots if they do).
        if self._conn is not None:
            self._conn._guc_state.observe_sql(sql)
        write_table = _detect_write(sql)
        if write_table:
            if write_table == _DDL_SENTINEL:
                self._cache.invalidate_all()
            else:
                self._cache.invalidate_table(write_table)
        return self._real.executemany(sql, params_list)

    def callproc(self, procname, params=None):
        self._cache.invalidate_all()
        return self._real.callproc(procname, params)

    def close(self):
        return self._real.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return self._real.__exit__(*args)

    def __iter__(self):
        if self._cached_rows is not None:
            return iter(self._cached_rows[self._fetch_index:])
        return iter(self._real)

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __getattr__(self, name):
        return getattr(self._real, name)

    def __setattr__(self, name, value):
        setattr(self._real, name, value)


class AsyncCachedConnection:
    def __init__(self, real_conn, cache):
        object.__setattr__(self, "_real", real_conn)
        object.__setattr__(self, "_cache", cache)
        object.__setattr__(self, "_in_transaction", False)
        # Per-connection unsafe-GUC state — see CachedConnection for the
        # full rationale. Keyed into the L1 cache slot so SETs on this
        # asyncpg connection don't leak cached rows to peer connections
        # with different RLS context.
        object.__setattr__(self, "_guc_state", ConnectionGucState())

    async def fetch(self, sql, *args):
        self._guc_state.observe_sql(sql)
        state_hash = self._guc_state.hash
        write_table = _detect_write(sql)
        if write_table:
            if write_table == _DDL_SENTINEL:
                self._cache.invalidate_all()
            else:
                self._cache.invalidate_table(write_table)
            return await self._real.fetch(sql, *args)

        if self._in_transaction:
            return await self._real.fetch(sql, *args)

        params = args if args else None
        entry = self._cache.get(sql, params, state_hash)
        if entry is not None:
            return entry.rows

        rows = await self._real.fetch(sql, *args)
        self._cache.put(sql, params, list(rows), None, state_hash)
        return rows

    async def fetchrow(self, sql, *args):
        self._guc_state.observe_sql(sql)
        state_hash = self._guc_state.hash
        write_table = _detect_write(sql)
        if write_table:
            if write_table == _DDL_SENTINEL:
                self._cache.invalidate_all()
            else:
                self._cache.invalidate_table(write_table)
            return await self._real.fetchrow(sql, *args)

        if self._in_transaction:
            return await self._real.fetchrow(sql, *args)

        params = args if args else None
        entry = self._cache.get(sql, params, state_hash)
        if entry is not None:
            return entry.rows[0] if entry.rows else None

        row = await self._real.fetchrow(sql, *args)
        if row is not None:
            self._cache.put(sql, params, [row], None, state_hash)
        return row

    async def fetchval(self, sql, *args, column=0):
        self._guc_state.observe_sql(sql)
        state_hash = self._guc_state.hash
        write_table = _detect_write(sql)
        if write_table:
            if write_table == _DDL_SENTINEL:
                self._cache.invalidate_all()
            else:
                self._cache.invalidate_table(write_table)
            return await self._real.fetchval(sql, *args, column=column)

        if self._in_transaction:
            return await self._real.fetchval(sql, *args, column=column)

        params = args if args else None
        entry = self._cache.get(sql, params, state_hash)
        if entry is not None:
            if entry.rows:
                row = entry.rows[0]
                if hasattr(row, "__getitem__"):
                    return row[column]
                return row
            return None

        val = await self._real.fetchval(sql, *args, column=column)
        return val

    async def execute(self, sql, *args):
        # `execute` doesn't read into the cache (asyncpg's execute returns
        # status strings, not rows), but we still observe SETs so a
        # subsequent fetch* call sees the updated state hash.
        self._guc_state.observe_sql(sql)
        write_table = _detect_write(sql)
        if write_table:
            if write_table == _DDL_SENTINEL:
                self._cache.invalidate_all()
            else:
                self._cache.invalidate_table(write_table)
        return await self._real.execute(sql, *args)

    def transaction(self, **kwargs):
        return _AsyncTransactionWrapper(self, self._real.transaction(**kwargs))

    async def close(self):
        return await self._real.close()

    def __getattr__(self, name):
        return getattr(self._real, name)

    def __setattr__(self, name, value):
        setattr(self._real, name, value)


class _AsyncTransactionWrapper:
    def __init__(self, cached_conn, real_txn):
        self._cached_conn = cached_conn
        self._real_txn = real_txn

    async def __aenter__(self):
        object.__setattr__(self._cached_conn, "_in_transaction", True)
        return await self._real_txn.__aenter__()

    async def __aexit__(self, *args):
        result = await self._real_txn.__aexit__(*args)
        object.__setattr__(self._cached_conn, "_in_transaction", False)
        return result
