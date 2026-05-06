import atexit
import asyncio

from goldlapel.cache import (
    NativeCache,
    ConnectionGucState,
    _detect_writes_multi,
    _DDL_SENTINEL,
    is_top_level_function_call,
    update_tx_state,
)


# --- Driver detection ---
#
# The wrapper supports two async pool drivers (asyncpg.Pool, psycopg's
# AsyncConnectionPool) and two sync drivers (psycopg2 / psycopg). Both
# default to issuing `DISCARD ALL` on connection release back to the pool
# — which means the wrapper's `observe_sql` parser sees the DISCARD and
# clears the per-connection state hash + dirty flag automatically. No
# extra round-trip required.
#
# `_detect_driver` returns one of:
#   "asyncpg"        — connection is asyncpg.Connection (or pool-acquired)
#   "psycopg-async"  — connection is psycopg.AsyncConnection
#   "psycopg-sync"   — connection is psycopg.Connection / psycopg2 connection
#   "unknown"        — a custom-class connection (custom pools, mocks, etc.)
#
# Today the only behaviour-shaping use is the verify-on-checkout fallback:
# for "unknown" drivers we don't trust the default DISCARD-on-release
# behaviour, so we lean harder on the dirty-flag verify path.

_DRIVER_ASYNCPG = "asyncpg"
_DRIVER_PSYCOPG_ASYNC = "psycopg-async"
_DRIVER_PSYCOPG_SYNC = "psycopg-sync"
_DRIVER_UNKNOWN = "unknown"


_cache = None
_atexit_registered = False


def _maybe_verify_sync_pre_call(cached_conn):
    """Sync verify-on-checkout helper. Fires `maybe_verify` ONLY when:
    - the GUC state is dirty (an unsafe SET / a stored-function call has
      left state ambiguous), AND
    - the driver is "unknown" — meaning we can't trust the pool's default
      DISCARD-on-release behaviour to have already cleared state.

    The dirty flag is otherwise cleared inline by `apply()` when a
    DISCARD is observed on the wire, so the common path (asyncpg /
    psycopg with their default reset_strategy) never reaches verify.
    Errors are swallowed by `maybe_verify`.
    """
    state = cached_conn._guc_state
    if not state.dirty:
        return
    if cached_conn._driver != _DRIVER_UNKNOWN:
        # Trusted driver: rely on the parser observing DISCARD ALL on
        # release. If we got here despite that, the pool didn't reset
        # — caller has overridden the default. The dirty flag persists
        # and L1 / proxy still serve a state-hash-keyed slot, just one
        # that may not exactly mirror server state until a DISCARD or
        # explicit RESET ALL is issued.
        return
    state.maybe_verify(cached_conn._real)


async def _maybe_verify_async_pre_call(cached_conn):
    """Async verify-on-checkout helper. Same gating as the sync version.
    Errors are swallowed by `maybe_verify_async`."""
    state = cached_conn._guc_state
    if not state.dirty:
        return
    if cached_conn._driver != _DRIVER_UNKNOWN:
        return
    await state.maybe_verify_async(cached_conn._real)


def _detect_driver(real_conn):
    """Return a string identifier for the underlying driver. Detection is
    by-class (no module imports forced): we check the conn's qualified
    class name. Mocks and shimmed test doubles land on "unknown" — which
    is the safe answer (we don't assume DISCARD-on-release).
    """
    cls = type(real_conn)
    # Walk the MRO so subclasses (asyncpg.pool.PoolAcquireContext returns
    # a subclass of Connection, etc.) get classified correctly.
    for ancestor in cls.__mro__:
        qualname = f"{ancestor.__module__}.{ancestor.__name__}"
        if qualname == "asyncpg.connection.Connection":
            return _DRIVER_ASYNCPG
        if qualname == "psycopg.AsyncConnection":
            return _DRIVER_PSYCOPG_ASYNC
        if qualname == "psycopg.Connection":
            return _DRIVER_PSYCOPG_SYNC
        if qualname == "psycopg2.extensions.connection":
            return _DRIVER_PSYCOPG_SYNC
    return _DRIVER_UNKNOWN


def _apply_write_invalidation(cache, write_result):
    """Invalidate cache entries based on `_detect_writes_multi` output.

    Returns True if any invalidation happened (i.e. the SQL was a write
    body and the caller should skip the read path). False means no
    writes were detected.
    """
    if write_result is None:
        return False
    if write_result is _DDL_SENTINEL:
        cache.invalidate_all()
        return True
    # Non-empty set of bare table names.
    for table in write_result:
        cache.invalidate_table(table)
    return True


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
        # Driver identifier — used today by the verify-on-checkout
        # fallback to decide whether the underlying pool's
        # DISCARD-on-release default can be trusted. Detected once at
        # wrap time; the underlying conn class doesn't change after that.
        object.__setattr__(self, "_driver", _detect_driver(real_conn))

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

        # Verify-on-checkout: if a prior unsafe SET or top-level function
        # call left the connection dirty AND the driver isn't one we
        # trust to issue DISCARD on release (asyncpg / psycopg / psycopg2
        # all default to DISCARD-on-release), reconcile state by reading
        # pg_settings before consulting the cache. Cheap when not dirty
        # (single attribute check); ~1ms when fired. Errors swallowed —
        # the pre-existing observe-on-the-wire path still produces a
        # reasonable state hash even if verify fails.
        if self._conn is not None:
            _maybe_verify_sync_pre_call(self._conn)

        # Observe SET / RESET commands BEFORE any cache decision so a
        # multi-statement Q like `SET app.user_id='42'; SELECT ...`
        # updates state and then keys the SELECT under the new hash.
        # observe_sql is fast-pathed for non-SET statements (no allocation
        # when there's no `;` and no "SET"/"RESET" prefix to match).
        if self._conn is not None:
            self._conn._guc_state.observe_sql(sql)
        state_hash = self._conn._guc_state.hash if self._conn is not None else 0

        # Top-level `SELECT <user_func>(...)` mark-dirty: the function body
        # might have done a server-side `SET` we never saw on the wire.
        # Flagging dirty post-call schedules the next `_maybe_verify_*_pre_call`
        # to reconcile state via pg_settings (item #6 in the RLS hardening
        # spec). The async path uses asyncio.create_task to verify
        # immediately after the user's response is dispatched; the sync
        # path just sets the flag and lets the next checkout re-read
        # pg_settings — no fire-and-forget threads in the sync wrapper,
        # which keeps the threading model simple.
        if (
            self._conn is not None
            and is_top_level_function_call(sql)
        ):
            self._conn._guc_state.mark_dirty()

        # Write detection + self-invalidation (always, even in transactions
        # or when the body has tx markers — `BEGIN; INSERT INTO t; COMMIT`
        # must still invalidate `t`). Multi-statement-aware so a single Q
        # message like `SET app.user_id='42'; INSERT INTO orders VALUES (1)`
        # also invalidates `orders` even though the first token is `SET`.
        write_found = _apply_write_invalidation(
            self._cache, _detect_writes_multi(sql)
        )

        # Transaction tracking — segment-walking so a multi-statement body
        # like `BEGIN; INSERT INTO t VALUES (1); COMMIT` converges the
        # wrapper's tx flag to the server's actual end-state (out-of-tx),
        # not just the first token's intent (in-tx). State lives on the
        # connection, not the cursor.
        if self._conn is not None:
            new_tx, had_tx_marker = update_tx_state(
                self._conn._in_transaction, sql
            )
            if new_tx != self._conn._in_transaction:
                object.__setattr__(self._conn, "_in_transaction", new_tx)
        else:
            had_tx_marker = False
        # Bodies that contain tx-boundary segments (BEGIN/COMMIT/etc.) are
        # never cached — the response is a status string, and pretending to
        # serve it from cache would skip a real server round-trip the
        # caller intends. Same shape as a write-found short-circuit.
        if had_tx_marker:
            return self._real.execute(sql, params)

        if write_found:
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
            _maybe_verify_sync_pre_call(self._conn)
            self._conn._guc_state.observe_sql(sql)
            # Top-level function calls in executemany — also mark dirty
            # so the next call's pre-call verify reconciles state. The
            # function body might mutate state on every row.
            if is_top_level_function_call(sql):
                self._conn._guc_state.mark_dirty()
        _apply_write_invalidation(self._cache, _detect_writes_multi(sql))
        # Track tx markers for parity with `execute`. Multi-statement bodies
        # containing BEGIN/COMMIT in `executemany` are unusual but the cost
        # is negligible (single-statement fast path) and keeps the wrapper's
        # `_in_transaction` flag from drifting if a caller does it anyway.
        if self._conn is not None:
            new_tx, _had_tx_marker = update_tx_state(
                self._conn._in_transaction, sql
            )
            if new_tx != self._conn._in_transaction:
                object.__setattr__(self._conn, "_in_transaction", new_tx)
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
        # Driver classification — currently only relevant for the verify-
        # on-checkout fallback gating. asyncpg / psycopg async pools both
        # default to DISCARD-on-release, so the parser observing DISCARD
        # ALL clears the dirty flag without an extra round-trip.
        object.__setattr__(self, "_driver", _detect_driver(real_conn))
        # Background verify tasks scheduled from `_schedule_post_call_verify`
        # (item #6 in the RLS hardening spec). Tracked so `close()` can
        # cancel them — never block the user's hot path, never leak a
        # dangling coroutine on connection teardown.
        object.__setattr__(self, "_pending_verify_tasks", set())
        # asyncpg / psycopg-async connections are single-op: at most one
        # `await fetch(...)` in flight at a time. The post-call verify
        # task and the user's NEXT call would race for the connection's
        # protocol lock without explicit serialization. This wrapper-side
        # lock makes the verify task and any subsequent user call wait
        # cleanly behind each other instead of one of them raising
        # `InterfaceError: another operation is in progress`. Lazily
        # created on first acquire so sync paths through the async
        # wrapper don't fail on "no running event loop".
        object.__setattr__(self, "_op_lock", None)

    def _get_op_lock(self):
        """Lazily create the per-connection op lock. asyncio.Lock requires
        a running event loop at construction time on some Python versions,
        so we defer creation until the first await on the wire.
        """
        if self._op_lock is None:
            object.__setattr__(self, "_op_lock", asyncio.Lock())
        return self._op_lock

    def _observe_tx(self, sql):
        """Walk segments to update `_in_transaction`. Returns
        `had_tx_marker` so the caller can short-circuit cache decisions
        on bodies that contain tx-boundary segments (the response is a
        status string, not rows worth caching). Mirrors the sync path's
        `update_tx_state` call.
        """
        new_tx, had_tx_marker = update_tx_state(self._in_transaction, sql)
        if new_tx != self._in_transaction:
            object.__setattr__(self, "_in_transaction", new_tx)
        return had_tx_marker

    def _schedule_post_call_verify(self, sql):
        """If `sql` is a top-level `SELECT <ident>(...)` whose function body
        could plausibly mutate session state, schedule an async verify
        task to read pg_settings AFTER the user's response is dispatched.

        The task uses `asyncio.create_task` so it runs concurrently with
        whatever the user does next. The op-lock serializes the verify
        task with subsequent user calls — asyncpg / psycopg-async
        connections are single-op, so without the lock the verify and
        the next user call would race for the protocol and one of them
        would raise `InterfaceError: another operation is in progress`.

        Errors are swallowed by `_run_verify_locked`. On failure the
        connection stays dirty and the next pre-call verify retries.
        Cancellation (e.g. `close()` while verify is in flight) is
        absorbed without surfacing to the user.
        """
        if not is_top_level_function_call(sql):
            return
        # Mark dirty BEFORE scheduling — even if create_task isn't
        # possible (we're not in an event loop, somehow), the dirty
        # flag survives and the next pre-call verify reconciles state.
        self._guc_state.mark_dirty()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running event loop — caller is in a sync path through
            # the async wrapper (rare). Fall back to mark-dirty only;
            # the next call's pre-call verify (also async) will run.
            return
        task = loop.create_task(self._run_verify_locked())
        # Add to tracking set BEFORE add_done_callback so cancellation
        # during scheduling doesn't leak. Discard on completion.
        self._pending_verify_tasks.add(task)
        task.add_done_callback(self._pending_verify_tasks.discard)

    async def _run_verify_locked(self):
        """Run `maybe_verify_async` under the op-lock so it serializes
        with concurrent user calls. The user's NEXT call after a
        function-call observation will wait for verify to finish before
        starting its own wire op (and vice versa) — eliminating the
        single-op-per-connection race."""
        async with self._get_op_lock():
            await self._guc_state.maybe_verify_async(self._real)

    async def fetch(self, sql, *args):
        async with self._get_op_lock():
            await _maybe_verify_async_pre_call(self)
            self._guc_state.observe_sql(sql)
            state_hash = self._guc_state.hash
            write_found = _apply_write_invalidation(
                self._cache, _detect_writes_multi(sql)
            )
            had_tx_marker = self._observe_tx(sql)
            if had_tx_marker or write_found:
                result = await self._real.fetch(sql, *args)
                self._schedule_post_call_verify(sql)
                return result

            if self._in_transaction:
                result = await self._real.fetch(sql, *args)
                self._schedule_post_call_verify(sql)
                return result

            params = args if args else None
            entry = self._cache.get(sql, params, state_hash)
            if entry is not None:
                # Cache hit — no real call landed on the wire, so no
                # post-call verify is needed (and triggering one would
                # race the user's next call without any new state-
                # mutation risk).
                return entry.rows

            rows = await self._real.fetch(sql, *args)
            # Skip caching empty result sets — most often these are
            # session-state commands like `SET` / `RESET` / `LISTEN`
            # routed through `fetch()`, which return `[]` and would
            # otherwise pollute the cache without ever serving a real
            # row. (psycopg's sync path already filters via `description
            # is not None`; asyncpg has no equivalent — `fetch` always
            # returns a list — so we gate on emptiness instead.)
            if rows:
                self._cache.put(sql, params, list(rows), None, state_hash)
            self._schedule_post_call_verify(sql)
            return rows

    async def fetchrow(self, sql, *args):
        async with self._get_op_lock():
            await _maybe_verify_async_pre_call(self)
            self._guc_state.observe_sql(sql)
            state_hash = self._guc_state.hash
            write_found = _apply_write_invalidation(
                self._cache, _detect_writes_multi(sql)
            )
            had_tx_marker = self._observe_tx(sql)
            if had_tx_marker or write_found:
                result = await self._real.fetchrow(sql, *args)
                self._schedule_post_call_verify(sql)
                return result

            if self._in_transaction:
                result = await self._real.fetchrow(sql, *args)
                self._schedule_post_call_verify(sql)
                return result

            params = args if args else None
            entry = self._cache.get(sql, params, state_hash)
            if entry is not None:
                return entry.rows[0] if entry.rows else None

            row = await self._real.fetchrow(sql, *args)
            if row is not None:
                self._cache.put(sql, params, [row], None, state_hash)
            self._schedule_post_call_verify(sql)
            return row

    async def fetchval(self, sql, *args, column=0):
        async with self._get_op_lock():
            await _maybe_verify_async_pre_call(self)
            self._guc_state.observe_sql(sql)
            state_hash = self._guc_state.hash
            write_found = _apply_write_invalidation(
                self._cache, _detect_writes_multi(sql)
            )
            had_tx_marker = self._observe_tx(sql)
            if had_tx_marker or write_found:
                result = await self._real.fetchval(sql, *args, column=column)
                self._schedule_post_call_verify(sql)
                return result

            if self._in_transaction:
                result = await self._real.fetchval(sql, *args, column=column)
                self._schedule_post_call_verify(sql)
                return result

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
            self._schedule_post_call_verify(sql)
            return val

    async def execute(self, sql, *args):
        # `execute` doesn't read into the cache (asyncpg's execute returns
        # status strings, not rows), but we still observe SETs so a
        # subsequent fetch* call sees the updated state hash, and walk
        # segments for tx markers so a body like
        # `BEGIN; INSERT INTO t VALUES (1); COMMIT` converges
        # `_in_transaction` to the server's actual end-state instead of
        # tracking only the first token.
        async with self._get_op_lock():
            await _maybe_verify_async_pre_call(self)
            self._guc_state.observe_sql(sql)
            _apply_write_invalidation(self._cache, _detect_writes_multi(sql))
            self._observe_tx(sql)
            result = await self._real.execute(sql, *args)
            self._schedule_post_call_verify(sql)
            return result

    def transaction(self, **kwargs):
        return _AsyncTransactionWrapper(self, self._real.transaction(**kwargs))

    async def close(self):
        # Cancel any in-flight verify tasks. Cancellation is awaited so we
        # don't return from close() while a verify is still talking to
        # the connection we're about to close. CancelledError on the
        # task is expected and absorbed.
        await self._cancel_pending_verifies()
        return await self._real.close()

    async def _cancel_pending_verifies(self):
        """Cancel and drain pending post-call verify tasks. Used by
        `close()` and exposed for tests. Safe to call multiple times.
        """
        if not self._pending_verify_tasks:
            return
        # Snapshot so concurrent done-callbacks discarding from the set
        # don't perturb the iteration.
        tasks = list(self._pending_verify_tasks)
        for task in tasks:
            if not task.done():
                task.cancel()
        # Wait with return_exceptions so a CancelledError on each task
        # doesn't escape — the spec is "NEVER fail the user's query if
        # verify itself errors", and `close()` is the user's call.
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def __getattr__(self, name):
        return getattr(self._real, name)

    def __setattr__(self, name, value):
        setattr(self._real, name, value)


class _AsyncTransactionWrapper:
    def __init__(self, cached_conn, real_txn):
        self._cached_conn = cached_conn
        self._real_txn = real_txn

    async def __aenter__(self):
        # BEGIN is a wire op too — must serialize with any in-flight
        # post-call verify task or we'd race the connection's protocol
        # lock and asyncpg would raise InterfaceError.
        async with self._cached_conn._get_op_lock():
            object.__setattr__(self._cached_conn, "_in_transaction", True)
            return await self._real_txn.__aenter__()

    async def __aexit__(self, *args):
        # COMMIT / ROLLBACK is a wire op too — serialize.
        async with self._cached_conn._get_op_lock():
            result = await self._real_txn.__aexit__(*args)
            object.__setattr__(self._cached_conn, "_in_transaction", False)
            return result
