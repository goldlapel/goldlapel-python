import json
import os
import re
import socket
import sys
import threading
import time
import uuid
from collections import namedtuple, OrderedDict
from importlib import metadata as _metadata

_DDL_SENTINEL = "__ddl__"

# --- native cache telemetry tuning ---
#
# Demand-driven model (2026-05-02): the wrapper has NO background timer.
# Cache counters increment on cache ops (free); state-change events are
# emitted synchronously when a relevant counter crosses a threshold;
# snapshot replies are sent only when the proxy asks via ?:<request>.
#
# Eviction-rate sliding window. cache_full fires when ≥
# `_EVICT_RATE_HIGH` of the last `_EVICT_RATE_WINDOW` cache writes
# (puts) caused an eviction; cache_recovered fires when the rate falls
# back below `_EVICT_RATE_LOW`. With a 32k-entry default capacity, a
# steady-state high eviction rate means the working set exceeds the
# cache — actionable signal for the dashboard.
_EVICT_RATE_WINDOW = 200
_EVICT_RATE_HIGH = 0.5  # 50% of recent puts evicted → cache_full
_EVICT_RATE_LOW = 0.1   # ≤ 10% → cache_recovered

CacheEntry = namedtuple("CacheEntry", ["rows", "description", "tables"])

_TX_START = re.compile(r"^\s*(BEGIN|START\s+TRANSACTION)\b", re.IGNORECASE)
_TX_END = re.compile(r"^\s*(COMMIT|ROLLBACK|END)\b", re.IGNORECASE)

_TABLE_PATTERN = re.compile(
    r"\b(?:FROM|JOIN)\s+(?:ONLY\s+)?(?:(\w+)\.)?(\w+)",
    re.IGNORECASE,
)

_SQL_KEYWORDS = frozenset({
    "select", "from", "where", "and", "or", "not", "in", "exists",
    "between", "like", "is", "null", "true", "false", "as", "on",
    "left", "right", "inner", "outer", "cross", "full", "natural",
    "group", "order", "having", "limit", "offset", "union", "intersect",
    "except", "all", "distinct", "lateral", "values",
})


# --- Per-connection GUC state tracking (L1 cache-key safety) ---
#
# Mirrors the proxy's `src/guc_state.rs` (Wave 2.5, "Option Y"). Custom-GUC-
# driven RLS — `SET app.user_id = '42'; SELECT * FROM accounts;` where the
# RLS policy reads `current_setting('app.user_id')` — can leak user A's
# results to user B if the L1 native cache is keyed purely by SQL+params.
# Each `CachedConnection` carries its own `ConnectionGucState`; the state
# hash is folded into the cache key so two connections with different
# unsafe-GUC values never share a cache slot.
#
# Classifier: a GUC name is **unsafe** if it's in the short hardcoded list
# below OR contains a `.` (namespaced — `app.*`, `myapp.*`, etc., the
# canonical pattern for custom RLS state). Match is case-insensitive.
#
# `SET LOCAL` is intentionally ignored for state-hash purposes: SET LOCAL
# only takes effect inside a transaction, and the wrapper already bypasses
# the cache for in-transaction reads (`_in_transaction`), so a SET LOCAL
# never influences a cacheable response.

_UNSAFE_GUC_SHORT_LIST = frozenset({
    "search_path",
    "role",
    "session_authorization",
    "default_transaction_isolation",
    "default_transaction_read_only",
    "transaction_isolation",
    "row_security",
})


def is_unsafe_guc(name):
    """Classify a GUC name as state-affecting (True) or harmless (False).

    Case-insensitive. A GUC is unsafe if it's in the short hardcoded list
    (search_path, role, session_authorization, default_transaction_*,
    transaction_isolation, row_security) OR contains a `.` (namespaced —
    app.*, myapp.*, etc.).
    """
    lower = name.lower()
    if "." in lower:
        return True
    return lower in _UNSAFE_GUC_SHORT_LIST


def split_statements(sql):
    """Split a SQL string on top-level `;` characters, respecting single-
    and double-quoted string literals (PG's doubled-quote escape `''`/`""`
    handled). Returns each segment with surrounding whitespace trimmed;
    empty segments dropped.

    The lightest possible "statement splitter" — does not understand
    dollar-quoted strings or comments. Good enough for splitting
    `SET foo='a'; SELECT 1`-style multi-statement bodies, which is the
    only reason it exists.
    """
    out = []
    start = 0
    quote = None
    i = 0
    n = len(sql)
    while i < n:
        c = sql[i]
        if quote is not None:
            if c == quote:
                # PG's `''` and SQL-standard `""` doubled-quote escape.
                if i + 1 < n and sql[i + 1] == quote:
                    i += 2
                    continue
                quote = None
        else:
            if c == "'" or c == '"':
                quote = c
            elif c == ";":
                segment = sql[start:i].strip()
                if segment:
                    out.append(segment)
                start = i + 1
        i += 1
    tail = sql[start:].strip()
    if tail:
        out.append(tail)
    return out


# Marker for `SetCommand` kinds. Returned as a 4-tuple
# `(kind, name, value)` from `parse_set_command` so callers can pattern-
# match without paying for a class allocation per call. Kinds:
#   "set"        — SET name = value (and SET SESSION ... variant)
#   "set_local"  — SET LOCAL name = value (ignored for state hash)
#   "reset"      — RESET name
#   "reset_all"  — RESET ALL
SET_KIND_SET = "set"
SET_KIND_SET_LOCAL = "set_local"
SET_KIND_RESET = "reset"
SET_KIND_RESET_ALL = "reset_all"


def _normalize_guc_name(token):
    """Lowercase the GUC name and strip surrounding double quotes (PG
    treats `"app.user_id"` and `app.user_id` as the same identifier
    when it's a configuration parameter — we discard case anyway)."""
    trimmed = token.strip('"')
    if not trimmed:
        return None
    return trimmed.lower()


def _strip_value_quotes(value):
    """Strip a single layer of matching surrounding quotes (`'...'` or
    `"..."`) from a value. Multi-token quoted values like `'foo bar'`
    arrive as the joined string already; this just peels the outer
    quotes."""
    v = value.strip()
    if len(v) >= 2:
        first = v[0]
        last = v[-1]
        if (first == "'" and last == "'") or (first == '"' and last == '"'):
            return v[1:-1]
    return v


def parse_set_command(sql):
    """Parse a `SET` / `RESET` command out of a single SQL statement.

    Recognises:
    - `SET name = value`, `SET name TO value`
    - `SET SESSION name = value`, `SET SESSION name TO value`
    - `SET LOCAL name = value`, `SET LOCAL name TO value`
    - `RESET name`
    - `RESET ALL`

    Returns `(kind, name, value)` where `kind` is one of `SET_KIND_*`.
    For `RESET ALL`, returns `("reset_all", None, None)`. For `RESET name`,
    returns `("reset", name, None)`. For SET / SET LOCAL, returns
    `("set"|"set_local", name, value)`.

    Returns `None` for anything else (including `SET TIME ZONE 'UTC'` —
    the legacy two-word form. Timezone is harmless and the unusual
    two-word GUC name doesn't fit the pattern; treating it as "not a
    trackable SET" is correct because it doesn't affect cache safety).

    Intentionally narrow — handles a single statement. For multi-
    statement SQL, use `split_statements()` first and call this on each
    segment.
    """
    s = sql.strip()
    # Strip a trailing semicolon if present.
    if s.endswith(";"):
        s = s[:-1].rstrip()
    if not s:
        return None

    tokens = s.split()
    if not tokens:
        return None
    head = tokens[0]

    # RESET branch.
    if head.lower() == "reset":
        if len(tokens) < 2:
            return None
        target = tokens[1]
        # `RESET name` — anything after `name` is junk we don't expect.
        if len(tokens) > 2:
            return None
        if target.lower() == "all":
            return (SET_KIND_RESET_ALL, None, None)
        name = _normalize_guc_name(target)
        if name is None:
            return None
        return (SET_KIND_RESET, name, None)

    if head.lower() != "set":
        return None

    # SET branch — check for optional LOCAL/SESSION modifier.
    idx = 1
    if idx >= len(tokens):
        return None
    modifier = tokens[idx].lower()
    is_local = False
    if modifier == "local":
        is_local = True
        idx += 1
    elif modifier == "session":
        idx += 1

    if idx >= len(tokens):
        return None
    next_tok = tokens[idx]
    idx += 1

    # The next token may have an `=` glued onto it (e.g. `SET app.user='42'`).
    glued_value = None
    if "=" in next_tok:
        eq_pos = next_tok.find("=")
        name_token = next_tok[:eq_pos]
        rest = next_tok[eq_pos + 1:]
        glued_value = rest if rest else None
    else:
        name_token = next_tok

    name = _normalize_guc_name(name_token)
    if name is None:
        return None

    # Resolve the value string.
    if glued_value is not None:
        rest_after = " ".join(tokens[idx:])
        if rest_after:
            value_str = f"{glued_value} {rest_after}"
        else:
            value_str = glued_value
    else:
        if idx >= len(tokens):
            return None
        sep = tokens[idx]
        idx += 1
        if sep != "=" and sep.lower() != "to":
            return None
        if idx >= len(tokens):
            # `SET foo =` / `SET foo TO` with no value.
            return None
        value_str = " ".join(tokens[idx:])

    value = _strip_value_quotes(value_str.strip())
    # Reject empty values (e.g. `SET foo = ''` would have a value of "" —
    # but the original `value_str` is `''`, which `strip` leaves as `''`,
    # which `_strip_value_quotes` peels to "". This is a real value (empty
    # string) and we accept it, distinguishing from `SET foo =` which we
    # reject above.).
    if not value_str.strip():
        return None

    if is_local:
        return (SET_KIND_SET_LOCAL, name, value)
    return (SET_KIND_SET, name, value)


class ConnectionGucState:
    """Per-connection unsafe-GUC state tracker.

    Stores values for unsafe GUCs only (harmless GUCs — timezone,
    application_name, planner cost knobs, etc. — never enter the map and
    never affect the hash). The state hash is recomputed on every
    mutation and folded into L1 cache keys.

    Hash is `0` for the empty (default) state, so a fresh connection's
    state hash matches "no GUCs set" cache slots from peer connections —
    which is the correct, secure default (any connection that has set an
    unsafe GUC gets a non-zero hash).
    """

    __slots__ = ("_values", "_hash")

    def __init__(self):
        self._values = {}  # lowercased GUC name → raw value string
        self._hash = 0

    @property
    def hash(self):
        """Current state hash. `0` for empty state."""
        return self._hash

    def apply(self, cmd):
        """Apply a parsed (kind, name, value) tuple from
        `parse_set_command`. No-op for SetLocal (cache is bypassed in
        transactions anyway), no-op for safe GUC names. Returns True
        if the hash mutated."""
        kind, name, value = cmd
        if kind == SET_KIND_SET:
            if is_unsafe_guc(name):
                prev = self._values.get(name)
                if prev != value:
                    self._values[name] = value
                    self._recompute_hash()
                    return True
            return False
        if kind == SET_KIND_SET_LOCAL:
            # Intentionally ignored (see class docstring).
            return False
        if kind == SET_KIND_RESET:
            if is_unsafe_guc(name) and name in self._values:
                del self._values[name]
                self._recompute_hash()
                return True
            return False
        if kind == SET_KIND_RESET_ALL:
            if self._values:
                self._values.clear()
                self._recompute_hash()
                return True
            return False
        return False

    def observe_sql(self, sql):
        """Observe a SQL string for SET / RESET commands and update
        state accordingly. Multi-statement bodies are split on top-level
        `;` (string literals respected) so a single Q like
        `SET app.user_id = '42'; SELECT 1` still updates state. Returns
        True if the hash mutated.
        """
        before = self._hash
        # Fast path for the common single-statement case — avoid
        # allocating the list from split_statements for every wire
        # message that isn't a multi-statement body.
        trimmed = sql.rstrip()
        if trimmed.endswith(";"):
            trimmed = trimmed[:-1]
        if ";" not in trimmed:
            cmd = parse_set_command(sql)
            if cmd is not None:
                self.apply(cmd)
        else:
            for stmt in split_statements(sql):
                cmd = parse_set_command(stmt)
                if cmd is not None:
                    self.apply(cmd)
        return self._hash != before

    def _recompute_hash(self):
        if not self._values:
            self._hash = 0
            return
        # Sorted iteration → deterministic hash regardless of insertion
        # order. Python's `hash()` is process-randomised, but the state
        # hash never crosses process boundaries (it's only ever used as
        # part of a local L1 cache key), so that's fine.
        self._hash = hash(tuple(sorted(self._values.items())))


def _make_key(sql, params, state_hash=0):
    if params is None:
        params_part = None
    elif isinstance(params, dict):
        params_part = tuple(sorted(params.items()))
    else:
        params_part = tuple(params)
    # state_hash defaults to 0 — empty / fresh-connection state. Two
    # connections that have never set an unsafe GUC share the same key
    # for the same SQL+params (which is correct — they have identical
    # security context).
    return (sql, params_part, state_hash)


def _detect_write(sql):
    trimmed = sql.strip()
    tokens = trimmed.split()
    if not tokens:
        return None
    first = tokens[0].upper()

    if first == "INSERT":
        if len(tokens) < 3 or tokens[1].upper() != "INTO":
            return None
        return _bare_table(tokens[2])
    elif first == "UPDATE":
        if len(tokens) < 2:
            return None
        return _bare_table(tokens[1])
    elif first == "DELETE":
        if len(tokens) < 3 or tokens[1].upper() != "FROM":
            return None
        return _bare_table(tokens[2])
    elif first == "TRUNCATE":
        if len(tokens) < 2:
            return None
        if tokens[1].upper() == "TABLE":
            if len(tokens) < 3:
                return None
            return _bare_table(tokens[2])
        return _bare_table(tokens[1])
    elif first in ("CREATE", "ALTER", "DROP", "REFRESH", "DO", "CALL"):
        return _DDL_SENTINEL
    elif first == "MERGE":
        if len(tokens) < 3 or tokens[1].upper() != "INTO":
            return None
        return _bare_table(tokens[2])
    elif first == "SELECT":
        saw_into = False
        into_target = None
        for tok in tokens[1:]:
            upper = tok.upper()
            if upper == "INTO" and not saw_into:
                saw_into = True
                continue
            if saw_into and into_target is None:
                if upper in ("TEMPORARY", "TEMP", "UNLOGGED"):
                    continue
                into_target = tok
                continue
            if saw_into and into_target is not None and upper == "FROM":
                return _DDL_SENTINEL
            if upper == "FROM":
                return None
        return None
    elif first == "COPY":
        if len(tokens) < 2:
            return None
        raw = tokens[1]
        if raw.startswith("("):
            return None
        table_part = raw.split("(")[0]
        for tok in tokens[2:]:
            upper = tok.upper()
            if upper == "FROM":
                return _bare_table(table_part)
            if upper == "TO":
                return None
        return None
    elif first == "WITH":
        rest_upper = trimmed[len(tokens[0]):].upper()
        for token in rest_upper.split():
            word = token.lstrip("(")
            if word in ("INSERT", "UPDATE", "DELETE"):
                return _DDL_SENTINEL
        return None

    return None


def _bare_table(raw):
    table = raw.split("(")[0]
    table = table.rsplit(".", 1)[-1]
    return table.lower()


def _extract_tables(sql):
    tables = set()
    for match in _TABLE_PATTERN.finditer(sql):
        table = match.group(2).lower()
        if table not in _SQL_KEYWORDS:
            tables.add(table)
    return tables


class NativeCache:
    _instance = None
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        # Accept (and discard) ctor args so the singleton constructor
        # signature stays in sync with __init__. Real handling lives in
        # __init__ — __new__ exists only to enforce the singleton.
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self, *, disabled=False):
        if self._initialized:
            # Singleton already constructed — propagate `disabled` so a
            # later wrap() can flip the flag (e.g. a second start() in
            # the same process). The cache is process-wide; most-recent
            # caller wins.
            self._disabled = bool(disabled)
            return
        self._cache = OrderedDict()
        self._table_index = {}
        self._max_entries = int(os.environ.get("GOLDLAPEL_NATIVE_CACHE_SIZE", "32768"))
        self._enabled = os.environ.get("GOLDLAPEL_NATIVE_CACHE", "true").lower() != "false"
        # Explicit native-cache disable: get() always misses, put() is a no-op.
        # Distinct from _enabled (env-var on/off) and _invalidation_connected
        # (transport state). When disabled, counters still tick so the
        # dashboard sees "wrapper connected, 0 hits, N misses" — a clear
        # signal that the native cache is intentionally off rather than the
        # wrapper being silent.
        self._disabled = bool(disabled)
        self._lock = threading.Lock()
        self._invalidation_connected = False
        self._invalidation_thread = None
        self._invalidation_stop = threading.Event()
        self._invalidation_port = 0
        self._reconnect_attempt = 0
        self.stats_hits = 0
        self.stats_misses = 0
        self.stats_invalidations = 0
        # native cache telemetry (2026-05-02). Eviction counter — was missing
        # before; bumped in `_evict_one`. Configurable opt-out: set
        # GOLDLAPEL_REPORT_STATS=false to disable all snapshot replies
        # and state-change emissions (cache continues to function).
        self.stats_evictions = 0
        self._report_stats = (
            os.environ.get("GOLDLAPEL_REPORT_STATS", "true").lower() != "false"
        )
        # Stable wrapper identity for the lifetime of the process.
        # Lets the proxy aggregate per wrapper across reconnects.
        self._wrapper_id = str(uuid.uuid4())
        self._wrapper_lang = "python"
        try:
            self._wrapper_version = _metadata.version("goldlapel")
        except Exception:
            self._wrapper_version = "unknown"
        # Synchronizes writes from the recv thread (replies to ?:) and
        # any cache-op thread (state-change emissions). The socket is a
        # single full-duplex stream; concurrent writes would interleave
        # bytes. recv stays on the existing thread, send is serialized
        # behind this lock.
        self._socket = None
        self._send_lock = threading.Lock()
        # Sliding window for eviction-rate state-change detection. A
        # bounded ring buffer; updates are O(1) amortised.
        self._recent_evictions = []  # 1 = evicted, 0 = inserted; len ≤ window
        self._recent_evictions_idx = 0
        # Latched state — only emit a state-change event when the state
        # transitions. Without latching the wrapper would re-emit every
        # tick the rate stays bad.
        self._state_cache_full = False
        self._initialized = True

    def get(self, sql, params, state_hash=0):
        if not self._enabled or not self._invalidation_connected:
            return None
        # Disabled mode: always miss, but still tick the counter so the
        # dashboard sees "wrapper alive, 0 hits, N misses" — i.e. the
        # native cache is explicitly off, not silent. Skip key computation (no
        # point) — even unhashable params bump the miss counter, which
        # is the desired signal: we attempted a get, the cache said no.
        if self._disabled:
            with self._lock:
                self.stats_misses += 1
            return None
        try:
            key = _make_key(sql, params, state_hash)
            hash(key)
        except TypeError:
            return None
        with self._lock:
            entry = self._cache.get(key)
            if entry is not None:
                self._cache.move_to_end(key)
                self.stats_hits += 1
                return entry
            self.stats_misses += 1
            return None

    def put(self, sql, params, rows, description, state_hash=0):
        if not self._enabled or not self._invalidation_connected:
            return
        # Disabled mode: silent no-op. We never store, so eviction can't
        # fire, so stats_evictions stays at 0 — another clear "native cache off"
        # signal in the dashboard snapshot.
        if self._disabled:
            return
        try:
            key = _make_key(sql, params, state_hash)
            hash(key)
        except TypeError:
            return
        tables = _extract_tables(sql)
        evicted = 0
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            elif len(self._cache) >= self._max_entries:
                self._evict_one()
                evicted = 1
            self._cache[key] = CacheEntry(rows, description, tables)
            for table in tables:
                if table not in self._table_index:
                    self._table_index[table] = set()
                self._table_index[table].add(key)
            self._record_eviction_locked(evicted)
        # Eviction-rate threshold check happens outside the lock — emit
        # may take `_send_lock` and we don't want to nest locks.
        self._maybe_emit_eviction_rate_state_change()

    def invalidate_table(self, table):
        table = table.lower()
        with self._lock:
            keys = self._table_index.pop(table, set())
            for key in keys:
                entry = self._cache.pop(key, None)
                if entry:
                    for other_table in entry.tables:
                        if other_table != table and other_table in self._table_index:
                            self._table_index[other_table].discard(key)
            self.stats_invalidations += len(keys)

    def invalidate_all(self):
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            self._table_index.clear()
            self.stats_invalidations += count

    def connect_invalidation(self, port):
        if self._invalidation_thread and self._invalidation_thread.is_alive():
            return
        self._invalidation_port = port
        self._invalidation_stop.clear()
        self._reconnect_attempt = 0
        self._invalidation_thread = threading.Thread(
            target=self._invalidation_loop, daemon=True
        )
        self._invalidation_thread.start()

    def stop_invalidation(self):
        self._invalidation_stop.set()
        if self._invalidation_thread:
            self._invalidation_thread.join(timeout=5)
            self._invalidation_thread = None
        self._invalidation_connected = False

    def _invalidation_loop(self):
        port = self._invalidation_port
        sock_path = f"/tmp/goldlapel-{port}.sock"

        while not self._invalidation_stop.is_set():
            sock = None
            try:
                if sys.platform != "win32" and os.path.exists(sock_path):
                    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    sock.connect(sock_path)
                else:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect(("127.0.0.1", port))

                sock.settimeout(30.0)
                self._invalidation_connected = True
                self._reconnect_attempt = 0
                # Stash the socket so `_send_line` (called from cache-op
                # threads on state-change, and from `_process_request`
                # on this thread for ?:/R:) writes to the live FD. Set
                # before the wrapper_connected emit so the very first
                # message goes out cleanly.
                self._socket = sock
                self._emit_state_change("wrapper_connected")
                buf = b""

                while not self._invalidation_stop.is_set():
                    try:
                        data = sock.recv(4096)
                        if not data:
                            break
                        buf += data
                        while b"\n" in buf:
                            line, buf = buf.split(b"\n", 1)
                            self._process_signal(
                                line.decode("utf-8", errors="replace")
                            )
                    except socket.timeout:
                        break

            except (OSError, ConnectionRefusedError):
                pass
            finally:
                # Drop the socket reference under the send lock so any
                # concurrent emitter doesn't write to a closed FD.
                with self._send_lock:
                    self._socket = None
                if self._invalidation_connected:
                    self._invalidation_connected = False
                    self.invalidate_all()
                if sock:
                    try:
                        sock.close()
                    except OSError:
                        pass

            wait_secs = min(2 ** self._reconnect_attempt, 15)
            if self._invalidation_stop.wait(wait_secs):
                return
            self._reconnect_attempt += 1

    def _process_signal(self, line):
        # Backwards-compat: unknown prefixes are silently ignored. Older
        # proxies sent only `I:` and `C:` and `P:` (keepalive); newer
        # proxies may add request types here. Forward-compat: the
        # wrapper accepts any well-formed prefix and routes by type.
        if line.startswith("I:"):
            table = line[2:].strip()
            if table == "*":
                self.invalidate_all()
            else:
                self.invalidate_table(table)
        elif line.startswith("?:"):
            # Snapshot request from the proxy. Reply with R:<json>.
            self._process_request(line[2:])
        # C: (config), P: (ping), and anything else — ignored.

    def _evict_one(self):
        if not self._cache:
            return
        lru_key, entry = self._cache.popitem(last=False)
        if entry:
            for table in entry.tables:
                if table in self._table_index:
                    self._table_index[table].discard(lru_key)
                    if not self._table_index[table]:
                        del self._table_index[table]
        self.stats_evictions += 1

    # ---- native cache telemetry: sliding windows ----

    def _record_eviction_locked(self, evicted):
        """Record a put() outcome (1 evicted, 0 inserted). Caller holds `_lock`.

        Bounded ring — once at capacity, overwrites oldest in O(1).
        """
        if len(self._recent_evictions) < _EVICT_RATE_WINDOW:
            self._recent_evictions.append(evicted)
        else:
            self._recent_evictions[self._recent_evictions_idx] = evicted
            self._recent_evictions_idx = (self._recent_evictions_idx + 1) % _EVICT_RATE_WINDOW

    # ---- native cache telemetry: snapshot + state-change emission ----

    def _build_snapshot(self):
        """Build the native-cache snapshot dict the proxy aggregates per-tick.

        All counters + cache size read in a single critical section so
        the snapshot is internally consistent (no torn reads where, e.g.,
        hits and misses straddle a concurrent get()). The proxy computes
        deltas across ticks; we just expose the raw counters.
        """
        with self._lock:
            snap = {
                "wrapper_id": self._wrapper_id,
                "lang": self._wrapper_lang,
                "version": self._wrapper_version,
                "hits": self.stats_hits,
                "misses": self.stats_misses,
                "evictions": self.stats_evictions,
                "invalidations": self.stats_invalidations,
                "current_size_entries": len(self._cache),
                "capacity_entries": self._max_entries,
            }
            # `disabled` is a forward-compat field for the dashboard.
            # Always emit so consumers can rely on its presence — Manor
            # display is free to ignore it today. Nested under
            # native_cache.wrappers[] on the wire, so context disambiguates.
            snap["disabled"] = self._disabled
            return snap

    def _send_line(self, line):
        """Serialize a line write under `_send_lock`. Best-effort —
        socket errors are swallowed (the recv loop will detect the
        broken connection on its next iteration and reconnect)."""
        if not self._report_stats:
            return
        sock = self._socket
        if sock is None:
            return
        data = line.encode("utf-8") if isinstance(line, str) else line
        if not data.endswith(b"\n"):
            data = data + b"\n"
        with self._send_lock:
            try:
                sock.sendall(data)
            except (OSError, ConnectionError):
                # Connection dead — recv loop will rebuild on next
                # iteration. Don't try to repair here; we'd race the
                # reconnect logic.
                pass

    def _emit_state_change(self, state):
        """Emit S:<json> with snapshot + state name."""
        if not self._report_stats:
            return
        payload = self._build_snapshot()
        payload["state"] = state
        payload["ts_ms"] = int(time.time() * 1000)
        try:
            line = "S:" + json.dumps(payload, separators=(",", ":"))
        except (TypeError, ValueError):
            return
        self._send_line(line)

    def _emit_response(self, snapshot=None):
        """Emit R:<json> snapshot reply to a ?:<request>."""
        if not self._report_stats:
            return
        if snapshot is None:
            snapshot = self._build_snapshot()
        snapshot.setdefault("ts_ms", int(time.time() * 1000))
        try:
            line = "R:" + json.dumps(snapshot, separators=(",", ":"))
        except (TypeError, ValueError):
            return
        self._send_line(line)

    def _maybe_emit_eviction_rate_state_change(self):
        """Check the eviction-rate sliding window and emit a state
        change if the latched state should flip. Hysteresis-guarded:
        crossing HIGH emits cache_full, falling back below LOW emits
        cache_recovered, and rates between LOW and HIGH leave the
        latched state unchanged (no flapping)."""
        # Read window state + flip latched flag under `_lock` so two
        # concurrent puts that both cross the threshold can't both emit.
        # Need at least a full window before reporting state — a single
        # eviction in 3 puts is noise.
        emit = None
        with self._lock:
            n = len(self._recent_evictions)
            if n < _EVICT_RATE_WINDOW:
                return
            rate = sum(self._recent_evictions) / n
            if not self._state_cache_full and rate >= _EVICT_RATE_HIGH:
                self._state_cache_full = True
                emit = "cache_full"
            elif self._state_cache_full and rate <= _EVICT_RATE_LOW:
                self._state_cache_full = False
                emit = "cache_recovered"
        # Emit outside the lock — `_emit_state_change` takes `_send_lock`
        # and may block on a socket write; we don't want to nest locks
        # or hold `_lock` across I/O.
        if emit is not None:
            self._emit_state_change(emit)

    def _process_request(self, raw):
        """Handle ?:<request> from the proxy. Today the only request
        is `snapshot` — the proxy asks for a current counter snapshot
        and we reply with R:<json>. Future requests can extend this
        without breaking older proxies (they'd ignore unknown R:
        lines, but only the proxy that sent ?:<x> will be expecting a
        reply, so the contract is local to the request type)."""
        # `raw` is the body after the `?:` prefix; today we accept any
        # non-empty value as "snapshot" — the proxy doesn't differentiate
        # request types yet.
        body = raw.strip() if raw else ""
        if not body or body == "snapshot":
            self._emit_response()

    def emit_wrapper_disconnected(self):
        """Emit a final `wrapper_disconnected` snapshot before shutdown.
        Called from atexit (registered by the wrapper layer) — best
        effort; the socket may already be torn down."""
        self._emit_state_change("wrapper_disconnected")

    @classmethod
    def _reset(cls):
        with cls._instance_lock:
            if cls._instance and cls._instance._invalidation_thread:
                cls._instance.stop_invalidation()
            cls._instance = None
