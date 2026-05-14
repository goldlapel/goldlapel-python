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

# Per-segment classifiers used by `update_tx_state`. A multi-statement Q
# message like `BEGIN; INSERT INTO t VALUES (1); COMMIT` flips wrapper-side
# `_in_transaction` based on the first token only (`BEGIN`) — but the
# COMMIT at the end means the server ends out-of-tx. Walking segments
# converges the wrapper's view to the server's actual end-state. Order
# matters: a segment whose first keyword is in `_TX_END_KEYWORDS` ends the
# transaction; one in `_TX_START_KEYWORDS` opens it; anything else leaves
# state unchanged.
#
# SAVEPOINT and RELEASE are intentionally NOT classified as boundary
# keywords. `SAVEPOINT` errors outside a transaction (so the wrapper's
# `_in_transaction` is already True when it appears — a flip would be a
# no-op). `RELEASE SAVEPOINT` does NOT end the outer transaction; it
# just commits a nested savepoint, and the outer tx continues. Treating
# RELEASE as `_in_transaction = False` would let subsequent in-tx reads
# route through the cache while the server is still in-tx, producing
# stale reads / read-your-own-writes violations.
_TX_START_KEYWORDS = frozenset({"BEGIN", "START"})
_TX_END_KEYWORDS = frozenset({"COMMIT", "ROLLBACK", "END"})

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
    # Output-format / locale GUCs — these do NOT change which rows the
    # server returns, but they DO change how those rows are textually
    # rendered on the wire (DateStyle, TimeZone, IntervalStyle, bytea_output,
    # lc_*). Two connections with the same SQL but different DateStyle would
    # otherwise share a cache slot and the second connection would observe
    # the first connection's rendering — a correctness gap, even if not an
    # RLS leak. Cheap to fold into the state hash; covers a real footgun.
    "datestyle",
    "intervalstyle",
    "timezone",
    "bytea_output",
    "lc_messages",
    "lc_monetary",
    "lc_numeric",
    "lc_time",
})


def is_unsafe_guc(name):
    """Classify a GUC name as state-affecting (True) or harmless (False).

    Case-insensitive. A GUC is unsafe if it's in the short hardcoded list
    OR contains a `.` (namespaced — `app.*`, `myapp.*`, etc.). The
    hardcoded list covers two classes of GUC:

    - Identity / authorization / RLS-relevant: `search_path`, `role`,
      `session_authorization`, `default_transaction_*`,
      `transaction_isolation`, `row_security`. Affect WHICH rows
      the server returns.
    - Output-format / locale: `DateStyle`, `IntervalStyle`, `TimeZone`,
      `bytea_output`, `lc_messages`, `lc_monetary`, `lc_numeric`,
      `lc_time`. Affect HOW returned rows are textually rendered. Two
      connections sharing a cache slot under different DateStyle would
      otherwise observe each other's rendering — a correctness gap.
    """
    lower = name.lower()
    if "." in lower:
        return True
    return lower in _UNSAFE_GUC_SHORT_LIST


def _strip_string_literals(sql):
    """Replace the contents of `'...'` and `"..."` string literals with
    spaces, preserving overall length so positions line up with the
    original. PG's doubled-quote `''` / `""` escapes are handled the same
    way as in `split_statements`. Used by `_detect_write`'s SELECT branch
    so that bare words like `INTO` inside a literal (e.g.
    `SELECT 'INSERT INTO orders' FROM audit_log`) don't trip the
    SELECT-INTO DDL classifier.
    """
    if not sql:
        return sql
    out = list(sql)
    quote = None
    i = 0
    n = len(sql)
    while i < n:
        c = sql[i]
        if quote is not None:
            if c == quote:
                if i + 1 < n and sql[i + 1] == quote:
                    # Doubled-quote escape: blank both, stay inside literal.
                    out[i] = " "
                    out[i + 1] = " "
                    i += 2
                    continue
                # Closing quote: leave the delimiter, drop the literal body.
                quote = None
            else:
                out[i] = " "
        else:
            if c == "'" or c == '"':
                quote = c
        i += 1
    return "".join(out)


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


# Marker for `SetCommand` kinds. Returned as a 3-tuple `(kind, name, value)`
# from `parse_set_command` so callers can pattern-match without paying for a
# class allocation per call. Kinds:
#   "set"          — SET name = value (and SET SESSION ... variant) AND
#                    SELECT [pg_catalog.]set_config(name, value, false) — the
#                    function form is treated identically to a SET because
#                    PG's docs explicitly document them as equivalent (the
#                    Supabase / PostgREST canonical RLS-context shape:
#                    `SELECT set_config('app.user_id', '42', false)`).
#   "set_local"    — SET LOCAL name = value AND set_config(..., true) —
#                    ignored for state hash.
#   "reset"        — RESET name
#   "reset_all"    — RESET ALL  AND  DISCARD ALL
#   "discard_other"— DISCARD PLANS / SEQUENCES / TEMP / TEMPORARY. Recorded as
#                    a parsed command (so callers know a DISCARD was observed
#                    and can clear the `dirty` flag on the state) but does
#                    NOT mutate the state-hash map — these subcommands don't
#                    touch GUCs.
SET_KIND_SET = "set"
SET_KIND_SET_LOCAL = "set_local"
SET_KIND_RESET = "reset"
SET_KIND_RESET_ALL = "reset_all"
SET_KIND_DISCARD_OTHER = "discard_other"


# `DISCARD <subcommand>` recognized values. PG accepts ALL / PLANS /
# SEQUENCES / TEMP / TEMPORARY (the last two are aliases). Anything else is
# a syntax error server-side; the parser returns None for unknown forms.
_DISCARD_SUBCOMMANDS_OTHER = frozenset({"PLANS", "SEQUENCES", "TEMP", "TEMPORARY"})


def _normalize_guc_name(token):
    """Lowercase the GUC name and strip surrounding double quotes (PG
    treats `"app.user_id"` and `app.user_id` as the same identifier
    when it's a configuration parameter — we discard case anyway)."""
    trimmed = token.strip('"')
    if not trimmed:
        return None
    return trimmed.lower()


# `SELECT set_config('name', 'value', is_local)` parser. Tightly scoped:
# only the canonical 3-arg shape with literal arguments. Anything weirder
# (param placeholders, computed expressions) returns None and falls through
# to the post-call verify path — the connection gets marked dirty there
# instead, which is cheaper than trying to parse arbitrary expressions on
# the hot path.
_SET_CONFIG_RE = re.compile(
    r"""
    ^\s*SELECT\s+
    (?:pg_catalog\s*\.\s*)?           # optional pg_catalog. qualifier
    set_config\s*\(\s*
    ('(?:[^']|'')*'|"(?:[^"]|"")*")   # arg 1: GUC name (single or double quoted literal)
    \s*,\s*
    ('(?:[^']|'')*'|"(?:[^"]|"")*")   # arg 2: value literal (same shape)
    \s*,\s*
    (true|false|t|f|on|off|0|1|'(?:true|false|t|f|on|off|0|1|yes|no|y|n)')  # arg 3: is_local boolean
    \s*\)\s*;?\s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _parse_quoted_literal(tok):
    """Peel a single layer of `'...'` / `"..."` quotes from a literal,
    handling PG's doubled-quote escape (`''` → `'`, `""` → `"`). Caller
    has already validated the literal shape via `_SET_CONFIG_RE`."""
    if len(tok) < 2:
        return tok
    q = tok[0]
    if q != "'" and q != '"':
        return tok
    body = tok[1:-1]
    return body.replace(q + q, q)


def _coerce_bool_literal(tok):
    """Map a SET_CONFIG-style boolean literal (matched by `_SET_CONFIG_RE`)
    to True / False. Accepts both bare and quoted forms."""
    raw = tok.strip().lower()
    if raw.startswith("'") and raw.endswith("'"):
        raw = raw[1:-1]
    return raw in ("true", "t", "on", "1", "yes", "y")


def _parse_set_config_call(sql):
    """Parse `SELECT [pg_catalog.]set_config('name', 'value', is_local)`.

    Returns `(SET_KIND_SET, name, value)` for `is_local=false`,
    `(SET_KIND_SET_LOCAL, name, value)` for `is_local=true`, or `None` if
    the SQL doesn't match the canonical 3-literal-arg shape.

    The Supabase / PostgREST canonical RLS-context shape lands here:
        SELECT set_config('app.user_id', '42', false)
    Their query also typically uses positional params (`$1`, `$2`, `$3`),
    which we deliberately don't try to parse — when the args aren't
    literals the connection is marked dirty by the
    `is_top_level_function_call` heuristic instead and a post-call verify
    fires to reconstruct state. That path is correct, just slightly more
    expensive (one extra round-trip per SET).
    """
    m = _SET_CONFIG_RE.match(sql)
    if m is None:
        return None
    name_lit, value_lit, bool_lit = m.group(1), m.group(2), m.group(3)
    name = _normalize_guc_name(_parse_quoted_literal(name_lit))
    if name is None:
        return None
    value = _parse_quoted_literal(value_lit)
    is_local = _coerce_bool_literal(bool_lit)
    if is_local:
        return (SET_KIND_SET_LOCAL, name, value)
    return (SET_KIND_SET, name, value)


# `SELECT <ident>(...)` at statement-level (not embedded in a SELECT-list /
# WHERE / etc.). The whole statement is a single function call. Used to
# trigger the post-call verify path — function bodies might do server-side
# `SET` we couldn't observe on the wire, so we re-read pg_settings after
# the call returns.
#
# Captures `<ident>` as group 1 — including an optional schema-qualifier
# (`pg_catalog.set_config`, `myschema.refresh_state`). The post-call
# verify ignores `set_config` itself (already parsed inline) and a small
# whitelist of well-known stateless builtins (`now()`, `version()`, ...) —
# see `_VERIFY_SAFE_BUILTINS`. Anything user-defined or unrecognised
# triggers the verify.
_TOP_LEVEL_FUNCTION_RE = re.compile(
    r"""
    ^\s*SELECT\s+
    (?:pg_catalog\s*\.\s*)?           # optional pg_catalog.
    ([a-zA-Z_][a-zA-Z0-9_]*           # identifier
        (?:\s*\.\s*[a-zA-Z_][a-zA-Z0-9_]*)?  # optional schema-qualified second part
    )
    \s*\(                              # opening paren
    """,
    re.IGNORECASE | re.VERBOSE,
)


# Builtins that don't read or mutate session state — safe to skip the
# post-call verify. Conservative list: anything that touches `current_setting`
# / `set_config` / triggers / SECURITY DEFINER procedures is excluded.
_VERIFY_SAFE_BUILTINS = frozenset({
    "now", "current_timestamp", "current_date", "current_time",
    "version", "current_user", "session_user", "user", "current_database",
    "pg_backend_pid", "txid_current", "pg_current_xact_id",
    "set_config",  # the inline parser already captured the mutation
})


def is_top_level_function_call(sql):
    """Return True iff `sql` is a top-level `SELECT <ident>(...)` whose
    function body could plausibly mutate session state.

    Used to trigger the async post-call verify path (item #6 in the RLS
    hardening spec). The match is intentionally conservative — it scans
    only the leading text and gives False for any SQL that's clearly
    something else (a SELECT-list, a SELECT FROM, etc.).

    The returned function name is matched against `_VERIFY_SAFE_BUILTINS`;
    well-known stateless builtins (`now()`, `version()`, ...) skip the
    verify because they can't possibly mutate state. This avoids a
    pg_settings round-trip after every `SELECT now()`. Anything
    user-defined or unrecognised triggers the verify — the cost is a
    handful of microseconds against the ~1ms verify and is fully
    backgrounded relative to the user's hot path.
    """
    if not sql:
        return False
    s = sql.lstrip()
    if not s:
        return False
    m = _TOP_LEVEL_FUNCTION_RE.match(s)
    if m is None:
        return False
    # Reject a SELECT that has a FROM clause AFTER the function call —
    # `SELECT my_func() FROM tbl` is a SELECT-list call, not a top-level
    # function. We check this by walking the raw text after the matched
    # opening `(` for a balanced closing `)` and then looking for FROM.
    func_name = m.group(1)
    # Strip schema qualifier for the safe-builtins check.
    bare = func_name.split(".")[-1].strip().lower()
    open_paren = m.end() - 1  # the `(` is the last char of the match
    rest_after_call = _scan_past_balanced_paren(s, open_paren)
    if rest_after_call is None:
        # Unbalanced parens — not a clean top-level call.
        return False
    tail = rest_after_call.strip()
    if tail.startswith(";"):
        tail = tail[1:].strip()
    # Anything after the function call other than EOF / `;` indicates the
    # call is embedded in a larger statement (e.g. `SELECT f() FROM t`).
    if tail:
        return False
    if bare in _VERIFY_SAFE_BUILTINS:
        return False
    return True


def _scan_past_balanced_paren(s, open_idx):
    """Given `s` and `s[open_idx] == '('`, return the substring of `s`
    after the matching balanced `)`. Returns None if the parens are
    unbalanced. String literals are respected (PG `''` / `""` doubled-
    escapes also handled).
    """
    if open_idx >= len(s) or s[open_idx] != "(":
        return None
    depth = 1
    i = open_idx + 1
    n = len(s)
    quote = None
    while i < n:
        c = s[i]
        if quote is not None:
            if c == quote:
                if i + 1 < n and s[i + 1] == quote:
                    i += 2
                    continue
                quote = None
        else:
            if c == "'" or c == '"':
                quote = c
            elif c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                if depth == 0:
                    return s[i + 1:]
        i += 1
    return None


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
    """Parse a `SET` / `RESET` / `DISCARD` / `SELECT set_config(...)` command
    out of a single SQL statement.

    Recognises:
    - `SET name = value`, `SET name TO value`
    - `SET SESSION name = value`, `SET SESSION name TO value`
    - `SET LOCAL name = value`, `SET LOCAL name TO value`
    - `RESET name`
    - `RESET ALL`
    - `DISCARD ALL` — equivalent to `RESET ALL` for state-hash purposes
      (and additionally clears the `dirty` flag — see ConnectionGucState)
    - `DISCARD PLANS` / `DISCARD SEQUENCES` / `DISCARD TEMP` / `DISCARD
      TEMPORARY` — returns `("discard_other", None, None)`. These don't
      change GUC state but observing them lets the wrapper clear the
      `dirty` flag if it was set by a prior unsafe SET.
    - `SELECT set_config('name', 'value', is_local)` and
      `SELECT pg_catalog.set_config(...)` — the canonical Supabase /
      PostgREST RLS-context shape, equivalent to a regular SET (or SET
      LOCAL when is_local=true). Per PG docs, set_config(...) is the
      function-form equivalent of SET / SET LOCAL.

    Returns `(kind, name, value)` where `kind` is one of `SET_KIND_*`.
    Returns `None` for anything else (including `SET TIME ZONE 'UTC'` —
    the legacy two-word form. Timezone is tracked via the one-word
    `timezone` GUC; the two-word grammar doesn't fit our parser.
    Returning None here is safe because the connection is marked dirty
    via the verify-on-checkout path if the unusual form ever lands).

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
    head_lower = head.lower()

    # DISCARD branch — `DISCARD ALL` clears the entire session state and is
    # treated identically to `RESET ALL` for the state-hash. The other
    # subcommands (PLANS / SEQUENCES / TEMP / TEMPORARY) don't touch GUCs
    # but ARE relevant for the verify-on-checkout `dirty` flag — observing
    # them tells us the connection has been recycled by its pool, so any
    # uncertainty from prior unsafe SETs is moot.
    if head_lower == "discard":
        if len(tokens) != 2:
            return None
        sub = tokens[1].rstrip(";").upper()
        if sub == "ALL":
            return (SET_KIND_RESET_ALL, None, None)
        if sub in _DISCARD_SUBCOMMANDS_OTHER:
            return (SET_KIND_DISCARD_OTHER, None, None)
        return None

    # SELECT set_config(...) / SELECT pg_catalog.set_config(...) — function
    # form of SET. Delegated to a dedicated parser so the SET path stays
    # tightly scoped to keyword-prefixed grammars.
    if head_lower == "select":
        return _parse_set_config_call(s)

    # RESET branch.
    if head_lower == "reset":
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

    if head_lower != "set":
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


# --- Pending-mutation events (Wave 2 SET-actually-applied) ---
#
# Background. Earlier, the wrapper applied SET / RESET / DISCARD / set_config
# mutations to per-connection GUC state OPTIMISTICALLY — at the moment the
# SQL was observed on its way to the server. If the server then errored
# (e.g. `SET role = 'badrole'` failed because the role doesn't exist), the
# wrapper-side state diverged from server reality and the next L1 cache
# lookup keyed under a state hash that didn't match the server.
#
# Fix. Defer state mutation until after the dispatch returns. The hot path
# parses pending mutations from `prepare_pending(sql)`, dispatches the user's
# query, then calls `apply_pending(state, batch, success=...)` based on
# whether the dispatch raised.
#
# Multi-statement intent. The events list preserves segment order plus
# tx-boundary markers. The apply-step honors sub-tx semantics:
#   - `BEGIN; SET app.x='1'; ROLLBACK` succeeds end-to-end, but the SET
#     was rolled back server-side. apply_pending discards the SET.
#   - `BEGIN; SET app.x='1'; COMMIT` succeeds and commits the SET. Apply.
#   - `SET app.x='1'; SELECT bad_query` errors. Under simple-Q
#     statement-at-a-time semantics, the leading SETs landed before the
#     erroring statement; apply them. (A pure-SET-only body that errors
#     means the SET is what failed; discard.)
#
# Tests live in test_cache.py (TestPendingBatch / TestApplyPending) and
# test_wrap.py (TestSetActuallyApplied{Sync,Async}).

_EVENT_SET = "set"
_EVENT_TX_START = "tx_start"
_EVENT_TX_COMMIT = "tx_commit"
_EVENT_TX_ROLLBACK = "tx_rollback"
_EVENT_OTHER = "other"


class _PendingBatch:
    """A parsed multi-segment SQL body's mutation events, ready to be
    applied or discarded once the server's response is known.

    `events` is an ordered list of one of:
        ("set", (kind, name, value))   — a parsed SET/RESET/DISCARD command
        ("tx_start", None)             — BEGIN / START
        ("tx_commit", None)            — COMMIT / END
        ("tx_rollback", None)          — ROLLBACK
        ("other", None)                — any other segment

    Treated as immutable by callers — `apply_pending` reads but does not
    mutate. The `__slots__` keeps the per-call allocation tight.
    """
    __slots__ = ("events",)

    def __init__(self, events):
        self.events = events

    @property
    def is_empty(self):
        return not self.events

    @property
    def has_set_segments(self):
        for ev in self.events:
            if ev[0] == _EVENT_SET:
                return True
        return False


# Empty-batch sentinel — returned from `prepare_pending` when there's
# nothing state-relevant in the body. Avoids per-call allocation for the
# overwhelmingly common case of a plain SELECT / INSERT / etc.
_EMPTY_BATCH = _PendingBatch(())


def _classify_segment(segment):
    """Classify a single segment as one of the pending-batch event kinds.
    Returns one of `(_EVENT_SET, parsed_cmd)`, `(_EVENT_TX_START, None)`,
    `(_EVENT_TX_COMMIT, None)`, `(_EVENT_TX_ROLLBACK, None)`, or
    `(_EVENT_OTHER, None)`.

    SET-like segments take precedence — `parse_set_command` recognises
    SET / RESET / RESET ALL / DISCARD ALL / DISCARD <other> /
    SELECT [pg_catalog.]set_config(...). Anything it doesn't recognise
    falls through to the tx-marker first-keyword classifier.
    """
    cmd = parse_set_command(segment)
    if cmd is not None:
        return (_EVENT_SET, cmd)
    s = segment.lstrip()
    if not s:
        return (_EVENT_OTHER, None)
    end = 0
    n = len(s)
    while end < n and not s[end].isspace() and s[end] != ";":
        end += 1
    head = s[:end].upper()
    if head in _TX_START_KEYWORDS:
        return (_EVENT_TX_START, None)
    if head == "ROLLBACK":
        return (_EVENT_TX_ROLLBACK, None)
    if head == "COMMIT" or head == "END":
        return (_EVENT_TX_COMMIT, None)
    return (_EVENT_OTHER, None)


def prepare_pending(sql):
    """Parse `sql` into a `_PendingBatch` of events to be applied or
    discarded once the dispatch's success/failure is known.

    Single-statement bodies skip the splitter allocation entirely — the
    wrapper's hot path is `cur.execute("SELECT ...")`-style single
    statements, and we don't want to charge those a list allocation per
    call. Returns `_EMPTY_BATCH` when there's nothing state-relevant.
    """
    if not sql:
        return _EMPTY_BATCH
    trimmed = sql.rstrip()
    if trimmed.endswith(";"):
        trimmed_no_semi = trimmed[:-1]
    else:
        trimmed_no_semi = trimmed
    if ";" not in trimmed_no_semi:
        ev = _classify_segment(sql)
        if ev[0] == _EVENT_OTHER:
            return _EMPTY_BATCH
        return _PendingBatch((ev,))
    events = []
    for seg in split_statements(sql):
        events.append(_classify_segment(seg))
    if not events or all(e[0] == _EVENT_OTHER for e in events):
        return _EMPTY_BATCH
    return _PendingBatch(tuple(events))


def apply_pending(state, batch, success):
    """Commit or discard a `_PendingBatch` against `state`.

    On success: walk events in order; SETs inside a sub-tx that ends with
    ROLLBACK are dropped (server-side rolled them back); SETs inside a
    sub-tx that ends with COMMIT, or outside any sub-tx, are applied.

    On error: under the simple-Q multi-statement model, the server has
    already executed earlier segments and a later segment errored. We
    apply the SET segments that precede the FIRST non-SET, non-tx-marker
    segment — those are assumed to have landed before the trailing
    statement raised. A body that's pure SETs and errors means one of
    the SETs themselves failed; we discard the entire batch (we can't
    tell which SET failed, and applying any of them risks divergence).

    Returns True if `state` mutated; False otherwise.
    """
    if batch.is_empty:
        return False

    before_hash = state.hash

    if not success:
        # Locate the first non-SET segment. Tx markers count as non-SET
        # for this purpose — `BEGIN; SET; SELECT bad` would hit the
        # tx_start as the first non-SET, and we'd apply nothing (which
        # is correct: BEGIN opened a tx, and the failing SELECT aborted
        # it, rolling back the SET).
        idx_first_non_set = None
        for i, ev in enumerate(batch.events):
            if ev[0] != _EVENT_SET:
                idx_first_non_set = i
                break
        if idx_first_non_set is None:
            # Pure SETs body — failure means a SET errored. Discard all.
            return False
        for ev in batch.events[:idx_first_non_set]:
            state.apply(ev[1])
        return state.hash != before_hash

    # Success path: walk events with sub-tx awareness.
    sub_tx_buffer = []
    in_sub_tx = False
    for ev in batch.events:
        kind = ev[0]
        if kind == _EVENT_SET:
            if in_sub_tx:
                sub_tx_buffer.append(ev[1])
            else:
                state.apply(ev[1])
        elif kind == _EVENT_TX_START:
            # Nested BEGIN is a server-side error in real life; defensive
            # behaviour is to leave any prior buffer intact. We don't
            # observe the segment that would carry the error, so just
            # mark in-sub-tx and continue.
            in_sub_tx = True
        elif kind == _EVENT_TX_COMMIT:
            for cmd in sub_tx_buffer:
                state.apply(cmd)
            sub_tx_buffer = []
            in_sub_tx = False
        elif kind == _EVENT_TX_ROLLBACK:
            sub_tx_buffer = []
            in_sub_tx = False
        # _EVENT_OTHER — no state effect

    # Body ended still inside a sub-tx (e.g. `BEGIN; SET app.x='1'`):
    # the tx is open server-side. Apply now so the in-tx cache-bypass
    # path keys correctly under the post-SET hash. Cross-call ROLLBACK
    # handling is a future improvement (would require connection-level
    # pending-buffer that survives between calls); the existing wrapper
    # already had this gap and we leave it intact for now.
    if sub_tx_buffer:
        for cmd in sub_tx_buffer:
            state.apply(cmd)

    return state.hash != before_hash


class ConnectionGucState:
    """Per-connection unsafe-GUC state tracker.

    Stores values for unsafe GUCs only (harmless GUCs — application_name,
    planner cost knobs, etc. — never enter the map and never affect the
    hash). The state hash is recomputed on every mutation and folded into
    L1 cache keys.

    Hash is `0` for the empty (default) state, so a fresh connection's
    state hash matches "no GUCs set" cache slots from peer connections —
    which is the correct, secure default (any connection that has set an
    unsafe GUC gets a non-zero hash).

    The `dirty` flag is the verify-on-checkout fallback (item #5 in the
    RLS hardening spec). It's set when an unsafe SET is observed and
    cleared when a DISCARD is observed (or when verify completes). Pool
    integrations that don't issue DISCARD on release can call
    `maybe_verify(conn)` / `await maybe_verify_async(conn)` on checkout
    to reconcile state by reading `pg_settings`. The default behaviour
    is no-op — the proxy + L1 still get the state hash via observe_sql,
    and verify only runs when explicitly invoked.
    """

    __slots__ = ("_values", "_hash", "_dirty", "_discards_observed", "_dml_seq")

    def __init__(self):
        self._values = {}  # lowercased GUC name → raw value string
        self._hash = 0
        # Set True the first time an unsafe SET is observed on this
        # connection. Cleared when (a) a DISCARD is observed, OR (b)
        # `maybe_verify(...)` completes successfully. The flag exists
        # so a custom-pool checkout can issue ONE pg_settings round-trip
        # only when there's actual uncertainty about state.
        self._dirty = False
        # Counter incremented on every observed DISCARD (including
        # DISCARD ALL). Used to detect "no DISCARD observed since the
        # connection became dirty" — `maybe_verify` reads the counter at
        # entry and skips work if it's nonzero (a DISCARD already
        # reconciled state for us).
        self._discards_observed = 0
        # Per-connection DML sequence counter. Bumped via `bump_dml_seq`
        # on every observed write while aggressive-verify is on. Folded
        # into the state hash so the L1 cache key naturally changes
        # post-write — the next read on this connection bypasses the
        # stale slot and goes to the proxy, which makes its own decision
        # about whether to serve the cached value. Replaces the older
        # "detect triggers, then run a pg_settings verify after DML"
        # approach: cheaper (no extra round-trip), simpler (no
        # detection), and strictly safer (every DML invalidates the
        # connection's L1 view, not just the ones we detected as risky).
        self._dml_seq = 0

    @property
    def hash(self):
        """Current state hash. `0` for empty state."""
        return self._hash

    @property
    def dirty(self):
        """True iff an unsafe SET has been observed and not yet
        reconciled by a DISCARD or a successful verify. Pool integrations
        with non-DISCARD reset strategies can read this to decide whether
        to invoke `maybe_verify` on checkout."""
        return self._dirty

    @property
    def discards_observed(self):
        """Monotonic counter of DISCARD commands observed (any subcommand,
        including DISCARD ALL). Read by `maybe_verify` to decide whether
        the dirty-flag suspicion is already reconciled."""
        return self._discards_observed

    @property
    def dml_seq(self):
        """Per-connection DML sequence counter — bumped via
        `bump_dml_seq` on every observed write. Folded into the state
        hash so post-write reads naturally key under a fresh slot and
        bypass any pre-write cached row on this connection."""
        return self._dml_seq

    def is_dirty(self):
        """Method form of the `dirty` property. The wrap.py hot path
        calls this to gate L1 cache lookups: when dirty, the cache is
        bypassed entirely and the query goes to the proxy, which makes
        its own decision about whether the cached value is still good.
        Mirrors the property — both return the same bool."""
        return self._dirty

    def bump_dml_seq(self):
        """Increment the per-connection DML sequence counter and
        recompute the state hash. Called by the wrap.py hot path on
        every observed write while aggressive-verify is on (default).
        Cheap — one int bump + one tuple hash.

        Replaces the older "mark_dirty + post-DML verify query" pattern:
        a hash bump is enough — the cache key changes, the L1 slot is
        no longer addressable from this connection, and the proxy
        decides on the next round-trip whether the row is still valid.
        No verify round-trip required."""
        self._dml_seq += 1
        self._recompute_hash()

    def mark_dirty(self):
        """Force-set the dirty flag. Used by the post-call verify path —
        when we see a `SELECT <user_func>(...)` we don't know whether
        the function body did a server-side SET, so we mark dirty so the
        next checkout reconciles state. The async post-call verify
        (`schedule_post_call_verify`) is preferred when an event loop
        is available; this is the synchronous fallback."""
        self._dirty = True

    def apply(self, cmd):
        """Apply a parsed (kind, name, value) tuple from
        `parse_set_command`. No-op for SetLocal (cache is bypassed in
        transactions anyway), no-op for safe GUC names. Returns True
        if the hash mutated."""
        kind, name, value = cmd
        if kind == SET_KIND_SET:
            if is_unsafe_guc(name):
                # Mark dirty regardless of whether the value changed —
                # a re-SET to the same value still lands on the wire and
                # is a "we saw an unsafe SET" signal. The dirty flag is
                # cheap and the next DISCARD / verify clears it.
                self._dirty = True
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
            # DISCARD ALL routes here too — clear EVERYTHING (state map +
            # dirty flag + dml_seq). The connection has been fully reset
            # by the pool / by the user; no remaining uncertainty.
            self._discards_observed += 1
            self._dirty = False
            mutated = False
            if self._values:
                self._values.clear()
                mutated = True
            if self._dml_seq != 0:
                self._dml_seq = 0
                mutated = True
            if mutated:
                self._recompute_hash()
                return True
            return False
        if kind == SET_KIND_DISCARD_OTHER:
            # DISCARD PLANS / SEQUENCES / TEMP / TEMPORARY — doesn't touch
            # GUCs, so the state map is unchanged. But it's a DISCARD,
            # which is a strong signal the pool reset the connection;
            # clear the dirty flag and bump the counter so verify-on-
            # checkout knows the suspicion has been reconciled.
            self._discards_observed += 1
            self._dirty = False
            return False
        return False

    def observe_sql(self, sql):
        """Observe a SQL string for SET / RESET / DISCARD / set_config()
        commands and update state accordingly. Multi-statement bodies are
        split on top-level `;` (string literals respected) so a single Q
        like `SET app.user_id = '42'; SELECT 1` still updates state.
        Returns True if the hash mutated.

        Optimistic application — used by tests and any caller that
        doesn't have a request/response boundary to defer against. The
        wrap.py hot path uses `prepare_pending` + `apply_pending` instead
        so a server-side error on the SET doesn't leave wrapper state
        diverged from server reality (Wave 2 SET-actually-applied fix).
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
        # Fresh state (no SETs observed AND no DML observed) hashes to 0
        # — that's the shared "default-context" cache slot two clean
        # connections can correctly share. Once anything mutates state
        # (a SET, or a DML bump), we fold both the values map and the
        # DML counter into the hash so peer connections with different
        # values OR different DML history never collide.
        if not self._values and self._dml_seq == 0:
            self._hash = 0
            return
        # Sorted iteration → deterministic hash regardless of insertion
        # order. Python's `hash()` is process-randomised, but the state
        # hash never crosses process boundaries (it's only ever used as
        # part of a local L1 cache key), so that's fine.
        self._hash = hash((tuple(sorted(self._values.items())), self._dml_seq))

    # ---- verify-on-checkout: rebuild state map from pg_settings ----

    def _ingest_pg_settings_rows(self, rows):
        """Replace the unsafe-GUC state map from a list of `(name, value)`
        rows produced by `SELECT name, setting FROM pg_settings WHERE
        source='session'`. Rows whose `name` is safe-by-classifier are
        ignored. Updates the hash, clears `dirty`, but does NOT bump
        `_discards_observed` — verify is a wrapper-side reconciliation,
        not a wire-observed DISCARD.
        """
        new_values = {}
        for row in rows:
            # Tolerate both 2-tuple and Mapping-like rows (asyncpg.Record
            # supports __getitem__ by both index and column name; psycopg
            # cursors yield tuples). We only care about the first two
            # positional fields.
            try:
                name = row[0]
                value = row[1]
            except (KeyError, IndexError, TypeError):
                continue
            if name is None:
                continue
            normalized = _normalize_guc_name(str(name))
            if normalized is None:
                continue
            if not is_unsafe_guc(normalized):
                continue
            new_values[normalized] = "" if value is None else str(value)
        self._values = new_values
        self._dirty = False
        self._recompute_hash()

    def maybe_verify(self, real_conn):
        """Sync verify-on-checkout. Reads pg_settings on `real_conn` and
        rebuilds the state map. No-op if `dirty` is False or if at least
        one DISCARD has been observed since the dirty span began.

        Errors are swallowed — the spec is "NEVER fail the user's query
        if verify itself errors". On error we leave `dirty=True` so a
        future verify attempt can retry.

        Returns True if a verify ran and succeeded; False otherwise.
        """
        if not self._dirty:
            return False
        # Discards-observed snapshot at dirty-mark time is implicit: any
        # DISCARD between mark_dirty and now would have cleared the
        # dirty flag in `apply`, so reaching here means there's been
        # no DISCARD since dirty was set.
        try:
            cursor = real_conn.cursor()
            try:
                cursor.execute(_VERIFY_SQL)
                rows = cursor.fetchall()
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass
            self._ingest_pg_settings_rows(rows)
            return True
        except Exception:
            return False

    async def maybe_verify_async(self, real_conn):
        """Async verify-on-checkout. Same shape as `maybe_verify` but
        uses asyncpg's `fetch()` API. No-op if not dirty. Errors swallowed.

        Returns True if a verify ran and succeeded; False otherwise.
        """
        if not self._dirty:
            return False
        try:
            rows = await real_conn.fetch(_VERIFY_SQL)
            # asyncpg.Record exposes positional indexing — convert to a
            # uniform `(name, value)` shape for `_ingest_pg_settings_rows`.
            normalised = [(r[0], r[1]) for r in rows]
            self._ingest_pg_settings_rows(normalised)
            return True
        except Exception:
            return False


# The pg_settings query used by both verify paths. `source = 'session'`
# means "set on this connection by SET / set_config()" and excludes
# server-default / client-startup-time values — that's exactly the slice
# we'd otherwise have observed on the wire.
_VERIFY_SQL = (
    "SELECT name, setting FROM pg_settings WHERE source = 'session'"
)


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


def update_tx_state(in_transaction, sql):
    """Compute the wrapper's transaction state after observing a SQL body.

    Walks each top-level statement in `sql` (string-literal-aware splitter)
    and folds per-segment first-keyword classification:
    - `BEGIN` / `START` → in_transaction = True
    - `COMMIT` / `ROLLBACK` / `END` → in_transaction = False
    - Anything else (including `SAVEPOINT` and `RELEASE`, which are
      intra-transaction markers that don't change the outer tx state) →
      leaves state unchanged.

    Returns `(new_state, had_tx_marker)`:
    - `new_state` — the final tx state (boolean) after walking all segments.
      The walk preserves order, so a body like `BEGIN; INSERT INTO t
      VALUES (1); COMMIT` ends with `False` — matching the server, where
      the COMMIT closes the tx the BEGIN opened. Without this fix, the
      wrapper's tx state diverges from the server's and subsequent reads
      bypass the cache forever (until a fresh BEGIN/COMMIT cycle resets).
    - `had_tx_marker` — True if any segment's first-token was a tx-boundary
      keyword. Lets callers detect bodies like `BEGIN; ROLLBACK` whose
      net state is unchanged but which still shouldn't go through the
      cache path.

    Cheap on the hot path — single-statement bodies skip the splitter
    allocation entirely.
    """
    # Fast path: no `;` → single segment, classify directly.
    trimmed = sql.rstrip()
    if trimmed.endswith(";"):
        trimmed = trimmed[:-1]
    if ";" not in trimmed:
        return _classify_tx_segment(in_transaction, sql)

    state = in_transaction
    had_marker = False
    for seg in split_statements(sql):
        state, seg_marker = _classify_tx_segment(state, seg)
        if seg_marker:
            had_marker = True
    return state, had_marker


def _classify_tx_segment(in_transaction, segment):
    """Classify a single segment's effect on tx state. First-keyword check —
    case-insensitive, no string-literal awareness needed because the
    keyword can only appear at the very start of a statement to be
    syntactically meaningful (and `split_statements` already trimmed
    whitespace).

    Returns `(new_state, is_tx_marker)`.
    """
    s = segment.lstrip()
    if not s:
        return in_transaction, False
    # First whitespace-delimited token, uppercase. Strip a trailing
    # semicolon from a single-token segment like `COMMIT;` (split_statements
    # already drops trailing `;` from segments it produces, but the
    # fast-path single-segment caller may pass `BEGIN;` directly).
    end = 0
    n = len(s)
    while end < n and not s[end].isspace() and s[end] != ";":
        end += 1
    head = s[:end].upper()
    if head in _TX_END_KEYWORDS:
        return False, True
    if head in _TX_START_KEYWORDS:
        return True, True
    return in_transaction, False


def _detect_writes_multi(sql):
    """Detect writes in a (potentially multi-statement) SQL body.

    Returns one of:
    - `_DDL_SENTINEL` — at least one segment is a DDL/CTE-write/SELECT INTO,
      so the caller must invalidate the entire cache.
    - a non-empty `set` of bare table names — every segment is a recognised
      write against a known table; caller invalidates each.
    - `None` — no writes detected; caller proceeds on the read path.

    A single Q message like `SET app.user_id='42'; INSERT INTO orders ...`
    looks like a SET to `_detect_write` (single-token first match) but
    contains a real write. Splitting and unioning per-segment results
    closes that gap.
    """
    # Fast path: no `;` → single statement, skip the splitter allocation.
    trimmed = sql.rstrip()
    if trimmed.endswith(";"):
        trimmed = trimmed[:-1]
    if ";" not in trimmed:
        t = _detect_write(sql)
        if t is None:
            return None
        if t == _DDL_SENTINEL:
            return _DDL_SENTINEL
        return {t}

    tables = set()
    for seg in split_statements(sql):
        t = _detect_write(seg)
        if t == _DDL_SENTINEL:
            return _DDL_SENTINEL
        if t is not None:
            tables.add(t)
    return tables if tables else None


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
        # Re-tokenize from a literal-stripped form so that bare words like
        # `INTO` or `FROM` inside `'...'` / `"..."` don't trigger the
        # SELECT-INTO DDL classifier (e.g. `SELECT 'INSERT INTO orders'
        # FROM audit_log`, `SELECT * FROM "into_table"`). Other detect
        # branches use fixed-position token checks (tokens[1], tokens[2])
        # and aren't affected by literal contents.
        scan_tokens = _strip_string_literals(trimmed).split()
        saw_into = False
        into_target = None
        for tok in scan_tokens[1:]:
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
