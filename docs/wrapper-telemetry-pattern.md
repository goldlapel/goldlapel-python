# Wrapper L1 telemetry pattern

Reference shape for porting L1 telemetry from `goldlapel-python` to the
JS / Ruby / Java / PHP / Go / .NET wrappers. The Python implementation
in `src/goldlapel/cache.py` is canon — when in doubt, mirror what's
there.

## Goals

L1 telemetry exists so the proxy / dashboard can show what's happening
inside each wrapper's local cache without the wrapper having to push
periodic snapshots. Three properties matter:

1. **Demand-driven**: no background timer thread. The proxy asks (`?:`),
   the wrapper replies (`R:`). If the proxy doesn't ask, no work is done.
2. **State-change push**: when something *transitions* (cache fills up,
   wrapper connects, wrapper exits), the wrapper emits an `S:<json>`
   line synchronously from the cache-op path. No buffering, no batching.
3. **Cheap**: counters are plain integers bumped on hot path. The
   sliding-window check that drives `cache_full` is O(1) amortised.

## Wire protocol

Single bidirectional line-delimited TCP/Unix socket (the existing
invalidation socket — telemetry piggybacks). Lines are `<prefix>:<body>\n`:

| Prefix | Direction | Body |
|--------|-----------|------|
| `I:`   | proxy → wrapper | Table to invalidate (`*` = all). Pre-existing. |
| `?:`   | proxy → wrapper | Request type — currently only `snapshot`. |
| `S:`   | wrapper → proxy | State-change event with JSON snapshot + `state` field. |
| `R:`   | wrapper → proxy | Reply to a `?:` request, JSON snapshot. |
| `P:`   | proxy → wrapper | Keepalive/ping. Pre-existing. |

Unknown prefixes MUST be silently ignored on both sides for
forward-compat.

## Snapshot fields

```json
{
  "wrapper_id": "uuid4-string",
  "lang": "python",
  "version": "0.3.0",
  "hits": 12345,
  "misses": 678,
  "evictions": 42,
  "invalidations": 7,
  "current_size_entries": 1234,
  "capacity_entries": 32768,
  "ts_ms": 1714600000000
}
```

`S:` adds `"state": "<event_name>"`. `R:` does not.

## State events emitted

- `wrapper_connected` — emitted on first successful socket connect.
- `wrapper_disconnected` — emitted from the language's at-exit hook on
  graceful shutdown. Best-effort; the socket may already be down.
- `cache_full` — emitted when ≥ 50% of the last 200 puts caused an
  eviction (sliding window). Indicates working set exceeds capacity.
- `cache_recovered` — emitted when the rate falls back below 10%.

Hysteresis (50% / 10%) avoids flapping at the boundary. The latched
state flag (`_state_cache_full` in Python) starts `False` and only flips
on transition; a sustained-bad rate emits exactly one `cache_full`, not
one per put.

**Do NOT add hit-rate detection.** An earlier iteration emitted
`hit_rate_dropped` / `hit_rate_recovered`; we removed it because hit
rate naturally varies with workload and there's no actionable
recommendation for a "low hit rate" event. See
`goldlapel/docs/todos/drop-hit-rate-dropped-event.md` for context.

## Required structure

1. **Stable `wrapper_id`** — UUID4 generated once at process start, kept
   for process lifetime. Lets the proxy aggregate per-wrapper across
   reconnects.
2. **`lang` + `version`** — language tag (literal `"python"`,
   `"ruby"`, `"go"`, etc.) and the wrapper package version (read from
   the package metadata if possible; fall back to `"unknown"`).
3. **Counters bumped under the cache lock** — `hits`, `misses`,
   `evictions`, `invalidations`. These already exist in every wrapper's
   cache impl; just expose them in the snapshot.
4. **Eviction sliding window** — bounded ring of length 200, recording
   1 (eviction happened) or 0 (insert without eviction) per `put()`.
   Append until full, then overwrite oldest in O(1).
5. **`?:` handler** — when the recv loop sees `?:snapshot` (or any
   non-empty body, today; the body is reserved for future request
   types), call `_emit_response()` to send `R:<json>`.
6. **`S:` emission on state transitions** — under the lock, check the
   eviction rate against the latched flag; flip the flag and stash the
   event name; release the lock; then emit. Never hold the cache lock
   across socket I/O.
7. **`_send_lock`** — a separate mutex guarding writes to the socket.
   Required because `R:` replies come from the recv thread and `S:`
   events come from the calling thread that ran `put()`. Concurrent
   `sendall` on the same FD without serialization could interleave
   bytes. Languages with native async I/O may use a queue/channel
   instead — either pattern works as long as writes are serialized.
8. **`report_stats` / `GOLDLAPEL_REPORT_STATS=false` opt-out** — when
   disabled, every emit path returns immediately. The cache continues
   to function (invalidation thread still runs; only telemetry output
   is suppressed). Customers who want zero proxy chatter can flip this.

## Threading model

The Python wrapper uses ONE background thread:

- **Invalidation/recv thread** (`daemon=True`) — connects to the
  invalidation socket, reads `I:` / `?:` / `P:` lines, dispatches them.
  Sends `wrapper_connected` on connect; sends `R:` replies inline
  when `?:` arrives.

`S:cache_full` and `S:wrapper_disconnected` emissions happen on
*whatever thread called the cache op* (or the at-exit thread). They
serialize on `_send_lock` so two threads can't tear each other's lines.

There is **NO** dedicated send thread. Sends are inline, fast, and
swallowed-on-error. If the socket is dead, the recv thread will detect
it on the next iteration and reconnect with exponential backoff (1s →
2s → 4s → 8s → 15s, capped). Other languages should mirror this — a
queue/worker thread is overkill for the volume of state-change events
we expect (single-digit per minute under stable load).

## Reconnect / failure handling

- Connect fails → exponential backoff, retry forever. No max attempts.
  Long-lived processes outlive proxy restarts; we want them to recover.
- `recv` returns 0 bytes or raises `OSError` → break inner loop,
  reconnect.
- `sendall` raises → swallow; the next recv iteration will detect the
  dead socket and reconnect. Don't try to repair from the send path —
  it races the reconnect logic.
- On disconnect, the wrapper invalidates its entire local cache (since
  it can no longer hear about upstream writes). This already exists in
  every wrapper.

## Graceful shutdown

Register an at-exit / process-exit hook (Python: `atexit`, Ruby:
`at_exit`, Go: `signal.Notify`, etc.) that calls
`emit_wrapper_disconnected()` BEFORE the process tears down. Best
effort — if the socket is already down, the emit is a silent no-op and
the proxy will time the wrapper out anyway.

The recv thread is `daemon=True` (or equivalent) so it doesn't block
process exit. After the at-exit hook fires, the daemon thread is killed
mid-`recv`. That's fine.

## Test pattern

Two layers of tests:

1. **Unit-level**: monkey-patch `_send_line` to a `list.append` lambda;
   exercise cache ops; assert on the captured emissions. No socket
   needed. Fast.
2. **Integration-level**: spin up a real `socket.socket(AF_INET)` on
   `127.0.0.1:0` (random port), `cache.connect_invalidation(port)`,
   accept the wrapper's connection, read its lines from a buffered
   reader thread, assert on protocol. Slow but proves the wire shape.

The Python suite has both (`tests/test_cache.py` —
`TestEvictionRateStateChange` for unit, `TestStateChangeEmission` for
integration). Mirror that split per language.

## Per-language notes

- **JS**: use `node:net` and `setImmediate` for emissions. ESM module.
- **Ruby**: `Socket.tcp` + a `Thread.new` recv loop; `Mutex` for send.
- **Java**: `java.net.Socket` + a daemon `Thread`; `synchronized` block
  for send.
- **PHP**: long-running CLI processes only — short-lived FPM workers
  don't benefit from this telemetry; they connect, do one query, exit.
  Decide per dispatch whether to no-op the whole telemetry path under
  FPM detection.
- **Go**: `net.Conn` + a goroutine; `sync.Mutex` for send. Use
  `context.Context` for cancellation rather than a stop event.
- **.NET**: `TcpClient` + a `Task.Run` loop; `lock` block for send.
  `IDisposable` for graceful shutdown.

In every language: the snapshot JSON shape MUST match exactly (same
field names, same types). The proxy parses it generically.
