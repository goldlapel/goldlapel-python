import atexit
import os
import platform
import queue
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path


DEFAULT_PROXY_PORT = 7932
_STARTUP_TIMEOUT = 10.0
_STARTUP_POLL_INTERVAL = 0.05

# Keys that are valid inside the structured `config` map. Top-level concepts
# (proxy_port, dashboard_port, log_level, mode, license, client,
# config_file) are exposed as top-level kwargs on `goldlapel.start`
# and on the `GoldLapel` constructor, and are NOT valid keys here — passing
# them through `config` raises.
_VALID_CONFIG_KEYS = frozenset({
    "min_pattern_count", "deep_pagination_threshold",
    "report_interval_secs", "proxy_cache_size", "batch_cache_size",
    "batch_cache_ttl_secs", "pool_size", "pool_timeout_secs",
    "pool_mode", "mgmt_idle_timeout", "fallback", "read_after_write_secs",
    "n1_threshold", "n1_window_ms", "n1_cross_threshold",
    "tls_cert", "tls_key", "tls_client_ca",
    "disable_btree_indexes",
    "disable_trigram_indexes", "disable_expression_indexes",
    "disable_partial_indexes", "disable_rewrite_prepared_cache",
    "disable_pool",
    "disable_n1", "disable_n1_cross_connection",
    "disable_coalescing", "replica", "exclude_tables",
})

_BOOLEAN_KEYS = frozenset({
    "disable_btree_indexes",
    "disable_trigram_indexes", "disable_expression_indexes",
    "disable_partial_indexes", "disable_rewrite_prepared_cache",
    "disable_pool",
    "disable_n1", "disable_n1_cross_connection",
    "disable_coalescing",
})

_LIST_KEYS = frozenset({
    "replica", "exclude_tables",
})

# log_level string → count of `-v` flags on the proxy CLI. The Rust binary
# currently exposes verbosity as a count flag (`-v`, `-vv`, `-vvv`) rather than
# `--log-level <level>`, so wrappers translate on the spawn side. Kept as a
# supported config option for API stability — if the proxy later adds
# `--log-level`, this mapping can be swapped out without breaking users.
_LOG_LEVEL_TO_VERBOSE = {
    "trace": "-vvv",
    "debug": "-vv",
    "info": "-v",
    "warn": None,
    "warning": None,
    "error": None,
}

# Options `goldlapel.start` / `GoldLapel(...)` no longer take, with why —
# named in the error a caller gets for passing one.
_REMOVED_OPTIONS = {
    "invalidation_port": "removed with the in-process cache",
    "disable_native_cache": "removed with the in-process cache",
    "native_cache": "removed with the in-process cache",
    "native_cache_size": "removed with the in-process cache",
    "aggressive_verify": "removed with the in-process cache",
    "disable_matviews": "removed: the proxy no longer builds materialized views",
}

# Connection parameters for the proxy's TLS/GSS hop to the upstream. The
# client URL handed to the app drops them: the proxy declines client TLS
# unless it was given --tls-cert/--tls-key, so `?sslmode=require` (every
# Neon / Supabase / RDS URL) would fail the app's connection to it.
_UPSTREAM_ONLY_PARAMS = frozenset({
    "sslmode", "sslcert", "sslkey", "sslrootcert", "sslcrl", "sslcrldir",
    "sslpassword", "sslsni", "sslnegotiation", "ssl_min_protocol_version",
    "ssl_max_protocol_version", "requiressl", "channel_binding",
    "gssencmode", "krbsrvname", "gsslib",
})

# Proxies started by the factories (`goldlapel.start`, `goldlapel.asyncio.start`),
# by upstream. An entry whose `_ready` event is unset is still starting.
_instances = {}
# Every GoldLapel holding its ports — factory-started or constructed
# directly — from the moment it picks them until it stops.
_live = set()
_cleanup_registered = False
_lock = threading.RLock()
_utils_mod = None
_spawner = None
_spawner_lock = threading.Lock()


def _utils():
    global _utils_mod
    if _utils_mod is None:
        from goldlapel import utils
        _utils_mod = utils
    return _utils_mod


def _config_to_args(config):
    if not config:
        return []

    unknown = set(config.keys()) - _VALID_CONFIG_KEYS
    if unknown:
        raise ValueError(f"Unknown config keys: {', '.join(sorted(unknown))}")

    args = []
    for key, value in config.items():
        flag = "--" + key.replace("_", "-")

        if key in _BOOLEAN_KEYS:
            if not isinstance(value, bool):
                raise TypeError(
                    f"Config key '{key}' expects a bool, got {type(value).__name__}"
                )
            if value:
                args.append(flag)
        elif key in _LIST_KEYS:
            if isinstance(value, str):
                value = [value]
            elif not isinstance(value, (list, tuple)):
                raise TypeError(
                    f"Config key '{key}' expects a list, got {type(value).__name__}"
                )
            for item in value:
                args.extend([flag, str(item)])
        else:
            args.extend([flag, str(value)])

    return args


def _unknown_options_message(names, prefix=""):
    """Error text for options Gold Lapel doesn't take — saying why, for
    the ones that were removed. `prefix` is stripped before the lookup
    (`goldlapel_` for SQLAlchemy engine kwargs)."""
    described = []
    for name in sorted(names):
        reason = _REMOVED_OPTIONS.get(name[len(prefix):] if name.startswith(prefix) else name)
        described.append(f"{name} ({reason})" if reason else name)
    return f"Unknown Gold Lapel options: {', '.join(described)}"


def _reject_unknown_options(unknown):
    if unknown:
        raise TypeError(_unknown_options_message(unknown))


def _client_tls(config, extra_args):
    """True when the proxy is told to serve TLS to its clients
    (`tls_cert`/`tls_key` in `config`, or the flags in `extra_args`) — then
    the app's URL keeps its TLS parameters."""
    config = config or {}
    extra_args = extra_args or []
    return bool(
        config.get("tls_cert") or config.get("tls_key")
        or "--tls-cert" in extra_args or "--tls-key" in extra_args
    )


def _strip_upstream_only_params(url):
    """`url` without the query parameters in _UPSTREAM_ONLY_PARAMS (keys
    compared case-insensitively). Everything else is kept byte for byte."""
    m = re.match(r'^([^?#]*)\?([^#]*)(#.*)?$', url)
    if not m:
        return url
    kept = [
        param for param in m.group(2).split("&")
        if param and param.split("=", 1)[0].lower() not in _UPSTREAM_ONLY_PARAMS
    ]
    query = "?" + "&".join(kept) if kept else ""
    return f"{m.group(1)}{query}{m.group(3) or ''}"


def _log_level_to_verbose_flag(level):
    """Translate a log-level string into the proxy's count-based verbosity
    flag (`-v`/`-vv`/`-vvv`). Returns None when no flag should be emitted
    (warn/error map to the binary's default level). Raises on invalid input.
    """
    if level is None:
        return None
    if not isinstance(level, str):
        raise TypeError(
            f"log_level expects a string, got {type(level).__name__}"
        )
    normalized = level.lower()
    if normalized not in _LOG_LEVEL_TO_VERBOSE:
        raise ValueError(
            "log_level must be one of: trace, debug, info, warn, error"
        )
    return _LOG_LEVEL_TO_VERBOSE[normalized]


def _is_python_shim(path):
    """Return True if `path` is a Python wrapper script (e.g. a pip-installed
    `[project.scripts]` entry point) rather than the real Rust binary.

    Detected by reading the first line: if it's a `#!` shebang that mentions
    `python`, it's a shim. Unreadable files (binaries, permission errors) are
    treated as not-a-shim so we don't spuriously skip the real binary.
    """
    try:
        with open(path, "rb") as f:
            first = f.readline(256)
    except OSError:
        return False
    if not first.startswith(b"#!"):
        return False
    return b"python" in first.lower()


def _find_binary():
    """Locate the Gold Lapel Rust binary. Search order:

    1. `GOLDLAPEL_BINARY` env var (explicit override — used as-is, no shim check).
    2. Bundled platform binary inside the installed package (`bin/goldlapel-<os>-<arch>`).
    3. `goldlapel` on `PATH`, walking entries in order and skipping Python shim
       scripts. In dev installs (`pip install -e .`), `pyproject.toml`'s
       `[project.scripts] goldlapel = "goldlapel.cli:main"` drops a Python
       wrapper into `.venv/bin/goldlapel` that would otherwise shadow the real
       Rust binary installed elsewhere on PATH.

    Raises `FileNotFoundError` if no real binary is found.
    """
    # 1. Explicit override via env var
    env_path = os.environ.get("GOLDLAPEL_BINARY")
    if env_path:
        p = Path(env_path)
        if p.is_file():
            return str(p)
        raise FileNotFoundError(f"GOLDLAPEL_BINARY points to {env_path} but file not found")

    # 2. Bundled binary (inside the installed package)
    pkg_dir = Path(__file__).parent
    system = platform.system().lower()
    machine = platform.machine().lower()

    if machine in ("x86_64", "amd64"):
        arch = "x86_64"
    elif machine in ("arm64", "aarch64"):
        arch = "aarch64"
    else:
        arch = machine

    if system == "linux":
        binary_name = f"goldlapel-linux-{arch}"
    elif system == "darwin":
        binary_name = f"goldlapel-darwin-{arch}"
    elif system == "windows":
        binary_name = f"goldlapel-windows-{arch}.exe"
    else:
        binary_name = f"goldlapel-{system}-{arch}"

    bundled = pkg_dir / "bin" / binary_name
    if bundled.is_file():
        return str(bundled)

    # 3. On PATH — walk entries manually so we can skip Python shims.
    path_env = os.environ.get("PATH", "")
    exe_names = ["goldlapel.exe", "goldlapel"] if system == "windows" else ["goldlapel"]
    for path_dir in path_env.split(os.pathsep):
        if not path_dir:
            continue
        for name in exe_names:
            candidate = os.path.join(path_dir, name)
            if not os.path.isfile(candidate) or not os.access(candidate, os.X_OK):
                continue
            if _is_python_shim(candidate):
                continue
            return candidate

    raise FileNotFoundError(
        "Gold Lapel binary not found. Set GOLDLAPEL_BINARY env var, "
        "install the platform-specific package, or ensure 'goldlapel' is on PATH."
    )


def _wrapper_version():
    """Return the wrapper's installed version, or "0.0.0" in dev installs.

    Used to build the application_name marker (`goldlapel:python:<version>`)
    that tags the wrapper's connections.
    """
    try:
        from importlib.metadata import version as _v, PackageNotFoundError
        try:
            return _v("goldlapel")
        except PackageNotFoundError:
            return "0.0.0"
    except ImportError:  # pragma: no cover — importlib.metadata is stdlib from 3.8+
        return "0.0.0"


def _application_name_marker():
    """The application_name string the wrapper sets on PG connections.

    Format: `goldlapel:python:<version>`. The proxy forwards it to Postgres
    untouched, so `pg_stat_activity` and ops dashboards can see which
    wrapper and version each connection came from. The proxy does not gate
    on it — these connections are cached exactly like any other client's.
    """
    return f"goldlapel:python:{_wrapper_version()}"


def _inject_application_name(url):
    """Append `application_name=goldlapel:python:<version>` to `url` unless the
    user already set one (in the original upstream or via env). Idempotent: a
    pre-existing `application_name` parameter is left untouched so caller
    overrides win.

    The marker is opt-in convention — if the user explicitly set application_name
    for their own debugging or telemetry tagging, we don't clobber it.
    """
    # Already has application_name (from the original upstream) — respect it.
    if re.search(r'[?&]application_name=', url):
        return url
    # Caller set one via libpq env var — respect it. (The Python wrapper itself
    # doesn't read PGAPPNAME, but psycopg/libpq do at connect time, and we don't
    # want to override that out from under them.)
    if os.environ.get("PGAPPNAME"):
        return url

    marker = _application_name_marker()
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}application_name={marker}"


def _make_proxy_url(upstream, port, client_tls=False):
    # Build a proxy URL: replace host with localhost and set the proxy port.
    # Uses regex instead of urlparse to avoid decoding percent-encoded characters
    # in passwords (e.g. %40 for @), which would corrupt the URL on reconstruction.
    # TLS/GSS parameters are for the proxy's upstream hop and are dropped
    # unless the proxy serves client TLS (see _UPSTREAM_ONLY_PARAMS).
    if not client_tls:
        upstream = _strip_upstream_only_params(upstream)

    # pg URL with explicit port: scheme://[userinfo@]host:PORT[/path][?query]
    # The port must be followed by /, ?, #, or end-of-string — not alphanumeric chars.
    # Without this anchor, passwords starting with digits (e.g. user:9password@host)
    # cause the regex to skip the userinfo group and misparse "user:9..." as host:port.
    m = re.match(r'^(postgres(?:ql)?://(?:.*@)?)([^:/?#]+):(\d+)([/?#].*)?$', upstream)
    if m:
        return _inject_application_name(f"{m.group(1)}localhost:{port}{m.group(4) or ''}")

    # pg URL without port: scheme://[userinfo@]host[/path][?query]
    m = re.match(r'^(postgres(?:ql)?://(?:.*@)?)([^:/?#]+)(.*)$', upstream)
    if m:
        return _inject_application_name(f"{m.group(1)}localhost:{port}{m.group(3)}")

    # bare host:port (only if not a URL — guard against splitting on scheme colons)
    # The marker is URL-encoded only; we don't try to inject into a bare host:port
    # form since callers using bare-host form are passing through unusual paths.
    if "://" not in upstream and ":" in upstream:
        return f"localhost:{port}"

    # bare host
    return f"localhost:{port}"


def _wait_for_port(host, port, timeout, process=None):
    """True once `port` accepts a connection. False on timeout, or as soon
    as `process` (the proxy being started) has exited."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process is not None and process.poll() is not None:
            return False
        try:
            sock = socket.create_connection((host, port), timeout=0.5)
            sock.close()
            return True
        except OSError:
            time.sleep(_STARTUP_POLL_INTERVAL)
    return False


def _port_in_use(port):
    try:
        sock = socket.create_connection(("127.0.0.1", port), timeout=0.5)
        sock.close()
        return True
    except OSError:
        return False


def _port_free(port):
    """True if `port` can be bound on all interfaces right now — the probe
    the proxy itself makes before it starts. SO_REUSEADDR (as Rust's std
    sets it on Unix) so a port with only TIME_WAIT connections counts as
    free; it never lets a bind share a port another socket listens on."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        if os.name != "nt":
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


_PROC = "/proc"


def _arg_value(argv, flag):
    try:
        return argv[argv.index(flag) + 1]
    except (ValueError, IndexError):
        return None


def _kill_orphan_on_port(port, upstream):
    """Stop a proxy an earlier run of this app left behind: a `goldlapel`
    process started for `upstream` on `port` whose parent has gone (it was
    reparented to init, pid 1). Never another live app's proxy, never one
    of our own children. Linux only — /proc gives each process's parent and
    exact argv. Elsewhere nothing is killed; the proxy refuses the busy port
    and says so."""
    if sys.platform != "linux" or os.getpid() == 1 or not _port_in_use(port):
        return
    killed = []
    try:
        entries = os.listdir(_PROC)
    except OSError:
        return
    for entry in entries:
        if not entry.isdigit():
            continue
        try:
            with open(os.path.join(_PROC, entry, "stat"), "rb") as f:
                stat = f.read()
            with open(os.path.join(_PROC, entry, "cmdline"), "rb") as f:
                cmdline = f.read()
            # Fields after the parenthesised command name: state, ppid, ...
            ppid = int(stat.rsplit(b")", 1)[1].split()[1])
        except (OSError, IndexError, ValueError):
            continue
        argv = [arg.decode(errors="surrogateescape") for arg in cmdline.rstrip(b"\0").split(b"\0")]
        if (
            ppid != 1
            or not os.path.basename(argv[0]).startswith("goldlapel")
            or _arg_value(argv, "--upstream") != upstream
            or _arg_value(argv, "--proxy-port") != str(port)
        ):
            continue
        try:
            os.kill(int(entry), signal.SIGTERM)
            killed.append(int(entry))
        except OSError:
            pass
    deadline = time.monotonic() + 2.0
    while killed and time.monotonic() < deadline:
        time.sleep(_STARTUP_POLL_INTERVAL)
        killed = [pid for pid in killed if os.path.exists(os.path.join(_PROC, str(pid)))]


def _redact_password(url):
    """`url` with the password in its userinfo replaced by `***`, for error
    messages."""
    return re.sub(r'^([^:/?#]+://[^:/?#@]*:).*@', r'\1***@', url)


def _claimed_ports(exclude=None):
    """Ports held by the proxies this process has started (or is starting),
    other than `exclude`, as {port: (upstream, "proxy" | "dashboard")}: each
    one's proxy port plus its dashboard port (none when disabled with 0). A
    proxy whose process has exited holds nothing. Caller holds `_lock`."""
    claimed = {}
    for inst in _live:
        if inst is exclude:
            continue
        if inst._process is not None and inst._process.poll() is not None:
            continue
        claimed[int(inst._proxy_port)] = (inst._upstream, "proxy")
        if inst._dashboard_port:
            claimed[inst._dashboard_port] = (inst._upstream, "dashboard")
    return claimed


def _check_ports_free(proxy_port, dashboard_port, claimed):
    """Raise if the proxy or dashboard port a new proxy would listen on is
    held by another live proxy of this process (`claimed`). Without this an
    explicit port would hand the caller the other upstream's proxy."""
    proxy_port = int(proxy_port)
    if dashboard_port is None:
        dashboard_port = proxy_port + 1
    for port, role in ((proxy_port, "proxy"), (int(dashboard_port), "dashboard")):
        if port and port in claimed:
            upstream, held_as = claimed[port]
            raise RuntimeError(
                f"Gold Lapel cannot use port {port} as the {role} port: this "
                f"process's proxy for {_redact_password(upstream)} already "
                f"holds it as its {held_as} port. Choose another port, or omit "
                "proxy_port and dashboard_port to have a free pair assigned."
            )


def _pick_proxy_port(dashboard_port, claimed):
    """Auto-assign a proxy port: the smallest P >= DEFAULT_PROXY_PORT such
    that neither P nor its dashboard port (P + 1 unless `dashboard_port` is
    given) is claimed by another proxy of this process, and both can be
    bound right now — so another app's proxy, or anything else listening,
    is stepped over too. An explicit dashboard port is the caller's choice,
    so only P is checked."""
    for port in range(DEFAULT_PROXY_PORT, 65535):
        if port in claimed:
            continue
        if dashboard_port is None:
            if port + 1 in claimed or not (_port_free(port) and _port_free(port + 1)):
                continue
        elif port == int(dashboard_port) or not _port_free(port):
            continue
        return port
    raise RuntimeError("Gold Lapel could not find a free proxy port")


def _set_pdeathsig():
    if sys.platform == "linux":
        try:
            import ctypes
            libc = ctypes.CDLL("libc.so.6", use_errno=True)
            PR_SET_PDEATHSIG = 1
            libc.prctl(PR_SET_PDEATHSIG, signal.SIGTERM)
        except Exception:
            pass


def _spawn_loop(requests):
    while True:
        cmd, kwargs, result, done = requests.get()
        try:
            result["process"] = subprocess.Popen(cmd, **kwargs)
        except BaseException as exc:
            result["error"] = exc
        finally:
            done.set()


def _popen(cmd, **kwargs):
    """subprocess.Popen — on Linux from one long-lived thread. The proxy's
    PR_SET_PDEATHSIG fires when the *thread* that spawned it exits, not the
    process: started from a short-lived thread (a request thread of Django's
    runserver, a pool worker that is retired) the proxy would die with it."""
    global _spawner
    if sys.platform != "linux":
        return subprocess.Popen(cmd, **kwargs)
    with _spawner_lock:
        # After a fork the old thread is gone (is_alive() is False in the
        # child), so the child starts its own.
        if _spawner is None or not _spawner[0].is_alive():
            requests = queue.Queue()
            thread = threading.Thread(
                target=_spawn_loop, args=(requests,), name="goldlapel-spawner", daemon=True,
            )
            thread.start()
            _spawner = (thread, requests)
        requests = _spawner[1]
    result, done = {}, threading.Event()
    requests.put((cmd, kwargs, result, done))
    done.wait()
    if "error" in result:
        raise result["error"]
    return result["process"]


def _stderr_tail(process, lines=20):
    try:
        text = process.stderr.read().decode(errors="replace")
        process.stderr.close()
    except Exception:
        return ""
    return "\n".join(text.strip().splitlines()[-lines:])


class GoldLapel:
    def __init__(
        self,
        upstream,
        *,
        proxy_port=None,
        dashboard_port=None,
        log_level=None,
        mode=None,
        license=None,
        api_key=None,
        client=None,
        config_file=None,
        config=None,
        extra_args=None,
        silent=False,
        mesh=False,
        mesh_tag=None,
        disable_proxy_cache=False,
        disable_sqloptimize=False,
        disable_auto_indexes=False,
        **unknown,
    ):
        _reject_unknown_options(unknown)
        self._upstream = upstream
        # Without an explicit proxy_port, start() assigns the first free
        # pair; 7932 is the answer when nothing else holds it.
        self._proxy_port_explicit = proxy_port is not None
        self._proxy_port = proxy_port if proxy_port is not None else DEFAULT_PROXY_PORT

        # Dashboard port defaults to proxyPort + 1 when unset. An explicit
        # value (including 0 for "disable dashboard") overrides the
        # derivation and is emitted as --dashboard-port at spawn time.
        self._dashboard_port_explicit = dashboard_port is not None
        self._dashboard_port = (
            int(dashboard_port) if dashboard_port is not None else self._proxy_port + 1
        )

        self._log_level = log_level
        self._mode = mode
        self._license = license
        # api_key (Wave 1 of api-key-model rollout): the new primary
        # license credential. When set, the proxy fetches and auto-
        # renews its PEM from HQ. The Rust binary's precedence: api_key
        # > license file > anonymous trial.  If both are passed, log a
        # warning and let api_key win (it's the recommended path).
        if api_key is not None and license is not None:
            import logging
            logging.getLogger(__name__).warning(
                "Both api_key and license were passed to GoldLapel; "
                "api_key takes precedence (license file becomes the offline fallback)."
            )
        self._api_key = api_key
        self._client = client
        self._config_file = config_file
        self._silent = bool(silent)
        # Mesh membership (startup intent — HQ enforces license).
        self._mesh = bool(mesh)
        self._mesh_tag = mesh_tag if mesh_tag else None
        # Promoted disable flags. Each maps 1:1 to the proxy CLI flag at
        # spawn time. Not valid in the structured `config` map — passing
        # them through `config={...}` is a hard error.
        self._disable_proxy_cache = bool(disable_proxy_cache)
        self._disable_sqloptimize = bool(disable_sqloptimize)
        self._disable_auto_indexes = bool(disable_auto_indexes)

        # Validate structured-config keys eagerly so a test that constructs
        # without spawning still catches bad keys.
        if config is not None:
            unknown = set(config.keys()) - _VALID_CONFIG_KEYS
            if unknown:
                raise ValueError(
                    f"Unknown config keys: {', '.join(sorted(unknown))}"
                )
        self._config = config

        self._extra_args = extra_args or []
        self._process = None
        self._proxy_url = None
        self._conn = None
        # Callers sharing this proxy: every start() of a running upstream
        # adds one, every stop() drops one, the last one stops the proxy.
        self._holders = 1
        # Set once a factory start of this instance has finished, either
        # way; concurrent starts of the same upstream wait on it.
        self._ready = threading.Event()
        # Dashboard token — resolved at start() time. When we spawn the proxy
        # ourselves, we generate a random token per-session and pass it via
        # env. When the proxy is externally launched, we read the token from
        # env/file at DDL-call time (see goldlapel/ddl.py).
        self._dashboard_token = None
        # Per-instance contextvar for `with gl.using(conn):` — async-safe, scoped override.
        self._using_conn = ContextVar(f"goldlapel_using_conn_{id(self)}", default=None)

        # Nested namespaces — canonical schema-to-core sub-API instances. Each
        # holds a back-reference to this client for shared state (license,
        # dashboard token, http session, conn, DDL pattern cache).
        #
        # As of Phase 5 the Redis-compat helper families (counter / zset /
        # hash / queue / geo) are nested too, alongside streams (Phase 1+2)
        # and documents (Phase 4). Search / cache / auth remain flat —
        # they'll migrate when their own schema-to-core phase fires.
        from goldlapel.documents import DocumentsAPI
        from goldlapel.streams import StreamsAPI
        from goldlapel.counters import CountersAPI
        from goldlapel.zsets import ZsetsAPI
        from goldlapel.hashes import HashesAPI
        from goldlapel.queues import QueuesAPI
        from goldlapel.geos import GeosAPI
        self.documents = DocumentsAPI(self)
        self.streams = StreamsAPI(self)
        self.counters = CountersAPI(self)
        self.zsets = ZsetsAPI(self)
        self.hashes = HashesAPI(self)
        self.queues = QueuesAPI(self)
        self.geos = GeosAPI(self)

    # Context manager support: `with goldlapel.start(...) as gl:` auto-stops on exit.
    def __enter__(self):
        if not self.running:
            self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False

    @contextmanager
    def using(self, conn):
        """Scoped override: all wrapper methods called inside this `with` block
        will use `conn` (typically your own psycopg2/psycopg3 connection that may
        be inside a transaction) instead of the instance's internal connection.
        """
        token = self._using_conn.set(conn)
        try:
            yield self
        finally:
            self._using_conn.reset(token)

    def _effective_conn(self, override=None):
        """Resolve which conn a wrapper method should use.
        Precedence: explicit method kwarg > scoped `using()` conn > internal conn.
        """
        if override is not None:
            return override
        scoped = self._using_conn.get()
        if scoped is not None:
            return scoped
        return self.conn  # raises if not started

    def start(self):
        if self.running:
            return self._proxy_url
        self._spawn()
        # If connect() raises (network hiccup, bad creds, KeyboardInterrupt,
        # ...) the subprocess is already running and would leak, holding its
        # ports. Clean it up before re-raising.
        try:
            self._open_conn()
        except BaseException:
            self._kill_process()
            self._release_ports()
            raise
        self._print_banner()
        return self._proxy_url

    def _open_conn(self):
        driver_name, driver = _detect_sync_driver()
        # The factory entry point `goldlapel.start(url)` raises ImportError if no
        # driver is available, so in that flow `driver` is always non-None here.
        # This guard protects direct `GoldLapel(...)` construction (a supported
        # public entry point, re-exported from `goldlapel.__init__`), which doesn't
        # pre-check: without a driver we skip opening the internal connection, and
        # the user can still use `gl.url` with their own async/raw driver.
        if driver is None:
            return
        if driver_name == "psycopg3":
            self._conn = driver.connect(self._proxy_url, autocommit=True)
        else:
            self._conn = driver.connect(self._proxy_url)

    def _command(self, binary):
        cmd = [
            binary,
            "--upstream", self._upstream,
            "--proxy-port", str(self._proxy_port),
        ]
        # Top-level options (promoted out of the config map) emit their own
        # CLI flags before the tuning-knob config map. Each is suppressed
        # when the user hasn't set it, so the Rust binary applies its own
        # defaults.
        if self._dashboard_port_explicit:
            cmd += ["--dashboard-port", str(self._dashboard_port)]
        verbose_flag = _log_level_to_verbose_flag(self._log_level)
        if verbose_flag is not None:
            cmd.append(verbose_flag)
        if self._mode is not None:
            cmd += ["--mode", self._mode]
        if self._license is not None:
            cmd += ["--license", self._license]
        if self._client is not None:
            cmd += ["--client", self._client]
        if self._config_file is not None:
            cmd += ["--config", self._config_file]
        if self._mesh:
            cmd.append("--mesh")
        if self._mesh_tag is not None:
            cmd += ["--mesh-tag", self._mesh_tag]
        # Promoted disable flags — emitted as 1:1 CLI flags. Suppressed
        # when False so the binary applies its own defaults.
        if self._disable_proxy_cache:
            cmd.append("--disable-proxy-cache")
        if self._disable_sqloptimize:
            cmd.append("--disable-sqloptimize")
        if self._disable_auto_indexes:
            cmd.append("--disable-auto-indexes")
        return cmd + _config_to_args(self._config) + self._extra_args

    def _claim_ports(self):
        """Pick this proxy's ports (unless given) and claim them, so no
        other proxy of this process is handed them until it stops."""
        global _cleanup_registered
        with _lock:
            claimed = _claimed_ports(exclude=self)
            dashboard_port = self._dashboard_port if self._dashboard_port_explicit else None
            if not self._proxy_port_explicit:
                self._proxy_port = _pick_proxy_port(dashboard_port, claimed)
                if dashboard_port is None:
                    self._dashboard_port = self._proxy_port + 1
            _check_ports_free(self._proxy_port, dashboard_port, claimed)
            _live.add(self)
            if not _cleanup_registered:
                atexit.register(_cleanup)
                _cleanup_registered = True

    def _release_ports(self):
        with _lock:
            _live.discard(self)

    def _spawn(self):
        """Claim ports, spawn the proxy and wait until it serves them.
        Shared by the sync and async starts. Any failure — KeyboardInterrupt
        and cancellation included — kills the child and releases the ports."""
        # Validate everything that can be validated before claiming anything.
        binary = _find_binary()
        self._command(binary)
        self._claim_ports()
        try:
            cmd = self._command(binary)
            _kill_orphan_on_port(self._proxy_port, self._upstream)
            busy = [
                port for port in (self._proxy_port, self._dashboard_port)
                if port and not _port_free(port)
            ]

            env = os.environ.copy()
            # GOLDLAPEL_CLIENT env var is only set when the user hasn't opted in
            # via the top-level `client` kwarg (which emits --client and takes
            # precedence over the env var).
            if self._client is None:
                env.setdefault("GOLDLAPEL_CLIENT", "python")
            # Pass api_key to the binary as an env var rather than CLI flag so
            # it doesn't show up in `ps` output (credentials hygiene). The
            # Rust binary reads `GOLDLAPEL_API_KEY` at startup and uses it
            # to fetch + auto-renew the license from HQ.
            if self._api_key is not None:
                env["GOLDLAPEL_API_KEY"] = self._api_key
            # Provision a session-scoped dashboard token so the wrapper can call
            # /api/ddl/* without depending on ~/.goldlapel/dashboard-token. Pre-set
            # env wins (user may already have a token they want to use).
            if "GOLDLAPEL_DASHBOARD_TOKEN" in env and env["GOLDLAPEL_DASHBOARD_TOKEN"]:
                self._dashboard_token = env["GOLDLAPEL_DASHBOARD_TOKEN"]
            else:
                import secrets
                self._dashboard_token = secrets.token_hex(32)
                env["GOLDLAPEL_DASHBOARD_TOKEN"] = self._dashboard_token
            popen_kwargs = dict(
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            if sys.platform == "linux":
                popen_kwargs["preexec_fn"] = _set_pdeathsig
            self._process = _popen(cmd, **popen_kwargs)
            self._wait_ready(busy)
            self._process.stderr.close()
            self._proxy_url = _make_proxy_url(
                self._upstream, self._proxy_port,
                client_tls=_client_tls(self._config, self._extra_args),
            )
            self._holders = 1
        except BaseException:
            self._kill_process()
            self._release_ports()
            raise

    def _wait_ready(self, busy):
        """Return once the proxy answers on its port and is still alive;
        otherwise raise with its exit status and the tail of its stderr
        (where the proxy says why — e.g. a port already in use)."""
        port = self._proxy_port
        if busy:
            # Something already listens on a port this proxy needs, so a
            # connect would reach it, not our proxy. The proxy refuses a
            # busy port and exits: wait for that.
            try:
                self._process.wait(timeout=_STARTUP_TIMEOUT)
            except subprocess.TimeoutExpired:
                pass
        elif (
            _wait_for_port("127.0.0.1", port, _STARTUP_TIMEOUT, self._process)
            and self._process.poll() is None
        ):
            return
        status = self._process.poll()
        if status is None:
            self._process.kill()
            reason = f"within {_STARTUP_TIMEOUT}s"
            if busy:
                reason += f" (port {busy[0]} was already in use)"
        else:
            reason = f"— the proxy exited with status {status}"
        raise RuntimeError(
            f"Gold Lapel failed to start on port {port} {reason}.\n"
            f"stderr: {_stderr_tail(self._process)}"
        )

    def _kill_process(self):
        process, self._process, self._proxy_url = self._process, None, None
        if process is not None and process.poll() is None:
            try:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            except Exception:
                pass

    def _print_banner(self):
        # Startup banner: stderr, not stdout. Library code writing to stdout
        # pollutes app output, CI logs, and anything that captures stdout
        # (pytest -s, subprocess piping). Suppressed entirely when the caller
        # passes `silent=True`.
        if self._silent:
            return
        if self._dashboard_port:
            banner = (
                f"goldlapel → :{self._proxy_port} (proxy) | "
                f"http://127.0.0.1:{self._dashboard_port} (dashboard)"
            )
        else:
            banner = f"goldlapel → :{self._proxy_port} (proxy)"
        print(banner, file=sys.stderr)

    def stop(self):
        """Stop the proxy — or, when other callers started the same
        upstream and share it, just this caller's hold on it: the last
        stop() stops the proxy."""
        with _lock:
            if self._holders > 1:
                self._holders -= 1
                return
            self._holders = 0
            # Only `self`: a directly-constructed GoldLapel for the same
            # upstream must not evict the factory's live one.
            if _instances.get(self._upstream) is self:
                del _instances[self._upstream]
        # Drop any cached DDL patterns — they are tied to the proxy
        # instance we're about to kill, and they must not leak into the
        # next start() of the same upstream URL.
        try:
            from goldlapel import ddl as _ddl
            _ddl.invalidate(self)
        except Exception:
            pass
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
        self._kill_process()
        self._dashboard_token = None
        # Released only once the process is gone, so its ports are free.
        self._release_ports()

    @property
    def conn(self):
        if self._conn is None:
            raise RuntimeError("Not connected. Call start() first.")
        return self._conn

    @property
    def url(self):
        return self._proxy_url

    @property
    def dashboard_url(self):
        if self._dashboard_port and self._process and self._process.poll() is None:
            return f"http://127.0.0.1:{self._dashboard_port}"
        return None

    @property
    def proxy_port(self):
        """Proxy listen port."""
        return self._proxy_port

    @property
    def dashboard_port(self):
        """Dashboard port (0 if disabled via setting to 0)."""
        return self._dashboard_port

    @property
    def dashboard_token(self):
        """Dashboard token used by the DDL API. Resolved on start() when the
        wrapper spawns the proxy itself; None when the proxy is external —
        in that case goldlapel/ddl.py falls back to env/file."""
        return self._dashboard_token

    @property
    def running(self):
        return self._process is not None and self._process.poll() is None

    # -- Document store: gl.documents.<verb>(...). See goldlapel/documents.py.

    # -- Search ----------------------------------------------------------------

    def search(self, *args, conn=None, **kwargs):
        return _utils().search(self._effective_conn(conn), *args, **kwargs)

    def search_fuzzy(self, *args, conn=None, **kwargs):
        return _utils().search_fuzzy(self._effective_conn(conn), *args, **kwargs)

    def search_phonetic(self, *args, conn=None, **kwargs):
        return _utils().search_phonetic(self._effective_conn(conn), *args, **kwargs)

    def similar(self, *args, conn=None, **kwargs):
        return _utils().similar(self._effective_conn(conn), *args, **kwargs)

    def suggest(self, *args, conn=None, **kwargs):
        return _utils().suggest(self._effective_conn(conn), *args, **kwargs)

    def facets(self, *args, conn=None, **kwargs):
        return _utils().facets(self._effective_conn(conn), *args, **kwargs)

    def aggregate(self, *args, conn=None, **kwargs):
        return _utils().aggregate(self._effective_conn(conn), *args, **kwargs)

    def create_search_config(self, *args, conn=None, **kwargs):
        return _utils().create_search_config(self._effective_conn(conn), *args, **kwargs)

    # -- Pub/sub ---------------------------------------------------------------

    def publish(self, *args, conn=None, **kwargs):
        return _utils().publish(self._effective_conn(conn), *args, **kwargs)

    def subscribe(self, *args, conn=None, **kwargs):
        return _utils().subscribe(self._effective_conn(conn), *args, **kwargs)

    # -- Phase 5 Redis-compat families: gl.counters / gl.zsets / gl.hashes /
    #    gl.queues / gl.geos. The legacy flat methods (incr, hset, zadd,
    #    enqueue, geoadd, …) are gone — see the per-family modules under
    #    src/goldlapel/{counters,zsets,hashes,queues,geos}.py.

    # -- Misc ------------------------------------------------------------------

    def count_distinct(self, *args, conn=None, **kwargs):
        return _utils().count_distinct(self._effective_conn(conn), *args, **kwargs)

    def script(self, *args, conn=None, **kwargs):
        return _utils().script(self._effective_conn(conn), *args, **kwargs)

    # -- Streams: gl.streams.<verb>(...). See goldlapel/streams.py.

    # -- Percolator ------------------------------------------------------------

    def percolate_add(self, *args, conn=None, **kwargs):
        return _utils().percolate_add(self._effective_conn(conn), *args, **kwargs)

    def percolate(self, *args, conn=None, **kwargs):
        return _utils().percolate(self._effective_conn(conn), *args, **kwargs)

    def percolate_delete(self, *args, conn=None, **kwargs):
        return _utils().percolate_delete(self._effective_conn(conn), *args, **kwargs)

    # -- Analysis --------------------------------------------------------------

    def analyze(self, *args, conn=None, **kwargs):
        return _utils().analyze(self._effective_conn(conn), *args, **kwargs)

    def explain_score(self, *args, conn=None, **kwargs):
        return _utils().explain_score(self._effective_conn(conn), *args, **kwargs)


def _ensure_running(upstream, **options):
    """The running factory proxy for `upstream` — started now, or shared
    with the callers that started it (each holds it until its stop()).
    A start already in progress for `upstream` in another thread is waited
    for, never raced: no second spawn on the same ports, and never an
    instance whose url or conn aren't set yet."""
    while True:
        with _lock:
            inst = _instances.get(upstream)
            if inst is not None and inst._ready.is_set():
                if inst.running:
                    # Started by goldlapel.asyncio: open the sync conn.
                    if inst._conn is None:
                        inst._open_conn()
                    inst._holders += 1
                    return inst
                del _instances[upstream]
                inst = None
            if inst is None:
                # Option errors raise here, before anything is registered.
                inst = GoldLapel(upstream, **options)
                _instances[upstream] = inst
                break
        inst._ready.wait()

    try:
        inst.start()
    except BaseException:
        with _lock:
            if _instances.get(upstream) is inst:
                del _instances[upstream]
        raise
    finally:
        inst._ready.set()
    return inst


def _detect_sync_driver():
    try:
        import psycopg
        return "psycopg3", psycopg
    except ImportError:
        pass
    try:
        import psycopg2
        return "psycopg2", psycopg2
    except ImportError:
        pass
    return None, None


def _detect_async_driver():
    try:
        import asyncpg
        return "asyncpg", asyncpg
    except ImportError:
        pass
    try:
        import psycopg
        return "psycopg3", psycopg
    except ImportError:
        pass
    return None, None


def start(
    upstream,
    *,
    proxy_port=None,
    dashboard_port=None,
    log_level=None,
    mode=None,
    license=None,
    api_key=None,
    client=None,
    config_file=None,
    config=None,
    extra_args=None,
    silent=False,
    mesh=False,
    mesh_tag=None,
    disable_proxy_cache=False,
    disable_sqloptimize=False,
    disable_auto_indexes=False,
    **unknown,
):
    """Factory: spawn a Gold Lapel proxy in front of `upstream` and return a
    GoldLapel instance. Call wrapper methods on the returned instance
    (e.g. `gl.search(...)`), or use `gl.url` with your own Postgres driver.

    Eager: opens the instance's internal DB connection before returning so the
    first wrapper method call is fast. Requires a sync Postgres driver
    installed (psycopg2 or psycopg3) — raises ImportError otherwise.

    Top-level kwargs match the canonical config surface shared across every
    Gold Lapel wrapper:

    - proxy_port: proxy listen port. Default: 7932, or for further upstreams
        the next port whose pair (proxy + dashboard) no other proxy started
        by this process holds — 7934, 7936, ...
    - dashboard_port: dashboard port (derived as proxy_port + 1 when unset; 0 disables)
    - log_level: one of 'trace', 'debug', 'info', 'warn', 'error'
    - mode: proxy operating mode ('waiter', 'consideration', ...)
    - api_key: stable customer credential (`gl_live_*` / `gl_test_*`).
        The proxy fetches and auto-renews its license from HQ — recommended.
    - license: path to a license PEM file. Backup / offline path; api_key
        takes precedence when both are passed.
    - client: client identifier for telemetry tagging (sets GOLDLAPEL_CLIENT)
    - config_file: path to a TOML config file (passed as --config to the binary)
    - config: dict of tuning knobs (pool_size, disable_*, replica, ...)
    - extra_args: raw CLI flags appended to the binary invocation
    - silent: suppress the startup banner
    - mesh: opt into the mesh at startup (HQ enforces license; denial is non-fatal)
    - mesh_tag: optional tag — instances sharing a tag cluster together
    - disable_proxy_cache: turn off the proxy's result cache (--disable-proxy-cache).
        Default False.
    - disable_sqloptimize: skip SQL rewriting (--disable-sqloptimize).
        Default False.
    - disable_auto_indexes: skip automatic index creation (--disable-auto-indexes).
        Default False.

    Promoted top-level concepts are rejected inside the `config` dict.

    Usage:
        gl = goldlapel.start("postgresql://user:pass@db/mydb")
        gl.search("articles", "body", "postgres")
        conn = psycopg2.connect(gl.url)    # raw driver usage still supported

    Context manager usage:
        with goldlapel.start("postgresql://...") as gl:
            gl.search(...)
        # proxy stopped automatically on exit

    Starting an upstream that is already running in this process returns
    that proxy; it keeps running until every start() of it has been
    matched by a stop() (sync and async alike). `goldlapel.stop(url)`
    stops it outright.
    """
    _reject_unknown_options(unknown)
    _, driver = _detect_sync_driver()
    if driver is None:
        raise ImportError(
            "Gold Lapel wrapper methods need a sync Postgres driver. "
            "Install one: `pip install psycopg2-binary` or `pip install psycopg`."
        )
    inst = _ensure_running(
        upstream,
        proxy_port=proxy_port,
        dashboard_port=dashboard_port,
        log_level=log_level,
        mode=mode,
        license=license,
        api_key=api_key,
        client=client,
        config_file=config_file,
        config=config,
        extra_args=extra_args,
        silent=silent,
        mesh=mesh,
        mesh_tag=mesh_tag,
        disable_proxy_cache=disable_proxy_cache,
        disable_sqloptimize=disable_sqloptimize,
        disable_auto_indexes=disable_auto_indexes,
    )
    return inst




def connect(upstream=None):
    with _lock:
        if upstream is not None:
            inst = _instances.get(upstream)
        elif len(_instances) == 1:
            inst = next(iter(_instances.values()))
        else:
            inst = None
    if inst is None or not inst.running:
        raise RuntimeError("Gold Lapel is not running. Call start() first.")
    driver_name, driver = _detect_sync_driver()
    if driver is None:
        raise ImportError("No supported sync Postgres driver found.")
    if driver_name == "psycopg3":
        return driver.connect(inst.url, autocommit=True)
    return driver.connect(inst.url)


def _stop_outright(inst):
    """Stop `inst` whoever else holds it."""
    with _lock:
        inst._holders = 1
    inst.stop()


def stop(upstream=None):
    """Stop the proxy for `upstream`, or every proxy the factories started —
    outright, however many callers share it."""
    with _lock:
        if upstream is not None:
            inst = _instances.get(upstream)
            insts = [inst] if inst is not None else []
        else:
            insts = list(_instances.values())
        for inst in insts:
            _stop_outright(inst)


def proxy_url(upstream=None):
    with _lock:
        if upstream is not None:
            inst = _instances.get(upstream)
            return inst.url if inst else None
        # Single-database convenience: return the only instance's URL
        if len(_instances) == 1:
            return next(iter(_instances.values())).url
        if not _instances:
            return None
        # Multiple instances -- caller must specify upstream
        raise RuntimeError(
            "Multiple Gold Lapel instances are running. "
            "Pass the upstream URL to proxy_url() to identify which one."
        )


def dashboard_url(upstream=None):
    with _lock:
        if upstream is not None:
            inst = _instances.get(upstream)
            return inst.dashboard_url if inst else None
        if len(_instances) == 1:
            return next(iter(_instances.values())).dashboard_url
        if not _instances:
            return None
        raise RuntimeError(
            "Multiple Gold Lapel instances are running. "
            "Pass the upstream URL to dashboard_url() to identify which one."
        )


def config_keys():
    return set(_VALID_CONFIG_KEYS)


def _cleanup():
    # Every proxy this process started, factory-started or not.
    with _lock:
        for inst in list(_live):
            _stop_outright(inst)
        _instances.clear()
