import atexit
import os
import platform
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
# (proxy_port, dashboard_port, invalidation_port, log_level, mode, license,
# client, config_file) are exposed as top-level kwargs on `goldlapel.start`
# and on the `GoldLapel` constructor, and are NOT valid keys here — passing
# them through `config` raises.
_VALID_CONFIG_KEYS = frozenset({
    "min_pattern_count", "refresh_interval_secs", "pattern_ttl_secs",
    "max_tables_per_view", "max_columns_per_view", "deep_pagination_threshold",
    "report_interval_secs", "result_cache_size", "batch_cache_size",
    "batch_cache_ttl_secs", "pool_size", "pool_timeout_secs",
    "pool_mode", "mgmt_idle_timeout", "fallback", "read_after_write_secs",
    "n1_threshold", "n1_window_ms", "n1_cross_threshold",
    "tls_cert", "tls_key", "tls_client_ca",
    "disable_matviews", "disable_consolidation", "disable_btree_indexes",
    "disable_trigram_indexes", "disable_expression_indexes",
    "disable_partial_indexes", "disable_rewrite", "disable_prepared_cache",
    "disable_result_cache", "disable_pool",
    "disable_n1", "disable_n1_cross_connection", "disable_shadow_mode",
    "enable_coalescing", "replica", "exclude_tables",
})

_BOOLEAN_KEYS = frozenset({
    "disable_matviews", "disable_consolidation", "disable_btree_indexes",
    "disable_trigram_indexes", "disable_expression_indexes",
    "disable_partial_indexes", "disable_rewrite", "disable_prepared_cache",
    "disable_result_cache", "disable_pool",
    "disable_n1", "disable_n1_cross_connection", "disable_shadow_mode",
    "enable_coalescing",
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

_instances = {}
_cleanup_registered = False
_lock = threading.Lock()
_next_port = DEFAULT_PROXY_PORT
_utils_mod = None


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


def _make_proxy_url(upstream, port):
    # Build a proxy URL: replace host with localhost and set the proxy port.
    # Uses regex instead of urlparse to avoid decoding percent-encoded characters
    # in passwords (e.g. %40 for @), which would corrupt the URL on reconstruction.

    # pg URL with explicit port: scheme://[userinfo@]host:PORT[/path][?query]
    # The port must be followed by /, ?, #, or end-of-string — not alphanumeric chars.
    # Without this anchor, passwords starting with digits (e.g. user:9password@host)
    # cause the regex to skip the userinfo group and misparse "user:9..." as host:port.
    m = re.match(r'^(postgres(?:ql)?://(?:.*@)?)([^:/?#]+):(\d+)([/?#].*)?$', upstream)
    if m:
        return f"{m.group(1)}localhost:{port}{m.group(4) or ''}"

    # pg URL without port: scheme://[userinfo@]host[/path][?query]
    m = re.match(r'^(postgres(?:ql)?://(?:.*@)?)([^:/?#]+)(.*)$', upstream)
    if m:
        return f"{m.group(1)}localhost:{port}{m.group(3)}"

    # bare host:port (only if not a URL — guard against splitting on scheme colons)
    if "://" not in upstream and ":" in upstream:
        return f"localhost:{port}"

    # bare host
    return f"localhost:{port}"


def _wait_for_port(host, port, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
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


def _kill_orphan_on_port(port):
    if not _port_in_use(port):
        return
    if shutil.which("lsof"):
        try:
            out = subprocess.check_output(
                ["lsof", "-ti", f":{port}", "-c", "goldlapel"],
                stderr=subprocess.DEVNULL, text=True,
            )
            for pid_str in out.strip().split():
                pid = int(pid_str)
                if pid != os.getpid():
                    os.kill(pid, signal.SIGTERM)
            time.sleep(0.5)
        except (subprocess.CalledProcessError, ValueError, OSError):
            pass


def _set_pdeathsig():
    if sys.platform == "linux":
        try:
            import ctypes
            libc = ctypes.CDLL("libc.so.6", use_errno=True)
            PR_SET_PDEATHSIG = 1
            libc.prctl(PR_SET_PDEATHSIG, signal.SIGTERM)
        except Exception:
            pass


class GoldLapel:
    def __init__(
        self,
        upstream,
        *,
        proxy_port=None,
        dashboard_port=None,
        invalidation_port=None,
        log_level=None,
        mode=None,
        license=None,
        client=None,
        config_file=None,
        config=None,
        extra_args=None,
        silent=False,
    ):
        self._upstream = upstream
        self._proxy_port = proxy_port if proxy_port is not None else DEFAULT_PROXY_PORT

        # Dashboard / invalidation ports default to proxyPort + 1 / + 2 when
        # unset. An explicit value (including 0 for "disable dashboard")
        # overrides the derivation and is emitted as --dashboard-port /
        # --invalidation-port at spawn time.
        self._dashboard_port_explicit = dashboard_port is not None
        self._dashboard_port = (
            int(dashboard_port) if dashboard_port is not None else self._proxy_port + 1
        )
        self._invalidation_port_explicit = invalidation_port is not None
        self._invalidation_port = (
            int(invalidation_port) if invalidation_port is not None else self._proxy_port + 2
        )

        self._log_level = log_level
        self._mode = mode
        self._license = license
        self._client = client
        self._config_file = config_file
        self._silent = bool(silent)

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
        # Dashboard token — resolved at start() time. When we spawn the proxy
        # ourselves, we generate a random token per-session and pass it via
        # env. When the proxy is externally launched, we read the token from
        # env/file at DDL-call time (see goldlapel/ddl.py).
        self._dashboard_token = None
        # Per-instance contextvar for `with gl.using(conn):` — async-safe, scoped override.
        self._using_conn = ContextVar(f"goldlapel_using_conn_{id(self)}", default=None)

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
        if self._process and self._process.poll() is None:
            return self._proxy_url

        binary = _find_binary()
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
        if self._invalidation_port_explicit:
            cmd += ["--invalidation-port", str(self._invalidation_port)]
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
        cmd += _config_to_args(self._config) + self._extra_args

        _kill_orphan_on_port(self._proxy_port)

        env = os.environ.copy()
        # GOLDLAPEL_CLIENT env var is only set when the user hasn't opted in
        # via the top-level `client` kwarg (which emits --client and takes
        # precedence over the env var).
        if self._client is None:
            env.setdefault("GOLDLAPEL_CLIENT", "python")
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
        self._process = subprocess.Popen(cmd, **popen_kwargs)

        if not _wait_for_port("127.0.0.1", self._proxy_port, _STARTUP_TIMEOUT):
            self._process.kill()
            stderr = self._process.stderr.read().decode(errors="replace")
            self._process.stderr.close()
            raise RuntimeError(
                f"Gold Lapel failed to start on port {self._proxy_port} "
                f"within {_STARTUP_TIMEOUT}s.\nstderr: {stderr}"
            )

        self._process.stderr.close()
        self._proxy_url = _make_proxy_url(self._upstream, self._proxy_port)

        driver_name, driver = _detect_sync_driver()
        # The factory entry point `goldlapel.start(url)` raises ImportError if no
        # driver is available, so in that flow `driver` is always non-None here.
        # This guard protects direct `GoldLapel(...)` construction (a supported
        # public entry point, re-exported from `goldlapel.__init__`), which doesn't
        # pre-check: without a driver we skip opening the internal connection, and
        # the user can still use `gl.url` with their own async/raw driver.
        if driver is not None:
            # If driver.connect() raises (network hiccup, bad creds, auth failure, etc.),
            # the subprocess is already running and would leak. Clean it up before re-raising.
            try:
                if driver_name == "psycopg3":
                    raw_conn = driver.connect(self._proxy_url, autocommit=True)
                else:
                    raw_conn = driver.connect(self._proxy_url)
                from goldlapel.wrap import wrap
                # invalidation_port is resolved at construction: either the
                # explicit kwarg or proxy_port + 2.
                self._conn = wrap(raw_conn, invalidation_port=self._invalidation_port)
            except BaseException:
                # Kill the subprocess we just spawned; leaked running processes = port
                # collisions on retry + zombie resources. BaseException catches KeyboardInterrupt too.
                try:
                    self._process.terminate()
                    try:
                        self._process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        self._process.kill()
                        self._process.wait()
                finally:
                    self._process = None
                    self._proxy_url = None
                raise

        # Startup banner: stderr, not stdout. Library code writing to stdout
        # pollutes app output, CI logs, and anything that captures stdout
        # (pytest -s, subprocess piping). Suppressed entirely when the caller
        # passes `silent=True`.
        if not self._silent:
            if self._dashboard_port:
                banner = (
                    f"goldlapel → :{self._proxy_port} (proxy) | "
                    f"http://127.0.0.1:{self._dashboard_port} (dashboard)"
                )
            else:
                banner = f"goldlapel → :{self._proxy_port} (proxy)"
            print(banner, file=sys.stderr)

        return self._proxy_url

    def stop(self):
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
        if self._process and self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait()
        self._process = None
        self._proxy_url = None
        self._dashboard_token = None
        # Drop ourselves from the registry so the next start(same_url) gets a
        # fresh allocation instead of silently drifting to a new port because
        # _next_port has advanced. Option A from the v0.2 review findings —
        # clean slate on next start. dict.pop is atomic under the GIL, so this
        # is safe without _lock (and reacquiring _lock here would deadlock the
        # bulk stop()/atexit paths that hold it while iterating _instances).
        _instances.pop(self._upstream, None)

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
    def invalidation_port(self):
        """Cache-invalidation port (proxy_port + 2 by default, overridden
        via the `invalidation_port` kwarg)."""
        return self._invalidation_port

    @property
    def dashboard_token(self):
        """Dashboard token used by the DDL API. Resolved on start() when the
        wrapper spawns the proxy itself; None when the proxy is external —
        in that case goldlapel/ddl.py falls back to env/file."""
        return self._dashboard_token

    @property
    def running(self):
        return self._process is not None and self._process.poll() is None

    # -- Document store --------------------------------------------------------

    def doc_create_collection(self, *args, conn=None, **kwargs):
        return _utils().doc_create_collection(self._effective_conn(conn), *args, **kwargs)

    def doc_insert(self, *args, conn=None, **kwargs):
        return _utils().doc_insert(self._effective_conn(conn), *args, **kwargs)

    def doc_insert_many(self, *args, conn=None, **kwargs):
        return _utils().doc_insert_many(self._effective_conn(conn), *args, **kwargs)

    def doc_find(self, *args, conn=None, **kwargs):
        return _utils().doc_find(self._effective_conn(conn), *args, **kwargs)

    def doc_find_one(self, *args, conn=None, **kwargs):
        return _utils().doc_find_one(self._effective_conn(conn), *args, **kwargs)

    def doc_update(self, *args, conn=None, **kwargs):
        return _utils().doc_update(self._effective_conn(conn), *args, **kwargs)

    def doc_update_one(self, *args, conn=None, **kwargs):
        return _utils().doc_update_one(self._effective_conn(conn), *args, **kwargs)

    def doc_delete(self, *args, conn=None, **kwargs):
        return _utils().doc_delete(self._effective_conn(conn), *args, **kwargs)

    def doc_delete_one(self, *args, conn=None, **kwargs):
        return _utils().doc_delete_one(self._effective_conn(conn), *args, **kwargs)

    def doc_find_one_and_update(self, *args, conn=None, **kwargs):
        return _utils().doc_find_one_and_update(self._effective_conn(conn), *args, **kwargs)

    def doc_find_one_and_delete(self, *args, conn=None, **kwargs):
        return _utils().doc_find_one_and_delete(self._effective_conn(conn), *args, **kwargs)

    def doc_distinct(self, *args, conn=None, **kwargs):
        return _utils().doc_distinct(self._effective_conn(conn), *args, **kwargs)

    def doc_find_cursor(self, *args, conn=None, **kwargs):
        return _utils().doc_find_cursor(self._effective_conn(conn), *args, **kwargs)

    def doc_count(self, *args, conn=None, **kwargs):
        return _utils().doc_count(self._effective_conn(conn), *args, **kwargs)

    def doc_create_index(self, *args, conn=None, **kwargs):
        return _utils().doc_create_index(self._effective_conn(conn), *args, **kwargs)

    def doc_aggregate(self, *args, conn=None, **kwargs):
        return _utils().doc_aggregate(self._effective_conn(conn), *args, **kwargs)

    def doc_watch(self, *args, conn=None, **kwargs):
        return _utils().doc_watch(self._effective_conn(conn), *args, **kwargs)

    def doc_unwatch(self, *args, conn=None, **kwargs):
        return _utils().doc_unwatch(self._effective_conn(conn), *args, **kwargs)

    def doc_create_ttl_index(self, *args, conn=None, **kwargs):
        return _utils().doc_create_ttl_index(self._effective_conn(conn), *args, **kwargs)

    def doc_remove_ttl_index(self, *args, conn=None, **kwargs):
        return _utils().doc_remove_ttl_index(self._effective_conn(conn), *args, **kwargs)

    def doc_create_capped(self, *args, conn=None, **kwargs):
        return _utils().doc_create_capped(self._effective_conn(conn), *args, **kwargs)

    def doc_remove_cap(self, *args, conn=None, **kwargs):
        return _utils().doc_remove_cap(self._effective_conn(conn), *args, **kwargs)

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

    # -- Pub/sub & queues ------------------------------------------------------

    def publish(self, *args, conn=None, **kwargs):
        return _utils().publish(self._effective_conn(conn), *args, **kwargs)

    def subscribe(self, *args, conn=None, **kwargs):
        return _utils().subscribe(self._effective_conn(conn), *args, **kwargs)

    def enqueue(self, *args, conn=None, **kwargs):
        return _utils().enqueue(self._effective_conn(conn), *args, **kwargs)

    def dequeue(self, *args, conn=None, **kwargs):
        return _utils().dequeue(self._effective_conn(conn), *args, **kwargs)

    # -- Counters --------------------------------------------------------------

    def incr(self, *args, conn=None, **kwargs):
        return _utils().incr(self._effective_conn(conn), *args, **kwargs)

    def get_counter(self, *args, conn=None, **kwargs):
        return _utils().get_counter(self._effective_conn(conn), *args, **kwargs)

    # -- Hashes ----------------------------------------------------------------

    def hset(self, *args, conn=None, **kwargs):
        return _utils().hset(self._effective_conn(conn), *args, **kwargs)

    def hget(self, *args, conn=None, **kwargs):
        return _utils().hget(self._effective_conn(conn), *args, **kwargs)

    def hgetall(self, *args, conn=None, **kwargs):
        return _utils().hgetall(self._effective_conn(conn), *args, **kwargs)

    def hdel(self, *args, conn=None, **kwargs):
        return _utils().hdel(self._effective_conn(conn), *args, **kwargs)

    # -- Sorted sets -----------------------------------------------------------

    def zadd(self, *args, conn=None, **kwargs):
        return _utils().zadd(self._effective_conn(conn), *args, **kwargs)

    def zincrby(self, *args, conn=None, **kwargs):
        return _utils().zincrby(self._effective_conn(conn), *args, **kwargs)

    def zrange(self, *args, conn=None, **kwargs):
        return _utils().zrange(self._effective_conn(conn), *args, **kwargs)

    def zrank(self, *args, conn=None, **kwargs):
        return _utils().zrank(self._effective_conn(conn), *args, **kwargs)

    def zscore(self, *args, conn=None, **kwargs):
        return _utils().zscore(self._effective_conn(conn), *args, **kwargs)

    def zrem(self, *args, conn=None, **kwargs):
        return _utils().zrem(self._effective_conn(conn), *args, **kwargs)

    # -- Geo -------------------------------------------------------------------

    def georadius(self, *args, conn=None, **kwargs):
        return _utils().georadius(self._effective_conn(conn), *args, **kwargs)

    def geoadd(self, *args, conn=None, **kwargs):
        return _utils().geoadd(self._effective_conn(conn), *args, **kwargs)

    def geodist(self, *args, conn=None, **kwargs):
        return _utils().geodist(self._effective_conn(conn), *args, **kwargs)

    # -- Misc ------------------------------------------------------------------

    def count_distinct(self, *args, conn=None, **kwargs):
        return _utils().count_distinct(self._effective_conn(conn), *args, **kwargs)

    def script(self, *args, conn=None, **kwargs):
        return _utils().script(self._effective_conn(conn), *args, **kwargs)

    # -- Streams ---------------------------------------------------------------

    def _stream_patterns(self, stream):
        """Fetch (and cache) canonical stream DDL + query patterns from the
        proxy's DDL API. The DDL is executed on the proxy side — this call
        only returns the patterns the wrapper should run against the proxy."""
        from goldlapel.utils import _validate_identifier
        _validate_identifier(stream)
        from goldlapel import ddl as _ddl
        token = self._dashboard_token or _ddl.token_from_env_or_file()
        return _ddl.fetch_patterns(self, "stream", stream, self._dashboard_port, token)

    def stream_add(self, stream, payload, *, conn=None):
        patterns = self._stream_patterns(stream)
        return _utils().stream_add(self._effective_conn(conn), stream, payload, patterns=patterns)

    def stream_create_group(self, stream, group, *, conn=None):
        patterns = self._stream_patterns(stream)
        return _utils().stream_create_group(
            self._effective_conn(conn), stream, group, patterns=patterns
        )

    def stream_read(self, stream, group, consumer, count=1, *, conn=None):
        patterns = self._stream_patterns(stream)
        return _utils().stream_read(
            self._effective_conn(conn), stream, group, consumer, count, patterns=patterns
        )

    def stream_ack(self, stream, group, message_id, *, conn=None):
        patterns = self._stream_patterns(stream)
        return _utils().stream_ack(
            self._effective_conn(conn), stream, group, message_id, patterns=patterns
        )

    def stream_claim(self, stream, group, consumer, min_idle_ms=60000, *, conn=None):
        patterns = self._stream_patterns(stream)
        return _utils().stream_claim(
            self._effective_conn(conn),
            stream,
            group,
            consumer,
            min_idle_ms,
            patterns=patterns,
        )

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


def _ensure_running(
    upstream,
    *,
    proxy_port=None,
    dashboard_port=None,
    invalidation_port=None,
    log_level=None,
    mode=None,
    license=None,
    client=None,
    config_file=None,
    config=None,
    extra_args=None,
    silent=False,
):
    global _cleanup_registered, _next_port
    with _lock:
        if upstream in _instances:
            inst = _instances[upstream]
            if inst.running:
                return inst
            del _instances[upstream]

        if proxy_port is None:
            proxy_port = _next_port
        if proxy_port >= _next_port:
            _next_port = proxy_port + 1

        inst = GoldLapel(
            upstream,
            proxy_port=proxy_port,
            dashboard_port=dashboard_port,
            invalidation_port=invalidation_port,
            log_level=log_level,
            mode=mode,
            license=license,
            client=client,
            config_file=config_file,
            config=config,
            extra_args=extra_args,
            silent=silent,
        )
        _instances[upstream] = inst
        if not _cleanup_registered:
            atexit.register(_cleanup)
            _cleanup_registered = True
    try:
        inst.start()
        return inst
    except Exception:
        with _lock:
            _instances.pop(upstream, None)
        raise


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
    invalidation_port=None,
    log_level=None,
    mode=None,
    license=None,
    client=None,
    config_file=None,
    config=None,
    extra_args=None,
    silent=False,
):
    """Factory: spawn a Gold Lapel proxy in front of `upstream` and return a
    GoldLapel instance. Call wrapper methods on the returned instance
    (e.g. `gl.search(...)`), or use `gl.url` with your own Postgres driver.

    Eager: opens the instance's internal DB connection before returning so the
    first wrapper method call is fast. Requires a sync Postgres driver
    installed (psycopg2 or psycopg3) — raises ImportError otherwise.

    Top-level kwargs match the canonical config surface shared across every
    Gold Lapel wrapper:

    - proxy_port: proxy listen port (default 7932)
    - dashboard_port: dashboard port (derived as proxy_port + 1 when unset; 0 disables)
    - invalidation_port: cache-invalidation port (derived as proxy_port + 2)
    - log_level: one of 'trace', 'debug', 'info', 'warn', 'error'
    - mode: proxy operating mode ('waiter', 'bellhop', ...)
    - license: path to the license file
    - client: client identifier for telemetry tagging (sets GOLDLAPEL_CLIENT)
    - config_file: path to a TOML config file (passed as --config to the binary)
    - config: dict of tuning knobs (pool_size, disable_*, replica, ...)
    - extra_args: raw CLI flags appended to the binary invocation
    - silent: suppress the startup banner

    Promoted top-level concepts are rejected inside the `config` dict.

    Usage:
        gl = goldlapel.start("postgresql://user:pass@db/mydb")
        gl.search("articles", "body", "postgres")
        conn = psycopg2.connect(gl.url)    # raw driver usage still supported

    Context manager usage:
        with goldlapel.start("postgresql://...") as gl:
            gl.search(...)
        # proxy stopped automatically on exit
    """
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
        invalidation_port=invalidation_port,
        log_level=log_level,
        mode=mode,
        license=license,
        client=client,
        config_file=config_file,
        config=config,
        extra_args=extra_args,
        silent=silent,
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
        conn = driver.connect(inst.url, autocommit=True)
    else:
        conn = driver.connect(inst.url)
    from goldlapel.wrap import wrap
    return wrap(conn, invalidation_port=inst.invalidation_port)


def stop(upstream=None):
    with _lock:
        if upstream is not None:
            inst = _instances.pop(upstream, None)
            if inst:
                inst.stop()
        else:
            # Snapshot values — inst.stop() pops itself from _instances, so
            # iterating the live view would raise "dict changed size during
            # iteration".
            for inst in list(_instances.values()):
                inst.stop()
            _instances.clear()


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
    with _lock:
        # Snapshot values — inst.stop() pops itself from _instances, so
        # iterating the live view would raise during shutdown.
        for inst in list(_instances.values()):
            inst.stop()
        _instances.clear()
