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
from pathlib import Path


DEFAULT_PORT = 7932
DEFAULT_DASHBOARD_PORT = 7933
_STARTUP_TIMEOUT = 10.0
_STARTUP_POLL_INTERVAL = 0.05

_VALID_CONFIG_KEYS = frozenset({
    "mode", "min_pattern_count", "refresh_interval_secs", "pattern_ttl_secs",
    "max_tables_per_view", "max_columns_per_view", "deep_pagination_threshold",
    "report_interval_secs", "result_cache_size", "batch_cache_size",
    "batch_cache_ttl_secs", "pool_size", "pool_timeout_secs",
    "pool_mode", "mgmt_idle_timeout", "fallback", "read_after_write_secs",
    "n1_threshold", "n1_window_ms", "n1_cross_threshold",
    "tls_cert", "tls_key", "tls_client_ca", "config", "dashboard_port",
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

_instances = {}
_cleanup_registered = False
_lock = threading.Lock()
_next_port = DEFAULT_PORT


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


def _find_binary():
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

    # 3. On PATH
    on_path = shutil.which("goldlapel")
    if on_path:
        return on_path

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
    def __init__(self, upstream, config=None, port=None, extra_args=None):
        self._upstream = upstream
        self._port = port if port is not None else DEFAULT_PORT
        self._dashboard_port = int(config.get("dashboard_port", DEFAULT_DASHBOARD_PORT)) if config else DEFAULT_DASHBOARD_PORT
        self._config = config
        self._extra_args = extra_args or []
        self._process = None
        self._proxy_url = None

    def start(self):
        if self._process and self._process.poll() is None:
            return self._proxy_url

        binary = _find_binary()
        cmd = [
            binary,
            "--upstream", self._upstream,
            "--port", str(self._port),
        ] + _config_to_args(self._config) + self._extra_args

        _kill_orphan_on_port(self._port)

        env = os.environ.copy()
        env.setdefault("GOLDLAPEL_CLIENT", "python")
        popen_kwargs = dict(
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        if sys.platform == "linux":
            popen_kwargs["preexec_fn"] = _set_pdeathsig
        self._process = subprocess.Popen(cmd, **popen_kwargs)

        if not _wait_for_port("127.0.0.1", self._port, _STARTUP_TIMEOUT):
            self._process.kill()
            stderr = self._process.stderr.read().decode(errors="replace")
            self._process.stderr.close()
            raise RuntimeError(
                f"Gold Lapel failed to start on port {self._port} "
                f"within {_STARTUP_TIMEOUT}s.\nstderr: {stderr}"
            )

        self._process.stderr.close()
        self._proxy_url = _make_proxy_url(self._upstream, self._port)

        if self._dashboard_port:
            print(f"goldlapel → :{self._port} (proxy) | http://127.0.0.1:{self._dashboard_port} (dashboard)")
        else:
            print(f"goldlapel → :{self._port} (proxy)")

        return self._proxy_url

    def stop(self):
        if self._process and self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait()
        self._process = None
        self._proxy_url = None

    @property
    def url(self):
        return self._proxy_url

    @property
    def dashboard_url(self):
        if self._dashboard_port and self._process and self._process.poll() is None:
            return f"http://127.0.0.1:{self._dashboard_port}"
        return None

    @property
    def running(self):
        return self._process is not None and self._process.poll() is None


def start(upstream, config=None, port=None, extra_args=None):
    global _cleanup_registered, _next_port
    with _lock:
        if upstream in _instances:
            inst = _instances[upstream]
            if inst.running:
                return inst.url
            # Dead instance -- remove and recreate below
            del _instances[upstream]

        if port is None:
            port = _next_port
        # Always advance _next_port past the port being used
        if port >= _next_port:
            _next_port = port + 1

        inst = GoldLapel(upstream, port=port, config=config, extra_args=extra_args)
        _instances[upstream] = inst
        if not _cleanup_registered:
            atexit.register(_cleanup)
            _cleanup_registered = True
    try:
        return inst.start()
    except Exception:
        with _lock:
            _instances.pop(upstream, None)
        raise


def stop(upstream=None):
    with _lock:
        if upstream is not None:
            inst = _instances.pop(upstream, None)
            if inst:
                inst.stop()
        else:
            for inst in _instances.values():
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
        for inst in _instances.values():
            inst.stop()
        _instances.clear()
