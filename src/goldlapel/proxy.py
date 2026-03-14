import atexit
import os
import platform
import re
import shutil
import socket
import subprocess
import time
from pathlib import Path


DEFAULT_PORT = 7932
_STARTUP_TIMEOUT = 10.0
_STARTUP_POLL_INTERVAL = 0.05

_VALID_CONFIG_KEYS = frozenset({
    "mode", "min_pattern_count", "refresh_interval_secs", "pattern_ttl_secs",
    "max_tables_per_view", "max_columns_per_view", "deep_pagination_threshold",
    "report_interval_secs", "result_cache_size", "batch_cache_size",
    "batch_cache_ttl_secs", "redis_url", "pool_size", "pool_timeout_secs",
    "pool_mode", "mgmt_idle_timeout", "fallback", "read_after_write_secs",
    "n1_threshold", "n1_window_ms", "n1_cross_threshold",
    "tls_cert", "tls_key", "tls_client_ca", "config", "dashboard_port",
    "disable_matviews", "disable_consolidation", "disable_btree_indexes",
    "disable_trigram_indexes", "disable_expression_indexes",
    "disable_partial_indexes", "disable_rewrite", "disable_prepared_cache",
    "disable_result_cache", "disable_redis_cache", "disable_pool",
    "disable_n1", "disable_n1_cross_connection", "disable_shadow_mode",
    "enable_coalescing", "replica", "exclude_tables",
})

_BOOLEAN_KEYS = frozenset({
    "disable_matviews", "disable_consolidation", "disable_btree_indexes",
    "disable_trigram_indexes", "disable_expression_indexes",
    "disable_partial_indexes", "disable_rewrite", "disable_prepared_cache",
    "disable_result_cache", "disable_redis_cache", "disable_pool",
    "disable_n1", "disable_n1_cross_connection", "disable_shadow_mode",
    "enable_coalescing",
})

_LIST_KEYS = frozenset({
    "replica", "exclude_tables",
})

_instance = None
_cleanup_registered = False


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
    m = re.match(r'^(postgres(?:ql)?://(?:.*@)?)([^:/?#]+):(\d+)(.*)$', upstream)
    if m:
        return f"{m.group(1)}localhost:{port}{m.group(4)}"

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


class GoldLapel:
    def __init__(self, upstream, config=None, port=None, extra_args=None):
        self._upstream = upstream
        self._port = port if port is not None else DEFAULT_PORT
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

        self._process = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

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
    def running(self):
        return self._process is not None and self._process.poll() is None


def start(upstream, config=None, port=None, extra_args=None):
    global _instance, _cleanup_registered
    if _instance and _instance.running:
        if _instance._upstream != upstream:
            raise RuntimeError(
                "Gold Lapel is already running for a different upstream. "
                "Call goldlapel.stop() before starting with a new upstream."
            )
        return _instance.url
    _instance = GoldLapel(upstream, port=port, config=config, extra_args=extra_args)
    if not _cleanup_registered:
        atexit.register(_cleanup)
        _cleanup_registered = True
    return _instance.start()


def stop():
    global _instance
    if _instance:
        _instance.stop()
        _instance = None


def proxy_url():
    if _instance:
        return _instance.url
    return None


def config_keys():
    return set(_VALID_CONFIG_KEYS)


def _cleanup():
    global _instance
    if _instance:
        _instance.stop()
        _instance = None
