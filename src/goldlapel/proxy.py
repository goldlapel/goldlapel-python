import atexit
import os
import platform
import re
import shutil
import socket
import subprocess
import time
from pathlib import Path


_DEFAULT_PORT = 7932
_STARTUP_TIMEOUT = 10.0
_STARTUP_POLL_INTERVAL = 0.05

_instance = None
_cleanup_registered = False


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
    def __init__(self, upstream, port=None, extra_args=None):
        self._upstream = upstream
        self._port = port if port is not None else _DEFAULT_PORT
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
        ] + self._extra_args

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


def start(upstream, port=None, extra_args=None):
    global _instance, _cleanup_registered
    if _instance and _instance.running:
        return _instance.url
    _instance = GoldLapel(upstream, port=port, extra_args=extra_args)
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


def _cleanup():
    global _instance
    if _instance:
        _instance.stop()
        _instance = None
