import os
import platform
from pathlib import Path
from unittest.mock import patch

import pytest

from goldlapel.proxy import (
    _find_binary,
    _make_proxy_url,
    _wait_for_port,
    GoldLapel,
    start,
    stop,
    proxy_url,
)


class TestFindBinary:
    def test_env_var_override(self, tmp_path):
        binary = tmp_path / "goldlapel"
        binary.touch()
        with patch.dict(os.environ, {"GOLDLAPEL_BINARY": str(binary)}):
            assert _find_binary() == str(binary)

    def test_env_var_missing_file(self):
        with patch.dict(os.environ, {"GOLDLAPEL_BINARY": "/nonexistent/goldlapel"}):
            with pytest.raises(FileNotFoundError, match="GOLDLAPEL_BINARY"):
                _find_binary()

    def test_bundled_binary(self, tmp_path):
        system = platform.system().lower()
        machine = platform.machine().lower()
        if machine in ("x86_64", "amd64"):
            arch = "x86_64"
        elif machine in ("arm64", "aarch64"):
            arch = "aarch64"
        else:
            arch = machine

        binary_name = f"goldlapel-{system}-{arch}"
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        binary = bin_dir / binary_name
        binary.touch()

        fake_module = str(tmp_path / "proxy.py")
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GOLDLAPEL_BINARY", None)
            with patch("goldlapel.proxy.__file__", fake_module):
                assert _find_binary() == str(binary)

    def test_not_found_raises(self, tmp_path):
        fake_module = str(tmp_path / "proxy.py")
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GOLDLAPEL_BINARY", None)
            with patch("goldlapel.proxy.__file__", fake_module), \
                 patch("goldlapel.proxy.shutil.which", return_value=None):
                with pytest.raises(FileNotFoundError, match="Gold Lapel binary not found"):
                    _find_binary()


class TestMakeProxyUrl:
    def test_postgresql_url(self):
        url = "postgresql://user:pass@dbhost:5432/mydb"
        assert _make_proxy_url(url, 7932) == "postgresql://user:pass@localhost:7932/mydb"

    def test_postgres_url(self):
        url = "postgres://user:pass@remote.aws.com:5432/mydb"
        assert _make_proxy_url(url, 7932) == "postgres://user:pass@localhost:7932/mydb"

    def test_pg_url_without_port(self):
        url = "postgresql://user:pass@host.aws.com/mydb"
        assert _make_proxy_url(url, 7932) == "postgresql://user:pass@localhost:7932/mydb"

    def test_pg_url_without_port_or_path(self):
        url = "postgresql://user:pass@host.aws.com"
        assert _make_proxy_url(url, 7932) == "postgresql://user:pass@localhost:7932"

    def test_bare_host_port(self):
        assert _make_proxy_url("dbhost:5432", 7932) == "localhost:7932"

    def test_host_only(self):
        assert _make_proxy_url("dbhost", 7932) == "localhost:7932"

    def test_preserves_params(self):
        url = "postgresql://user:pass@remote:5432/mydb?sslmode=require"
        assert _make_proxy_url(url, 7932) == "postgresql://user:pass@localhost:7932/mydb?sslmode=require"

    def test_preserves_percent_encoded_password(self):
        url = "postgresql://user:p%40ss@remote:5432/mydb"
        assert _make_proxy_url(url, 7932) == "postgresql://user:p%40ss@localhost:7932/mydb"

    def test_no_userinfo(self):
        url = "postgresql://dbhost:5432/mydb"
        assert _make_proxy_url(url, 7932) == "postgresql://localhost:7932/mydb"

    def test_no_userinfo_no_port(self):
        url = "postgresql://dbhost/mydb"
        assert _make_proxy_url(url, 7932) == "postgresql://localhost:7932/mydb"

    def test_localhost_stays_localhost(self):
        url = "postgresql://user:pass@localhost:5432/mydb"
        assert _make_proxy_url(url, 7932) == "postgresql://user:pass@localhost:7932/mydb"

    def test_at_sign_in_password_with_port(self):
        url = "postgresql://user:p@ss@host:5432/mydb"
        assert _make_proxy_url(url, 7932) == "postgresql://user:p@ss@localhost:7932/mydb"

    def test_at_sign_in_password_without_port(self):
        url = "postgresql://user:p@ss@host/mydb"
        assert _make_proxy_url(url, 7932) == "postgresql://user:p@ss@localhost:7932/mydb"

    def test_at_sign_in_password_with_query_params(self):
        url = "postgresql://user:p@ss@host:5432/mydb?sslmode=require&param=val@ue"
        assert _make_proxy_url(url, 7932) == "postgresql://user:p@ss@localhost:7932/mydb?sslmode=require&param=val@ue"


class TestWaitForPort:
    def test_open_port(self):
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        port = sock.getsockname()[1]
        try:
            assert _wait_for_port("127.0.0.1", port, timeout=1.0) is True
        finally:
            sock.close()

    def test_closed_port_timeout(self):
        assert _wait_for_port("127.0.0.1", 19999, timeout=0.2) is False


class TestGoldLapelClass:
    def test_default_port(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        assert gl._port == 7932

    def test_custom_port(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb", port=9000)
        assert gl._port == 9000

    def test_port_zero(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb", port=0)
        assert gl._port == 0

    def test_not_running_initially(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        assert gl.running is False
        assert gl.url is None


class TestModuleFunctions:
    def test_proxy_url_none_when_not_started(self):
        stop()
        assert proxy_url() is None
