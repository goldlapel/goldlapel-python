import os
import platform
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import goldlapel.proxy as proxy_mod
from goldlapel.proxy import (
    _config_to_args,
    _find_binary,
    _make_proxy_url,
    _wait_for_port,
    DEFAULT_PORT,
    GoldLapel,
    config_keys,
    dashboard_url,
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

    def test_password_starting_with_digit_with_port(self):
        url = "postgresql://user:9password@host:5432/mydb"
        assert _make_proxy_url(url, 7932) == "postgresql://user:9password@localhost:7932/mydb"

    def test_password_starting_with_digit_without_port(self):
        url = "postgresql://user:9password@host/mydb"
        assert _make_proxy_url(url, 7932) == "postgresql://user:9password@localhost:7932/mydb"

    def test_password_all_digits_without_port(self):
        url = "postgresql://user:123456@host/mydb"
        assert _make_proxy_url(url, 7932) == "postgresql://user:123456@localhost:7932/mydb"

    def test_password_all_digits_with_port(self):
        url = "postgresql://user:123456@host:5432/mydb"
        assert _make_proxy_url(url, 7932) == "postgresql://user:123456@localhost:7932/mydb"

    def test_password_starting_with_digit_no_path(self):
        url = "postgresql://user:9secret@host"
        assert _make_proxy_url(url, 7932) == "postgresql://user:9secret@localhost:7932"


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


class TestDashboardUrl:
    def test_dashboard_url_default(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        assert gl._dashboard_port == 7933

    def test_dashboard_url_custom_port(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb", config={"dashboard_port": 8080})
        assert gl._dashboard_port == 8080

    def test_dashboard_url_disabled(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb", config={"dashboard_port": 0})
        assert gl._dashboard_port == 0
        assert gl.dashboard_url is None

    def test_dashboard_url_not_running(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        assert gl.dashboard_url is None

    def test_dashboard_port_from_config(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb", config={"dashboard_port": "9090"})
        assert gl._dashboard_port == 9090


class TestConfigToArgs:
    def test_string_value(self):
        assert _config_to_args({"mode": "waiter"}) == ["--mode", "waiter"]

    def test_numeric_value(self):
        assert _config_to_args({"pool_size": 50}) == ["--pool-size", "50"]

    def test_boolean_true(self):
        assert _config_to_args({"disable_matviews": True}) == ["--disable-matviews"]

    def test_boolean_false(self):
        assert _config_to_args({"disable_matviews": False}) == []

    def test_list_value(self):
        result = _config_to_args({"replica": ["url1", "url2"]})
        assert result == ["--replica", "url1", "--replica", "url2"]

    def test_exclude_tables_list(self):
        result = _config_to_args({"exclude_tables": ["users", "sessions"]})
        assert result == ["--exclude-tables", "users", "--exclude-tables", "sessions"]

    def test_unknown_key_raises(self):
        with pytest.raises(ValueError, match="Unknown config keys"):
            _config_to_args({"bogus": 1})

    def test_multiple_keys(self):
        result = _config_to_args({"mode": "waiter", "pool_size": 10, "disable_pool": True})
        assert "--mode" in result
        assert "waiter" in result
        assert "--pool-size" in result
        assert "10" in result
        assert "--disable-pool" in result

    def test_empty_config(self):
        assert _config_to_args({}) == []

    def test_none_config(self):
        assert _config_to_args(None) == []

    def test_boolean_non_bool_raises(self):
        with pytest.raises(TypeError, match="expects a bool"):
            _config_to_args({"disable_pool": "yes"})

    def test_list_key_given_string_wraps_to_list(self):
        result = _config_to_args({"replica": "postgresql://replica:5432/mydb"})
        assert result == ["--replica", "postgresql://replica:5432/mydb"]

    def test_exclude_tables_given_string_wraps_to_list(self):
        result = _config_to_args({"exclude_tables": "users"})
        assert result == ["--exclude-tables", "users"]

    def test_list_key_given_non_list_non_string_raises(self):
        with pytest.raises(TypeError, match="expects a list"):
            _config_to_args({"replica": 42})

    def test_config_with_constructor(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb", config={"mode": "waiter"})
        assert gl._config == {"mode": "waiter"}


class TestConfigKeys:
    def test_config_keys_returns_all_keys(self):
        keys = config_keys()
        assert isinstance(keys, set)
        assert "mode" in keys
        assert "pool_size" in keys
        assert len(keys) == 42


class TestModuleFunctions:
    def test_proxy_url_none_when_not_started(self):
        stop()
        assert proxy_url() is None

    def test_dashboard_url_none_when_not_started(self):
        stop()
        assert dashboard_url() is None


def _reset_module_state():
    proxy_mod._instances.clear()
    proxy_mod._next_port = DEFAULT_PORT


def _mock_popen():
    proc = MagicMock()
    proc.poll.return_value = None  # process is "running"
    proc.stderr = MagicMock()
    return proc


def _mock_driver():
    mock_mod = MagicMock()
    mock_conn = MagicMock()
    mock_mod.connect.return_value = mock_conn
    return "psycopg3", mock_mod


class TestMultiInstance:
    def setup_method(self):
        _reset_module_state()

    def teardown_method(self):
        _reset_module_state()

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_two_upstreams_get_different_ports(self, mock_find, mock_popen, mock_wait, mock_detect, mock_wrap):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        url_a = "postgresql://host-a:5432/db_a"
        url_b = "postgresql://host-b:5432/db_b"

        start(url_a)
        start(url_b)

        assert len(proxy_mod._instances) == 2
        ports = [inst._port for inst in proxy_mod._instances.values()]
        assert 7932 in ports
        assert 7933 in ports

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._kill_orphan_on_port")
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_same_upstream_returns_existing(self, mock_find, mock_popen, mock_wait, mock_orphan, mock_detect, mock_wrap):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        url = "postgresql://host:5432/mydb"
        start(url)
        start(url)

        assert len(proxy_mod._instances) == 1
        assert mock_popen.call_count == 1  # Only spawned once

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_stop_specific_upstream(self, mock_find, mock_popen, mock_wait, mock_detect, mock_wrap):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        url_a = "postgresql://host-a:5432/db_a"
        url_b = "postgresql://host-b:5432/db_b"

        start(url_a)
        start(url_b)

        stop(url_a)
        assert len(proxy_mod._instances) == 1
        assert url_a not in proxy_mod._instances
        assert url_b in proxy_mod._instances

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_stop_all(self, mock_find, mock_popen, mock_wait, mock_detect, mock_wrap):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host-a:5432/db_a")
        start("postgresql://host-b:5432/db_b")

        stop()
        assert len(proxy_mod._instances) == 0

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_proxy_url_single_instance(self, mock_find, mock_popen, mock_wait, mock_detect, mock_wrap):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        url = "postgresql://host:5432/mydb"
        start(url)
        purl = proxy_url()
        assert purl is not None
        assert "7932" in purl

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_proxy_url_multi_instance_requires_upstream(self, mock_find, mock_popen, mock_wait, mock_detect, mock_wrap):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        url_a = "postgresql://host-a:5432/db_a"
        url_b = "postgresql://host-b:5432/db_b"
        start(url_a)
        start(url_b)

        # Without upstream arg, should raise
        with pytest.raises(RuntimeError, match="Multiple Gold Lapel instances"):
            proxy_url()

        # With upstream arg, should return the correct URL
        assert proxy_url(url_a) is not None
        assert proxy_url(url_b) is not None

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_dashboard_url_multi_instance_requires_upstream(self, mock_find, mock_popen, mock_wait, mock_detect, mock_wrap):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        url_a = "postgresql://host-a:5432/db_a"
        url_b = "postgresql://host-b:5432/db_b"
        start(url_a)
        start(url_b)

        with pytest.raises(RuntimeError, match="Multiple Gold Lapel instances"):
            dashboard_url()

        # With upstream arg, should return the dashboard URL
        assert dashboard_url(url_a) is not None
        assert dashboard_url(url_b) is not None

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_explicit_port_advances_next_port(self, mock_find, mock_popen, mock_wait, mock_detect, mock_wrap):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        url_a = "postgresql://host-a:5432/db_a"
        url_b = "postgresql://host-b:5432/db_b"

        start(url_a, port=8000)
        start(url_b)  # Should auto-assign 8001, not 7932

        inst_b = proxy_mod._instances[url_b]
        assert inst_b._port == 8001

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_proxy_url_unknown_upstream(self, mock_find, mock_popen, mock_wait, mock_detect, mock_wrap):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host:5432/mydb")
        assert proxy_url("postgresql://unknown:5432/nope") is None

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._kill_orphan_on_port")
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_dead_instance_gets_recreated(self, mock_find, mock_popen, mock_wait, mock_orphan, mock_detect, mock_wrap):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        url = "postgresql://host:5432/mydb"
        start(url)

        # Simulate process dying
        inst = proxy_mod._instances[url]
        inst._process.poll.return_value = 1  # non-None = exited

        # Starting again should recreate
        proxy_2 = start(url)
        assert proxy_2 is not None
        assert mock_popen.call_count == 2

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_cleanup_stops_all(self, mock_find, mock_popen, mock_wait, mock_detect, mock_wrap):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host-a:5432/db_a")
        start("postgresql://host-b:5432/db_b")

        from goldlapel.proxy import _cleanup
        _cleanup()

        assert len(proxy_mod._instances) == 0

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=False)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_failed_start_cleans_up_instance(self, mock_find, mock_popen, mock_wait, mock_detect, mock_wrap):
        proc = _mock_popen()
        proc.stderr.read.return_value = b"bind error"
        mock_popen.return_value = proc

        url = "postgresql://host:5432/mydb"
        with pytest.raises(RuntimeError, match="failed to start"):
            start(url)

        assert url not in proxy_mod._instances

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_stop_nonexistent_upstream_is_noop(self, mock_find, mock_popen, mock_wait, mock_detect, mock_wrap):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host:5432/mydb")
        stop("postgresql://nonexistent:5432/nope")  # Should not raise
        assert len(proxy_mod._instances) == 1

    @patch("goldlapel.proxy._detect_sync_driver", return_value=(None, None))
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_start_raises_when_no_driver(self, mock_find, mock_popen, mock_wait, mock_detect):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        with pytest.raises(ImportError, match="sync Postgres driver"):
            start("postgresql://host:5432/mydb")
