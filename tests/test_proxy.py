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
    DEFAULT_PROXY_PORT,
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
        empty_path = str(tmp_path / "empty-path-dir")
        Path(empty_path).mkdir()
        with patch.dict(os.environ, {"PATH": empty_path}, clear=False):
            os.environ.pop("GOLDLAPEL_BINARY", None)
            with patch("goldlapel.proxy.__file__", fake_module):
                with pytest.raises(FileNotFoundError, match="Gold Lapel binary not found"):
                    _find_binary()

    def test_skips_python_shim_on_path(self, tmp_path):
        # Regression test for TODO 04: pip-installed `[project.scripts]` shim at
        # .venv/bin/goldlapel would shadow the real Rust binary in dev installs.
        # `_find_binary()` must skip the Python shim and find the real binary
        # further down PATH.
        shim_dir = tmp_path / "shim"
        shim_dir.mkdir()
        shim = shim_dir / "goldlapel"
        shim.write_text("#!/usr/bin/env python\nimport sys\nsys.exit(0)\n")
        shim.chmod(0o755)

        real_dir = tmp_path / "real"
        real_dir.mkdir()
        real_binary = real_dir / "goldlapel"
        # Real Rust binary — starts with ELF magic, no shebang, just needs to be
        # an executable file that isn't a Python script.
        real_binary.write_bytes(b"\x7fELF\x02\x01\x01\x00" + b"\x00" * 56)
        real_binary.chmod(0o755)

        fake_module = str(tmp_path / "proxy.py")
        # Shim is first on PATH; real binary is second. Without the fix, the
        # shim would be returned.
        patched_path = os.pathsep.join([str(shim_dir), str(real_dir)])
        with patch.dict(os.environ, {"PATH": patched_path}, clear=False):
            os.environ.pop("GOLDLAPEL_BINARY", None)
            with patch("goldlapel.proxy.__file__", fake_module):
                result = _find_binary()
                assert result == str(real_binary), \
                    f"Expected real binary {real_binary}, got {result} (shim was not skipped)"

    def test_raises_when_only_python_shim_on_path(self, tmp_path):
        # If the only candidate on PATH is a Python shim, _find_binary must
        # raise the "binary not found" error rather than returning the shim.
        shim_dir = tmp_path / "shim"
        shim_dir.mkdir()
        shim = shim_dir / "goldlapel"
        shim.write_text("#!/usr/bin/env python\nimport sys\nsys.exit(0)\n")
        shim.chmod(0o755)

        fake_module = str(tmp_path / "proxy.py")
        with patch.dict(os.environ, {"PATH": str(shim_dir)}, clear=False):
            os.environ.pop("GOLDLAPEL_BINARY", None)
            with patch("goldlapel.proxy.__file__", fake_module):
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
        assert gl._proxy_port == 7932

    def test_custom_port(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb", proxy_port=9000)
        assert gl._proxy_port == 9000

    def test_port_zero(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb", proxy_port=0)
        assert gl._proxy_port == 0

    def test_not_running_initially(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        assert gl.running is False
        assert gl.url is None


class TestDashboardUrl:
    def test_dashboard_url_default(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        assert gl._dashboard_port == 7933

    def test_dashboard_url_custom_port(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb", dashboard_port=8080)
        assert gl._dashboard_port == 8080

    def test_dashboard_url_disabled(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb", dashboard_port=0)
        assert gl._dashboard_port == 0
        assert gl.dashboard_url is None

    def test_dashboard_url_not_running(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        assert gl.dashboard_url is None

    def test_dashboard_port_in_config_map_rejected(self):
        # Regression guard: dashboard_port was promoted to a top-level kwarg.
        # Passing it inside the `config` dict must raise.
        with pytest.raises(ValueError, match="Unknown config keys"):
            GoldLapel("postgresql://localhost:5432/mydb", config={"dashboard_port": 9090})

    def test_dashboard_port_derives_from_custom_proxy_port(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb", proxy_port=17932)
        assert gl._dashboard_port == 17933

    def test_explicit_dashboard_port_overrides_derivation(self):
        gl = GoldLapel(
            "postgresql://localhost:5432/mydb",
            proxy_port=17932,
            dashboard_port=9999,
        )
        assert gl._dashboard_port == 9999

    def test_invalidation_port_derives_from_custom_proxy_port(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb", proxy_port=17932)
        assert gl.invalidation_port == 17934

    def test_explicit_invalidation_port_overrides_derivation(self):
        gl = GoldLapel(
            "postgresql://localhost:5432/mydb",
            proxy_port=17932,
            invalidation_port=9999,
        )
        assert gl.invalidation_port == 9999


class TestConfigToArgs:
    def test_string_value(self):
        assert _config_to_args({"pool_mode": "transaction"}) == ["--pool-mode", "transaction"]

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
        result = _config_to_args({"pool_mode": "transaction", "pool_size": 10, "disable_pool": True})
        assert "--pool-mode" in result
        assert "transaction" in result
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

    def test_log_level_in_config_map_rejected(self):
        # Regression guard: log_level was promoted to a top-level option.
        # Passing it through config must raise.
        with pytest.raises(ValueError, match="Unknown config keys"):
            _config_to_args({"log_level": "info"})

    def test_mode_in_config_map_rejected(self):
        # Regression guard: mode was promoted to a top-level option.
        with pytest.raises(ValueError, match="Unknown config keys"):
            _config_to_args({"mode": "waiter"})

    def test_silent_in_config_map_rejected(self):
        # Regression guard: silent was promoted to a top-level option.
        with pytest.raises(ValueError, match="Unknown config keys"):
            _config_to_args({"silent": True})

    def test_log_level_to_verbose_flag(self):
        from goldlapel.proxy import _log_level_to_verbose_flag
        assert _log_level_to_verbose_flag("trace") == "-vvv"
        assert _log_level_to_verbose_flag("debug") == "-vv"
        assert _log_level_to_verbose_flag("info") == "-v"
        assert _log_level_to_verbose_flag("warn") is None
        assert _log_level_to_verbose_flag("error") is None
        assert _log_level_to_verbose_flag(None) is None
        assert _log_level_to_verbose_flag("DEBUG") == "-vv"

    def test_log_level_to_verbose_flag_non_string_raises(self):
        from goldlapel.proxy import _log_level_to_verbose_flag
        with pytest.raises(TypeError, match="expects a string"):
            _log_level_to_verbose_flag(2)

    def test_log_level_to_verbose_flag_invalid_raises(self):
        from goldlapel.proxy import _log_level_to_verbose_flag
        with pytest.raises(ValueError, match="log_level must be one of"):
            _log_level_to_verbose_flag("verbose")

    def test_config_with_constructor(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb", config={"pool_mode": "transaction"})
        assert gl._config == {"pool_mode": "transaction"}


class TestConfigKeys:
    def test_config_keys_returns_all_keys(self):
        # Tuning knobs still live in the structured config map.
        keys = config_keys()
        assert isinstance(keys, set)
        assert "pool_size" in keys
        assert "disable_matviews" in keys
        assert "replica" in keys

    def test_config_keys_does_not_contain_promoted_top_level_keys(self):
        # Top-level concepts (mode, log_level, dashboard_port, etc.) were
        # promoted out of the structured config map on the canonical surface.
        keys = config_keys()
        for promoted in (
            "mode", "log_level", "dashboard_port", "invalidation_port",
            "config", "license", "client", "silent",
        ):
            assert promoted not in keys


class TestModuleFunctions:
    def test_proxy_url_none_when_not_started(self):
        stop()
        assert proxy_url() is None

    def test_dashboard_url_none_when_not_started(self):
        stop()
        assert dashboard_url() is None


def _reset_module_state():
    proxy_mod._instances.clear()
    proxy_mod._next_port = DEFAULT_PROXY_PORT


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
        ports = [inst._proxy_port for inst in proxy_mod._instances.values()]
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

        start(url_a, proxy_port=8000)
        start(url_b)  # Should auto-assign 8001, not 7932

        inst_b = proxy_mod._instances[url_b]
        assert inst_b._proxy_port == 8001

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

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._kill_orphan_on_port")
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_instance_stop_removes_from_registry(
        self, mock_find, mock_popen, mock_wait, mock_orphan, mock_detect, mock_wrap,
    ):
        # Regression for v0.2 review finding (MEDIUM, Option A): after
        # gl.stop(), the _instances entry must be dropped so the next
        # start(same_url) doesn't get a stale entry (and silently land on a
        # different port because _next_port has advanced).
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        url = "postgresql://host:5432/mydb"
        gl = start(url)
        assert url in proxy_mod._instances

        gl.stop()
        assert url not in proxy_mod._instances, \
            "gl.stop() must remove itself from _instances"

        # Start a second, different upstream so _next_port advances; then
        # the re-start of `url` should get the freshly allocated port
        # (7934), not the stale 7932. The key invariant is: the port is
        # *newly* allocated — no silent reuse of the stale entry.
        start("postgresql://other:5432/other_db")
        gl2 = start(url)
        assert gl2._proxy_port != 7932, \
            f"restart after stop silently reused stale port 7932: got {gl2._proxy_port}"
        assert gl2._proxy_port == 7934  # two intermediate allocations advanced _next_port


class TestStartupBanner:
    """Regression tests for the startup banner stream + silent opt-out.

    Library code must not unconditionally print to stdout — it pollutes app
    output, CI logs, and anything that captures stdout (pytest -s, subprocess
    piping). Banner goes to stderr; `config={"silent": True}` suppresses it
    entirely.
    """

    def setup_method(self):
        _reset_module_state()

    def teardown_method(self):
        _reset_module_state()

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_banner_writes_to_stderr_not_stdout(
        self, mock_find, mock_popen, mock_wait, mock_detect, mock_wrap, capsys,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host:5432/mydb")

        captured = capsys.readouterr()
        assert "goldlapel →" not in captured.out, \
            f"Banner leaked to stdout: {captured.out!r}"
        assert "goldlapel →" in captured.err, \
            f"Banner missing from stderr: {captured.err!r}"
        assert "(proxy)" in captured.err
        assert "(dashboard)" in captured.err
        assert "7932" in captured.err
        assert "7933" in captured.err

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_silent_config_suppresses_banner(
        self, mock_find, mock_popen, mock_wait, mock_detect, mock_wrap, capsys,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host:5432/mydb", silent=True)

        captured = capsys.readouterr()
        assert "goldlapel →" not in captured.out, \
            f"Banner leaked to stdout under silent=True: {captured.out!r}"
        assert "goldlapel →" not in captured.err, \
            f"Banner leaked to stderr under silent=True: {captured.err!r}"

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_silent_false_prints_banner_to_stderr(
        self, mock_find, mock_popen, mock_wait, mock_detect, mock_wrap, capsys,
    ):
        # Explicit silent=False should behave the same as the default — banner
        # on stderr, nothing on stdout.
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host:5432/mydb", silent=False)

        captured = capsys.readouterr()
        assert "goldlapel →" not in captured.out
        assert "goldlapel →" in captured.err

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_silent_not_forwarded_to_binary(
        self, mock_find, mock_popen, mock_wait, mock_detect, mock_wrap,
    ):
        # `silent` is a wrapper-side-only kwarg — it must never appear in the
        # argv passed to the Rust binary.
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host:5432/mydb", silent=True)

        # Popen is called as Popen(cmd, **popen_kwargs); first positional arg is the cmd list.
        call_args, _ = mock_popen.call_args
        cmd = call_args[0]
        assert "--silent" not in cmd, f"--silent leaked into binary argv: {cmd}"

    def test_silent_in_config_map_rejected(self):
        # Regression guard: silent is a top-level wrapper kwarg; passing it
        # through the config map is a user error.
        with pytest.raises(ValueError, match="Unknown config keys"):
            _config_to_args({"silent": True})

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_banner_suppressed_when_dashboard_disabled(
        self, mock_find, mock_popen, mock_wait, mock_detect, mock_wrap, capsys,
    ):
        # With dashboard_port=0 we take the no-dashboard banner branch; it
        # must still go to stderr and still honor silent.
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start(
            "postgresql://host:5432/mydb",
            dashboard_port=0,
            silent=True,
        )
        captured = capsys.readouterr()
        assert captured.out == ""
        assert "goldlapel →" not in captured.err

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_banner_without_dashboard_goes_to_stderr(
        self, mock_find, mock_popen, mock_wait, mock_detect, mock_wrap, capsys,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start(
            "postgresql://host:5432/mydb",
            dashboard_port=0,
        )
        captured = capsys.readouterr()
        assert "goldlapel →" not in captured.out
        assert "goldlapel →" in captured.err
        assert "(proxy)" in captured.err
        # No-dashboard branch — banner should not include the dashboard URL.
        assert "dashboard" not in captured.err


class TestMeshKwargs:
    """Mesh startup kwargs: `mesh` (bool) + `mesh_tag` (optional str).

    Canonical surface — top-level, not inside the structured `config` map.
    Translate to `--mesh` / `--mesh-tag` CLI flags when spawning the binary.
    """

    def setup_method(self):
        _reset_module_state()

    def teardown_method(self):
        _reset_module_state()

    def test_mesh_defaults_false(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        assert gl._mesh is False
        assert gl._mesh_tag is None

    def test_mesh_true_stored(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb", mesh=True)
        assert gl._mesh is True

    def test_mesh_tag_stored(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb", mesh=True, mesh_tag="prod-east")
        assert gl._mesh_tag == "prod-east"

    def test_mesh_tag_empty_string_normalized_to_none(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb", mesh=True, mesh_tag="")
        assert gl._mesh_tag is None

    def test_mesh_in_config_map_rejected(self):
        # Regression guard: mesh/mesh_tag are top-level kwargs, not config keys.
        with pytest.raises(ValueError, match="Unknown config keys"):
            _config_to_args({"mesh": True})
        with pytest.raises(ValueError, match="Unknown config keys"):
            _config_to_args({"mesh_tag": "prod"})

    def test_mesh_not_in_config_keys(self):
        keys = config_keys()
        assert "mesh" not in keys
        assert "mesh_tag" not in keys

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_mesh_flag_forwarded_to_binary(
        self, mock_find, mock_popen, mock_wait, mock_detect, mock_wrap,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host:5432/mydb", mesh=True, mesh_tag="prod-east", silent=True)

        call_args, _ = mock_popen.call_args
        cmd = call_args[0]
        assert "--mesh" in cmd, f"--mesh missing from argv: {cmd}"
        assert "--mesh-tag" in cmd
        idx = cmd.index("--mesh-tag")
        assert cmd[idx + 1] == "prod-east"

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_mesh_false_no_flag(
        self, mock_find, mock_popen, mock_wait, mock_detect, mock_wrap,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host:5432/mydb", silent=True)

        call_args, _ = mock_popen.call_args
        cmd = call_args[0]
        assert "--mesh" not in cmd
        assert "--mesh-tag" not in cmd

    @patch("goldlapel.wrap.wrap", side_effect=lambda c, **kw: c)
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_mesh_without_tag_forwards_only_bool_flag(
        self, mock_find, mock_popen, mock_wait, mock_detect, mock_wrap,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host:5432/mydb", mesh=True, silent=True)

        call_args, _ = mock_popen.call_args
        cmd = call_args[0]
        assert "--mesh" in cmd
        assert "--mesh-tag" not in cmd
