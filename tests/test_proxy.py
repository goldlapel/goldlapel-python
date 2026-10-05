import os
import platform
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import goldlapel.proxy as proxy_mod
from goldlapel.proxy import (
    _port_free as _real_port_free,
    _application_name_marker,
    _config_to_args,
    _find_binary,
    _make_proxy_url,
    _wait_for_port,
    _wrapper_version,
    DEFAULT_PROXY_PORT,
    GoldLapel,
    config_keys,
    dashboard_url,
    start,
    stop,
    proxy_url,
)


# The proxy URL gets `application_name=goldlapel:python:<version>` appended so
# wrapper connections are recognisable in pg_stat_activity (the proxy caches
# them like any other client). The marker is suppressed if the user already
# set application_name (URL or PGAPPNAME).
_APP_NAME_SUFFIX = f"application_name=goldlapel:python:{_wrapper_version()}"


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
    """The wrapper rewrites host/port to point at the proxy and appends
    `application_name=goldlapel:python:<version>` so the proxy can distinguish
    wrapper traffic from raw clients. PGAPPNAME is cleared from the env in
    each test so the marker is applied deterministically (a developer running
    `pytest` with PGAPPNAME set would otherwise see different URLs)."""

    @pytest.fixture(autouse=True)
    def _no_pgappname(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PGAPPNAME", None)
            yield

    def test_postgresql_url(self):
        url = "postgresql://user:pass@dbhost:5432/mydb"
        assert _make_proxy_url(url, 7932) == f"postgresql://user:pass@localhost:7932/mydb?{_APP_NAME_SUFFIX}"

    def test_postgres_url(self):
        url = "postgres://user:pass@remote.aws.com:5432/mydb"
        assert _make_proxy_url(url, 7932) == f"postgres://user:pass@localhost:7932/mydb?{_APP_NAME_SUFFIX}"

    def test_pg_url_without_port(self):
        url = "postgresql://user:pass@host.aws.com/mydb"
        assert _make_proxy_url(url, 7932) == f"postgresql://user:pass@localhost:7932/mydb?{_APP_NAME_SUFFIX}"

    def test_pg_url_without_port_or_path(self):
        url = "postgresql://user:pass@host.aws.com"
        assert _make_proxy_url(url, 7932) == f"postgresql://user:pass@localhost:7932?{_APP_NAME_SUFFIX}"

    def test_bare_host_port(self):
        # Bare-host form skips the marker — atypical caller path.
        assert _make_proxy_url("dbhost:5432", 7932) == "localhost:7932"

    def test_host_only(self):
        assert _make_proxy_url("dbhost", 7932) == "localhost:7932"

    def test_preserves_params(self):
        url = "postgresql://user:pass@remote:5432/mydb?sslmode=require"
        assert _make_proxy_url(url, 7932) == f"postgresql://user:pass@localhost:7932/mydb?{_APP_NAME_SUFFIX}"

    def test_preserves_percent_encoded_password(self):
        url = "postgresql://user:p%40ss@remote:5432/mydb"
        assert _make_proxy_url(url, 7932) == f"postgresql://user:p%40ss@localhost:7932/mydb?{_APP_NAME_SUFFIX}"

    def test_no_userinfo(self):
        url = "postgresql://dbhost:5432/mydb"
        assert _make_proxy_url(url, 7932) == f"postgresql://localhost:7932/mydb?{_APP_NAME_SUFFIX}"

    def test_no_userinfo_no_port(self):
        url = "postgresql://dbhost/mydb"
        assert _make_proxy_url(url, 7932) == f"postgresql://localhost:7932/mydb?{_APP_NAME_SUFFIX}"

    def test_localhost_stays_localhost(self):
        url = "postgresql://user:pass@localhost:5432/mydb"
        assert _make_proxy_url(url, 7932) == f"postgresql://user:pass@localhost:7932/mydb?{_APP_NAME_SUFFIX}"

    def test_at_sign_in_password_with_port(self):
        url = "postgresql://user:p@ss@host:5432/mydb"
        assert _make_proxy_url(url, 7932) == f"postgresql://user:p@ss@localhost:7932/mydb?{_APP_NAME_SUFFIX}"

    def test_at_sign_in_password_without_port(self):
        url = "postgresql://user:p@ss@host/mydb"
        assert _make_proxy_url(url, 7932) == f"postgresql://user:p@ss@localhost:7932/mydb?{_APP_NAME_SUFFIX}"

    def test_at_sign_in_password_with_query_params(self):
        url = "postgresql://user:p@ss@host:5432/mydb?sslmode=require&param=val@ue"
        assert _make_proxy_url(url, 7932) == f"postgresql://user:p@ss@localhost:7932/mydb?param=val@ue&{_APP_NAME_SUFFIX}"

    def test_password_starting_with_digit_with_port(self):
        url = "postgresql://user:9password@host:5432/mydb"
        assert _make_proxy_url(url, 7932) == f"postgresql://user:9password@localhost:7932/mydb?{_APP_NAME_SUFFIX}"

    def test_password_starting_with_digit_without_port(self):
        url = "postgresql://user:9password@host/mydb"
        assert _make_proxy_url(url, 7932) == f"postgresql://user:9password@localhost:7932/mydb?{_APP_NAME_SUFFIX}"

    def test_password_all_digits_without_port(self):
        url = "postgresql://user:123456@host/mydb"
        assert _make_proxy_url(url, 7932) == f"postgresql://user:123456@localhost:7932/mydb?{_APP_NAME_SUFFIX}"

    def test_password_all_digits_with_port(self):
        url = "postgresql://user:123456@host:5432/mydb"
        assert _make_proxy_url(url, 7932) == f"postgresql://user:123456@localhost:7932/mydb?{_APP_NAME_SUFFIX}"

    def test_password_starting_with_digit_no_path(self):
        url = "postgresql://user:9secret@host"
        assert _make_proxy_url(url, 7932) == f"postgresql://user:9secret@localhost:7932?{_APP_NAME_SUFFIX}"


class TestApplicationNameMarker:
    """Wrappers tag their connections with PG `application_name` so they're
    recognisable in pg_stat_activity. The proxy doesn't gate on it."""

    @pytest.fixture(autouse=True)
    def _no_pgappname(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PGAPPNAME", None)
            yield

    def test_marker_format(self):
        marker = _application_name_marker()
        assert marker.startswith("goldlapel:python:")
        # version segment is non-empty
        assert marker.split(":", 2)[2]

    def test_marker_appended_when_no_existing_query(self):
        url = "postgresql://localhost:5432/mydb"
        out = _make_proxy_url(url, 7932)
        assert f"?{_APP_NAME_SUFFIX}" in out

    def test_marker_appended_with_existing_query(self):
        url = "postgresql://localhost:5432/mydb?connect_timeout=5"
        out = _make_proxy_url(url, 7932)
        assert "connect_timeout=5" in out
        assert f"&{_APP_NAME_SUFFIX}" in out

    def test_user_override_via_url_respected(self):
        # User explicitly set application_name — wrapper does not clobber it.
        url = "postgresql://localhost:5432/mydb?application_name=my-app"
        out = _make_proxy_url(url, 7932)
        assert "application_name=my-app" in out
        assert "goldlapel:python" not in out

    def test_user_override_via_pgappname_respected(self):
        url = "postgresql://localhost:5432/mydb"
        with patch.dict(os.environ, {"PGAPPNAME": "my-app"}):
            out = _make_proxy_url(url, 7932)
        assert "application_name=" not in out
        assert "goldlapel:python" not in out


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

class TestConfigToArgs:
    def test_string_value(self):
        assert _config_to_args({"pool_mode": "transaction"}) == ["--pool-mode", "transaction"]

    def test_numeric_value(self):
        assert _config_to_args({"pool_size": 50}) == ["--pool-size", "50"]

    def test_boolean_true(self):
        # `disable_pool` is a representative still-in-config bool key.
        assert _config_to_args({"disable_pool": True}) == ["--disable-pool"]

    def test_boolean_false(self):
        assert _config_to_args({"disable_pool": False}) == []

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
        assert "disable_pool" in keys
        assert "replica" in keys

    def test_config_keys_does_not_contain_promoted_top_level_keys(self):
        # Top-level concepts (mode, log_level, dashboard_port, etc.) were
        # promoted out of the structured config map on the canonical surface.
        keys = config_keys()
        for promoted in (
            "mode", "log_level", "dashboard_port",
            "config", "license", "client", "silent",
            "disable_proxy_cache",
            "disable_sqloptimize", "disable_auto_indexes",
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

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_two_upstreams_get_different_ports(self, mock_find, mock_popen, mock_wait, mock_detect):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        url_a = "postgresql://host-a:5432/db_a"
        url_b = "postgresql://host-b:5432/db_b"

        start(url_a)
        start(url_b)

        # Each proxy holds a pair (proxy port + dashboard port), so the
        # second one steps over the first one's dashboard on 7933.
        assert proxy_mod._instances[url_a]._proxy_port == 7932
        assert proxy_mod._instances[url_b]._proxy_port == 7934
        assert proxy_mod._instances[url_b]._dashboard_port == 7935

    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_conn_and_connect_are_plain_driver_connections(self, mock_find, mock_popen, mock_wait):
        # No wrapper-side cache: gl.conn and connect() hand back exactly
        # what the driver's connect() returned.
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()
        driver_name, driver = _mock_driver()
        internal, extra = MagicMock(name="internal"), MagicMock(name="extra")
        driver.connect.side_effect = [internal, extra]
        with patch("goldlapel.proxy._detect_sync_driver", return_value=(driver_name, driver)):
            gl = start("postgresql://host:5432/mydb", silent=True)
            assert gl.conn is internal
            assert proxy_mod.connect("postgresql://host:5432/mydb") is extra

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._kill_orphan_on_port")
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_same_upstream_returns_existing(self, mock_find, mock_popen, mock_wait, mock_orphan, mock_detect):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        url = "postgresql://host:5432/mydb"
        start(url)
        start(url)

        assert len(proxy_mod._instances) == 1
        assert mock_popen.call_count == 1  # Only spawned once

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_stop_specific_upstream(self, mock_find, mock_popen, mock_wait, mock_detect):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        url_a = "postgresql://host-a:5432/db_a"
        url_b = "postgresql://host-b:5432/db_b"

        start(url_a)
        start(url_b)

        stop(url_a)
        assert len(proxy_mod._instances) == 1
        assert url_a not in proxy_mod._instances
        assert url_b in proxy_mod._instances

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_stop_all(self, mock_find, mock_popen, mock_wait, mock_detect):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host-a:5432/db_a")
        start("postgresql://host-b:5432/db_b")

        stop()
        assert len(proxy_mod._instances) == 0

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_proxy_url_single_instance(self, mock_find, mock_popen, mock_wait, mock_detect):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        url = "postgresql://host:5432/mydb"
        start(url)
        purl = proxy_url()
        assert purl is not None
        assert "7932" in purl

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_proxy_url_multi_instance_requires_upstream(self, mock_find, mock_popen, mock_wait, mock_detect):
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

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_dashboard_url_multi_instance_requires_upstream(self, mock_find, mock_popen, mock_wait, mock_detect):
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

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_explicit_port_is_used_as_given_and_claims_its_pair(
        self, mock_find, mock_popen, mock_wait, mock_detect,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        url_a = "postgresql://host-a:5432/db_a"
        url_b = "postgresql://host-b:5432/db_b"

        start(url_a, proxy_port=7933)
        start(url_b)

        assert proxy_mod._instances[url_a]._proxy_port == 7933
        # 7932's dashboard would be 7933, 7933/7934 are taken: next free pair.
        assert proxy_mod._instances[url_b]._proxy_port == 7935

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_far_explicit_port_leaves_default_free(
        self, mock_find, mock_popen, mock_wait, mock_detect,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host-a:5432/db_a", proxy_port=8000)
        gl_b = start("postgresql://host-b:5432/db_b")

        assert gl_b._proxy_port == DEFAULT_PROXY_PORT

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_explicit_dashboard_port_is_skipped(
        self, mock_find, mock_popen, mock_wait, mock_detect,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host-a:5432/db_a", dashboard_port=7934)
        gl_b = start("postgresql://host-b:5432/db_b")

        # Claimed: 7932 (proxy) and 7934 (dashboard). 7933 is free but its
        # dashboard would land on 7934, so the first free pair is 7935/7936.
        assert gl_b._proxy_port == 7935
        assert gl_b._dashboard_port == 7936

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_disabled_dashboard_claims_only_the_proxy_port(
        self, mock_find, mock_popen, mock_wait, mock_detect,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host-a:5432/db_a", dashboard_port=0)
        gl_b = start("postgresql://host-b:5432/db_b", dashboard_port=0)
        gl_c = start("postgresql://host-c:5432/db_c")

        assert gl_b._proxy_port == 7933
        assert gl_c._proxy_port == 7934

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_new_explicit_dashboard_port_avoids_claimed_proxy_port(
        self, mock_find, mock_popen, mock_wait, mock_detect,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host-a:5432/db_a")
        gl_b = start("postgresql://host-b:5432/db_b", dashboard_port=9000)

        # 7932/7933 are claimed by the first proxy; with an explicit
        # dashboard the second only needs a free proxy port.
        assert gl_b._proxy_port == 7934
        assert gl_b._dashboard_port == 9000

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_stop_releases_ports(self, mock_find, mock_popen, mock_wait, mock_detect):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        url_a = "postgresql://host-a:5432/db_a"
        start(url_a)
        start("postgresql://host-b:5432/db_b")
        stop(url_a)

        gl_c = start("postgresql://host-c:5432/db_c")
        assert gl_c._proxy_port == 7932

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_exited_proxy_releases_ports(self, mock_find, mock_popen, mock_wait, mock_detect):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        gl_a = start("postgresql://host-a:5432/db_a")
        gl_a._process.poll.return_value = 1  # proxy died

        gl_b = start("postgresql://host-b:5432/db_b")
        assert gl_b._proxy_port == 7932

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_proxy_url_unknown_upstream(self, mock_find, mock_popen, mock_wait, mock_detect):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host:5432/mydb")
        assert proxy_url("postgresql://unknown:5432/nope") is None

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._kill_orphan_on_port")
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_dead_instance_gets_recreated(self, mock_find, mock_popen, mock_wait, mock_orphan, mock_detect):
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

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_cleanup_stops_all(self, mock_find, mock_popen, mock_wait, mock_detect):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host-a:5432/db_a")
        start("postgresql://host-b:5432/db_b")

        from goldlapel.proxy import _cleanup
        _cleanup()

        assert len(proxy_mod._instances) == 0

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=False)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_failed_start_cleans_up_instance(self, mock_find, mock_popen, mock_wait, mock_detect):
        proc = _mock_popen()
        proc.stderr.read.return_value = b"bind error"
        mock_popen.return_value = proc

        url = "postgresql://host:5432/mydb"
        with pytest.raises(RuntimeError, match="failed to start"):
            start(url)

        assert url not in proxy_mod._instances

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_stop_nonexistent_upstream_is_noop(self, mock_find, mock_popen, mock_wait, mock_detect):
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

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._kill_orphan_on_port")
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_instance_stop_removes_from_registry(
        self, mock_find, mock_popen, mock_wait, mock_orphan, mock_detect,
    ):
        # Regression for v0.2 review finding (MEDIUM, Option A): after
        # gl.stop(), the _instances entry must be dropped so the next
        # start(same_url) gets a fresh instance and its ports are free for
        # other upstreams.
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        url = "postgresql://host:5432/mydb"
        gl = start(url)
        assert url in proxy_mod._instances

        gl.stop()
        assert url not in proxy_mod._instances, \
            "gl.stop() must remove itself from _instances"

        # The stopped proxy's pair is free again, so another upstream takes
        # 7932 and the restart of `url` gets the next free pair.
        other = start("postgresql://other:5432/other_db")
        gl2 = start(url)
        assert other._proxy_port == 7932
        assert gl2 is not gl
        assert gl2._proxy_port == 7934

    def test_stopping_unregistered_instance_keeps_registered_one(self):
        # A directly-constructed GoldLapel for the same upstream must not
        # drop the factory's registered instance (and free its ports).
        url = "postgresql://host:5432/mydb"
        registered = GoldLapel(url)
        proxy_mod._instances[url] = registered

        GoldLapel(url).stop()
        assert proxy_mod._instances[url] is registered


class TestExplicitPortCollision:
    """An explicit port that a live proxy of this process already holds for
    a different upstream raises before anything is spawned or killed."""

    def setup_method(self):
        _reset_module_state()

    def teardown_method(self):
        _reset_module_state()

    def _start_a(self):
        return start("postgresql://u:secret@host-a:5432/db_a", silent=True)

    @pytest.mark.parametrize("kwargs, port", [
        ({"proxy_port": 7932}, 7932),            # A's proxy port
        ({"proxy_port": 7933}, 7933),            # A's dashboard port
        ({"proxy_port": 7931}, 7932),            # B's derived dashboard lands on A's proxy
        ({"proxy_port": 8000, "dashboard_port": 7933}, 7933),
        ({"dashboard_port": 7932}, 7932),        # auto proxy port, explicit dashboard
    ])
    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._kill_orphan_on_port")
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_port_held_by_other_upstream_raises(
        self, mock_find, mock_popen, mock_wait, mock_orphan, mock_detect, kwargs, port,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()
        self._start_a()
        mock_orphan.reset_mock()

        url_b = "postgresql://host-b:5432/db_b"
        with pytest.raises(RuntimeError) as exc:
            start(url_b, silent=True, **kwargs)

        msg = str(exc.value)
        assert f"port {port}" in msg
        assert "postgresql://u:***@host-a:5432/db_a" in msg
        assert "secret" not in msg
        assert mock_popen.call_count == 1
        mock_orphan.assert_not_called()
        assert url_b not in proxy_mod._instances

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._kill_orphan_on_port")
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_port_of_exited_proxy_is_free(
        self, mock_find, mock_popen, mock_wait, mock_orphan, mock_detect,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()
        gl_a = self._start_a()
        gl_a._process.poll.return_value = 1  # proxy died

        gl_b = start("postgresql://host-b:5432/db_b", proxy_port=7932, silent=True)
        assert gl_b._proxy_port == 7932

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._kill_orphan_on_port")
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_same_upstream_with_its_own_port_reuses(
        self, mock_find, mock_popen, mock_wait, mock_orphan, mock_detect,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()
        gl_a = self._start_a()

        again = start("postgresql://u:secret@host-a:5432/db_a", proxy_port=7932)
        assert again is gl_a
        assert mock_popen.call_count == 1


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

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_banner_writes_to_stderr_not_stdout(
        self, mock_find, mock_popen, mock_wait, mock_detect, capsys,
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

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_silent_config_suppresses_banner(
        self, mock_find, mock_popen, mock_wait, mock_detect, capsys,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host:5432/mydb", silent=True)

        captured = capsys.readouterr()
        assert "goldlapel →" not in captured.out, \
            f"Banner leaked to stdout under silent=True: {captured.out!r}"
        assert "goldlapel →" not in captured.err, \
            f"Banner leaked to stderr under silent=True: {captured.err!r}"

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_silent_false_prints_banner_to_stderr(
        self, mock_find, mock_popen, mock_wait, mock_detect, capsys,
    ):
        # Explicit silent=False should behave the same as the default — banner
        # on stderr, nothing on stdout.
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host:5432/mydb", silent=False)

        captured = capsys.readouterr()
        assert "goldlapel →" not in captured.out
        assert "goldlapel →" in captured.err

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_silent_not_forwarded_to_binary(
        self, mock_find, mock_popen, mock_wait, mock_detect,
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

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_banner_suppressed_when_dashboard_disabled(
        self, mock_find, mock_popen, mock_wait, mock_detect, capsys,
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

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_banner_without_dashboard_goes_to_stderr(
        self, mock_find, mock_popen, mock_wait, mock_detect, capsys,
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

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_mesh_flag_forwarded_to_binary(
        self, mock_find, mock_popen, mock_wait, mock_detect,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host:5432/mydb", mesh=True, mesh_tag="prod-east", silent=True)

        call_args, _ = mock_popen.call_args
        cmd = call_args[0]
        assert "--mesh" in cmd, f"--mesh missing from argv: {cmd}"
        assert "--mesh-tag" in cmd
        idx = cmd.index("--mesh-tag")
        assert cmd[idx + 1] == "prod-east"

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_mesh_false_no_flag(
        self, mock_find, mock_popen, mock_wait, mock_detect,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host:5432/mydb", silent=True)

        call_args, _ = mock_popen.call_args
        cmd = call_args[0]
        assert "--mesh" not in cmd
        assert "--mesh-tag" not in cmd

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_mesh_without_tag_forwards_only_bool_flag(
        self, mock_find, mock_popen, mock_wait, mock_detect,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()

        start("postgresql://host:5432/mydb", mesh=True, silent=True)

        call_args, _ = mock_popen.call_args
        cmd = call_args[0]
        assert "--mesh" in cmd
        assert "--mesh-tag" not in cmd


class TestPromotedDisableFlags:
    """Top-level disable kwargs that map 1:1 to proxy CLI flags. Each
    defaults to False; True emits the corresponding `--disable-X` flag.
    Not valid in the structured `config` map — passing them through
    `config={...}` is a hard error.
    """

    def setup_method(self):
        _reset_module_state()

    def teardown_method(self):
        _reset_module_state()

    # -- Stored attribute defaults / mutability ------------------------

    def test_disable_proxy_cache_defaults_false(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        assert gl._disable_proxy_cache is False

    def test_disable_proxy_cache_true_stored(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb", disable_proxy_cache=True)
        assert gl._disable_proxy_cache is True

    def test_disable_sqloptimize_defaults_false(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        assert gl._disable_sqloptimize is False

    def test_disable_sqloptimize_true_stored(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb", disable_sqloptimize=True)
        assert gl._disable_sqloptimize is True

    def test_disable_auto_indexes_defaults_false(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        assert gl._disable_auto_indexes is False

    def test_disable_auto_indexes_true_stored(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb", disable_auto_indexes=True)
        assert gl._disable_auto_indexes is True

    # -- Rejected from config map (atomic break) -----------------------

    def test_disable_proxy_cache_in_config_map_rejected(self):
        with pytest.raises(ValueError, match="Unknown config keys"):
            _config_to_args({"disable_proxy_cache": True})

    def test_disable_sqloptimize_in_config_map_rejected(self):
        with pytest.raises(ValueError, match="Unknown config keys"):
            _config_to_args({"disable_sqloptimize": True})

    def test_disable_auto_indexes_in_config_map_rejected(self):
        with pytest.raises(ValueError, match="Unknown config keys"):
            _config_to_args({"disable_auto_indexes": True})

    def test_disable_keys_not_in_config_keys(self):
        keys = config_keys()
        for promoted in (
            "disable_proxy_cache",
            "disable_sqloptimize", "disable_auto_indexes",
        ):
            assert promoted not in keys, (
                f"{promoted} is now a top-level kwarg, must not be in config map"
            )

    # -- argv emission --------------------------------------------------

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_disable_proxy_cache_emits_flag(
        self, mock_find, mock_popen, mock_wait, mock_detect,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()
        start("postgresql://host:5432/mydb", disable_proxy_cache=True, silent=True)
        cmd = mock_popen.call_args[0][0]
        assert "--disable-proxy-cache" in cmd

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_disable_sqloptimize_emits_flag(
        self, mock_find, mock_popen, mock_wait, mock_detect,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()
        start("postgresql://host:5432/mydb", disable_sqloptimize=True, silent=True)
        cmd = mock_popen.call_args[0][0]
        assert "--disable-sqloptimize" in cmd

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_disable_auto_indexes_emits_flag(
        self, mock_find, mock_popen, mock_wait, mock_detect,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()
        start("postgresql://host:5432/mydb", disable_auto_indexes=True, silent=True)
        cmd = mock_popen.call_args[0][0]
        assert "--disable-auto-indexes" in cmd

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_default_no_disable_flags(
        self, mock_find, mock_popen, mock_wait, mock_detect,
    ):
        # Default state: none of the promoted flags should appear in argv.
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()
        start("postgresql://host:5432/mydb", silent=True)
        cmd = mock_popen.call_args[0][0]
        for flag in (
            "--disable-proxy-cache",
            "--disable-sqloptimize", "--disable-auto-indexes",
        ):
            assert flag not in cmd, f"{flag} unexpectedly present in default argv"

    @patch("goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver())
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    def test_all_three_flags_compose(
        self, mock_find, mock_popen, mock_wait, mock_detect,
    ):
        mock_popen.side_effect = lambda *a, **kw: _mock_popen()
        start(
            "postgresql://host:5432/mydb",
            disable_proxy_cache=True,
            disable_sqloptimize=True,
            disable_auto_indexes=True,
            silent=True,
        )
        cmd = mock_popen.call_args[0][0]
        for flag in (
            "--disable-proxy-cache",
            "--disable-sqloptimize", "--disable-auto-indexes",
        ):
            assert flag in cmd

    # -- enable_proxy_cache_for_wrappers regression — gone for good ----

    def test_enable_proxy_cache_for_wrappers_kwarg_rejected(self):
        # Atomic break (Model B): the wrapper-skip override flag was
        # dropped on both sides. Passing the old kwarg now raises
        # TypeError on the unknown keyword.
        with pytest.raises(TypeError):
            GoldLapel(
                "postgresql://host:5432/mydb",
                enable_proxy_cache_for_wrappers=True,
            )

    @pytest.mark.parametrize("kwarg", [
        "invalidation_port", "disable_native_cache", "aggressive_verify",
        "disable_matviews",
    ])
    def test_removed_cache_kwargs_rejected(self, kwarg):
        # The in-process cache and matviews are gone — no aliases.
        with pytest.raises(TypeError):
            GoldLapel("postgresql://host:5432/mydb", **{kwarg: True})
        with pytest.raises(TypeError):
            start("postgresql://host:5432/mydb", **{kwarg: True})

    @pytest.mark.parametrize("key", [
        "refresh_interval_secs", "pattern_ttl_secs", "max_tables_per_view",
        "max_columns_per_view", "disable_consolidation", "disable_rewrite",
        "disable_shadow_mode", "enable_coalescing",
    ])
    def test_removed_matview_config_keys_rejected(self, key):
        with pytest.raises(ValueError, match="Unknown config keys"):
            _config_to_args({key: True})


# -- Port claims, readiness, sharing and cleanup ---------------------------

_SYNC_SPAWN_PATCHES = (
    ("goldlapel.proxy._find_binary", {"return_value": "/usr/bin/goldlapel"}),
    ("goldlapel.proxy._kill_orphan_on_port", {}),
)


def _spawn_patches(popen=None, wait=True, driver=True):
    """Patch the spawn so no binary runs. Returns (ExitStack, Popen mock)."""
    from contextlib import ExitStack
    stack = ExitStack()
    for target, kwargs in _SYNC_SPAWN_PATCHES:
        stack.enter_context(patch(target, **kwargs))
    if isinstance(wait, bool):
        stack.enter_context(patch("goldlapel.proxy._wait_for_port", return_value=wait))
    else:
        stack.enter_context(patch("goldlapel.proxy._wait_for_port", side_effect=wait))
    if driver:
        stack.enter_context(patch(
            "goldlapel.proxy._detect_sync_driver", side_effect=lambda: _mock_driver(),
        ))
    mock_popen = stack.enter_context(patch("goldlapel.proxy.subprocess.Popen"))
    mock_popen.side_effect = popen or (lambda *a, **kw: _mock_popen())
    return stack, mock_popen


def _exited_popen(status, stderr):
    proc = MagicMock()
    proc.poll.return_value = status
    proc.wait.return_value = status
    proc.stderr.read.return_value = stderr
    return proc


_REFUSAL = (
    "I'm afraid port 7940, for the proxy, is already in use — perhaps "
    "another Gold Lapel. Choose another with --proxy-port.\n"
).encode()


class TestClientUrlDropsUpstreamTls:
    """The proxy declines client TLS unless it serves it, so the app's URL
    must not carry the upstream hop's TLS/GSS parameters."""

    def test_ssl_and_channel_binding_dropped_others_kept(self):
        url = ("postgresql://u:p@db.example.com:5432/app"
               "?sslmode=require&channel_binding=require&application_name=web")
        assert _make_proxy_url(url, 7932) == "postgresql://u:p@localhost:7932/app?application_name=web"

    def test_every_tls_and_gss_key_dropped_case_insensitively(self):
        url = ("postgresql://u:p@h/db?SSLMode=verify-full&sslrootcert=/ca.pem"
               "&sslcert=/c&sslkey=/k&sslcrl=/l&sslcrldir=/d&sslpassword=x&sslsni=1"
               "&sslnegotiation=direct&ssl_min_protocol_version=TLSv1.2"
               "&ssl_max_protocol_version=TLSv1.3&requiressl=1&Channel_Binding=prefer"
               "&gssencmode=disable&krbsrvname=postgres&gsslib=gssapi&connect_timeout=5")
        assert _make_proxy_url(url, 7932) == (
            f"postgresql://u:p@localhost:7932/db?connect_timeout=5&{_APP_NAME_SUFFIX}"
        )

    def test_url_with_only_tls_params_gets_a_clean_query(self):
        url = "postgresql://u:p@h:5432/db?sslmode=require"
        assert _make_proxy_url(url, 7932) == f"postgresql://u:p@localhost:7932/db?{_APP_NAME_SUFFIX}"

    def test_kept_when_proxy_serves_client_tls(self):
        url = "postgresql://u:p@h:5432/db?sslmode=require"
        assert _make_proxy_url(url, 7932, client_tls=True) == (
            f"postgresql://u:p@localhost:7932/db?sslmode=require&{_APP_NAME_SUFFIX}"
        )

    def test_start_keeps_upstream_tls_and_strips_client_url(self):
        url = "postgresql://u:p@h:5432/db?sslmode=require&channel_binding=require"
        stack, mock_popen = _spawn_patches()
        with stack:
            gl = start(url, silent=True)
        cmd = mock_popen.call_args[0][0]
        assert cmd[cmd.index("--upstream") + 1] == url
        assert "sslmode" not in gl.url
        assert "channel_binding" not in gl.url

    def test_start_with_client_tls_keeps_client_url_params(self):
        stack, _ = _spawn_patches()
        with stack:
            gl = start("postgresql://u:p@h:5432/db?sslmode=require", silent=True,
                       config={"tls_cert": "/c.pem", "tls_key": "/k.pem"})
        assert "sslmode=require" in gl.url


class TestOsLevelPortProbe:
    def test_probe_sees_a_listener(self):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(("0.0.0.0", 0))
        listener.listen()
        port = listener.getsockname()[1]
        try:
            assert _real_port_free(port) is False
        finally:
            listener.close()
        assert _real_port_free(port) is True

    def test_auto_assignment_skips_ports_bound_elsewhere(self, monkeypatch):
        # 7932 and 7935 are held by something outside this process.
        monkeypatch.setattr(proxy_mod, "_port_free", lambda port: port not in (7932, 7935))
        stack, _ = _spawn_patches()
        with stack:
            a = start("postgresql://host-a:5432/db_a", silent=True)
            b = start("postgresql://host-b:5432/db_b", silent=True)
        assert (a.proxy_port, a.dashboard_port) == (7933, 7934)
        assert (b.proxy_port, b.dashboard_port) == (7936, 7937)

    def test_explicit_dashboard_only_probes_the_proxy_port(self, monkeypatch):
        monkeypatch.setattr(proxy_mod, "_port_free", lambda port: port != 7932)
        stack, _ = _spawn_patches()
        with stack:
            gl = start("postgresql://host-a:5432/db_a", dashboard_port=9000, silent=True)
        assert gl.proxy_port == 7933


class TestReadiness:
    """A start succeeds only if the port answers and the spawned proxy is
    still alive — otherwise its exit status and stderr are surfaced."""

    def test_exited_proxy_fails_even_if_port_answers(self):
        # The port answered — but it was some other listener: our proxy
        # refused the port and exited.
        stack, _ = _spawn_patches(popen=lambda *a, **kw: _exited_popen(1, _REFUSAL))
        url = "postgresql://host:5432/db"
        with stack, pytest.raises(RuntimeError) as exc:
            start(url, silent=True)
        msg = str(exc.value)
        assert "exited with status 1" in msg
        assert "already in use" in msg
        assert url not in proxy_mod._instances
        assert not proxy_mod._live

    def test_busy_port_waits_for_the_proxy_to_refuse(self, monkeypatch):
        # An explicit port something else listens on: a connect would
        # reach that listener, so readiness waits for the proxy's verdict.
        monkeypatch.setattr(proxy_mod, "_port_free", lambda port: port != 7940)
        proc = _exited_popen(None, _REFUSAL)
        proc.wait.side_effect = lambda timeout=None: setattr(proc.poll, "return_value", 1) or 1
        stack, _ = _spawn_patches(popen=lambda *a, **kw: proc)
        with stack, patch("goldlapel.proxy._wait_for_port") as mock_wait, \
                pytest.raises(RuntimeError) as exc:
            start("postgresql://host:5432/db", proxy_port=7940, silent=True)
        mock_wait.assert_not_called()
        assert "status 1" in str(exc.value)
        assert "port 7940, for the proxy, is already in use" in str(exc.value)

    def test_busy_port_proxy_that_never_exits_is_killed(self, monkeypatch):
        monkeypatch.setattr(proxy_mod, "_port_free", lambda port: port != 7940)
        proc = _exited_popen(None, b"")
        proc.wait.side_effect = subprocess.TimeoutExpired("goldlapel", 10)
        stack, _ = _spawn_patches(popen=lambda *a, **kw: proc)
        with stack, pytest.raises(RuntimeError, match="port 7940 was already in use"):
            start("postgresql://host:5432/db", proxy_port=7940, silent=True)
        proc.kill.assert_called()
        assert not proxy_mod._live

    def test_wait_for_port_returns_when_process_exits(self):
        proc = MagicMock()
        proc.poll.return_value = 1
        began = time.monotonic()
        assert _wait_for_port("127.0.0.1", 1, timeout=5.0, process=proc) is False
        assert time.monotonic() - began < 1.0


class TestConcurrentStarts:
    """Threads starting the same upstream at once share one proxy: the
    late arrival waits for the start in progress and gets the finished
    instance — never a second spawn, never url/conn still None."""

    def test_late_arrival_waits_and_shares(self):
        def slow_wait(*args, **kwargs):
            time.sleep(0.3)
            return True

        stack, mock_popen = _spawn_patches(wait=slow_wait)
        url = "postgresql://host:5432/db"
        results, errors = [], []

        def run():
            try:
                results.append(start(url, silent=True))
            except BaseException as exc:
                errors.append(exc)

        with stack:
            threads = [threading.Thread(target=run) for _ in range(2)]
            threads[0].start()
            time.sleep(0.1)
            threads[1].start()
            for t in threads:
                t.join()

        assert not errors
        assert mock_popen.call_count == 1
        a, b = results
        assert a is b
        assert a.url is not None and a._conn is not None
        assert a._holders == 2

    def test_late_arrival_starts_its_own_after_a_failed_start(self):
        outcomes = iter([False, True])

        def wait(*args, **kwargs):
            time.sleep(0.3)
            return next(outcomes)

        stack, mock_popen = _spawn_patches(wait=wait)
        url = "postgresql://host:5432/db"
        results, errors = [], []

        def run():
            try:
                results.append(start(url, silent=True))
            except RuntimeError as exc:
                errors.append(exc)

        with stack:
            threads = [threading.Thread(target=run) for _ in range(2)]
            threads[0].start()
            time.sleep(0.1)
            threads[1].start()
            for t in threads:
                t.join()

        assert len(errors) == 1 and len(results) == 1
        assert mock_popen.call_count == 2
        assert proxy_mod._instances[url] is results[0]
        assert results[0].running


class TestDirectInstancesClaimPorts:
    """A GoldLapel constructed directly claims its ports on start() like a
    factory-started one, and releases them on stop()."""

    def test_direct_start_claims_its_pair(self):
        stack, _ = _spawn_patches()
        with stack:
            direct = GoldLapel("postgresql://host-a:5432/db_a", silent=True)
            direct.start()
            b = start("postgresql://host-b:5432/db_b", silent=True)
            assert direct.proxy_port == 7932
            assert b.proxy_port == 7934

            direct.stop()
            c = start("postgresql://host-c:5432/db_c", silent=True)
            assert c.proxy_port == 7932

    def test_two_direct_instances_get_distinct_pairs(self):
        stack, _ = _spawn_patches()
        with stack:
            a = GoldLapel("postgresql://host-a:5432/db_a", silent=True)
            b = GoldLapel("postgresql://host-b:5432/db_b", silent=True)
            a.start()
            b.start()
        assert (a.proxy_port, b.proxy_port) == (7932, 7934)

    def test_direct_explicit_port_held_by_another_proxy_raises(self):
        stack, mock_popen = _spawn_patches()
        with stack:
            start("postgresql://host-a:5432/db_a", silent=True)
            direct = GoldLapel("postgresql://host-b:5432/db_b", proxy_port=7933)
            with pytest.raises(RuntimeError, match="port 7933"):
                direct.start()
        assert mock_popen.call_count == 1


class TestSharedProxyHolders:
    """Every start() of a running upstream shares its proxy; each stop()
    gives up one hold and the last one stops the proxy."""

    def test_stop_by_one_holder_keeps_the_proxy_for_the_other(self):
        stack, _ = _spawn_patches()
        url = "postgresql://host:5432/db"
        with stack:
            first = start(url, silent=True)
            second = start(url, silent=True)
            process = first._process

            second.stop()
            assert first.running
            process.terminate.assert_not_called()
            assert proxy_mod._instances[url] is first

            first.stop()
        process.terminate.assert_called_once()
        assert url not in proxy_mod._instances
        assert not proxy_mod._live

    def test_with_block_on_a_shared_proxy_leaves_it_running(self):
        stack, _ = _spawn_patches()
        url = "postgresql://host:5432/db"
        with stack:
            outer = start(url, silent=True)
            with start(url, silent=True):
                pass
            assert outer.running

    def test_module_stop_stops_outright(self):
        stack, _ = _spawn_patches()
        url = "postgresql://host:5432/db"
        with stack:
            gl = start(url, silent=True)
            start(url, silent=True)
            process = gl._process
            stop(url)
        process.terminate.assert_called_once()
        assert not gl.running
        assert not proxy_mod._live


class TestInterruptedStartReleasesPorts:
    @pytest.mark.parametrize("where", ["readiness", "connect"])
    def test_keyboard_interrupt_releases_everything(self, where):
        procs = []

        def popen(*args, **kwargs):
            procs.append(_mock_popen())
            return procs[-1]

        wait = True
        if where == "readiness":
            def wait(*args, **kwargs):
                raise KeyboardInterrupt
        stack, _ = _spawn_patches(popen=popen, wait=wait, driver=False)
        driver_name, driver = _mock_driver()
        if where == "connect":
            driver.connect.side_effect = KeyboardInterrupt
        url = "postgresql://host:5432/db"
        with stack, patch("goldlapel.proxy._detect_sync_driver",
                          return_value=(driver_name, driver)):
            with pytest.raises(KeyboardInterrupt):
                start(url, silent=True)
        procs[0].terminate.assert_called_once()
        assert url not in proxy_mod._instances
        assert not proxy_mod._live

    def test_invalid_option_claims_nothing(self):
        stack, mock_popen = _spawn_patches()
        url = "postgresql://host:5432/db"
        with stack:
            with pytest.raises(ValueError, match="log_level"):
                start(url, log_level="loud")
            assert url not in proxy_mod._instances
            assert not proxy_mod._live
            assert start("postgresql://other:5432/db", silent=True).proxy_port == 7932
        mock_popen.assert_called_once()


class TestUnknownOptions:
    @pytest.mark.parametrize("factory", [
        lambda **kw: start("postgresql://host:5432/db", **kw),
        lambda **kw: GoldLapel("postgresql://host:5432/db", **kw),
    ])
    def test_removed_option_says_why(self, factory):
        with pytest.raises(TypeError) as exc:
            factory(invalidation_port=7934)
        assert "Unknown Gold Lapel options: invalidation_port (removed with the in-process cache)" in str(exc.value)

    def test_unknown_option_named(self):
        with pytest.raises(TypeError, match="Unknown Gold Lapel options: disable_matviews .*materialized views.*, prot$"):
            start("postgresql://host:5432/db", prot=1, disable_matviews=True)


def _fake_proc_entry(root, pid, ppid, argv):
    d = root / str(pid)
    d.mkdir()
    (d / "stat").write_bytes(f"{pid} (goldlapel) S {ppid} {pid} {pid} 0 -1".encode())
    (d / "cmdline").write_bytes(b"\0".join(a.encode() for a in argv) + b"\0")


class TestKillOrphanOnPort:
    """Only a true orphan of this upstream is stopped: a goldlapel process
    reparented to init whose --upstream and --proxy-port are ours."""

    UPSTREAM = "postgresql://u:p@db:5432/app"

    @pytest.fixture
    def fake_proc(self, tmp_path, monkeypatch):
        monkeypatch.setattr(proxy_mod, "_PROC", str(tmp_path))
        monkeypatch.setattr(proxy_mod, "_port_in_use", lambda port: True)
        killed = []

        def fake_kill(pid, sig):
            killed.append(pid)
            import shutil
            shutil.rmtree(tmp_path / str(pid))

        monkeypatch.setattr(proxy_mod.os, "kill", fake_kill)
        return tmp_path, killed

    def _argv(self, upstream=None, port="7932", binary="/opt/bin/goldlapel-linux-x86_64"):
        return [binary, "--upstream", upstream or self.UPSTREAM, "--proxy-port", port]

    @pytest.mark.skipif(sys.platform != "linux", reason="orphans are only detected on Linux")
    def test_kills_orphan_of_this_upstream_only(self, fake_proc):
        root, killed = fake_proc
        _fake_proc_entry(root, 501, 1, self._argv())                      # orphan: ours
        _fake_proc_entry(root, 502, 4242, self._argv())                   # another live app's
        _fake_proc_entry(root, 503, os.getpid(), self._argv())            # our own child
        _fake_proc_entry(root, 504, 1, self._argv(upstream="postgresql://other/db"))
        _fake_proc_entry(root, 505, 1, self._argv(port="7934"))
        _fake_proc_entry(root, 506, 1, self._argv(binary="/usr/bin/python3"))

        began = time.monotonic()
        proxy_mod._kill_orphan_on_port(7932, self.UPSTREAM)

        assert killed == [501]
        assert time.monotonic() - began < 1.5

    def test_no_kill_off_linux(self, fake_proc, monkeypatch):
        root, killed = fake_proc
        _fake_proc_entry(root, 501, 1, self._argv())
        monkeypatch.setattr(proxy_mod.sys, "platform", "darwin")
        proxy_mod._kill_orphan_on_port(7932, self.UPSTREAM)
        assert killed == []

    def test_free_port_skips_the_scan(self, monkeypatch):
        monkeypatch.setattr(proxy_mod, "_port_in_use", lambda port: False)
        with patch("goldlapel.proxy.os.listdir") as listdir:
            proxy_mod._kill_orphan_on_port(7932, self.UPSTREAM)
        listdir.assert_not_called()


@pytest.mark.skipif(sys.platform != "linux", reason="PR_SET_PDEATHSIG is Linux-only")
class TestSpawnOutlivesStartingThread:
    def test_proxy_survives_the_thread_that_started_it(self):
        # PR_SET_PDEATHSIG fires when the spawning *thread* exits: a proxy
        # started from a short-lived thread (a request thread) must not die
        # with it.
        box = {}

        def run():
            box["proc"] = proxy_mod._popen(
                ["sleep", "30"], preexec_fn=proxy_mod._set_pdeathsig,
            )

        t = threading.Thread(target=run)
        t.start()
        t.join()
        proc = box["proc"]
        try:
            time.sleep(0.3)
            assert proc.poll() is None
        finally:
            proc.kill()
            proc.wait()
