from unittest.mock import MagicMock, patch

import pytest

from goldlapel.django.base import DatabaseWrapper, _build_upstream_url


# --- _build_upstream_url ---


class TestBuildUpstreamUrl:
    def test_standard(self):
        settings = {"HOST": "db.example.com", "PORT": "5432", "NAME": "mydb",
                     "USER": "admin", "PASSWORD": "secret"}
        assert _build_upstream_url(settings) == "postgresql://admin:secret@db.example.com:5432/mydb"

    def test_empty_host_defaults_to_localhost(self):
        assert "localhost:" in _build_upstream_url({"HOST": "", "PORT": "5432", "NAME": "db"})

    def test_none_host_defaults_to_localhost(self):
        assert "localhost:" in _build_upstream_url({"HOST": None, "PORT": "5432", "NAME": "db"})

    def test_empty_port_defaults_to_5432(self):
        assert ":5432/" in _build_upstream_url({"HOST": "h", "PORT": "", "NAME": "db"})

    def test_none_port_defaults_to_5432(self):
        assert ":5432/" in _build_upstream_url({"HOST": "h", "PORT": None, "NAME": "db"})

    def test_special_chars_in_password(self):
        url = _build_upstream_url({"HOST": "h", "PORT": "5432", "NAME": "db",
                                   "USER": "u", "PASSWORD": "@:/"})
        assert "u:%40%3A%2F@" in url

    def test_special_chars_in_user(self):
        url = _build_upstream_url({"HOST": "h", "PORT": "5432", "NAME": "db",
                                   "USER": "u@ser", "PASSWORD": "p"})
        assert "u%40ser:p@" in url

    def test_no_user_no_password(self):
        url = _build_upstream_url({"HOST": "h", "PORT": "5432", "NAME": "db"})
        assert url == "postgresql://h:5432/db"

    def test_user_without_password(self):
        url = _build_upstream_url({"HOST": "h", "PORT": "5432", "NAME": "db",
                                   "USER": "admin"})
        assert "admin@h:" in url
        assert ":admin" not in url  # no colon before user in userinfo

    def test_special_chars_in_name(self):
        url = _build_upstream_url({"HOST": "h", "PORT": "5432", "NAME": "my#db?v=1",
                                   "USER": "u", "PASSWORD": "p"})
        assert url.endswith("/my%23db%3Fv%3D1")

    def test_unix_socket_raises(self):
        with pytest.raises(ValueError, match="Unix socket"):
            _build_upstream_url({"HOST": "/var/run/postgresql", "PORT": "5432", "NAME": "db"})


# --- DatabaseWrapper.get_connection_params ---


GOLDLAPEL_DEFAULT_PROXY_PORT = 7932


def _make_wrapper(settings_dict):
    wrapper = MagicMock(spec=DatabaseWrapper)
    wrapper.settings_dict = settings_dict
    return wrapper


class TestGetConnectionParams:
    @patch("goldlapel.django.base.goldlapel")
    @patch("goldlapel.django.base.PgDatabaseWrapper.get_connection_params")
    def test_starts_proxy_and_swaps_host_port(self, mock_super, mock_gl):
        mock_gl.DEFAULT_PROXY_PORT = GOLDLAPEL_DEFAULT_PROXY_PORT
        mock_super.return_value = {"host": "db.example.com", "port": 5432}
        settings = {"HOST": "db.example.com", "PORT": "5432", "NAME": "mydb",
                     "USER": "u", "PASSWORD": "p"}

        wrapper = _make_wrapper(settings)
        params = DatabaseWrapper.get_connection_params(wrapper)

        mock_gl.start.assert_called_once()
        assert params["host"] == "127.0.0.1"
        assert params["port"] == GOLDLAPEL_DEFAULT_PROXY_PORT

    @patch("goldlapel.django.base.goldlapel")
    @patch("goldlapel.django.base.PgDatabaseWrapper.get_connection_params")
    def test_custom_port(self, mock_super, mock_gl):
        mock_gl.DEFAULT_PROXY_PORT = GOLDLAPEL_DEFAULT_PROXY_PORT
        mock_super.return_value = {"host": "h", "port": 5432,
                                   "goldlapel": {"proxy_port": 9000}}
        wrapper = _make_wrapper({"HOST": "h", "PORT": "5432", "NAME": "db",
                                 "USER": "u", "PASSWORD": "p"})
        params = DatabaseWrapper.get_connection_params(wrapper)

        mock_gl.start.assert_called_once_with(
            _build_upstream_url(wrapper.settings_dict),
            proxy_port=9000, client="django",
        )
        assert params["port"] == 9000

    @patch("goldlapel.django.base.goldlapel")
    @patch("goldlapel.django.base.PgDatabaseWrapper.get_connection_params")
    def test_extra_args(self, mock_super, mock_gl):
        mock_gl.DEFAULT_PROXY_PORT = GOLDLAPEL_DEFAULT_PROXY_PORT
        extra = ["--threshold-duration-ms", "200"]
        mock_super.return_value = {"host": "h", "port": 5432,
                                   "goldlapel": {"extra_args": extra}}
        wrapper = _make_wrapper({"HOST": "h", "PORT": "5432", "NAME": "db",
                                 "USER": "u", "PASSWORD": "p"})
        DatabaseWrapper.get_connection_params(wrapper)

        mock_gl.start.assert_called_once_with(
            _build_upstream_url(wrapper.settings_dict),
            proxy_port=GOLDLAPEL_DEFAULT_PROXY_PORT, client="django",
            extra_args=extra,
        )

    @patch("goldlapel.django.base.goldlapel")
    @patch("goldlapel.django.base.PgDatabaseWrapper.get_connection_params")
    def test_goldlapel_key_removed_from_params(self, mock_super, mock_gl):
        mock_gl.DEFAULT_PROXY_PORT = GOLDLAPEL_DEFAULT_PROXY_PORT
        mock_super.return_value = {"host": "h", "port": 5432,
                                   "goldlapel": {"proxy_port": 9000}}
        wrapper = _make_wrapper({"HOST": "h", "PORT": "5432", "NAME": "db",
                                 "USER": "u", "PASSWORD": "p"})
        params = DatabaseWrapper.get_connection_params(wrapper)

        assert "goldlapel" not in params

    @patch("goldlapel.django.base.goldlapel")
    @patch("goldlapel.django.base.PgDatabaseWrapper.get_connection_params")
    def test_config_dict(self, mock_super, mock_gl):
        mock_gl.DEFAULT_PROXY_PORT = GOLDLAPEL_DEFAULT_PROXY_PORT
        cfg = {"pool_size": 30, "disable_n1": True}
        mock_super.return_value = {"host": "h", "port": 5432,
                                   "goldlapel": {"config": cfg}}
        wrapper = _make_wrapper({"HOST": "h", "PORT": "5432", "NAME": "db",
                                 "USER": "u", "PASSWORD": "p"})
        DatabaseWrapper.get_connection_params(wrapper)

        mock_gl.start.assert_called_once_with(
            _build_upstream_url(wrapper.settings_dict),
            proxy_port=GOLDLAPEL_DEFAULT_PROXY_PORT, client="django",
            config=cfg,
        )

    @patch("goldlapel.django.base.goldlapel")
    @patch("goldlapel.django.base.PgDatabaseWrapper.get_connection_params")
    def test_config_with_port_and_extra_args(self, mock_super, mock_gl):
        mock_gl.DEFAULT_PROXY_PORT = GOLDLAPEL_DEFAULT_PROXY_PORT
        cfg = {"pool_size": 30}
        extra = ["--verbose"]
        mock_super.return_value = {"host": "h", "port": 5432,
                                   "goldlapel": {"config": cfg,
                                                 "proxy_port": 9000,
                                                 "mode": "waiter",
                                                 "extra_args": extra}}
        wrapper = _make_wrapper({"HOST": "h", "PORT": "5432", "NAME": "db",
                                 "USER": "u", "PASSWORD": "p"})
        params = DatabaseWrapper.get_connection_params(wrapper)

        mock_gl.start.assert_called_once_with(
            _build_upstream_url(wrapper.settings_dict),
            proxy_port=9000, client="django",
            mode="waiter", config=cfg, extra_args=extra,
        )
        assert params["port"] == 9000

    @patch("goldlapel.django.base.goldlapel")
    @patch("goldlapel.django.base.PgDatabaseWrapper.get_connection_params")
    def test_no_options_uses_defaults(self, mock_super, mock_gl):
        mock_gl.DEFAULT_PROXY_PORT = GOLDLAPEL_DEFAULT_PROXY_PORT
        mock_super.return_value = {"host": "h", "port": 5432}
        wrapper = _make_wrapper({"HOST": "h", "PORT": "5432", "NAME": "db",
                                 "USER": "u", "PASSWORD": "p"})
        params = DatabaseWrapper.get_connection_params(wrapper)

        mock_gl.start.assert_called_once_with(
            _build_upstream_url(wrapper.settings_dict),
            proxy_port=GOLDLAPEL_DEFAULT_PROXY_PORT, client="django",
        )
        assert params["host"] == "127.0.0.1"
        assert params["port"] == GOLDLAPEL_DEFAULT_PROXY_PORT


class TestProxyStartFailureFallback:
    @patch("goldlapel.django.base.goldlapel")
    @patch("goldlapel.django.base.PgDatabaseWrapper.get_connection_params")
    def test_fallback_preserves_original_host_port(self, mock_super, mock_gl):
        mock_gl.DEFAULT_PROXY_PORT = GOLDLAPEL_DEFAULT_PROXY_PORT
        mock_super.return_value = {"host": "db.example.com", "port": 5432}
        mock_gl.start.side_effect = RuntimeError("binary not found")

        wrapper = _make_wrapper({"HOST": "db.example.com", "PORT": "5432", "NAME": "db",
                                 "USER": "u", "PASSWORD": "p"})
        params = DatabaseWrapper.get_connection_params(wrapper)

        assert params["host"] == "db.example.com"
        assert params["port"] == 5432
        assert wrapper._gl_active is False

    @patch("goldlapel.django.base.goldlapel")
    @patch("goldlapel.django.base.PgDatabaseWrapper.get_connection_params")
    def test_fallback_logs_warning(self, mock_super, mock_gl):
        mock_gl.DEFAULT_PROXY_PORT = GOLDLAPEL_DEFAULT_PROXY_PORT
        mock_super.return_value = {"host": "h", "port": 5432}
        mock_gl.start.side_effect = OSError("port conflict")

        wrapper = _make_wrapper({"HOST": "h", "PORT": "5432", "NAME": "db",
                                 "USER": "u", "PASSWORD": "p"})
        import logging
        with patch.object(logging.getLogger("goldlapel.django"), "warning") as mock_warn:
            DatabaseWrapper.get_connection_params(wrapper)
            mock_warn.assert_called_once()
            assert "falling back to direct connection" in mock_warn.call_args[0][0]

    @patch("goldlapel.django.base.goldlapel")
    @patch("goldlapel.django.base.PgDatabaseWrapper.get_connection_params")
    def test_gl_active_true_on_success(self, mock_super, mock_gl):
        mock_gl.DEFAULT_PROXY_PORT = GOLDLAPEL_DEFAULT_PROXY_PORT
        mock_super.return_value = {"host": "h", "port": 5432}

        wrapper = _make_wrapper({"HOST": "h", "PORT": "5432", "NAME": "db",
                                 "USER": "u", "PASSWORD": "p"})
        DatabaseWrapper.get_connection_params(wrapper)

        assert wrapper._gl_active is True

    @patch("goldlapel.django.base.goldlapel")
    @patch("goldlapel.django.base.PgDatabaseWrapper.get_connection_params")
    def test_fallback_catches_file_not_found(self, mock_super, mock_gl):
        mock_gl.DEFAULT_PROXY_PORT = GOLDLAPEL_DEFAULT_PROXY_PORT
        mock_super.return_value = {"host": "h", "port": 5432}
        mock_gl.start.side_effect = FileNotFoundError("goldlapel binary not found")

        wrapper = _make_wrapper({"HOST": "h", "PORT": "5432", "NAME": "db",
                                 "USER": "u", "PASSWORD": "p"})
        params = DatabaseWrapper.get_connection_params(wrapper)

        assert params["host"] == "h"
        assert wrapper._gl_active is False


class TestGetNewConnection:
    @patch("goldlapel.django.base.goldlapel")
    @patch("goldlapel.django.base.PgDatabaseWrapper.get_new_connection")
    def test_wraps_connection_with_native_cache(self, mock_super, mock_gl):
        mock_gl.DEFAULT_PROXY_PORT = GOLDLAPEL_DEFAULT_PROXY_PORT
        mock_conn = MagicMock()
        mock_super.return_value = mock_conn
        mock_gl.wrap.return_value = MagicMock()

        wrapper = _make_wrapper({
            "HOST": "h", "PORT": "5432", "NAME": "db",
            "USER": "u", "PASSWORD": "p",
            "OPTIONS": {},
        })
        wrapper._gl_proxy_port = GOLDLAPEL_DEFAULT_PROXY_PORT
        wrapper._gl_active = True
        result = DatabaseWrapper.get_new_connection(wrapper, {"host": "127.0.0.1"})

        mock_gl.wrap.assert_called_once_with(mock_conn, invalidation_port=GOLDLAPEL_DEFAULT_PROXY_PORT + 2)
        assert result == mock_gl.wrap.return_value

    @patch("goldlapel.django.base.goldlapel")
    @patch("goldlapel.django.base.PgDatabaseWrapper.get_new_connection")
    def test_custom_invalidation_port(self, mock_super, mock_gl):
        mock_gl.DEFAULT_PROXY_PORT = GOLDLAPEL_DEFAULT_PROXY_PORT
        mock_super.return_value = MagicMock()
        mock_gl.wrap.return_value = MagicMock()

        wrapper = _make_wrapper({
            "HOST": "h", "PORT": "5432", "NAME": "db",
            "USER": "u", "PASSWORD": "p",
            "OPTIONS": {"goldlapel": {"invalidation_port": 9999}},
        })
        wrapper._gl_proxy_port = GOLDLAPEL_DEFAULT_PROXY_PORT
        wrapper._gl_active = True
        DatabaseWrapper.get_new_connection(wrapper, {"host": "127.0.0.1"})

        mock_gl.wrap.assert_called_once_with(mock_super.return_value, invalidation_port=9999)

    @patch("goldlapel.django.base.goldlapel")
    @patch("goldlapel.django.base.PgDatabaseWrapper.get_new_connection")
    def test_invalidation_port_derived_from_gl_port(self, mock_super, mock_gl):
        mock_gl.DEFAULT_PROXY_PORT = GOLDLAPEL_DEFAULT_PROXY_PORT
        mock_super.return_value = MagicMock()
        mock_gl.wrap.return_value = MagicMock()

        wrapper = _make_wrapper({
            "HOST": "h", "PORT": "5432", "NAME": "db",
            "USER": "u", "PASSWORD": "p",
            "OPTIONS": {"goldlapel": {"proxy_port": 8000}},
        })
        wrapper._gl_proxy_port = 8000
        wrapper._gl_active = True
        DatabaseWrapper.get_new_connection(wrapper, {"host": "127.0.0.1"})

        mock_gl.wrap.assert_called_once_with(mock_super.return_value, invalidation_port=8002)

    @patch("goldlapel.django.base.goldlapel")
    @patch("goldlapel.django.base.PgDatabaseWrapper.get_new_connection")
    def test_skips_wrap_when_proxy_inactive(self, mock_super, mock_gl):
        mock_gl.DEFAULT_PROXY_PORT = GOLDLAPEL_DEFAULT_PROXY_PORT
        mock_conn = MagicMock()
        mock_super.return_value = mock_conn

        wrapper = _make_wrapper({
            "HOST": "h", "PORT": "5432", "NAME": "db",
            "USER": "u", "PASSWORD": "p",
            "OPTIONS": {},
        })
        wrapper._gl_proxy_port = GOLDLAPEL_DEFAULT_PROXY_PORT
        wrapper._gl_active = False
        result = DatabaseWrapper.get_new_connection(wrapper, {"host": "h"})

        mock_gl.wrap.assert_not_called()
        assert result == mock_conn
