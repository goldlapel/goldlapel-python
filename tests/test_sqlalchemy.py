import os
from unittest.mock import MagicMock, patch, call

import pytest

from goldlapel.sqlalchemy import (
    _url_to_str,
    _strip_dialect,
    _restore_dialect,
    create_engine,
    create_async_engine,
    init,
)
import goldlapel.sqlalchemy as goldlapel_sqlalchemy


PROXY_URL = "postgresql://localhost:7932/mydb"


def _make_url_object(url_str, password=None):
    mock_url = MagicMock()
    mock_url.render_as_string = MagicMock(return_value=url_str)
    masked = url_str
    if password:
        masked = url_str.replace(password, "***")
    mock_url.__str__ = MagicMock(return_value=masked)
    return mock_url


class TestUrlToStr:
    def test_plain_string_passthrough(self):
        assert _url_to_str("postgresql://user:pass@host/db") == "postgresql://user:pass@host/db"

    def test_url_object_uses_render_as_string(self):
        url = _make_url_object("postgresql://user:s3cret@host:5432/db", password="s3cret")
        result = _url_to_str(url)
        assert result == "postgresql://user:s3cret@host:5432/db"
        url.render_as_string.assert_called_once_with(hide_password=False)

    def test_url_object_without_render_as_string_falls_back_to_str(self):
        class PlainUrl:
            def __str__(self):
                return "postgresql://user:pass@host/db"

        assert _url_to_str(PlainUrl()) == "postgresql://user:pass@host/db"


class TestStripDialect:
    def test_strips_asyncpg(self):
        url, dialect = _strip_dialect("postgresql+asyncpg://user:pass@host:5432/db")
        assert url == "postgresql://user:pass@host:5432/db"
        assert dialect == "asyncpg"

    def test_strips_psycopg(self):
        url, dialect = _strip_dialect("postgresql+psycopg://user:pass@host:5432/db")
        assert url == "postgresql://user:pass@host:5432/db"
        assert dialect == "psycopg"

    def test_plain_postgresql_unchanged(self):
        url, dialect = _strip_dialect("postgresql://user:pass@host:5432/db")
        assert url == "postgresql://user:pass@host:5432/db"
        assert dialect is None

    def test_plain_postgres_unchanged(self):
        url, dialect = _strip_dialect("postgres://user:pass@host:5432/db")
        assert url == "postgres://user:pass@host:5432/db"
        assert dialect is None


class TestRestoreDialect:
    def test_restores_asyncpg(self):
        result = _restore_dialect("postgresql://localhost:7932/db", "asyncpg")
        assert result == "postgresql+asyncpg://localhost:7932/db"

    def test_noop_when_dialect_is_none(self):
        result = _restore_dialect("postgresql://localhost:7932/db", None)
        assert result == "postgresql://localhost:7932/db"


class TestCreateEngine:
    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_starts_proxy_and_returns_engine(self, mock_sa, mock_gl):
        mock_gl.start.return_value.url = PROXY_URL
        mock_sa.return_value = MagicMock()

        engine = create_engine("postgresql://user:pass@host:5432/db")

        mock_gl.start.assert_called_once_with(
            "postgresql://user:pass@host:5432/db", client="sqlalchemy"
        )
        # No wrapper-side cache: SQLAlchemy connects to the proxy URL with
        # its own driver — no creator is injected.
        assert mock_sa.call_count == 1
        sa_kwargs = mock_sa.call_args
        assert sa_kwargs[0] == (PROXY_URL,)
        assert "creator" not in sa_kwargs[1]
        assert engine is mock_sa.return_value

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_user_creator_passed_through(self, mock_sa, mock_gl):
        mock_gl.start.return_value.url = PROXY_URL
        user_creator = MagicMock()

        create_engine("postgresql://host/db", creator=user_creator)

        assert mock_sa.call_args[1]["creator"] is user_creator

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_strips_and_restores_dialect(self, mock_sa, mock_gl):
        mock_gl.start.return_value.url = PROXY_URL

        create_engine("postgresql+asyncpg://user:pass@host:5432/db")

        mock_gl.start.assert_called_once_with(
            "postgresql://user:pass@host:5432/db", client="sqlalchemy"
        )
        assert mock_sa.call_args[0] == ("postgresql+asyncpg://localhost:7932/mydb",)

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_pops_goldlapel_port(self, mock_sa, mock_gl):
        mock_gl.start.return_value.url = PROXY_URL

        create_engine("postgresql://host/db", goldlapel_proxy_port=9000)

        mock_gl.start.assert_called_once_with(
            "postgresql://host/db", client="sqlalchemy", proxy_port=9000
        )
        # goldlapel_proxy_port must not leak to SQLAlchemy
        sa_kwargs = mock_sa.call_args[1]
        assert "goldlapel_proxy_port" not in sa_kwargs

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_pops_goldlapel_extra_args(self, mock_sa, mock_gl):
        mock_gl.start.return_value.url = PROXY_URL
        extra = ["--threshold-duration-ms", "200"]

        create_engine("postgresql://host/db", goldlapel_extra_args=extra)

        mock_gl.start.assert_called_once_with(
            "postgresql://host/db", client="sqlalchemy", extra_args=extra
        )
        sa_kwargs = mock_sa.call_args[1]
        assert "goldlapel_extra_args" not in sa_kwargs

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_pops_goldlapel_config(self, mock_sa, mock_gl):
        mock_gl.start.return_value.url = PROXY_URL
        cfg = {"mode": "waiter", "pool_size": 30}

        create_engine("postgresql://host/db", goldlapel_config=cfg)

        mock_gl.start.assert_called_once_with(
            "postgresql://host/db", client="sqlalchemy", config=cfg
        )
        # goldlapel_config must not leak to SQLAlchemy
        sa_kwargs = mock_sa.call_args[1]
        assert "goldlapel_config" not in sa_kwargs

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_passes_remaining_kwargs(self, mock_sa, mock_gl):
        mock_gl.start.return_value.url = PROXY_URL

        create_engine("postgresql://host/db", echo=True, pool_size=5)

        sa_kwargs = mock_sa.call_args[1]
        assert sa_kwargs["echo"] is True
        assert sa_kwargs["pool_size"] == 5

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_url_object_preserves_password(self, mock_sa, mock_gl):
        mock_gl.start.return_value.url = PROXY_URL
        url = _make_url_object("postgresql://user:s3cret@host:5432/db", password="s3cret")

        create_engine(url)

        mock_gl.start.assert_called_once_with(
            "postgresql://user:s3cret@host:5432/db", client="sqlalchemy"
        )

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_url_object_with_dialect_preserves_password(self, mock_sa, mock_gl):
        mock_gl.start.return_value.url = PROXY_URL
        url = _make_url_object(
            "postgresql+psycopg://user:s3cret@host:5432/db", password="s3cret"
        )

        create_engine(url)

        mock_gl.start.assert_called_once_with(
            "postgresql://user:s3cret@host:5432/db", client="sqlalchemy"
        )


class TestCreateAsyncEngine:
    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("sqlalchemy.ext.asyncio.create_async_engine")
    def test_starts_proxy_and_returns_async_engine(self, mock_sa_async, mock_gl):
        mock_gl.start.return_value.url = PROXY_URL
        mock_sa_async.return_value = MagicMock()

        engine = create_async_engine("postgresql+asyncpg://user:pass@host:5432/db")

        mock_gl.start.assert_called_once_with(
            "postgresql://user:pass@host:5432/db", client="sqlalchemy"
        )
        mock_sa_async.assert_called_once_with("postgresql+asyncpg://localhost:7932/mydb")
        assert engine is mock_sa_async.return_value

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("sqlalchemy.ext.asyncio.create_async_engine")
    def test_pops_goldlapel_config(self, mock_sa_async, mock_gl):
        mock_gl.start.return_value.url = PROXY_URL
        cfg = {"mode": "waiter", "pool_size": 30}

        create_async_engine("postgresql+asyncpg://host/db", goldlapel_config=cfg)

        mock_gl.start.assert_called_once_with(
            "postgresql://host/db", client="sqlalchemy", config=cfg
        )
        # goldlapel_config must not leak to SQLAlchemy
        mock_sa_async.assert_called_once_with("postgresql+asyncpg://localhost:7932/mydb")

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("sqlalchemy.ext.asyncio.create_async_engine")
    def test_passes_remaining_kwargs(self, mock_sa_async, mock_gl):
        mock_gl.start.return_value.url = PROXY_URL

        create_async_engine("postgresql+asyncpg://host/db", echo=True, pool_size=5)

        mock_sa_async.assert_called_once_with(
            "postgresql+asyncpg://localhost:7932/mydb", echo=True, pool_size=5
        )


class TestInit:
    @pytest.fixture(autouse=True)
    def _restore_database_url(self, monkeypatch):
        # init() rewrites DATABASE_URL in os.environ; monkeypatch puts the
        # original back after each test.
        monkeypatch.delenv("DATABASE_URL", raising=False)

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_rewrites_database_url(self, mock_gl, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@host:5432/db")
        mock_gl.start.return_value.url = PROXY_URL

        init()

        assert os.environ["DATABASE_URL"] == PROXY_URL

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_explicit_url_over_env(self, mock_gl, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://old@host/db")
        mock_gl.start.return_value.url = PROXY_URL

        init(url="postgresql://new@host/db")

        mock_gl.start.assert_called_once_with(
            "postgresql://new@host/db", client="sqlalchemy"
        )

    def test_raises_when_no_url(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        with pytest.raises(ValueError, match="DATABASE_URL not set"):
            init()

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_returns_proxy_url(self, mock_gl):
        mock_gl.start.return_value.url = PROXY_URL

        result = init(url="postgresql://host/db")

        assert result == PROXY_URL

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_preserves_dialect_suffix(self, mock_gl, monkeypatch):
        mock_gl.start.return_value.url = PROXY_URL
        monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pass@host:5432/db")

        init()

        mock_gl.start.assert_called_once_with(
            "postgresql://user:pass@host:5432/db", client="sqlalchemy"
        )
        assert os.environ["DATABASE_URL"] == "postgresql+asyncpg://localhost:7932/mydb"

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_passes_config(self, mock_gl):
        mock_gl.start.return_value.url = PROXY_URL
        cfg = {"mode": "waiter", "pool_size": 30}

        init(url="postgresql://host/db", config=cfg)

        mock_gl.start.assert_called_once_with(
            "postgresql://host/db", client="sqlalchemy", config=cfg
        )

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_url_object_preserves_password(self, mock_gl):
        mock_gl.start.return_value.url = PROXY_URL
        url = _make_url_object("postgresql://user:s3cret@host:5432/db", password="s3cret")

        init(url=url)

        mock_gl.start.assert_called_once_with(
            "postgresql://user:s3cret@host:5432/db", client="sqlalchemy"
        )


class TestReExports:
    def test_start(self):
        assert goldlapel_sqlalchemy.start is goldlapel_sqlalchemy.goldlapel.start

    def test_stop(self):
        assert goldlapel_sqlalchemy.stop is goldlapel_sqlalchemy.goldlapel.stop

    def test_proxy_url(self):
        assert goldlapel_sqlalchemy.proxy_url is goldlapel_sqlalchemy.goldlapel.proxy_url

    def test_goldlapel_class(self):
        assert goldlapel_sqlalchemy.GoldLapel is goldlapel_sqlalchemy.goldlapel.GoldLapel

    def test_default_port(self):
        assert goldlapel_sqlalchemy.DEFAULT_PROXY_PORT is goldlapel_sqlalchemy.goldlapel.DEFAULT_PROXY_PORT



# Every keyword option of the core `goldlapel.start`, with a non-default
# value. The SQLAlchemy integration forwards each one as
# `goldlapel_<name>` (engine kwargs) or `<name>` (init).
_ALL_START_OPTIONS = {
    "proxy_port": 9000,
    "dashboard_port": 9001,
    "log_level": "debug",
    "mode": "waiter",
    "license": "/etc/gl/license.pem",
    "api_key": "gl_test_abc",
    "client": "my-app",
    "config_file": "/etc/gl/goldlapel.toml",
    "config": {"pool_size": 30},
    "extra_args": ["--verbose"],
    "silent": True,
    "mesh": True,
    "mesh_tag": "eu",
    "disable_proxy_cache": True,
    "disable_sqloptimize": True,
    "disable_auto_indexes": True,
}


def _core_start_options():
    import inspect
    from goldlapel.proxy import start as core_start
    return {
        name for name, p in inspect.signature(core_start).parameters.items()
        if p.kind is inspect.Parameter.KEYWORD_ONLY
    }


class TestOptionForwarding:
    def test_option_table_covers_core_signature(self):
        assert set(_ALL_START_OPTIONS) == _core_start_options()

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_create_engine_forwards_every_option(self, mock_sa, mock_gl):
        mock_gl.start.return_value.url = PROXY_URL
        kwargs = {f"goldlapel_{k}": v for k, v in _ALL_START_OPTIONS.items()}

        create_engine("postgresql://host/db", echo=True, **kwargs)

        mock_gl.start.assert_called_once_with("postgresql://host/db", **_ALL_START_OPTIONS)
        # None of the goldlapel_* kwargs leak to SQLAlchemy.
        assert mock_sa.call_args[1] == {"echo": True}

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("sqlalchemy.ext.asyncio.create_async_engine")
    def test_create_async_engine_forwards_every_option(self, mock_sa_async, mock_gl):
        mock_gl.start.return_value.url = PROXY_URL
        kwargs = {f"goldlapel_{k}": v for k, v in _ALL_START_OPTIONS.items()}

        create_async_engine("postgresql+asyncpg://host/db", **kwargs)

        mock_gl.start.assert_called_once_with("postgresql://host/db", **_ALL_START_OPTIONS)
        mock_sa_async.assert_called_once_with("postgresql+asyncpg://localhost:7932/mydb")

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_init_forwards_every_option(self, mock_gl, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        mock_gl.start.return_value.url = PROXY_URL

        init("postgresql://host/db", **_ALL_START_OPTIONS)

        mock_gl.start.assert_called_once_with("postgresql://host/db", **_ALL_START_OPTIONS)


class TestMultipleEngines:
    """Two engines for different databases each get their own proxy, and
    each engine connects to its own proxy's URL."""

    def setup_method(self):
        import goldlapel.proxy as proxy_mod
        proxy_mod._instances.clear()

    def teardown_method(self):
        import goldlapel.proxy as proxy_mod
        proxy_mod._instances.clear()

    @patch("goldlapel.proxy._detect_sync_driver",
           side_effect=lambda: ("psycopg3", MagicMock()))
    @patch("goldlapel.proxy._kill_orphan_on_port")
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_two_engines_get_their_own_proxy_urls(
        self, mock_sa, mock_find, mock_popen, mock_wait, mock_orphan, mock_detect,
    ):
        def popen(*args, **kwargs):
            proc = MagicMock()
            proc.poll.return_value = None
            return proc
        mock_popen.side_effect = popen

        create_engine("postgresql+psycopg://u:p@h:5432/main", goldlapel_silent=True)
        create_engine("postgresql+psycopg://u:p@h:5432/analytics", goldlapel_silent=True)

        first, second = (c[0][0] for c in mock_sa.call_args_list)
        assert first.startswith("postgresql+psycopg://u:p@localhost:7932/main")
        assert second.startswith("postgresql+psycopg://u:p@localhost:7934/analytics")


class TestUnknownOptions:
    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_create_engine_rejects_removed_kwarg(self, mock_sa, mock_gl):
        with pytest.raises(TypeError) as exc:
            create_engine("postgresql://host/db", goldlapel_invalidation_port=7934)
        assert "goldlapel_invalidation_port (removed with the in-process cache)" in str(exc.value)
        mock_gl.start.assert_not_called()
        mock_sa.assert_not_called()

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_create_async_engine_rejects_unknown_kwarg(self, mock_gl):
        with pytest.raises(TypeError, match="goldlapel_native_cache"):
            create_async_engine("postgresql+asyncpg://host/db", goldlapel_native_cache=False)
        mock_gl.start.assert_not_called()

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_init_rejects_removed_option(self, mock_gl):
        with pytest.raises(TypeError, match="aggressive_verify \\(removed with the in-process cache\\)"):
            init("postgresql://host/db", aggressive_verify="always")
        mock_gl.start.assert_not_called()


class TestEngineUrlDropsUpstreamTls:
    @patch("goldlapel.proxy._detect_sync_driver",
           side_effect=lambda: ("psycopg3", MagicMock()))
    @patch("goldlapel.proxy._kill_orphan_on_port")
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_sslmode_goes_upstream_not_to_the_engine(
        self, mock_sa, mock_find, mock_popen, mock_wait, mock_orphan, mock_detect,
    ):
        mock_popen.side_effect = lambda *a, **kw: MagicMock(**{"poll.return_value": None})
        create_engine("postgresql+psycopg://u:p@h:5432/main?sslmode=require",
                      goldlapel_silent=True)

        engine_url = mock_sa.call_args[0][0]
        assert engine_url.startswith("postgresql+psycopg://u:p@localhost:7932/main")
        assert "sslmode" not in engine_url
        cmd = mock_popen.call_args[0][0]
        assert cmd[cmd.index("--upstream") + 1] == "postgresql://u:p@h:5432/main?sslmode=require"
