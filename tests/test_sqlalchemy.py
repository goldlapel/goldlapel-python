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
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PROXY_PORT = 7932
        mock_sa.return_value = MagicMock()

        engine = create_engine("postgresql://user:pass@host:5432/db")

        mock_gl.start.assert_called_once_with(
            "postgresql://user:pass@host:5432/db", proxy_port=None, dashboard_port=None, log_level=None, mode=None, client="sqlalchemy", config=None, extra_args=None
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
        mock_gl.proxy_url.return_value = PROXY_URL
        user_creator = MagicMock()

        create_engine("postgresql://host/db", creator=user_creator)

        assert mock_sa.call_args[1]["creator"] is user_creator

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_strips_and_restores_dialect(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PROXY_PORT = 7932

        create_engine("postgresql+asyncpg://user:pass@host:5432/db")

        mock_gl.start.assert_called_once_with(
            "postgresql://user:pass@host:5432/db", proxy_port=None, dashboard_port=None, log_level=None, mode=None, client="sqlalchemy", config=None, extra_args=None
        )
        assert mock_sa.call_args[0] == ("postgresql+asyncpg://localhost:7932/mydb",)

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_pops_goldlapel_port(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PROXY_PORT = 7932

        create_engine("postgresql://host/db", goldlapel_proxy_port=9000)

        mock_gl.start.assert_called_once_with(
            "postgresql://host/db", proxy_port=9000, dashboard_port=None, log_level=None, mode=None, client="sqlalchemy", config=None, extra_args=None
        )
        # goldlapel_port must not leak to SQLAlchemy
        sa_kwargs = mock_sa.call_args[1]
        assert "goldlapel_port" not in sa_kwargs

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_pops_goldlapel_extra_args(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PROXY_PORT = 7932
        extra = ["--threshold-duration-ms", "200"]

        create_engine("postgresql://host/db", goldlapel_extra_args=extra)

        mock_gl.start.assert_called_once_with(
            "postgresql://host/db", proxy_port=None, dashboard_port=None, log_level=None, mode=None, client="sqlalchemy", config=None, extra_args=extra
        )
        sa_kwargs = mock_sa.call_args[1]
        assert "goldlapel_extra_args" not in sa_kwargs

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_pops_goldlapel_config(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PROXY_PORT = 7932
        cfg = {"mode": "waiter", "pool_size": 30}

        create_engine("postgresql://host/db", goldlapel_config=cfg)

        mock_gl.start.assert_called_once_with(
            "postgresql://host/db", proxy_port=None, dashboard_port=None, log_level=None, mode=None, client="sqlalchemy", config=cfg, extra_args=None
        )
        # goldlapel_config must not leak to SQLAlchemy
        sa_kwargs = mock_sa.call_args[1]
        assert "goldlapel_config" not in sa_kwargs

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_passes_remaining_kwargs(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PROXY_PORT = 7932

        create_engine("postgresql://host/db", echo=True, pool_size=5)

        sa_kwargs = mock_sa.call_args[1]
        assert sa_kwargs["echo"] is True
        assert sa_kwargs["pool_size"] == 5

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_url_object_preserves_password(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PROXY_PORT = 7932
        url = _make_url_object("postgresql://user:s3cret@host:5432/db", password="s3cret")

        create_engine(url)

        mock_gl.start.assert_called_once_with(
            "postgresql://user:s3cret@host:5432/db", proxy_port=None, dashboard_port=None, log_level=None, mode=None, client="sqlalchemy", config=None, extra_args=None
        )

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_url_object_with_dialect_preserves_password(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PROXY_PORT = 7932
        url = _make_url_object(
            "postgresql+psycopg://user:s3cret@host:5432/db", password="s3cret"
        )

        create_engine(url)

        mock_gl.start.assert_called_once_with(
            "postgresql://user:s3cret@host:5432/db", proxy_port=None, dashboard_port=None, log_level=None, mode=None, client="sqlalchemy", config=None, extra_args=None
        )


class TestCreateAsyncEngine:
    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("sqlalchemy.ext.asyncio.create_async_engine")
    def test_starts_proxy_and_returns_async_engine(self, mock_sa_async, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PROXY_PORT = 7932
        mock_sa_async.return_value = MagicMock()

        engine = create_async_engine("postgresql+asyncpg://user:pass@host:5432/db")

        mock_gl.start.assert_called_once_with(
            "postgresql://user:pass@host:5432/db", proxy_port=None, dashboard_port=None, log_level=None, mode=None, client="sqlalchemy", config=None, extra_args=None
        )
        mock_sa_async.assert_called_once_with("postgresql+asyncpg://localhost:7932/mydb")
        assert engine is mock_sa_async.return_value

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("sqlalchemy.ext.asyncio.create_async_engine")
    def test_pops_goldlapel_config(self, mock_sa_async, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PROXY_PORT = 7932
        cfg = {"mode": "waiter", "pool_size": 30}

        create_async_engine("postgresql+asyncpg://host/db", goldlapel_config=cfg)

        mock_gl.start.assert_called_once_with(
            "postgresql://host/db", proxy_port=None, dashboard_port=None, log_level=None, mode=None, client="sqlalchemy", config=cfg, extra_args=None
        )
        # goldlapel_config must not leak to SQLAlchemy
        mock_sa_async.assert_called_once_with("postgresql+asyncpg://localhost:7932/mydb")

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("sqlalchemy.ext.asyncio.create_async_engine")
    def test_passes_remaining_kwargs(self, mock_sa_async, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PROXY_PORT = 7932

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
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PROXY_PORT = 7932

        init()

        assert os.environ["DATABASE_URL"] == PROXY_URL

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_explicit_url_over_env(self, mock_gl, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://old@host/db")
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PROXY_PORT = 7932

        init(url="postgresql://new@host/db")

        mock_gl.start.assert_called_once_with(
            "postgresql://new@host/db", proxy_port=None, dashboard_port=None, log_level=None, mode=None, client="sqlalchemy", config=None, extra_args=None
        )

    def test_raises_when_no_url(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        with pytest.raises(ValueError, match="DATABASE_URL not set"):
            init()

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_returns_proxy_url(self, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PROXY_PORT = 7932

        result = init(url="postgresql://host/db")

        assert result == PROXY_URL

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_preserves_dialect_suffix(self, mock_gl, monkeypatch):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PROXY_PORT = 7932
        monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pass@host:5432/db")

        init()

        mock_gl.start.assert_called_once_with(
            "postgresql://user:pass@host:5432/db", proxy_port=None, dashboard_port=None, log_level=None, mode=None, client="sqlalchemy", config=None, extra_args=None
        )
        assert os.environ["DATABASE_URL"] == "postgresql+asyncpg://localhost:7932/mydb"

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_passes_config(self, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PROXY_PORT = 7932
        cfg = {"mode": "waiter", "pool_size": 30}

        init(url="postgresql://host/db", config=cfg)

        mock_gl.start.assert_called_once_with(
            "postgresql://host/db", proxy_port=None, dashboard_port=None, log_level=None, mode=None, client="sqlalchemy", config=cfg, extra_args=None
        )

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_url_object_preserves_password(self, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PROXY_PORT = 7932
        url = _make_url_object("postgresql://user:s3cret@host:5432/db", password="s3cret")

        init(url=url)

        mock_gl.start.assert_called_once_with(
            "postgresql://user:s3cret@host:5432/db", proxy_port=None, dashboard_port=None, log_level=None, mode=None, client="sqlalchemy", config=None, extra_args=None
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

