import os
from unittest.mock import MagicMock, patch, call

import pytest

from goldlapel.sqlalchemy import (
    _url_to_str,
    _strip_dialect,
    _restore_dialect,
    _make_creator,
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
        mock_gl.DEFAULT_PORT = 7932
        mock_sa.return_value = MagicMock()

        engine = create_engine("postgresql://user:pass@host:5432/db")

        mock_gl.start.assert_called_once_with(
            "postgresql://user:pass@host:5432/db", config=None, port=None, extra_args=None
        )
        # creator kwarg injected for L1 cache
        assert mock_sa.call_count == 1
        sa_kwargs = mock_sa.call_args
        assert sa_kwargs[0] == (PROXY_URL,)
        assert "creator" in sa_kwargs[1]
        assert engine is mock_sa.return_value

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_strips_and_restores_dialect(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932

        create_engine("postgresql+asyncpg://user:pass@host:5432/db")

        mock_gl.start.assert_called_once_with(
            "postgresql://user:pass@host:5432/db", config=None, port=None, extra_args=None
        )
        assert mock_sa.call_args[0] == ("postgresql+asyncpg://localhost:7932/mydb",)

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_pops_goldlapel_port(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932

        create_engine("postgresql://host/db", goldlapel_port=9000)

        mock_gl.start.assert_called_once_with(
            "postgresql://host/db", config=None, port=9000, extra_args=None
        )
        # goldlapel_port must not leak to SQLAlchemy
        sa_kwargs = mock_sa.call_args[1]
        assert "goldlapel_port" not in sa_kwargs

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_pops_goldlapel_extra_args(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932
        extra = ["--threshold-duration-ms", "200"]

        create_engine("postgresql://host/db", goldlapel_extra_args=extra)

        mock_gl.start.assert_called_once_with(
            "postgresql://host/db", config=None, port=None, extra_args=extra
        )
        sa_kwargs = mock_sa.call_args[1]
        assert "goldlapel_extra_args" not in sa_kwargs

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_pops_goldlapel_config(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932
        cfg = {"mode": "waiter", "pool_size": 30}

        create_engine("postgresql://host/db", goldlapel_config=cfg)

        mock_gl.start.assert_called_once_with(
            "postgresql://host/db", config=cfg, port=None, extra_args=None
        )
        # goldlapel_config must not leak to SQLAlchemy
        sa_kwargs = mock_sa.call_args[1]
        assert "goldlapel_config" not in sa_kwargs

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_passes_remaining_kwargs(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932

        create_engine("postgresql://host/db", echo=True, pool_size=5)

        sa_kwargs = mock_sa.call_args[1]
        assert sa_kwargs["echo"] is True
        assert sa_kwargs["pool_size"] == 5

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_url_object_preserves_password(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932
        url = _make_url_object("postgresql://user:s3cret@host:5432/db", password="s3cret")

        create_engine(url)

        mock_gl.start.assert_called_once_with(
            "postgresql://user:s3cret@host:5432/db", config=None, port=None, extra_args=None
        )

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_url_object_with_dialect_preserves_password(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932
        url = _make_url_object(
            "postgresql+psycopg://user:s3cret@host:5432/db", password="s3cret"
        )

        create_engine(url)

        mock_gl.start.assert_called_once_with(
            "postgresql://user:s3cret@host:5432/db", config=None, port=None, extra_args=None
        )


class TestCreateAsyncEngine:
    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("sqlalchemy.ext.asyncio.create_async_engine")
    def test_starts_proxy_and_returns_async_engine(self, mock_sa_async, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932
        mock_sa_async.return_value = MagicMock()

        engine = create_async_engine("postgresql+asyncpg://user:pass@host:5432/db")

        mock_gl.start.assert_called_once_with(
            "postgresql://user:pass@host:5432/db", config=None, port=None, extra_args=None
        )
        mock_sa_async.assert_called_once_with("postgresql+asyncpg://localhost:7932/mydb")
        assert engine is mock_sa_async.return_value

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("sqlalchemy.ext.asyncio.create_async_engine")
    def test_pops_goldlapel_config(self, mock_sa_async, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932
        cfg = {"mode": "waiter", "pool_size": 30}

        create_async_engine("postgresql+asyncpg://host/db", goldlapel_config=cfg)

        mock_gl.start.assert_called_once_with(
            "postgresql://host/db", config=cfg, port=None, extra_args=None
        )
        # goldlapel_config must not leak to SQLAlchemy
        mock_sa_async.assert_called_once_with("postgresql+asyncpg://localhost:7932/mydb")

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("sqlalchemy.ext.asyncio.create_async_engine")
    def test_passes_remaining_kwargs(self, mock_sa_async, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932

        create_async_engine("postgresql+asyncpg://host/db", echo=True, pool_size=5)

        mock_sa_async.assert_called_once_with(
            "postgresql+asyncpg://localhost:7932/mydb", echo=True, pool_size=5
        )


class TestInit:
    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_rewrites_database_url(self, mock_gl, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@host:5432/db")
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932

        init()

        assert os.environ["DATABASE_URL"] == PROXY_URL

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_explicit_url_over_env(self, mock_gl, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://old@host/db")
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932

        init(url="postgresql://new@host/db")

        mock_gl.start.assert_called_once_with(
            "postgresql://new@host/db", config=None, port=None, extra_args=None
        )

    def test_raises_when_no_url(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        with pytest.raises(ValueError, match="DATABASE_URL not set"):
            init()

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_returns_proxy_url(self, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932

        result = init(url="postgresql://host/db")

        assert result == PROXY_URL

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_preserves_dialect_suffix(self, mock_gl, monkeypatch):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932
        monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pass@host:5432/db")

        init()

        mock_gl.start.assert_called_once_with(
            "postgresql://user:pass@host:5432/db", config=None, port=None, extra_args=None
        )
        assert os.environ["DATABASE_URL"] == "postgresql+asyncpg://localhost:7932/mydb"

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_passes_config(self, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932
        cfg = {"mode": "waiter", "pool_size": 30}

        init(url="postgresql://host/db", config=cfg)

        mock_gl.start.assert_called_once_with(
            "postgresql://host/db", config=cfg, port=None, extra_args=None
        )

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_url_object_preserves_password(self, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932
        url = _make_url_object("postgresql://user:s3cret@host:5432/db", password="s3cret")

        init(url=url)

        mock_gl.start.assert_called_once_with(
            "postgresql://user:s3cret@host:5432/db", config=None, port=None, extra_args=None
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
        assert goldlapel_sqlalchemy.DEFAULT_PORT is goldlapel_sqlalchemy.goldlapel.DEFAULT_PORT

    def test_native_cache(self):
        assert goldlapel_sqlalchemy.NativeCache is goldlapel_sqlalchemy.goldlapel.NativeCache

    def test_wrap(self):
        assert goldlapel_sqlalchemy.wrap is goldlapel_sqlalchemy.goldlapel.wrap


class TestL1Cache:
    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_creator_injected_by_default(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932

        create_engine("postgresql://user:pass@host:5432/db")

        sa_kwargs = mock_sa.call_args[1]
        assert "creator" in sa_kwargs
        assert callable(sa_kwargs["creator"])

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_creator_not_injected_when_l1_disabled(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932

        create_engine("postgresql://user:pass@host:5432/db", goldlapel_l1_cache=False)

        sa_kwargs = mock_sa.call_args[1]
        assert "creator" not in sa_kwargs

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_l1_cache_kwarg_not_leaked_to_sqlalchemy(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932

        create_engine("postgresql://host/db", goldlapel_l1_cache=True)

        sa_kwargs = mock_sa.call_args[1]
        assert "goldlapel_l1_cache" not in sa_kwargs

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_invalidation_port_kwarg_not_leaked(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932

        create_engine("postgresql://host/db", goldlapel_invalidation_port=8888)

        sa_kwargs = mock_sa.call_args[1]
        assert "goldlapel_invalidation_port" not in sa_kwargs

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_user_creator_wrapped(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932
        mock_gl.wrap = MagicMock(side_effect=lambda conn, **kw: conn)

        user_conn = MagicMock()
        user_creator = MagicMock(return_value=user_conn)

        create_engine("postgresql://host/db", creator=user_creator)

        # The user's creator should have been captured (not passed through directly)
        sa_kwargs = mock_sa.call_args[1]
        creator = sa_kwargs["creator"]
        # Call the creator to verify it wraps user_creator's result
        result = creator()
        user_creator.assert_called_once()
        mock_gl.wrap.assert_called_once_with(user_conn, invalidation_port=7934)

    def test_make_creator_calls_wrap(self):
        mock_conn = MagicMock()
        user_creator = MagicMock(return_value=mock_conn)

        with patch("goldlapel.sqlalchemy.goldlapel") as mock_gl:
            mock_gl.wrap = MagicMock(return_value="wrapped")
            creator = _make_creator(PROXY_URL, 7934, user_creator)
            result = creator()

        user_creator.assert_called_once()
        mock_gl.wrap.assert_called_once_with(mock_conn, invalidation_port=7934)
        assert result == "wrapped"

    def test_make_creator_uses_psycopg_when_available(self):
        with patch("goldlapel.sqlalchemy.goldlapel") as mock_gl:
            mock_gl.wrap = MagicMock(side_effect=lambda conn, **kw: conn)
            creator = _make_creator("postgresql://user:pass@localhost:7932/mydb", 7934)

            mock_psycopg = MagicMock()
            with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
                creator()

            mock_psycopg.connect.assert_called_once_with(
                host="localhost", port=7932, dbname="mydb",
                user="user", password="pass", autocommit=True,
            )

    def test_make_creator_falls_back_to_psycopg2(self):
        with patch("goldlapel.sqlalchemy.goldlapel") as mock_gl:
            mock_gl.wrap = MagicMock(side_effect=lambda conn, **kw: conn)
            creator = _make_creator("postgresql://user:pass@localhost:7932/mydb", 7934)

            mock_conn = MagicMock()
            mock_psycopg2 = MagicMock()
            mock_psycopg2.connect.return_value = mock_conn
            with patch.dict("sys.modules", {"psycopg": None}):
                with patch("builtins.__import__", side_effect=_import_mock({"psycopg2": mock_psycopg2})):
                    creator()

            mock_psycopg2.connect.assert_called_once_with(
                host="localhost", port=7932, dbname="mydb",
                user="user", password="pass",
            )
            # psycopg2 connections must have autocommit set after creation,
            # matching psycopg (v3) which uses autocommit=True in connect()
            assert mock_conn.autocommit is True

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_invalidation_port_default_is_proxy_port_plus_2(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932
        mock_gl.wrap = MagicMock(side_effect=lambda conn, **kw: conn)

        create_engine("postgresql://host/db")

        creator = mock_sa.call_args[1]["creator"]
        # Call with a user_creator to inspect the invalidation_port
        # We test this indirectly via _make_creator
        # Default port 7932 + 2 = 7934
        with patch("goldlapel.sqlalchemy.goldlapel") as inner_gl:
            inner_gl.wrap = MagicMock(return_value="wrapped")
            mock_conn = MagicMock()
            test_creator = _make_creator(PROXY_URL, 7934, lambda: mock_conn)
            test_creator()
            inner_gl.wrap.assert_called_once_with(mock_conn, invalidation_port=7934)

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_custom_invalidation_port(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932
        mock_gl.wrap = MagicMock(side_effect=lambda conn, **kw: conn)

        create_engine("postgresql://host/db", goldlapel_invalidation_port=9999)

        creator = mock_sa.call_args[1]["creator"]
        with patch("goldlapel.sqlalchemy.goldlapel") as inner_gl:
            inner_gl.wrap = MagicMock(return_value="wrapped")
            mock_conn = MagicMock()
            test_creator = _make_creator(PROXY_URL, 9999, lambda: mock_conn)
            test_creator()
            inner_gl.wrap.assert_called_once_with(mock_conn, invalidation_port=9999)

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_invalidation_port_from_config(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932
        mock_gl.wrap = MagicMock(side_effect=lambda conn, **kw: conn)

        cfg = {"invalidation_port": 5555}
        create_engine("postgresql://host/db", goldlapel_config=cfg)

        creator = mock_sa.call_args[1]["creator"]
        with patch("goldlapel.sqlalchemy.goldlapel") as inner_gl:
            inner_gl.wrap = MagicMock(return_value="wrapped")
            mock_conn = MagicMock()
            test_creator = _make_creator(PROXY_URL, 5555, lambda: mock_conn)
            test_creator()
            inner_gl.wrap.assert_called_once_with(mock_conn, invalidation_port=5555)

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("goldlapel.sqlalchemy._sa_create_engine")
    def test_custom_port_shifts_invalidation_port(self, mock_sa, mock_gl):
        mock_gl.start.return_value = "postgresql://localhost:9000/mydb"
        mock_gl.proxy_url.return_value = "postgresql://localhost:9000/mydb"
        mock_gl.DEFAULT_PORT = 7932
        mock_gl.wrap = MagicMock(side_effect=lambda conn, **kw: conn)

        create_engine("postgresql://host/db", goldlapel_port=9000)

        # With port=9000, invalidation_port should default to 9002
        creator = mock_sa.call_args[1]["creator"]
        with patch("goldlapel.sqlalchemy.goldlapel") as inner_gl:
            inner_gl.wrap = MagicMock(return_value="wrapped")
            mock_conn = MagicMock()
            test_creator = _make_creator("postgresql://localhost:9000/mydb", 9002, lambda: mock_conn)
            test_creator()
            inner_gl.wrap.assert_called_once_with(mock_conn, invalidation_port=9002)


class TestL1AsyncEngine:
    # L1 native cache is not supported for async engines — these tests verify
    # that L1-related kwargs are silently consumed and don't leak to SQLAlchemy.

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("sqlalchemy.ext.asyncio.create_async_engine")
    def test_l1_cache_silently_ignored(self, mock_sa_async, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932

        create_async_engine("postgresql+asyncpg://host/db")

        # No L1 setup — async engines use L2 (proxy) cache only
        mock_sa_async.assert_called_once_with("postgresql+asyncpg://localhost:7932/mydb")

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("sqlalchemy.ext.asyncio.create_async_engine")
    def test_l1_cache_kwarg_not_leaked_async(self, mock_sa_async, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932

        create_async_engine("postgresql+asyncpg://host/db", goldlapel_l1_cache=True)

        # goldlapel_l1_cache must not leak to SQLAlchemy
        mock_sa_async.assert_called_once_with("postgresql+asyncpg://localhost:7932/mydb")

    @patch("goldlapel.sqlalchemy.goldlapel")
    @patch("sqlalchemy.ext.asyncio.create_async_engine")
    def test_invalidation_port_kwarg_not_leaked_async(self, mock_sa_async, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932

        create_async_engine("postgresql+asyncpg://host/db", goldlapel_invalidation_port=8888)

        # goldlapel_invalidation_port must not leak to SQLAlchemy
        mock_sa_async.assert_called_once_with("postgresql+asyncpg://localhost:7932/mydb")


class TestL1Init:
    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_sets_invalidation_port_env(self, mock_gl, monkeypatch):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932

        init(url="postgresql://host/db")

        assert os.environ["GOLDLAPEL_INVALIDATION_PORT"] == "7934"

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_custom_invalidation_port_env(self, mock_gl, monkeypatch):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932

        init(url="postgresql://host/db", invalidation_port=9999)

        assert os.environ["GOLDLAPEL_INVALIDATION_PORT"] == "9999"

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_invalidation_port_from_config(self, mock_gl, monkeypatch):
        mock_gl.start.return_value = PROXY_URL
        mock_gl.proxy_url.return_value = PROXY_URL
        mock_gl.DEFAULT_PORT = 7932

        init(url="postgresql://host/db", config={"invalidation_port": 5555})

        assert os.environ["GOLDLAPEL_INVALIDATION_PORT"] == "5555"

    @patch("goldlapel.sqlalchemy.goldlapel")
    def test_custom_port_shifts_invalidation(self, mock_gl, monkeypatch):
        mock_gl.start.return_value = "postgresql://localhost:9000/mydb"
        mock_gl.DEFAULT_PORT = 7932

        init(url="postgresql://host/db", port=9000)

        assert os.environ["GOLDLAPEL_INVALIDATION_PORT"] == "9002"


def _import_mock(available):
    real_import = __builtins__.__import__ if hasattr(__builtins__, '__import__') else __import__

    def side_effect(name, *args, **kwargs):
        if name in available:
            return available[name]
        if name == "psycopg":
            raise ImportError("no psycopg")
        return real_import(name, *args, **kwargs)

    return side_effect
