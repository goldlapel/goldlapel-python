import os
import re
from urllib.parse import urlparse

import goldlapel
from sqlalchemy import create_engine as _sa_create_engine

_DIALECT_RE = re.compile(r'^(postgres(?:ql)?)\+(\w+)(://)')


def _url_to_str(url):
    if hasattr(url, 'render_as_string'):
        return url.render_as_string(hide_password=False)
    return str(url)


def _strip_dialect(url):
    m = _DIALECT_RE.match(url)
    if m:
        return _DIALECT_RE.sub(r'\1\3', url), m.group(2)
    return url, None


def _restore_dialect(proxy_url, dialect):
    if dialect:
        return re.sub(r'^(postgres(?:ql)?)(://)', rf'\1+{dialect}\2', proxy_url)
    return proxy_url


def _start_proxy(url, kwargs):
    proxy_port = kwargs.pop("goldlapel_proxy_port", None)
    config = kwargs.pop("goldlapel_config", None)
    extra_args = kwargs.pop("goldlapel_extra_args", None)
    invalidation_port = kwargs.pop("goldlapel_invalidation_port", None)
    dashboard_port = kwargs.pop("goldlapel_dashboard_port", None)
    log_level = kwargs.pop("goldlapel_log_level", None)
    mode = kwargs.pop("goldlapel_mode", None)
    l1_cache = kwargs.pop("goldlapel_l1_cache", True)
    clean_url, dialect = _strip_dialect(_url_to_str(url))
    inst = goldlapel.start(
        clean_url,
        proxy_port=proxy_port,
        dashboard_port=dashboard_port,
        invalidation_port=invalidation_port,
        log_level=log_level,
        mode=mode,
        client="sqlalchemy",
        config=config,
        extra_args=extra_args,
    )
    proxy_url = goldlapel.proxy_url() or clean_url
    # `inst` is a GoldLapel instance under the canonical surface; legacy mocks
    # in tests may return a bare URL string — fall back to the resolved-at-
    # caller kwarg or proxy_port + 2.
    if hasattr(inst, "invalidation_port"):
        inv_port = inst.invalidation_port
    elif invalidation_port is not None:
        inv_port = int(invalidation_port)
    else:
        resolved_port = proxy_port if proxy_port is not None else goldlapel.DEFAULT_PROXY_PORT
        inv_port = resolved_port + 2

    return _restore_dialect(proxy_url, dialect), inv_port, l1_cache


def _make_creator(proxy_url, invalidation_port, user_creator=None):
    def creator():
        if user_creator is not None:
            conn = user_creator()
        else:
            parsed = urlparse(proxy_url)
            host = parsed.hostname or "127.0.0.1"
            port = parsed.port or 7932
            dbname = parsed.path.lstrip("/") or "postgres"
            user = parsed.username
            password = parsed.password
            try:
                import psycopg
                conn = psycopg.connect(
                    host=host, port=port, dbname=dbname,
                    user=user, password=password, autocommit=True,
                )
            except ImportError:
                import psycopg2
                conn = psycopg2.connect(
                    host=host, port=port, dbname=dbname,
                    user=user, password=password,
                )
                conn.autocommit = True
        return goldlapel.wrap(conn, invalidation_port=invalidation_port)
    return creator


def create_engine(url, **kwargs):
    proxy, inv_port, l1_cache = _start_proxy(url, kwargs)

    if l1_cache:
        # Strip dialect for the creator — it needs a plain postgresql:// URL
        plain_proxy = _DIALECT_RE.sub(r'\1\3', proxy)
        user_creator = kwargs.pop("creator", None)
        kwargs["creator"] = _make_creator(plain_proxy, inv_port, user_creator)

    return _sa_create_engine(proxy, **kwargs)


def create_async_engine(url, **kwargs):
    # L1 native cache is not yet supported for async engines.
    # Queries go through the GL proxy (L2 cache).
    from sqlalchemy.ext.asyncio import create_async_engine as _sa_create_async_engine
    proxy, _inv_port, _l1_cache = _start_proxy(url, kwargs)

    return _sa_create_async_engine(proxy, **kwargs)


def init(
    url=None,
    *,
    config=None,
    proxy_port=None,
    dashboard_port=None,
    invalidation_port=None,
    log_level=None,
    mode=None,
    extra_args=None,
):
    url = url or os.environ.get("DATABASE_URL")
    if not url:
        raise ValueError("Gold Lapel: DATABASE_URL not set. Pass a URL or set DATABASE_URL.")
    clean_url, dialect = _strip_dialect(_url_to_str(url))
    inst = goldlapel.start(
        clean_url,
        proxy_port=proxy_port,
        dashboard_port=dashboard_port,
        invalidation_port=invalidation_port,
        log_level=log_level,
        mode=mode,
        client="sqlalchemy",
        config=config,
        extra_args=extra_args,
    )
    if hasattr(inst, "url"):
        proxy = inst.url
    else:
        proxy = inst  # legacy mock: returned a bare URL string
    proxy = _restore_dialect(proxy, dialect)
    os.environ["DATABASE_URL"] = proxy
    if hasattr(inst, "invalidation_port"):
        os.environ["GOLDLAPEL_INVALIDATION_PORT"] = str(inst.invalidation_port)
    elif invalidation_port is not None:
        os.environ["GOLDLAPEL_INVALIDATION_PORT"] = str(invalidation_port)
    else:
        resolved_port = proxy_port if proxy_port is not None else goldlapel.DEFAULT_PROXY_PORT
        os.environ["GOLDLAPEL_INVALIDATION_PORT"] = str(resolved_port + 2)

    return proxy


start = goldlapel.start
stop = goldlapel.stop
proxy_url = goldlapel.proxy_url
GoldLapel = goldlapel.GoldLapel
NativeCache = goldlapel.NativeCache
wrap = goldlapel.wrap
DEFAULT_PROXY_PORT = goldlapel.DEFAULT_PROXY_PORT

doc_insert = goldlapel.doc_insert
doc_insert_many = goldlapel.doc_insert_many
doc_find = goldlapel.doc_find
doc_find_one = goldlapel.doc_find_one
doc_update = goldlapel.doc_update
doc_update_one = goldlapel.doc_update_one
doc_delete = goldlapel.doc_delete
doc_delete_one = goldlapel.doc_delete_one
doc_count = goldlapel.doc_count
doc_create_index = goldlapel.doc_create_index
doc_aggregate = goldlapel.doc_aggregate
doc_watch = goldlapel.doc_watch
doc_unwatch = goldlapel.doc_unwatch
doc_create_ttl_index = goldlapel.doc_create_ttl_index
doc_remove_ttl_index = goldlapel.doc_remove_ttl_index
doc_create_capped = goldlapel.doc_create_capped
doc_remove_cap = goldlapel.doc_remove_cap
