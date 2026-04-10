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
    port = kwargs.pop("goldlapel_port", None)
    config = kwargs.pop("goldlapel_config", None)
    extra_args = kwargs.pop("goldlapel_extra_args", None)
    invalidation_port = kwargs.pop("goldlapel_invalidation_port", None)
    l1_cache = kwargs.pop("goldlapel_l1_cache", True)
    clean_url, dialect = _strip_dialect(_url_to_str(url))
    os.environ.setdefault("GOLDLAPEL_CLIENT", "sqlalchemy")
    goldlapel.start(clean_url, config=config, port=port, extra_args=extra_args)
    proxy_url = goldlapel.proxy_url() or clean_url

    resolved_port = port or goldlapel.DEFAULT_PORT
    if invalidation_port is None:
        inv_port = int((config or {}).get("invalidation_port", resolved_port + 2))
    else:
        inv_port = invalidation_port

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


def init(url=None, *, config=None, port=None, extra_args=None, invalidation_port=None):
    url = url or os.environ.get("DATABASE_URL")
    if not url:
        raise ValueError("Gold Lapel: DATABASE_URL not set. Pass a URL or set DATABASE_URL.")
    clean_url, dialect = _strip_dialect(_url_to_str(url))
    os.environ.setdefault("GOLDLAPEL_CLIENT", "sqlalchemy")
    proxy = goldlapel.start(clean_url, config=config, port=port, extra_args=extra_args)
    proxy = _restore_dialect(proxy, dialect)
    os.environ["DATABASE_URL"] = proxy

    resolved_port = port or goldlapel.DEFAULT_PORT
    if invalidation_port is None:
        inv_port = int((config or {}).get("invalidation_port", resolved_port + 2))
    else:
        inv_port = invalidation_port
    os.environ["GOLDLAPEL_INVALIDATION_PORT"] = str(inv_port)

    return proxy


start = goldlapel.start
stop = goldlapel.stop
proxy_url = goldlapel.proxy_url
GoldLapel = goldlapel.GoldLapel
NativeCache = goldlapel.NativeCache
wrap = goldlapel.wrap
DEFAULT_PORT = goldlapel.DEFAULT_PORT

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
