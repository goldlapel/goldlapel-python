import os
import re

import goldlapel
from goldlapel.proxy import _unknown_options_message
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


# Keyword options of `goldlapel.start`. Engine factories take each one as a
# `goldlapel_<name>` kwarg (popped before SQLAlchemy sees the rest); init()
# takes them under their own names.
_START_OPTIONS = (
    "proxy_port", "dashboard_port", "log_level", "mode", "license",
    "api_key", "client", "config_file", "config", "extra_args", "silent",
    "mesh", "mesh_tag", "disable_proxy_cache", "disable_sqloptimize",
    "disable_auto_indexes",
)


def _proxy_url_for(url, options):
    # Start (or reuse) the proxy for `url` and return the URL of that
    # proxy — not goldlapel.proxy_url(), which can't tell several apart.
    unknown = set(options) - set(_START_OPTIONS)
    if unknown:
        raise TypeError(_unknown_options_message(unknown))
    clean_url, dialect = _strip_dialect(_url_to_str(url))
    inst = goldlapel.start(clean_url, **{"client": "sqlalchemy", **options})
    return _restore_dialect(inst.url, dialect)


def _start_proxy(url, kwargs):
    unknown = [
        key for key in kwargs
        if key.startswith("goldlapel_") and key[len("goldlapel_"):] not in _START_OPTIONS
    ]
    if unknown:
        raise TypeError(_unknown_options_message(unknown, prefix="goldlapel_"))
    options = {}
    for name in _START_OPTIONS:
        key = "goldlapel_" + name
        if key in kwargs:
            options[name] = kwargs.pop(key)
    return _proxy_url_for(url, options)


def create_engine(url, **kwargs):
    proxy = _start_proxy(url, kwargs)
    return _sa_create_engine(proxy, **kwargs)


def create_async_engine(url, **kwargs):
    from sqlalchemy.ext.asyncio import create_async_engine as _sa_create_async_engine
    proxy = _start_proxy(url, kwargs)
    return _sa_create_async_engine(proxy, **kwargs)


# Start the proxy for `url` (default: $DATABASE_URL), point DATABASE_URL at
# it and return the proxy URL. `options` are the keyword options of
# `goldlapel.start`.
def init(url=None, **options):
    url = url or os.environ.get("DATABASE_URL")
    if not url:
        raise ValueError("Gold Lapel: DATABASE_URL not set. Pass a URL or set DATABASE_URL.")
    proxy = _proxy_url_for(url, options)
    os.environ["DATABASE_URL"] = proxy
    return proxy


start = goldlapel.start
stop = goldlapel.stop
proxy_url = goldlapel.proxy_url
GoldLapel = goldlapel.GoldLapel
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
