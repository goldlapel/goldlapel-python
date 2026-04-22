# v0.2.0 factory API — `goldlapel.start(url)` returns a GoldLapel instance.
# For async, use `from goldlapel.asyncio import start`.
from goldlapel.proxy import start, connect, stop, proxy_url, dashboard_url, config_keys, GoldLapel, DEFAULT_PORT
from goldlapel.wrap import wrap
from goldlapel.cache import NativeCache

# Package version — pulled dynamically from installed metadata so we don't
# drift from pyproject.toml (which CI rewrites from the git tag at release).
try:
    from importlib.metadata import version as _version, PackageNotFoundError as _PackageNotFoundError
    try:
        __version__ = _version("goldlapel")
    except _PackageNotFoundError:
        __version__ = "unknown"
except ImportError:  # pragma: no cover — importlib.metadata is stdlib from 3.8+
    __version__ = "unknown"
# Underlying utility functions — used internally by GoldLapel methods. Available
# at module level for advanced users who already have their own connection.
from goldlapel.utils import publish, subscribe, enqueue, dequeue, incr, get_counter, hset, hget, hgetall, hdel, zadd, zincrby, zrange, zrank, zscore, zrem, georadius, geoadd, geodist, count_distinct, script, stream_add, stream_create_group, stream_read, stream_ack, stream_claim, search, search_fuzzy, search_phonetic, similar, suggest, facets, aggregate, create_search_config, percolate_add, percolate, percolate_delete, analyze, explain_score, doc_insert, doc_insert_many, doc_find, doc_find_one, doc_update, doc_update_one, doc_delete, doc_delete_one, doc_count, doc_create_index, doc_aggregate, doc_watch, doc_unwatch, doc_create_ttl_index, doc_remove_ttl_index, doc_create_capped, doc_remove_cap, doc_find_one_and_update, doc_find_one_and_delete, doc_distinct, doc_find_cursor, doc_create_collection
