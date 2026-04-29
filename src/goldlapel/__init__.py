# v0.2.0 factory API — `goldlapel.start(url)` returns a GoldLapel instance.
# For async, use `from goldlapel.asyncio import start`.
from goldlapel.proxy import start, connect, stop, proxy_url, dashboard_url, config_keys, GoldLapel, DEFAULT_PROXY_PORT
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
#
# Note: helper-family utils (doc_*, stream_*, counter_*, zset_*, hash_*, queue_*,
# geo_*) all require a `patterns=` kwarg — DDL is proxy-owned, and the helpers
# fetch patterns via the namespaced sub-APIs (gl.documents / gl.streams /
# gl.counters / gl.zsets / gl.hashes / gl.queues / gl.geos). Direct util
# usage is supported for testing and migration scenarios.
from goldlapel.utils import (
    publish, subscribe, count_distinct, script,
    # Streams
    stream_add, stream_create_group, stream_read, stream_ack, stream_claim,
    # Search / percolator / analysis
    search, search_fuzzy, search_phonetic, similar, suggest, facets, aggregate,
    create_search_config, percolate_add, percolate, percolate_delete,
    analyze, explain_score,
    # Doc store
    doc_insert, doc_insert_many, doc_find, doc_find_one, doc_update,
    doc_update_one, doc_delete, doc_delete_one, doc_count, doc_create_index,
    doc_aggregate, doc_watch, doc_unwatch, doc_create_ttl_index,
    doc_remove_ttl_index, doc_create_capped, doc_remove_cap,
    doc_find_one_and_update, doc_find_one_and_delete, doc_distinct,
    doc_find_cursor, doc_create_collection,
    # Phase 5 Redis-compat families
    counter_incr, counter_decr, counter_set, counter_get, counter_delete,
    counter_count_keys,
    zset_add, zset_incr_by, zset_score, zset_remove, zset_range,
    zset_range_by_score, zset_rank, zset_card,
    hash_set, hash_get, hash_get_all, hash_keys, hash_values, hash_exists,
    hash_delete, hash_len,
    queue_enqueue, queue_claim, queue_ack, queue_abandon, queue_extend,
    queue_peek, queue_count_ready, queue_count_claimed,
    geo_add, geo_pos, geo_dist, geo_radius, geo_radius_by_member,
    geo_remove, geo_count,
)
