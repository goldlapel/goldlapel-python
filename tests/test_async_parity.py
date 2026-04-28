"""Parity test: AsyncGoldLapel must expose every public method on GoldLapel.

The async wrapper's method surface is auto-derived at import time by walking
GoldLapel's public methods (see _derive_async_methods in
src/goldlapel/asyncio/_proxy.py). This test guards the invariant: when
someone adds a method to the sync class, it either auto-appears on the async
class, or they consciously add it to _ASYNC_SKIPPED. Drift in either
direction fails this test loudly.

Pattern modeled on Motor (PyMongo's async driver) and aioboto3 — both have
used auto-derive in production for years.
"""

import inspect

from goldlapel import GoldLapel
from goldlapel.asyncio import AsyncGoldLapel
from goldlapel.asyncio._proxy import _ASYNC_SKIPPED


def _public_method_names(cls):
    """Return the set of public (non-_-prefixed) method names defined on cls
    and its bases, excluding properties and other non-function attrs."""
    return frozenset(
        name for name, _ in inspect.getmembers(cls, predicate=inspect.isfunction)
        if not name.startswith("_")
    )


def test_sync_async_method_parity():
    """Every public sync method (modulo _ASYNC_SKIPPED) must exist on the
    async class. Adding to GoldLapel without adding to AsyncGoldLapel (or
    consciously skipping) trips this test."""
    sync_methods = _public_method_names(GoldLapel)
    async_methods = _public_method_names(AsyncGoldLapel)

    expected_async = sync_methods - _ASYNC_SKIPPED
    missing_on_async = expected_async - async_methods

    assert not missing_on_async, (
        f"AsyncGoldLapel is missing public methods that exist on GoldLapel: "
        f"{sorted(missing_on_async)}. Either add an async-native version to "
        f"AsyncGoldLapel, let auto-derive pick them up (any method on a sync "
        f"util in goldlapel.asyncio._utils with the same name will be wrapped "
        f"automatically), or add to _ASYNC_SKIPPED with a comment explaining "
        f"why the method shouldn't appear on the async surface."
    )


def test_no_unexpected_async_only_methods():
    """async-only methods are allowed (e.g. async generators with no sync
    equivalent), but a divergence ought to be deliberate — surface them so
    the human reviewer can confirm."""
    sync_methods = _public_method_names(GoldLapel)
    async_methods = _public_method_names(AsyncGoldLapel)

    extra_on_async = async_methods - sync_methods

    # Today this set is empty; if it grows in the future, update this list
    # (and document why in the same commit). Forcing the explicit list keeps
    # async-only methods discoverable.
    KNOWN_ASYNC_ONLY = frozenset({
        # (none today — every async method has a sync counterpart)
    })

    unexpected = extra_on_async - KNOWN_ASYNC_ONLY
    assert not unexpected, (
        f"AsyncGoldLapel has methods that aren't on GoldLapel and aren't in "
        f"the KNOWN_ASYNC_ONLY allow-list: {sorted(unexpected)}. If this is "
        f"intentional, add the names to KNOWN_ASYNC_ONLY in this test."
    )


def test_skip_list_is_a_frozenset():
    """Defensive: _ASYNC_SKIPPED must be a frozenset so callers can't mutate
    it from outside the module."""
    assert isinstance(_ASYNC_SKIPPED, frozenset), (
        f"_ASYNC_SKIPPED should be a frozenset, got {type(_ASYNC_SKIPPED).__name__}"
    )


def test_auto_derived_methods_have_async_signatures():
    """Smoke check the auto-derive machinery actually attached coroutines
    (or async generators) to AsyncGoldLapel — not plain functions.

    Only auto-derived methods (those with `__wrapped__` pointing at the
    matching sync method) are checked. Hand-written async-native methods
    like `start`, `stop`, `using`, `stream_*` have their own dedicated
    coverage in tests/test_v02_asyncio.py — `using`, for instance, is an
    `@asynccontextmanager` factory and is a plain function under inspect's
    rules, which is correct behavior, not a bug.
    """
    for name in vars(AsyncGoldLapel):
        if name.startswith("_"):
            continue
        method = getattr(AsyncGoldLapel, name)
        wrapped = getattr(method, "__wrapped__", None)
        if wrapped is None or wrapped is not getattr(GoldLapel, name, None):
            # Not auto-derived (hand-written native or a property).
            continue
        is_coro = inspect.iscoroutinefunction(method)
        is_gen = inspect.isasyncgenfunction(method)
        assert is_coro or is_gen, (
            f"AsyncGoldLapel.{name} should be `async def` (coroutine or "
            f"async generator), got plain function or non-callable."
        )


def test_auto_derived_method_inherits_qualname():
    """@wraps in _make_async_wrapper carries __wrapped__ and identity-friendly
    metadata. We override __qualname__ to point at AsyncGoldLapel for
    debuggability — verify that override survives."""
    # search() is auto-derived (the sync `def search` is a thin dispatch into
    # _utils().search) — pick one auto-derived method and check.
    method = AsyncGoldLapel.search
    assert method.__qualname__ == "AsyncGoldLapel.search"
    # __wrapped__ is set by functools.wraps and points at the sync method —
    # confirms auto-derive used the sync class as its source of truth.
    assert getattr(method, "__wrapped__", None) is GoldLapel.search


def test_auto_derived_method_has_a_docstring():
    """Auto-derived wrappers fall back to a custom docstring when the sync
    method has none. Verify the help() experience isn't blank."""
    method = AsyncGoldLapel.search
    assert method.__doc__, "AsyncGoldLapel.search should have a docstring"
