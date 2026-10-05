import pytest

import goldlapel.proxy as proxy_mod


@pytest.fixture(autouse=True)
def _isolated_proxy_state(request, monkeypatch):
    """Unit tests start from an empty registry and see every port as free
    at the OS level, so port assignments don't depend on what else runs on
    the test machine. Integration tests use the real probe."""
    if request.node.get_closest_marker("integration") is None:
        monkeypatch.setattr(proxy_mod, "_port_free", lambda port: True)
    proxy_mod._instances.clear()
    proxy_mod._live.clear()
    yield
    proxy_mod._instances.clear()
    proxy_mod._live.clear()
