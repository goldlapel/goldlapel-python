"""The shared proxy: one per machine and configuration, for the workers of a
multi-process server. Each "worker" here is a Python process of its own;
each test has its own TMPDIR, where the binary keeps the shared proxies'
files ($TMPDIR/goldlapel-<uid>/<key>.json).

Gated on GOLDLAPEL_INTEGRATION=1 + GOLDLAPEL_TEST_UPSTREAM and a goldlapel
binary (GOLDLAPEL_BINARY).
"""

import glob
import json
import os
import signal
import subprocess
import sys
import time

import pytest

from _integration_gate import require_integration_upstream

pytestmark = pytest.mark.integration

WORKER = """
import json, os, goldlapel
gl = goldlapel.start(os.environ["GOLDLAPEL_TEST_UPSTREAM"], shared=True, silent=True)
cur = gl.conn.cursor()
cur.execute("SELECT 1")
assert cur.fetchone()[0] == 1
print(json.dumps({"port": gl.proxy_port}))
"""


def _alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _wait(condition, seconds):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.1)
    return condition()


@pytest.fixture
def shared(tmp_path):
    require_integration_upstream()
    if not os.environ.get("GOLDLAPEL_BINARY"):
        pytest.skip("Set GOLDLAPEL_BINARY to a goldlapel binary to run this test.")
    instances = lambda: [
        json.load(open(f)) for f in glob.glob(str(tmp_path / f"goldlapel-{os.geteuid()}" / "*.json"))
    ]

    def worker(**env):
        result = subprocess.run(
            [sys.executable, "-c", WORKER],
            env={
                **os.environ,
                "TMPDIR": str(tmp_path),
                # Off the network, as the proxy's own tests run it
                "GOLDLAPEL_DISABLE_TRIAL_REGISTER": "true",
                "GOLDLAPEL_HQ_BASE": "http://127.0.0.1:9",
                "GOLDLAPEL_TELEMETRY_URL": "http://127.0.0.1:9",
                **env,
            },
            capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return json.loads(result.stdout.splitlines()[-1])["port"]

    worker.instances = instances
    yield worker
    for info in instances():
        os.kill(info["pid"], signal.SIGTERM)
        _wait(lambda: not _alive(info["pid"]), 10)


def test_workers_share_one_proxy_that_outlives_them(shared):
    port = shared()
    assert shared() == port
    instances = shared.instances()
    assert len(instances) == 1
    assert instances[0]["proxy_port"] == port
    assert _alive(instances[0]["pid"]), "the proxy left with the workers"


def test_it_idles_out_once_no_worker_holds_it(shared):
    shared(GOLDLAPEL_IDLE_EXIT_SECS="2")
    pid = shared.instances()[0]["pid"]
    assert _wait(lambda: not _alive(pid), 15), "never idled out"
    assert shared.instances() == []
