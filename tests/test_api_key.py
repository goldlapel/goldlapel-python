"""Tests for the `api_key` parameter (Wave 1 of api-key-model rollout).

The wrapper accepts a stable customer credential (`gl_live_*` /
`gl_test_*`) and forwards it to the spawned Rust binary as the
`GOLDLAPEL_API_KEY` env var. The binary then fetches and auto-
renews the license from HQ, removing the need for the customer
to hand-place a PEM file.

api_key takes precedence over `license` (the file-path option)
when both are passed; license remains as the offline fallback.
"""

import logging
import os
from unittest.mock import MagicMock, patch

import pytest

import goldlapel
from goldlapel.proxy import GoldLapel


class TestApiKeyAcceptedAsKwarg:
    def test_api_key_stored_on_instance(self):
        gl = GoldLapel("postgresql://host/db", api_key="gl_test_abc123")
        assert gl._api_key == "gl_test_abc123"

    def test_api_key_default_is_none(self):
        gl = GoldLapel("postgresql://host/db")
        assert gl._api_key is None

    def test_factory_start_forwards_api_key(self):
        """`goldlapel.start(url, api_key=...)` makes it through to the
        underlying GoldLapel instance."""
        with patch(
            "goldlapel.proxy._detect_sync_driver",
            return_value=("psycopg2", MagicMock()),
        ):
            with patch("goldlapel.proxy._ensure_running") as mock_ensure:
                fake = MagicMock(spec=GoldLapel)
                mock_ensure.return_value = fake
                goldlapel.start(
                    "postgresql://host/db", api_key="gl_test_factory_check"
                )
                _, kwargs = mock_ensure.call_args
                assert kwargs.get("api_key") == "gl_test_factory_check"


class TestApiKeyExportedAtSpawn:
    """When the binary is spawned, GOLDLAPEL_API_KEY must be in its
    env. We exercise the env-construction inside `start()` without
    actually spawning by patching the heavy bits."""

    def _patches_for_spawn(self, env_capture):
        """Patch out everything that would actually spawn or connect, and
        capture the env passed to subprocess.Popen.
        """
        proc = MagicMock()
        proc.poll.return_value = None
        proc.stderr = MagicMock()
        proc.stderr.read.return_value = b""

        def fake_popen(cmd, **kwargs):
            env_capture["cmd"] = cmd
            env_capture["env"] = kwargs.get("env", {})
            return proc

        return [
            patch("goldlapel.proxy._find_binary", return_value="/usr/bin/goldlapel"),
            patch("goldlapel.proxy._kill_orphan_on_port"),
            patch("goldlapel.proxy._wait_for_port", return_value=True),
            patch("subprocess.Popen", side_effect=fake_popen),
            # Avoid opening a real DB connection.
            patch(
                "goldlapel.proxy._detect_sync_driver",
                return_value=("psycopg2", MagicMock()),
            ),
        ]

    def test_api_key_set_on_subprocess_env(self):
        env_capture = {}
        gl = GoldLapel(
            "postgresql://host/db", api_key="gl_live_e2e_smoke_xyz"
        )
        patches = self._patches_for_spawn(env_capture)
        for p in patches:
            p.start()
        try:
            try:
                gl.start()
            except Exception:
                # Connection bits inside start() may fail past the spawn —
                # that's fine, we only care about the env captured by
                # subprocess.Popen.
                pass
            assert env_capture.get("env", {}).get("GOLDLAPEL_API_KEY") == (
                "gl_live_e2e_smoke_xyz"
            )
        finally:
            for p in patches:
                p.stop()

    def test_api_key_not_in_env_when_none(self):
        env_capture = {}
        gl = GoldLapel("postgresql://host/db")  # no api_key
        # Belt-and-braces: if GOLDLAPEL_API_KEY happens to be in the
        # parent env (developer-set), we still want the wrapper to
        # *not* mutate it. Save and restore.
        prev = os.environ.pop("GOLDLAPEL_API_KEY", None)
        try:
            patches = self._patches_for_spawn(env_capture)
            for p in patches:
                p.start()
            try:
                try:
                    gl.start()
                except Exception:
                    pass
                assert "GOLDLAPEL_API_KEY" not in env_capture.get("env", {})
            finally:
                for p in patches:
                    p.stop()
        finally:
            if prev is not None:
                os.environ["GOLDLAPEL_API_KEY"] = prev

    def test_api_key_not_in_cmdline_args(self):
        """Pass api_key as env var, not CLI flag — keeps it out of `ps`
        output for credentials hygiene."""
        env_capture = {}
        gl = GoldLapel(
            "postgresql://host/db", api_key="gl_test_secret_abcdef"
        )
        patches = self._patches_for_spawn(env_capture)
        for p in patches:
            p.start()
        try:
            try:
                gl.start()
            except Exception:
                pass
            cmd = env_capture.get("cmd", [])
            assert "--api-key" not in cmd
            assert "gl_test_secret_abcdef" not in cmd
        finally:
            for p in patches:
                p.stop()


class TestApiKeyPrecedenceOverLicense:
    def test_warning_logged_when_both_passed(self, caplog):
        """Passing both api_key and license logs a warning saying
        api_key wins."""
        with caplog.at_level(logging.WARNING):
            gl = GoldLapel(
                "postgresql://host/db",
                api_key="gl_live_xyz",
                license="/path/to/license.key",
            )
        # Both stay on the instance — the wrapper just forwards both.
        # Precedence is enforced inside the Rust binary.
        assert gl._api_key == "gl_live_xyz"
        assert gl._license == "/path/to/license.key"
        assert any("api_key" in r.message and "license" in r.message for r in caplog.records)

    def test_no_warning_when_only_api_key(self, caplog):
        with caplog.at_level(logging.WARNING):
            GoldLapel("postgresql://host/db", api_key="gl_live_xyz")
        assert not any("api_key" in r.message for r in caplog.records)

    def test_no_warning_when_only_license(self, caplog):
        with caplog.at_level(logging.WARNING):
            GoldLapel("postgresql://host/db", license="/path/to/license.key")
        assert not any("api_key" in r.message for r in caplog.records)
