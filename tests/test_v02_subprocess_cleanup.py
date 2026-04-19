"""Regression test: GoldLapel.start() must clean up its subprocess if the
eager driver.connect() raises after the subprocess has been spawned.

This was a real bug caught in review: Popen succeeded, port bound, but then
driver.connect() failed (bad creds, network issue, etc.) and the subprocess
kept running indefinitely.
"""

from unittest.mock import MagicMock, patch

import pytest

from goldlapel.proxy import GoldLapel


class TestSubprocessCleanupOnConnectFailure:
    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy._find_binary", return_value="/fake/goldlapel")
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._detect_sync_driver")
    def test_popen_terminated_when_driver_connect_raises(
        self, mock_detect, mock_popen_cls, mock_find, mock_wait
    ):
        mock_process = MagicMock()
        mock_process.poll.return_value = None
        mock_process.stderr = MagicMock()
        mock_popen_cls.return_value = mock_process

        fake_driver = MagicMock()
        fake_driver.connect.side_effect = ConnectionError("bad creds")
        mock_detect.return_value = ("psycopg2", fake_driver)

        gl = GoldLapel("postgresql://host/db")
        with pytest.raises(ConnectionError, match="bad creds"):
            gl.start()

        mock_process.terminate.assert_called_once()
        assert gl._process is None
        assert gl._proxy_url is None

    @patch("goldlapel.proxy._wait_for_port", return_value=True)
    @patch("goldlapel.proxy._find_binary", return_value="/fake/goldlapel")
    @patch("goldlapel.proxy.subprocess.Popen")
    @patch("goldlapel.proxy._detect_sync_driver")
    def test_popen_killed_if_terminate_times_out(
        self, mock_detect, mock_popen_cls, mock_find, mock_wait
    ):
        import subprocess as subprocess_module
        mock_process = MagicMock()
        mock_process.poll.return_value = None
        mock_process.stderr = MagicMock()
        mock_process.wait.side_effect = [
            subprocess_module.TimeoutExpired(cmd="goldlapel", timeout=3),
            None,
        ]
        mock_popen_cls.return_value = mock_process

        fake_driver = MagicMock()
        fake_driver.connect.side_effect = RuntimeError("kaboom")
        mock_detect.return_value = ("psycopg2", fake_driver)

        gl = GoldLapel("postgresql://host/db")
        with pytest.raises(RuntimeError, match="kaboom"):
            gl.start()

        mock_process.terminate.assert_called_once()
        mock_process.kill.assert_called_once()
        assert gl._process is None
