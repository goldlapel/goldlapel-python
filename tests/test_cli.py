import sys
from unittest.mock import MagicMock, patch

import pytest

from goldlapel.cli import main


class TestCli:
    @patch("goldlapel.cli.os.execvp")
    @patch("goldlapel.cli._find_binary", return_value="/usr/bin/goldlapel")
    @patch("goldlapel.cli.os.name", "posix")
    def test_unix_uses_execvp(self, mock_find, mock_execvp):
        with patch.object(sys, "argv", ["goldlapel", "activate", "abc123"]):
            main()
        mock_execvp.assert_called_once_with(
            "/usr/bin/goldlapel",
            ["/usr/bin/goldlapel", "activate", "abc123"],
        )

    @patch("goldlapel.cli.subprocess.run")
    @patch("goldlapel.cli._find_binary", return_value="/usr/bin/goldlapel")
    @patch("goldlapel.cli.os.name", "nt")
    def test_windows_uses_subprocess_run(self, mock_find, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        with patch.object(sys, "argv", ["goldlapel", "activate", "abc123"]):
            with pytest.raises(SystemExit) as exc:
                main()
        assert exc.value.code == 0
        mock_run.assert_called_once_with(
            ["/usr/bin/goldlapel", "activate", "abc123"],
        )

    @patch("goldlapel.cli.subprocess.run")
    @patch("goldlapel.cli._find_binary", return_value="/usr/bin/goldlapel")
    @patch("goldlapel.cli.os.name", "nt")
    def test_windows_forwards_exit_code(self, mock_find, mock_run):
        mock_run.return_value = MagicMock(returncode=42)
        with patch.object(sys, "argv", ["goldlapel"]):
            with pytest.raises(SystemExit) as exc:
                main()
        assert exc.value.code == 42

    @patch("goldlapel.cli._find_binary", side_effect=FileNotFoundError("binary not found"))
    def test_file_not_found_prints_error_and_exits(self, mock_find, capsys):
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 1
        captured = capsys.readouterr()
        assert "Error: binary not found" in captured.err

    @patch("goldlapel.cli.os.execvp")
    @patch("goldlapel.cli._find_binary", return_value="/usr/bin/goldlapel")
    @patch("goldlapel.cli.os.name", "posix")
    def test_argv_forwarded_correctly(self, mock_find, mock_execvp):
        with patch.object(sys, "argv", ["goldlapel", "activate", "abc123"]):
            main()
        args = mock_execvp.call_args[0]
        assert args[1] == ["/usr/bin/goldlapel", "activate", "abc123"]

    @patch("goldlapel.cli.os.execvp")
    @patch("goldlapel.cli._find_binary", return_value="/usr/bin/goldlapel")
    @patch("goldlapel.cli.os.name", "posix")
    def test_no_args_forwarded(self, mock_find, mock_execvp):
        with patch.object(sys, "argv", ["goldlapel"]):
            main()
        args = mock_execvp.call_args[0]
        assert args[1] == ["/usr/bin/goldlapel"]

    @patch("goldlapel.cli.os.execvp")
    @patch("goldlapel.cli._find_binary", return_value="/usr/bin/goldlapel")
    @patch("goldlapel.cli.os.name", "posix")
    def test_main_module_importable(self, mock_find, mock_execvp):
        # __main__.py calls main() at import time, so we must mock dependencies.
        # Remove from cache to force re-import.
        sys.modules.pop("goldlapel.__main__", None)
        import goldlapel.__main__  # noqa: F401
        mock_execvp.assert_called_once()
