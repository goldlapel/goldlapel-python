#!/usr/bin/env bash
# Run the Python test suite and surface a skip-count summary at the end.
#
# Integration tests in tests/test_v02_integration.py and siblings skip cleanly
# when Postgres isn't reachable. That's valid behavior — we don't want to fail
# tests just because a developer doesn't have Postgres running locally — but
# the skips are invisible in default pytest output, so a developer might
# reasonably assume "all tests passed" when integration coverage never ran.
#
# This wrapper runs pytest, captures its output (while still streaming it to
# the terminal in real time), then parses the trailing summary line for the
# skipped count and prints a highlighted reminder if any tests skipped.
#
# Exit code is preserved from pytest.

set -uo pipefail

# Resolve repo root so the script works from any cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# Run pytest, tee output to stderr for live streaming, capture full output
# for post-processing. PIPESTATUS[0] preserves pytest's exit code across
# the tee pipeline.
output=$(pytest "$@" 2>&1 | tee /dev/stderr; exit "${PIPESTATUS[0]}")
rc=$?

# Parse skip count from pytest summary line:
#   "====== 592 passed, 8 skipped in 12.34s ======"
# grep -oE isolates the "N skipped" token; a second grep extracts the int.
# Default to 0 if no match (e.g. all tests passed with no skips).
skipped=$(printf '%s\n' "$output" | grep -oE '[0-9]+ skipped' | head -1 | grep -oE '[0-9]+' || true)
skipped=${skipped:-0}

# Only show the summary banner for successful runs with skips. On failure,
# pytest's own output is what the developer needs to see — don't bury it.
if [ "$rc" -eq 0 ] && [ "$skipped" -gt 0 ]; then
    printf '\n\033[33m==========================================\033[0m\n'
    printf '\033[33m⚠  %d tests skipped\033[0m — integration tests require a local Postgres.\n' "$skipped"
    printf '   Set PGHOST / PGUSER / PGPASSWORD and re-run, or rely on the CI workflow\n'
    printf '   (.github/workflows/test.yml) which provisions postgres:16 automatically.\n'
    printf '\033[33m==========================================\033[0m\n\n'
fi

exit "$rc"
