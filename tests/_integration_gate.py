"""Shared integration-test gating — standardized across all Gold Lapel wrappers.

Convention:

  - GOLDLAPEL_INTEGRATION=1  — explicit opt-in gate ("yes, really run these")
  - GOLDLAPEL_TEST_UPSTREAM  — Postgres URL for the test upstream

Both must be set. If GOLDLAPEL_INTEGRATION=1 is set but
GOLDLAPEL_TEST_UPSTREAM is missing, we fail loudly — this prevents a
half-configured CI from silently skipping integration tests and producing
a false-green unit-only run.

If GOLDLAPEL_INTEGRATION is unset, integration tests skip silently (the
expected unit-only path).
"""

import os

import pytest


def require_integration_upstream():
    """Return the upstream Postgres URL for integration tests, or skip/fail.

    Returns:
        str: the Postgres URL from GOLDLAPEL_TEST_UPSTREAM.

    Behaviour:
        - GOLDLAPEL_INTEGRATION=1 + GOLDLAPEL_TEST_UPSTREAM set -> returns URL.
        - GOLDLAPEL_INTEGRATION=1 set but GOLDLAPEL_TEST_UPSTREAM missing ->
          pytest.fail() (false-green prevention).
        - GOLDLAPEL_INTEGRATION unset -> pytest.skip().
    """
    integration = os.environ.get("GOLDLAPEL_INTEGRATION") == "1"
    upstream = os.environ.get("GOLDLAPEL_TEST_UPSTREAM")

    if integration and not upstream:
        pytest.fail(
            "GOLDLAPEL_INTEGRATION=1 is set but GOLDLAPEL_TEST_UPSTREAM is "
            "missing. Set GOLDLAPEL_TEST_UPSTREAM to a Postgres URL "
            "(e.g. postgresql://postgres@localhost/postgres) or unset "
            "GOLDLAPEL_INTEGRATION to skip integration tests."
        )

    if not integration:
        pytest.skip(
            "integration tests skipped; set GOLDLAPEL_INTEGRATION=1 and "
            "GOLDLAPEL_TEST_UPSTREAM to run"
        )

    return upstream
