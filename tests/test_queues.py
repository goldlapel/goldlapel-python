"""Unit tests for goldlapel.queues.QueuesAPI.

Phase 5 introduces at-least-once delivery with visibility-timeout. The
breaking change is `dequeue` (delete-on-fetch) → `claim` (lease + ack).
These tests verify:

  - `enqueue` returns the assigned id from the proxy's RETURNING clause.
  - `claim` returns `(id, payload)` or `None` — explicit tuple shape.
  - `ack` is a separate call, NOT bundled into claim.
  - `abandon` / `nack` releases the claim immediately.
  - `extend` pushes the visibility deadline.
"""

from unittest.mock import MagicMock, patch

import pytest

from goldlapel.proxy import GoldLapel
from goldlapel.queues import QueuesAPI
from goldlapel import utils as real_utils


@pytest.fixture
def gl():
    inst = GoldLapel("postgresql://localhost:5432/mydb")
    inst._conn = MagicMock(name="internal_conn")
    inst._dashboard_token = "test-token"
    return inst


@pytest.fixture
def fake_patterns():
    main = "_goldlapel.queue_jobs"
    return {
        "tables": {"main": main},
        "query_patterns": {
            "enqueue": f"INSERT INTO {main} (payload) VALUES ($1::jsonb) RETURNING id, created_at",
            "claim": f"WITH next_msg AS ( SELECT id FROM {main} WHERE status = 'ready' AND visible_at <= NOW() ORDER BY visible_at, id FOR UPDATE SKIP LOCKED LIMIT 1 ) UPDATE {main} SET status = 'claimed', visible_at = NOW() + INTERVAL '1 millisecond' * $1 FROM next_msg WHERE {main}.id = next_msg.id RETURNING {main}.id, {main}.payload, {main}.visible_at, {main}.created_at",
            "ack": f"DELETE FROM {main} WHERE id = $1",
            "extend": f"WITH target AS (SELECT $1::bigint AS id, $2::bigint AS additional_ms) UPDATE {main} m SET visible_at = m.visible_at + INTERVAL '1 millisecond' * target.additional_ms FROM target WHERE m.id = target.id AND m.status = 'claimed' RETURNING m.visible_at",
            "nack": f"UPDATE {main} SET status = 'ready', visible_at = NOW() WHERE id = $1 AND status = 'claimed' RETURNING id",
            "peek": f"SELECT id, payload, visible_at, status, created_at FROM {main} WHERE status = 'ready' AND visible_at <= NOW() ORDER BY visible_at, id LIMIT 1",
            "count_ready": f"SELECT COUNT(*) FROM {main} WHERE status = 'ready' AND visible_at <= NOW()",
            "count_claimed": f"SELECT COUNT(*) FROM {main} WHERE status = 'claimed'",
            "delete_all": f"DELETE FROM {main}",
        },
    }


class TestNamespaceShape:
    def test_queues_is_a_QueuesAPI(self, gl):
        assert isinstance(gl.queues, QueuesAPI)

    def test_no_legacy_flat_methods(self):
        # Phase 5 hard cut — `enqueue` / `dequeue` are gone. Use `claim`/`ack`.
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        for legacy in ["enqueue", "dequeue"]:
            assert not hasattr(gl, legacy), (
                f"Phase 5 removed flat {legacy} — use gl.queues.<verb>."
            )

    def test_no_dequeue_alias_on_queues_api(self):
        # The dispatcher considered shipping a dequeue compat shim that
        # combined claim+ack. The master plan rejected that — there must
        # be no compat alias here.
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        assert not hasattr(gl.queues, "dequeue"), (
            "Phase 5 forbids a dequeue alias — claim+ack is explicit by design."
        )


class TestVerbDispatch:
    @patch("goldlapel.ddl.fetch_patterns")
    def test_enqueue_dispatches(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "queue_enqueue", return_value=42) as m:
            assert gl.queues.enqueue("jobs", {"work": "foo"}) == 42
            m.assert_called_once_with(
                gl._conn, "jobs", {"work": "foo"}, patterns=fake_patterns,
            )

    @patch("goldlapel.ddl.fetch_patterns")
    def test_claim_passes_visibility_timeout(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "queue_claim", return_value=(1, {})) as m:
            gl.queues.claim("jobs", visibility_timeout_ms=60000)
            m.assert_called_once_with(
                gl._conn, "jobs", visibility_timeout_ms=60000, patterns=fake_patterns,
            )

    @patch("goldlapel.ddl.fetch_patterns")
    def test_ack_passes_message_id(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "queue_ack", return_value=True) as m:
            gl.queues.ack("jobs", 42)
            m.assert_called_once_with(
                gl._conn, "jobs", 42, patterns=fake_patterns,
            )


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commit = MagicMock()

    def cursor(self):
        return self._cursor


def _cursor(*, fetchone=None, rowcount=0):
    cur = MagicMock()
    cur.fetchone.return_value = fetchone
    cur.rowcount = rowcount
    return cur


class TestSqlBuilders:
    def test_enqueue_returns_id_from_proxy(self, fake_patterns):
        cur = _cursor(fetchone=(99,))
        raw = _FakeConn(cur)
        result = real_utils.queue_enqueue(raw, "jobs", {"x": 1}, patterns=fake_patterns)
        assert result == 99
        params = cur.execute.call_args[0][1]
        assert params == ('{"x": 1}',)

    def test_claim_returns_tuple_id_and_payload(self, fake_patterns):
        cur = _cursor(fetchone=(7, {"x": 1}, "2026-04-30T00:00", "2026-04-30T00:00"))
        raw = _FakeConn(cur)
        result = real_utils.queue_claim(raw, "jobs", 30000, patterns=fake_patterns)
        assert result == (7, {"x": 1})

    def test_claim_decodes_string_jsonb_payload(self, fake_patterns):
        cur = _cursor(fetchone=(7, '{"x": 1}', None, None))
        raw = _FakeConn(cur)
        result = real_utils.queue_claim(raw, "jobs", 30000, patterns=fake_patterns)
        assert result == (7, {"x": 1})

    def test_claim_returns_none_when_empty(self, fake_patterns):
        cur = _cursor(fetchone=None)
        raw = _FakeConn(cur)
        assert real_utils.queue_claim(raw, "jobs", 30000, patterns=fake_patterns) is None

    def test_ack_returns_true_when_deleted(self, fake_patterns):
        cur = _cursor(rowcount=1)
        raw = _FakeConn(cur)
        assert real_utils.queue_ack(raw, "jobs", 42, patterns=fake_patterns) is True

    def test_ack_returns_false_when_id_unknown(self, fake_patterns):
        cur = _cursor(rowcount=0)
        raw = _FakeConn(cur)
        assert real_utils.queue_ack(raw, "jobs", 999, patterns=fake_patterns) is False

    def test_abandon_uses_nack_pattern(self, fake_patterns):
        cur = _cursor(fetchone=(42,))
        raw = _FakeConn(cur)
        assert real_utils.queue_abandon(raw, "jobs", 42, patterns=fake_patterns) is True
        sql = cur.execute.call_args[0][0]
        assert "status = 'ready'" in sql

    def test_extend_returns_new_visible_at(self, fake_patterns):
        cur = _cursor(fetchone=("2026-05-01T00:00",))
        raw = _FakeConn(cur)
        result = real_utils.queue_extend(raw, "jobs", 42, 5000, patterns=fake_patterns)
        assert result == "2026-05-01T00:00"
        # Proxy contract: $1=id, $2=additional_ms. The proxy emits the SQL
        # via a CTE so $1 + $2 appear in source-text order — same params
        # tuple works for psycopg and native-$N drivers.
        assert cur.execute.call_args[0][1] == (42, 5000)

    def test_peek_returns_dict(self, fake_patterns):
        cur = _cursor(fetchone=(42, {"work": "foo"}, "vat", "ready", "cat"))
        raw = _FakeConn(cur)
        result = real_utils.queue_peek(raw, "jobs", patterns=fake_patterns)
        assert result == {
            "id": 42,
            "payload": {"work": "foo"},
            "visible_at": "vat",
            "status": "ready",
            "created_at": "cat",
        }

    def test_phase5_claim_and_ack_are_distinct_calls(self, fake_patterns):
        cur = _cursor(fetchone=(7, {}, None, None))
        raw = _FakeConn(cur)
        real_utils.queue_claim(raw, "jobs", 30000, patterns=fake_patterns)
        assert cur.execute.call_count == 1
        sql = cur.execute.call_args[0][0]
        assert "DELETE" not in sql.upper()
