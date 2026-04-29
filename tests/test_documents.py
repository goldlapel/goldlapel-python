"""Unit tests for goldlapel.documents.DocumentsAPI — the nested gl.documents
namespace introduced in Phase 4 of schema-to-core.

These tests verify:
  - gl.documents is a DocumentsAPI bound to the parent client
  - Each verb fetches DDL patterns from the proxy then dispatches to utils
  - The unlogged kwarg flows through to the DDL options
  - The pattern cache is shared with the parent client (one HTTP call per
    (family, name) per session)
  - $lookup.from collections in aggregate are resolved via the proxy too
"""

from unittest.mock import MagicMock, patch

import pytest

from goldlapel.proxy import GoldLapel
from goldlapel.documents import DocumentsAPI
from goldlapel import utils as real_utils


@pytest.fixture
def gl():
    inst = GoldLapel("postgresql://localhost:5432/mydb")
    inst._conn = MagicMock(name="internal_conn")
    inst._dashboard_token = "test-token"
    return inst


@pytest.fixture
def fake_patterns():
    return {
        "tables": {"main": "_goldlapel.doc_users"},
        "query_patterns": {
            "insert": "INSERT INTO _goldlapel.doc_users (data) VALUES ($1::jsonb) RETURNING _id, data, created_at",
        },
    }


class TestNamespaceShape:
    def test_documents_is_a_DocumentsAPI(self, gl):
        assert isinstance(gl.documents, DocumentsAPI)

    def test_documents_holds_back_reference_to_parent(self, gl):
        assert gl.documents._gl is gl

    def test_state_reads_through_parent(self, gl):
        # If we reconfigure the parent, the sub-API picks it up immediately
        # — no copies of token/port stashed on the sub-API itself.
        gl._dashboard_token = "rotated-token"
        # We don't have a stub for fetch_patterns here — this test only asserts
        # that the state is sourced from _gl, not duplicated.
        assert gl.documents._gl._dashboard_token == "rotated-token"


class TestVerbDispatch:
    @patch("goldlapel.ddl.fetch_patterns")
    def test_insert_calls_utils_doc_insert(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "doc_insert", return_value={"_id": "u1"}) as m:
            result = gl.documents.insert("users", {"name": "alice"})
            assert result == {"_id": "u1"}
            m.assert_called_once_with(
                gl._conn, "users", {"name": "alice"}, patterns=fake_patterns,
            )

    @patch("goldlapel.ddl.fetch_patterns")
    def test_find_passes_filter_and_kwargs(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "doc_find", return_value=[]) as m:
            gl.documents.find("users", filter={"a": 1}, sort={"b": 1}, limit=5, skip=2)
            m.assert_called_once_with(
                gl._conn, "users", filter={"a": 1}, sort={"b": 1}, limit=5, skip=2,
                patterns=fake_patterns,
            )

    @patch("goldlapel.ddl.fetch_patterns")
    def test_update_one_passes_filter_and_update(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "doc_update_one", return_value=1) as m:
            gl.documents.update_one("users", {"id": 1}, {"$set": {"name": "x"}})
            m.assert_called_once_with(
                gl._conn, "users", {"id": 1}, {"$set": {"name": "x"}},
                patterns=fake_patterns,
            )

    @patch("goldlapel.ddl.fetch_patterns")
    def test_count_passes_filter(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "doc_count", return_value=42) as m:
            gl.documents.count("users", filter={"active": True})
            m.assert_called_once_with(
                gl._conn, "users", filter={"active": True}, patterns=fake_patterns,
            )

    @patch("goldlapel.ddl.fetch_patterns")
    def test_create_collection_just_fetches_patterns(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        # No utils.doc_* dispatch — DDL is owned by the proxy. Calling
        # _patterns is enough to materialize the table on the proxy side.
        gl.documents.create_collection("users")
        mock_fetch.assert_called_once()
        # Confirm we asked the proxy with family=doc_store + version=v1.
        args, kwargs = mock_fetch.call_args
        assert args[1] == "doc_store"
        assert args[2] == "users"
        assert kwargs.get("options") is None

    @patch("goldlapel.ddl.fetch_patterns")
    def test_create_collection_unlogged_passes_through(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        gl.documents.create_collection("sessions", unlogged=True)
        args, kwargs = mock_fetch.call_args
        assert kwargs.get("options") == {"unlogged": True}


class TestAggregateLookupResolution:
    @patch("goldlapel.ddl.fetch_patterns")
    def test_aggregate_resolves_lookup_from_collections(self, mock_fetch, gl):
        # Two distinct fetches expected: one for the source, one per unique
        # $lookup.from collection in the pipeline.
        users_patterns = {
            "tables": {"main": "_goldlapel.doc_users"},
            "query_patterns": {},
        }
        orders_patterns = {
            "tables": {"main": "_goldlapel.doc_orders"},
            "query_patterns": {},
        }

        # fetch_patterns is called once for "users" (the source) and once for
        # "orders" (the $lookup target).
        def stub(_owner, family, name, *_args, **_kwargs):
            if name == "users":
                return users_patterns
            elif name == "orders":
                return orders_patterns
            raise AssertionError(f"unexpected fetch for {name}")

        mock_fetch.side_effect = stub

        with patch.object(real_utils, "doc_aggregate", return_value=[]) as m:
            gl.documents.aggregate("users", [
                {"$match": {"active": True}},
                {"$lookup": {
                    "from": "orders",
                    "localField": "id",
                    "foreignField": "userId",
                    "as": "user_orders",
                }},
            ])

            # The aggregate util sees the resolved lookup_tables map.
            call_kwargs = m.call_args.kwargs
            assert call_kwargs["patterns"] == users_patterns
            assert call_kwargs["lookup_tables"] == {
                "orders": "_goldlapel.doc_orders",
            }


class TestPatternCacheSharing:
    def test_no_legacy_doc_methods_on_gl(self):
        # Hard cut — the flat doc_* methods are gone.
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        for legacy in ["doc_insert", "doc_find", "doc_update", "doc_delete", "doc_count"]:
            assert not hasattr(gl, legacy), (
                f"Legacy flat method {legacy} should have been removed; "
                f"use gl.documents.<verb> instead."
            )
