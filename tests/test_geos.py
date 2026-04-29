"""Unit tests for goldlapel.geos.GeosAPI.

Phase 5 schema decisions:
  - GEOGRAPHY column type (not GEOMETRY) — distance returns are meters native.
  - `member TEXT PRIMARY KEY` — re-adding a member updates its location
    (idempotent), matching Redis GEOADD semantics.
  - `updated_at` stamped on every UPSERT.

These tests verify:
  - `add` is idempotent on member name (the proxy's ON CONFLICT DO UPDATE).
  - SQL uses the canonical GEOGRAPHY-native pattern (no `::geography` casts
    on the column reference because the column already IS geography).
  - Distance unit conversion at the wrapper edge (m / km / mi / ft).
"""

from unittest.mock import MagicMock, patch

import pytest

from goldlapel.proxy import GoldLapel
from goldlapel.geos import GeosAPI
from goldlapel import utils as real_utils


@pytest.fixture
def gl():
    inst = GoldLapel("postgresql://localhost:5432/mydb")
    inst._conn = MagicMock(name="internal_conn")
    inst._dashboard_token = "test-token"
    return inst


@pytest.fixture
def fake_patterns():
    main = "_goldlapel.geo_riders"
    return {
        "tables": {"main": main},
        "query_patterns": {
            "geoadd": f"INSERT INTO {main} (member, location, updated_at) VALUES ($1, ST_SetSRID(ST_MakePoint($2, $3), 4326)::geography, NOW()) ON CONFLICT (member) DO UPDATE SET location = EXCLUDED.location, updated_at = NOW() RETURNING ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat",
            "geopos": f"SELECT ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat FROM {main} WHERE member = $1",
            "geodist": f"SELECT ST_Distance(a.location, b.location) AS distance_m FROM {main} a, {main} b WHERE a.member = $1 AND b.member = $2",
            "georadius": f"SELECT member, ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat FROM {main} WHERE ST_DWithin(location, ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography, $2) ORDER BY ST_Distance(location, ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography) LIMIT $5",
            "georadius_with_dist": f"SELECT member, ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat, ST_Distance(location, ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography) AS distance_m FROM {main} WHERE ST_DWithin(location, ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography, $2) ORDER BY distance_m LIMIT $5",
            "geosearch_member": f"SELECT b.member, ST_X(b.location::geometry) AS lon, ST_Y(b.location::geometry) AS lat, ST_Distance(b.location, a.location) AS distance_m FROM {main} a, {main} b WHERE a.member = $1 AND ST_DWithin(b.location, a.location, $2) AND b.member <> $1 ORDER BY distance_m LIMIT $3",
            "geo_remove": f"DELETE FROM {main} WHERE member = $1",
            "geo_count": f"SELECT COUNT(*) FROM {main}",
            "delete_all": f"DELETE FROM {main}",
        },
    }


class TestNamespaceShape:
    def test_geos_is_a_GeosAPI(self, gl):
        assert isinstance(gl.geos, GeosAPI)

    def test_no_legacy_flat_methods(self):
        gl = GoldLapel("postgresql://localhost:5432/mydb")
        for legacy in ["geoadd", "geodist", "georadius"]:
            assert not hasattr(gl, legacy)


class TestVerbDispatch:
    @patch("goldlapel.ddl.fetch_patterns")
    def test_add_passes_member_lon_lat(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "geo_add", return_value=(13.4, 52.5)) as m:
            gl.geos.add("riders", "alice", 13.4, 52.5)
            m.assert_called_once_with(
                gl._conn, "riders", "alice", 13.4, 52.5, patterns=fake_patterns,
            )

    @patch("goldlapel.ddl.fetch_patterns")
    def test_dist_passes_unit_kwarg(self, mock_fetch, gl, fake_patterns):
        mock_fetch.return_value = fake_patterns
        with patch.object(real_utils, "geo_dist", return_value=2.5) as m:
            gl.geos.dist("riders", "alice", "bob", unit="km")
            m.assert_called_once_with(
                gl._conn, "riders", "alice", "bob", unit="km", patterns=fake_patterns,
            )


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commit = MagicMock()

    def cursor(self):
        return self._cursor


def _cursor(*, fetchone=None, fetchall=None, description=None, rowcount=0):
    cur = MagicMock()
    cur.fetchone.return_value = fetchone
    cur.fetchall.return_value = fetchall or []
    cur.rowcount = rowcount
    if description is not None:
        cur.description = description
    return cur


class TestSqlBuilders:
    def test_geo_add_is_idempotent_via_on_conflict(self, fake_patterns):
        cur = _cursor(fetchone=(13.4, 52.5))
        raw = _FakeConn(cur)
        real_utils.geo_add(raw, "riders", "alice", 13.4, 52.5, patterns=fake_patterns)
        assert cur.execute.call_count == 1
        sql = cur.execute.call_args[0][0]
        assert "ON CONFLICT (member)" in sql
        assert "DO UPDATE" in sql

    def test_geo_add_pattern_is_geography_native(self, fake_patterns):
        sql = fake_patterns["query_patterns"]["geoadd"]
        assert "GEOGRAPHY" in sql.upper() or "geography" in sql

    def test_geo_pos_returns_lon_lat_tuple(self, fake_patterns):
        cur = _cursor(fetchone=(13.4, 52.5))
        raw = _FakeConn(cur)
        assert real_utils.geo_pos(raw, "riders", "alice", patterns=fake_patterns) == (13.4, 52.5)

    def test_geo_pos_returns_none_for_unknown_member(self, fake_patterns):
        cur = _cursor(fetchone=None)
        raw = _FakeConn(cur)
        assert real_utils.geo_pos(raw, "riders", "missing", patterns=fake_patterns) is None

    def test_geo_dist_returns_meters_by_default(self, fake_patterns):
        cur = _cursor(fetchone=(1234.0,))
        raw = _FakeConn(cur)
        assert real_utils.geo_dist(raw, "riders", "alice", "bob", patterns=fake_patterns) == 1234.0

    def test_geo_dist_converts_to_km(self, fake_patterns):
        cur = _cursor(fetchone=(1234.0,))
        raw = _FakeConn(cur)
        assert real_utils.geo_dist(raw, "riders", "alice", "bob", unit="km", patterns=fake_patterns) == 1.234

    def test_geo_dist_converts_to_miles(self, fake_patterns):
        cur = _cursor(fetchone=(1609.344,))
        raw = _FakeConn(cur)
        result = real_utils.geo_dist(raw, "riders", "alice", "bob", unit="mi", patterns=fake_patterns)
        assert result == pytest.approx(1.0, rel=1e-6)

    def test_geo_dist_unknown_unit_raises(self, fake_patterns):
        cur = _cursor(fetchone=(1.0,))
        raw = _FakeConn(cur)
        with pytest.raises(ValueError, match="Unknown distance unit"):
            real_utils.geo_dist(raw, "riders", "a", "b", unit="parsec", patterns=fake_patterns)

    def test_geo_radius_converts_unit_to_meters_for_query(self, fake_patterns):
        cur = _cursor(fetchall=[], description=[("member",), ("lon",), ("lat",), ("distance_m",)])
        raw = _FakeConn(cur)
        real_utils.geo_radius(raw, "riders", 13.4, 52.5, 5, unit="km", patterns=fake_patterns)
        params = cur.execute.call_args[0][1]
        # Proxy contract: $1=lon, $2=lat, $3=radius_m, $4=limit. CTE anchor
        # means each $N appears exactly once in the rendered SQL.
        assert params == (13.4, 52.5, 5000.0, 50)

    def test_geo_radius_by_member_passes_member_twice(self, fake_patterns):
        cur = _cursor(fetchall=[], description=[("member",), ("lon",), ("lat",), ("distance_m",)])
        raw = _FakeConn(cur)
        real_utils.geo_radius_by_member(raw, "riders", "alice", 1000, patterns=fake_patterns)
        params = cur.execute.call_args[0][1]
        # Proxy `geosearch_member` after `$N → %s` (psycopg) keeps source
        # order: a.member=$1 → %s, ST_DWithin(...,$3) → %s, b.member<>$2 → %s,
        # LIMIT $4 → %s. Params bind in that source-order sequence.
        assert params == ("alice", 1000.0, "alice", 50)

    def test_geo_remove_returns_true_when_deleted(self, fake_patterns):
        cur = _cursor(rowcount=1)
        raw = _FakeConn(cur)
        assert real_utils.geo_remove(raw, "riders", "alice", patterns=fake_patterns) is True

    def test_geo_count_query(self, fake_patterns):
        cur = _cursor(fetchone=(3,))
        raw = _FakeConn(cur)
        assert real_utils.geo_count(raw, "riders", patterns=fake_patterns) == 3
