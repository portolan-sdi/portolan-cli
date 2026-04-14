"""Tests for spatial partitioning utilities (Phase 2).

Tests for geohash precision detection, geohash column computation,
and bbox column computation.
"""

import struct

import pyarrow as pa
import pytest

from portolan_cli.backends.iceberg.spatial import (
    compute_bbox_columns,
    compute_geohash_column,
    detect_geohash_precision,
)


def _make_wkb_point(x: float, y: float) -> bytes:
    """Create a WKB point (little-endian)."""
    return struct.pack("<BIdd", 1, 1, x, y)


def _make_geo_table(points: list[tuple[float, float]]) -> pa.Table:
    """Create a PyArrow table with a WKB geometry column."""
    wkb_values = [_make_wkb_point(x, y) for x, y in points]
    return pa.table(
        {
            "id": pa.array(range(len(points)), type=pa.int64()),
            "geometry": pa.array(wkb_values, type=pa.binary()),
        }
    )


# --- detect_geohash_precision ---


@pytest.mark.unit
def test_detect_precision_no_partitioning():
    """<100K rows should return None (no partitioning needed)."""
    assert detect_geohash_precision(0) is None
    assert detect_geohash_precision(1000) is None
    assert detect_geohash_precision(99_999) is None


@pytest.mark.unit
def test_detect_precision_medium():
    """100K to <10M rows should return precision 3 (~150km cells)."""
    assert detect_geohash_precision(100_000) == 3
    assert detect_geohash_precision(5_000_000) == 3
    assert detect_geohash_precision(9_999_999) == 3


@pytest.mark.unit
def test_detect_precision_large():
    """>=10M rows should return precision 4 (~20km cells)."""
    assert detect_geohash_precision(10_000_000) == 4
    assert detect_geohash_precision(100_000_000) == 4
    assert detect_geohash_precision(2_540_000_000) == 4


# --- compute_geohash_column ---


@pytest.mark.unit
def test_compute_geohash_column_adds_column():
    """Should add a geohash_{precision} column to the table."""
    table = _make_geo_table([(2.3522, 48.8566), (-73.9857, 40.7484)])  # Paris, NYC
    result = compute_geohash_column(table, precision=4)

    assert "geohash_4" in result.column_names
    assert len(result) == 2


@pytest.mark.unit
def test_compute_geohash_column_correct_values():
    """Geohash values should match expected prefixes for known coordinates."""
    table = _make_geo_table([(2.3522, 48.8566)])  # Paris
    result = compute_geohash_column(table, precision=4)

    geohash_val = result.column("geohash_4")[0].as_py()
    assert len(geohash_val) == 4
    # Paris should start with 'u09t' at precision 4
    assert geohash_val.startswith("u09")


@pytest.mark.unit
def test_compute_geohash_column_precision_3():
    """Should work with precision 3."""
    table = _make_geo_table([(2.3522, 48.8566)])
    result = compute_geohash_column(table, precision=3)

    assert "geohash_3" in result.column_names
    geohash_val = result.column("geohash_3")[0].as_py()
    assert len(geohash_val) == 3


@pytest.mark.unit
def test_compute_geohash_column_preserves_existing():
    """Existing columns should be preserved."""
    table = _make_geo_table([(0.0, 0.0)])
    result = compute_geohash_column(table, precision=4)

    assert "id" in result.column_names
    assert "geometry" in result.column_names
    assert result.column("id")[0].as_py() == 0


@pytest.mark.unit
def test_compute_geohash_column_no_geometry_returns_unchanged():
    """Table without geometry column should be returned unchanged."""
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})
    result = compute_geohash_column(table, precision=4)

    assert "geohash_4" not in result.column_names
    assert result.equals(table)


# --- compute_bbox_columns ---


@pytest.mark.unit
def test_compute_bbox_columns_adds_four_columns():
    """Should add bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax columns."""
    table = _make_geo_table([(2.3522, 48.8566), (-73.9857, 40.7484)])
    result = compute_bbox_columns(table)

    for col in ["bbox_xmin", "bbox_ymin", "bbox_xmax", "bbox_ymax"]:
        assert col in result.column_names


@pytest.mark.unit
def test_compute_bbox_columns_correct_values_for_points():
    """For points, bbox min and max should be equal."""
    table = _make_geo_table([(2.3522, 48.8566)])
    result = compute_bbox_columns(table)

    assert result.column("bbox_xmin")[0].as_py() == pytest.approx(2.3522)
    assert result.column("bbox_xmax")[0].as_py() == pytest.approx(2.3522)
    assert result.column("bbox_ymin")[0].as_py() == pytest.approx(48.8566)
    assert result.column("bbox_ymax")[0].as_py() == pytest.approx(48.8566)


@pytest.mark.unit
def test_compute_bbox_columns_no_geometry_returns_unchanged():
    """Table without geometry column should be returned unchanged."""
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})
    result = compute_bbox_columns(table)

    assert "bbox_xmin" not in result.column_names
    assert result.equals(table)


@pytest.mark.unit
def test_compute_bbox_columns_preserves_existing():
    """Existing columns should be preserved."""
    table = _make_geo_table([(10.0, 20.0)])
    result = compute_bbox_columns(table)

    assert "id" in result.column_names
    assert "geometry" in result.column_names
