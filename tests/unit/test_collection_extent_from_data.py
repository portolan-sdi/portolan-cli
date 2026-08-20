"""Regression tests for collection extents measured from data (PTL-DAT-005).

``_extract_bbox_wgs84`` reprojected the GeoParquet's stored bbox — a *rectangle*
in the source CRS. For a projected CRS the WGS84 envelope of that rectangle is
strictly larger than the envelope of the data inside it, because the corners of
a UTM bbox hold no data and the grid lines curve. The declared extent therefore
overstated the data on every collection built in a native projected CRS, and
`check` rejected it against the PMTiles built from the same file (PTL-DAT-005),
which report the tighter, correct box.
"""

from __future__ import annotations

import pytest

from portolan_cli.crs import measure_wgs84_bbox, transform_bbox_to_wgs84
from portolan_cli.preparation import _extract_bbox_wgs84

pytestmark = pytest.mark.unit

# EPSG:25830 (ETRS89 / UTM zone 30N), the CRS Spanish regional open data ships.
UTM30N = "EPSG:25830"


def _write_diamond(path, crs=UTM30N, geometry_column="geometry"):
    """A diamond whose bbox corners hold no data — the shape the bug overstates."""
    import geopandas as gpd
    from shapely.geometry import Polygon

    # Spans ~200 km, centred on Extremadura; the bbox corners sit far outside it.
    diamond = Polygon([(600000, 4300000), (700000, 4400000), (600000, 4500000), (500000, 4400000)])
    gdf = gpd.GeoDataFrame({"name": ["diamond"]}, geometry=[diamond], crs=crs)
    if geometry_column != "geometry":
        gdf = gdf.rename_geometry(geometry_column)
    gdf.to_parquet(path)
    return gdf


class TestMeasureWgs84Bbox:
    """measure_wgs84_bbox reads geometry; transform_bbox_to_wgs84 reads a rectangle."""

    def test_measured_extent_is_tighter_than_the_reprojected_rectangle(self, tmp_path):
        path = tmp_path / "diamond.parquet"
        gdf = _write_diamond(path)

        measured = measure_wgs84_bbox(path, "geometry", UTM30N)
        assert measured is not None

        native = tuple(gdf.total_bounds)
        rectangle = transform_bbox_to_wgs84(native, UTM30N)

        # Every side of the measured box sits inside the reprojected rectangle,
        # and at least one is strictly tighter — that gap is the defect.
        assert measured[0] >= rectangle[0]
        assert measured[1] >= rectangle[1]
        assert measured[2] <= rectangle[2]
        assert measured[3] <= rectangle[3]
        assert measured != tuple(rectangle)

    def test_measured_extent_matches_geopandas(self, tmp_path):
        """The measurement must equal a straight reprojection of the geometry."""
        path = tmp_path / "diamond.parquet"
        gdf = _write_diamond(path)

        measured = measure_wgs84_bbox(path, "geometry", UTM30N)
        expected = gdf.to_crs("EPSG:4326").total_bounds

        assert measured == pytest.approx(tuple(expected), abs=1e-9)

    def test_custom_geometry_column(self, tmp_path):
        path = tmp_path / "custom.parquet"
        gdf = _write_diamond(path, geometry_column="geom")

        measured = measure_wgs84_bbox(path, "geom", UTM30N)
        assert measured == pytest.approx(tuple(gdf.to_crs("EPSG:4326").total_bounds), abs=1e-9)

    def test_null_geometries_are_skipped(self, tmp_path):
        """NULL geometry is legal in GeoParquet and must not poison the extent."""
        import geopandas as gpd
        from shapely.geometry import Polygon

        path = tmp_path / "with_nulls.parquet"
        diamond = Polygon(
            [(600000, 4300000), (700000, 4400000), (600000, 4500000), (500000, 4400000)]
        )
        gdf = gpd.GeoDataFrame({"name": ["a", "b"]}, geometry=[diamond, None], crs=UTM30N)
        gdf.to_parquet(path)

        measured = measure_wgs84_bbox(path, "geometry", UTM30N)
        assert measured is not None
        assert all(v == v for v in measured)  # no NaN

    def test_returns_none_for_wgs84_source(self, tmp_path):
        """Already WGS84: the stored bbox is the answer, nothing to measure."""
        path = tmp_path / "wgs84.parquet"
        _write_diamond(path, crs="EPSG:4326")
        assert measure_wgs84_bbox(path, "geometry", "EPSG:4326") is None

    def test_returns_none_without_crs(self, tmp_path):
        path = tmp_path / "diamond.parquet"
        _write_diamond(path)
        assert measure_wgs84_bbox(path, "geometry", None) is None

    def test_returns_none_for_missing_column(self, tmp_path):
        path = tmp_path / "diamond.parquet"
        _write_diamond(path)
        assert measure_wgs84_bbox(path, "not_a_column", UTM30N) is None

    def test_returns_none_for_unreadable_file(self, tmp_path):
        missing = tmp_path / "nope.parquet"
        assert measure_wgs84_bbox(missing, "geometry", UTM30N) is None


class TestExtractBboxWgs84UsesMeasurement:
    """_extract_bbox_wgs84 prefers the measurement and falls back safely."""

    def test_uses_measurement_when_data_path_given(self, tmp_path):
        from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata

        path = tmp_path / "diamond.parquet"
        gdf = _write_diamond(path)
        metadata = extract_geoparquet_metadata(path)

        bbox = _extract_bbox_wgs84(metadata, path)
        expected = gdf.to_crs("EPSG:4326").total_bounds

        assert bbox == pytest.approx(list(expected), abs=1e-9)

    def test_falls_back_to_the_rectangle_without_a_data_path(self, tmp_path):
        from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata

        path = tmp_path / "diamond.parquet"
        _write_diamond(path)
        metadata = extract_geoparquet_metadata(path)

        bbox = _extract_bbox_wgs84(metadata)
        assert bbox == list(transform_bbox_to_wgs84(metadata.bbox, UTM30N))
