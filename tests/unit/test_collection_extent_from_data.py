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

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from portolan_cli.crs import measure_wgs84_bbox, transform_bbox_to_wgs84
from portolan_cli.preparation import _extract_bbox_wgs84

if TYPE_CHECKING:
    import geopandas as gpd

pytestmark = pytest.mark.unit

# EPSG:25830 (ETRS89 / UTM zone 30N), the CRS Spanish regional open data ships.
UTM30N = "EPSG:25830"


def _write_diamond(
    path: Path,
    crs: str = UTM30N,
    geometry_column: str = "geometry",
) -> gpd.GeoDataFrame:
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

    def test_measured_extent_is_tighter_than_the_reprojected_rectangle(
        self, tmp_path: Path
    ) -> None:
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

    def test_measured_extent_matches_geopandas(self, tmp_path: Path) -> None:
        """The measurement must equal a straight reprojection of the geometry."""
        path = tmp_path / "diamond.parquet"
        gdf = _write_diamond(path)

        measured = measure_wgs84_bbox(path, "geometry", UTM30N)
        expected = gdf.to_crs("EPSG:4326").total_bounds

        assert measured == pytest.approx(tuple(expected), abs=1e-9)

    def test_custom_geometry_column(self, tmp_path: Path) -> None:
        path = tmp_path / "custom.parquet"
        gdf = _write_diamond(path, geometry_column="geom")

        measured = measure_wgs84_bbox(path, "geom", UTM30N)
        assert measured == pytest.approx(tuple(gdf.to_crs("EPSG:4326").total_bounds), abs=1e-9)

    def test_null_geometries_are_skipped(self, tmp_path: Path) -> None:
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

    def test_returns_none_for_wgs84_source(self, tmp_path: Path) -> None:
        """Already WGS84: the stored bbox is the answer, nothing to measure."""
        path = tmp_path / "wgs84.parquet"
        _write_diamond(path, crs="EPSG:4326")
        assert measure_wgs84_bbox(path, "geometry", "EPSG:4326") is None

    def test_returns_none_without_crs(self, tmp_path: Path) -> None:
        path = tmp_path / "diamond.parquet"
        _write_diamond(path)
        assert measure_wgs84_bbox(path, "geometry", None) is None

    def test_returns_none_for_missing_column(self, tmp_path: Path) -> None:
        path = tmp_path / "diamond.parquet"
        _write_diamond(path)
        assert measure_wgs84_bbox(path, "not_a_column", UTM30N) is None

    def test_returns_none_for_unreadable_file(self, tmp_path: Path) -> None:
        missing = tmp_path / "nope.parquet"
        assert measure_wgs84_bbox(missing, "geometry", UTM30N) is None


class TestExtractBboxWgs84UsesMeasurement:
    """_extract_bbox_wgs84 prefers the measurement and falls back safely."""

    def test_uses_measurement_when_data_path_given(self, tmp_path: Path) -> None:
        from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata

        path = tmp_path / "diamond.parquet"
        gdf = _write_diamond(path)
        metadata = extract_geoparquet_metadata(path)

        bbox = _extract_bbox_wgs84(metadata, path)
        expected = gdf.to_crs("EPSG:4326").total_bounds

        assert bbox == pytest.approx(list(expected), abs=1e-9)

    def test_falls_back_to_the_rectangle_without_a_data_path(self, tmp_path: Path) -> None:
        from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata

        path = tmp_path / "diamond.parquet"
        _write_diamond(path)
        metadata = extract_geoparquet_metadata(path)

        bbox = _extract_bbox_wgs84(metadata)
        assert bbox == list(transform_bbox_to_wgs84(metadata.bbox, UTM30N))


class TestAntimeridian:
    """A crossing extent must stay compact, not widen to the whole globe."""

    # EPSG:3832 (WGS 84 / PDC Mercator) centres on 150E, so it crosses the
    # antimeridian without a break in its own coordinates.
    PDC = "EPSG:3832"

    def test_crossing_extent_keeps_west_greater_than_east(self, tmp_path: Path) -> None:
        """RFC 7946 writes a crossing bbox as west > east."""
        import geopandas as gpd
        from shapely.geometry import Point

        path = tmp_path / "antimeridian.parquet"
        # Either side of 180: about 178.7E and 177.7W.
        points = [Point(3.2e6, -1.0e5), Point(3.6e6, 1.0e5)]
        gpd.GeoDataFrame({"n": [1, 2]}, geometry=points, crs=self.PDC).to_parquet(path)

        measured = measure_wgs84_bbox(path, "geometry", self.PDC)
        assert measured is not None
        west, _, east, _ = measured
        assert west > east, "a crossing bbox must not be flattened to a global one"
        assert west == pytest.approx(178.746, abs=0.01)
        assert east == pytest.approx(-177.661, abs=0.01)

    def test_crossing_extent_agrees_with_the_bbox_transform(self, tmp_path: Path) -> None:
        """The measurement must not contradict the fallback on the same data."""
        import geopandas as gpd
        from shapely.geometry import Point

        path = tmp_path / "antimeridian.parquet"
        points = [Point(3.2e6, -1.0e5), Point(3.6e6, 1.0e5)]
        gdf = gpd.GeoDataFrame({"n": [1, 2]}, geometry=points, crs=self.PDC)
        gdf.to_parquet(path)

        measured = measure_wgs84_bbox(path, "geometry", self.PDC)
        rectangle = transform_bbox_to_wgs84(tuple(gdf.total_bounds), self.PDC)
        assert measured == pytest.approx(tuple(rectangle), abs=0.01)

    def test_non_crossing_data_keeps_the_plain_frame(self, tmp_path: Path) -> None:
        """Data far from the antimeridian must be untouched by the shift."""
        path = tmp_path / "diamond.parquet"
        gdf = _write_diamond(path)

        measured = measure_wgs84_bbox(path, "geometry", UTM30N)
        assert measured == pytest.approx(tuple(gdf.to_crs("EPSG:4326").total_bounds), abs=1e-9)
        assert measured is not None and measured[0] < measured[2]


class TestTransformerFailureFallsBack:
    """A CRS pair pyproj cannot bridge must fall back, not raise."""

    def test_transformer_construction_failure_returns_none(self, tmp_path: Path) -> None:
        path = tmp_path / "diamond.parquet"
        _write_diamond(path)

        import portolan_cli.crs as crs_module

        def _boom(*args: object, **kwargs: object) -> None:
            raise RuntimeError("no pipeline between these CRSs")

        original = crs_module.Transformer.from_crs
        crs_module.Transformer.from_crs = _boom  # type: ignore[assignment]
        try:
            assert measure_wgs84_bbox(path, "geometry", UTM30N) is None
        finally:
            crs_module.Transformer.from_crs = original  # type: ignore[assignment]

    def test_extract_bbox_falls_back_when_measurement_fails(self, tmp_path: Path) -> None:
        """The caller still gets a bbox, from the reprojected rectangle."""
        from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata

        path = tmp_path / "diamond.parquet"
        _write_diamond(path)
        metadata = extract_geoparquet_metadata(path)

        import portolan_cli.preparation as prep

        original = prep.measure_wgs84_bbox
        prep.measure_wgs84_bbox = lambda *a, **k: None  # type: ignore[assignment]
        try:
            bbox = _extract_bbox_wgs84(metadata, path)
        finally:
            prep.measure_wgs84_bbox = original  # type: ignore[assignment]

        assert bbox == list(transform_bbox_to_wgs84(metadata.bbox, UTM30N))
