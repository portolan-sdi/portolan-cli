"""Regression tests for issue #488.

`portolan add` wrote ``proj:epsg: 3857`` into a collection regardless of the
source data's real CRS. Root cause: a tracked ``.pmtiles`` companion contributes
a hardcoded ``proj:epsg: 3857`` (the Web-Mercator *tiles*), and the GeoParquet
vector asset contributed no ``proj:epsg`` at all, so the tile CRS became the
collection's CRS.

The collection-level ``proj:epsg`` must reflect the source *data* CRS. The
PMTiles tile CRS is a visualization artifact and must never overwrite a real
source CRS, regardless of the order the assets are applied.
"""

from __future__ import annotations

import pytest

from portolan_cli.metadata.geoparquet import GeoParquetMetadata
from portolan_cli.metadata.pmtiles import PMTilesMetadata
from portolan_cli.stac import add_collection_properties_from_metadata, create_collection


def _geoparquet(crs: str) -> GeoParquetMetadata:
    return GeoParquetMetadata(
        bbox=None,
        crs=crs,
        geometry_type="Polygon",
        geometry_column="standardized_location",
        feature_count=10,
        schema={},
    )


def _pmtiles() -> PMTilesMetadata:
    return PMTilesMetadata(
        bbox=(-180.0, -90.0, 180.0, 90.0),
        min_zoom=0,
        max_zoom=14,
        tile_type="mvt",
        center=None,
        layer_name="data",
    )


class TestCollectionProjEpsg488:
    @pytest.mark.unit
    def test_pmtiles_does_not_clobber_geoparquet_crs(self) -> None:
        """GeoParquet applied first, then PMTiles: real CRS must survive."""
        collection = create_collection(
            collection_id="soil-maps",
            description="Soil maps in RD New.",
        )
        add_collection_properties_from_metadata(collection, _geoparquet("EPSG:4258"))
        add_collection_properties_from_metadata(collection, _pmtiles())

        assert collection.extra_fields["proj:epsg"] == 4258

    @pytest.mark.unit
    def test_geoparquet_crs_wins_regardless_of_order(self) -> None:
        """PMTiles applied first, then GeoParquet: real CRS must still win."""
        collection = create_collection(
            collection_id="soil-maps",
            description="Soil maps in RD New.",
        )
        add_collection_properties_from_metadata(collection, _pmtiles())
        add_collection_properties_from_metadata(collection, _geoparquet("EPSG:28992"))

        assert collection.extra_fields["proj:epsg"] == 28992

    @pytest.mark.unit
    def test_pmtiles_only_collection_still_reports_tile_crs(self) -> None:
        """With no vector source CRS, PMTiles 3857 remains a valid fallback."""
        collection = create_collection(
            collection_id="tiles-only",
            description="A PMTiles-only collection.",
        )
        add_collection_properties_from_metadata(collection, _pmtiles())

        assert collection.extra_fields["proj:epsg"] == 3857
