"""Collection-level projection metadata lands as ``proj:code`` on the data asset.

Issue #654 (spec reconciliation): projection v2.0.0 removed ``proj:epsg``.
The reference catalog in portolan-spec places ``proj:code`` (e.g.
``"EPSG:4269"``) on the collection's *data asset*, never on the collection
top level. These tests replace the top-level ``proj:epsg`` behavior that
tests/unit/test_collection_proj_epsg_488.py used to pin.

The issue #488 clobber scenario (a PMTiles companion's hardcoded Web-Mercator
tile CRS overwriting the real source CRS) is eliminated structurally: PMTiles
metadata no longer contributes any projection field at all.
"""

from __future__ import annotations

import pystac
import pytest

from portolan_cli.metadata.geoparquet import GeoParquetMetadata
from portolan_cli.metadata.pmtiles import PMTilesMetadata
from portolan_cli.stac import (
    EXTENSION_URLS,
    add_collection_properties_from_metadata,
    create_collection,
)


def _geoparquet(crs: str) -> GeoParquetMetadata:
    return GeoParquetMetadata(
        bbox=None,
        crs=crs,
        geometry_type="Polygon",
        geometry_column="geometry",
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


def _collection_with_assets(**assets: pystac.Asset) -> pystac.Collection:
    collection = create_collection(
        collection_id="soil-maps",
        description="Soil maps in RD New.",
    )
    for key, asset in assets.items():
        collection.add_asset(key, asset)
    return collection


def _data_asset() -> pystac.Asset:
    return pystac.Asset(
        href="./soil-maps.parquet",
        media_type="application/vnd.apache.parquet",
        roles=["data"],
    )


def _pmtiles_asset() -> pystac.Asset:
    return pystac.Asset(
        href="./soil-maps.pmtiles",
        media_type="application/vnd.pmtiles",
        roles=["visual"],
    )


class TestCollectionProjCode:
    @pytest.mark.unit
    def test_data_asset_gets_proj_code(self) -> None:
        """GeoParquet CRS lands as proj:code on the data asset (reference shape)."""
        collection = _collection_with_assets(data=_data_asset())

        add_collection_properties_from_metadata(
            collection, _geoparquet("EPSG:4258"), asset_keys=["data"]
        )

        assert collection.assets["data"].extra_fields["proj:code"] == "EPSG:4258"
        assert "proj:epsg" not in collection.extra_fields
        assert "proj:code" not in collection.extra_fields
        assert EXTENSION_URLS["projection"] in (collection.stac_extensions or [])

    @pytest.mark.unit
    def test_pmtiles_contributes_no_projection(self) -> None:
        """PMTiles metadata contributes pmtiles:* fields but no projection field."""
        collection = _collection_with_assets(tiles=_pmtiles_asset())

        add_collection_properties_from_metadata(collection, _pmtiles(), asset_keys=["tiles"])

        assert "proj:code" not in collection.assets["tiles"].extra_fields
        assert "proj:epsg" not in collection.extra_fields
        assert "proj:code" not in collection.extra_fields
        assert EXTENSION_URLS["projection"] not in (collection.stac_extensions or [])
        # pmtiles:* properties still land on the collection
        assert collection.extra_fields["pmtiles:min_zoom"] == 0

    @pytest.mark.unit
    def test_pmtiles_never_clobbers_data_crs(self) -> None:
        """#488 regression, restated: tile CRS never displaces the data CRS."""
        collection = _collection_with_assets(data=_data_asset(), tiles=_pmtiles_asset())

        add_collection_properties_from_metadata(collection, _pmtiles(), asset_keys=["tiles"])
        add_collection_properties_from_metadata(
            collection, _geoparquet("EPSG:28992"), asset_keys=["data"]
        )

        assert collection.assets["data"].extra_fields["proj:code"] == "EPSG:28992"
        assert "proj:code" not in collection.assets["tiles"].extra_fields

    @pytest.mark.unit
    def test_legacy_top_level_proj_epsg_is_removed(self) -> None:
        """A stale top-level proj:epsg from an older catalog is stripped on re-add."""
        collection = _collection_with_assets(data=_data_asset())
        collection.extra_fields["proj:epsg"] = 3857

        add_collection_properties_from_metadata(
            collection, _geoparquet("EPSG:4258"), asset_keys=["data"]
        )

        assert "proj:epsg" not in collection.extra_fields
        assert collection.assets["data"].extra_fields["proj:code"] == "EPSG:4258"

    @pytest.mark.unit
    def test_unknown_crs_contributes_no_proj_code(self) -> None:
        """A CRS that resolves to no EPSG code writes no proj:code and no declaration."""
        collection = _collection_with_assets(data=_data_asset())

        add_collection_properties_from_metadata(
            collection, _geoparquet("not-a-crs"), asset_keys=["data"]
        )

        assert "proj:code" not in collection.assets["data"].extra_fields
        assert EXTENSION_URLS["projection"] not in (collection.stac_extensions or [])

    @pytest.mark.unit
    def test_defaults_to_data_role_assets_when_keys_omitted(self) -> None:
        """Without explicit keys, assets carrying the data role receive proj:code."""
        collection = _collection_with_assets(data=_data_asset(), tiles=_pmtiles_asset())

        add_collection_properties_from_metadata(collection, _geoparquet("EPSG:4269"))

        assert collection.assets["data"].extra_fields["proj:code"] == "EPSG:4269"
        assert "proj:code" not in collection.assets["tiles"].extra_fields
