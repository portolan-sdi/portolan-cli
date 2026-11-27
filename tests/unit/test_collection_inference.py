"""Unit tests for collection ID inference based on format type."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from portolan_cli.collection_id import infer_nested_collection_id

if TYPE_CHECKING:
    from pathlib import Path


class TestCollectionInferenceByFormat:
    """Tests that collection inference respects format distinctions."""

    @pytest.mark.unit
    def test_vector_file_uses_parent_as_collection(self, tmp_path: Path) -> None:
        catalog_root = tmp_path
        collection_dir = catalog_root / "demographics"
        collection_dir.mkdir()
        vector_file = collection_dir / "boundaries.parquet"
        vector_file.write_bytes(b"PAR1" + b"\x00" * 100)  # Minimal parquet magic

        result = infer_nested_collection_id(vector_file, catalog_root)

        assert result == "demographics"

    @pytest.mark.unit
    def test_raster_file_uses_grandparent_as_collection(self, tmp_path: Path) -> None:
        catalog_root = tmp_path
        collection_dir = catalog_root / "2025"
        collection_dir.mkdir()
        item_dir = collection_dir / "tile1"
        item_dir.mkdir()
        raster_file = item_dir / "data.tif"
        raster_file.write_bytes(b"II*\x00" + b"\x00" * 100)  # Minimal TIFF magic

        result = infer_nested_collection_id(raster_file, catalog_root)

        assert result == "2025"

    @pytest.mark.unit
    def test_nested_raster_uses_correct_depth(self, tmp_path: Path) -> None:
        catalog_root = tmp_path
        (catalog_root / "imagery" / "2025" / "tile1").mkdir(parents=True)
        raster_file = catalog_root / "imagery" / "2025" / "tile1" / "scene.tif"
        raster_file.write_bytes(b"II*\x00" + b"\x00" * 100)

        result = infer_nested_collection_id(raster_file, catalog_root)

        assert result == "imagery/2025"

    @pytest.mark.unit
    def test_nested_vector_uses_parent(self, tmp_path: Path) -> None:
        catalog_root = tmp_path
        (catalog_root / "environment" / "air-quality").mkdir(parents=True)
        vector_file = catalog_root / "environment" / "air-quality" / "pm25.parquet"
        vector_file.write_bytes(b"PAR1" + b"\x00" * 100)

        result = infer_nested_collection_id(vector_file, catalog_root)

        assert result == "environment/air-quality"

    @pytest.mark.unit
    def test_geojson_uses_parent_as_collection(self, tmp_path: Path) -> None:
        catalog_root = tmp_path
        collection_dir = catalog_root / "boundaries"
        collection_dir.mkdir()
        geojson_file = collection_dir / "regions.geojson"
        geojson_file.write_text('{"type": "FeatureCollection", "features": []}')

        result = infer_nested_collection_id(geojson_file, catalog_root)

        assert result == "boundaries"

    @pytest.mark.unit
    def test_cog_uses_grandparent_as_collection(self, tmp_path: Path) -> None:
        catalog_root = tmp_path
        (catalog_root / "satellite" / "scene-001").mkdir(parents=True)
        cog_file = catalog_root / "satellite" / "scene-001" / "B04.tif"
        cog_file.write_bytes(b"II*\x00" + b"\x00" * 100)

        result = infer_nested_collection_id(cog_file, catalog_root)

        assert result == "satellite"

    @pytest.mark.unit
    def test_raster_at_shallow_depth_raises_error(self, tmp_path: Path) -> None:
        catalog_root = tmp_path
        collection_dir = catalog_root / "imagery"
        collection_dir.mkdir()
        raster_file = collection_dir / "data.tif"
        raster_file.write_bytes(b"II*\x00" + b"\x00" * 100)

        with pytest.raises(ValueError, match="must be in a subdirectory"):
            infer_nested_collection_id(raster_file, catalog_root)

    @pytest.mark.unit
    def test_shapefile_uses_parent_as_collection(self, tmp_path: Path) -> None:
        catalog_root = tmp_path
        collection_dir = catalog_root / "parcels"
        collection_dir.mkdir()
        shp_file = collection_dir / "boundaries.shp"
        shp_file.write_bytes(b"\x00" * 100)  # Minimal content

        result = infer_nested_collection_id(shp_file, catalog_root)

        assert result == "parcels"

    @pytest.mark.unit
    def test_geopackage_uses_parent_as_collection(self, tmp_path: Path) -> None:
        catalog_root = tmp_path
        collection_dir = catalog_root / "transportation"
        collection_dir.mkdir()
        gpkg_file = collection_dir / "roads.gpkg"
        gpkg_file.write_bytes(b"SQLite format 3\x00" + b"\x00" * 100)

        result = infer_nested_collection_id(gpkg_file, catalog_root)

        assert result == "transportation"
