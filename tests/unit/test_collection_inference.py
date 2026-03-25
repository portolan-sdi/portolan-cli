"""Unit tests for collection ID inference based on format type.

Per ADR-0031:
- Vector data: parent directory = collection (collection-level asset)
- Raster data: grandparent directory = collection, parent = item

This ensures raster tiles like 2025/tile1/data.tif become:
- Collection: 2025
- Item: tile1

Not (incorrectly):
- Collection: 2025/tile1
- Item: data (collection-level)
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.dataset import infer_nested_collection_id


class TestCollectionInferenceByFormat:
    """Tests that collection inference respects ADR-0031 format distinctions."""

    @pytest.mark.unit
    def test_vector_file_uses_parent_as_collection(self, tmp_path: Path) -> None:
        """Vector files should use parent directory as collection.

        Structure: demographics/boundaries.parquet
        Expected: collection = "demographics"
        """
        catalog_root = tmp_path
        collection_dir = catalog_root / "demographics"
        collection_dir.mkdir()
        vector_file = collection_dir / "boundaries.parquet"
        vector_file.write_bytes(b"PAR1" + b"\x00" * 100)  # Minimal parquet magic

        result = infer_nested_collection_id(vector_file, catalog_root)

        assert result == "demographics"

    @pytest.mark.unit
    def test_raster_file_uses_grandparent_as_collection(self, tmp_path: Path) -> None:
        """Raster files should use grandparent directory as collection.

        Structure: 2025/tile1/data.tif
        Expected: collection = "2025", item = "tile1"
        """
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
        """Nested raster structure should still use grandparent as collection.

        Structure: imagery/2025/tile1/scene.tif
        Expected: collection = "imagery/2025", item = "tile1"
        """
        catalog_root = tmp_path
        (catalog_root / "imagery" / "2025" / "tile1").mkdir(parents=True)
        raster_file = catalog_root / "imagery" / "2025" / "tile1" / "scene.tif"
        raster_file.write_bytes(b"II*\x00" + b"\x00" * 100)

        result = infer_nested_collection_id(raster_file, catalog_root)

        assert result == "imagery/2025"

    @pytest.mark.unit
    def test_nested_vector_uses_parent(self, tmp_path: Path) -> None:
        """Nested vector structure should use parent as collection.

        Structure: environment/air-quality/pm25.parquet
        Expected: collection = "environment/air-quality"
        """
        catalog_root = tmp_path
        (catalog_root / "environment" / "air-quality").mkdir(parents=True)
        vector_file = catalog_root / "environment" / "air-quality" / "pm25.parquet"
        vector_file.write_bytes(b"PAR1" + b"\x00" * 100)

        result = infer_nested_collection_id(vector_file, catalog_root)

        assert result == "environment/air-quality"

    @pytest.mark.unit
    def test_geojson_uses_parent_as_collection(self, tmp_path: Path) -> None:
        """GeoJSON (vector) should use parent as collection.

        Structure: boundaries/regions.geojson
        Expected: collection = "boundaries"
        """
        catalog_root = tmp_path
        collection_dir = catalog_root / "boundaries"
        collection_dir.mkdir()
        geojson_file = collection_dir / "regions.geojson"
        geojson_file.write_text('{"type": "FeatureCollection", "features": []}')

        result = infer_nested_collection_id(geojson_file, catalog_root)

        assert result == "boundaries"

    @pytest.mark.unit
    def test_cog_uses_grandparent_as_collection(self, tmp_path: Path) -> None:
        """COG (raster) should use grandparent as collection.

        Structure: satellite/scene-001/B04.tif
        Expected: collection = "satellite", item = "scene-001"
        """
        catalog_root = tmp_path
        (catalog_root / "satellite" / "scene-001").mkdir(parents=True)
        cog_file = catalog_root / "satellite" / "scene-001" / "B04.tif"
        cog_file.write_bytes(b"II*\x00" + b"\x00" * 100)

        result = infer_nested_collection_id(cog_file, catalog_root)

        assert result == "satellite"

    @pytest.mark.unit
    def test_raster_at_shallow_depth_raises_error(self, tmp_path: Path) -> None:
        """Raster file directly in collection (no item subdir) should raise.

        Structure: imagery/data.tif (no item subdirectory)
        Expected: ValueError (rasters need item subdirectory)
        """
        catalog_root = tmp_path
        collection_dir = catalog_root / "imagery"
        collection_dir.mkdir()
        raster_file = collection_dir / "data.tif"
        raster_file.write_bytes(b"II*\x00" + b"\x00" * 100)

        with pytest.raises(ValueError, match="must be in a subdirectory"):
            infer_nested_collection_id(raster_file, catalog_root)

    @pytest.mark.unit
    def test_shapefile_uses_parent_as_collection(self, tmp_path: Path) -> None:
        """Shapefile (vector) should use parent as collection.

        Structure: parcels/boundaries.shp
        Expected: collection = "parcels"
        """
        catalog_root = tmp_path
        collection_dir = catalog_root / "parcels"
        collection_dir.mkdir()
        shp_file = collection_dir / "boundaries.shp"
        shp_file.write_bytes(b"\x00" * 100)  # Minimal content

        result = infer_nested_collection_id(shp_file, catalog_root)

        assert result == "parcels"

    @pytest.mark.unit
    def test_geopackage_uses_parent_as_collection(self, tmp_path: Path) -> None:
        """GeoPackage (vector) should use parent as collection.

        Structure: transportation/roads.gpkg
        Expected: collection = "transportation"
        """
        catalog_root = tmp_path
        collection_dir = catalog_root / "transportation"
        collection_dir.mkdir()
        gpkg_file = collection_dir / "roads.gpkg"
        gpkg_file.write_bytes(b"SQLite format 3\x00" + b"\x00" * 100)

        result = infer_nested_collection_id(gpkg_file, catalog_root)

        assert result == "transportation"
