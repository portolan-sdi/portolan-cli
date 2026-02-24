"""Integration tests for dataset orchestration with real file conversions.

These tests exercise the full add_dataset workflow with real fixtures,
verifying that format conversion, metadata extraction, and STAC creation
work end-to-end.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.dataset import (
    add_dataset,
    compute_checksum,
    convert_raster,
    convert_vector,
    get_dataset_info,
    list_datasets,
)
from portolan_cli.formats import FormatType


@pytest.fixture
def initialized_catalog(tmp_path: Path) -> Path:
    """Create an initialized Portolan catalog structure."""
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()

    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "portolan-catalog",
        "description": "A Portolan-managed STAC catalog",
        "links": [],
    }
    (portolan_dir / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    return tmp_path


class TestConvertVector:
    """Integration tests for convert_vector function."""

    @pytest.mark.integration
    def test_convert_geojson_to_geoparquet(
        self, valid_points_geojson: Path, tmp_path: Path
    ) -> None:
        """convert_vector converts GeoJSON to GeoParquet."""
        dest_dir = tmp_path / "output"
        dest_dir.mkdir()

        result = convert_vector(valid_points_geojson, dest_dir)

        assert result.exists()
        assert result.suffix == ".parquet"
        assert result.stat().st_size > 0

    @pytest.mark.integration
    def test_convert_preserves_parquet(self, valid_points_parquet: Path, tmp_path: Path) -> None:
        """convert_vector copies existing GeoParquet without conversion."""
        dest_dir = tmp_path / "output"
        dest_dir.mkdir()

        result = convert_vector(valid_points_parquet, dest_dir)

        assert result.exists()
        # Should be roughly the same size (copy, not conversion)
        assert abs(result.stat().st_size - valid_points_parquet.stat().st_size) < 100

    @pytest.mark.integration
    def test_convert_polygons(self, valid_polygons_geojson: Path, tmp_path: Path) -> None:
        """convert_vector handles polygon geometries."""
        dest_dir = tmp_path / "output"
        dest_dir.mkdir()

        result = convert_vector(valid_polygons_geojson, dest_dir)

        assert result.exists()
        assert result.suffix == ".parquet"

    @pytest.mark.integration
    def test_convert_lines(self, valid_lines_geojson: Path, tmp_path: Path) -> None:
        """convert_vector handles line geometries."""
        dest_dir = tmp_path / "output"
        dest_dir.mkdir()

        result = convert_vector(valid_lines_geojson, dest_dir)

        assert result.exists()


class TestConvertRaster:
    """Integration tests for convert_raster function."""

    @pytest.mark.integration
    def test_convert_tiff_to_cog(self, valid_rgb_cog: Path, tmp_path: Path) -> None:
        """convert_raster produces valid COG output."""
        from rio_cogeo.cogeo import cog_validate

        dest_dir = tmp_path / "output"
        dest_dir.mkdir()

        result = convert_raster(valid_rgb_cog, dest_dir)

        assert result.exists()
        assert result.suffix == ".tif"

        # Verify it's a valid COG
        is_valid, errors, _ = cog_validate(str(result))
        assert is_valid, f"Output is not a valid COG: {errors}"

    @pytest.mark.integration
    def test_convert_singleband(self, valid_singleband_cog: Path, tmp_path: Path) -> None:
        """convert_raster handles single-band rasters."""
        dest_dir = tmp_path / "output"
        dest_dir.mkdir()

        result = convert_raster(valid_singleband_cog, dest_dir)

        assert result.exists()

    @pytest.mark.integration
    def test_convert_float32(self, valid_float32_cog: Path, tmp_path: Path) -> None:
        """convert_raster handles float32 data type."""
        dest_dir = tmp_path / "output"
        dest_dir.mkdir()

        result = convert_raster(valid_float32_cog, dest_dir)

        assert result.exists()


class TestComputeChecksum:
    """Integration tests for checksum computation."""

    @pytest.mark.integration
    def test_checksum_is_deterministic(self, valid_points_geojson: Path) -> None:
        """compute_checksum returns same value for same file."""
        checksum1 = compute_checksum(valid_points_geojson)
        checksum2 = compute_checksum(valid_points_geojson)

        assert checksum1 == checksum2
        assert len(checksum1) == 64  # SHA-256 produces 64 hex chars

    @pytest.mark.integration
    def test_checksum_differs_for_different_files(
        self, valid_points_geojson: Path, valid_polygons_geojson: Path
    ) -> None:
        """compute_checksum returns different values for different files."""
        checksum1 = compute_checksum(valid_points_geojson)
        checksum2 = compute_checksum(valid_polygons_geojson)

        assert checksum1 != checksum2

    @pytest.mark.integration
    def test_checksum_rejects_symlink_to_directory(self, tmp_path: Path) -> None:
        """compute_checksum rejects symlinks pointing to directories (MAJOR #5)."""
        target_dir = tmp_path / "target_dir"
        target_dir.mkdir()
        symlink = tmp_path / "symlink_to_dir"
        symlink.symlink_to(target_dir)

        with pytest.raises(ValueError, match="Not a regular file"):
            compute_checksum(symlink)

    @pytest.mark.integration
    def test_checksum_follows_symlink_to_file(self, tmp_path: Path) -> None:
        """compute_checksum follows symlinks to regular files (valid case)."""
        target_file = tmp_path / "real_file.txt"
        target_file.write_text("test content")
        symlink = tmp_path / "symlink_to_file"
        symlink.symlink_to(target_file)

        # Should work - symlinks to files are valid
        checksum = compute_checksum(symlink)
        assert len(checksum) == 64

    @pytest.mark.integration
    def test_checksum_rejects_nonexistent_file(self, tmp_path: Path) -> None:
        """compute_checksum raises FileNotFoundError for missing files."""
        nonexistent = tmp_path / "does_not_exist.txt"

        with pytest.raises(FileNotFoundError):
            compute_checksum(nonexistent)


class TestAddDatasetIntegration:
    """Integration tests for full add_dataset workflow."""

    @pytest.mark.integration
    def test_add_vector_dataset_end_to_end(
        self, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """add_dataset converts GeoJSON and creates STAC item."""
        result = add_dataset(
            path=valid_points_geojson,
            catalog_root=initialized_catalog,
            collection_id="test-vectors",
            title="Test Points",
        )

        assert result.collection_id == "test-vectors"
        assert result.format_type == FormatType.VECTOR
        assert result.title == "Test Points"
        assert len(result.bbox) == 4
        # Bbox should be valid (not Null Island)
        assert result.bbox != [0, 0, 0, 0]

        # Verify STAC structure was created
        collection_dir = initialized_catalog / ".portolan" / "collections" / "test-vectors"
        assert collection_dir.exists()
        assert (collection_dir / "collection.json").exists()
        assert (collection_dir / "versions.json").exists()

        # Verify the converted file exists
        item_dir = collection_dir / valid_points_geojson.stem
        assert item_dir.exists()
        assert (item_dir / f"{valid_points_geojson.stem}.parquet").exists()

    @pytest.mark.integration
    def test_add_raster_dataset_end_to_end(
        self, initialized_catalog: Path, valid_rgb_cog: Path
    ) -> None:
        """add_dataset converts raster and creates STAC item."""
        result = add_dataset(
            path=valid_rgb_cog,
            catalog_root=initialized_catalog,
            collection_id="imagery",
        )

        assert result.collection_id == "imagery"
        assert result.format_type == FormatType.RASTER
        assert len(result.bbox) == 4

        # Verify STAC structure
        collection_dir = initialized_catalog / ".portolan" / "collections" / "imagery"
        assert (collection_dir / "collection.json").exists()

    @pytest.mark.integration
    def test_add_multiple_datasets_same_collection(
        self, initialized_catalog: Path, valid_points_geojson: Path, valid_polygons_geojson: Path
    ) -> None:
        """Multiple datasets can be added to the same collection."""
        add_dataset(
            path=valid_points_geojson,
            catalog_root=initialized_catalog,
            collection_id="vectors",
        )
        add_dataset(
            path=valid_polygons_geojson,
            catalog_root=initialized_catalog,
            collection_id="vectors",
        )

        datasets = list_datasets(initialized_catalog, collection_id="vectors")
        assert len(datasets) == 2

    @pytest.mark.integration
    def test_add_and_retrieve_dataset_info(
        self, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """get_dataset_info returns correct info for added dataset."""
        add_result = add_dataset(
            path=valid_points_geojson,
            catalog_root=initialized_catalog,
            collection_id="test-col",
        )

        info = get_dataset_info(initialized_catalog, f"test-col/{add_result.item_id}")

        assert info.item_id == add_result.item_id
        assert info.collection_id == "test-col"
        assert info.bbox == add_result.bbox
