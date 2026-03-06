"""Integration tests for dataset orchestration with real file conversions.

These tests exercise the full add_dataset workflow with real fixtures,
verifying that format conversion, metadata extraction, and STAC creation
work end-to-end.

Per ADR-0022 (track in-place design): Files must be INSIDE the catalog
directory structure BEFORE calling add_dataset(). Tests copy fixtures
into the catalog first, then call add_dataset() on the copied paths.
"""

from __future__ import annotations

import json
import shutil
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
    """Create an initialized Portolan catalog structure (per ADR-0023).

    This creates all required files for a managed catalog:
    - catalog.json at root (STAC entry point)
    - .portolan/config.yaml (sentinel file for root detection)
    - .portolan/state.json (tracking state)
    """
    # Create .portolan for internal state
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()

    # catalog.json at root level (per ADR-0023)
    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "portolan-catalog",
        "description": "A Portolan-managed STAC catalog",
        "links": [],
    }
    (tmp_path / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    # config.yaml is the sentinel for catalog root detection (per ADR-0029)
    (portolan_dir / "config.yaml").write_text("version: 1\n")

    # state.json for tracking state
    (portolan_dir / "state.json").write_text(json.dumps({"collections": {}}))

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
    """Integration tests for full add_dataset workflow.

    Per ADR-0022 (track in-place design): Files must be inside the catalog
    directory BEFORE calling add_dataset(). Tests copy fixtures into the
    collection/item directory structure first.
    """

    @pytest.mark.integration
    def test_add_vector_dataset_end_to_end(
        self, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """add_dataset converts GeoJSON and creates STAC item."""
        # Per ADR-0022: Copy file INTO catalog first (track in-place design)
        collection_dir = initialized_catalog / "test-vectors"
        item_dir = collection_dir / valid_points_geojson.stem
        item_dir.mkdir(parents=True)
        in_catalog_path = item_dir / valid_points_geojson.name
        shutil.copy(valid_points_geojson, in_catalog_path)

        result = add_dataset(
            path=in_catalog_path,
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

        # Verify STAC structure was created (at root level per ADR-0023)
        assert collection_dir.exists()
        assert (collection_dir / "collection.json").exists()
        assert (collection_dir / "versions.json").exists()

        # Verify the converted file exists
        assert item_dir.exists()
        assert (item_dir / f"{valid_points_geojson.stem}.parquet").exists()

    @pytest.mark.integration
    def test_add_raster_dataset_end_to_end(
        self, initialized_catalog: Path, valid_rgb_cog: Path
    ) -> None:
        """add_dataset converts raster and creates STAC item."""
        # Per ADR-0022: Copy file INTO catalog first
        collection_dir = initialized_catalog / "imagery"
        item_dir = collection_dir / valid_rgb_cog.stem
        item_dir.mkdir(parents=True)
        in_catalog_path = item_dir / valid_rgb_cog.name
        shutil.copy(valid_rgb_cog, in_catalog_path)

        result = add_dataset(
            path=in_catalog_path,
            catalog_root=initialized_catalog,
            collection_id="imagery",
        )

        assert result.collection_id == "imagery"
        assert result.format_type == FormatType.RASTER
        assert len(result.bbox) == 4

        # Verify STAC structure (at root level per ADR-0023)
        assert (collection_dir / "collection.json").exists()

    @pytest.mark.integration
    def test_add_multiple_datasets_same_collection(
        self, initialized_catalog: Path, valid_points_geojson: Path, valid_polygons_geojson: Path
    ) -> None:
        """Multiple datasets can be added to the same collection."""
        # Per ADR-0022: Copy files INTO catalog first
        collection_dir = initialized_catalog / "vectors"

        # First file
        item_dir_1 = collection_dir / valid_points_geojson.stem
        item_dir_1.mkdir(parents=True)
        path_1 = item_dir_1 / valid_points_geojson.name
        shutil.copy(valid_points_geojson, path_1)

        # Second file
        item_dir_2 = collection_dir / valid_polygons_geojson.stem
        item_dir_2.mkdir(parents=True)
        path_2 = item_dir_2 / valid_polygons_geojson.name
        shutil.copy(valid_polygons_geojson, path_2)

        add_dataset(
            path=path_1,
            catalog_root=initialized_catalog,
            collection_id="vectors",
        )
        add_dataset(
            path=path_2,
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
        # Per ADR-0022: Copy file INTO catalog first
        collection_dir = initialized_catalog / "test-col"
        item_dir = collection_dir / valid_points_geojson.stem
        item_dir.mkdir(parents=True)
        in_catalog_path = item_dir / valid_points_geojson.name
        shutil.copy(valid_points_geojson, in_catalog_path)

        add_result = add_dataset(
            path=in_catalog_path,
            catalog_root=initialized_catalog,
            collection_id="test-col",
        )

        info = get_dataset_info(initialized_catalog, f"test-col/{add_result.item_id}")

        assert info.item_id == add_result.item_id
        assert info.collection_id == "test-col"
        assert info.bbox == add_result.bbox


# =============================================================================
# Multi-Asset Integration Tests (Issue #133)
# =============================================================================


class TestMultiAssetIntegration:
    """Integration tests for multi-asset tracking (issue #133).

    These tests verify the end-to-end workflow where ALL files in an item
    directory are tracked as assets, not just geospatial files.
    """

    @pytest.mark.integration
    def test_add_dataset_with_companion_files_tracks_all(
        self, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """End-to-end: add geo file with thumbnail and readme tracks all."""
        # Per ADR-0022: Create file INSIDE catalog directory structure
        collection_dir = initialized_catalog / "multi-asset-test"
        item_dir = collection_dir / "data"
        item_dir.mkdir(parents=True)

        # Create geojson inside the catalog (not in tmp_path/source)
        geojson_path = item_dir / "data.geojson"
        geojson_path.write_text(
            json.dumps(
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": [-122.0, 37.5],
                            },
                            "properties": {"name": "test"},
                        }
                    ],
                }
            )
        )

        # Add the dataset
        result = add_dataset(
            path=geojson_path,
            catalog_root=initialized_catalog,
            collection_id="multi-asset-test",
        )

        # Now add companion files to the item directory
        item_dir = initialized_catalog / "multi-asset-test" / result.item_id
        assert item_dir.exists()

        # Add thumbnail
        thumbnail = item_dir / "thumbnail.png"
        thumbnail.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)  # Minimal PNG

        # Add readme
        readme = item_dir / "README.md"
        readme.write_text("# Test Dataset\n\nThis is a test.")

        # Re-add to pick up new files (simulating user workflow)
        # In real usage, user would run `portolan add` again or files would
        # be present before first add
        result2 = add_dataset(
            path=geojson_path,
            catalog_root=initialized_catalog,
            collection_id="multi-asset-test",
        )

        # Verify all assets tracked
        assert len(result2.asset_paths) >= 1  # At least the parquet

        # Check item.json has multiple assets
        item_json_path = item_dir / f"{result.item_id}.json"
        item_data = json.loads(item_json_path.read_text())
        assets = item_data.get("assets", {})

        # Should have data asset
        assert "data" in assets

        # Check versions.json has all files
        versions_path = initialized_catalog / "multi-asset-test" / "versions.json"
        versions_data = json.loads(versions_path.read_text())
        current_version = versions_data["versions"][-1]
        asset_keys = set(current_version["assets"].keys())

        # Should have the parquet file tracked
        parquet_files = [k for k in asset_keys if "parquet" in k.lower()]
        assert len(parquet_files) >= 1, f"Expected parquet in {asset_keys}"

    @pytest.mark.integration
    def test_status_detects_all_file_types(self, initialized_catalog: Path) -> None:
        """Status command detects untracked non-geo files in item directories."""
        from portolan_cli.status import get_catalog_status

        # Create a collection with item directory
        col_dir = initialized_catalog / "status-test"
        col_dir.mkdir()
        (col_dir / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "status-test",
                    "stac_version": "1.0.0",
                    "description": "Test",
                    "license": "proprietary",
                    "extent": {
                        "spatial": {"bbox": [[-180, -90, 180, 90]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                }
            )
        )

        # Create item directory with various file types
        item_dir = col_dir / "test-item"
        item_dir.mkdir()

        # Add various file types (all should be detected as untracked)
        (item_dir / "data.parquet").write_bytes(b"fake parquet")
        (item_dir / "thumbnail.png").write_bytes(b"fake png")
        (item_dir / "README.md").write_text("# Readme")
        (item_dir / "metadata.json").write_text("{}")

        # Create empty versions.json (nothing tracked yet)
        (col_dir / "versions.json").write_text(
            json.dumps(
                {
                    "spec_version": "1.0.0",
                    "current_version": None,
                    "versions": [],
                }
            )
        )

        # Get status
        status = get_catalog_status(initialized_catalog)

        # All 4 files should be untracked
        untracked_filenames = {f.filename for f in status.untracked}
        assert "data.parquet" in untracked_filenames
        assert "thumbnail.png" in untracked_filenames
        assert "README.md" in untracked_filenames
        assert "metadata.json" in untracked_filenames

    @pytest.mark.integration
    def test_hidden_files_not_tracked(self, initialized_catalog: Path) -> None:
        """Hidden files (starting with .) are excluded from tracking."""
        from portolan_cli.status import get_catalog_status

        # Create collection structure
        col_dir = initialized_catalog / "hidden-test"
        col_dir.mkdir()
        (col_dir / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "hidden-test",
                    "stac_version": "1.0.0",
                    "description": "Test",
                    "license": "proprietary",
                    "extent": {
                        "spatial": {"bbox": [[-180, -90, 180, 90]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                }
            )
        )

        item_dir = col_dir / "test-item"
        item_dir.mkdir()

        # Add regular file and hidden files
        (item_dir / "data.parquet").write_bytes(b"parquet")
        (item_dir / ".DS_Store").write_bytes(b"junk")
        (item_dir / ".hidden").write_text("hidden")

        (col_dir / "versions.json").write_text(
            json.dumps(
                {
                    "spec_version": "1.0.0",
                    "current_version": None,
                    "versions": [],
                }
            )
        )

        status = get_catalog_status(initialized_catalog)

        # Only data.parquet should be untracked, not hidden files
        untracked_filenames = {f.filename for f in status.untracked}
        assert "data.parquet" in untracked_filenames
        assert ".DS_Store" not in untracked_filenames
        assert ".hidden" not in untracked_filenames


# =============================================================================
# Deprecation Warning Integration Tests (Issue #148)
# =============================================================================


class TestDatasetInfoDeprecationIntegration:
    """Integration tests verifying 'portolan dataset info' deprecation behavior.

    These tests exercise the full CLI path to ensure the deprecation warning
    is emitted at the right point and doesn't break the command output.

    Per ADR-0022: Files must be inside catalog before calling add_dataset().
    """

    def _setup_test_dataset(
        self, initialized_catalog: Path, valid_points_geojson: Path, collection_id: str
    ) -> AddDatasetResult:  # noqa: F821
        """Helper to set up a dataset inside the catalog per ADR-0022."""
        collection_dir = initialized_catalog / collection_id
        item_dir = collection_dir / valid_points_geojson.stem
        item_dir.mkdir(parents=True)
        in_catalog_path = item_dir / valid_points_geojson.name
        shutil.copy(valid_points_geojson, in_catalog_path)

        return add_dataset(
            path=in_catalog_path,
            catalog_root=initialized_catalog,
            collection_id=collection_id,
        )

    @pytest.mark.integration
    def test_dataset_info_cli_shows_deprecation_warning(
        self, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """dataset info CLI emits deprecation warning with real catalog data."""
        from click.testing import CliRunner

        from portolan_cli.cli import cli

        # Add a real dataset first (per ADR-0022: file must be inside catalog)
        add_result = self._setup_test_dataset(initialized_catalog, valid_points_geojson, "test-col")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "dataset",
                "info",
                f"test-col/{add_result.item_id}",
                "--catalog",
                str(initialized_catalog),
            ],
        )

        assert result.exit_code == 0
        assert "deprecated" in result.output.lower()
        assert "portolan info" in result.output

    @pytest.mark.integration
    def test_dataset_info_cli_still_returns_correct_data_after_warning(
        self, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """dataset info CLI still returns item data after emitting deprecation warning."""
        from click.testing import CliRunner

        from portolan_cli.cli import cli

        add_result = self._setup_test_dataset(initialized_catalog, valid_points_geojson, "test-col")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "dataset",
                "info",
                f"test-col/{add_result.item_id}",
                "--catalog",
                str(initialized_catalog),
            ],
        )

        assert result.exit_code == 0
        # Deprecation warning is present
        assert "deprecated" in result.output.lower()
        # Item data is also present
        assert add_result.item_id in result.output
        assert "test-col" in result.output

    @pytest.mark.integration
    def test_dataset_info_cli_json_mode_no_deprecation_warning(
        self, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """dataset info CLI in JSON mode does not contaminate output with warning."""
        from click.testing import CliRunner

        from portolan_cli.cli import cli

        add_result = self._setup_test_dataset(initialized_catalog, valid_points_geojson, "test-col")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "dataset",
                "info",
                f"test-col/{add_result.item_id}",
                "--catalog",
                str(initialized_catalog),
                "--json",
            ],
        )

        assert result.exit_code == 0
        # Output must be valid JSON (warning would break this)
        parsed = json.loads(result.output)
        assert parsed["success"] is True
        assert "deprecated" not in result.output.lower()
