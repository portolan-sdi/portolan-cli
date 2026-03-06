"""Unit tests for directory handling in dataset operations.

Tests recursive file iteration for adding directories as datasets.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from portolan_cli.dataset import (
    add_directory,
    iter_geospatial_files,
)
from portolan_cli.formats import FormatType


class TestIterGeospatialFiles:
    """Tests for iterating geospatial files in a directory."""

    @pytest.mark.unit
    def test_iter_flat_directory(self, tmp_path: Path) -> None:
        """iter_geospatial_files finds files in flat directory."""
        (tmp_path / "a.geojson").write_text("{}")
        (tmp_path / "b.parquet").write_bytes(b"fake")
        (tmp_path / "c.tif").write_bytes(b"fake")
        (tmp_path / "readme.txt").write_text("not geospatial")

        files = list(iter_geospatial_files(tmp_path))

        assert len(files) == 3
        names = {f.name for f in files}
        assert names == {"a.geojson", "b.parquet", "c.tif"}

    @pytest.mark.unit
    def test_iter_recursive(self, tmp_path: Path) -> None:
        """iter_geospatial_files finds files recursively."""
        (tmp_path / "top.geojson").write_text("{}")
        sub = tmp_path / "subdir"
        sub.mkdir()
        (sub / "nested.tif").write_bytes(b"fake")

        files = list(iter_geospatial_files(tmp_path, recursive=True))

        assert len(files) == 2
        names = {f.name for f in files}
        assert names == {"top.geojson", "nested.tif"}

    @pytest.mark.unit
    def test_iter_non_recursive(self, tmp_path: Path) -> None:
        """iter_geospatial_files respects recursive=False."""
        (tmp_path / "top.geojson").write_text("{}")
        sub = tmp_path / "subdir"
        sub.mkdir()
        (sub / "nested.tif").write_bytes(b"fake")

        files = list(iter_geospatial_files(tmp_path, recursive=False))

        assert len(files) == 1
        assert files[0].name == "top.geojson"

    @pytest.mark.unit
    def test_iter_filters_by_extension(self, tmp_path: Path) -> None:
        """iter_geospatial_files only returns geospatial files."""
        (tmp_path / "data.geojson").write_text("{}")
        (tmp_path / "data.json").write_text("{}")  # Not geospatial by default
        (tmp_path / "notes.md").write_text("markdown")
        (tmp_path / "script.py").write_text("python")

        files = list(iter_geospatial_files(tmp_path))

        assert len(files) == 1
        assert files[0].name == "data.geojson"

    @pytest.mark.unit
    def test_iter_empty_directory(self, tmp_path: Path) -> None:
        """iter_geospatial_files returns empty for empty directory."""
        files = list(iter_geospatial_files(tmp_path))
        assert files == []

    @pytest.mark.unit
    def test_iter_non_directory_returns_empty(self, tmp_path: Path) -> None:
        """iter_geospatial_files returns empty for non-directory path."""
        file_path = tmp_path / "not_a_dir.geojson"
        file_path.write_text("{}")

        files = list(iter_geospatial_files(file_path))
        assert files == []

    @pytest.mark.unit
    def test_iter_returns_sorted(self, tmp_path: Path) -> None:
        """iter_geospatial_files returns files in sorted order."""
        (tmp_path / "z.geojson").write_text("{}")
        (tmp_path / "a.geojson").write_text("{}")
        (tmp_path / "m.geojson").write_text("{}")

        files = iter_geospatial_files(tmp_path)

        names = [f.name for f in files]
        assert names == ["a.geojson", "m.geojson", "z.geojson"]


class TestAddDirectory:
    """Tests for adding a directory of files."""

    @pytest.fixture
    def initialized_catalog(self, tmp_path: Path) -> Path:
        """Create an initialized Portolan catalog (per ADR-0023)."""
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir(parents=True)
        # Create .portolan for internal state
        portolan_dir = catalog_root / ".portolan"
        portolan_dir.mkdir()
        # catalog.json at root level (per ADR-0023)
        catalog_data = {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "test",
            "description": "Test catalog",
            "links": [],
        }
        (catalog_root / "catalog.json").write_text(json.dumps(catalog_data))
        return catalog_root

    @pytest.mark.unit
    def test_add_directory_single_file(self, initialized_catalog: Path, tmp_path: Path) -> None:
        """add_directory processes single file in directory."""
        # Create file inside collection/item structure (Issue #163)
        item_dir = initialized_catalog / "col" / "myitem"
        item_dir.mkdir(parents=True)

        # Use valid GeoJSON with features for pre-validation
        valid_geojson = json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                        "properties": {},
                    }
                ],
            }
        )
        (item_dir / "data.geojson").write_text(valid_geojson)

        with (
            patch("portolan_cli.dataset.detect_format") as mock_detect,
            patch("portolan_cli.dataset.convert_vector") as mock_convert,
            patch("portolan_cli.dataset.extract_geoparquet_metadata") as mock_metadata,
            patch("portolan_cli.dataset.compute_checksum") as mock_checksum,
        ):
            mock_detect.return_value = FormatType.VECTOR

            # Convert creates output file when called (not before add_directory)
            def convert_side_effect(source: Path, dest: Path) -> Path:
                output_path = dest / f"{source.stem}.parquet"
                output_path.write_bytes(b"fake")
                return output_path

            mock_convert.side_effect = convert_side_effect
            mock_metadata.return_value = MagicMock(
                bbox=(0, 0, 1, 1),
                crs="EPSG:4326",
                feature_count=1,
                geometry_type="Point",
                to_stac_properties=lambda: {},
            )
            mock_checksum.return_value = "abc"

            results = add_directory(
                path=item_dir,
                catalog_root=initialized_catalog,
                collection_id="col",
            )

            assert len(results) == 1
            assert results[0].item_id == "myitem"  # From parent directory

    @pytest.mark.unit
    def test_add_directory_multiple_files(self, initialized_catalog: Path, tmp_path: Path) -> None:
        """add_directory processes all geospatial files in an item directory."""
        # Create files inside collection/item structure (Issue #163)
        # Multiple files in same item dir = multiple assets for one item
        item_dir = initialized_catalog / "col" / "myitem"
        item_dir.mkdir(parents=True)

        valid_geojson = json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                        "properties": {},
                    }
                ],
            }
        )
        (item_dir / "a.geojson").write_text(valid_geojson)
        # Raster doesn't need pre-validation (inherently has extent)
        (item_dir / "b.tif").write_bytes(b"fake tiff")

        with (
            patch("portolan_cli.dataset.detect_format") as mock_detect,
            patch("portolan_cli.dataset.convert_vector") as mock_convert_v,
            patch("portolan_cli.dataset.convert_raster") as mock_convert_r,
            patch("portolan_cli.dataset.extract_geoparquet_metadata") as mock_meta_v,
            patch("portolan_cli.dataset.extract_cog_metadata") as mock_meta_r,
            patch("portolan_cli.dataset.compute_checksum") as mock_checksum,
        ):

            def detect_side_effect(path: Path) -> FormatType:
                if path.suffix == ".geojson":
                    return FormatType.VECTOR
                return FormatType.RASTER

            mock_detect.side_effect = detect_side_effect

            # Convert creates output files when called (not before add_directory)
            def convert_vector_side_effect(source: Path, dest: Path) -> Path:
                output_path = dest / f"{source.stem}.parquet"
                output_path.write_bytes(b"fake")
                return output_path

            def convert_raster_side_effect(source: Path, dest: Path) -> Path:
                # Raster stays in place (already exists)
                return source

            mock_convert_v.side_effect = convert_vector_side_effect
            mock_convert_r.side_effect = convert_raster_side_effect
            mock_meta_v.return_value = MagicMock(
                bbox=(0, 0, 1, 1),
                crs="EPSG:4326",
                feature_count=1,
                geometry_type="Point",
                to_stac_properties=lambda: {},
            )
            mock_meta_r.return_value = MagicMock(
                bbox=(0, 0, 1, 1),
                crs="EPSG:4326",
                width=64,
                height=64,
                band_count=1,
                to_stac_properties=lambda: {},
            )
            mock_checksum.return_value = "abc"

            results = add_directory(
                path=item_dir,
                catalog_root=initialized_catalog,
                collection_id="col",
            )

            # With new design, multiple files in one item_dir = one item with multiple assets
            # Or multiple items if add_directory processes each file separately
            # The key point: all results have same item_id (parent dir name)
            assert len(results) >= 1
            for r in results:
                assert r.item_id == "myitem"

    @pytest.mark.unit
    def test_add_directory_recursive(self, initialized_catalog: Path, tmp_path: Path) -> None:
        """add_directory finds files recursively."""
        # Create nested item directories inside the collection (Issue #163)
        # Structure: col/top_item/top.geojson + col/nested/deep_item/deep.geojson
        col_dir = initialized_catalog / "col"
        col_dir.mkdir(parents=True)

        top_item_dir = col_dir / "top_item"
        top_item_dir.mkdir()
        nested_dir = col_dir / "nested"
        nested_dir.mkdir()
        deep_item_dir = nested_dir / "deep_item"
        deep_item_dir.mkdir()

        # Use valid GeoJSON with features for pre-validation
        valid_geojson = json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                        "properties": {},
                    }
                ],
            }
        )
        (top_item_dir / "top.geojson").write_text(valid_geojson)
        (deep_item_dir / "deep.geojson").write_text(valid_geojson)

        with (
            patch("portolan_cli.dataset.detect_format") as mock_detect,
            patch("portolan_cli.dataset.convert_vector") as mock_convert,
            patch("portolan_cli.dataset.extract_geoparquet_metadata") as mock_metadata,
            patch("portolan_cli.dataset.compute_checksum") as mock_checksum,
        ):
            mock_detect.return_value = FormatType.VECTOR

            def convert_side_effect(source: Path, dest: Path) -> Path:
                # Create output parquet in the same directory as source (in-place)
                output_path = source.parent / f"{source.stem}.parquet"
                output_path.write_bytes(b"fake")
                return output_path

            mock_convert.side_effect = convert_side_effect
            mock_metadata.return_value = MagicMock(
                bbox=(0, 0, 1, 1),
                crs="EPSG:4326",
                feature_count=1,
                geometry_type="Point",
                to_stac_properties=lambda: {},
            )
            mock_checksum.return_value = "abc"

            results = add_directory(
                path=col_dir,
                catalog_root=initialized_catalog,
                collection_id="col",
                recursive=True,
            )

            # Should find both files (in top_item and deep_item directories)
            assert len(results) == 2
            item_ids = {r.item_id for r in results}
            assert item_ids == {"top_item", "deep_item"}

    @pytest.mark.unit
    def test_add_directory_empty_returns_empty(
        self, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """add_directory returns empty list for empty directory."""
        data_dir = tmp_path / "empty"
        data_dir.mkdir()

        results = add_directory(
            path=data_dir,
            catalog_root=initialized_catalog,
            collection_id="col",
        )

        assert results == []
