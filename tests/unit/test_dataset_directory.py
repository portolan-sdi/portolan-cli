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
        """Create an initialized Portolan catalog."""
        portolan_dir = tmp_path / "catalog" / ".portolan"
        portolan_dir.mkdir(parents=True)
        catalog_data = {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "test",
            "description": "Test catalog",
            "links": [],
        }
        (portolan_dir / "catalog.json").write_text(json.dumps(catalog_data))
        return tmp_path / "catalog"

    @pytest.mark.unit
    def test_add_directory_single_file(self, initialized_catalog: Path, tmp_path: Path) -> None:
        """add_directory processes single file in directory."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "only.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        # Create output file
        output_dir = initialized_catalog / ".portolan" / "collections" / "col" / "only"
        output_dir.mkdir(parents=True)
        (output_dir / "only.parquet").write_bytes(b"fake")

        with (
            patch("portolan_cli.dataset.detect_format") as mock_detect,
            patch("portolan_cli.dataset.convert_vector") as mock_convert,
            patch("portolan_cli.dataset.extract_geoparquet_metadata") as mock_metadata,
            patch("portolan_cli.dataset.compute_checksum") as mock_checksum,
        ):
            mock_detect.return_value = FormatType.VECTOR
            mock_convert.return_value = output_dir / "only.parquet"
            mock_metadata.return_value = MagicMock(
                bbox=(0, 0, 1, 1),
                crs="EPSG:4326",
                feature_count=0,
                geometry_type="Point",
                to_stac_properties=lambda: {},
            )
            mock_checksum.return_value = "abc"

            results = add_directory(
                path=data_dir,
                catalog_root=initialized_catalog,
                collection_id="col",
            )

            assert len(results) == 1
            assert results[0].item_id == "only"

    @pytest.mark.unit
    def test_add_directory_multiple_files(self, initialized_catalog: Path, tmp_path: Path) -> None:
        """add_directory processes all geospatial files."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "a.geojson").write_text('{"type": "FeatureCollection", "features": []}')
        (data_dir / "b.tif").write_bytes(b"fake tiff")

        # Create output files
        for name in ["a", "b"]:
            output_dir = initialized_catalog / ".portolan" / "collections" / "col" / name
            output_dir.mkdir(parents=True)
            ext = "parquet" if name == "a" else "tif"
            (output_dir / f"{name}.{ext}").write_bytes(b"fake")

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
            mock_convert_v.return_value = (
                initialized_catalog / ".portolan" / "collections" / "col" / "a" / "a.parquet"
            )
            mock_convert_r.return_value = (
                initialized_catalog / ".portolan" / "collections" / "col" / "b" / "b.tif"
            )
            mock_meta_v.return_value = MagicMock(
                bbox=(0, 0, 1, 1),
                crs="EPSG:4326",
                feature_count=0,
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
                path=data_dir,
                catalog_root=initialized_catalog,
                collection_id="col",
            )

            assert len(results) == 2
            item_ids = {r.item_id for r in results}
            assert item_ids == {"a", "b"}

    @pytest.mark.unit
    def test_add_directory_recursive(self, initialized_catalog: Path, tmp_path: Path) -> None:
        """add_directory finds files recursively."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        sub = data_dir / "nested"
        sub.mkdir()
        (data_dir / "top.geojson").write_text('{"type": "FeatureCollection", "features": []}')
        (sub / "deep.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        # Create output files
        for name in ["top", "deep"]:
            output_dir = initialized_catalog / ".portolan" / "collections" / "col" / name
            output_dir.mkdir(parents=True)
            (output_dir / f"{name}.parquet").write_bytes(b"fake")

        with (
            patch("portolan_cli.dataset.detect_format") as mock_detect,
            patch("portolan_cli.dataset.convert_vector") as mock_convert,
            patch("portolan_cli.dataset.extract_geoparquet_metadata") as mock_metadata,
            patch("portolan_cli.dataset.compute_checksum") as mock_checksum,
        ):
            mock_detect.return_value = FormatType.VECTOR

            def convert_side_effect(source: Path, dest: Path) -> Path:
                return dest / f"{source.stem}.parquet"

            mock_convert.side_effect = convert_side_effect
            mock_metadata.return_value = MagicMock(
                bbox=(0, 0, 1, 1),
                crs="EPSG:4326",
                feature_count=0,
                geometry_type="Point",
                to_stac_properties=lambda: {},
            )
            mock_checksum.return_value = "abc"

            results = add_directory(
                path=data_dir,
                catalog_root=initialized_catalog,
                collection_id="col",
                recursive=True,
            )

            assert len(results) == 2

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
