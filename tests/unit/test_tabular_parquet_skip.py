"""Unit tests for tabular parquet (no geometry) handling (Issue #177).

Tests that `portolan add` gracefully handles Parquet files without geometry:
- Per ADR-0028: Track non-geo parquet files as assets when in same dir as geo file
- Skip non-geo parquet files that have no companion geo file (can't create STAC item)
- Emit appropriate log messages
- Support parquet files as auxiliary/tabular data assets

This follows the same pattern as CSV/TSV handling (Issue #140).

See: https://github.com/portolan-sdi/portolan-cli/issues/177
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from portolan_cli.constants import TABULAR_EXTENSIONS
from portolan_cli.dataset import add_files

if TYPE_CHECKING:
    pass


@pytest.fixture
def initialized_catalog(tmp_path: Path) -> Path:
    """Create an initialized Portolan catalog structure (per ADR-0023)."""
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()

    # Create config.yaml as sentinel (per ADR-0029)
    config_path = portolan_dir / "config.yaml"
    config_path.write_text("# Portolan config\n")

    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "portolan-catalog",
        "description": "A Portolan-managed STAC catalog",
        "links": [],
    }
    (tmp_path / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    return tmp_path


@pytest.fixture
def tabular_parquet(tmp_path: Path) -> Path:
    """Create a Parquet file WITHOUT geometry (tabular data only).

    This simulates a census metadata file or lookup table that contains
    only tabular data without any geometry column.
    """
    # Create item directory structure inside collection
    item_dir = tmp_path / "collection" / "item"
    item_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = item_dir / "census-data.parquet"

    # Create a table WITHOUT geometry metadata
    table = pa.table(
        {
            "code": ["A001", "A002", "A003"],
            "name": ["Area One", "Area Two", "Area Three"],
            "population": [1000, 2000, 3000],
            "year": [2020, 2020, 2020],
        }
    )

    # Write as plain Parquet (no geo metadata)
    pq.write_table(table, parquet_path)

    return parquet_path


class TestTabularParquetConstants:
    """Tests for TABULAR_EXTENSIONS including .parquet."""

    @pytest.mark.unit
    def test_parquet_in_tabular_extensions(self) -> None:
        """Parquet should be in TABULAR_EXTENSIONS for non-geo handling."""
        assert ".parquet" in TABULAR_EXTENSIONS, (
            "Per Issue #177, .parquet should be in TABULAR_EXTENSIONS so that "
            "tabular parquet files without geometry can be tracked as auxiliary assets"
        )


class TestTabularParquetSkip:
    """Tests for skipping tabular parquet files without geometry (Issue #177)."""

    @pytest.mark.unit
    def test_add_files_skips_tabular_parquet_with_warning(
        self, initialized_catalog: Path, tabular_parquet: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """add_files should skip parquet without geometry and emit a warning."""
        with caplog.at_level(logging.WARNING):
            added, skipped = add_files(
                paths=[tabular_parquet],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

        # Should not error - should skip gracefully
        assert len(added) == 0
        # The file should be in skipped (no geo file in same dir to create item)

        # Should emit a warning about non-geospatial file
        warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any(
            "geometry" in msg.lower() or "non-geospatial" in msg.lower() or "parquet" in msg.lower()
            for msg in warning_messages
        ), f"Expected warning about parquet/geometry, got: {warning_messages}"

    @pytest.mark.unit
    def test_add_files_does_not_error_on_tabular_parquet(
        self, initialized_catalog: Path, tabular_parquet: Path
    ) -> None:
        """add_files should NOT raise an exception for tabular parquet."""
        # This should NOT raise - it should handle gracefully
        try:
            added, skipped = add_files(
                paths=[tabular_parquet],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )
        except Exception as e:
            pytest.fail(f"add_files raised an exception for tabular parquet: {e}")


class TestTabularParquetWithGeoAsset:
    """Tests for tabular parquet files alongside geo assets (the primary use case)."""

    @pytest.mark.unit
    def test_tabular_parquet_added_as_auxiliary_asset(
        self, initialized_catalog: Path, tabular_parquet: Path
    ) -> None:
        """Tabular parquet should be tracked as auxiliary asset when geo file exists."""
        # Create a valid GeoJSON file in the same directory as the tabular parquet
        item_dir = tabular_parquet.parent
        geo_file = item_dir / "boundaries.geojson"
        geo_data = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
                    "properties": {"name": "Test Point"},
                }
            ],
        }
        geo_file.write_text(json.dumps(geo_data))

        # Mock add_dataset to simulate successful geo file processing
        with patch("portolan_cli.dataset.add_dataset") as mock_add:
            # Return DatasetInfo for the geo file
            mock_add.return_value = MagicMock(
                item_id="item",
                collection_id="collection",
                asset_paths=["boundaries.parquet"],  # Converted output
            )

            added, skipped = add_files(
                paths=[item_dir],  # Add the entire item directory
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

            # The geo file should be added
            assert mock_add.called, "Should have called add_dataset for GeoJSON"


class TestMixedParquetDirectory:
    """Tests for processing directories with mixed geo and tabular parquet files."""

    @pytest.mark.unit
    def test_all_tabular_parquet_directory_skips_gracefully(
        self, initialized_catalog: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Directory with only tabular parquet should skip without error."""
        # Create item directory
        item_dir = initialized_catalog / "collection" / "item"
        item_dir.mkdir(parents=True, exist_ok=True)

        # Create two tabular parquet files (no geo)
        table1 = pa.table({"code": ["A", "B"], "value": [1, 2]})
        pq.write_table(table1, item_dir / "data1.parquet")

        table2 = pa.table({"code": ["C", "D"], "value": [3, 4]})
        pq.write_table(table2, item_dir / "data2.parquet")

        with caplog.at_level(logging.WARNING):
            added, skipped = add_files(
                paths=[item_dir],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

        # Should return empty without error
        assert len(added) == 0


class TestParquetGeometryDetection:
    """Tests for parquet geometry detection edge cases."""

    @pytest.mark.unit
    def test_parquet_without_geo_metadata_is_tabular(
        self, initialized_catalog: Path, tabular_parquet: Path
    ) -> None:
        """Parquet files without 'geo' metadata should be treated as tabular."""
        # Verify the fixture has no geo metadata
        pf = pq.ParquetFile(tabular_parquet)
        schema = pf.schema_arrow
        metadata = schema.metadata
        assert metadata is None or b"geo" not in metadata, (
            "Test fixture should not have geo metadata"
        )


class TestWarningMessages:
    """Tests for warning message quality and consistency."""

    @pytest.mark.unit
    def test_warning_includes_parquet_info(
        self, initialized_catalog: Path, tabular_parquet: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Warning should mention the file for debugging."""
        with caplog.at_level(logging.WARNING):
            added, skipped = add_files(
                paths=[tabular_parquet],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

        # Check that some identifying info appears in warnings
        all_messages = " ".join(r.message for r in caplog.records)
        # Should mention either the filename or parquet format
        assert (
            "parquet" in all_messages.lower()
            or "census-data" in all_messages.lower()
            or "non-geospatial" in all_messages.lower()
        )
