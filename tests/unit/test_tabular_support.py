"""Unit tests for non-geo tabular data support (Issue #432).

Tests the classification, configuration, and handling of tabular data
(CSV, TSV, XLSX, plain Parquet) as collection-level assets.

Design decisions tested:
- GeoParquet vs plain Parquet detection via metadata peeking
- tabular.enabled config (default: false)
- tabular.convert config (default: true)
- Error with hint when tabular.enabled is false
- XLSX/XLS basic support (single sheet)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from portolan_cli.scan_classify import (
    FileCategory,
    classify_file,
)

if TYPE_CHECKING:
    pass


@pytest.mark.unit
class TestParquetClassification:
    """Tests for distinguishing GeoParquet from plain Parquet."""

    def test_geoparquet_classified_as_geo_asset(self, tmp_path: Path) -> None:
        """GeoParquet files (with geo metadata) should be GEO_ASSET."""
        parquet_file = tmp_path / "data.parquet"

        # Create a minimal GeoParquet file with geo metadata
        table = pa.table({"id": [1, 2], "geometry": [b"wkb1", b"wkb2"]})
        geo_metadata = {
            "version": "1.0.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
        }
        existing_meta = table.schema.metadata or {}
        new_meta = {**existing_meta, b"geo": json.dumps(geo_metadata).encode()}
        table = table.replace_schema_metadata(new_meta)
        pq.write_table(table, parquet_file)

        category, skip_type, message = classify_file(parquet_file)

        assert category == FileCategory.GEO_ASSET
        assert skip_type is None
        assert message is None

    def test_plain_parquet_classified_as_tabular(self, tmp_path: Path) -> None:
        """Plain Parquet files (no geo metadata) should be TABULAR_DATA."""
        parquet_file = tmp_path / "demographics.parquet"

        # Create a plain Parquet file without geo metadata
        table = pa.table({"tract_id": ["001", "002"], "population": [5000, 7500]})
        pq.write_table(table, parquet_file)

        category, skip_type, message = classify_file(parquet_file)

        assert category == FileCategory.TABULAR_DATA
        assert skip_type is not None
        assert "tabular" in message.lower() or "parquet" in message.lower()

    def test_parquet_with_geometry_column_but_no_geo_metadata(self, tmp_path: Path) -> None:
        """Parquet with geometry column but no geo metadata should be TABULAR_DATA.

        This is an edge case where someone wrote a Parquet file with a geometry
        column but didn't include GeoParquet metadata. We classify based on
        metadata, not column names, to be consistent and fast.
        """
        parquet_file = tmp_path / "maybe_geo.parquet"

        # Create a Parquet with a geometry-like column but NO geo metadata
        table = pa.table({"id": [1], "geometry": [b"some_wkb_bytes"]})
        pq.write_table(table, parquet_file)

        category, _, _ = classify_file(parquet_file)

        # Without geo metadata, it's tabular (gpio will detect on read if needed)
        assert category == FileCategory.TABULAR_DATA


@pytest.mark.unit
class TestTabularExtensions:
    """Tests for tabular file extension classification."""

    @pytest.mark.parametrize(
        "filename,expected_category",
        [
            ("data.csv", FileCategory.TABULAR_DATA),
            ("data.tsv", FileCategory.TABULAR_DATA),
            ("data.xlsx", FileCategory.TABULAR_DATA),
            ("data.xls", FileCategory.TABULAR_DATA),
        ],
    )
    def test_tabular_extensions_classified_correctly(
        self, tmp_path: Path, filename: str, expected_category: FileCategory
    ) -> None:
        """CSV, TSV, XLSX, XLS should all be classified as TABULAR_DATA."""
        file_path = tmp_path / filename
        file_path.touch()

        category, skip_type, message = classify_file(file_path)

        assert category == expected_category
        assert skip_type is not None  # Tabular files have skip reasons


@pytest.mark.unit
class TestTabularConfig:
    """Tests for tabular configuration settings."""

    def test_tabular_enabled_default_is_false(self) -> None:
        """tabular.enabled should default to false."""
        from portolan_cli.config import DEFAULT_SETTINGS

        assert DEFAULT_SETTINGS.get("tabular.enabled") is False

    def test_tabular_convert_default_is_true(self) -> None:
        """tabular.convert should default to true."""
        from portolan_cli.config import DEFAULT_SETTINGS

        assert DEFAULT_SETTINGS.get("tabular.convert") is True

    def test_tabular_settings_in_known_settings(self) -> None:
        """Tabular settings should be in KNOWN_SETTINGS."""
        from portolan_cli.config import KNOWN_SETTINGS

        assert "tabular.enabled" in KNOWN_SETTINGS
        assert "tabular.convert" in KNOWN_SETTINGS


@pytest.mark.unit
class TestConstantsUnification:
    """Tests that tabular constants are unified across modules."""

    def test_constants_tabular_includes_parquet(self) -> None:
        """constants.py TABULAR_EXTENSIONS should include .parquet."""
        from portolan_cli.constants import TABULAR_EXTENSIONS

        assert ".parquet" in TABULAR_EXTENSIONS

    def test_constants_tabular_includes_excel(self) -> None:
        """constants.py TABULAR_EXTENSIONS should include Excel formats."""
        from portolan_cli.constants import TABULAR_EXTENSIONS

        assert ".xlsx" in TABULAR_EXTENSIONS
        assert ".xls" in TABULAR_EXTENSIONS

    def test_scan_classify_tabular_matches_constants(self) -> None:
        """scan_classify.py TABULAR_EXTENSIONS should match constants.py."""
        from portolan_cli.constants import TABULAR_EXTENSIONS as CONST_TABULAR
        from portolan_cli.scan_classify import TABULAR_EXTENSIONS as SCAN_TABULAR

        # scan_classify should have at least the same extensions
        # (it may have more if needed for classification)
        assert CONST_TABULAR <= SCAN_TABULAR

    def test_parquet_not_in_geo_asset_extensions(self) -> None:
        """Parquet should NOT be in GEO_ASSET_EXTENSIONS (peeking required)."""
        from portolan_cli.scan_classify import GEO_ASSET_EXTENSIONS

        # .parquet requires metadata peeking, so it shouldn't be in the simple
        # extension-based GEO_ASSET list
        assert ".parquet" not in GEO_ASSET_EXTENSIONS


@pytest.mark.unit
class TestIsGeoParquet:
    """Tests for the is_geoparquet() helper function."""

    def test_is_geoparquet_with_geo_metadata(self, tmp_path: Path) -> None:
        """Files with geo metadata should return True."""
        from portolan_cli.scan_classify import is_geoparquet

        parquet_file = tmp_path / "geo.parquet"
        table = pa.table({"id": [1], "geometry": [b"wkb"]})
        geo_metadata = {
            "version": "1.0.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB"}},
        }
        existing_meta = table.schema.metadata or {}
        new_meta = {**existing_meta, b"geo": json.dumps(geo_metadata).encode()}
        table = table.replace_schema_metadata(new_meta)
        pq.write_table(table, parquet_file)

        assert is_geoparquet(parquet_file) is True

    def test_is_geoparquet_without_geo_metadata(self, tmp_path: Path) -> None:
        """Files without geo metadata should return False."""
        from portolan_cli.scan_classify import is_geoparquet

        parquet_file = tmp_path / "plain.parquet"
        table = pa.table({"id": [1], "value": [100]})
        pq.write_table(table, parquet_file)

        assert is_geoparquet(parquet_file) is False

    def test_is_geoparquet_handles_read_error(self, tmp_path: Path) -> None:
        """Should return False on read errors (not crash)."""
        from portolan_cli.scan_classify import is_geoparquet

        bad_file = tmp_path / "not_parquet.parquet"
        bad_file.write_text("this is not a parquet file")

        # Should not raise, should return False
        assert is_geoparquet(bad_file) is False

    def test_is_geoparquet_nonexistent_file(self, tmp_path: Path) -> None:
        """Should return False for nonexistent files."""
        from portolan_cli.scan_classify import is_geoparquet

        missing = tmp_path / "missing.parquet"

        assert is_geoparquet(missing) is False
