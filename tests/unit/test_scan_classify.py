"""Unit tests for portolan_cli/scan_classify.py.

Tests file classification into 10 categories and skip reason generation.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.scan_classify import (
    FileCategory,
    SkippedFile,
    SkipReasonType,
    classify_file,
    get_skip_reason,
)


@pytest.mark.unit
class TestFileCategory:
    """Tests for FileCategory enum."""

    def test_has_10_categories(self) -> None:
        """FileCategory should have exactly 10 categories."""
        assert len(FileCategory) == 10

    def test_geo_asset_value(self) -> None:
        """GEO_ASSET should have value 'geo_asset'."""
        assert FileCategory.GEO_ASSET.value == "geo_asset"

    def test_all_categories_have_string_values(self) -> None:
        """All categories should have string values."""
        for category in FileCategory:
            assert isinstance(category.value, str)


@pytest.mark.unit
class TestSkipReasonType:
    """Tests for SkipReasonType enum."""

    def test_has_expected_types(self) -> None:
        """SkipReasonType should have expected types."""
        expected = {
            "not_geospatial",
            "sidecar_file",
            "visualization",
            "metadata_file",
            "junk_file",
            "invalid_format",
            "special_directory",
            "unknown_format",
        }
        actual = {t.value for t in SkipReasonType}
        assert actual == expected


@pytest.mark.unit
class TestSkippedFile:
    """Tests for SkippedFile dataclass."""

    def test_skipped_file_creation(self, tmp_path: Path) -> None:
        """SkippedFile can be created with all required fields."""

        test_path = tmp_path / "test.csv"
        skipped = SkippedFile(
            path=test_path,
            relative_path="test.csv",
            category=FileCategory.TABULAR_DATA,
            reason_type=SkipReasonType.NOT_GEOSPATIAL,
            reason_message="CSV is tabular data, not a geospatial format",
        )
        assert skipped.path == test_path
        assert skipped.relative_path == "test.csv"
        assert skipped.category == FileCategory.TABULAR_DATA
        assert skipped.reason_type == SkipReasonType.NOT_GEOSPATIAL
        assert "tabular" in skipped.reason_message.lower()

    def test_skipped_file_is_frozen(self, tmp_path: Path) -> None:
        """SkippedFile should be immutable (frozen dataclass)."""

        test_path = tmp_path / "test.csv"
        skipped = SkippedFile(
            path=test_path,
            relative_path="test.csv",
            category=FileCategory.TABULAR_DATA,
            reason_type=SkipReasonType.NOT_GEOSPATIAL,
            reason_message="CSV is tabular data",
        )
        with pytest.raises(AttributeError):
            skipped.category = FileCategory.JUNK  # type: ignore[misc]

    def test_skipped_file_to_dict(self, tmp_path: Path) -> None:
        """SkippedFile.to_dict() returns expected structure."""

        test_path = tmp_path / "test.csv"
        skipped = SkippedFile(
            path=test_path,
            relative_path="test.csv",
            category=FileCategory.TABULAR_DATA,
            reason_type=SkipReasonType.NOT_GEOSPATIAL,
            reason_message="CSV is tabular data",
        )
        result = skipped.to_dict()
        assert result["path"] == str(test_path)
        assert result["relative_path"] == "test.csv"
        assert result["category"] == "tabular_data"
        assert result["reason_type"] == "not_geospatial"
        assert result["reason"] == "CSV is tabular data"


@pytest.mark.unit
class TestClassifyFile:
    """Tests for classify_file function."""

    def test_csv_classified_as_tabular_data(self, tmp_path: Path) -> None:
        """CSV files are classified as TABULAR_DATA."""

        test_path = tmp_path / "data.csv"
        test_path.write_text("a,b,c\n1,2,3\n")
        category, skip_type, skip_msg = classify_file(test_path)
        assert category == FileCategory.TABULAR_DATA
        assert skip_type == SkipReasonType.NOT_GEOSPATIAL
        assert skip_msg is not None

    def test_exe_classified_as_junk(self, tmp_path: Path) -> None:
        """Executable files are classified as JUNK."""

        test_path = tmp_path / "program.exe"
        test_path.write_bytes(b"\x00")
        category, skip_type, skip_msg = classify_file(test_path)
        assert category == FileCategory.JUNK
        assert skip_type == SkipReasonType.JUNK_FILE
        assert skip_msg is not None

    def test_pycache_classified_as_junk(self, tmp_path: Path) -> None:
        """__pycache__ files are classified as JUNK."""

        pycache = tmp_path / "__pycache__"
        pycache.mkdir()
        test_path = pycache / "module.cpython-312.pyc"
        test_path.write_bytes(b"\x00")
        category, skip_type, skip_msg = classify_file(test_path)
        assert category == FileCategory.JUNK
        assert skip_type == SkipReasonType.JUNK_FILE

    def test_pycache_uppercase_classified_as_junk(self, tmp_path: Path) -> None:
        """__PYCACHE__ (uppercase) files are classified as JUNK.

        Windows/macOS filesystems are case-insensitive, so __PYCACHE__
        should be treated the same as __pycache__.
        """
        pycache = tmp_path / "__PYCACHE__"
        pycache.mkdir()
        test_path = pycache / "module.cpython-312.pyc"
        test_path.write_bytes(b"\x00")
        category, skip_type, skip_msg = classify_file(test_path)
        assert category == FileCategory.JUNK
        assert skip_type == SkipReasonType.JUNK_FILE

    def test_git_uppercase_classified_as_junk(self, tmp_path: Path) -> None:
        """.GIT (uppercase) directories are classified as JUNK.

        Windows/macOS filesystems are case-insensitive, so .GIT
        should be treated the same as .git.
        """
        git_dir = tmp_path / ".GIT"
        git_dir.mkdir()
        test_path = git_dir / "config"
        test_path.write_text("[core]\n")
        category, skip_type, skip_msg = classify_file(test_path)
        assert category == FileCategory.JUNK
        assert skip_type == SkipReasonType.JUNK_FILE

    def test_pmtiles_classified_as_visualization(self, tmp_path: Path) -> None:
        """.pmtiles files are classified as VISUALIZATION."""

        test_path = tmp_path / "tiles.pmtiles"
        test_path.write_bytes(b"\x00")
        category, skip_type, skip_msg = classify_file(test_path)
        assert category == FileCategory.VISUALIZATION
        assert skip_type == SkipReasonType.VISUALIZATION_ONLY

    def test_geojson_classified_as_geo_asset(self, tmp_path: Path) -> None:
        """GeoJSON files are classified as GEO_ASSET."""

        test_path = tmp_path / "data.geojson"
        test_path.write_text('{"type": "FeatureCollection", "features": []}')
        category, skip_type, skip_msg = classify_file(test_path)
        assert category == FileCategory.GEO_ASSET
        assert skip_type is None
        assert skip_msg is None

    def test_shapefile_sidecar_classified_as_sidecar(self, tmp_path: Path) -> None:
        """Shapefile sidecar (.dbf) classified as KNOWN_SIDECAR."""

        test_path = tmp_path / "data.dbf"
        test_path.write_bytes(b"\x00")
        category, skip_type, skip_msg = classify_file(test_path)
        assert category == FileCategory.KNOWN_SIDECAR
        assert skip_type == SkipReasonType.SIDECAR_FILE

    def test_markdown_classified_as_documentation(self, tmp_path: Path) -> None:
        """Markdown files are classified as DOCUMENTATION."""

        test_path = tmp_path / "README.md"
        test_path.write_text("# Readme\n")
        category, skip_type, skip_msg = classify_file(test_path)
        assert category == FileCategory.DOCUMENTATION
        assert skip_type == SkipReasonType.NOT_GEOSPATIAL

    def test_catalog_json_classified_as_stac_metadata(self, tmp_path: Path) -> None:
        """catalog.json files are classified as STAC_METADATA."""

        test_path = tmp_path / "catalog.json"
        test_path.write_text('{"type": "Catalog"}')
        category, skip_type, skip_msg = classify_file(test_path)
        assert category == FileCategory.STAC_METADATA
        assert skip_type == SkipReasonType.METADATA_FILE

    def test_small_png_classified_as_thumbnail(self, tmp_path: Path) -> None:
        """Small PNG files (<1MB) are classified as THUMBNAIL."""

        test_path = tmp_path / "preview.png"
        # Write a small file (< 1MB)
        test_path.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)
        category, skip_type, skip_msg = classify_file(test_path)
        assert category == FileCategory.THUMBNAIL
        assert skip_type == SkipReasonType.NOT_GEOSPATIAL

    def test_unknown_extension_classified_as_unknown(self, tmp_path: Path) -> None:
        """Unknown extensions are classified as UNKNOWN."""

        test_path = tmp_path / "mystery.xyz123"
        test_path.write_text("unknown content")
        category, skip_type, skip_msg = classify_file(test_path)
        assert category == FileCategory.UNKNOWN
        assert skip_type == SkipReasonType.UNKNOWN_FORMAT


@pytest.mark.unit
class TestSkipReasons:
    """Tests for get_skip_reason function."""

    def test_get_skip_reason_tabular(self, tmp_path: Path) -> None:
        """get_skip_reason returns appropriate message for TABULAR_DATA."""

        test_path = tmp_path / "data.csv"
        skip_type, msg = get_skip_reason(FileCategory.TABULAR_DATA, test_path)
        assert skip_type == SkipReasonType.NOT_GEOSPATIAL
        assert "tabular" in msg.lower() or "csv" in msg.lower()

    def test_get_skip_reason_junk(self, tmp_path: Path) -> None:
        """get_skip_reason returns appropriate message for JUNK."""

        test_path = tmp_path / "program.exe"
        skip_type, msg = get_skip_reason(FileCategory.JUNK, test_path)
        assert skip_type == SkipReasonType.JUNK_FILE
        assert len(msg) > 0

    def test_get_skip_reason_geo_asset_raises(self, tmp_path: Path) -> None:
        """get_skip_reason raises ValueError for GEO_ASSET."""

        test_path = tmp_path / "data.geojson"
        with pytest.raises(ValueError, match="GEO_ASSET"):
            get_skip_reason(FileCategory.GEO_ASSET, test_path)
