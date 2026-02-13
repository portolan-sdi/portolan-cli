"""Unit tests for portolan_cli/scan_detect.py.

Tests special format detection: FileGDB, Hive partitioning, STAC catalogs, dual-format.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.scan_detect import (
    detect_dual_formats,
    detect_filegdb,
    detect_hive_partitions,
    detect_stac_catalogs,
    is_filegdb,
    is_filegdb_archive,
    is_hive_partition_dir,
)

# Fixture path helper
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "scan"


@pytest.fixture
def fixtures_dir() -> Path:
    """Return the path to scan test fixtures."""
    return FIXTURES_DIR


@pytest.mark.unit
class TestFileGDBDetection:
    """Tests for FileGDB detection."""

    def test_is_filegdb_true_for_gdb_directory(self, fixtures_dir: Path) -> None:
        """is_filegdb returns True for valid .gdb directory."""
        gdb_path = fixtures_dir / "filegdb" / "sample.gdb"
        assert is_filegdb(gdb_path) is True

    def test_is_filegdb_false_for_regular_directory(self, tmp_path: Path) -> None:
        """is_filegdb returns False for non-.gdb directory."""
        regular_dir = tmp_path / "data"
        regular_dir.mkdir()
        assert is_filegdb(regular_dir) is False

    def test_is_filegdb_false_for_file(self, tmp_path: Path) -> None:
        """is_filegdb returns False for files."""
        test_file = tmp_path / "data.gdb"  # File, not directory
        test_file.write_text("content")
        assert is_filegdb(test_file) is False

    def test_is_filegdb_false_for_empty_gdb_dir(self, tmp_path: Path) -> None:
        """is_filegdb returns False for empty .gdb directory (no gdbtable files)."""
        empty_gdb = tmp_path / "empty.gdb"
        empty_gdb.mkdir()
        # No internal structure - not a real FileGDB
        assert is_filegdb(empty_gdb) is False

    def test_is_filegdb_true_for_gdb_with_gdbtable(self, tmp_path: Path) -> None:
        """is_filegdb returns True for .gdb directory with .gdbtable files."""
        gdb_dir = tmp_path / "test.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"\x00")
        assert is_filegdb(gdb_dir) is True

    def test_is_filegdb_archive_true_for_gdb_zip(self, fixtures_dir: Path) -> None:
        """is_filegdb_archive returns True for .gdb.zip files."""
        zip_path = fixtures_dir / "filegdb" / "sample.gdb.zip"
        assert is_filegdb_archive(zip_path) is True

    def test_is_filegdb_archive_false_for_regular_zip(self, tmp_path: Path) -> None:
        """is_filegdb_archive returns False for non-.gdb.zip files."""
        regular_zip = tmp_path / "archive.zip"
        regular_zip.write_bytes(b"PK")  # Minimal zip header
        assert is_filegdb_archive(regular_zip) is False

    def test_is_filegdb_archive_false_for_directory(self, tmp_path: Path) -> None:
        """is_filegdb_archive returns False for directories."""
        gdb_dir = tmp_path / "sample.gdb"
        gdb_dir.mkdir()
        assert is_filegdb_archive(gdb_dir) is False

    def test_detect_filegdb_returns_special_format(self, fixtures_dir: Path) -> None:
        """detect_filegdb returns SpecialFormat for valid FileGDB."""
        gdb_path = fixtures_dir / "filegdb" / "sample.gdb"
        root = fixtures_dir / "filegdb"
        result = detect_filegdb(gdb_path, root)

        assert result is not None
        assert result.format_type == "filegdb"
        assert result.path == gdb_path
        assert "sample.gdb" in result.relative_path

    def test_detect_filegdb_returns_none_for_non_gdb(self, tmp_path: Path) -> None:
        """detect_filegdb returns None for non-FileGDB paths."""
        regular_dir = tmp_path / "data"
        regular_dir.mkdir()
        result = detect_filegdb(regular_dir, tmp_path)

        assert result is None

    def test_filegdb_lock_files_ignored(self, tmp_path: Path) -> None:
        """Lock files inside FileGDB directories are ignored."""
        gdb_dir = tmp_path / "test.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"\x00")
        # Create lock files that should be ignored
        (gdb_dir / "a00000001.lck").write_bytes(b"\x00")
        (gdb_dir / "lockfile").write_bytes(b"\x00")

        # Should still be detected as FileGDB
        assert is_filegdb(gdb_dir) is True

        result = detect_filegdb(gdb_dir, tmp_path)
        assert result is not None
        # Lock files should not be included in file count
        assert result.details.get("lock_files_present") is True or "gdbtable" in str(result.details)


@pytest.mark.unit
class TestHiveDetection:
    """Tests for Hive partition detection."""

    def test_is_hive_partition_dir_matches_pattern(self) -> None:
        """is_hive_partition_dir matches key=value pattern."""
        result = is_hive_partition_dir("year=2020")
        assert result == ("year", "2020")

    def test_is_hive_partition_dir_matches_multiple_values(self) -> None:
        """is_hive_partition_dir handles various value formats."""
        assert is_hive_partition_dir("state=CA") == ("state", "CA")
        assert is_hive_partition_dir("month=01") == ("month", "01")
        assert is_hive_partition_dir("country=United_States") == (
            "country",
            "United_States",
        )

    def test_is_hive_partition_dir_no_match_for_regular_dir(self) -> None:
        """is_hive_partition_dir returns None for non-Hive directories."""
        assert is_hive_partition_dir("data") is None
        assert is_hive_partition_dir("2020") is None
        assert is_hive_partition_dir("census_data") is None

    def test_is_hive_partition_dir_no_match_for_equals_only(self) -> None:
        """is_hive_partition_dir requires valid key format."""
        assert is_hive_partition_dir("=value") is None
        assert is_hive_partition_dir("key=") is None  # Empty value might be None or tuple

    def test_detect_hive_partitions_finds_partitioned_dataset(self, tmp_path: Path) -> None:
        """detect_hive_partitions finds Hive-partitioned datasets."""
        # Create Hive partition structure
        (tmp_path / "year=2020" / "state=CA").mkdir(parents=True)
        (tmp_path / "year=2020" / "state=CA" / "data.parquet").write_bytes(b"\x00")
        (tmp_path / "year=2020" / "state=NY").mkdir(parents=True)
        (tmp_path / "year=2020" / "state=NY" / "data.parquet").write_bytes(b"\x00")

        results = detect_hive_partitions(tmp_path)

        assert len(results) >= 1
        assert any(r.format_type == "hive_partition" for r in results)

    def test_detect_hive_partitions_returns_empty_for_regular(self, tmp_path: Path) -> None:
        """detect_hive_partitions returns empty list for non-Hive directories."""
        (tmp_path / "data").mkdir()
        (tmp_path / "data" / "file.parquet").write_bytes(b"\x00")

        results = detect_hive_partitions(tmp_path)
        assert results == []


@pytest.mark.unit
class TestSTACDetection:
    """Tests for STAC catalog detection."""

    def test_detect_stac_catalogs_finds_catalog_json(self, tmp_path: Path) -> None:
        """detect_stac_catalogs finds catalog.json files."""
        catalog_content = '{"type": "Catalog", "id": "test-catalog"}'
        (tmp_path / "catalog.json").write_text(catalog_content)

        results = detect_stac_catalogs(tmp_path)

        assert len(results) == 1
        assert results[0].format_type == "stac_catalog"
        assert "catalog.json" in results[0].relative_path

    def test_detect_stac_catalogs_finds_collection_json(self, tmp_path: Path) -> None:
        """detect_stac_catalogs finds collection.json files."""
        collection_content = '{"type": "Collection", "id": "test-collection"}'
        subdir = tmp_path / "my_collection"
        subdir.mkdir()
        (subdir / "collection.json").write_text(collection_content)

        results = detect_stac_catalogs(tmp_path)

        assert len(results) == 1
        assert results[0].format_type == "stac_collection"

    def test_detect_stac_catalogs_returns_empty_for_no_stac(self, tmp_path: Path) -> None:
        """detect_stac_catalogs returns empty list when no STAC files present."""
        (tmp_path / "data.geojson").write_text('{"type": "FeatureCollection"}')

        results = detect_stac_catalogs(tmp_path)
        assert results == []


@pytest.mark.unit
class TestDualFormat:
    """Tests for dual-format detection."""

    def test_detect_dual_formats_finds_same_basename(self, tmp_path: Path) -> None:
        """detect_dual_formats finds files with same basename, different extensions."""
        from portolan_cli.scan import FormatType, ScannedFile

        files = [
            ScannedFile(
                path=tmp_path / "boundaries.geojson",
                relative_path="boundaries.geojson",
                extension=".geojson",
                format_type=FormatType.VECTOR,
                size_bytes=100,
            ),
            ScannedFile(
                path=tmp_path / "boundaries.parquet",
                relative_path="boundaries.parquet",
                extension=".parquet",
                format_type=FormatType.VECTOR,
                size_bytes=200,
            ),
        ]

        results = detect_dual_formats(files)

        assert len(results) == 1
        assert results[0].basename == "boundaries"
        assert ".geojson" in results[0].format_types or ".parquet" in results[0].format_types

    def test_detect_dual_formats_ignores_different_basenames(self, tmp_path: Path) -> None:
        """detect_dual_formats ignores files with different basenames."""
        from portolan_cli.scan import FormatType, ScannedFile

        files = [
            ScannedFile(
                path=tmp_path / "boundaries.geojson",
                relative_path="boundaries.geojson",
                extension=".geojson",
                format_type=FormatType.VECTOR,
                size_bytes=100,
            ),
            ScannedFile(
                path=tmp_path / "census.parquet",
                relative_path="census.parquet",
                extension=".parquet",
                format_type=FormatType.VECTOR,
                size_bytes=200,
            ),
        ]

        results = detect_dual_formats(files)
        assert results == []

    def test_detect_dual_formats_ignores_cross_type_pairs(self, tmp_path: Path) -> None:
        """detect_dual_formats ignores vector/raster pairs (intentional pairing)."""
        from portolan_cli.scan import FormatType, ScannedFile

        files = [
            ScannedFile(
                path=tmp_path / "landcover.geojson",
                relative_path="landcover.geojson",
                extension=".geojson",
                format_type=FormatType.VECTOR,
                size_bytes=100,
            ),
            ScannedFile(
                path=tmp_path / "landcover.tif",
                relative_path="landcover.tif",
                extension=".tif",
                format_type=FormatType.RASTER,
                size_bytes=200,
            ),
        ]

        results = detect_dual_formats(files)
        assert results == []
