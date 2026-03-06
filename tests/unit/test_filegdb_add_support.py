"""Unit tests for FileGDB add support (Issue #154).

FileGDBs should be processable by `portolan add`, not just detected by `scan`.
This requires promoting FileGDBs from `special_formats` to `ready` list.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from portolan_cli.scan import FormatType, ScanOptions, scan_directory


@pytest.mark.unit
class TestFileGDBInReadyList:
    """Tests that FileGDB directories appear in scan result's ready list."""

    def test_filegdb_in_ready_list(self, tmp_path: Path) -> None:
        """FileGDB directories should appear in ready list as processable assets."""
        gdb_dir = tmp_path / "sample.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"A" * 100)
        (gdb_dir / "a00000001.gdbtablx").write_bytes(b"B" * 50)

        result = scan_directory(tmp_path)

        # FileGDB should be in ready list
        ready_paths = [f.path for f in result.ready]
        assert gdb_dir in ready_paths, (
            f"FileGDB should be in ready list. Ready: {ready_paths}, "
            f"Special formats: {[sf.path for sf in result.special_formats]}"
        )

    def test_filegdb_scanned_file_properties(self, tmp_path: Path) -> None:
        """FileGDB ScannedFile should have correct extension and format type."""
        gdb_dir = tmp_path / "census_data.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"x" * 1000)
        (gdb_dir / "a00000002.gdbtable").write_bytes(b"y" * 2000)

        result = scan_directory(tmp_path)

        # Find the FileGDB in ready
        filegdb_files = [f for f in result.ready if f.path == gdb_dir]
        assert len(filegdb_files) == 1, f"Expected 1 FileGDB in ready, got {len(filegdb_files)}"

        scanned = filegdb_files[0]
        assert scanned.extension == ".gdb", f"Expected .gdb extension, got {scanned.extension}"
        assert scanned.format_type == FormatType.VECTOR, (
            f"FileGDB should be VECTOR format, got {scanned.format_type}"
        )
        # Size should be total of internal files
        assert scanned.size_bytes == 3000, f"Expected 3000 bytes, got {scanned.size_bytes}"

    def test_filegdb_relative_path(self, tmp_path: Path) -> None:
        """FileGDB ScannedFile should have correct relative path."""
        subdir = tmp_path / "data" / "census"
        subdir.mkdir(parents=True)
        gdb_dir = subdir / "blocks.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"x")

        result = scan_directory(tmp_path, options=ScanOptions(recursive=True))

        filegdb_files = [f for f in result.ready if f.path == gdb_dir]
        assert len(filegdb_files) == 1

        scanned = filegdb_files[0]
        assert scanned.relative_path == "data/census/blocks.gdb", (
            f"Expected 'data/census/blocks.gdb', got '{scanned.relative_path}'"
        )

    def test_multiple_filegdb_all_in_ready(self, tmp_path: Path) -> None:
        """Multiple FileGDB directories should all appear in ready list."""
        gdb_names = ["parcels.gdb", "roads.gdb", "buildings.gdb"]
        for name in gdb_names:
            gdb_dir = tmp_path / name
            gdb_dir.mkdir()
            (gdb_dir / "a00000001.gdbtable").write_bytes(b"x")

        result = scan_directory(tmp_path)

        ready_paths = {f.path for f in result.ready}
        for name in gdb_names:
            expected_path = tmp_path / name
            assert expected_path in ready_paths, f"FileGDB {name} should be in ready list"

        # All should be VECTOR format
        filegdb_files = [f for f in result.ready if f.extension == ".gdb"]
        assert len(filegdb_files) == 3
        assert all(f.format_type == FormatType.VECTOR for f in filegdb_files)

    def test_filegdb_mixed_with_regular_files(self, tmp_path: Path) -> None:
        """FileGDBs should appear in ready alongside regular geospatial files."""
        # Create a FileGDB
        gdb_dir = tmp_path / "sample.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"x")

        # Create regular geospatial files
        (tmp_path / "points.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        result = scan_directory(tmp_path)

        ready_paths = {f.path for f in result.ready}
        assert gdb_dir in ready_paths, "FileGDB should be in ready"
        assert (tmp_path / "points.geojson") in ready_paths, "GeoJSON should be in ready"

    def test_empty_gdb_not_in_ready(self, tmp_path: Path) -> None:
        """Empty .gdb directory (no .gdbtable files) should NOT be in ready."""
        # Create empty .gdb directory - NOT a valid FileGDB
        empty_gdb = tmp_path / "fake.gdb"
        empty_gdb.mkdir()
        # Add a random file inside (should be walked as normal directory)
        (empty_gdb / "readme.txt").write_text("Not a real FileGDB")

        result = scan_directory(tmp_path)

        # Empty .gdb should NOT be in ready
        ready_paths = [f.path for f in result.ready]
        assert empty_gdb not in ready_paths, "Empty .gdb dir should not be in ready"

        # The txt file inside should be in skipped
        skipped_paths = [f.path for f in result.skipped]
        txt_found = any(p.name == "readme.txt" for p in skipped_paths)
        assert txt_found, "Files inside invalid .gdb should be walked normally"


@pytest.mark.unit
class TestFileGDBCountsInSummary:
    """Tests that FileGDBs are counted correctly in scan summary."""

    def test_filegdb_counted_as_geo_asset(self, tmp_path: Path) -> None:
        """FileGDB should be counted in classification_summary as geo_asset."""
        gdb_dir = tmp_path / "data.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"x")

        result = scan_directory(tmp_path)

        summary = result.classification_summary
        assert summary.get("geo_asset", 0) >= 1, (
            f"FileGDB should be counted as geo_asset. Summary: {summary}"
        )

    def test_filegdb_included_in_ready_count(self, tmp_path: Path) -> None:
        """FileGDB should be included in ready_count in to_dict output."""
        gdb_dir = tmp_path / "data.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"x")

        result = scan_directory(tmp_path)
        result_dict = result.to_dict()

        assert result_dict["summary"]["ready_count"] >= 1, (
            f"ready_count should include FileGDB. Got: {result_dict['summary']}"
        )

    def test_filegdb_extension_in_format_breakdown(self, tmp_path: Path) -> None:
        """FileGDB .gdb extension should appear in format breakdown."""
        gdb_dir = tmp_path / "sample.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"x")

        result = scan_directory(tmp_path)

        # Check that .gdb extension is represented
        extensions = [f.extension for f in result.ready]
        assert ".gdb" in extensions, f"Expected .gdb in extensions. Got: {extensions}"


@pytest.mark.unit
class TestFileGDBNotInSpecialFormats:
    """Tests that FileGDBs are NO LONGER in special_formats after this change."""

    def test_filegdb_not_in_special_formats(self, tmp_path: Path) -> None:
        """FileGDB should NOT appear in special_formats anymore (moved to ready)."""
        gdb_dir = tmp_path / "sample.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"x")

        result = scan_directory(tmp_path)

        # FileGDB should NOT be in special_formats
        special_filegdb = [sf for sf in result.special_formats if sf.format_type == "filegdb"]
        assert len(special_filegdb) == 0, (
            f"FileGDB should NOT be in special_formats anymore. Found: {special_filegdb}"
        )

        # But should be in ready
        ready_paths = [f.path for f in result.ready]
        assert gdb_dir in ready_paths, "FileGDB should be in ready list"


@pytest.mark.unit
class TestFileGDBPropertyBased:
    """Property-based tests for FileGDB in ready list."""

    @given(
        gdb_name=st.from_regex(r"[a-zA-Z][a-zA-Z0-9_]{0,10}\.gdb", fullmatch=True),
        num_tables=st.integers(min_value=1, max_value=5),
        table_sizes=st.lists(st.integers(min_value=1, max_value=1000), min_size=1, max_size=5),
    )
    @settings(max_examples=20)
    def test_valid_filegdb_always_in_ready(
        self, gdb_name: str, num_tables: int, table_sizes: list[int]
    ) -> None:
        """Any valid FileGDB structure should appear in ready list."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            gdb_dir = tmp_path / gdb_name
            gdb_dir.mkdir(exist_ok=True)

            # Create internal structure
            total_size = 0
            for i in range(min(num_tables, len(table_sizes))):
                size = table_sizes[i]
                (gdb_dir / f"a0000000{i + 1}.gdbtable").write_bytes(b"x" * size)
                total_size += size

            result = scan_directory(tmp_path)

            # Should be in ready
            ready_paths = [f.path for f in result.ready]
            assert gdb_dir in ready_paths, f"FileGDB {gdb_name} should be in ready"

            # Should have correct size
            scanned = next(f for f in result.ready if f.path == gdb_dir)
            assert scanned.size_bytes == total_size, (
                f"Size mismatch: expected {total_size}, got {scanned.size_bytes}"
            )

    @given(
        num_filegdbs=st.integers(min_value=1, max_value=5),
    )
    @settings(max_examples=10)
    def test_multiple_filegdbs_all_in_ready(self, num_filegdbs: int) -> None:
        """Multiple FileGDBs should all appear in ready list."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            gdb_dirs = []

            for i in range(num_filegdbs):
                gdb_dir = tmp_path / f"data_{i}.gdb"
                gdb_dir.mkdir(exist_ok=True)
                (gdb_dir / "a00000001.gdbtable").write_bytes(b"x")
                gdb_dirs.append(gdb_dir)

            result = scan_directory(tmp_path)

            ready_paths = {f.path for f in result.ready}
            for gdb_dir in gdb_dirs:
                assert gdb_dir in ready_paths, f"FileGDB {gdb_dir.name} should be in ready"
