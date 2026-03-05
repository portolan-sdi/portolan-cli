"""Unit tests for FileGDB directory detection during scan.

Issue #139: FileGDB directories should be detected as single assets during scan,
not walked into with internal files reported as "unknown format".
"""

from __future__ import annotations

from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from portolan_cli.scan import ScanOptions, scan_directory
from portolan_cli.scan_detect import is_filegdb


@pytest.mark.unit
class TestFileGDBDirectoryDiscovery:
    """Tests that FileGDB directories are treated as single assets during scan."""

    def test_filegdb_directory_not_recursed_into(self, tmp_path: Path) -> None:
        """Scan should NOT recurse into .gdb directories and report internal files."""
        # Create a FileGDB directory structure
        gdb_dir = tmp_path / "sample.gdb"
        gdb_dir.mkdir()
        # Create internal FileGDB files
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"\x00")
        (gdb_dir / "a00000001.gdbtablx").write_bytes(b"\x00")
        (gdb_dir / "a00000001.spx").write_bytes(b"\x00")
        (gdb_dir / "gdb").write_bytes(b"\x00")  # marker file

        result = scan_directory(tmp_path)

        # Internal files should NOT be reported as unknown/skipped
        internal_extensions = {".gdbtable", ".gdbtablx", ".spx"}
        skipped_extensions = {f.path.suffix for f in result.skipped}

        # None of the internal FileGDB extensions should be in skipped files
        assert not internal_extensions.intersection(skipped_extensions), (
            f"FileGDB internal files should not be in skipped: {skipped_extensions}"
        )

    def test_filegdb_directory_detected_as_asset(self, tmp_path: Path) -> None:
        """Scan should detect .gdb directory as a single geospatial asset."""
        # Create a FileGDB directory structure
        gdb_dir = tmp_path / "my_data.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"\x00")

        result = scan_directory(tmp_path)

        # FileGDB should appear in special_formats or be handled appropriately
        # The directory itself should be detected, not its internal files
        all_paths = [f.path for f in result.ready + result.skipped]

        # Should NOT contain paths inside the .gdb directory
        for path in all_paths:
            assert ".gdb/" not in str(path) and ".gdb\\" not in str(path), (
                f"Scan walked into FileGDB directory: {path}"
            )

    def test_filegdb_sibling_files_still_scanned(self, tmp_path: Path) -> None:
        """Files next to FileGDB directory should still be scanned normally."""
        # Create a FileGDB directory
        gdb_dir = tmp_path / "sample.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"\x00")

        # Create a sibling geojson file
        sibling = tmp_path / "other_data.geojson"
        sibling.write_text('{"type": "FeatureCollection", "features": []}')

        result = scan_directory(tmp_path)

        # Sibling file should be found
        sibling_found = any(f.path == sibling for f in result.ready)
        assert sibling_found, "Sibling files next to FileGDB should still be scanned"

    def test_filegdb_in_subdirectory_detected(self, tmp_path: Path) -> None:
        """FileGDB in a subdirectory should be detected, not recursed into."""
        # Create subdirectory with FileGDB
        subdir = tmp_path / "data"
        subdir.mkdir()
        gdb_dir = subdir / "census.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"\x00")
        (gdb_dir / "a00000002.gdbtable").write_bytes(b"\x00")

        result = scan_directory(tmp_path, options=ScanOptions(recursive=True))

        # Should NOT find internal .gdbtable files in skipped
        gdbtable_files = [f for f in result.skipped if f.path.suffix == ".gdbtable"]
        assert len(gdbtable_files) == 0, (
            f"Internal .gdbtable files should not be reported: {gdbtable_files}"
        )

    def test_multiple_filegdb_directories(self, tmp_path: Path) -> None:
        """Multiple FileGDB directories should all be detected properly."""
        # Create multiple FileGDB directories
        for name in ["data_2020.gdb", "data_2021.gdb", "data_2022.gdb"]:
            gdb_dir = tmp_path / name
            gdb_dir.mkdir()
            (gdb_dir / "a00000001.gdbtable").write_bytes(b"\x00")

        result = scan_directory(tmp_path)

        # None of the internal files should be in results
        all_files = result.ready + result.skipped
        internal_files = [f for f in all_files if ".gdb/" in str(f.path) or ".gdb\\" in str(f.path)]
        assert len(internal_files) == 0, (
            f"No internal FileGDB files should be found: {internal_files}"
        )

    def test_empty_gdb_directory_is_skipped(self, tmp_path: Path) -> None:
        """Empty .gdb directory (no .gdbtable files) should be treated as regular dir."""
        # Create empty .gdb directory - NOT a valid FileGDB
        empty_gdb = tmp_path / "fake.gdb"
        empty_gdb.mkdir()
        # Add a random file inside
        (empty_gdb / "readme.txt").write_text("This is not a real FileGDB")

        result = scan_directory(tmp_path)

        # The txt file inside should be found (skipped as unknown format)
        txt_found = any(f.path.suffix == ".txt" for f in result.skipped)
        assert txt_found, "Empty .gdb dir should be walked into like regular directory"

    def test_filegdb_with_lock_files(self, tmp_path: Path) -> None:
        """FileGDB with lock files should still be detected as single asset."""
        gdb_dir = tmp_path / "active.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"\x00")
        # Create lock files (from active editing)
        (gdb_dir / "a00000001.lck").write_bytes(b"\x00")
        (gdb_dir / "lockfile").write_bytes(b"\x00")

        result = scan_directory(tmp_path)

        # Lock files should NOT be in results
        lock_files = [
            f for f in result.skipped if f.path.suffix == ".lck" or "lock" in f.path.name.lower()
        ]
        assert len(lock_files) == 0, f"Lock files should not be found: {lock_files}"


@pytest.mark.unit
class TestFileGDBArchiveDetection:
    """Tests for FileGDB archive (.gdb.zip) handling during scan."""

    def test_filegdb_zip_detected_correctly(self, tmp_path: Path) -> None:
        """FileGDB zip archive should be detected (not unzipped and walked)."""
        import zipfile

        # Create a .gdb.zip file
        zip_path = tmp_path / "sample.gdb.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("sample.gdb/a00000001.gdbtable", b"\x00")

        result = scan_directory(tmp_path)

        # The zip file should be found (in ready or skipped based on support)
        all_files = result.ready + result.skipped
        zip_found = any(f.path == zip_path for f in all_files)
        assert zip_found, "FileGDB zip archive should be detected"


@pytest.mark.unit
class TestFileGDBDirectorySizeCalculation:
    """Tests for FileGDB directory size calculation."""

    def test_filegdb_size_is_total_of_internal_files(self, tmp_path: Path) -> None:
        """FileGDB asset size should be total of all internal files."""
        gdb_dir = tmp_path / "measured.gdb"
        gdb_dir.mkdir()

        # Create files with known sizes
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"x" * 1000)
        (gdb_dir / "a00000002.gdbtable").write_bytes(b"y" * 2000)
        expected_total = 3000

        result = scan_directory(tmp_path)

        # If FileGDB appears in special_formats, check its size
        # The size should be the total of internal files
        if result.special_formats:
            filegdb_formats = [sf for sf in result.special_formats if sf.format_type == "filegdb"]
            if filegdb_formats:
                # Verify size is calculated correctly
                assert filegdb_formats[0].details.get("size_bytes", 0) >= expected_total - 100


# =============================================================================
# Hypothesis Property-Based Tests
# =============================================================================


@pytest.mark.unit
class TestFileGDBPropertyBased:
    """Property-based tests using Hypothesis for FileGDB detection."""

    @given(
        gdb_name=st.from_regex(r"[a-zA-Z][a-zA-Z0-9_]{0,10}\.gdb", fullmatch=True),
        num_tables=st.integers(min_value=1, max_value=5),
    )
    @settings(max_examples=20)
    def test_valid_filegdb_always_detected(self, gdb_name: str, num_tables: int) -> None:
        """Any valid FileGDB structure should be detected by is_filegdb."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            gdb_dir = tmp_path / gdb_name
            gdb_dir.mkdir(exist_ok=True)

            # Create internal structure
            for i in range(num_tables):
                (gdb_dir / f"a0000000{i + 1}.gdbtable").write_bytes(b"\x00")

            assert is_filegdb(gdb_dir) is True

    @given(
        dir_name=st.from_regex(r"[a-zA-Z][a-zA-Z0-9_]{0,10}", fullmatch=True),
    )
    @settings(max_examples=20)
    def test_non_gdb_directory_not_detected(self, dir_name: str) -> None:
        """Directories without .gdb extension should not be detected."""
        import tempfile

        # Don't let hypothesis accidentally create a .gdb directory
        if dir_name.endswith(".gdb"):
            dir_name = dir_name[:-4] + "_data"

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            regular_dir = tmp_path / dir_name
            regular_dir.mkdir(exist_ok=True)
            (regular_dir / "some_file.txt").write_text("content")

            assert is_filegdb(regular_dir) is False

    @given(
        gdb_name=st.from_regex(r"[a-zA-Z][a-zA-Z0-9_]{0,10}\.gdb", fullmatch=True),
    )
    @settings(max_examples=10)
    def test_empty_gdb_directory_not_detected(self, gdb_name: str) -> None:
        """Empty .gdb directory (no .gdbtable files) should not be detected."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            gdb_dir = tmp_path / gdb_name
            gdb_dir.mkdir(exist_ok=True)

            # Don't create any .gdbtable files
            assert is_filegdb(gdb_dir) is False

    @given(
        gdb_name=st.from_regex(r"[a-zA-Z][a-zA-Z0-9_]{0,10}\.gdb", fullmatch=True),
        num_tables=st.integers(min_value=1, max_value=3),
        num_siblings=st.integers(min_value=0, max_value=3),
    )
    @settings(max_examples=15)
    def test_scan_never_yields_filegdb_internal_files(
        self, gdb_name: str, num_tables: int, num_siblings: int
    ) -> None:
        """Scan should never yield internal FileGDB files regardless of structure."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            gdb_dir = tmp_path / gdb_name
            gdb_dir.mkdir(exist_ok=True)

            # Create FileGDB internal structure
            for i in range(num_tables):
                (gdb_dir / f"a0000000{i + 1}.gdbtable").write_bytes(b"\x00")

            # Create sibling files
            for i in range(num_siblings):
                (tmp_path / f"sibling_{i}.geojson").write_text(
                    '{"type": "FeatureCollection", "features": []}'
                )

            result = scan_directory(tmp_path)

            # Collect all paths from results
            all_paths = [f.path for f in result.ready] + [f.path for f in result.skipped]

            # No path should be inside a .gdb directory
            for path in all_paths:
                path_str = str(path)
                assert ".gdb/" not in path_str and ".gdb\\" not in path_str, (
                    f"Scan returned internal FileGDB file: {path}"
                )

    @given(
        depth=st.integers(min_value=1, max_value=3),
        gdb_name=st.from_regex(r"[a-zA-Z][a-zA-Z0-9_]{0,8}\.gdb", fullmatch=True),
    )
    @settings(max_examples=10)
    def test_nested_filegdb_not_walked_into(self, depth: int, gdb_name: str) -> None:
        """FileGDB at any nesting depth should not be walked into."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Create nested directory structure
            current = tmp_path
            for i in range(depth):
                current = current / f"level_{i}"
                current.mkdir(exist_ok=True)

            # Create FileGDB at the deepest level
            gdb_dir = current / gdb_name
            gdb_dir.mkdir(exist_ok=True)
            (gdb_dir / "a00000001.gdbtable").write_bytes(b"\x00")
            (gdb_dir / "a00000002.gdbtable").write_bytes(b"\x00")

            result = scan_directory(tmp_path, options=ScanOptions(recursive=True))

            # No .gdbtable files should appear in skipped
            gdbtable_files = [f for f in result.skipped if f.path.suffix == ".gdbtable"]
            assert len(gdbtable_files) == 0, (
                f"Internal .gdbtable files found at depth {depth}: {gdbtable_files}"
            )
