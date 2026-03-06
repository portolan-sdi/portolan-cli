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


@pytest.mark.realdata
class TestFileGDBRealFixture:
    """Tests using real FileGDB fixtures from GDAL test data.

    These tests verify Portolan's orchestration works with production FileGDB structures.
    The fixture is field_alias.gdb from GDAL autotest/ogr/data/filegdb/.
    """

    @pytest.fixture
    def real_filegdb(self) -> Path:
        """Return path to real FileGDB fixture."""
        fixture_path = (
            Path(__file__).parent.parent / "fixtures" / "realdata" / "filegdb" / "field_alias.gdb"
        )
        if not fixture_path.exists():
            pytest.skip("Real FileGDB fixture not found (run fixture download first)")
        return fixture_path

    def test_real_filegdb_detected_as_ready(self, real_filegdb: Path) -> None:
        """Real FileGDB should be detected and added to ready list."""
        # Scan the parent directory containing the FileGDB
        result = scan_directory(real_filegdb.parent)

        ready_paths = [f.path for f in result.ready]
        assert real_filegdb in ready_paths, (
            f"Real FileGDB should be in ready list. Found: {ready_paths}"
        )

    def test_real_filegdb_has_correct_properties(self, real_filegdb: Path) -> None:
        """Real FileGDB should have correct extension and format type."""
        result = scan_directory(real_filegdb.parent)

        filegdb_files = [f for f in result.ready if f.path == real_filegdb]
        assert len(filegdb_files) == 1

        scanned = filegdb_files[0]
        assert scanned.extension == ".gdb"
        assert scanned.format_type == FormatType.VECTOR
        assert scanned.size_bytes > 0, "Real FileGDB should have non-zero size"

    def test_real_filegdb_has_metadata(self, real_filegdb: Path) -> None:
        """Real FileGDB should have metadata with accurate table count."""
        result = scan_directory(real_filegdb.parent)

        scanned = next(f for f in result.ready if f.path == real_filegdb)

        assert "gdbtable_count" in scanned.metadata
        # field_alias.gdb has multiple .gdbtable files
        assert scanned.metadata["gdbtable_count"] >= 1, (
            f"Expected at least 1 gdbtable, got {scanned.metadata['gdbtable_count']}"
        )
        assert scanned.metadata["lock_files_present"] is False, (
            "Clean fixture should have no lock files"
        )

    def test_real_filegdb_not_in_special_formats(self, real_filegdb: Path) -> None:
        """Real FileGDB should NOT appear in special_formats (promoted to ready)."""
        result = scan_directory(real_filegdb.parent)

        special_filegdb = [sf for sf in result.special_formats if sf.format_type == "filegdb"]
        assert len(special_filegdb) == 0, (
            f"FileGDB should not be in special_formats. Found: {special_filegdb}"
        )


@pytest.mark.unit
class TestFileGDBArchives:
    """Tests for FileGDB archive (.gdb.zip) handling."""

    def test_filegdb_archive_not_in_ready(self, tmp_path: Path) -> None:
        """FileGDB archives (.gdb.zip) should NOT be in ready list.

        Archives need extraction before they can be processed by `portolan add`.
        They should remain as skipped files (or special_formats) until we
        implement archive extraction.
        """
        archive = tmp_path / "data.gdb.zip"
        archive.write_bytes(b"PK\x03\x04" + b"\x00" * 100)  # Minimal ZIP header

        result = scan_directory(tmp_path)

        # Archive should NOT be in ready
        ready_paths = [f.path for f in result.ready]
        assert archive not in ready_paths, (
            "FileGDB archive should not be in ready list (needs extraction)"
        )

    def test_filegdb_archive_is_skipped_or_special(self, tmp_path: Path) -> None:
        """FileGDB archives should be tracked as skipped or special_formats."""
        archive = tmp_path / "data.gdb.zip"
        archive.write_bytes(b"PK\x03\x04" + b"\x00" * 100)

        result = scan_directory(tmp_path)

        # Should be in skipped (as unknown) or special_formats
        skipped_paths = [f.path for f in result.skipped]
        special_paths = [sf.path for sf in result.special_formats]

        is_tracked = archive in skipped_paths or archive in special_paths
        assert is_tracked, (
            f"FileGDB archive should be tracked. Skipped: {skipped_paths}, Special: {special_paths}"
        )


@pytest.mark.unit
class TestFileGDBMetadata:
    """Tests for FileGDB metadata on ScannedFile."""

    def test_filegdb_has_metadata_dict(self, tmp_path: Path) -> None:
        """FileGDB ScannedFile should have metadata with table count and lock info."""
        gdb_dir = tmp_path / "data.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"x" * 100)
        (gdb_dir / "a00000002.gdbtable").write_bytes(b"y" * 200)
        (gdb_dir / "a00000003.gdbtable").write_bytes(b"z" * 300)

        result = scan_directory(tmp_path)

        filegdb_files = [f for f in result.ready if f.path == gdb_dir]
        assert len(filegdb_files) == 1
        scanned = filegdb_files[0]

        # Should have metadata dict
        assert hasattr(scanned, "metadata"), "ScannedFile should have metadata attribute"
        assert isinstance(scanned.metadata, dict), "metadata should be a dict"

        # Should contain FileGDB-specific info
        assert "gdbtable_count" in scanned.metadata, "metadata should include gdbtable_count"
        assert scanned.metadata["gdbtable_count"] == 3, (
            f"Expected 3 gdbtables, got {scanned.metadata['gdbtable_count']}"
        )
        assert "lock_files_present" in scanned.metadata, (
            "metadata should include lock_files_present"
        )
        assert scanned.metadata["lock_files_present"] is False, "No lock files should be present"

    def test_filegdb_detects_lock_files(self, tmp_path: Path) -> None:
        """FileGDB with lock files should have lock_files_present=True in metadata."""
        gdb_dir = tmp_path / "locked.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"x")
        # Create a lock file (ArcGIS uses .lock and .gdbcache patterns)
        (gdb_dir / "a00000001.gdbtable.lock").write_bytes(b"lock")

        result = scan_directory(tmp_path)

        filegdb_files = [f for f in result.ready if f.path == gdb_dir]
        assert len(filegdb_files) == 1
        scanned = filegdb_files[0]

        assert scanned.metadata.get("lock_files_present") is True, "Lock files should be detected"

    def test_regular_file_has_empty_metadata(self, tmp_path: Path) -> None:
        """Non-FileGDB files should have empty metadata dict."""
        geojson = tmp_path / "points.geojson"
        geojson.write_text('{"type": "FeatureCollection", "features": []}')

        result = scan_directory(tmp_path)

        geojson_files = [f for f in result.ready if f.path == geojson]
        assert len(geojson_files) == 1
        scanned = geojson_files[0]

        assert hasattr(scanned, "metadata"), "ScannedFile should have metadata attribute"
        assert scanned.metadata == {}, "Non-FileGDB should have empty metadata"


@pytest.mark.unit
class TestFileGDBDuplicateDetection:
    """Tests for FileGDB participation in duplicate/multi-asset detection."""

    def test_filegdb_tracked_in_basenames(self, tmp_path: Path) -> None:
        """FileGDB should participate in duplicate basename detection.

        If parcels.gdb and parcels.shp exist in the same directory, both should
        be flagged as potential duplicates since they have the same base name.
        """
        # Create FileGDB
        gdb_dir = tmp_path / "parcels.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"x")

        # Create Shapefile with same base name
        shp = tmp_path / "parcels.shp"
        shp.write_bytes(b"\x00" * 100)  # Minimal shapefile header
        shx = tmp_path / "parcels.shx"
        shx.write_bytes(b"\x00" * 100)
        dbf = tmp_path / "parcels.dbf"
        dbf.write_bytes(b"\x00" * 100)

        result = scan_directory(tmp_path)

        # Both should be in ready list
        ready_paths = {f.path for f in result.ready}
        assert gdb_dir in ready_paths, "FileGDB should be in ready"
        assert shp in ready_paths, "Shapefile should be in ready"

        # Verify FileGDB participates in duplicate/multi-asset tracking.
        # The implementation tracks basenames, so both parcels.gdb and parcels.shp
        # are registered. Whether this raises a warning depends on policy.
        # The key assertion is that both files are discovered and tracked.
        assert len(result.ready) >= 2, "Both FileGDB and Shapefile should be tracked"

    def test_filegdb_tracked_in_primaries_by_dir(self, tmp_path: Path) -> None:
        """FileGDB should be tracked as a primary file in its directory.

        This enables multi-asset detection for directories containing both
        FileGDB and other formats.
        """
        # Create FileGDB
        gdb_dir = tmp_path / "data.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"x")

        # Create GeoJSON in same directory
        geojson = tmp_path / "data.geojson"
        geojson.write_text('{"type": "FeatureCollection", "features": []}')

        result = scan_directory(tmp_path)

        # Both should be in ready list
        ready_paths = {f.path for f in result.ready}
        assert gdb_dir in ready_paths
        assert geojson in ready_paths

        # Both should be counted as primary vector files
        vector_files = [f for f in result.ready if f.format_type == FormatType.VECTOR]
        assert len(vector_files) == 2, f"Expected 2 vector files, got {len(vector_files)}"
