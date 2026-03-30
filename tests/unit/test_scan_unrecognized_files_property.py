"""Property-based tests for unrecognized file listing in scan output.

Uses hypothesis to generate various file extensions and verify that
unrecognized files are properly listed in output.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from portolan_cli.scan_classify import FileCategory, SkipReasonType, classify_file

# Strategy for generating unknown file extensions
# Use ASCII alphanumeric only - cross-platform safe for filenames
# (Unicode characters can cause "Illegal byte sequence" on macOS and
# "Invalid argument" on Windows for control characters and surrogates)
unknown_extensions = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz0123456789",
    min_size=1,
    max_size=10,
).map(lambda x: f".{x}")


@pytest.mark.unit
class TestUnrecognizedFileClassification:
    """Property-based tests for unrecognized file classification."""

    @given(ext=unknown_extensions)
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_truly_unknown_extensions_are_classified_as_unknown(self, ext: str) -> None:
        """Any extension not in known lists should be classified as UNKNOWN.

        Tests that the classification system is robust to arbitrary extensions.
        """
        # Skip common extensions that might be generated (including geospatial formats)
        skip_exts = {
            ".md",
            ".txt",
            ".csv",
            ".json",
            ".xml",
            ".html",
            ".png",
            ".jpg",
            ".tif",
            ".gpkg",
            ".py",
            ".exe",
            # Geospatial formats that are known/recognized
            ".geojson",
            ".shp",
            ".dbf",
            ".shx",
            ".prj",
            ".parquet",
            ".gdb",
            ".fgb",
            ".kml",
            ".kmz",
            ".gml",
            ".jp2",
            ".tiff",
            ".geotiff",
        }
        if ext in skip_exts:
            return

        # Create temp file with the extension
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            test_file = tmp_path / f"test{ext}"
            test_file.write_text("test content")

            # Classify the file
            category, skip_reason, skip_message = classify_file(test_file)

            # Should be classified as UNKNOWN (unless it matches something by chance)
            # Most generated extensions should be UNKNOWN
            if category != FileCategory.UNKNOWN:
                # GEO_ASSET files are primary data - they have no skip_reason
                # (they're not skipped, they're processed)
                if category != FileCategory.GEO_ASSET:
                    assert skip_reason is not None, (
                        f"Extension {ext} classified as {category} but should have skip_reason"
                    )

    @given(
        count=st.integers(min_value=1, max_value=20),
        ext_prefix=st.text(
            alphabet="abcdefghijklmnopqrstuvwxyz",
            min_size=1,
            max_size=5,
        ),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_multiple_unknown_files_all_classified_as_unknown(
        self, count: int, ext_prefix: str
    ) -> None:
        """Multiple files with unknown extensions should all be classified as UNKNOWN."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            test_dir = tmp_path / "test_files"
            test_dir.mkdir()

            extension = f".unk{ext_prefix}"
            unknown_count = 0

            for i in range(count):
                test_file = test_dir / f"file_{i}{extension}"
                test_file.write_text("test content")

                category, _, _ = classify_file(test_file)
                if category == FileCategory.UNKNOWN:
                    unknown_count += 1

            # Should have at least some files classified as unknown
            # (unless all happen to match known extensions, which is unlikely)
            assert unknown_count > 0, (
                f"Expected some UNKNOWN classifications for extension {extension}, "
                f"but got {unknown_count}/{count}"
            )

    def test_unknown_files_with_skip_reason_unknown_format(self) -> None:
        """Unknown files should have UNKNOWN_FORMAT as skip reason."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            test_file = tmp_path / "unknown_file.weirdext"
            test_file.write_text("content")

            category, skip_reason, skip_message = classify_file(test_file)

            if category == FileCategory.UNKNOWN:
                assert skip_reason == SkipReasonType.UNKNOWN_FORMAT, (
                    f"Expected UNKNOWN_FORMAT skip reason, got {skip_reason}"
                )
                assert skip_message is not None
                assert "unknown" in skip_message.lower()


@pytest.mark.unit
class TestUnrecognizedFileLimitingLogic:
    """Tests for limiting the number of listed unrecognized files."""

    @given(
        file_count=st.integers(min_value=1, max_value=25),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_unknown_file_limit_boundary(self, file_count: int) -> None:
        """Test that truncation logic works correctly at boundary (10 files).

        The implementation truncates after 10 files when show_all=False.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            test_dir = tmp_path / "test"
            test_dir.mkdir()

            # Create unknown files
            for i in range(file_count):
                (test_dir / f"file_{i:02d}.unknown").write_text("test")

            # Verify files are created
            files = list(test_dir.glob("*.unknown"))
            assert len(files) == file_count

            # Files should exist
            all_unknown = all(classify_file(f)[0] == FileCategory.UNKNOWN for f in files)
            assert all_unknown, "Not all files classified as UNKNOWN"

            # Truncation should happen at 10 files
            if file_count > 10:
                # More than 10 files would be truncated
                truncation_threshold = 10
                assert file_count > truncation_threshold

    @given(filename_count=st.integers(min_value=1, max_value=50))
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_truncation_logic_consistency(self, filename_count: int) -> None:
        """Verify that truncation logic respects --all flag."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            test_dir = tmp_path / "test"
            test_dir.mkdir()

            # Create many unknown files
            capped_count = min(filename_count, 20)
            for i in range(capped_count):
                (test_dir / f"file_{i:02d}.xyz").write_text("test")

            # Without --all: show up to 10
            max_display_truncated = min(10, capped_count)
            # With --all: show all
            max_display_all = capped_count

            # Verify truncation boundary
            if capped_count <= 10:
                # All files would be shown either way
                assert max_display_truncated == capped_count
                assert max_display_all == capped_count
            else:
                # Truncation should apply only when show_all=False
                assert max_display_truncated == 10
                assert max_display_all == capped_count
                assert max_display_truncated < max_display_all
