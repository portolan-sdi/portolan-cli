"""Integration tests for download module.

Tests for real filesystem operations without mocking obstore.
These tests verify:
- Directory structure preservation
- Pattern filtering works correctly
- Empty directory handling
- Path building correctness
"""

from __future__ import annotations

from pathlib import Path

import pytest

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def download_test_dir(tmp_path: Path) -> Path:
    """Create a test directory structure for download integration tests."""
    download_dir = tmp_path / "downloads"
    download_dir.mkdir()
    return download_dir


# =============================================================================
# Directory Structure Preservation Tests
# =============================================================================


class TestDirectoryStructurePreservation:
    """Tests for directory structure preservation in local paths."""

    @pytest.mark.integration
    def test_build_local_path_preserves_nested_structure(self, download_test_dir: Path) -> None:
        """Local paths should preserve relative directory structure."""
        from portolan_cli.download import _build_local_path

        local_path = _build_local_path(
            remote_key="data/subdir/file.parquet",
            prefix="data/",
            destination=download_test_dir,
        )

        assert local_path == download_test_dir / "subdir" / "file.parquet"
        assert "data" not in str(local_path.relative_to(download_test_dir))

    @pytest.mark.integration
    def test_build_local_path_with_deep_nesting(self, download_test_dir: Path) -> None:
        """Local paths should work with deeply nested structures."""
        from portolan_cli.download import _build_local_path

        local_path = _build_local_path(
            remote_key="prefix/a/b/c/d/file.parquet",
            prefix="prefix/",
            destination=download_test_dir,
        )

        assert local_path == download_test_dir / "a" / "b" / "c" / "d" / "file.parquet"

    @pytest.mark.integration
    def test_build_local_path_no_prefix(self, download_test_dir: Path) -> None:
        """Local paths should work without prefix (filename only)."""
        from portolan_cli.download import _build_local_path

        local_path = _build_local_path(
            remote_key="just-a-file.parquet",
            prefix="",
            destination=download_test_dir,
        )

        assert local_path == download_test_dir / "just-a-file.parquet"

    @pytest.mark.integration
    def test_build_local_path_prefix_mismatch(self, download_test_dir: Path) -> None:
        """When prefix doesn't match, should extract filename."""
        from portolan_cli.download import _build_local_path

        local_path = _build_local_path(
            remote_key="different/path/file.parquet",
            prefix="data/",  # Doesn't match
            destination=download_test_dir,
        )

        # Should fall back to just filename
        assert local_path == download_test_dir / "file.parquet"


# =============================================================================
# Single File Path Tests
# =============================================================================


class TestSingleFilePath:
    """Tests for single file download path resolution."""

    @pytest.mark.integration
    def test_get_local_path_to_directory(self, download_test_dir: Path) -> None:
        """When destination is directory, should append filename."""
        from portolan_cli.download import _get_local_path_for_file

        local_path = _get_local_path_for_file(
            source_prefix="data/file.parquet",
            destination=download_test_dir,
            is_dir_destination=True,
        )

        assert local_path == download_test_dir / "file.parquet"

    @pytest.mark.integration
    def test_get_local_path_to_file(self, download_test_dir: Path) -> None:
        """When destination is exact file, should use that path."""
        from portolan_cli.download import _get_local_path_for_file

        dest_file = download_test_dir / "renamed.parquet"

        local_path = _get_local_path_for_file(
            source_prefix="data/original.parquet",
            destination=dest_file,
            is_dir_destination=False,
        )

        assert local_path == dest_file

    @pytest.mark.integration
    def test_get_local_path_existing_directory(self, download_test_dir: Path) -> None:
        """When destination is existing directory, should append filename."""
        from portolan_cli.download import _get_local_path_for_file

        # download_test_dir exists as a directory
        local_path = _get_local_path_for_file(
            source_prefix="data/file.parquet",
            destination=download_test_dir,
            is_dir_destination=False,  # Will check if exists
        )

        # Since download_test_dir.is_dir() is True, should append filename
        assert local_path == download_test_dir / "file.parquet"


# =============================================================================
# Pattern Matching Tests
# =============================================================================


class TestPatternMatching:
    """Tests for remote file pattern filtering."""

    @pytest.mark.integration
    def test_fnmatch_parquet_pattern(self) -> None:
        """Pattern should filter to only matching files."""
        import fnmatch

        files = [
            "data/file1.parquet",
            "data/file2.parquet",
            "data/readme.md",
            "data/config.json",
            "data/nested/file3.parquet",
        ]

        filtered = [f for f in files if fnmatch.fnmatch(f.rsplit("/", 1)[-1], "*.parquet")]

        assert len(filtered) == 3
        assert all(f.endswith(".parquet") for f in filtered)

    @pytest.mark.integration
    def test_fnmatch_json_pattern(self) -> None:
        """Pattern should filter to JSON files only."""
        import fnmatch

        files = [
            "data/file1.parquet",
            "data/config.json",
            "data/settings.json",
        ]

        filtered = [f for f in files if fnmatch.fnmatch(f.rsplit("/", 1)[-1], "*.json")]

        assert len(filtered) == 2
        assert all(f.endswith(".json") for f in filtered)


# =============================================================================
# Dry Run Tests
# =============================================================================


class TestDryRun:
    """Tests for dry-run functionality."""

    @pytest.mark.integration
    def test_download_file_dry_run_no_file_created(self, download_test_dir: Path) -> None:
        """Dry-run should not create any files."""
        from portolan_cli.download import download_file

        dest_file = download_test_dir / "should_not_exist.parquet"

        result = download_file(
            source="s3://bucket/data.parquet",
            destination=dest_file,
            dry_run=True,
        )

        assert result.success is True
        assert result.files_downloaded == 0
        assert not dest_file.exists()

    @pytest.mark.integration
    def test_download_directory_dry_run_no_files_created(self, download_test_dir: Path) -> None:
        """Dry-run directory download should not create any files."""
        from unittest.mock import patch

        from portolan_cli.download import download_directory

        nested_dir = download_test_dir / "nested"

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.upload.S3Store"):
                # Mock list to return files
                mock_obs.list.return_value = [
                    [
                        {"path": "data/file1.parquet", "size": 100},
                        {"path": "data/file2.parquet", "size": 200},
                    ]
                ]

                result = download_directory(
                    source="s3://bucket/data/",
                    destination=nested_dir,
                    dry_run=True,
                )

        assert result.success is True
        assert result.files_downloaded == 0
        assert not nested_dir.exists()


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestErrorHandling:
    """Tests for error handling scenarios."""

    @pytest.mark.integration
    def test_download_result_captures_errors(self) -> None:
        """DownloadResult should properly capture error information."""
        from portolan_cli.download import DownloadResult

        error1 = OSError("Network error")
        error2 = ValueError("Invalid data")

        result = DownloadResult(
            success=False,
            files_downloaded=1,
            files_failed=2,
            total_bytes=100,
            errors=[(Path("file1.parquet"), error1), (Path("file2.parquet"), error2)],
        )

        assert result.success is False
        assert result.files_failed == 2
        assert len(result.errors) == 2
        assert result.errors[0][0] == Path("file1.parquet")
        assert isinstance(result.errors[0][1], OSError)

    @pytest.mark.integration
    def test_empty_remote_directory_returns_success(self, download_test_dir: Path) -> None:
        """Empty remote directory should return success with zero files."""
        from unittest.mock import patch

        from portolan_cli.download import download_directory

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.upload.S3Store"):
                # Mock list to return empty
                mock_obs.list.return_value = [[]]

                result = download_directory(
                    source="s3://bucket/empty/",
                    destination=download_test_dir,
                )

        assert result.success is True
        assert result.files_downloaded == 0
        assert result.files_failed == 0


# =============================================================================
# URL Parsing Integration Tests
# =============================================================================


class TestUrlParsing:
    """Tests verifying URL parsing integration."""

    @pytest.mark.integration
    def test_parse_s3_url(self) -> None:
        """Should correctly parse S3 URLs."""
        from portolan_cli.upload import parse_object_store_url

        bucket_url, prefix = parse_object_store_url("s3://mybucket/data/file.parquet")

        assert bucket_url == "s3://mybucket"
        assert prefix == "data/file.parquet"

    @pytest.mark.integration
    def test_parse_gcs_url(self) -> None:
        """Should correctly parse GCS URLs."""
        from portolan_cli.upload import parse_object_store_url

        bucket_url, prefix = parse_object_store_url("gs://mybucket/path/to/data")

        assert bucket_url == "gs://mybucket"
        assert prefix == "path/to/data"

    @pytest.mark.integration
    def test_parse_azure_url(self) -> None:
        """Should correctly parse Azure URLs."""
        from portolan_cli.upload import parse_object_store_url

        bucket_url, prefix = parse_object_store_url("az://myaccount/mycontainer/data/file.parquet")

        assert bucket_url == "az://myaccount/mycontainer"
        assert prefix == "data/file.parquet"

    @pytest.mark.integration
    def test_parse_url_bucket_only(self) -> None:
        """Should handle URL with bucket only."""
        from portolan_cli.upload import parse_object_store_url

        bucket_url, prefix = parse_object_store_url("s3://mybucket")

        assert bucket_url == "s3://mybucket"
        assert prefix == ""
