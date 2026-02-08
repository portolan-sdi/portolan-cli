"""Integration tests for upload module.

Tests for real filesystem operations without mocking obstore.
These tests verify:
- Directory structure preservation
- Pattern filtering works correctly
- Empty directory handling
- File existence validation
"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def upload_test_dir(tmp_path: Path) -> Path:
    """Create a test directory structure for upload integration tests."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    # Create some data files
    (data_dir / "file1.parquet").write_bytes(b"parquet content 1")
    (data_dir / "file2.parquet").write_bytes(b"parquet content 2")

    # Create nested structure
    nested = data_dir / "subdir"
    nested.mkdir()
    (nested / "file3.parquet").write_bytes(b"parquet content 3")

    # Create non-parquet files
    (data_dir / "readme.md").write_text("# README")
    (data_dir / "config.json").write_text('{"key": "value"}')

    return data_dir


class TestDirectoryStructurePreservation:
    """Tests for directory structure preservation in target keys."""

    @pytest.mark.integration
    def test_build_target_key_preserves_nested_structure(self, upload_test_dir: Path) -> None:
        """Target keys should preserve relative directory structure."""
        from portolan_cli.upload import _build_target_key

        nested_file = upload_test_dir / "subdir" / "file3.parquet"

        key = _build_target_key(nested_file, upload_test_dir, "output")

        assert key == "output/subdir/file3.parquet"
        assert "data" not in key  # Source dir name should not appear

    @pytest.mark.integration
    def test_build_target_key_with_deep_nesting(self, tmp_path: Path) -> None:
        """Target keys should work with deeply nested structures."""
        from portolan_cli.upload import _build_target_key

        # Create deep nesting
        deep_path = tmp_path / "a" / "b" / "c" / "d"
        deep_path.mkdir(parents=True)
        file_path = deep_path / "file.parquet"
        file_path.touch()

        key = _build_target_key(file_path, tmp_path, "prefix")

        assert key == "prefix/a/b/c/d/file.parquet"


class TestPatternFiltering:
    """Tests for file pattern filtering."""

    @pytest.mark.integration
    def test_rglob_parquet_pattern(self, upload_test_dir: Path) -> None:
        """Pattern should filter to only matching files."""
        files = list(upload_test_dir.rglob("*.parquet"))
        files = [f for f in files if f.is_file()]

        assert len(files) == 3
        assert all(f.suffix == ".parquet" for f in files)

    @pytest.mark.integration
    def test_rglob_json_pattern(self, upload_test_dir: Path) -> None:
        """Pattern should filter to JSON files only."""
        files = list(upload_test_dir.rglob("*.json"))
        files = [f for f in files if f.is_file()]

        assert len(files) == 1
        assert files[0].name == "config.json"

    @pytest.mark.integration
    def test_rglob_all_files(self, upload_test_dir: Path) -> None:
        """No pattern should find all files."""
        files = list(upload_test_dir.rglob("*"))
        files = [f for f in files if f.is_file()]

        assert len(files) == 5  # 3 parquet + 1 md + 1 json


class TestEmptyDirectoryHandling:
    """Tests for empty directory handling."""

    @pytest.mark.integration
    def test_empty_directory_returns_success(self, tmp_path: Path) -> None:
        """Empty directory should return success with zero files."""
        from portolan_cli.upload import upload_directory

        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        result = upload_directory(
            source=empty_dir,
            destination="s3://bucket/path/",
            dry_run=True,  # Use dry_run to avoid needing credentials
        )

        assert result.success is True
        assert result.files_uploaded == 0
        assert result.files_failed == 0

    @pytest.mark.integration
    def test_directory_with_no_matching_pattern(self, upload_test_dir: Path) -> None:
        """Directory with no matching files should return success."""
        from portolan_cli.upload import upload_directory

        result = upload_directory(
            source=upload_test_dir,
            destination="s3://bucket/path/",
            pattern="*.nonexistent",
            dry_run=True,
        )

        assert result.success is True
        assert result.files_uploaded == 0


class TestFileValidation:
    """Tests for file existence and type validation."""

    @pytest.mark.integration
    def test_upload_file_nonexistent_raises(self, tmp_path: Path) -> None:
        """Uploading a nonexistent file should raise FileNotFoundError."""
        from portolan_cli.upload import upload_file

        nonexistent = tmp_path / "does_not_exist.parquet"

        with pytest.raises(FileNotFoundError, match="Source file not found"):
            upload_file(source=nonexistent, destination="s3://bucket/file.parquet")

    @pytest.mark.integration
    def test_upload_file_directory_raises(self, upload_test_dir: Path) -> None:
        """Uploading a directory as a file should raise ValueError."""
        from portolan_cli.upload import upload_file

        with pytest.raises(ValueError, match="Source is not a file"):
            upload_file(source=upload_test_dir, destination="s3://bucket/path")

    @pytest.mark.integration
    def test_upload_directory_file_raises(self, tmp_path: Path) -> None:
        """Uploading a file as directory should raise ValueError."""
        from portolan_cli.upload import upload_directory

        file_path = tmp_path / "file.txt"
        file_path.write_text("content")

        with pytest.raises(ValueError, match="Source is not a directory"):
            upload_directory(source=file_path, destination="s3://bucket/path/")


class TestUrlParsing:
    """Integration tests for URL parsing with real URLs."""

    @pytest.mark.integration
    def test_s3_url_with_complex_prefix(self) -> None:
        """S3 URL with complex prefix should parse correctly."""
        from portolan_cli.upload import parse_object_store_url

        bucket_url, prefix = parse_object_store_url("s3://mybucket/path/to/deeply/nested/data/")

        assert bucket_url == "s3://mybucket"
        assert prefix == "path/to/deeply/nested/data/"

    @pytest.mark.integration
    def test_azure_url_complex(self) -> None:
        """Azure URL with path should parse correctly."""
        from portolan_cli.upload import parse_object_store_url

        bucket_url, prefix = parse_object_store_url("az://storageaccount/container/data/path")

        assert bucket_url == "az://storageaccount/container"
        assert prefix == "data/path"


class TestDryRunMode:
    """Integration tests for dry-run mode."""

    @pytest.mark.integration
    def test_dry_run_file_does_not_require_credentials(self, tmp_path: Path) -> None:
        """Dry-run should work without valid credentials."""
        from portolan_cli.upload import upload_file

        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"test content")

        # This should succeed even without AWS credentials
        result = upload_file(
            source=test_file,
            destination="s3://bucket/test.parquet",
            dry_run=True,
        )

        assert result.success is True
        assert result.files_uploaded == 0

    @pytest.mark.integration
    def test_dry_run_directory_does_not_require_credentials(self, upload_test_dir: Path) -> None:
        """Dry-run directory upload should work without credentials."""
        from portolan_cli.upload import upload_directory

        result = upload_directory(
            source=upload_test_dir,
            destination="s3://bucket/data/",
            dry_run=True,
        )

        assert result.success is True
        assert result.files_uploaded == 0


class TestCredentialChecking:
    """Integration tests for credential checking."""

    @pytest.mark.integration
    def test_check_credentials_with_real_env(self) -> None:
        """Credential checking should work with real environment."""
        import os

        from portolan_cli.upload import check_credentials

        # Save original env vars
        original_access = os.environ.get("AWS_ACCESS_KEY_ID")
        original_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")

        try:
            # Clear env vars to test hint generation
            if original_access:
                del os.environ["AWS_ACCESS_KEY_ID"]
            if original_secret:
                del os.environ["AWS_SECRET_ACCESS_KEY"]

            # Should return helpful hints
            valid, hint = check_credentials("s3://bucket/path")

            # Either credentials exist from file or we get hints
            if not valid:
                assert "AWS_ACCESS_KEY_ID" in hint or "aws configure" in hint.lower()
        finally:
            # Restore original env vars
            if original_access:
                os.environ["AWS_ACCESS_KEY_ID"] = original_access
            if original_secret:
                os.environ["AWS_SECRET_ACCESS_KEY"] = original_secret
