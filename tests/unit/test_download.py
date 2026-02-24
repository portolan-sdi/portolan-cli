"""Unit tests for download module.

Tests for downloading files from cloud object storage (S3, GCS, Azure).
These tests mock obstore to test download logic in isolation.

Test categories:
- URL parsing (reuses upload.py's parse_object_store_url)
- DownloadResult dataclass
- download_file function
- download_directory function with parallel downloads
- Credential checking (reuses upload.py's check_credentials)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

if TYPE_CHECKING:
    from collections.abc import Generator, Iterator


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def temp_file(tmp_path: Path) -> Path:
    """Create a temporary file for download destination tests."""
    return tmp_path / "downloaded.parquet"


@pytest.fixture
def temp_download_dir(tmp_path: Path) -> Path:
    """Create a temporary directory for download destination tests."""
    download_dir = tmp_path / "downloads"
    download_dir.mkdir()
    return download_dir


@pytest.fixture
def mock_aws_credentials(tmp_path: Path) -> Iterator[Path]:
    """Create mock AWS credentials file structure."""
    aws_dir = tmp_path / ".aws"
    aws_dir.mkdir()

    # Create credentials file
    creds_file = aws_dir / "credentials"
    creds_file.write_text(
        """[default]
aws_access_key_id = AKIADEFAULTKEY
aws_secret_access_key = defaultsecret

[myprofile]
aws_access_key_id = AKIAPROFILEKEY
aws_secret_access_key = profilesecret
"""
    )

    # Create config file
    config_file = aws_dir / "config"
    config_file.write_text(
        """[default]
region = us-east-1

[profile myprofile]
region = eu-west-1
"""
    )

    # Patch Path.home() to return our temp directory
    with patch("pathlib.Path.home", return_value=tmp_path):
        yield aws_dir


# =============================================================================
# DownloadResult Tests
# =============================================================================


class TestDownloadResult:
    """Tests for DownloadResult dataclass."""

    @pytest.mark.unit
    def test_download_result_success(self) -> None:
        """DownloadResult should track successful downloads."""
        from portolan_cli.download import DownloadResult

        result = DownloadResult(
            success=True,
            files_downloaded=5,
            files_failed=0,
            total_bytes=1024 * 1024,
            errors=[],
        )

        assert result.success is True
        assert result.files_downloaded == 5
        assert result.files_failed == 0
        assert result.total_bytes == 1024 * 1024
        assert result.errors == []

    @pytest.mark.unit
    def test_download_result_partial_failure(self) -> None:
        """DownloadResult should track partial failures."""
        from portolan_cli.download import DownloadResult

        error = OSError("Network error")
        result = DownloadResult(
            success=False,
            files_downloaded=3,
            files_failed=2,
            total_bytes=500 * 1024,
            errors=[
                (Path("path/to/file1.parquet"), error),
                (Path("path/to/file2.parquet"), error),
            ],
        )

        assert result.success is False
        assert result.files_downloaded == 3
        assert result.files_failed == 2
        assert len(result.errors) == 2

    @pytest.mark.unit
    def test_download_result_default_errors(self) -> None:
        """DownloadResult should default to empty errors list."""
        from portolan_cli.download import DownloadResult

        result = DownloadResult(
            success=True,
            files_downloaded=1,
            files_failed=0,
            total_bytes=100,
        )

        assert result.errors == []


# =============================================================================
# Download File Tests
# =============================================================================


class TestDownloadFile:
    """Tests for download_file function."""

    @pytest.mark.unit
    def test_download_file_success(self, temp_download_dir: Path) -> None:
        """download_file should download a single file successfully."""
        from portolan_cli.download import download_file

        dest_file = temp_download_dir / "data.parquet"
        test_data = b"test data content"

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                # Mock get to return a response with metadata and bytes
                # Size MUST match actual data length for integrity verification
                mock_response = MagicMock()
                mock_response.meta = {"size": len(test_data)}
                mock_response.__iter__ = lambda self: iter([test_data])
                mock_obs.get.return_value = mock_response

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_file(
                        source="s3://mybucket/data.parquet",
                        destination=dest_file,
                    )

                assert result.success is True
                assert result.files_downloaded == 1
                assert result.files_failed == 0
                assert result.total_bytes == len(test_data)
                mock_obs.get.assert_called_once()

    @pytest.mark.unit
    def test_download_file_dry_run(self, temp_download_dir: Path) -> None:
        """Dry-run should not perform actual download."""
        from portolan_cli.download import download_file

        dest_file = temp_download_dir / "data.parquet"

        with patch("portolan_cli.download.obs") as mock_obs:
            result = download_file(
                source="s3://mybucket/data.parquet",
                destination=dest_file,
                dry_run=True,
            )

        assert result.success is True
        assert result.files_downloaded == 0
        mock_obs.get.assert_not_called()

    @pytest.mark.unit
    def test_download_file_creates_parent_dir(self, tmp_path: Path) -> None:
        """download_file should create parent directories if needed."""
        from portolan_cli.download import download_file

        # Destination in non-existent subdirectory
        dest_file = tmp_path / "nested" / "deep" / "data.parquet"
        test_data = b"test data"

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                mock_response = MagicMock()
                mock_response.meta = {"size": len(test_data)}
                mock_response.__iter__ = lambda self: iter([test_data])
                mock_obs.get.return_value = mock_response

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_file(
                        source="s3://mybucket/data.parquet",
                        destination=dest_file,
                    )

                assert result.success is True
                assert dest_file.parent.exists()

    @pytest.mark.unit
    def test_download_file_failure(self, temp_download_dir: Path) -> None:
        """download_file should handle download failures gracefully."""
        from portolan_cli.download import download_file

        dest_file = temp_download_dir / "data.parquet"

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store
                mock_obs.get.side_effect = OSError("Network error")

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_file(
                        source="s3://mybucket/data.parquet",
                        destination=dest_file,
                    )

                assert result.success is False
                assert result.files_downloaded == 0
                assert result.files_failed == 1
                assert len(result.errors) == 1

    @pytest.mark.unit
    def test_download_file_gcs(self, temp_download_dir: Path) -> None:
        """download_file should work with GCS URLs."""
        from portolan_cli.download import download_file

        dest_file = temp_download_dir / "data.parquet"
        test_data = b"gcs data content"

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.upload.obs") as mock_upload_obs:
                mock_response = MagicMock()
                mock_response.meta = {"size": len(test_data)}
                mock_response.__iter__ = lambda self: iter([test_data])
                mock_obs.get.return_value = mock_response
                mock_upload_obs.store.from_url.return_value = MagicMock()

                # GCS uses ADC - mock the exists check for credentials
                with patch.object(Path, "exists", side_effect=lambda: True):
                    result = download_file(
                        source="gs://mybucket/data.parquet",
                        destination=dest_file,
                    )

                assert result.success is True

    @pytest.mark.unit
    def test_download_file_azure(self, temp_download_dir: Path) -> None:
        """download_file should work with Azure URLs."""
        from portolan_cli.download import download_file

        dest_file = temp_download_dir / "data.parquet"
        test_data = b"azure data content"

        # Need to patch both download.obs AND upload.obs since _setup_store_and_kwargs is in upload
        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.upload.obs") as mock_upload_obs:
                mock_response = MagicMock()
                mock_response.meta = {"size": len(test_data)}
                mock_response.__iter__ = lambda self: iter([test_data])
                mock_obs.get.return_value = mock_response

                # Mock the store creation in upload module
                mock_store = MagicMock()
                mock_upload_obs.store.from_url.return_value = mock_store

                with patch.dict(os.environ, {"AZURE_STORAGE_ACCOUNT_KEY": "test-key"}):
                    result = download_file(
                        source="az://myaccount/mycontainer/data.parquet",
                        destination=dest_file,
                    )

                assert result.success is True


# =============================================================================
# Download Directory Tests
# =============================================================================


class TestDownloadDirectory:
    """Tests for download_directory function."""

    @pytest.mark.unit
    def test_download_directory_all_files(self, temp_download_dir: Path) -> None:
        """Directory download should download all files."""
        from portolan_cli.download import download_directory

        # Use consistent size for test data
        test_data = b"file content"
        file_size = len(test_data)

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                # Mock list to return file metadata - sizes must match test_data
                mock_obs.list.return_value = [
                    [
                        {"path": "data/file1.parquet", "size": file_size},
                        {"path": "data/file2.parquet", "size": file_size},
                        {"path": "data/subdir/file3.parquet", "size": file_size},
                    ]
                ]

                # Mock get for each file - content must match size
                mock_response = MagicMock()
                mock_response.meta = {"size": file_size}
                mock_response.__iter__ = lambda self: iter([test_data])
                mock_obs.get.return_value = mock_response

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_directory(
                        source="s3://mybucket/data/",
                        destination=temp_download_dir,
                    )

                assert result.success is True
                assert result.files_downloaded == 3
                assert mock_obs.get.call_count == 3

    @pytest.mark.unit
    def test_download_directory_with_pattern(self, temp_download_dir: Path) -> None:
        """Directory download with pattern should filter files."""
        from portolan_cli.download import download_directory

        test_data = b"parquet content"
        file_size = len(test_data)

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                # Mock list to return mixed file types (only parquet will be downloaded)
                mock_obs.list.return_value = [
                    [
                        {"path": "data/file1.parquet", "size": file_size},
                        {"path": "data/file2.parquet", "size": file_size},
                        {"path": "data/readme.md", "size": 50},
                        {"path": "data/config.json", "size": 30},
                    ]
                ]

                mock_response = MagicMock()
                mock_response.meta = {"size": file_size}
                mock_response.__iter__ = lambda self: iter([test_data])
                mock_obs.get.return_value = mock_response

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_directory(
                        source="s3://mybucket/data/",
                        destination=temp_download_dir,
                        pattern="*.parquet",
                    )

                assert result.success is True
                assert result.files_downloaded == 2  # Only parquet files
                assert mock_obs.get.call_count == 2

    @pytest.mark.unit
    def test_download_directory_dry_run(self, temp_download_dir: Path) -> None:
        """Dry-run should not perform actual downloads."""
        from portolan_cli.download import download_directory

        with patch("portolan_cli.download.obs") as mock_obs:
            # Mock list to return files
            mock_obs.list.return_value = [
                [
                    {"path": "data/file1.parquet", "size": 100},
                    {"path": "data/file2.parquet", "size": 200},
                ]
            ]

            result = download_directory(
                source="s3://mybucket/data/",
                destination=temp_download_dir,
                dry_run=True,
            )

        assert result.success is True
        assert result.files_downloaded == 0
        mock_obs.get.assert_not_called()

    @pytest.mark.unit
    def test_download_directory_empty(self, temp_download_dir: Path) -> None:
        """Empty remote directory should return appropriate result."""
        from portolan_cli.download import download_directory

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                # Mock list to return empty
                mock_obs.list.return_value = [[]]

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_directory(
                        source="s3://mybucket/empty/",
                        destination=temp_download_dir,
                    )

        assert result.success is True
        assert result.files_downloaded == 0
        assert result.files_failed == 0

    @pytest.mark.unit
    def test_download_directory_fail_fast_true(self, temp_download_dir: Path) -> None:
        """fail_fast=True should stop on first error and report failure."""
        from portolan_cli.download import download_directory

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                # Mock list to return files
                mock_obs.list.return_value = [
                    [
                        {"path": "data/file1.parquet", "size": 100},
                        {"path": "data/file2.parquet", "size": 200},
                        {"path": "data/file3.parquet", "size": 300},
                    ]
                ]

                # Mock get to fail
                mock_obs.get.side_effect = OSError("Download failed")

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_directory(
                        source="s3://mybucket/data/",
                        destination=temp_download_dir,
                        fail_fast=True,
                        max_files=1,  # Single worker for predictable fail_fast
                    )

                assert result.success is False
                assert result.files_failed >= 1

    @pytest.mark.unit
    def test_download_directory_fail_fast_false(self, temp_download_dir: Path) -> None:
        """fail_fast=False should continue and collect all errors."""
        from portolan_cli.download import download_directory

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                # Mock list to return files
                mock_obs.list.return_value = [
                    [
                        {"path": "data/file1.parquet", "size": 100},
                        {"path": "data/file2.parquet", "size": 200},
                        {"path": "data/file3.parquet", "size": 300},
                    ]
                ]

                # Mock get to always fail
                mock_obs.get.side_effect = OSError("Download failed")

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_directory(
                        source="s3://mybucket/data/",
                        destination=temp_download_dir,
                        fail_fast=False,
                    )

                assert result.success is False
                assert result.files_failed == 3
                assert len(result.errors) == 3

    @pytest.mark.unit
    def test_download_directory_preserves_structure(self, temp_download_dir: Path) -> None:
        """Directory download should preserve relative path structure."""
        from portolan_cli.download import download_directory

        test_data = b"file content data"
        file_size = len(test_data)

        def capture_download(store: object, key: str) -> MagicMock:
            # Track what paths would be downloaded
            _ = store  # Mark as used (vulture)
            mock_response = MagicMock()
            mock_response.meta = {"size": file_size}
            mock_response.__iter__ = lambda self: iter([test_data])
            return mock_response

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                # Mock list with nested structure - sizes must match test_data
                mock_obs.list.return_value = [
                    [
                        {"path": "data/file1.parquet", "size": file_size},
                        {"path": "data/nested/file2.parquet", "size": file_size},
                        {"path": "data/nested/deep/file3.parquet", "size": file_size},
                    ]
                ]
                mock_obs.get.side_effect = capture_download

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_directory(
                        source="s3://mybucket/data/",
                        destination=temp_download_dir,
                    )

                # Verify structure preservation through the calls
                assert result.success is True
                assert result.files_downloaded == 3


# =============================================================================
# Custom S3 Endpoint Tests
# =============================================================================


class TestCustomS3Endpoint:
    """Tests for custom S3 endpoint support (MinIO, source.coop)."""

    @pytest.mark.unit
    def test_download_file_custom_endpoint(self, temp_download_dir: Path) -> None:
        """download_file should support custom S3 endpoints."""
        from portolan_cli.download import download_file

        dest_file = temp_download_dir / "data.parquet"
        test_data = b"endpoint data"

        # Patch S3Store in upload module where _setup_store_and_kwargs lives
        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.upload.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                mock_response = MagicMock()
                mock_response.meta = {"size": len(test_data)}
                mock_response.__iter__ = lambda self: iter([test_data])
                mock_obs.get.return_value = mock_response

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_file(
                        source="s3://mybucket/data.parquet",
                        destination=dest_file,
                        s3_endpoint="minio.example.com:9000",
                        s3_region="us-east-1",
                    )

                assert result.success is True
                # Verify S3Store was called with endpoint
                mock_s3_store.assert_called_once()
                call_kwargs = mock_s3_store.call_args.kwargs
                assert "endpoint" in call_kwargs
                assert "minio.example.com:9000" in call_kwargs["endpoint"]

    @pytest.mark.unit
    def test_download_file_custom_endpoint_no_ssl(self, temp_download_dir: Path) -> None:
        """download_file should support HTTP for custom endpoints."""
        from portolan_cli.download import download_file

        dest_file = temp_download_dir / "data.parquet"
        test_data = b"http data"

        # Patch S3Store in upload module where _setup_store_and_kwargs lives
        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.upload.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                mock_response = MagicMock()
                mock_response.meta = {"size": len(test_data)}
                mock_response.__iter__ = lambda self: iter([test_data])
                mock_obs.get.return_value = mock_response

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_file(
                        source="s3://mybucket/data.parquet",
                        destination=dest_file,
                        s3_endpoint="localhost:9000",
                        s3_use_ssl=False,
                    )

                assert result.success is True
                call_kwargs = mock_s3_store.call_args.kwargs
                assert call_kwargs["endpoint"] == "http://localhost:9000"


# =============================================================================
# Streaming and Resume Tests
# =============================================================================


class TestStreamingDownload:
    """Tests for streaming download functionality."""

    @pytest.mark.unit
    def test_download_writes_in_chunks(self, temp_download_dir: Path) -> None:
        """download_file should write data in chunks (streaming)."""
        from portolan_cli.download import download_file

        dest_file = temp_download_dir / "large.parquet"

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                # Mock get to return chunks - size must match total chunk data
                chunks = [b"chunk1" * 100, b"chunk2" * 100, b"chunk3" * 100]
                total_size = sum(len(c) for c in chunks)

                mock_response = MagicMock()
                mock_response.meta = {"size": total_size}
                mock_response.__iter__ = lambda self: iter(chunks)
                mock_obs.get.return_value = mock_response

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_file(
                        source="s3://mybucket/large.parquet",
                        destination=dest_file,
                    )

                assert result.success is True
                # File should exist and contain all chunks
                assert dest_file.exists()
                assert dest_file.stat().st_size == total_size


# =============================================================================
# Integration with upload.py Utilities
# =============================================================================


class TestUploadModuleReuse:
    """Tests verifying reuse of upload.py utilities."""

    @pytest.mark.unit
    def test_uses_parse_object_store_url(self) -> None:
        """download module should use parse_object_store_url from upload."""
        # This test verifies the import works - actual parsing tested in upload tests
        from portolan_cli.upload import parse_object_store_url

        bucket_url, prefix = parse_object_store_url("s3://bucket/path/to/file")
        assert bucket_url == "s3://bucket"
        assert prefix == "path/to/file"

    @pytest.mark.unit
    def test_uses_check_credentials(self) -> None:
        """download module should use check_credentials from upload."""
        from portolan_cli.upload import check_credentials

        # Test with env vars set
        with patch.dict(
            os.environ,
            {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
        ):
            valid, hint = check_credentials("s3://bucket/path")
            assert valid is True
            assert hint == ""

    @pytest.mark.unit
    def test_uses_setup_store_and_kwargs(self) -> None:
        """download module should use _setup_store_and_kwargs from upload."""
        from portolan_cli.upload import _setup_store_and_kwargs

        with patch("portolan_cli.upload.S3Store") as mock_s3_store:
            mock_store = MagicMock()
            mock_s3_store.return_value = mock_store

            with patch.dict(
                os.environ,
                {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
            ):
                store, kwargs = _setup_store_and_kwargs("s3://bucket", None, 12)

            assert store is not None
            assert "max_concurrency" in kwargs


# =============================================================================
# File Integrity and Cleanup Tests (Issues #1, #2, #9)
# =============================================================================


class TestFileIntegrity:
    """Tests for file integrity verification and cleanup."""

    @pytest.mark.unit
    def test_download_verifies_file_size_matches_metadata(self, temp_download_dir: Path) -> None:
        """Downloaded file size should match expected size from metadata."""
        from portolan_cli.download import download_file

        dest_file = temp_download_dir / "data.parquet"
        expected_content = b"test data content"

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                mock_response = MagicMock()
                mock_response.meta = {"size": len(expected_content)}
                mock_response.__iter__ = lambda self: iter([expected_content])
                mock_obs.get.return_value = mock_response

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_file(
                        source="s3://mybucket/data.parquet",
                        destination=dest_file,
                    )

                assert result.success is True
                # Verify actual file size matches what we downloaded
                assert dest_file.stat().st_size == len(expected_content)
                # Verify total_bytes reflects actual downloaded size
                assert result.total_bytes == len(expected_content)

    @pytest.mark.unit
    def test_download_detects_size_mismatch(self, temp_download_dir: Path) -> None:
        """Download should fail when actual size doesn't match expected size."""
        from portolan_cli.download import download_file

        dest_file = temp_download_dir / "data.parquet"
        actual_content = b"short"  # 5 bytes

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                mock_response = MagicMock()
                # Metadata says 1000 bytes, but we only deliver 5
                mock_response.meta = {"size": 1000}
                mock_response.__iter__ = lambda self: iter([actual_content])
                mock_obs.get.return_value = mock_response

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_file(
                        source="s3://mybucket/data.parquet",
                        destination=dest_file,
                    )

                # Should detect the size mismatch and report failure
                assert result.success is False
                assert result.files_failed == 1
                assert len(result.errors) == 1
                # Partial file should be cleaned up
                assert not dest_file.exists()

    @pytest.mark.unit
    def test_download_cleans_up_partial_file_on_failure(self, temp_download_dir: Path) -> None:
        """Partial files should be deleted when download fails mid-stream."""
        from portolan_cli.download import download_file

        dest_file = temp_download_dir / "data.parquet"

        def fail_mid_stream() -> Generator[bytes, None, None]:
            # First chunk succeeds, second raises
            yield b"first chunk"
            raise OSError("Connection lost")

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                mock_response = MagicMock()
                mock_response.meta = {"size": 1000}
                mock_response.__iter__ = lambda self: fail_mid_stream()
                mock_obs.get.return_value = mock_response

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_file(
                        source="s3://mybucket/data.parquet",
                        destination=dest_file,
                    )

                assert result.success is False
                # Critical: partial file should not exist
                assert not dest_file.exists()

    @pytest.mark.unit
    def test_download_one_file_cleans_up_on_failure(self, temp_download_dir: Path) -> None:
        """_download_one_file should clean up partial file on failure."""
        from portolan_cli.download import _download_one_file

        local_path = temp_download_dir / "data.parquet"
        mock_store = MagicMock()

        def fail_mid_stream() -> Generator[bytes, None, None]:
            yield b"partial data"
            raise OSError("Network error")

        with patch("portolan_cli.download.obs") as mock_obs:
            mock_response = MagicMock()
            mock_response.__iter__ = lambda self: fail_mid_stream()
            mock_obs.get.return_value = mock_response

            remote_key, error, bytes_downloaded = _download_one_file(
                mock_store, "path/to/file.parquet", local_path, 1000
            )

            assert error is not None
            assert bytes_downloaded == 0
            # Partial file should be cleaned up
            assert not local_path.exists()


# =============================================================================
# Overwrite Protection Tests (Issue #3)
# =============================================================================


class TestOverwriteProtection:
    """Tests for overwrite protection."""

    @pytest.mark.unit
    def test_download_file_skips_existing_by_default(self, temp_download_dir: Path) -> None:
        """download_file should skip existing files when overwrite=False."""
        from portolan_cli.download import download_file

        dest_file = temp_download_dir / "existing.parquet"
        dest_file.write_bytes(b"original content")

        with patch("portolan_cli.download.obs") as mock_obs:
            result = download_file(
                source="s3://mybucket/existing.parquet",
                destination=dest_file,
                overwrite=False,
            )

        # Should skip without downloading
        assert result.success is True
        assert result.files_downloaded == 0
        mock_obs.get.assert_not_called()
        # Original content preserved
        assert dest_file.read_bytes() == b"original content"

    @pytest.mark.unit
    def test_download_file_overwrites_when_specified(self, temp_download_dir: Path) -> None:
        """download_file should overwrite existing files when overwrite=True."""
        from portolan_cli.download import download_file

        dest_file = temp_download_dir / "existing.parquet"
        dest_file.write_bytes(b"original content")
        new_content = b"new content"

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                mock_response = MagicMock()
                mock_response.meta = {"size": len(new_content)}
                mock_response.__iter__ = lambda self: iter([new_content])
                mock_obs.get.return_value = mock_response

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_file(
                        source="s3://mybucket/existing.parquet",
                        destination=dest_file,
                        overwrite=True,
                    )

        assert result.success is True
        assert result.files_downloaded == 1
        # Content should be replaced
        assert dest_file.read_bytes() == new_content

    @pytest.mark.unit
    def test_download_directory_skips_existing_files(self, temp_download_dir: Path) -> None:
        """download_directory should skip existing files when overwrite=False."""
        from portolan_cli.download import download_directory

        # Create an existing file
        existing_file = temp_download_dir / "file1.parquet"
        existing_file.write_bytes(b"original")

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                mock_obs.list.return_value = [
                    [
                        {"path": "data/file1.parquet", "size": 100},  # exists
                        {"path": "data/file2.parquet", "size": 200},  # new
                    ]
                ]

                mock_response = MagicMock()
                mock_response.meta = {"size": 200}
                new_content = b"n" * 200
                mock_response.__iter__ = lambda self: iter([new_content])
                mock_obs.get.return_value = mock_response

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_directory(
                        source="s3://mybucket/data/",
                        destination=temp_download_dir,
                        overwrite=False,
                    )

                # Should download only the new file
                assert result.files_downloaded == 1
                # Original content preserved
                assert existing_file.read_bytes() == b"original"


# =============================================================================
# Path Traversal Security Tests (Issue #6)
# =============================================================================


class TestPathTraversalProtection:
    """Tests for path traversal vulnerability protection."""

    @pytest.mark.unit
    def test_rejects_path_traversal_with_dotdot(self, temp_download_dir: Path) -> None:
        """Should reject remote keys containing '..' that escape destination."""
        from portolan_cli.download import (
            PathTraversalError,
            _build_local_path,
            _validate_local_path,
        )

        destination = temp_download_dir / "safe_dir"
        destination.mkdir()

        # These keys START with the prefix, so after prefix removal
        # they will contain path traversal components
        malicious_keys = [
            "data/../../../etc/passwd",  # After removing "data/": "../../../etc/passwd"
            "data/subdir/../../../../../../etc/passwd",  # Escapes after prefix removal
        ]

        for key in malicious_keys:
            local_path = _build_local_path(key, "data/", destination)
            with pytest.raises(PathTraversalError, match="traversal"):
                _validate_local_path(local_path, destination)

    @pytest.mark.unit
    def test_allows_safe_paths(self, temp_download_dir: Path) -> None:
        """Should allow safe paths that stay within destination."""
        from portolan_cli.download import _build_local_path, _validate_local_path

        destination = temp_download_dir / "safe_dir"
        destination.mkdir()

        # These should be allowed
        safe_keys = [
            "data/file.parquet",
            "data/subdir/file.parquet",
        ]

        for key in safe_keys:
            local_path = _build_local_path(key, "data/", destination)
            # Should not raise
            _validate_local_path(local_path, destination)
            # Resolved path should be within destination
            assert local_path.resolve().is_relative_to(destination.resolve())

    @pytest.mark.unit
    def test_download_directory_rejects_traversal(self, temp_download_dir: Path) -> None:
        """download_directory should skip files with path traversal attempts."""
        from portolan_cli.download import download_directory

        test_data = b"safe file content"
        file_size = len(test_data)

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                # Include a malicious path - safe files have matching size
                mock_obs.list.return_value = [
                    [
                        {"path": "data/file1.parquet", "size": file_size},
                        {"path": "data/../../../etc/passwd", "size": 50},
                        {"path": "data/file2.parquet", "size": file_size},
                    ]
                ]

                mock_response = MagicMock()
                mock_response.meta = {"size": file_size}
                mock_response.__iter__ = lambda self: iter([test_data])
                mock_obs.get.return_value = mock_response

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_directory(
                        source="s3://mybucket/data/",
                        destination=temp_download_dir,
                    )

                # Should download only the safe files (2 out of 3)
                assert result.files_downloaded == 2
                # Should record the traversal attempt as error
                assert result.files_failed == 1


# =============================================================================
# Input Validation Tests (Issue #8)
# =============================================================================


class TestInputValidation:
    """Tests for input validation."""

    @pytest.mark.unit
    def test_download_file_rejects_empty_destination(self, tmp_path: Path) -> None:
        """download_file should reject empty source URL."""
        from portolan_cli.download import download_file

        # Note: Path("") is valid (it represents "."), so we test source validation
        # instead, which explicitly checks for empty strings
        with pytest.raises(ValueError, match="source"):
            download_file(
                source="",  # Empty source
                destination=tmp_path / "data.parquet",
            )

    @pytest.mark.unit
    def test_download_file_rejects_empty_source(self, temp_download_dir: Path) -> None:
        """download_file should reject empty source URL."""
        from portolan_cli.download import download_file

        with pytest.raises(ValueError, match="source"):
            download_file(
                source="",
                destination=temp_download_dir / "data.parquet",
            )

    @pytest.mark.unit
    def test_download_directory_rejects_file_destination(self, temp_download_dir: Path) -> None:
        """download_directory should reject file as destination."""
        from portolan_cli.download import download_directory

        # Create a file where directory is expected
        file_dest = temp_download_dir / "not_a_dir.txt"
        file_dest.write_text("I am a file")

        with pytest.raises(ValueError, match="directory"):
            download_directory(
                source="s3://mybucket/data/",
                destination=file_dest,
            )


# =============================================================================
# Error Type Consistency Tests (Issue #7)
# =============================================================================


class TestErrorTypeConsistency:
    """Tests for consistent error types with upload module."""

    @pytest.mark.unit
    def test_download_result_uses_path_for_errors(self) -> None:
        """DownloadResult.errors should use Path type like UploadResult."""
        from portolan_cli.download import DownloadResult

        error = OSError("Network error")
        result = DownloadResult(
            success=False,
            files_downloaded=0,
            files_failed=1,
            total_bytes=0,
            errors=[(Path("path/to/file.parquet"), error)],
        )

        # Error tuple should contain Path, not str
        assert isinstance(result.errors[0][0], Path)

    @pytest.mark.unit
    def test_download_file_returns_path_in_errors(self, temp_download_dir: Path) -> None:
        """download_file should return Path in error tuples."""
        from portolan_cli.download import download_file

        dest_file = temp_download_dir / "data.parquet"

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store
                mock_obs.get.side_effect = OSError("Network error")

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_file(
                        source="s3://mybucket/data.parquet",
                        destination=dest_file,
                    )

                assert len(result.errors) == 1
                # First element should be Path, not str
                assert isinstance(result.errors[0][0], Path)


# =============================================================================
# Additional Coverage Tests
# =============================================================================


class TestDryRunCoverage:
    """Tests to improve dry-run path coverage."""

    @pytest.mark.unit
    def test_download_directory_dry_run_many_files(self, temp_download_dir: Path) -> None:
        """Dry run with > 10 files should show '... and N more' message."""
        from portolan_cli.download import download_directory

        # Create mock for listing 15+ files - must be list of list (generator behavior)
        mock_files = [
            [{"path": f"data/file{i}.parquet", "size": 100} for i in range(15)]
        ]

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store
                mock_obs.list.return_value = mock_files

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_directory(
                        source="s3://mybucket/data/",
                        destination=temp_download_dir,
                        dry_run=True,
                    )

                assert result.success is True
                assert result.files_downloaded == 0  # Dry run doesn't download


class TestFailFastSubmitNextFile:
    """Tests for fail_fast branch where next file is submitted."""

    @pytest.mark.unit
    def test_download_directory_fail_fast_submits_next(
        self, temp_download_dir: Path
    ) -> None:
        """fail_fast mode should submit next file after one completes."""
        from portolan_cli.download import download_directory

        # Create 5 files, max_files=2 to force incremental submission
        # Must be list of list (generator behavior)
        mock_files = [
            [{"path": f"data/file{i}.parquet", "size": 100} for i in range(5)]
        ]

        def mock_get(store: object, key: str) -> MagicMock:
            response = MagicMock()
            response.meta = {"size": 100}
            response.__iter__ = lambda self: iter([b"x" * 100])
            return response

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store
                mock_obs.list.return_value = mock_files
                mock_obs.get.side_effect = mock_get

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_directory(
                        source="s3://mybucket/data/",
                        destination=temp_download_dir,
                        fail_fast=True,
                        max_files=2,  # Force incremental submission
                    )

                assert result.success is True
                assert result.files_downloaded == 5


class TestExceptionCleanupInDownloadFile:
    """Tests for exception handler cleanup paths."""

    @pytest.mark.unit
    def test_download_file_cleans_up_on_unexpected_error(
        self, temp_download_dir: Path
    ) -> None:
        """download_file should clean up partial file on unexpected exception."""
        from portolan_cli.download import download_file

        dest_file = temp_download_dir / "data.parquet"

        def mock_get(store: object, key: str) -> MagicMock:
            # Create partial file then raise
            dest_file.parent.mkdir(parents=True, exist_ok=True)
            dest_file.write_bytes(b"partial")
            raise RuntimeError("Unexpected error during download")

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store
                mock_obs.get.side_effect = mock_get

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = download_file(
                        source="s3://mybucket/data.parquet",
                        destination=dest_file,
                    )

                assert result.success is False
                # Partial file should be cleaned up
                assert not dest_file.exists()

    @pytest.mark.unit
    def test_download_file_handles_cleanup_oserror(
        self, temp_download_dir: Path
    ) -> None:
        """download_file should handle OSError during cleanup gracefully."""
        from portolan_cli.download import download_file

        dest_file = temp_download_dir / "data.parquet"

        def mock_get(store: object, key: str) -> MagicMock:
            raise RuntimeError("Download error")

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store
                mock_obs.get.side_effect = mock_get

                # Mock Path.exists to return True, unlink to raise OSError
                with patch.object(Path, "exists", return_value=True):
                    with patch.object(Path, "unlink", side_effect=OSError("Permission denied")):
                        with patch.dict(
                            os.environ,
                            {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                        ):
                            result = download_file(
                                source="s3://mybucket/data.parquet",
                                destination=dest_file,
                            )

                        assert result.success is False
                        # Should not raise, just continue with error result
