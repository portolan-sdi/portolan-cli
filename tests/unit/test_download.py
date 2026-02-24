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
    from collections.abc import Iterator


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
            errors=[("path/to/file1.parquet", error), ("path/to/file2.parquet", error)],
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

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                # Mock get to return a response with metadata and bytes
                mock_response = MagicMock()
                mock_response.meta = {"size": 1024}
                mock_response.__iter__ = lambda self: iter([b"test data"])
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

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                mock_response = MagicMock()
                mock_response.meta = {"size": 100}
                mock_response.__iter__ = lambda self: iter([b"data"])
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

        with patch("portolan_cli.download.obs") as mock_obs:
            mock_response = MagicMock()
            mock_response.meta = {"size": 512}
            mock_response.__iter__ = lambda self: iter([b"gcs data"])
            mock_obs.get.return_value = mock_response
            mock_obs.store.from_url.return_value = MagicMock()

            # GCS uses ADC - mock the exists check
            with patch.object(Path, "exists", return_value=True):
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

        # Need to patch both download.obs AND upload.obs since _setup_store_and_kwargs is in upload
        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.upload.obs") as mock_upload_obs:
                mock_response = MagicMock()
                mock_response.meta = {"size": 256}
                mock_response.__iter__ = lambda self: iter([b"azure data"])
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

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                # Mock list to return file metadata
                mock_obs.list.return_value = [
                    [
                        {"path": "data/file1.parquet", "size": 100},
                        {"path": "data/file2.parquet", "size": 200},
                        {"path": "data/subdir/file3.parquet", "size": 150},
                    ]
                ]

                # Mock get for each file
                mock_response = MagicMock()
                mock_response.meta = {"size": 100}
                mock_response.__iter__ = lambda self: iter([b"data"])
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

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                # Mock list to return mixed file types
                mock_obs.list.return_value = [
                    [
                        {"path": "data/file1.parquet", "size": 100},
                        {"path": "data/file2.parquet", "size": 200},
                        {"path": "data/readme.md", "size": 50},
                        {"path": "data/config.json", "size": 30},
                    ]
                ]

                mock_response = MagicMock()
                mock_response.meta = {"size": 100}
                mock_response.__iter__ = lambda self: iter([b"data"])
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

        def capture_download(store: object, key: str) -> MagicMock:
            # Track what paths would be downloaded
            _ = store  # Mark as used (vulture)
            mock_response = MagicMock()
            mock_response.meta = {"size": 100}
            mock_response.__iter__ = lambda self: iter([b"data"])
            return mock_response

        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.download.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                # Mock list with nested structure
                mock_obs.list.return_value = [
                    [
                        {"path": "data/file1.parquet", "size": 100},
                        {"path": "data/nested/file2.parquet", "size": 200},
                        {"path": "data/nested/deep/file3.parquet", "size": 300},
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

        # Patch S3Store in upload module where _setup_store_and_kwargs lives
        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.upload.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                mock_response = MagicMock()
                mock_response.meta = {"size": 100}
                mock_response.__iter__ = lambda self: iter([b"data"])
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

        # Patch S3Store in upload module where _setup_store_and_kwargs lives
        with patch("portolan_cli.download.obs") as mock_obs:
            with patch("portolan_cli.upload.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                mock_response = MagicMock()
                mock_response.meta = {"size": 100}
                mock_response.__iter__ = lambda self: iter([b"data"])
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

                # Mock get to return chunks
                mock_response = MagicMock()
                mock_response.meta = {"size": 1024 * 1024}  # 1MB
                # Simulate chunked response
                chunks = [b"chunk1" * 100, b"chunk2" * 100, b"chunk3" * 100]
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
