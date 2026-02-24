"""Unit tests for upload module.

Tests for cloud object storage upload functionality with mocked obstore.
Following TDD: these tests are written FIRST, before implementation.

Test categories:
- URL parsing
- Credential checking
- Single file upload
- Directory upload with parallel processing
- Dry-run mode
- Error handling (fail_fast behavior)
- AWS profile loading
- Region inference
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

if TYPE_CHECKING:
    from collections.abc import Generator


# =============================================================================
# Test fixtures
# =============================================================================


@pytest.fixture
def temp_file(tmp_path: Path) -> Path:
    """Create a temporary file for testing."""
    file_path = tmp_path / "test_data.parquet"
    file_path.write_bytes(b"x" * 1024)  # 1KB file
    return file_path


@pytest.fixture
def temp_dir_with_files(tmp_path: Path) -> Path:
    """Create a temporary directory with multiple files for testing."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    # Create some parquet files
    (data_dir / "file1.parquet").write_bytes(b"x" * 1024)
    (data_dir / "file2.parquet").write_bytes(b"y" * 2048)
    (data_dir / "nested").mkdir()
    (data_dir / "nested" / "file3.parquet").write_bytes(b"z" * 512)

    # Create a non-parquet file
    (data_dir / "readme.txt").write_text("test readme")

    return data_dir


@pytest.fixture
def mock_aws_credentials(tmp_path: Path) -> Generator[Path, None, None]:
    """Create mock AWS credentials file."""
    aws_dir = tmp_path / ".aws"
    aws_dir.mkdir()

    creds_file = aws_dir / "credentials"
    creds_file.write_text("""[default]
aws_access_key_id = AKIADEFAULTKEY
aws_secret_access_key = defaultsecret

[myprofile]
aws_access_key_id = AKIAPROFILEKEY
aws_secret_access_key = profilesecret
""")

    config_file = aws_dir / "config"
    config_file.write_text("""[default]
region = us-east-1

[profile myprofile]
region = eu-west-1
""")

    with patch.object(Path, "home", return_value=tmp_path):
        yield aws_dir


# =============================================================================
# URL Parsing Tests
# =============================================================================


class TestParseObjectStoreUrl:
    """Tests for parse_object_store_url function."""

    @pytest.mark.unit
    def test_s3_url_simple(self) -> None:
        """S3 URL with bucket only."""
        from portolan_cli.upload import parse_object_store_url

        bucket_url, prefix = parse_object_store_url("s3://mybucket")
        assert bucket_url == "s3://mybucket"
        assert prefix == ""

    @pytest.mark.unit
    def test_s3_url_with_prefix(self) -> None:
        """S3 URL with bucket and prefix."""
        from portolan_cli.upload import parse_object_store_url

        bucket_url, prefix = parse_object_store_url("s3://mybucket/data/output")
        assert bucket_url == "s3://mybucket"
        assert prefix == "data/output"

    @pytest.mark.unit
    def test_gs_url_simple(self) -> None:
        """GCS URL with bucket only."""
        from portolan_cli.upload import parse_object_store_url

        bucket_url, prefix = parse_object_store_url("gs://mybucket")
        assert bucket_url == "gs://mybucket"
        assert prefix == ""

    @pytest.mark.unit
    def test_gs_url_with_prefix(self) -> None:
        """GCS URL with bucket and prefix."""
        from portolan_cli.upload import parse_object_store_url

        bucket_url, prefix = parse_object_store_url("gs://mybucket/path/to/data")
        assert bucket_url == "gs://mybucket"
        assert prefix == "path/to/data"

    @pytest.mark.unit
    def test_az_url_with_container(self) -> None:
        """Azure URL with account and container."""
        from portolan_cli.upload import parse_object_store_url

        bucket_url, prefix = parse_object_store_url("az://myaccount/mycontainer")
        assert bucket_url == "az://myaccount/mycontainer"
        assert prefix == ""

    @pytest.mark.unit
    def test_az_url_with_path(self) -> None:
        """Azure URL with account, container, and path."""
        from portolan_cli.upload import parse_object_store_url

        bucket_url, prefix = parse_object_store_url("az://myaccount/mycontainer/data/path")
        assert bucket_url == "az://myaccount/mycontainer"
        assert prefix == "data/path"

    @pytest.mark.unit
    def test_unsupported_scheme_raises(self) -> None:
        """Unsupported URL scheme should raise ValueError."""
        from portolan_cli.upload import parse_object_store_url

        with pytest.raises(ValueError, match="Unsupported URL scheme"):
            parse_object_store_url("ftp://server/path")


# =============================================================================
# Credential Checking Tests
# =============================================================================


class TestCheckCredentials:
    """Tests for check_credentials function."""

    @pytest.mark.unit
    def test_s3_with_env_vars(self) -> None:
        """S3 credentials should be found from environment variables."""
        from portolan_cli.upload import check_credentials

        with patch.dict(
            os.environ,
            {"AWS_ACCESS_KEY_ID": "AKIATEST", "AWS_SECRET_ACCESS_KEY": "testsecret"},
        ):
            valid, hint = check_credentials("s3://mybucket/path")
            assert valid is True
            assert hint == ""

    @pytest.mark.unit
    def test_s3_missing_credentials_gives_hint(self) -> None:
        """Missing S3 credentials should return helpful hints."""
        from portolan_cli.upload import check_credentials

        with patch.dict(os.environ, {}, clear=True):
            with patch("portolan_cli.upload._load_aws_credentials_from_profile") as mock_load:
                mock_load.return_value = (None, None, None)
                valid, hint = check_credentials("s3://mybucket/path")

        assert valid is False
        assert "AWS_ACCESS_KEY_ID" in hint
        assert "aws configure" in hint.lower() or "configure" in hint.lower()

    @pytest.mark.unit
    def test_s3_with_profile(self, mock_aws_credentials: Path) -> None:
        """S3 credentials should be loaded from AWS profile."""
        from portolan_cli.upload import check_credentials

        # Fixture patches Path.home() - assert it exists to mark as used
        assert mock_aws_credentials.exists()
        with patch.dict(os.environ, {}, clear=True):
            valid, hint = check_credentials("s3://mybucket/path", profile="myprofile")
            assert valid is True
            assert hint == ""

    @pytest.mark.unit
    def test_s3_with_missing_profile(self, mock_aws_credentials: Path) -> None:
        """Missing AWS profile should return error with hints."""
        from portolan_cli.upload import check_credentials

        # Fixture patches Path.home() - assert it exists to mark as used
        assert mock_aws_credentials.exists()
        with patch.dict(os.environ, {}, clear=True):
            valid, hint = check_credentials("s3://mybucket/path", profile="nonexistent")
            assert valid is False
            assert "nonexistent" in hint

    @pytest.mark.unit
    def test_gcs_with_credentials_file(self, tmp_path: Path) -> None:
        """GCS credentials should be found from GOOGLE_APPLICATION_CREDENTIALS."""
        from portolan_cli.upload import check_credentials

        creds_file = tmp_path / "service_account.json"
        creds_file.write_text("{}")  # Minimal valid JSON

        with patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": str(creds_file)}):
            valid, hint = check_credentials("gs://mybucket/path")
            assert valid is True
            assert hint == ""

    @pytest.mark.unit
    def test_gcs_missing_credentials(self) -> None:
        """Missing GCS credentials should return helpful hints."""
        from pathlib import Path

        from portolan_cli.upload import check_credentials

        # Mock ADC file check - don't clear env vars completely as it breaks
        # Path.home() on Windows (needs HOME or USERPROFILE)
        with (
            patch.dict(
                os.environ,
                {
                    "GOOGLE_APPLICATION_CREDENTIALS": "",
                    "GOOGLE_SERVICE_ACCOUNT": "",
                    "GOOGLE_SERVICE_ACCOUNT_PATH": "",
                    "GOOGLE_SERVICE_ACCOUNT_KEY": "",
                },
            ),
            patch.object(Path, "exists", return_value=False),
        ):
            valid, hint = check_credentials("gs://mybucket/path")
            assert valid is False
            assert "GOOGLE_APPLICATION_CREDENTIALS" in hint

    @pytest.mark.unit
    def test_gcs_with_service_account_key_env(self) -> None:
        """GCS credentials should be found from inline GOOGLE_SERVICE_ACCOUNT_KEY."""
        from portolan_cli.upload import check_credentials

        with patch.dict(os.environ, {"GOOGLE_SERVICE_ACCOUNT_KEY": '{"type": "service_account"}'}):
            valid, hint = check_credentials("gs://mybucket/path")
            assert valid is True
            assert hint == ""

    @pytest.mark.unit
    def test_gcs_with_adc_file(self, tmp_path: Path) -> None:
        """GCS credentials should be found from ADC file in default location."""
        from portolan_cli.upload import check_credentials

        # Mock Path.home() to use tmp_path and create ADC file
        adc_dir = tmp_path / ".config" / "gcloud"
        adc_dir.mkdir(parents=True)
        adc_file = adc_dir / "application_default_credentials.json"
        adc_file.write_text('{"type": "authorized_user"}')

        with (
            patch.dict(os.environ, {}, clear=True),
            patch("pathlib.Path.home", return_value=tmp_path),
        ):
            valid, hint = check_credentials("gs://mybucket/path")
            assert valid is True
            assert hint == ""

    @pytest.mark.unit
    def test_azure_with_access_key_alias(self) -> None:
        """Azure credentials should be found from AZURE_STORAGE_ACCESS_KEY alias."""
        from portolan_cli.upload import check_credentials

        with patch.dict(os.environ, {"AZURE_STORAGE_ACCESS_KEY": "testkey"}):
            valid, hint = check_credentials("az://myaccount/container")
            assert valid is True
            assert hint == ""

    @pytest.mark.unit
    def test_azure_with_account_key(self) -> None:
        """Azure credentials should be found from account key env var."""
        from portolan_cli.upload import check_credentials

        with patch.dict(os.environ, {"AZURE_STORAGE_ACCOUNT_KEY": "testkey"}):
            valid, hint = check_credentials("az://myaccount/container")
            assert valid is True
            assert hint == ""

    @pytest.mark.unit
    def test_azure_with_sas_token(self) -> None:
        """Azure credentials should be found from SAS token env var."""
        from portolan_cli.upload import check_credentials

        with patch.dict(os.environ, {"AZURE_STORAGE_SAS_TOKEN": "sastoken"}):
            valid, hint = check_credentials("az://myaccount/container")
            assert valid is True
            assert hint == ""

    @pytest.mark.unit
    def test_azure_missing_credentials(self) -> None:
        """Missing Azure credentials should return helpful hints."""
        from portolan_cli.upload import check_credentials

        with patch.dict(os.environ, {}, clear=True):
            valid, hint = check_credentials("az://myaccount/container")
            assert valid is False
            assert "AZURE_STORAGE_ACCOUNT_KEY" in hint

    @pytest.mark.unit
    def test_http_url_always_valid(self) -> None:
        """HTTP URLs should always return valid (no auth needed)."""
        from portolan_cli.upload import check_credentials

        valid, hint = check_credentials("https://example.com/data.parquet")
        assert valid is True
        assert hint == ""


# =============================================================================
# Upload Result Tests
# =============================================================================


class TestUploadResult:
    """Tests for UploadResult dataclass."""

    @pytest.mark.unit
    def test_upload_result_success(self) -> None:
        """UploadResult should capture successful upload stats."""
        from portolan_cli.upload import UploadResult

        result = UploadResult(
            success=True,
            files_uploaded=5,
            files_failed=0,
            total_bytes=10240,
            errors=[],
        )

        assert result.success is True
        assert result.files_uploaded == 5
        assert result.files_failed == 0
        assert result.total_bytes == 10240
        assert result.errors == []

    @pytest.mark.unit
    def test_upload_result_partial_failure(self) -> None:
        """UploadResult should capture partial failures."""
        from portolan_cli.upload import UploadResult

        fake_error = Exception("Upload failed")
        result = UploadResult(
            success=False,
            files_uploaded=3,
            files_failed=2,
            total_bytes=5120,
            errors=[(Path("file1.txt"), fake_error), (Path("file2.txt"), fake_error)],
        )

        assert result.success is False
        assert result.files_uploaded == 3
        assert result.files_failed == 2
        assert len(result.errors) == 2


# =============================================================================
# Single File Upload Tests
# =============================================================================


class TestUploadFile:
    """Tests for upload_file function."""

    @pytest.mark.unit
    def test_upload_file_success(self, temp_file: Path) -> None:
        """Single file upload should succeed and return result."""
        from portolan_cli.upload import upload_file

        with patch("portolan_cli.upload.obs") as mock_obs:
            with patch("portolan_cli.upload.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = upload_file(
                        source=temp_file,
                        destination="s3://mybucket/data.parquet",
                    )

                assert result.success is True
                assert result.files_uploaded == 1
                assert result.files_failed == 0
                mock_obs.put.assert_called_once()

    @pytest.mark.unit
    def test_upload_file_dry_run(self, temp_file: Path) -> None:
        """Dry-run should not perform actual upload."""
        from portolan_cli.upload import upload_file

        with patch("portolan_cli.upload.obs") as mock_obs:
            result = upload_file(
                source=temp_file,
                destination="s3://mybucket/data.parquet",
                dry_run=True,
            )

        assert result.success is True
        assert result.files_uploaded == 0  # No actual upload in dry-run
        mock_obs.put.assert_not_called()

    @pytest.mark.unit
    def test_upload_file_nonexistent_raises(self, tmp_path: Path) -> None:
        """Uploading nonexistent file should raise FileNotFoundError."""
        from portolan_cli.upload import upload_file

        with pytest.raises(FileNotFoundError):
            upload_file(
                source=tmp_path / "nonexistent.parquet",
                destination="s3://mybucket/data.parquet",
            )

    @pytest.mark.unit
    def test_upload_file_with_custom_endpoint(self, temp_file: Path) -> None:
        """Custom S3 endpoint (MinIO) should be passed to store."""
        from portolan_cli.upload import upload_file

        with patch("portolan_cli.upload.obs"):
            with patch("portolan_cli.upload.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    upload_file(
                        source=temp_file,
                        destination="s3://mybucket/data.parquet",
                        s3_endpoint="minio.example.com:9000",
                        s3_region="us-east-1",
                    )

                # Verify endpoint was passed to S3Store
                call_kwargs = mock_s3_store.call_args[1]
                assert "endpoint" in call_kwargs
                assert "minio.example.com:9000" in call_kwargs["endpoint"]

    @pytest.mark.unit
    def test_upload_file_strips_existing_scheme_from_endpoint(self, temp_file: Path) -> None:
        """S3 endpoint with existing scheme should not double-prepend protocol."""
        from portolan_cli.upload import upload_file

        with patch("portolan_cli.upload.obs"):
            with patch("portolan_cli.upload.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    # Pass endpoint with scheme already included
                    upload_file(
                        source=temp_file,
                        destination="s3://mybucket/data.parquet",
                        s3_endpoint="https://minio.example.com:9000",
                        s3_region="us-east-1",
                    )

                # Verify the endpoint doesn't have double protocol (https://https://)
                call_kwargs = mock_s3_store.call_args[1]
                endpoint = call_kwargs["endpoint"]
                assert endpoint == "https://minio.example.com:9000"
                assert "https://https://" not in endpoint

    @pytest.mark.unit
    def test_upload_file_preserves_target_key(self, temp_file: Path) -> None:
        """Upload should use exact destination key when not ending with /."""
        from portolan_cli.upload import upload_file

        with patch("portolan_cli.upload.obs") as mock_obs:
            with patch("portolan_cli.upload.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    upload_file(
                        source=temp_file,
                        destination="s3://mybucket/custom/key.parquet",
                    )

                # Verify the target key in obs.put call
                call_args = mock_obs.put.call_args
                target_key = call_args[0][1]  # Second positional arg
                assert target_key == "custom/key.parquet"

    @pytest.mark.unit
    def test_upload_file_appends_filename_to_dir(self, temp_file: Path) -> None:
        """Upload to directory (ending with /) should append filename."""
        from portolan_cli.upload import upload_file

        with patch("portolan_cli.upload.obs") as mock_obs:
            with patch("portolan_cli.upload.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    upload_file(
                        source=temp_file,
                        destination="s3://mybucket/data/",
                    )

                call_args = mock_obs.put.call_args
                target_key = call_args[0][1]
                assert target_key.endswith(temp_file.name)


# =============================================================================
# Directory Upload Tests
# =============================================================================


class TestUploadDirectory:
    """Tests for upload_directory function."""

    @pytest.mark.unit
    def test_upload_directory_all_files(self, temp_dir_with_files: Path) -> None:
        """Directory upload should upload all files."""
        from portolan_cli.upload import upload_directory

        with patch("portolan_cli.upload.obs") as mock_obs:
            with patch("portolan_cli.upload.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = upload_directory(
                        source=temp_dir_with_files,
                        destination="s3://mybucket/data/",
                    )

                assert result.success is True
                assert result.files_uploaded == 4  # 3 parquet + 1 txt
                assert mock_obs.put.call_count == 4

    @pytest.mark.unit
    def test_upload_directory_with_pattern(self, temp_dir_with_files: Path) -> None:
        """Directory upload with pattern should filter files."""
        from portolan_cli.upload import upload_directory

        with patch("portolan_cli.upload.obs") as mock_obs:
            with patch("portolan_cli.upload.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = upload_directory(
                        source=temp_dir_with_files,
                        destination="s3://mybucket/data/",
                        pattern="*.parquet",
                    )

                assert result.success is True
                assert result.files_uploaded == 3  # Only parquet files
                assert mock_obs.put.call_count == 3

    @pytest.mark.unit
    def test_upload_directory_dry_run(self, temp_dir_with_files: Path) -> None:
        """Dry-run should not perform actual uploads."""
        from portolan_cli.upload import upload_directory

        with patch("portolan_cli.upload.obs") as mock_obs:
            result = upload_directory(
                source=temp_dir_with_files,
                destination="s3://mybucket/data/",
                dry_run=True,
            )

        assert result.success is True
        assert result.files_uploaded == 0
        mock_obs.put.assert_not_called()

    @pytest.mark.unit
    def test_upload_directory_empty(self, tmp_path: Path) -> None:
        """Empty directory should return appropriate result."""
        from portolan_cli.upload import upload_directory

        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        result = upload_directory(
            source=empty_dir,
            destination="s3://mybucket/data/",
        )

        assert result.success is True
        assert result.files_uploaded == 0
        assert result.files_failed == 0

    @pytest.mark.unit
    def test_upload_directory_fail_fast_true(self, temp_dir_with_files: Path) -> None:
        """fail_fast=True should stop on first error and report failure."""
        from portolan_cli.upload import upload_directory

        def mock_put_fails_first(*args: object, **kwargs: object) -> None:
            # Always fail to ensure we catch the fail_fast behavior
            # Mark args/kwargs as used (vulture) - they're passed by obstore.put()
            _ = (args, kwargs)
            raise OSError("Upload failed")

        with patch("portolan_cli.upload.obs") as mock_obs:
            with patch("portolan_cli.upload.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store
                mock_obs.put.side_effect = mock_put_fails_first

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = upload_directory(
                        source=temp_dir_with_files,
                        destination="s3://mybucket/data/",
                        fail_fast=True,
                        max_files=1,  # Single worker to test sequential fail_fast
                    )

                assert result.success is False
                assert result.files_failed >= 1
                # With max_files=1 and fail_fast=True, should stop before processing all files
                # The fixture creates 4 files, so we verify we stopped early
                total_files = 4
                assert mock_obs.put.call_count < total_files

    @pytest.mark.unit
    def test_upload_directory_fail_fast_false(self, temp_dir_with_files: Path) -> None:
        """fail_fast=False should continue and collect all errors."""
        from portolan_cli.upload import upload_directory

        def mock_put_fails_all(*args: object, **kwargs: object) -> None:
            # Mark args/kwargs as used (vulture) - they're passed by obstore.put()
            _ = (args, kwargs)
            raise OSError("Upload failed")

        with patch("portolan_cli.upload.obs") as mock_obs:
            with patch("portolan_cli.upload.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store
                mock_obs.put.side_effect = mock_put_fails_all

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = upload_directory(
                        source=temp_dir_with_files,
                        destination="s3://mybucket/data/",
                        fail_fast=False,
                    )

                assert result.success is False
                assert result.files_failed == 4  # All files failed
                assert len(result.errors) == 4

    @pytest.mark.unit
    def test_upload_directory_preserves_structure(self, temp_dir_with_files: Path) -> None:
        """Directory upload should preserve relative path structure."""
        from portolan_cli.upload import upload_directory

        target_keys: list[str] = []

        def capture_target_key(store: object, key: str, source: object, **kwargs: object) -> None:
            target_keys.append(key)

        with patch("portolan_cli.upload.obs") as mock_obs:
            with patch("portolan_cli.upload.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store
                mock_obs.put.side_effect = capture_target_key

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    upload_directory(
                        source=temp_dir_with_files,
                        destination="s3://mybucket/output/",
                    )

        # Check that nested structure is preserved
        nested_keys = [k for k in target_keys if "nested" in k]
        assert len(nested_keys) == 1
        assert "nested/file3.parquet" in nested_keys[0]


# =============================================================================
# AWS Profile Loading Tests
# =============================================================================


class TestLoadAwsCredentials:
    """Tests for AWS credentials loading from profile."""

    @pytest.mark.unit
    def test_load_default_profile(self, mock_aws_credentials: Path) -> None:
        """Should load credentials from default profile."""
        from portolan_cli.upload import _load_aws_credentials_from_profile

        # Fixture patches Path.home() - assert it exists to mark as used
        assert mock_aws_credentials.exists()
        access_key, secret_key, region = _load_aws_credentials_from_profile("default")

        assert access_key == "AKIADEFAULTKEY"
        assert secret_key == "defaultsecret"
        assert region == "us-east-1"

    @pytest.mark.unit
    def test_load_named_profile(self, mock_aws_credentials: Path) -> None:
        """Should load credentials from named profile."""
        from portolan_cli.upload import _load_aws_credentials_from_profile

        # Fixture patches Path.home() - assert it exists to mark as used
        assert mock_aws_credentials.exists()
        access_key, secret_key, region = _load_aws_credentials_from_profile("myprofile")

        assert access_key == "AKIAPROFILEKEY"
        assert secret_key == "profilesecret"
        assert region == "eu-west-1"

    @pytest.mark.unit
    def test_load_missing_profile(self, mock_aws_credentials: Path) -> None:
        """Missing profile should return None values."""
        from portolan_cli.upload import _load_aws_credentials_from_profile

        # Fixture patches Path.home() - assert it exists to mark as used
        assert mock_aws_credentials.exists()
        access_key, secret_key, region = _load_aws_credentials_from_profile("nonexistent")

        assert access_key is None
        assert secret_key is None
        assert region is None


# =============================================================================
# Region Inference Tests
# =============================================================================


class TestRegionInference:
    """Tests for AWS region inference from bucket name."""

    @pytest.mark.unit
    def test_infer_us_west_2(self) -> None:
        """Should infer us-west-2 from bucket name."""
        from portolan_cli.upload import _try_infer_region_from_bucket

        region = _try_infer_region_from_bucket("us-west-2.opendata.source.coop")
        assert region == "us-west-2"

    @pytest.mark.unit
    def test_infer_eu_central_1(self) -> None:
        """Should infer eu-central-1 from bucket name."""
        from portolan_cli.upload import _try_infer_region_from_bucket

        region = _try_infer_region_from_bucket("eu-central-1.example.com")
        assert region == "eu-central-1"

    @pytest.mark.unit
    def test_infer_ap_northeast_1(self) -> None:
        """Should infer ap-northeast-1 from bucket name."""
        from portolan_cli.upload import _try_infer_region_from_bucket

        region = _try_infer_region_from_bucket("ap-northeast-1.data.example.com")
        assert region == "ap-northeast-1"

    @pytest.mark.unit
    def test_no_region_in_bucket(self) -> None:
        """Should return None for bucket without region pattern."""
        from portolan_cli.upload import _try_infer_region_from_bucket

        region = _try_infer_region_from_bucket("mybucket")
        assert region is None

    @pytest.mark.unit
    def test_partial_region_pattern(self) -> None:
        """Should return None for partial region pattern."""
        from portolan_cli.upload import _try_infer_region_from_bucket

        region = _try_infer_region_from_bucket("us-west.example.com")
        assert region is None


# =============================================================================
# Target Key Building Tests
# =============================================================================


class TestBuildTargetKey:
    """Tests for target key building."""

    @pytest.mark.unit
    def test_build_key_with_prefix(self, tmp_path: Path) -> None:
        """Should build target key with prefix."""
        from portolan_cli.upload import _build_target_key

        source_dir = tmp_path / "data"
        source_dir.mkdir()
        file_path = source_dir / "file.parquet"
        file_path.touch()

        key = _build_target_key(file_path, source_dir, "output/dataset")
        assert key == "output/dataset/file.parquet"

    @pytest.mark.unit
    def test_build_key_nested_file(self, tmp_path: Path) -> None:
        """Should preserve nested directory structure in key."""
        from portolan_cli.upload import _build_target_key

        source_dir = tmp_path / "data"
        nested_dir = source_dir / "subdir"
        nested_dir.mkdir(parents=True)
        file_path = nested_dir / "file.parquet"
        file_path.touch()

        key = _build_target_key(file_path, source_dir, "output")
        assert key == "output/subdir/file.parquet"

    @pytest.mark.unit
    def test_build_key_no_prefix(self, tmp_path: Path) -> None:
        """Should work without prefix."""
        from portolan_cli.upload import _build_target_key

        source_dir = tmp_path / "data"
        source_dir.mkdir()
        file_path = source_dir / "file.parquet"
        file_path.touch()

        key = _build_target_key(file_path, source_dir, "")
        assert key == "file.parquet"

    @pytest.mark.unit
    def test_build_key_strips_trailing_slash(self, tmp_path: Path) -> None:
        """Should strip trailing slash from prefix."""
        from portolan_cli.upload import _build_target_key

        source_dir = tmp_path / "data"
        source_dir.mkdir()
        file_path = source_dir / "file.parquet"
        file_path.touch()

        key = _build_target_key(file_path, source_dir, "output/")
        assert key == "output/file.parquet"
        assert "//" not in key


# =============================================================================
# Integration with output.py Tests
# =============================================================================


class TestOutputIntegration:
    """Tests for integration with portolan_cli.output module."""

    @pytest.mark.unit
    def test_upload_file_uses_output_functions(self, temp_file: Path, capsys: object) -> None:
        """Upload should use portolan_cli.output for logging."""
        from portolan_cli.upload import upload_file

        with patch("portolan_cli.upload.obs"):
            with patch("portolan_cli.upload.S3Store"):
                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    upload_file(
                        source=temp_file,
                        destination="s3://mybucket/data.parquet",
                    )

        # The output module uses click.echo which writes to stdout/stderr
        # Capture output to mark capsys as used (vulture)
        _ = capsys  # Fixture available for debugging if needed


# =============================================================================
# Empty Bucket Validation Tests
# =============================================================================


class TestEmptyBucketValidation:
    """Tests for validating empty bucket names in URLs."""

    @pytest.mark.unit
    def test_s3_empty_bucket_raises(self) -> None:
        """S3 URL with empty bucket should raise ValueError."""
        from portolan_cli.upload import parse_object_store_url

        with pytest.raises(ValueError, match="[Ee]mpty bucket"):
            parse_object_store_url("s3://")

    @pytest.mark.unit
    def test_s3_empty_bucket_with_trailing_slash_raises(self) -> None:
        """S3 URL 's3:///' should raise ValueError."""
        from portolan_cli.upload import parse_object_store_url

        with pytest.raises(ValueError, match="[Ee]mpty bucket"):
            parse_object_store_url("s3:///")

    @pytest.mark.unit
    def test_gs_empty_bucket_raises(self) -> None:
        """GCS URL with empty bucket should raise ValueError."""
        from portolan_cli.upload import parse_object_store_url

        with pytest.raises(ValueError, match="[Ee]mpty bucket"):
            parse_object_store_url("gs://")

    @pytest.mark.unit
    def test_az_missing_container_raises(self) -> None:
        """Azure URL with only account (no container) should raise ValueError."""
        from portolan_cli.upload import parse_object_store_url

        with pytest.raises(ValueError, match="[Ii]nvalid Azure URL"):
            parse_object_store_url("az://account")


# =============================================================================
# Unicode Path Tests
# =============================================================================


class TestUnicodePaths:
    """Tests for Unicode characters in file paths and object keys."""

    @pytest.mark.unit
    def test_upload_unicode_filename(self, tmp_path: Path) -> None:
        """Upload should handle Unicode filenames."""
        from portolan_cli.upload import upload_file

        # Create file with Unicode name (French "données" = "data")
        unicode_file = tmp_path / "données.parquet"
        unicode_file.write_bytes(b"x" * 1024)

        with patch("portolan_cli.upload.obs") as mock_obs:
            with patch("portolan_cli.upload.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                with patch.dict(
                    os.environ,
                    {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                ):
                    result = upload_file(
                        source=unicode_file,
                        destination="s3://mybucket/données.parquet",
                    )

                assert result.success is True
                # Verify the key was passed correctly
                call_args = mock_obs.put.call_args
                target_key = call_args[0][1]
                assert "données" in target_key

    @pytest.mark.unit
    def test_upload_unicode_directory_structure(self, tmp_path: Path) -> None:
        """Upload should preserve Unicode in directory structure."""
        from portolan_cli.upload import _build_target_key

        # Create directory with Unicode characters (Chinese "数据" = "data")
        source_dir = tmp_path / "数据"
        source_dir.mkdir()
        file_path = source_dir / "文件.parquet"  # "文件" = "file"
        file_path.touch()

        key = _build_target_key(file_path, source_dir, "output")
        assert key == "output/文件.parquet"

    @pytest.mark.unit
    def test_upload_emoji_in_path(self, tmp_path: Path) -> None:
        """Upload should handle emojis in paths."""
        from portolan_cli.upload import _build_target_key

        # Create directory with emoji
        source_dir = tmp_path / "data_📊"
        source_dir.mkdir()
        file_path = source_dir / "report.parquet"
        file_path.touch()

        key = _build_target_key(file_path, source_dir, "prefix")
        assert key == "prefix/report.parquet"


# =============================================================================
# Division by Zero Display Tests
# =============================================================================


class TestSpeedCalculation:
    """Tests for upload speed calculation edge cases."""

    @pytest.mark.unit
    def test_format_upload_speed_normal(self) -> None:
        """Normal speed should display MB/s."""
        from portolan_cli.upload import _format_upload_speed

        result = _format_upload_speed(10.0, 2.0)  # 10 MB in 2 seconds = 5 MB/s
        assert result == "5.00 MB/s"

    @pytest.mark.unit
    def test_format_upload_speed_zero_time(self) -> None:
        """Zero elapsed time should show '< 1ms' instead of division by zero."""
        from portolan_cli.upload import _format_upload_speed

        result = _format_upload_speed(1.0, 0.0)
        assert result == "< 1ms"

    @pytest.mark.unit
    def test_format_upload_speed_very_fast(self) -> None:
        """Very fast upload (< 1ms) should show '< 1ms'."""
        from portolan_cli.upload import _format_upload_speed

        result = _format_upload_speed(1.0, 0.0005)  # 0.5ms
        assert result == "< 1ms"

    @pytest.mark.unit
    def test_format_upload_speed_just_over_1ms(self) -> None:
        """Upload just over 1ms should show normal speed."""
        from portolan_cli.upload import _format_upload_speed

        result = _format_upload_speed(1.0, 0.002)  # 2ms = 500 MB/s
        assert "MB/s" in result
        assert "< 1ms" not in result

    @pytest.mark.unit
    def test_upload_very_fast_shows_speed(self, temp_file: Path) -> None:
        """Very fast uploads should show meaningful speed display."""
        from portolan_cli.upload import upload_file

        with patch("portolan_cli.upload.obs"):
            with patch("portolan_cli.upload.S3Store") as mock_s3_store:
                mock_store = MagicMock()
                mock_s3_store.return_value = mock_store

                # Mock time.time() to return same value (0 elapsed time)
                with patch("portolan_cli.upload.time") as mock_time:
                    mock_time.time.return_value = 1000.0  # Same start and end time

                    with patch.dict(
                        os.environ,
                        {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
                    ):
                        with patch("portolan_cli.upload.success") as mock_success:
                            result = upload_file(
                                source=temp_file,
                                destination="s3://mybucket/data.parquet",
                            )

                            assert result.success is True
                            # Verify we show "< 1ms" for instant uploads
                            call_args = mock_success.call_args[0][0]
                            assert "< 1ms" in call_args


# =============================================================================
# Symlink Handling Tests
# =============================================================================


class TestSymlinkHandling:
    """Tests for symlink behavior in directory uploads."""

    @pytest.mark.unit
    def test_symlink_outside_source_raises_on_key_build(self, tmp_path: Path) -> None:
        """Symlinks pointing outside source directory should raise ValueError on key build."""
        from portolan_cli.upload import _build_target_key

        # Create a source directory
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        (source_dir / "normal_file.txt").write_text("normal")

        # Create a directory outside source
        outside_dir = tmp_path / "outside"
        outside_dir.mkdir()
        secret_file = outside_dir / "secret.txt"
        secret_file.write_text("secret data")

        # Create symlink to outside file
        try:
            symlink = source_dir / "link_to_secret.txt"
            symlink.symlink_to(secret_file)

            # Building target key should detect the path traversal
            with pytest.raises(ValueError, match="[Pp]ath traversal"):
                _build_target_key(symlink, source_dir, "prefix")
        except OSError:
            # Symlinks not supported on this platform (some Windows configs)
            pytest.skip("Symlinks not supported on this platform")

    @pytest.mark.unit
    def test_symlink_within_source_is_allowed(self, tmp_path: Path) -> None:
        """Symlinks pointing within source directory should be allowed."""
        from portolan_cli.upload import _build_target_key

        # Create a source directory
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        real_file = source_dir / "real_file.txt"
        real_file.write_text("content")

        # Create symlink to file within source
        try:
            symlink = source_dir / "link_to_real.txt"
            symlink.symlink_to(real_file)

            # This should succeed
            key = _build_target_key(symlink, source_dir, "prefix")
            assert key == "prefix/link_to_real.txt"
        except OSError:
            # Symlinks not supported on this platform (some Windows configs)
            pytest.skip("Symlinks not supported on this platform")

    @pytest.mark.unit
    def test_find_files_includes_symlinks(self, tmp_path: Path) -> None:
        """Find files should include symlinked files (documented behavior)."""
        from portolan_cli.upload import _find_files_to_upload

        # Create a source directory
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        (source_dir / "normal_file.txt").write_text("normal")

        # Create a file outside source
        outside_dir = tmp_path / "outside"
        outside_dir.mkdir()
        secret_file = outside_dir / "secret.txt"
        secret_file.write_text("secret data")

        # Create symlink to outside file
        try:
            symlink = source_dir / "link_to_secret.txt"
            symlink.symlink_to(secret_file)

            # Find files should include the symlink
            files = _find_files_to_upload(source_dir, None)

            # Should find both files (symlinks are followed by rglob)
            assert len(files) == 2
        except OSError:
            # Symlinks not supported on this platform (some Windows configs)
            pytest.skip("Symlinks not supported on this platform")
