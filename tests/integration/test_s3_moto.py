"""Moto-based S3 integration tests.

These tests use moto's server mode to create a real HTTP endpoint that works
with obstore (Rust-based S3 client). This is necessary because obstore makes
direct HTTP calls and doesn't integrate with boto3's patching mechanism.

This addresses the unchecked test plan item:
  [ ] Integration tests with real S3 bucket
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from collections.abc import Generator
from pathlib import Path

import pytest

# Import moto - will skip tests if not installed
moto = pytest.importorskip("moto")
boto3 = pytest.importorskip("boto3")

from moto.server import ThreadedMotoServer  # noqa: E402

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def moto_server() -> Generator[str, None, None]:
    """Start a moto server that provides a real HTTP endpoint.

    This is necessary because obstore (Rust-based S3 client) makes direct
    HTTP calls and doesn't integrate with boto3's patching mechanism.
    """
    # Start moto server on a random available port
    # Use 127.0.0.1 explicitly - Windows can't bind to 0.0.0.0
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()

    # Get the actual port
    host, port = server.get_host_and_port()
    endpoint_url = f"http://{host}:{port}"

    yield endpoint_url

    server.stop()


@pytest.fixture
def s3_bucket(moto_server: str) -> Generator[tuple[str, str], None, None]:
    """Create a mock S3 bucket using the moto server.

    Uses a unique bucket name per test to ensure isolation.
    Returns (bucket_name, endpoint_url) tuple.
    """
    # Unique bucket name per test for isolation
    bucket_name = f"test-bucket-{uuid.uuid4().hex[:8]}"

    # Create bucket using boto3 pointed at moto server
    client = boto3.client(
        "s3",
        endpoint_url=moto_server,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        region_name="us-east-1",
    )
    client.create_bucket(Bucket=bucket_name)

    yield bucket_name, moto_server


@pytest.fixture
def catalog_with_data(tmp_path: Path, s3_bucket: tuple[str, str]) -> Path:
    """Create a local catalog with test data."""
    bucket_name, endpoint_url = s3_bucket

    catalog_dir = tmp_path / "catalog"
    catalog_dir.mkdir()

    # Create .portolan directory
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text(f"version: 1\nremote: s3://{bucket_name}/catalog\n")

    # Create catalog.json
    (catalog_dir / "catalog.json").write_text(
        json.dumps(
            {
                "type": "Catalog",
                "id": "test-catalog",
                "stac_version": "1.0.0",
                "description": "Test catalog for moto integration tests",
                "links": [
                    {"rel": "self", "href": "./catalog.json"},
                    {"rel": "child", "href": "./test-collection/collection.json"},
                ],
            }
        )
    )

    # Create test collection
    collection_dir = catalog_dir / "test-collection"
    collection_dir.mkdir()

    (collection_dir / "collection.json").write_text(
        json.dumps(
            {
                "type": "Collection",
                "id": "test-collection",
                "stac_version": "1.0.0",
                "description": "Test collection",
                "license": "CC0-1.0",
                "extent": {
                    "spatial": {"bbox": [[-180, -90, 180, 90]]},
                    "temporal": {"interval": [[None, None]]},
                },
                "links": [],
            }
        )
    )

    # Create test data file
    test_data = b"Test parquet data content for integration testing"
    (collection_dir / "data.parquet").write_bytes(test_data)

    # Create versions.json with computed hash (must match actual schema)
    sha256 = hashlib.sha256(test_data).hexdigest()
    (collection_dir / "versions.json").write_text(
        json.dumps(
            {
                "spec_version": "1.0.0",
                "current_version": "1.0.0",
                "versions": [
                    {
                        "version": "1.0.0",
                        "created": "2024-01-01T00:00:00Z",
                        "breaking": False,
                        "changes": ["data.parquet"],
                        "assets": {
                            "data.parquet": {
                                "sha256": sha256,
                                "size_bytes": len(test_data),
                                "href": "test-collection/data.parquet",
                            }
                        },
                    }
                ],
            }
        )
    )

    return catalog_dir


@pytest.fixture
def nested_catalog_with_data(tmp_path: Path, s3_bucket: tuple[str, str]) -> Path:
    """A nested catalog (ADR-0032) with intermediate catalog.json files (Issue #547, #552)."""
    bucket_name, endpoint_url = s3_bucket

    catalog_dir = tmp_path / "catalog"
    (catalog_dir / ".portolan").mkdir(parents=True)
    (catalog_dir / ".portolan" / "config.yaml").write_text(
        f"version: 1\nremote: s3://{bucket_name}/catalog\n"
    )

    def _catalog_json(path: Path, catalog_id: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": catalog_id,
                    "stac_version": "1.1.0",
                    "description": f"Catalog: {catalog_id}",
                    "links": [],
                }
            )
        )

    _catalog_json(catalog_dir / "catalog.json", "root")
    _catalog_json(catalog_dir / "tst" / "catalog.json", "tst")
    _catalog_json(catalog_dir / "tst" / "latest" / "catalog.json", "tst/latest")

    for layer in ("adm0", "adm1"):
        leaf = catalog_dir / "tst" / "latest" / layer
        leaf.mkdir(parents=True)
        data = f"parquet-{layer}".encode()
        (leaf / f"{layer}.parquet").write_bytes(data)
        (leaf / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": f"tst/latest/{layer}",
                    "stac_version": "1.1.0",
                    "description": f"Collection {layer}",
                    "license": "CC0-1.0",
                    "extent": {
                        "spatial": {"bbox": [[-180, -90, 180, 90]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                }
            )
        )
        (leaf / "versions.json").write_text(
            json.dumps(
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [
                        {
                            "version": "1.0.0",
                            "created": "2024-01-01T00:00:00Z",
                            "breaking": False,
                            "changes": [f"{layer}.parquet"],
                            "assets": {
                                f"{layer}.parquet": {
                                    "sha256": hashlib.sha256(data).hexdigest(),
                                    "size_bytes": len(data),
                                    "href": f"tst/latest/{layer}/{layer}.parquet",
                                }
                            },
                        }
                    ],
                }
            )
        )

    return catalog_dir


@pytest.fixture
def _aws_env(s3_bucket: tuple[str, str]) -> Generator[None, None, None]:
    """Set up AWS environment variables for obstore to use moto server.

    Note: Named with underscore prefix to indicate it's used for side effects only.
    """
    bucket_name, endpoint_url = s3_bucket

    old_env = {
        "AWS_ENDPOINT_URL": os.environ.get("AWS_ENDPOINT_URL"),
        "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY"),
        "AWS_DEFAULT_REGION": os.environ.get("AWS_DEFAULT_REGION"),
        "AWS_ALLOW_HTTP": os.environ.get("AWS_ALLOW_HTTP"),
    }

    os.environ["AWS_ENDPOINT_URL"] = endpoint_url
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["AWS_ALLOW_HTTP"] = "true"  # Required for http:// endpoints

    yield

    # Restore original env
    for key, value in old_env.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


# =============================================================================
# Push Integration Tests
# =============================================================================


class TestPushS3Integration:
    """Integration tests for push to S3 using moto server."""

    @pytest.mark.integration
    def test_push_uploads_files_to_s3(
        self,
        s3_bucket: tuple[str, str],
        catalog_with_data: Path,
        _aws_env: None,
    ) -> None:
        """Push should upload files to the S3 bucket."""
        bucket_name, endpoint_url = s3_bucket

        from portolan_cli.push import push

        result = push(
            catalog_root=catalog_with_data,
            collection="test-collection",
            destination=f"s3://{bucket_name}/catalog",
        )

        assert result.success is True, f"Push failed: {result.errors}"
        assert result.files_uploaded > 0

        # Verify files were actually uploaded to mock S3
        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            region_name="us-east-1",
        )
        response = client.list_objects_v2(Bucket=bucket_name, Prefix="catalog/")
        uploaded_keys = [obj["Key"] for obj in response.get("Contents", [])]

        # Should have uploaded versions.json and data.parquet
        assert any("versions.json" in key for key in uploaded_keys)
        assert any("data.parquet" in key for key in uploaded_keys)

    @pytest.mark.integration
    def test_push_incremental_only_uploads_new_files(
        self,
        s3_bucket: tuple[str, str],
        catalog_with_data: Path,
        _aws_env: None,
    ) -> None:
        """Issue #329: Push should only upload new/changed files."""
        bucket_name, endpoint_url = s3_bucket

        from portolan_cli.push import push

        # First push - should upload everything (fresh bucket)
        result1 = push(
            catalog_root=catalog_with_data,
            collection="test-collection",
            destination=f"s3://{bucket_name}/catalog",
        )
        assert result1.success is True, f"First push failed: {result1.errors}"
        # First push to fresh bucket should upload files
        assert result1.files_uploaded > 0, "First push should upload files to empty bucket"
        initial_uploads = result1.files_uploaded

        # Add a new file locally
        new_content = b"New file content"
        new_sha256 = hashlib.sha256(new_content).hexdigest()
        collection_dir = catalog_with_data / "test-collection"
        (collection_dir / "new_file.parquet").write_bytes(new_content)

        # Update versions.json with new version containing both files
        versions_path = collection_dir / "versions.json"
        versions_data = json.loads(versions_path.read_text())

        # Get existing assets from version 1.0.0
        existing_assets = versions_data["versions"][0]["assets"].copy()

        # Add new file
        existing_assets["new_file.parquet"] = {
            "sha256": new_sha256,
            "size_bytes": len(new_content),
            "href": "test-collection/new_file.parquet",
        }

        # Add version 2.0.0
        versions_data["versions"].append(
            {
                "version": "2.0.0",
                "created": "2024-02-01T00:00:00Z",
                "breaking": False,
                "changes": ["new_file.parquet"],
                "assets": existing_assets,
            }
        )
        versions_data["current_version"] = "2.0.0"
        versions_path.write_text(json.dumps(versions_data))

        # Second push - should only upload the NEW file (not re-upload unchanged files)
        result2 = push(
            catalog_root=catalog_with_data,
            collection="test-collection",
            destination=f"s3://{bucket_name}/catalog",
        )

        assert result2.success is True, f"Second push failed: {result2.errors}"
        # Key assertion: second push uploads fewer or equal files than initial
        # (ideally just new_file.parquet + metadata, not data.parquet again)
        assert result2.files_uploaded <= initial_uploads, (
            f"Second push ({result2.files_uploaded} files) should not upload more "
            f"than initial push ({initial_uploads} files)"
        )

    @pytest.mark.integration
    def test_push_uploads_intermediate_catalogs_to_s3(
        self,
        s3_bucket: tuple[str, str],
        nested_catalog_with_data: Path,
        _aws_env: None,
    ) -> None:
        """Issue #547/#552: catalog-wide push lands intermediate catalog.json in the bucket.

        The push->bucket round-trip is the guard against the silent-drop class:
        a client walking child links from the root must find every intermediate
        catalog.json, not just root catalog.json and leaf collection.json.
        """
        bucket_name, endpoint_url = s3_bucket

        from portolan_cli.push import push_all_collections

        result = push_all_collections(
            catalog_root=nested_catalog_with_data,
            destination=f"s3://{bucket_name}/catalog",
            force=False,
            dry_run=False,
            profile=None,
        )
        assert result.success is True, f"Push failed: {result}"

        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            region_name="us-east-1",
        )
        response = client.list_objects_v2(Bucket=bucket_name, Prefix="catalog/")
        uploaded_keys = {obj["Key"] for obj in response.get("Contents", [])}

        # The two intermediate catalogs that every prior code path dropped.
        assert "catalog/tst/catalog.json" in uploaded_keys, uploaded_keys
        assert "catalog/tst/latest/catalog.json" in uploaded_keys, uploaded_keys
        # Sanity: root and leaves still land too.
        assert "catalog/catalog.json" in uploaded_keys, uploaded_keys
        assert "catalog/tst/latest/adm0/collection.json" in uploaded_keys, uploaded_keys
        assert "catalog/tst/latest/adm1/collection.json" in uploaded_keys, uploaded_keys


# =============================================================================
# Pull Integration Tests
# =============================================================================


class TestPullS3Integration:
    """Integration tests for pull from S3 using moto server."""

    @pytest.mark.integration
    def test_pull_downloads_files_from_s3(
        self,
        s3_bucket: tuple[str, str],
        catalog_with_data: Path,
        _aws_env: None,
    ) -> None:
        """Pull should download files from S3."""
        bucket_name, endpoint_url = s3_bucket

        from portolan_cli.pull import pull
        from portolan_cli.push import push

        # First push to populate S3
        push_result = push(
            catalog_root=catalog_with_data,
            collection="test-collection",
            destination=f"s3://{bucket_name}/catalog",
        )
        assert push_result.success is True, f"Push failed: {push_result.errors}"

        # Create a new empty directory for pull target
        pull_target = catalog_with_data.parent / "pulled"
        pull_target.mkdir()

        # Initialize the target with minimal structure
        portolan_dir = pull_target / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("version: 1\n")

        collection_dir = pull_target / "test-collection"
        collection_dir.mkdir()

        # Pull from S3
        pull_result = pull(
            remote_url=f"s3://{bucket_name}/catalog",
            local_root=pull_target,
            collection="test-collection",
        )

        assert pull_result.success is True, f"Pull failed with up_to_date={pull_result.up_to_date}"
        assert pull_result.files_downloaded > 0

        # Verify files exist locally
        assert (collection_dir / "versions.json").exists()


# =============================================================================
# Restore Integration Tests (#325)
# =============================================================================


class TestRestoreS3Integration:
    """Integration tests for --restore flag with S3."""

    @pytest.mark.integration
    def test_restore_redownloads_missing_files(
        self,
        s3_bucket: tuple[str, str],
        catalog_with_data: Path,
        _aws_env: None,
    ) -> None:
        """Issue #325: --restore should re-download missing local files."""
        bucket_name, endpoint_url = s3_bucket

        from portolan_cli.pull import pull
        from portolan_cli.push import push

        # Push to S3
        push_result = push(
            catalog_root=catalog_with_data,
            collection="test-collection",
            destination=f"s3://{bucket_name}/catalog",
        )
        assert push_result.success is True, f"Push failed: {push_result.errors}"

        # Delete local data file (simulating accidental deletion)
        data_file = catalog_with_data / "test-collection" / "data.parquet"
        assert data_file.exists()
        data_file.unlink()
        assert not data_file.exists()

        # Regular pull should say "up to date" (versions match)
        pull_result1 = pull(
            remote_url=f"s3://{bucket_name}/catalog",
            local_root=catalog_with_data,
            collection="test-collection",
            restore=False,
        )
        assert pull_result1.up_to_date is True
        assert not data_file.exists()  # Still missing!

        # Pull with --restore should re-download the missing file
        pull_result2 = pull(
            remote_url=f"s3://{bucket_name}/catalog",
            local_root=catalog_with_data,
            collection="test-collection",
            restore=True,
        )

        assert pull_result2.success is True
        assert pull_result2.files_restored > 0 or pull_result2.files_downloaded > 0
        assert data_file.exists()  # Restored!
