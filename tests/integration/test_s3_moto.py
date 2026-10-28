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
from typing import TYPE_CHECKING

import pytest

# The moto_server, s3_bucket, and _aws_env fixtures live in tests/conftest.py.
boto3 = pytest.importorskip("boto3")

if TYPE_CHECKING:
    from pathlib import Path

    from tests.integration.conftest import Bucket, PutFaults

# =============================================================================
# Fixtures
# =============================================================================


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
    """A nested catalog with intermediate catalog.json files (Issue #547, #552)."""
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

        from portolan_cli.sync.push import push

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

        from portolan_cli.sync.push import push

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
    def test_push_restores_bytes_of_an_older_version_on_s3(
        self,
        s3_bucket: tuple[str, str],
        catalog_with_data: Path,
        _aws_env: None,
    ) -> None:
        """Issue #962: bytes A, then B, then A again. The store must hold A.

        Every version writes the same object key. Before the fix, the third
        push found A in remote version 1.0.0 and skipped the upload. The
        store kept B while the remote current version recorded A.
        """
        bucket_name, endpoint_url = s3_bucket

        from portolan_cli.sync.push import push

        collection_dir = catalog_with_data / "test-collection"
        data_file = collection_dir / "data.parquet"
        versions_path = collection_dir / "versions.json"
        bytes_a = data_file.read_bytes()
        bytes_b = b"Bytes B are longer, so a HEAD shows which bytes the key holds"

        def push_version(version: str, content: bytes) -> None:
            data_file.write_bytes(content)
            versions_data = json.loads(versions_path.read_text())
            if version != "1.0.0":
                versions_data["versions"].append(
                    {
                        "version": version,
                        "created": "2024-02-01T00:00:00Z",
                        "breaking": False,
                        "changes": ["data.parquet"],
                        "assets": {
                            "data.parquet": {
                                "sha256": hashlib.sha256(content).hexdigest(),
                                "size_bytes": len(content),
                                "href": "test-collection/data.parquet",
                            }
                        },
                    }
                )
                versions_data["current_version"] = version
                versions_path.write_text(json.dumps(versions_data))
            result = push(
                catalog_root=catalog_with_data,
                collection="test-collection",
                destination=f"s3://{bucket_name}/catalog",
            )
            assert result.success is True, f"Push {version} failed: {result.errors}"

        push_version("1.0.0", bytes_a)
        push_version("1.0.1", bytes_b)
        push_version("1.0.2", bytes_a)

        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            region_name="us-east-1",
        )
        key = "catalog/test-collection/data.parquet"
        stored = client.get_object(Bucket=bucket_name, Key=key)["Body"].read()
        remote_versions = json.loads(
            client.get_object(Bucket=bucket_name, Key="catalog/test-collection/versions.json")[
                "Body"
            ].read()
        )

        assert remote_versions["current_version"] == "1.0.2"
        assert remote_versions["versions"][-1]["assets"]["data.parquet"]["sha256"] == (
            hashlib.sha256(bytes_a).hexdigest()
        )
        assert stored == bytes_a

    @pytest.mark.integration
    def test_force_push_without_new_versions_restores_bytes_on_s3(
        self,
        s3_bucket: tuple[str, str],
        catalog_with_data: Path,
        _aws_env: None,
    ) -> None:
        """Issue #962: a force push with no new local version must upload A.

        The remote holds 1.0.0 (A) and 1.0.1 (B), so the store holds B. Local
        holds only 1.0.0 (A). `push --force` writes the local versions.json,
        so the store must hold A after it.
        """
        bucket_name, endpoint_url = s3_bucket

        from portolan_cli.sync.push import push

        collection_dir = catalog_with_data / "test-collection"
        data_file = collection_dir / "data.parquet"
        versions_path = collection_dir / "versions.json"
        destination = f"s3://{bucket_name}/catalog"
        bytes_a = data_file.read_bytes()
        versions_a = versions_path.read_text()
        bytes_b = b"Bytes B are longer, so a HEAD shows which bytes the key holds"

        assert push(catalog_with_data, "test-collection", destination).success is True

        versions_data = json.loads(versions_a)
        versions_data["versions"].append(
            {
                "version": "1.0.1",
                "created": "2024-02-01T00:00:00Z",
                "breaking": False,
                "changes": ["data.parquet"],
                "assets": {
                    "data.parquet": {
                        "sha256": hashlib.sha256(bytes_b).hexdigest(),
                        "size_bytes": len(bytes_b),
                        "href": "test-collection/data.parquet",
                    }
                },
            }
        )
        versions_data["current_version"] = "1.0.1"
        versions_path.write_text(json.dumps(versions_data))
        data_file.write_bytes(bytes_b)
        assert push(catalog_with_data, "test-collection", destination).success is True

        # Local goes back to 1.0.0 (A) only. Remote 1.0.1 is not local.
        versions_path.write_text(versions_a)
        data_file.write_bytes(bytes_a)
        result = push(catalog_with_data, "test-collection", destination, force=True)
        assert result.success is True, f"Force push failed: {result.errors}"

        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            region_name="us-east-1",
        )
        stored = client.get_object(Bucket=bucket_name, Key="catalog/test-collection/data.parquet")[
            "Body"
        ].read()
        remote_versions = json.loads(
            client.get_object(Bucket=bucket_name, Key="catalog/test-collection/versions.json")[
                "Body"
            ].read()
        )

        assert remote_versions["current_version"] == "1.0.0"
        assert stored == bytes_a

    @pytest.mark.integration
    def test_verify_push_repairs_a_damaged_object_without_new_version(
        self,
        s3_bucket: tuple[str, str],
        catalog_with_data: Path,
        _aws_env: None,
        fault_store: PutFaults,
    ) -> None:
        """Issue #966: `push --verify` repairs an object that disagrees with the manifest.

        The store holds other bytes of another size under the asset key. The
        manifests list the same versions, and no local version is new. A plain
        push skips the asset. `push --verify` must upload it and must not write
        versions.json. The remote manifest carries a marker key that the local
        one lacks, so a write of the local manifest would drop it.
        """
        bucket_name, endpoint_url = s3_bucket

        from portolan_cli.sync.push import push

        data_file = catalog_with_data / "test-collection" / "data.parquet"
        destination = f"s3://{bucket_name}/catalog"
        data_key = "catalog/test-collection/data.parquet"
        versions_key = "catalog/test-collection/versions.json"
        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            region_name="us-east-1",
        )

        assert push(catalog_with_data, "test-collection", destination).success is True
        remote_manifest = json.loads(
            client.get_object(Bucket=bucket_name, Key=versions_key)["Body"].read()
        )
        remote_manifest["x-remote-marker"] = "kept"
        versions_before = json.dumps(remote_manifest).encode()
        client.put_object(Bucket=bucket_name, Key=versions_key, Body=versions_before)

        # Damage the store the way the #962 bug did. Other bytes, other size.
        client.put_object(Bucket=bucket_name, Key=data_key, Body=b"stale bytes")

        plain = push(catalog_with_data, "test-collection", destination)
        assert plain.success is True
        assert plain.verify is None
        stored = client.get_object(Bucket=bucket_name, Key=data_key)["Body"].read()
        assert stored == b"stale bytes"

        result = push(catalog_with_data, "test-collection", destination, verify=True)
        assert result.success is True, f"Verify push failed: {result.errors}"
        assert result.versions_pushed == 0
        assert result.verify is not None
        assert (result.verify.checked, result.verify.mismatched) == (1, 1)

        stored = client.get_object(Bucket=bucket_name, Key=data_key)["Body"].read()
        versions_after = client.get_object(Bucket=bucket_name, Key=versions_key)["Body"].read()
        assert stored == data_file.read_bytes()
        assert versions_after == versions_before
        assert json.loads(versions_after)["x-remote-marker"] == "kept"

        # The store now agrees, so a second verify uploads no asset.
        asset_puts = fault_store.keys().count(data_key)
        again = push(catalog_with_data, "test-collection", destination, verify=True)
        assert again.success is True, again.errors
        assert fault_store.keys().count(data_key) == asset_puts
        assert again.verify is not None
        assert (again.verify.checked, again.verify.mismatched) == (1, 0)

    @pytest.mark.integration
    @pytest.mark.parametrize("missing", [False, True], ids=["existing", "missing"])
    def test_verify_repair_preserves_a_concurrent_s3_publication(
        self, catalog_with_data: Path, bucket: Bucket, fault_store: PutFaults, missing: bool
    ) -> None:
        """An S3 writer that lands between verify's HEAD and PUT keeps its publication."""
        from portolan_cli.sync.push import push

        root = catalog_with_data
        assert push(root, "test-collection", bucket.url).success is True
        key = bucket.key("test-collection/data.parquet")
        manifest_key = bucket.key("test-collection/versions.json")
        if missing:
            bucket.client.delete_object(Bucket=bucket.name, Key=key)
        else:
            bucket.second_writer(key, b"damaged")
        newer = b"another writer's asset bytes"
        winner = json.loads((root / "test-collection" / "versions.json").read_text())
        latest = winner["versions"][-1]
        winner["versions"].append(
            {
                **latest,
                "version": "1.0.1",
                "assets": {
                    name: {
                        **entry,
                        "sha256": hashlib.sha256(newer).hexdigest(),
                        "size_bytes": len(newer),
                    }
                    for name, entry in latest["assets"].items()
                },
            }
        )
        winner["current_version"] = "1.0.1"
        winner_bytes = json.dumps(winner).encode()

        def publish() -> None:
            bucket.second_writer(key, newer)
            bucket.second_writer(manifest_key, winner_bytes)

        fault_store.arm(1, when=lambda target: target == key, action=publish)
        result = push(root, "test-collection", bucket.url, verify=True)

        assert result.success is False, "repair overwrote a concurrent publication"
        assert result.files_uploaded == 0
        for target, expected in [(key, newer), (manifest_key, winner_bytes)]:
            assert (
                bucket.client.get_object(Bucket=bucket.name, Key=target)["Body"].read() == expected
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

        from portolan_cli.sync.push import push_all_collections

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

    @pytest.mark.integration
    def test_single_collection_push_uploads_intermediate_catalogs_to_s3(
        self,
        s3_bucket: tuple[str, str],
        nested_catalog_with_data: Path,
        _aws_env: None,
    ) -> None:
        """Issue #547/#552: the SINGLE-collection push path also lands intermediates.

        Distinct code path from the catalog-wide test above: single-collection
        push discovers intermediates via ``_discover_stac_files(include_catalog=True)``,
        not the central ``_push_all_upload_root_files`` phase. Both must upload the
        ancestor catalog.json, or a targeted ``push <collection>`` still leaves the
        remote child-link chain broken.
        """
        bucket_name, endpoint_url = s3_bucket

        from portolan_cli.sync.push import push

        result = push(
            catalog_root=nested_catalog_with_data,
            collection="tst/latest/adm0",
            destination=f"s3://{bucket_name}/catalog",
            force=False,
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

        assert "catalog/tst/catalog.json" in uploaded_keys, uploaded_keys
        assert "catalog/tst/latest/catalog.json" in uploaded_keys, uploaded_keys
        assert "catalog/catalog.json" in uploaded_keys, uploaded_keys
        assert "catalog/tst/latest/adm0/collection.json" in uploaded_keys, uploaded_keys

    @pytest.mark.integration
    def test_force_refreshes_regenerated_collection_json_on_s3(
        self,
        s3_bucket: tuple[str, str],
        nested_catalog_with_data: Path,
        _aws_env: None,
    ) -> None:
        """Issue #496: --force re-uploads regenerated metadata with NO version delta.

        Full round-trip against the bucket, reading actual bytes back:
        1. push -> remote collection.json holds the original description;
        2. regenerate collection.json in place (no version bump, versions.json
           unchanged), push WITHOUT force -> "Nothing to push", remote unchanged
           (the gate still guards against churn);
        3. push WITH force -> remote collection.json now holds the new bytes.
        """
        bucket_name, endpoint_url = s3_bucket

        from portolan_cli.sync.push import push

        collection = "tst/latest/adm0"
        destination = f"s3://{bucket_name}/catalog"
        coll_json = nested_catalog_with_data / "tst" / "latest" / "adm0" / "collection.json"

        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            region_name="us-east-1",
        )

        def remote_collection_json() -> str:
            obj = client.get_object(Bucket=bucket_name, Key=f"catalog/{collection}/collection.json")
            body: str = obj["Body"].read().decode()
            return body

        # 1. Initial push: remote gets the original description.
        assert (
            push(
                catalog_root=nested_catalog_with_data,
                collection=collection,
                destination=destination,
                force=False,
            ).success
            is True
        )
        assert "Collection adm0" in remote_collection_json()

        # 2. Regenerate collection.json locally (no version bump).
        doc = json.loads(coll_json.read_text())
        doc["description"] = "REGENERATED DESCRIPTION"
        coll_json.write_text(json.dumps(doc))

        # No-force push is a no-op: the remote keeps the ORIGINAL bytes.
        assert (
            push(
                catalog_root=nested_catalog_with_data,
                collection=collection,
                destination=destination,
                force=False,
            ).success
            is True
        )
        assert "REGENERATED DESCRIPTION" not in remote_collection_json()

        # 3. Force push refreshes metadata even though the version list is identical.
        assert (
            push(
                catalog_root=nested_catalog_with_data,
                collection=collection,
                destination=destination,
                force=True,
            ).success
            is True
        )
        assert "REGENERATED DESCRIPTION" in remote_collection_json()


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

        from portolan_cli.sync.pull import pull
        from portolan_cli.sync.push import push

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

        from portolan_cli.sync.pull import pull
        from portolan_cli.sync.push import push

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
