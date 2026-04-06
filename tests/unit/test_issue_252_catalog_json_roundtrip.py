"""Unit tests for Issue #252 - Full Catalog Push/Pull Sync.

Tests that push uploads ALL STAC metadata files (catalog.json, collection.json,
{item_id}.json) in addition to assets and versions.json.

Design principle from ADR-0006: Portolan owns bucket contents.
The catalog should round-trip perfectly: push -> clone should recreate the catalog.

Test categories:
- Push uploads catalog.json so standalone push creates clonable remote catalog
- Push uploads collection.json for each collection
- Push uploads {item_id}.json for each item (Portolan naming convention)
- push_all_collections uploads catalog.json at the end (when all collections succeed)
- Manifest-last pattern preserved (data first, then manifests)

Note: Both push() and push_all_collections() upload catalog.json. This ensures
standalone push() creates a complete catalog, while push_all_collections()
ensures a final consistent catalog.json when pushing multiple collections.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

# =============================================================================
# Test fixtures
# =============================================================================


@pytest.fixture
def full_catalog(tmp_path: Path) -> Path:
    """Create a complete catalog with catalog.json, collection.json, {item_id}.json.

    Uses Portolan's actual naming convention where item STAC files are named
    {item_id}.json (not item.json).

    Structure:
        catalog/
            .portolan/
                config.yaml
            catalog.json            <- root STAC catalog
            versions.json           <- root versions file
            test-collection/
                collection.json     <- STAC collection
                versions.json       <- collection versions
                test-item/
                    test-item.json  <- STAC item ({item_id}.json naming)
                    data.parquet    <- asset file
    """
    catalog_dir = tmp_path / "catalog"
    catalog_dir.mkdir()

    # Create .portolan directory (required for valid catalog)
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("# Portolan configuration\n")

    # Create catalog.json at root
    catalog_json = {
        "type": "Catalog",
        "id": "test-catalog",
        "stac_version": "1.0.0",
        "description": "Test catalog for Issue #252",
        "links": [
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            {"rel": "self", "href": "./catalog.json", "type": "application/json"},
            {
                "rel": "child",
                "href": "./test-collection/collection.json",
                "type": "application/json",
            },
        ],
    }
    (catalog_dir / "catalog.json").write_text(json.dumps(catalog_json, indent=2))

    # Create root versions.json (catalog-level)
    root_versions = {
        "schema_version": "1.0.0",
        "catalog_id": "test-catalog",
        "created": "2024-01-01T00:00:00Z",
        "collections": {},
    }
    (catalog_dir / "versions.json").write_text(json.dumps(root_versions, indent=2))

    # Create collection directory and files
    collection_dir = catalog_dir / "test-collection"
    collection_dir.mkdir()

    # Note: item link uses {item_id}.json naming (Portolan convention)
    collection_json = {
        "type": "Collection",
        "id": "test-collection",
        "stac_version": "1.0.0",
        "description": "Test collection",
        "license": "proprietary",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [[None, None]]},
        },
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            {"rel": "self", "href": "./collection.json", "type": "application/json"},
            {
                "rel": "item",
                "href": "./test-item/test-item.json",
                "type": "application/json",
            },
        ],
    }
    (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

    # Create collection versions.json
    collection_versions = {
        "spec_version": "1.0.0",
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-01T00:00:00Z",
                "breaking": False,
                "message": "Initial version",
                "assets": {
                    "data": {
                        "href": "test-collection/test-item/data.parquet",
                        "sha256": "abc123",
                        "size_bytes": 1024,
                    }
                },
            }
        ],
    }
    (collection_dir / "versions.json").write_text(json.dumps(collection_versions, indent=2))

    # Create item directory and files
    # Item ID = "test-item", so STAC file is "test-item.json" (Portolan convention)
    item_dir = collection_dir / "test-item"
    item_dir.mkdir()

    item_json = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "test-item",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-180, -90], [-180, 90], [180, 90], [180, -90], [-180, -90]]],
        },
        "bbox": [-180, -90, 180, 90],
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
        "links": [
            {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../collection.json", "type": "application/json"},
            {"rel": "self", "href": "./test-item.json", "type": "application/json"},
        ],
        "assets": {
            "data": {
                "href": "./data.parquet",
                "type": "application/x-parquet",
            }
        },
    }
    # Use {item_id}.json naming convention
    (item_dir / "test-item.json").write_text(json.dumps(item_json, indent=2))

    # Create asset file
    (item_dir / "data.parquet").write_bytes(b"fake parquet data" * 64)

    return catalog_dir


# =============================================================================
# Push: catalog.json upload tests
# =============================================================================


@pytest.mark.unit
class TestPushUploadsCatalogJson:
    """Tests that catalog.json is uploaded by both push() and push_all_collections().

    push() uploads catalog.json so standalone push creates a complete, clonable
    remote catalog. push_all_collections() also uploads catalog.json at the end
    (when all collections succeed) to ensure a final consistent state.
    """

    def test_per_collection_push_uploads_catalog_json(self, full_catalog: Path) -> None:
        """Per-collection push SHOULD upload catalog.json for standalone use."""
        from portolan_cli.push import push

        uploaded_keys: list[str] = []

        async def mock_put_async(store: Any, key: str, data: Any, **kwargs: Any) -> None:
            uploaded_keys.append(key)

        async def mock_get_async(store: Any, key: str) -> bytes:
            raise FileNotFoundError("Not found")

        with patch("portolan_cli.push.obs.put_async", side_effect=mock_put_async):
            with patch("portolan_cli.push.obs.get_async", side_effect=mock_get_async):
                result = push(
                    catalog_root=full_catalog,
                    collection="test-collection",
                    destination="s3://test-bucket/test-prefix",
                )

        # Verify catalog.json WAS uploaded by per-collection push
        catalog_json_keys = [k for k in uploaded_keys if k.endswith("catalog.json")]
        assert len(catalog_json_keys) == 1, (
            f"catalog.json SHOULD be uploaded by per-collection push: {uploaded_keys}"
        )
        assert catalog_json_keys[0] == "test-prefix/catalog.json"
        assert result.success

    def test_push_all_collections_uploads_catalog_json(self, full_catalog: Path) -> None:
        """push_all_collections uploads catalog.json at the end (may also be uploaded by push).

        Each push() call uploads catalog.json, and push_all_collections also uploads
        it at the end when all collections succeed. This ensures a consistent final state.
        """
        from portolan_cli.push import push_all_collections

        uploaded_keys: list[str] = []

        async def mock_put_async(store: Any, key: str, data: Any, **kwargs: Any) -> None:
            uploaded_keys.append(key)

        def mock_put_sync(store: Any, key: str, data: Any, **kwargs: Any) -> None:
            # Sync version for _upload_catalog_json helper
            uploaded_keys.append(key)

        with patch("portolan_cli.push.obs.put_async", side_effect=mock_put_async):
            with patch("portolan_cli.push.obs.put", side_effect=mock_put_sync):
                with patch("portolan_cli.push.obs.get_async", new_callable=AsyncMock) as mock_get:
                    mock_get.side_effect = FileNotFoundError("Not found")

                    result = push_all_collections(
                        catalog_root=full_catalog,
                        destination="s3://test-bucket/test-prefix",
                    )

        # Verify catalog.json was uploaded at least once (could be 2x: by push + push_all)
        catalog_json_keys = [k for k in uploaded_keys if k.endswith("catalog.json")]
        assert len(catalog_json_keys) >= 1, (
            f"Expected at least one catalog.json upload: {uploaded_keys}"
        )
        assert all(k == "test-prefix/catalog.json" for k in catalog_json_keys)
        assert result.success

    def test_push_all_catalog_json_content_matches_local(self, full_catalog: Path) -> None:
        """Uploaded catalog.json content should match local file."""
        from portolan_cli.push import push_all_collections

        uploaded_content: dict[str, bytes] = {}

        async def mock_put_async(store: Any, key: str, data: Any, **kwargs: Any) -> None:
            if isinstance(data, Path):
                uploaded_content[key] = data.read_bytes()
            else:
                uploaded_content[key] = bytes(data) if not isinstance(data, bytes) else data

        def mock_put_sync(store: Any, key: str, data: Any, **kwargs: Any) -> None:
            # Sync version for _push_all_upload_catalog helper
            if isinstance(data, Path):
                uploaded_content[key] = data.read_bytes()
            else:
                uploaded_content[key] = bytes(data) if not isinstance(data, bytes) else data

        with patch("portolan_cli.push.obs.put_async", side_effect=mock_put_async):
            with patch("portolan_cli.push.obs.put", side_effect=mock_put_sync):
                with patch("portolan_cli.push.obs.get_async", new_callable=AsyncMock) as mock_get:
                    mock_get.side_effect = FileNotFoundError("Not found")

                    push_all_collections(
                        catalog_root=full_catalog,
                        destination="s3://test-bucket/test-prefix",
                    )

        # Verify content matches
        local_content = (full_catalog / "catalog.json").read_bytes()
        assert "test-prefix/catalog.json" in uploaded_content
        assert uploaded_content["test-prefix/catalog.json"] == local_content


# =============================================================================
# Push: collection.json upload tests
# =============================================================================


@pytest.mark.unit
class TestPushUploadsCollectionJson:
    """Tests that push uploads collection.json for each collection."""

    def test_push_uploads_collection_json(self, full_catalog: Path) -> None:
        """Push should upload collection.json for the pushed collection."""
        from portolan_cli.push import push

        uploaded_keys: list[str] = []

        async def mock_put_async(store: Any, key: str, data: Any, **kwargs: Any) -> None:
            uploaded_keys.append(key)

        with patch("portolan_cli.push.obs.put_async", side_effect=mock_put_async):
            with patch("portolan_cli.push.obs.get_async", new_callable=AsyncMock) as mock_get:
                mock_get.side_effect = FileNotFoundError("Not found")

                result = push(
                    catalog_root=full_catalog,
                    collection="test-collection",
                    destination="s3://test-bucket/test-prefix",
                )

        # Verify collection.json was uploaded
        collection_json_keys = [k for k in uploaded_keys if k.endswith("collection.json")]
        assert len(collection_json_keys) == 1, (
            f"Expected collection.json upload, got: {uploaded_keys}"
        )
        assert collection_json_keys[0] == "test-prefix/test-collection/collection.json"
        assert result.success


# =============================================================================
# Push: item STAC file upload tests
# =============================================================================


@pytest.mark.unit
class TestPushUploadsItemStacFiles:
    """Tests that push uploads {item_id}.json for each item.

    Portolan uses the naming convention {item_id}.json for item STAC files,
    where item_id matches the item directory name.
    """

    def test_push_uploads_item_stac_file(self, full_catalog: Path) -> None:
        """Push should upload {item_id}.json for all items in the collection."""
        from portolan_cli.push import push

        uploaded_keys: list[str] = []

        async def mock_put_async(store: Any, key: str, data: Any, **kwargs: Any) -> None:
            uploaded_keys.append(key)

        with patch("portolan_cli.push.obs.put_async", side_effect=mock_put_async):
            with patch("portolan_cli.push.obs.get_async", new_callable=AsyncMock) as mock_get:
                mock_get.side_effect = FileNotFoundError("Not found")

                result = push(
                    catalog_root=full_catalog,
                    collection="test-collection",
                    destination="s3://test-bucket/test-prefix",
                )

        # Verify test-item.json was uploaded (Portolan naming: {item_id}.json)
        item_json_keys = [k for k in uploaded_keys if k.endswith("test-item.json")]
        assert len(item_json_keys) == 1, f"Expected test-item.json upload, got: {uploaded_keys}"
        assert item_json_keys[0] == "test-prefix/test-collection/test-item/test-item.json"
        assert result.success


# =============================================================================
# Push: manifest-last ordering tests
# =============================================================================


@pytest.mark.unit
class TestPushManifestLastOrdering:
    """Tests that push follows manifest-last pattern for atomicity."""

    def test_push_uploads_assets_before_manifests(self, full_catalog: Path) -> None:
        """Push should upload assets before any JSON manifests.

        Order for per-collection push should be:
        1. Asset files (data.parquet, etc.)
        2. {item_id}.json (leaf manifests)
        3. collection.json (intermediate manifests)
        4. catalog.json (root manifest)
        5. versions.json (last - makes the push "visible")
        """
        from portolan_cli.push import push

        upload_order: list[str] = []

        async def mock_put_async(store: Any, key: str, data: Any, **kwargs: Any) -> None:
            upload_order.append(key)

        with patch("portolan_cli.push.obs.put_async", side_effect=mock_put_async):
            with patch("portolan_cli.push.obs.get_async", new_callable=AsyncMock) as mock_get:
                mock_get.side_effect = FileNotFoundError("Not found")

                push(
                    catalog_root=full_catalog,
                    collection="test-collection",
                    destination="s3://test-bucket/test-prefix",
                )

        # Find indices
        def find_index(suffix: str) -> int:
            for i, key in enumerate(upload_order):
                if key.endswith(suffix):
                    return i
            return -1

        asset_idx = find_index("data.parquet")
        item_idx = find_index("test-item.json")  # Portolan naming: {item_id}.json
        collection_idx = find_index("collection.json")
        catalog_idx = find_index("catalog.json")
        versions_idx = find_index("versions.json")

        # Verify required files were uploaded
        assert asset_idx >= 0, f"data.parquet not uploaded: {upload_order}"
        assert item_idx >= 0, f"test-item.json not uploaded: {upload_order}"
        assert collection_idx >= 0, f"collection.json not uploaded: {upload_order}"
        assert catalog_idx >= 0, f"catalog.json not uploaded: {upload_order}"
        assert versions_idx >= 0, f"versions.json not uploaded: {upload_order}"

        # Verify order: assets < {item_id}.json < collection.json < catalog.json < versions.json
        assert asset_idx < item_idx, f"Assets should be uploaded before item STAC: {upload_order}"
        assert item_idx < collection_idx, (
            f"Item STAC should be uploaded before collection.json: {upload_order}"
        )
        assert collection_idx < catalog_idx, (
            f"collection.json should be uploaded before catalog.json: {upload_order}"
        )
        assert catalog_idx < versions_idx, (
            f"catalog.json should be uploaded before versions.json: {upload_order}"
        )


# =============================================================================
# Push: complete file list tests
# =============================================================================


@pytest.mark.unit
class TestPushUploadsAllFiles:
    """Tests that push uploads all required files for a complete catalog."""

    def test_per_collection_push_uploads_complete_structure(self, full_catalog: Path) -> None:
        """Per-collection push should upload complete STAC structure including catalog.json."""
        from portolan_cli.push import push

        uploaded_keys: set[str] = set()

        async def mock_put_async(store: Any, key: str, data: Any, **kwargs: Any) -> None:
            uploaded_keys.add(key)

        with patch("portolan_cli.push.obs.put_async", side_effect=mock_put_async):
            with patch("portolan_cli.push.obs.get_async", new_callable=AsyncMock) as mock_get:
                mock_get.side_effect = FileNotFoundError("Not found")

                push(
                    catalog_root=full_catalog,
                    collection="test-collection",
                    destination="s3://test-bucket/test-prefix",
                )

        # Per-collection push uploads complete STAC structure for standalone use
        expected_files = {
            "test-prefix/catalog.json",  # Root catalog for clonable remote
            "test-prefix/test-collection/collection.json",
            "test-prefix/test-collection/test-item/test-item.json",  # {item_id}.json
            "test-prefix/test-collection/test-item/data.parquet",
            "test-prefix/test-collection/versions.json",
        }

        assert expected_files.issubset(uploaded_keys), (
            f"Missing files. Expected: {expected_files}, Got: {uploaded_keys}"
        )

    def test_push_all_uploads_complete_stac_structure(self, full_catalog: Path) -> None:
        """push_all_collections should upload complete STAC structure including catalog.json."""
        from portolan_cli.push import push_all_collections

        uploaded_keys: set[str] = set()

        async def mock_put_async(store: Any, key: str, data: Any, **kwargs: Any) -> None:
            uploaded_keys.add(key)

        def mock_put_sync(store: Any, key: str, data: Any, **kwargs: Any) -> None:
            # Sync version for _push_all_upload_catalog helper
            uploaded_keys.add(key)

        with patch("portolan_cli.push.obs.put_async", side_effect=mock_put_async):
            with patch("portolan_cli.push.obs.put", side_effect=mock_put_sync):
                with patch("portolan_cli.push.obs.get_async", new_callable=AsyncMock) as mock_get:
                    mock_get.side_effect = FileNotFoundError("Not found")

                    push_all_collections(
                        catalog_root=full_catalog,
                        destination="s3://test-bucket/test-prefix",
                    )

        # push_all_collections uploads everything including catalog.json
        expected_files = {
            "test-prefix/catalog.json",
            "test-prefix/test-collection/collection.json",
            "test-prefix/test-collection/test-item/test-item.json",  # {item_id}.json
            "test-prefix/test-collection/test-item/data.parquet",
            "test-prefix/test-collection/versions.json",
        }

        assert expected_files.issubset(uploaded_keys), (
            f"Missing files. Expected: {expected_files}, Got: {uploaded_keys}"
        )
