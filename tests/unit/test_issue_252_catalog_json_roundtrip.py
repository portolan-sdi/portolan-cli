"""Unit tests for Issue #252 - Full Catalog Push/Pull Sync.

Tests that push uploads ALL STAC metadata files (catalog.json, collection.json,
item.json) in addition to assets and versions.json. Similarly tests that clone/pull
downloads all these files.

Design principle from ADR-0006: Portolan owns bucket contents.
The catalog should round-trip perfectly: push -> clone should recreate the catalog.

Test categories:
- Push uploads catalog.json from catalog root
- Push uploads collection.json for each collection
- Push uploads item.json for each item
- Manifest-last pattern preserved (data first, then manifests)
- Clone/pull downloads all STAC metadata files
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

# =============================================================================
# Test fixtures
# =============================================================================


@pytest.fixture
def full_catalog(tmp_path: Path) -> Path:
    """Create a complete catalog with catalog.json, collection.json, item.json.

    Structure:
        catalog/
            .portolan/
                config.yaml
                state.json
            catalog.json            <- root STAC catalog
            versions.json           <- root versions file
            test-collection/
                collection.json     <- STAC collection
                versions.json       <- collection versions
                test-item/
                    item.json       <- STAC item
                    data.parquet    <- asset file
    """
    catalog_dir = tmp_path / "catalog"
    catalog_dir.mkdir()

    # Create .portolan directory (required for valid catalog)
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("# Portolan configuration\n")
    (portolan_dir / "state.json").write_text("{}\n")

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
            {"rel": "item", "href": "./test-item/item.json", "type": "application/json"},
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
            {"rel": "self", "href": "./item.json", "type": "application/json"},
        ],
        "assets": {
            "data": {
                "href": "./data.parquet",
                "type": "application/x-parquet",
            }
        },
    }
    (item_dir / "item.json").write_text(json.dumps(item_json, indent=2))

    # Create asset file
    (item_dir / "data.parquet").write_bytes(b"fake parquet data" * 64)

    return catalog_dir


# =============================================================================
# Push: catalog.json upload tests
# =============================================================================


@pytest.mark.unit
class TestPushUploadsCatalogJson:
    """Tests that push uploads catalog.json from catalog root."""

    def test_push_uploads_catalog_json(self, full_catalog: Path) -> None:
        """Push should upload catalog.json from the catalog root."""
        from portolan_cli.push import push

        uploaded_keys: list[str] = []

        def mock_put(store: Any, key: str, data: Any, **kwargs: Any) -> None:
            uploaded_keys.append(key)

        with patch("portolan_cli.push.obs.put", side_effect=mock_put):
            with patch("portolan_cli.push.obs.get") as mock_get:
                # Simulate no remote versions.json (first push)
                mock_get.side_effect = FileNotFoundError("Not found")

                result = push(
                    catalog_root=full_catalog,
                    collection="test-collection",
                    destination="s3://test-bucket/test-prefix",
                )

        # Verify catalog.json was uploaded
        catalog_json_keys = [k for k in uploaded_keys if k.endswith("catalog.json")]
        assert len(catalog_json_keys) == 1, f"Expected catalog.json upload, got: {uploaded_keys}"
        assert catalog_json_keys[0] == "test-prefix/catalog.json"
        assert result.success

    def test_push_catalog_json_content_matches_local(self, full_catalog: Path) -> None:
        """Uploaded catalog.json content should match local file."""
        from portolan_cli.push import push

        uploaded_content: dict[str, bytes] = {}

        def mock_put(store: Any, key: str, data: Any, **kwargs: Any) -> None:
            if isinstance(data, Path):
                uploaded_content[key] = data.read_bytes()
            else:
                uploaded_content[key] = bytes(data) if not isinstance(data, bytes) else data

        with patch("portolan_cli.push.obs.put", side_effect=mock_put):
            with patch("portolan_cli.push.obs.get") as mock_get:
                mock_get.side_effect = FileNotFoundError("Not found")

                push(
                    catalog_root=full_catalog,
                    collection="test-collection",
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

        def mock_put(store: Any, key: str, data: Any, **kwargs: Any) -> None:
            uploaded_keys.append(key)

        with patch("portolan_cli.push.obs.put", side_effect=mock_put):
            with patch("portolan_cli.push.obs.get") as mock_get:
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
# Push: item.json upload tests
# =============================================================================


@pytest.mark.unit
class TestPushUploadsItemJson:
    """Tests that push uploads item.json for each item."""

    def test_push_uploads_item_json(self, full_catalog: Path) -> None:
        """Push should upload item.json for all items in the collection."""
        from portolan_cli.push import push

        uploaded_keys: list[str] = []

        def mock_put(store: Any, key: str, data: Any, **kwargs: Any) -> None:
            uploaded_keys.append(key)

        with patch("portolan_cli.push.obs.put", side_effect=mock_put):
            with patch("portolan_cli.push.obs.get") as mock_get:
                mock_get.side_effect = FileNotFoundError("Not found")

                result = push(
                    catalog_root=full_catalog,
                    collection="test-collection",
                    destination="s3://test-bucket/test-prefix",
                )

        # Verify item.json was uploaded
        item_json_keys = [k for k in uploaded_keys if k.endswith("item.json")]
        assert len(item_json_keys) == 1, f"Expected item.json upload, got: {uploaded_keys}"
        assert item_json_keys[0] == "test-prefix/test-collection/test-item/item.json"
        assert result.success


# =============================================================================
# Push: manifest-last ordering tests
# =============================================================================


@pytest.mark.unit
class TestPushManifestLastOrdering:
    """Tests that push follows manifest-last pattern for atomicity."""

    def test_push_uploads_assets_before_manifests(self, full_catalog: Path) -> None:
        """Push should upload assets before any JSON manifests.

        Order should be:
        1. Asset files (data.parquet, etc.)
        2. item.json (leaf manifests)
        3. collection.json (intermediate manifests)
        4. catalog.json (root manifest)
        5. versions.json (last - makes the push "visible")
        """
        from portolan_cli.push import push

        upload_order: list[str] = []

        def mock_put(store: Any, key: str, data: Any, **kwargs: Any) -> None:
            upload_order.append(key)

        with patch("portolan_cli.push.obs.put", side_effect=mock_put):
            with patch("portolan_cli.push.obs.get") as mock_get:
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
        item_idx = find_index("item.json")
        collection_idx = find_index("collection.json")
        catalog_idx = find_index("catalog.json")
        versions_idx = find_index("versions.json")

        # Verify all files were uploaded
        assert asset_idx >= 0, f"data.parquet not uploaded: {upload_order}"
        assert item_idx >= 0, f"item.json not uploaded: {upload_order}"
        assert collection_idx >= 0, f"collection.json not uploaded: {upload_order}"
        assert catalog_idx >= 0, f"catalog.json not uploaded: {upload_order}"
        assert versions_idx >= 0, f"versions.json not uploaded: {upload_order}"

        # Verify order: assets < item.json < collection.json < catalog.json < versions.json
        assert asset_idx < item_idx, f"Assets should be uploaded before item.json: {upload_order}"
        assert item_idx < collection_idx, (
            f"item.json should be uploaded before collection.json: {upload_order}"
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

    def test_push_uploads_complete_stac_structure(self, full_catalog: Path) -> None:
        """Push should upload the complete STAC structure."""
        from portolan_cli.push import push

        uploaded_keys: set[str] = set()

        def mock_put(store: Any, key: str, data: Any, **kwargs: Any) -> None:
            uploaded_keys.add(key)

        with patch("portolan_cli.push.obs.put", side_effect=mock_put):
            with patch("portolan_cli.push.obs.get") as mock_get:
                mock_get.side_effect = FileNotFoundError("Not found")

                push(
                    catalog_root=full_catalog,
                    collection="test-collection",
                    destination="s3://test-bucket/test-prefix",
                )

        # Verify all STAC files were uploaded
        expected_files = {
            "test-prefix/catalog.json",
            "test-prefix/test-collection/collection.json",
            "test-prefix/test-collection/test-item/item.json",
            "test-prefix/test-collection/test-item/data.parquet",
            "test-prefix/test-collection/versions.json",
        }

        assert expected_files.issubset(uploaded_keys), (
            f"Missing files. Expected: {expected_files}, Got: {uploaded_keys}"
        )
