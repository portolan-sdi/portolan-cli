"""Unit tests for Issue #354: Fix asset path resolution in versions.json.

Tests that asset keys in versions.json are collection-relative (no path doubling).

Bug: Asset keys were incorrectly including collection directory name as prefix:
  WRONG:  "fme_disk_tunnels/fme_disk_tunnels.parquet"
  RIGHT:  "fme_disk_tunnels.parquet"

The asset key should be collection-relative, while href contains the full path.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from portolan_cli.preparation import PreparedItem

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_extract_side_requests")]


FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


class TestAssetKeyGeneration:
    """Tests for correct asset key generation in versions.json."""

    def test_collection_level_asset_key_is_filename_only(self, tmp_path: Path) -> None:
        """Collection-level assets have filename-only keys, not prefixed with collection_id."""
        from portolan_cli.add import finalize_items, prepare_item

        # Set up catalog
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()

        # Create config.yaml sentinel
        config_yaml = catalog_root / ".portolan" / "config.yaml"
        config_yaml.write_text("title: Test Catalog\n")

        # Create catalog.json
        catalog_json = catalog_root / "catalog.json"
        catalog_json.write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test",
                    "stac_version": "1.1.0",
                    "description": "Test",
                    "links": [],
                }
            )
        )

        # Copy fixture to collection directory (collection-level asset)
        collection_id = "my_collection"
        collection_dir = catalog_root / collection_id
        collection_dir.mkdir()
        fixture_path = FIXTURES_DIR / "simple.parquet"
        parquet_path = collection_dir / "my_collection.parquet"
        shutil.copy(fixture_path, parquet_path)

        # Prepare and finalize
        prepared = prepare_item(
            path=parquet_path,
            catalog_root=catalog_root,
            collection_id=collection_id,
        )

        finalize_items(catalog_root, [prepared])

        # Check versions.json
        versions_path = collection_dir / "versions.json"
        assert versions_path.exists(), "versions.json should exist"

        versions_data = json.loads(versions_path.read_text())
        latest_version = versions_data["versions"][-1]
        asset_keys = list(latest_version["assets"].keys())

        # Asset key should NOT include collection_id prefix
        # WRONG: "my_collection/my_collection.parquet"
        # RIGHT: "my_collection.parquet"
        assert "my_collection.parquet" in asset_keys, (
            f"Asset key should be filename only, got: {asset_keys}"
        )
        assert "my_collection/my_collection.parquet" not in asset_keys, (
            "Asset key should NOT include collection_id prefix"
        )

    def test_item_level_asset_key_includes_item_id(self, tmp_path: Path) -> None:
        """Item-level assets have item_id/filename keys."""
        from portolan_cli.add import finalize_items, prepare_item

        # Set up catalog
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()

        config_yaml = catalog_root / ".portolan" / "config.yaml"
        config_yaml.write_text("title: Test Catalog\n")

        catalog_json = catalog_root / "catalog.json"
        catalog_json.write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test",
                    "stac_version": "1.1.0",
                    "description": "Test",
                    "links": [],
                }
            )
        )

        # Copy fixture to item directory (item-level asset)
        collection_id = "my_collection"
        item_id = "my_item"
        collection_dir = catalog_root / collection_id
        item_dir = collection_dir / item_id
        item_dir.mkdir(parents=True)
        fixture_path = FIXTURES_DIR / "simple.parquet"
        parquet_path = item_dir / "data.parquet"
        shutil.copy(fixture_path, parquet_path)

        # Prepare and finalize
        prepared = prepare_item(
            path=parquet_path,
            catalog_root=catalog_root,
            collection_id=collection_id,
            item_id=item_id,
        )

        finalize_items(catalog_root, [prepared])

        # Check versions.json
        versions_path = collection_dir / "versions.json"
        versions_data = json.loads(versions_path.read_text())
        latest_version = versions_data["versions"][-1]
        asset_keys = list(latest_version["assets"].keys())

        # Item-level asset key should be item_id/filename
        expected_key = f"{item_id}/data.parquet"
        assert expected_key in asset_keys, (
            f"Asset key should be '{expected_key}', got: {asset_keys}"
        )

    def test_nested_catalog_asset_key_no_doubling(self, tmp_path: Path) -> None:
        """Nested catalogs don't double the path in asset keys."""
        from portolan_cli.add import finalize_items, prepare_item

        # Set up catalog with nested structure
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()

        config_yaml = catalog_root / ".portolan" / "config.yaml"
        config_yaml.write_text("title: Test Catalog\n")

        catalog_json = catalog_root / "catalog.json"
        catalog_json.write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "root",
                    "stac_version": "1.1.0",
                    "description": "Root",
                    "links": [],
                }
            )
        )

        # Nested collection: subcatalog/collection
        collection_id = "subcatalog/fme_disk_tunnels"
        collection_dir = catalog_root / "subcatalog" / "fme_disk_tunnels"
        collection_dir.mkdir(parents=True)

        fixture_path = FIXTURES_DIR / "simple.parquet"
        parquet_path = collection_dir / "fme_disk_tunnels.parquet"
        shutil.copy(fixture_path, parquet_path)

        # Prepare and finalize
        prepared = prepare_item(
            path=parquet_path,
            catalog_root=catalog_root,
            collection_id=collection_id,
        )

        finalize_items(catalog_root, [prepared])

        # Check versions.json
        versions_path = collection_dir / "versions.json"
        versions_data = json.loads(versions_path.read_text())
        latest_version = versions_data["versions"][-1]
        asset_keys = list(latest_version["assets"].keys())

        # Asset key should be collection-relative (filename only for collection-level)
        # WRONG: "fme_disk_tunnels/fme_disk_tunnels.parquet" or
        #        "subcatalog/fme_disk_tunnels/fme_disk_tunnels.parquet"
        # RIGHT: "fme_disk_tunnels.parquet"
        assert "fme_disk_tunnels.parquet" in asset_keys, (
            f"Asset key should be filename only, got: {asset_keys}"
        )

        # Verify href is correct (full path from catalog root)
        asset = latest_version["assets"]["fme_disk_tunnels.parquet"]
        expected_href = "subcatalog/fme_disk_tunnels/fme_disk_tunnels.parquet"
        assert asset["href"] == expected_href, (
            f"href should be '{expected_href}', got: {asset['href']}"
        )


def _stage_and_commit(collection_dir: Path, prepared: PreparedItem) -> None:
    """Stage the item as add does, in a run on the catalog above the collection."""
    from portolan_cli.finalization import _stage_items
    from portolan_cli.run_state import RunState
    from portolan_cli.version_ops import commit_run

    state = RunState.open(collection_dir.parent)
    _stage_items(state, collection_dir.name, collection_dir, [prepared])
    commit_run(state)


class TestBatchUpdateVersionsAssetKeys:
    """Tests for the asset keys of the version that add stages."""

    def test_batch_update_collection_level_asset_keys(self, tmp_path: Path) -> None:
        """The staged version keys a collection-level asset by its file name."""
        collection_dir = tmp_path / "my_collection"
        collection_dir.mkdir()

        # Create a dummy asset file
        fixture_path = FIXTURES_DIR / "simple.parquet"
        asset_path = collection_dir / "my_collection.parquet"
        shutil.copy(fixture_path, asset_path)

        # Create prepared item with correct fields
        prepared = PreparedItem(
            item_id="my_collection",
            collection_id="my_collection",
            format_type="vector",
            bbox=[0, 0, 1, 1],
            asset_files={"my_collection.parquet": (asset_path, "abc123", 1024)},
            item_json_path=collection_dir / "item.json",
            is_collection_level_asset=True,
        )

        _stage_and_commit(collection_dir, prepared)

        # Check versions.json
        versions_path = collection_dir / "versions.json"
        versions_data = json.loads(versions_path.read_text())
        latest = versions_data["versions"][-1]

        # Key should be filename only, NOT "my_collection/my_collection.parquet"
        assert "my_collection.parquet" in latest["assets"]
        assert "my_collection/my_collection.parquet" not in latest["assets"]

    def test_batch_update_item_level_asset_keys(self, tmp_path: Path) -> None:
        """The staged version keys an item-level asset by its item directory."""
        collection_dir = tmp_path / "my_collection"
        item_dir = collection_dir / "my_item"
        item_dir.mkdir(parents=True)

        fixture_path = FIXTURES_DIR / "simple.parquet"
        asset_path = item_dir / "data.parquet"
        shutil.copy(fixture_path, asset_path)

        prepared = PreparedItem(
            item_id="my_item",
            collection_id="my_collection",
            format_type="vector",
            bbox=[0, 0, 1, 1],
            asset_files={"data.parquet": (asset_path, "abc123", 1024)},
            item_json_path=item_dir / "item.json",
            is_collection_level_asset=False,
        )

        _stage_and_commit(collection_dir, prepared)

        versions_path = collection_dir / "versions.json"
        versions_data = json.loads(versions_path.read_text())
        latest = versions_data["versions"][-1]

        # Key should be "my_item/data.parquet"
        assert "my_item/data.parquet" in latest["assets"]


class TestAssetPathsInExtraction:
    """Tests for asset path handling in extraction workflow."""

    def test_extracted_asset_keys_are_collection_relative(self, tmp_path: Path) -> None:
        """Extraction workflow produces collection-relative asset keys."""
        from unittest.mock import patch

        from portolan_cli.extract.arcgis.discovery import LayerInfo, ServiceDiscoveryResult
        from portolan_cli.extract.arcgis.orchestrator import (
            ArcGISLayerSource,
            ExtractionOptions,
            extract_arcgis_catalog,
        )

        output_dir = tmp_path / "extraction_output"
        fixture_src = FIXTURES_DIR / "simple.parquet"

        def mock_extract_side_effect(
            self: object, service_url: str, layer: object, output_path: Path
        ) -> int:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(fixture_src, output_path)
            return 100

        with (
            patch.object(
                ArcGISLayerSource, "fetch", autospec=True, side_effect=mock_extract_side_effect
            ),
            patch("portolan_cli.extract.arcgis.orchestrator.discover_layers") as mock,
        ):
            mock.return_value = ServiceDiscoveryResult(
                layers=[LayerInfo(id=0, name="TestLayer", layer_type="Feature Layer")],
            )

            extract_arcgis_catalog(
                url="https://example.com/arcgis/rest/services/Test/FeatureServer",
                output_dir=output_dir,
                options=ExtractionOptions(dry_run=False, raw=False, license="CC-BY-4.0"),
            )

        # Check versions.json in extracted collection
        versions_path = output_dir / "testlayer" / "versions.json"
        assert versions_path.exists(), "versions.json should be created by extraction"

        versions_data = json.loads(versions_path.read_text())
        latest = versions_data["versions"][-1]
        asset_keys = list(latest["assets"].keys())

        # No doubled paths
        for key in asset_keys:
            assert not key.startswith("testlayer/testlayer"), (
                f"Asset key should not double collection name: {key}"
            )
