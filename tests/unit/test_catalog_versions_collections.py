"""Tests for catalog-level versions.json collection tracking (issue #142).

Per ADR-0005, the catalog-level versions.json should track:
- Which collections exist
- When each was created/modified
- Summary metadata (item count, etc.)
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from portolan_cli.catalog import init_catalog


class TestCatalogVersionsCollections:
    """Tests for catalog-level versions.json collection tracking."""

    @pytest.mark.unit
    def test_init_catalog_creates_empty_collections(self, tmp_path: Path) -> None:
        """init_catalog should create versions.json with empty collections dict."""
        init_catalog(tmp_path)

        versions_file = tmp_path / "versions.json"
        data = json.loads(versions_file.read_text())

        assert "collections" in data
        assert data["collections"] == {}

    @pytest.mark.unit
    def test_update_catalog_versions_on_new_collection(self, tmp_path: Path) -> None:
        """Creating a new collection should add entry to catalog versions.json."""
        from portolan_cli.catalog_versions import update_catalog_versions_collection

        init_catalog(tmp_path)

        # Simulate creating a new collection
        update_catalog_versions_collection(
            catalog_root=tmp_path,
            collection_id="my-collection",
            item_count=0,
        )

        versions_file = tmp_path / "versions.json"
        data = json.loads(versions_file.read_text())

        assert "my-collection" in data["collections"]
        collection_info = data["collections"]["my-collection"]
        assert "created" in collection_info
        assert "current_version" in collection_info
        assert collection_info["item_count"] == 0

    @pytest.mark.unit
    def test_update_catalog_versions_increments_item_count(self, tmp_path: Path) -> None:
        """Adding items to a collection should update item_count."""
        from portolan_cli.catalog_versions import update_catalog_versions_collection

        init_catalog(tmp_path)

        # Create collection with 1 item
        update_catalog_versions_collection(
            catalog_root=tmp_path,
            collection_id="my-collection",
            item_count=1,
        )

        # Update with more items
        update_catalog_versions_collection(
            catalog_root=tmp_path,
            collection_id="my-collection",
            item_count=3,
        )

        versions_file = tmp_path / "versions.json"
        data = json.loads(versions_file.read_text())

        assert data["collections"]["my-collection"]["item_count"] == 3

    @pytest.mark.unit
    def test_update_catalog_versions_preserves_created_timestamp(self, tmp_path: Path) -> None:
        """Updating a collection should preserve the original created timestamp."""
        from portolan_cli.catalog_versions import update_catalog_versions_collection

        init_catalog(tmp_path)

        # Create collection
        update_catalog_versions_collection(
            catalog_root=tmp_path,
            collection_id="my-collection",
            item_count=1,
        )

        versions_file = tmp_path / "versions.json"
        data1 = json.loads(versions_file.read_text())
        original_created = data1["collections"]["my-collection"]["created"]

        # Update collection
        update_catalog_versions_collection(
            catalog_root=tmp_path,
            collection_id="my-collection",
            item_count=5,
        )

        data2 = json.loads(versions_file.read_text())
        assert data2["collections"]["my-collection"]["created"] == original_created

    @pytest.mark.unit
    def test_update_catalog_versions_updates_modified_timestamp(self, tmp_path: Path) -> None:
        """Updating a collection should update the modified timestamp."""
        from portolan_cli.catalog_versions import update_catalog_versions_collection

        init_catalog(tmp_path)

        # Create collection
        update_catalog_versions_collection(
            catalog_root=tmp_path,
            collection_id="my-collection",
            item_count=1,
        )

        versions_file = tmp_path / "versions.json"
        data1 = json.loads(versions_file.read_text())
        # First call shouldn't have modified (new collection)
        first_modified = data1["collections"]["my-collection"].get("modified")
        assert first_modified is None

        # Update collection (this should update modified timestamp)
        update_catalog_versions_collection(
            catalog_root=tmp_path,
            collection_id="my-collection",
            item_count=5,
        )

        data2 = json.loads(versions_file.read_text())
        second_modified = data2["collections"]["my-collection"].get("modified")

        # modified should exist after update
        assert second_modified is not None

    @pytest.mark.unit
    def test_update_catalog_versions_multiple_collections(self, tmp_path: Path) -> None:
        """Should track multiple collections independently."""
        from portolan_cli.catalog_versions import update_catalog_versions_collection

        init_catalog(tmp_path)

        update_catalog_versions_collection(
            catalog_root=tmp_path,
            collection_id="collection-a",
            item_count=2,
        )
        update_catalog_versions_collection(
            catalog_root=tmp_path,
            collection_id="collection-b",
            item_count=5,
        )

        versions_file = tmp_path / "versions.json"
        data = json.loads(versions_file.read_text())

        assert len(data["collections"]) == 2
        assert data["collections"]["collection-a"]["item_count"] == 2
        assert data["collections"]["collection-b"]["item_count"] == 5

    @pytest.mark.unit
    def test_update_catalog_versions_with_version_string(self, tmp_path: Path) -> None:
        """Should track current_version for each collection."""
        from portolan_cli.catalog_versions import update_catalog_versions_collection

        init_catalog(tmp_path)

        update_catalog_versions_collection(
            catalog_root=tmp_path,
            collection_id="my-collection",
            item_count=1,
            current_version="1.0.0",
        )

        versions_file = tmp_path / "versions.json"
        data = json.loads(versions_file.read_text())

        assert data["collections"]["my-collection"]["current_version"] == "1.0.0"

    @pytest.mark.unit
    def test_read_catalog_versions_returns_dict(self, tmp_path: Path) -> None:
        """read_catalog_versions should return the catalog versions data."""
        from portolan_cli.catalog_versions import read_catalog_versions

        init_catalog(tmp_path)

        data = read_catalog_versions(tmp_path)

        assert "schema_version" in data
        assert "catalog_id" in data
        assert "created" in data
        assert "collections" in data

    @pytest.mark.unit
    def test_read_catalog_versions_file_not_found(self, tmp_path: Path) -> None:
        """read_catalog_versions should raise FileNotFoundError if not found."""
        from portolan_cli.catalog_versions import read_catalog_versions

        with pytest.raises(FileNotFoundError):
            read_catalog_versions(tmp_path)

    @pytest.mark.unit
    def test_get_collection_info_returns_none_for_missing(self, tmp_path: Path) -> None:
        """get_collection_info should return None for non-existent collection."""
        from portolan_cli.catalog_versions import get_collection_info

        init_catalog(tmp_path)

        info = get_collection_info(tmp_path, "nonexistent")
        assert info is None

    @pytest.mark.unit
    def test_get_collection_info_returns_data_for_existing(self, tmp_path: Path) -> None:
        """get_collection_info should return collection data for existing collection."""
        from portolan_cli.catalog_versions import (
            get_collection_info,
            update_catalog_versions_collection,
        )

        init_catalog(tmp_path)
        update_catalog_versions_collection(
            catalog_root=tmp_path,
            collection_id="my-collection",
            item_count=3,
            current_version="2.0.0",
        )

        info = get_collection_info(tmp_path, "my-collection")

        assert info is not None
        assert info["item_count"] == 3
        assert info["current_version"] == "2.0.0"


class TestCatalogVersionsDataClass:
    """Tests for the CatalogVersionsFile data structure."""

    @pytest.mark.unit
    def test_collection_entry_dataclass_fields(self) -> None:
        """CollectionEntry should have required fields."""
        from portolan_cli.catalog_versions import CollectionEntry

        entry = CollectionEntry(
            created="2024-01-01T00:00:00Z",
            current_version="1.0.0",
            item_count=5,
        )

        assert entry.created == "2024-01-01T00:00:00Z"
        assert entry.current_version == "1.0.0"
        assert entry.item_count == 5
        assert entry.modified is None  # Optional field

    @pytest.mark.unit
    def test_collection_entry_with_modified(self) -> None:
        """CollectionEntry should support optional modified field."""
        from portolan_cli.catalog_versions import CollectionEntry

        entry = CollectionEntry(
            created="2024-01-01T00:00:00Z",
            modified="2024-06-01T12:00:00Z",
            current_version="1.2.0",
            item_count=10,
        )

        assert entry.modified == "2024-06-01T12:00:00Z"

    @pytest.mark.unit
    def test_catalog_versions_file_dataclass(self) -> None:
        """CatalogVersionsFile should have required fields."""
        from portolan_cli.catalog_versions import CatalogVersionsFile, CollectionEntry

        catalog_versions = CatalogVersionsFile(
            schema_version="1.0.0",
            catalog_id="test-catalog",
            created="2024-01-01T00:00:00Z",
            collections={
                "my-collection": CollectionEntry(
                    created="2024-01-01T00:00:00Z",
                    current_version="1.0.0",
                    item_count=3,
                )
            },
        )

        assert catalog_versions.schema_version == "1.0.0"
        assert catalog_versions.catalog_id == "test-catalog"
        assert "my-collection" in catalog_versions.collections


class TestCatalogVersionsAtomicWrite:
    """Tests for atomic write behavior."""

    @pytest.mark.unit
    def test_atomic_write_creates_file(self, tmp_path: Path) -> None:
        """write_catalog_versions should create file atomically."""
        from portolan_cli.catalog_versions import (
            CatalogVersionsFile,
            write_catalog_versions,
        )

        catalog_versions = CatalogVersionsFile(
            schema_version="1.0.0",
            catalog_id="test-catalog",
            created="2024-01-01T00:00:00Z",
            collections={},
        )

        write_catalog_versions(tmp_path, catalog_versions)

        versions_file = tmp_path / "versions.json"
        assert versions_file.exists()

    @pytest.mark.unit
    def test_atomic_write_no_temp_files_left(self, tmp_path: Path) -> None:
        """Atomic write should not leave temp files on success."""
        from portolan_cli.catalog_versions import (
            CatalogVersionsFile,
            write_catalog_versions,
        )

        catalog_versions = CatalogVersionsFile(
            schema_version="1.0.0",
            catalog_id="test-catalog",
            created="2024-01-01T00:00:00Z",
            collections={},
        )

        write_catalog_versions(tmp_path, catalog_versions)

        # No temp files should remain
        temp_files = list(tmp_path.glob(".versions_*.tmp"))
        assert len(temp_files) == 0


class TestCatalogVersionsHypothesis:
    """Property-based tests for catalog versions tracking."""

    @pytest.mark.unit
    @given(
        collection_ids=st.lists(
            st.text(
                alphabet=st.characters(
                    whitelist_categories=("Ll", "Lu", "Nd"),
                    whitelist_characters="-_",
                ),
                min_size=1,
                max_size=20,
            ).filter(lambda x: x and not x.startswith("-") and not x.endswith("-")),
            min_size=1,
            max_size=10,
            unique=True,
        ),
        item_counts=st.lists(st.integers(min_value=0, max_value=1000), min_size=1),
    )
    @settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_multiple_collections_all_tracked(
        self,
        tmp_path: Path,
        collection_ids: list[str],
        item_counts: list[int],
    ) -> None:
        """All added collections should be tracked in catalog versions."""
        # Create a fresh catalog for each test run using uuid for uniqueness
        from portolan_cli.catalog_versions import (
            read_catalog_versions,
            update_catalog_versions_collection,
        )

        test_dir = tmp_path / str(uuid.uuid4())
        test_dir.mkdir(parents=True, exist_ok=True)
        init_catalog(test_dir)

        # Add collections with varying item counts
        for i, collection_id in enumerate(collection_ids):
            count = item_counts[i % len(item_counts)]
            update_catalog_versions_collection(
                catalog_root=test_dir,
                collection_id=collection_id,
                item_count=count,
            )

        # Verify all collections are tracked
        data = read_catalog_versions(test_dir)
        assert len(data["collections"]) == len(collection_ids)
        for collection_id in collection_ids:
            assert collection_id in data["collections"]

    @pytest.mark.unit
    @given(
        item_count_sequence=st.lists(
            st.integers(min_value=0, max_value=1000),
            min_size=2,
            max_size=10,
        )
    )
    @settings(max_examples=30, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_item_count_always_reflects_last_update(
        self,
        tmp_path: Path,
        item_count_sequence: list[int],
    ) -> None:
        """Item count should always reflect the most recent update."""
        from portolan_cli.catalog_versions import (
            get_collection_info,
            update_catalog_versions_collection,
        )

        test_dir = tmp_path / str(uuid.uuid4())
        test_dir.mkdir(parents=True, exist_ok=True)
        init_catalog(test_dir)

        for count in item_count_sequence:
            update_catalog_versions_collection(
                catalog_root=test_dir,
                collection_id="test-collection",
                item_count=count,
            )

        info = get_collection_info(test_dir, "test-collection")
        assert info is not None
        assert info["item_count"] == item_count_sequence[-1]


class TestRemoveCollectionFromCatalogVersions:
    """Tests for removing collections from catalog versions."""

    @pytest.mark.unit
    def test_remove_collection_from_catalog_versions(self, tmp_path: Path) -> None:
        """Removing a collection should remove it from catalog versions."""
        from portolan_cli.catalog_versions import (
            read_catalog_versions,
            remove_collection_from_catalog_versions,
            update_catalog_versions_collection,
        )

        init_catalog(tmp_path)

        # Add a collection
        update_catalog_versions_collection(
            catalog_root=tmp_path,
            collection_id="to-remove",
            item_count=5,
        )

        # Verify it exists
        data = read_catalog_versions(tmp_path)
        assert "to-remove" in data["collections"]

        # Remove it
        remove_collection_from_catalog_versions(tmp_path, "to-remove")

        # Verify it's gone
        data = read_catalog_versions(tmp_path)
        assert "to-remove" not in data["collections"]

    @pytest.mark.unit
    def test_remove_nonexistent_collection_is_noop(self, tmp_path: Path) -> None:
        """Removing a non-existent collection should not raise an error."""
        from portolan_cli.catalog_versions import remove_collection_from_catalog_versions

        init_catalog(tmp_path)

        # Should not raise
        remove_collection_from_catalog_versions(tmp_path, "nonexistent")
