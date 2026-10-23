"""Tests for root-level file upload in push operations (Issue #357).

This module verifies that root README.md and versions.json are uploaded
when pushing all collections. Root catalog.json was already being uploaded;
root README.md and versions.json were being skipped.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import obstore
import pytest

from portolan_cli.sync.push import (
    PushResult,
    push_all_collections,
)
from tests.memory_remote import memory_remote, record_puts, stored_keys

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from obstore.store import MemoryStore

pytestmark = pytest.mark.unit


def _setup_valid_catalog(catalog_root: Path) -> None:
    """Helper to create a valid catalog with .portolan/config.yaml."""
    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir(parents=True, exist_ok=True)
    (portolan_dir / "config.yaml").write_text("version: '1.0'\n")


def _create_catalog_json(catalog_root: Path) -> Path:
    """Create a minimal catalog.json at root."""
    catalog_json = catalog_root / "catalog.json"
    catalog_json.write_text(
        json.dumps(
            {
                "type": "Catalog",
                "id": "test-catalog",
                "description": "Test catalog",
                "stac_version": "1.0.0",
                "links": [],
            }
        )
    )
    return catalog_json


def _create_root_readme(catalog_root: Path) -> Path:
    """Create a README.md at catalog root."""
    readme = catalog_root / "README.md"
    readme.write_text("# Test Catalog\n\nThis is the root README.\n")
    return readme


def _create_root_versions(catalog_root: Path) -> Path:
    """Create a versions.json at catalog root (Issue #357)."""
    versions = catalog_root / "versions.json"
    versions.write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "collections": {},
            }
        )
    )
    return versions


def _create_collection(catalog_root: Path, name: str, with_readme: bool = False) -> None:
    """Create a collection with required files."""
    collection_dir = catalog_root / name
    collection_dir.mkdir()
    (collection_dir / "versions.json").write_text(
        json.dumps({"versions": [{"version": "v1", "assets": {}}]})
    )
    (collection_dir / "collection.json").write_text(
        json.dumps(
            {
                "type": "Collection",
                "id": name,
                "description": f"Test collection {name}",
                "stac_version": "1.0.0",
                "links": [],
                "license": "proprietary",
                "extent": {
                    "spatial": {"bbox": [[-180, -90, 180, 90]]},
                    "temporal": {"interval": [[None, None]]},
                },
            }
        )
    )
    if with_readme:
        (collection_dir / "README.md").write_text(f"# {name}\n\nCollection README.\n")


@pytest.fixture
def store() -> Iterator[MemoryStore]:
    """The remote bucket: push_all_collections writes to this in-memory store."""
    with memory_remote() as memory:
        yield memory


def _keys(store: MemoryStore) -> list[str]:
    return sorted(stored_keys(store))


def _push_ok(mock_push: AsyncMock, files_uploaded: int = 5) -> None:
    mock_push.return_value = PushResult(
        success=True, files_uploaded=files_uploaded, versions_pushed=1, conflicts=[], errors=[]
    )


def _push_all(tmp_path: Path, *, dry_run: bool = False) -> Any:
    return push_all_collections(
        catalog_root=tmp_path,
        destination="s3://bucket/catalog",
        force=False,
        dry_run=dry_run,
        profile=None,
    )


class TestPushAllCollectionsRootFiles:
    """Tests for root-level file uploads in push_all_collections (Issue #357)."""

    @patch("portolan_cli.sync.push.push_async", new_callable=AsyncMock)
    def test_uploads_all_root_files_after_collections(
        self, mock_push: AsyncMock, store: MemoryStore, tmp_path: Path
    ) -> None:
        """push_all_collections uploads README.md, catalog.json, and versions.json."""
        _setup_valid_catalog(tmp_path)
        _create_catalog_json(tmp_path)
        _create_root_readme(tmp_path)
        _create_root_versions(tmp_path)
        _create_collection(tmp_path, "col1", with_readme=True)
        _push_ok(mock_push)

        result = _push_all(tmp_path)

        assert result.success is True
        assert _keys(store) == [
            "catalog/README.md",
            "catalog/catalog.json",
            "catalog/versions.json",
        ]
        stored = obstore.get(store, "catalog/versions.json").bytes()
        assert bytes(stored) == (tmp_path / "versions.json").read_bytes()

    @patch("portolan_cli.sync.push.push_async", new_callable=AsyncMock)
    def test_versions_json_uploaded_last(
        self,
        mock_push: AsyncMock,
        store: MemoryStore,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """versions.json is uploaded last (manifest-last atomicity)."""
        _setup_valid_catalog(tmp_path)
        _create_catalog_json(tmp_path)
        _create_root_readme(tmp_path)
        _create_root_versions(tmp_path)
        _create_collection(tmp_path, "col1")
        _push_ok(mock_push)
        order = record_puts(monkeypatch)

        _push_all(tmp_path)

        assert order == ["catalog/README.md", "catalog/catalog.json", "catalog/versions.json"]

    @patch("portolan_cli.sync.push.push_async", new_callable=AsyncMock)
    def test_skips_optional_files_when_not_present(
        self, mock_push: AsyncMock, store: MemoryStore, tmp_path: Path
    ) -> None:
        """push_all_collections doesn't fail if README.md or versions.json missing."""
        _setup_valid_catalog(tmp_path)
        _create_catalog_json(tmp_path)
        _create_collection(tmp_path, "col1")
        _push_ok(mock_push)

        result = _push_all(tmp_path)

        assert result.success is True
        assert _keys(store) == ["catalog/catalog.json"]

    @patch("portolan_cli.sync.push.push_async", new_callable=AsyncMock)
    def test_skips_root_files_when_collection_fails(
        self, mock_push: AsyncMock, store: MemoryStore, tmp_path: Path
    ) -> None:
        """push_all_collections skips root files when any collection fails."""
        _setup_valid_catalog(tmp_path)
        _create_catalog_json(tmp_path)
        _create_root_readme(tmp_path)
        _create_root_versions(tmp_path)
        _create_collection(tmp_path, "col1")
        mock_push.return_value = PushResult(
            success=False, files_uploaded=0, versions_pushed=0, conflicts=[], errors=["Failed"]
        )

        result = _push_all(tmp_path)

        assert result.success is False
        assert _keys(store) == []

    def test_skips_root_files_when_zero_successful(
        self, store: MemoryStore, tmp_path: Path
    ) -> None:
        """push_all_collections skips root files when the catalog has no collection."""
        _setup_valid_catalog(tmp_path)
        _create_catalog_json(tmp_path)
        _create_root_versions(tmp_path)

        result = _push_all(tmp_path)

        assert result.success is True
        assert result.total_files_uploaded == 0
        assert _keys(store) == []

    @patch("portolan_cli.sync.push.push_async", new_callable=AsyncMock)
    def test_dry_run_shows_all_root_files(
        self, mock_push: AsyncMock, store: MemoryStore, tmp_path: Path
    ) -> None:
        """push_all_collections in dry_run mode writes nothing."""
        _setup_valid_catalog(tmp_path)
        _create_catalog_json(tmp_path)
        _create_root_readme(tmp_path)
        _create_root_versions(tmp_path)
        _create_collection(tmp_path, "col1")
        mock_push.return_value = PushResult(
            success=True,
            files_uploaded=0,
            versions_pushed=0,
            conflicts=[],
            errors=[],
            dry_run=True,
            would_push_versions=1,
        )

        result = _push_all(tmp_path, dry_run=True)

        assert result.success is True
        assert _keys(store) == []

    @patch("portolan_cli.sync.push.push_async", new_callable=AsyncMock)
    def test_counts_all_root_files_in_total(
        self, mock_push: AsyncMock, store: MemoryStore, tmp_path: Path
    ) -> None:
        """All root files are counted in total_files_uploaded."""
        _setup_valid_catalog(tmp_path)
        _create_catalog_json(tmp_path)
        _create_root_readme(tmp_path)
        _create_root_versions(tmp_path)
        _create_collection(tmp_path, "col1")
        _push_ok(mock_push)

        result = _push_all(tmp_path)

        assert result.success is True
        # 5 from collection + 3 root files (README.md + catalog.json + versions.json)
        assert result.total_files_uploaded == 8

    @patch("portolan_cli.sync.push.setup_store")
    @patch("portolan_cli.sync.push.push_async", new_callable=AsyncMock)
    def test_handles_upload_error(
        self, mock_push: AsyncMock, mock_setup_store: MagicMock, tmp_path: Path
    ) -> None:
        """push_all_collections handles errors during root file upload."""
        _setup_valid_catalog(tmp_path)
        _create_catalog_json(tmp_path)
        _create_root_readme(tmp_path)
        _create_collection(tmp_path, "col1")
        _push_ok(mock_push)
        mock_setup_store.side_effect = ConnectionError("Network unreachable")

        result = _push_all(tmp_path)

        assert result.success is False
        assert result.collection_errors["root_files"] == ["Network unreachable"]

    @patch("portolan_cli.sync.push.push_async", new_callable=AsyncMock)
    def test_handles_partial_upload_failure(
        self,
        mock_push: AsyncMock,
        store: MemoryStore,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A failed catalog.json write fails the push and skips versions.json."""
        _setup_valid_catalog(tmp_path)
        _create_catalog_json(tmp_path)
        _create_root_readme(tmp_path)
        _create_root_versions(tmp_path)
        _create_collection(tmp_path, "col1")
        _push_ok(mock_push)

        async def disk_full_on_catalog(key: str) -> None:
            if key == "catalog/catalog.json":
                raise OSError("Disk full")

        record_puts(monkeypatch, disk_full_on_catalog)

        result = _push_all(tmp_path)

        assert result.success is False
        assert result.collection_errors["root_files"] == ["Disk full"]
        assert _keys(store) == ["catalog/README.md"]
