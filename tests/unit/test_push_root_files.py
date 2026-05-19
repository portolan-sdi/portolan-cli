"""Tests for root-level file upload in push operations (Issue #357).

This module verifies that root README.md and versions.json are uploaded
when pushing all collections. Root catalog.json was already being uploaded;
root README.md and versions.json were being skipped.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from portolan_cli.push import (
    PushResult,
    push_all_collections,
)

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


class TestPushAllCollectionsRootFiles:
    """Tests for root-level file uploads in push_all_collections (Issue #357)."""

    @patch("portolan_cli.push.obs.put")
    @patch("portolan_cli.push.push_async", new_callable=AsyncMock)
    def test_uploads_all_root_files_after_collections(
        self, mock_push: MagicMock, mock_obs_put: MagicMock, tmp_path: Path
    ) -> None:
        """push_all_collections uploads README.md, catalog.json, and versions.json."""
        _setup_valid_catalog(tmp_path)
        _create_catalog_json(tmp_path)
        _create_root_readme(tmp_path)
        _create_root_versions(tmp_path)
        _create_collection(tmp_path, "col1", with_readme=True)

        mock_push.return_value = PushResult(
            success=True,
            files_uploaded=5,
            versions_pushed=1,
            conflicts=[],
            errors=[],
        )

        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=False,
            profile=None,
        )

        assert result.success is True

        # Verify obs.put was called for all three root files
        put_calls = mock_obs_put.call_args_list
        uploaded_keys = [c.args[1] for c in put_calls]

        assert any(k.endswith("/README.md") or k == "README.md" for k in uploaded_keys), (
            f"Root README.md should be uploaded, got: {uploaded_keys}"
        )
        assert any(k.endswith("/catalog.json") or k == "catalog.json" for k in uploaded_keys), (
            f"catalog.json should be uploaded, got: {uploaded_keys}"
        )
        assert any(k.endswith("/versions.json") or k == "versions.json" for k in uploaded_keys), (
            f"Root versions.json should be uploaded, got: {uploaded_keys}"
        )

    @patch("portolan_cli.push.obs.put")
    @patch("portolan_cli.push.push_async", new_callable=AsyncMock)
    def test_versions_json_uploaded_last(
        self, mock_push: MagicMock, mock_obs_put: MagicMock, tmp_path: Path
    ) -> None:
        """versions.json is uploaded last (manifest-last atomicity per ADR-0005)."""
        _setup_valid_catalog(tmp_path)
        _create_catalog_json(tmp_path)
        _create_root_readme(tmp_path)
        _create_root_versions(tmp_path)
        _create_collection(tmp_path, "col1")

        mock_push.return_value = PushResult(
            success=True,
            files_uploaded=5,
            versions_pushed=1,
            conflicts=[],
            errors=[],
        )

        push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=False,
            profile=None,
        )

        # Check order: versions.json should be last
        put_calls = mock_obs_put.call_args_list
        uploaded_keys = [c.args[1] for c in put_calls]

        # Find indices
        versions_idx = next((i for i, k in enumerate(uploaded_keys) if "versions.json" in k), -1)
        catalog_idx = next((i for i, k in enumerate(uploaded_keys) if "catalog.json" in k), -1)

        assert versions_idx > catalog_idx, (
            f"versions.json should be uploaded after catalog.json. Order: {uploaded_keys}"
        )

    @patch("portolan_cli.push.obs.put")
    @patch("portolan_cli.push.push_async", new_callable=AsyncMock)
    def test_skips_optional_files_when_not_present(
        self, mock_push: MagicMock, mock_obs_put: MagicMock, tmp_path: Path
    ) -> None:
        """push_all_collections doesn't fail if README.md or versions.json missing."""
        _setup_valid_catalog(tmp_path)
        _create_catalog_json(tmp_path)
        # No root README.md or versions.json
        _create_collection(tmp_path, "col1")

        mock_push.return_value = PushResult(
            success=True,
            files_uploaded=5,
            versions_pushed=1,
            conflicts=[],
            errors=[],
        )

        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=False,
            profile=None,
        )

        assert result.success is True

        put_calls = mock_obs_put.call_args_list
        uploaded_keys = [c.args[1] for c in put_calls]

        # catalog.json should be uploaded
        assert any(k.endswith("/catalog.json") or k == "catalog.json" for k in uploaded_keys)
        # Optional files should NOT be uploaded
        assert not any(k.endswith("/README.md") or k == "README.md" for k in uploaded_keys)
        assert not any(k.endswith("/versions.json") or k == "versions.json" for k in uploaded_keys)

    @patch("portolan_cli.push.obs.put")
    @patch("portolan_cli.push.push_async", new_callable=AsyncMock)
    def test_skips_root_files_when_collection_fails(
        self, mock_push: MagicMock, mock_obs_put: MagicMock, tmp_path: Path
    ) -> None:
        """push_all_collections skips root files when any collection fails."""
        _setup_valid_catalog(tmp_path)
        _create_catalog_json(tmp_path)
        _create_root_readme(tmp_path)
        _create_root_versions(tmp_path)
        _create_collection(tmp_path, "col1")

        mock_push.return_value = PushResult(
            success=False,
            files_uploaded=0,
            versions_pushed=0,
            conflicts=[],
            errors=["Upload failed"],
        )

        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=False,
            profile=None,
        )

        assert result.success is False

        # No root files should be uploaded when collections fail
        mock_obs_put.assert_not_called()

    @patch("portolan_cli.push.obs.put")
    @patch("portolan_cli.push.push_async", new_callable=AsyncMock)
    def test_skips_root_files_when_zero_successful(
        self, mock_push: MagicMock, mock_obs_put: MagicMock, tmp_path: Path
    ) -> None:
        """push_all_collections skips root files when no collections succeeded.

        This handles edge cases like empty catalogs or all collections skipped.
        """
        _setup_valid_catalog(tmp_path)
        _create_catalog_json(tmp_path)
        _create_root_versions(tmp_path)
        # No collections - push won't be called but stats["successful"] == 0

        # Simulate scenario where discover_collections returns empty
        # This is handled before _push_all_upload_root_files is called
        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=False,
            profile=None,
        )

        # Should succeed but with 0 files uploaded
        assert result.success is True
        assert result.total_files_uploaded == 0
        mock_obs_put.assert_not_called()

    @patch("portolan_cli.push.obs.put")
    @patch("portolan_cli.push.push_async", new_callable=AsyncMock)
    def test_dry_run_shows_all_root_files(
        self, mock_push: MagicMock, mock_obs_put: MagicMock, tmp_path: Path
    ) -> None:
        """push_all_collections in dry_run mode mentions all root files."""
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

        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=True,
            profile=None,
        )

        assert result.success is True
        # In dry_run, obs.put should NOT be called
        mock_obs_put.assert_not_called()

    @patch("portolan_cli.push.obs.put")
    @patch("portolan_cli.push.push_async", new_callable=AsyncMock)
    def test_counts_all_root_files_in_total(
        self, mock_push: MagicMock, mock_obs_put: MagicMock, tmp_path: Path
    ) -> None:
        """All root files are counted in total_files_uploaded."""
        _setup_valid_catalog(tmp_path)
        _create_catalog_json(tmp_path)
        _create_root_readme(tmp_path)
        _create_root_versions(tmp_path)
        _create_collection(tmp_path, "col1")

        mock_push.return_value = PushResult(
            success=True,
            files_uploaded=5,
            versions_pushed=1,
            conflicts=[],
            errors=[],
        )

        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=False,
            profile=None,
        )

        assert result.success is True
        # 5 from collection + 3 root files (README.md + catalog.json + versions.json)
        assert result.total_files_uploaded == 8

    @patch("portolan_cli.push.setup_store")
    @patch("portolan_cli.push.push_async", new_callable=AsyncMock)
    def test_handles_upload_error(
        self, mock_push: MagicMock, mock_setup_store: MagicMock, tmp_path: Path
    ) -> None:
        """push_all_collections handles errors during root file upload."""
        _setup_valid_catalog(tmp_path)
        _create_catalog_json(tmp_path)
        _create_root_readme(tmp_path)
        _create_collection(tmp_path, "col1")

        mock_push.return_value = PushResult(
            success=True,
            files_uploaded=5,
            versions_pushed=1,
            conflicts=[],
            errors=[],
        )

        # Make setup_store raise an error
        mock_setup_store.side_effect = ConnectionError("Network unreachable")

        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=False,
            profile=None,
        )

        # Should fail due to root file upload error
        assert result.success is False
        assert "root_files" in result.collection_errors

    @patch("portolan_cli.push.obs.put")
    @patch("portolan_cli.push.setup_store")
    @patch("portolan_cli.push.push_async", new_callable=AsyncMock)
    def test_handles_partial_upload_failure(
        self,
        mock_push: MagicMock,
        mock_setup_store: MagicMock,
        mock_obs_put: MagicMock,
        tmp_path: Path,
    ) -> None:
        """push_all_collections handles failure after partial upload."""
        _setup_valid_catalog(tmp_path)
        _create_catalog_json(tmp_path)
        _create_root_readme(tmp_path)
        _create_root_versions(tmp_path)
        _create_collection(tmp_path, "col1")

        mock_push.return_value = PushResult(
            success=True,
            files_uploaded=5,
            versions_pushed=1,
            conflicts=[],
            errors=[],
        )

        mock_setup_store.return_value = (MagicMock(), "catalog")

        # First call (README) succeeds, second call (catalog.json) fails
        mock_obs_put.side_effect = [None, OSError("Disk full")]

        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=False,
            profile=None,
        )

        # Should fail due to partial upload error
        assert result.success is False
        assert "root_files" in result.collection_errors
