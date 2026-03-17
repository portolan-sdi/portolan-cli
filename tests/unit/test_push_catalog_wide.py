"""Tests for catalog-wide push functionality (issue #224).

This module tests the ability to push all collections at once without
specifying --collection flag.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from portolan_cli.push import (
    PushResult,
    discover_collections,
    push_all_collections,
)


def _setup_valid_catalog(catalog_root: Path) -> None:
    """Helper to create a valid catalog with .portolan/config.yaml."""
    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir(parents=True, exist_ok=True)
    (portolan_dir / "config.yaml").write_text("version: '1.0'\n")


class TestDiscoverCollections:
    """Tests for discover_collections() function."""

    def test_finds_single_collection(self, tmp_path: Path) -> None:
        """discover_collections finds a single collection with versions.json."""
        _setup_valid_catalog(tmp_path)

        collection_dir = tmp_path / "demographics"
        collection_dir.mkdir()
        (collection_dir / "versions.json").write_text(json.dumps({"versions": []}))

        collections = discover_collections(tmp_path)

        assert collections == ["demographics"]

    def test_finds_multiple_collections(self, tmp_path: Path) -> None:
        """discover_collections finds all collections in catalog."""
        _setup_valid_catalog(tmp_path)

        for name in ["nature", "climate", "environment"]:
            collection_dir = tmp_path / name
            collection_dir.mkdir()
            (collection_dir / "versions.json").write_text(json.dumps({"versions": []}))

        collections = discover_collections(tmp_path)

        assert set(collections) == {"nature", "climate", "environment"}

    def test_ignores_directories_without_versions_json(self, tmp_path: Path) -> None:
        """discover_collections ignores dirs without versions.json."""
        _setup_valid_catalog(tmp_path)

        (tmp_path / "initialized").mkdir()
        (tmp_path / "initialized" / "versions.json").write_text(json.dumps({"versions": []}))
        (tmp_path / "uninitialized").mkdir()

        collections = discover_collections(tmp_path)

        assert collections == ["initialized"]

    def test_ignores_dotfiles_and_hidden_dirs(self, tmp_path: Path) -> None:
        """discover_collections ignores .portolan and other hidden directories."""
        _setup_valid_catalog(tmp_path)

        (tmp_path / ".hidden").mkdir()
        (tmp_path / ".hidden" / "versions.json").write_text(json.dumps({"versions": []}))

        (tmp_path / "valid").mkdir()
        (tmp_path / "valid" / "versions.json").write_text(json.dumps({"versions": []}))

        collections = discover_collections(tmp_path)

        assert collections == ["valid"]

    def test_raises_for_non_catalog_directory(self, tmp_path: Path) -> None:
        """discover_collections raises ValueError if not a portolan catalog."""
        import pytest

        with pytest.raises(ValueError, match="Not a portolan catalog"):
            discover_collections(tmp_path)

    def test_returns_sorted_collections(self, tmp_path: Path) -> None:
        """discover_collections returns collections in sorted order."""
        _setup_valid_catalog(tmp_path)

        for name in ["zebra", "apple", "mango"]:
            collection_dir = tmp_path / name
            collection_dir.mkdir()
            (collection_dir / "versions.json").write_text(json.dumps({"versions": []}))

        collections = discover_collections(tmp_path)

        assert collections == ["apple", "mango", "zebra"]


class TestPushAllCollections:
    """Tests for push_all_collections() function."""

    @patch("portolan_cli.push.push")
    def test_pushes_single_collection(self, mock_push: MagicMock, tmp_path: Path) -> None:
        """push_all_collections pushes a single collection successfully."""
        _setup_valid_catalog(tmp_path)

        (tmp_path / "col1").mkdir()
        (tmp_path / "col1" / "versions.json").write_text(json.dumps({"versions": []}))

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
        assert result.total_collections == 1
        assert result.successful_collections == 1
        assert result.failed_collections == 0
        assert result.total_files_uploaded == 5
        assert result.total_versions_pushed == 1

        mock_push.assert_called_once_with(
            catalog_root=tmp_path,
            collection="col1",
            destination="s3://bucket/catalog",
            force=False,
            dry_run=False,
            profile=None,
        )

    @patch("portolan_cli.push.push")
    def test_pushes_multiple_collections_sequentially(
        self, mock_push: MagicMock, tmp_path: Path
    ) -> None:
        """push_all_collections processes multiple collections in sequence."""
        _setup_valid_catalog(tmp_path)

        for name in ["col1", "col2", "col3"]:
            (tmp_path / name).mkdir()
            (tmp_path / name / "versions.json").write_text(json.dumps({"versions": []}))

        mock_push.return_value = PushResult(
            success=True,
            files_uploaded=2,
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
        assert result.total_collections == 3
        assert result.successful_collections == 3
        assert result.failed_collections == 0
        assert result.total_files_uploaded == 6
        assert result.total_versions_pushed == 3

        assert mock_push.call_count == 3

    @patch("portolan_cli.push.push")
    def test_continues_on_individual_collection_failure(
        self, mock_push: MagicMock, tmp_path: Path
    ) -> None:
        """push_all_collections continues processing after individual failures."""
        _setup_valid_catalog(tmp_path)

        for name in ["col1", "col2", "col3"]:
            (tmp_path / name).mkdir()
            (tmp_path / name / "versions.json").write_text(json.dumps({"versions": []}))

        def push_side_effect(**kwargs):  # type: ignore[no-untyped-def]
            if kwargs["collection"] == "col2":
                return PushResult(
                    success=False,
                    files_uploaded=0,
                    versions_pushed=0,
                    conflicts=[],
                    errors=["Network error"],
                )
            return PushResult(
                success=True,
                files_uploaded=2,
                versions_pushed=1,
                conflicts=[],
                errors=[],
            )

        mock_push.side_effect = push_side_effect

        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=False,
            profile=None,
        )

        assert result.success is False
        assert result.total_collections == 3
        assert result.successful_collections == 2
        assert result.failed_collections == 1
        assert result.total_files_uploaded == 4
        assert result.total_versions_pushed == 2
        assert len(result.collection_errors) == 1
        assert "col2" in result.collection_errors

        assert mock_push.call_count == 3

    @patch("portolan_cli.push.push")
    def test_reports_all_errors_at_end(self, mock_push: MagicMock, tmp_path: Path) -> None:
        """push_all_collections collects and reports all errors."""
        _setup_valid_catalog(tmp_path)

        for name in ["col1", "col2"]:
            (tmp_path / name).mkdir()
            (tmp_path / name / "versions.json").write_text(json.dumps({"versions": []}))

        def push_side_effect(**kwargs):  # type: ignore[no-untyped-def]
            if kwargs["collection"] == "col1":
                return PushResult(
                    success=False,
                    files_uploaded=0,
                    versions_pushed=0,
                    conflicts=["Conflict detected"],
                    errors=[],
                )
            return PushResult(
                success=False,
                files_uploaded=0,
                versions_pushed=0,
                conflicts=[],
                errors=["Upload failed"],
            )

        mock_push.side_effect = push_side_effect

        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=False,
            profile=None,
        )

        assert result.success is False
        assert result.failed_collections == 2
        assert len(result.collection_errors) == 2
        assert "col1" in result.collection_errors
        assert "col2" in result.collection_errors

    @patch("portolan_cli.push.push")
    def test_handles_empty_catalog(self, mock_push: MagicMock, tmp_path: Path) -> None:
        """push_all_collections handles empty catalog with warning."""
        _setup_valid_catalog(tmp_path)

        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=False,
            profile=None,
        )

        assert result.success is False  # Changed: empty catalog is not success
        assert result.total_collections == 0
        assert result.successful_collections == 0
        assert result.failed_collections == 0

        mock_push.assert_not_called()

    @patch("portolan_cli.push.push")
    def test_dry_run_mode(self, mock_push: MagicMock, tmp_path: Path) -> None:
        """push_all_collections passes dry_run flag to individual pushes."""
        _setup_valid_catalog(tmp_path)

        (tmp_path / "col1").mkdir()
        (tmp_path / "col1" / "versions.json").write_text(json.dumps({"versions": []}))

        mock_push.return_value = PushResult(
            success=True,
            files_uploaded=0,
            versions_pushed=0,
            conflicts=[],
            errors=[],
            dry_run=True,
            would_push_versions=5,
        )

        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=True,
            profile=None,
        )

        assert result.success is True
        mock_push.assert_called_once_with(
            catalog_root=tmp_path,
            collection="col1",
            destination="s3://bucket/catalog",
            force=False,
            dry_run=True,
            profile=None,
        )
