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


class TestDiscoverCollections:
    """Tests for discover_collections() function."""

    def test_finds_single_collection(self, tmp_path: Path) -> None:
        """discover_collections finds a single collection with versions.json."""
        # Setup: create a collection directory with versions.json
        collection_dir = tmp_path / "demographics"
        collection_dir.mkdir()
        versions_json = collection_dir / "versions.json"
        versions_json.write_text(json.dumps({"versions": []}))

        collections = discover_collections(tmp_path)

        assert collections == ["demographics"]

    def test_finds_multiple_collections(self, tmp_path: Path) -> None:
        """discover_collections finds all collections in catalog."""
        # Setup: create multiple collections
        for name in ["nature", "climate", "environment"]:
            collection_dir = tmp_path / name
            collection_dir.mkdir()
            (collection_dir / "versions.json").write_text(json.dumps({"versions": []}))

        collections = discover_collections(tmp_path)

        assert set(collections) == {"nature", "climate", "environment"}

    def test_ignores_directories_without_versions_json(self, tmp_path: Path) -> None:
        """discover_collections ignores dirs without versions.json."""
        # Setup: mix of initialized and uninitialized collections
        (tmp_path / "initialized").mkdir()
        (tmp_path / "initialized" / "versions.json").write_text(json.dumps({"versions": []}))

        (tmp_path / "uninitialized").mkdir()  # No versions.json

        collections = discover_collections(tmp_path)

        assert collections == ["initialized"]

    def test_ignores_dotfiles_and_hidden_dirs(self, tmp_path: Path) -> None:
        """discover_collections ignores .portolan and other hidden directories."""
        # Setup: create hidden directories with versions.json
        (tmp_path / ".portolan").mkdir()
        (tmp_path / ".portolan" / "versions.json").write_text(json.dumps({"versions": []}))

        (tmp_path / ".hidden").mkdir()
        (tmp_path / ".hidden" / "versions.json").write_text(json.dumps({"versions": []}))

        # Setup: create a valid collection
        (tmp_path / "valid").mkdir()
        (tmp_path / "valid" / "versions.json").write_text(json.dumps({"versions": []}))

        collections = discover_collections(tmp_path)

        assert collections == ["valid"]

    def test_returns_empty_list_for_empty_catalog(self, tmp_path: Path) -> None:
        """discover_collections returns empty list if no collections found."""
        collections = discover_collections(tmp_path)

        assert collections == []

    def test_returns_sorted_collections(self, tmp_path: Path) -> None:
        """discover_collections returns collections in sorted order."""
        # Setup: create collections in non-alphabetical order
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
        # Setup
        (tmp_path / "col1").mkdir()
        (tmp_path / "col1" / "versions.json").write_text(json.dumps({"versions": []}))

        mock_push.return_value = PushResult(
            success=True,
            files_uploaded=5,
            versions_pushed=1,
            conflicts=[],
            errors=[],
        )

        # Execute
        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=False,
            profile=None,
        )

        # Assert
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
        # Setup: create multiple collections
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

        # Execute
        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=False,
            profile=None,
        )

        # Assert
        assert result.success is True
        assert result.total_collections == 3
        assert result.successful_collections == 3
        assert result.failed_collections == 0
        assert result.total_files_uploaded == 6  # 2 * 3
        assert result.total_versions_pushed == 3  # 1 * 3

        assert mock_push.call_count == 3

    @patch("portolan_cli.push.push")
    def test_continues_on_individual_collection_failure(
        self, mock_push: MagicMock, tmp_path: Path
    ) -> None:
        """push_all_collections continues processing after individual failures."""
        # Setup
        for name in ["col1", "col2", "col3"]:
            (tmp_path / name).mkdir()
            (tmp_path / name / "versions.json").write_text(json.dumps({"versions": []}))

        # Mock: col2 fails, others succeed
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

        # Execute
        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=False,
            profile=None,
        )

        # Assert: overall failure, but processed all collections
        assert result.success is False
        assert result.total_collections == 3
        assert result.successful_collections == 2
        assert result.failed_collections == 1
        assert result.total_files_uploaded == 4  # 2 * 2 (col1 + col3)
        assert result.total_versions_pushed == 2  # 1 * 2
        assert len(result.collection_errors) == 1
        assert "col2" in result.collection_errors

        assert mock_push.call_count == 3

    @patch("portolan_cli.push.push")
    def test_reports_all_errors_at_end(self, mock_push: MagicMock, tmp_path: Path) -> None:
        """push_all_collections collects and reports all errors."""
        # Setup
        for name in ["col1", "col2"]:
            (tmp_path / name).mkdir()
            (tmp_path / name / "versions.json").write_text(json.dumps({"versions": []}))

        # Mock: both fail with different errors
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

        # Execute
        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=False,
            profile=None,
        )

        # Assert
        assert result.success is False
        assert result.failed_collections == 2
        assert len(result.collection_errors) == 2
        assert "col1" in result.collection_errors
        assert "col2" in result.collection_errors

    @patch("portolan_cli.push.push")
    def test_handles_empty_catalog(self, mock_push: MagicMock, tmp_path: Path) -> None:
        """push_all_collections handles empty catalog gracefully."""
        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=False,
            profile=None,
        )

        assert result.success is True
        assert result.total_collections == 0
        assert result.successful_collections == 0
        assert result.failed_collections == 0

        mock_push.assert_not_called()

    @patch("portolan_cli.push.push")
    def test_dry_run_mode(self, mock_push: MagicMock, tmp_path: Path) -> None:
        """push_all_collections passes dry_run flag to individual pushes."""
        # Setup
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

        # Execute
        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=True,
            profile=None,
        )

        # Assert
        assert result.success is True
        mock_push.assert_called_once_with(
            catalog_root=tmp_path,
            collection="col1",
            destination="s3://bucket/catalog",
            force=False,
            dry_run=True,
            profile=None,
        )
