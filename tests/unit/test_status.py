"""Tests for the status command.

Tests the status module (library layer) and CLI command that shows
tracking states: untracked, tracked, modified, deleted.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.status import (
    FileStatus,
    StatusResult,
    get_catalog_status,
)


class TestStatusLibrary:
    """Tests for the status library functions."""

    @pytest.mark.unit
    def test_empty_catalog_returns_empty_status(self, tmp_path: Path) -> None:
        """A catalog with no collections returns an empty status result."""
        # Create minimal catalog
        catalog_json = tmp_path / "catalog.json"
        catalog_json.write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test-catalog",
                    "stac_version": "1.0.0",
                    "description": "Test catalog",
                    "links": [],
                }
            )
        )

        result = get_catalog_status(tmp_path)

        assert result.untracked == []
        assert result.modified == []
        assert result.deleted == []
        assert result.is_clean()

    @pytest.mark.unit
    def test_untracked_file_detected(self, tmp_path: Path) -> None:
        """Files in collection dir but not in versions.json are untracked."""
        # Create catalog structure
        catalog_json = tmp_path / "catalog.json"
        catalog_json.write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test-catalog",
                    "stac_version": "1.0.0",
                    "description": "Test catalog",
                    "links": [{"rel": "child", "href": "./demographics/collection.json"}],
                }
            )
        )

        # Create collection with a geospatial file but no versions.json
        col_dir = tmp_path / "demographics"
        col_dir.mkdir()
        collection_json = col_dir / "collection.json"
        collection_json.write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "demographics",
                    "stac_version": "1.0.0",
                    "description": "Test collection",
                    "license": "proprietary",
                    "extent": {
                        "spatial": {"bbox": [[-180, -90, 180, 90]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                }
            )
        )

        # Add an untracked geospatial file
        data_file = col_dir / "census.parquet"
        data_file.write_bytes(b"fake parquet data")

        result = get_catalog_status(tmp_path)

        assert len(result.untracked) == 1
        assert result.untracked[0].collection_id == "demographics"
        assert result.untracked[0].filename == "census.parquet"
        assert not result.is_clean()

    @pytest.mark.unit
    def test_tracked_file_not_in_results(self, tmp_path: Path) -> None:
        """Files that are tracked and unchanged don't appear in status."""
        # Create catalog structure
        catalog_json = tmp_path / "catalog.json"
        catalog_json.write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test-catalog",
                    "stac_version": "1.0.0",
                    "description": "Test catalog",
                    "links": [{"rel": "child", "href": "./demographics/collection.json"}],
                }
            )
        )

        col_dir = tmp_path / "demographics"
        col_dir.mkdir()
        collection_json = col_dir / "collection.json"
        collection_json.write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "demographics",
                    "stac_version": "1.0.0",
                    "description": "Test collection",
                    "license": "proprietary",
                    "extent": {
                        "spatial": {"bbox": [[-180, -90, 180, 90]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                }
            )
        )

        # Add a tracked file with matching versions.json
        data_file = col_dir / "census.parquet"
        data_file.write_bytes(b"fake parquet data")

        # Create versions.json tracking this file
        portolan_dir = col_dir / ".portolan"
        portolan_dir.mkdir()
        versions_json = portolan_dir / "versions.json"
        file_stat = data_file.stat()
        versions_json.write_text(
            json.dumps(
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [
                        {
                            "version": "1.0.0",
                            "created": "2024-01-15T10:30:00Z",
                            "breaking": False,
                            "assets": {
                                "census.parquet": {
                                    "sha256": "abc123",
                                    "size_bytes": file_stat.st_size,
                                    "href": "demographics/census.parquet",
                                    "mtime": file_stat.st_mtime,
                                }
                            },
                            "changes": ["census.parquet"],
                        }
                    ],
                }
            )
        )

        result = get_catalog_status(tmp_path)

        # File is tracked and unchanged, should not appear in any list
        assert result.untracked == []
        assert result.modified == []
        assert result.deleted == []
        assert result.is_clean()

    @pytest.mark.unit
    def test_modified_file_detected(self, tmp_path: Path) -> None:
        """Files with changed content are detected as modified."""
        # Create catalog structure
        catalog_json = tmp_path / "catalog.json"
        catalog_json.write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test-catalog",
                    "stac_version": "1.0.0",
                    "description": "Test catalog",
                    "links": [{"rel": "child", "href": "./demographics/collection.json"}],
                }
            )
        )

        col_dir = tmp_path / "demographics"
        col_dir.mkdir()
        collection_json = col_dir / "collection.json"
        collection_json.write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "demographics",
                    "stac_version": "1.0.0",
                    "description": "Test collection",
                    "license": "proprietary",
                    "extent": {
                        "spatial": {"bbox": [[-180, -90, 180, 90]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                }
            )
        )

        # Add a file
        data_file = col_dir / "census.parquet"
        data_file.write_bytes(b"fake parquet data")

        # Create versions.json with different mtime/size (simulating change)
        portolan_dir = col_dir / ".portolan"
        portolan_dir.mkdir()
        versions_json = portolan_dir / "versions.json"
        versions_json.write_text(
            json.dumps(
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [
                        {
                            "version": "1.0.0",
                            "created": "2024-01-15T10:30:00Z",
                            "breaking": False,
                            "assets": {
                                "census.parquet": {
                                    "sha256": "different_hash",
                                    "size_bytes": 9999,  # Different size
                                    "href": "demographics/census.parquet",
                                    "mtime": 0.0,  # Different mtime
                                }
                            },
                            "changes": ["census.parquet"],
                        }
                    ],
                }
            )
        )

        result = get_catalog_status(tmp_path)

        assert len(result.modified) == 1
        assert result.modified[0].collection_id == "demographics"
        assert result.modified[0].filename == "census.parquet"
        assert not result.is_clean()

    @pytest.mark.unit
    def test_deleted_file_detected(self, tmp_path: Path) -> None:
        """Files in versions.json but missing from disk are deleted."""
        # Create catalog structure
        catalog_json = tmp_path / "catalog.json"
        catalog_json.write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test-catalog",
                    "stac_version": "1.0.0",
                    "description": "Test catalog",
                    "links": [{"rel": "child", "href": "./demographics/collection.json"}],
                }
            )
        )

        col_dir = tmp_path / "demographics"
        col_dir.mkdir()
        collection_json = col_dir / "collection.json"
        collection_json.write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "demographics",
                    "stac_version": "1.0.0",
                    "description": "Test collection",
                    "license": "proprietary",
                    "extent": {
                        "spatial": {"bbox": [[-180, -90, 180, 90]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                }
            )
        )

        # Create versions.json referencing a file that doesn't exist
        portolan_dir = col_dir / ".portolan"
        portolan_dir.mkdir()
        versions_json = portolan_dir / "versions.json"
        versions_json.write_text(
            json.dumps(
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [
                        {
                            "version": "1.0.0",
                            "created": "2024-01-15T10:30:00Z",
                            "breaking": False,
                            "assets": {
                                "deleted_file.parquet": {
                                    "sha256": "abc123",
                                    "size_bytes": 1024,
                                    "href": "demographics/deleted_file.parquet",
                                }
                            },
                            "changes": ["deleted_file.parquet"],
                        }
                    ],
                }
            )
        )

        result = get_catalog_status(tmp_path)

        assert len(result.deleted) == 1
        assert result.deleted[0].collection_id == "demographics"
        assert result.deleted[0].filename == "deleted_file.parquet"
        assert not result.is_clean()

    @pytest.mark.unit
    def test_multiple_collections(self, tmp_path: Path) -> None:
        """Status aggregates across multiple collections."""
        # Create catalog
        catalog_json = tmp_path / "catalog.json"
        catalog_json.write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test-catalog",
                    "stac_version": "1.0.0",
                    "description": "Test catalog",
                    "links": [
                        {"rel": "child", "href": "./demographics/collection.json"},
                        {"rel": "child", "href": "./imagery/collection.json"},
                    ],
                }
            )
        )

        # Collection 1: demographics with untracked file
        col1 = tmp_path / "demographics"
        col1.mkdir()
        (col1 / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "demographics",
                    "stac_version": "1.0.0",
                    "description": "Demographics",
                    "license": "proprietary",
                    "extent": {
                        "spatial": {"bbox": [[-180, -90, 180, 90]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                }
            )
        )
        (col1 / "new-file.parquet").write_bytes(b"data")

        # Collection 2: imagery with deleted file
        col2 = tmp_path / "imagery"
        col2.mkdir()
        (col2 / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "imagery",
                    "stac_version": "1.0.0",
                    "description": "Imagery",
                    "license": "proprietary",
                    "extent": {
                        "spatial": {"bbox": [[-180, -90, 180, 90]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                }
            )
        )
        portolan2 = col2 / ".portolan"
        portolan2.mkdir()
        (portolan2 / "versions.json").write_text(
            json.dumps(
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [
                        {
                            "version": "1.0.0",
                            "created": "2024-01-15T10:30:00Z",
                            "breaking": False,
                            "assets": {
                                "satellite.tif": {
                                    "sha256": "xyz789",
                                    "size_bytes": 1024,
                                    "href": "imagery/satellite.tif",
                                }
                            },
                            "changes": ["satellite.tif"],
                        }
                    ],
                }
            )
        )

        result = get_catalog_status(tmp_path)

        assert len(result.untracked) == 1
        assert result.untracked[0].collection_id == "demographics"
        assert len(result.deleted) == 1
        assert result.deleted[0].collection_id == "imagery"

    @pytest.mark.unit
    def test_non_geospatial_files_ignored(self, tmp_path: Path) -> None:
        """Non-geospatial files (README, etc.) are not reported."""
        # Create catalog
        catalog_json = tmp_path / "catalog.json"
        catalog_json.write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test-catalog",
                    "stac_version": "1.0.0",
                    "description": "Test catalog",
                    "links": [{"rel": "child", "href": "./demographics/collection.json"}],
                }
            )
        )

        col_dir = tmp_path / "demographics"
        col_dir.mkdir()
        (col_dir / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "demographics",
                    "stac_version": "1.0.0",
                    "description": "Test collection",
                    "license": "proprietary",
                    "extent": {
                        "spatial": {"bbox": [[-180, -90, 180, 90]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                }
            )
        )

        # Add non-geospatial files
        (col_dir / "README.md").write_text("# Documentation")
        (col_dir / "notes.txt").write_text("Some notes")

        result = get_catalog_status(tmp_path)

        assert result.untracked == []
        assert result.is_clean()

    @pytest.mark.unit
    def test_no_catalog_raises_error(self, tmp_path: Path) -> None:
        """get_catalog_status raises error when no catalog exists."""
        with pytest.raises(FileNotFoundError, match="catalog.json not found"):
            get_catalog_status(tmp_path)


class TestStatusCLI:
    """Tests for the status CLI command."""

    @pytest.mark.unit
    def test_status_clean_catalog(self, tmp_path: Path) -> None:
        """Status command shows clean message when nothing to report."""
        import os

        # Create minimal catalog
        catalog_json = tmp_path / "catalog.json"
        catalog_json.write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test-catalog",
                    "stac_version": "1.0.0",
                    "description": "Test catalog",
                    "links": [],
                }
            )
        )

        runner = CliRunner()
        # Change to catalog directory before running command
        old_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            result = runner.invoke(cli, ["status"], catch_exceptions=False)
        finally:
            os.chdir(old_cwd)

        assert result.exit_code == 0
        assert "Nothing to commit, working tree clean" in result.output

    @pytest.mark.unit
    def test_status_shows_untracked(self, tmp_path: Path) -> None:
        """Status command shows untracked files with # prefix."""
        import os

        # Create catalog with untracked file
        catalog_json = tmp_path / "catalog.json"
        catalog_json.write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test-catalog",
                    "stac_version": "1.0.0",
                    "description": "Test catalog",
                    "links": [{"rel": "child", "href": "./demographics/collection.json"}],
                }
            )
        )

        col_dir = tmp_path / "demographics"
        col_dir.mkdir()
        (col_dir / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "demographics",
                    "stac_version": "1.0.0",
                    "description": "Test collection",
                    "license": "proprietary",
                    "extent": {
                        "spatial": {"bbox": [[-180, -90, 180, 90]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                }
            )
        )
        (col_dir / "new-file.parquet").write_bytes(b"data")

        runner = CliRunner()
        # Change to catalog directory before running command
        old_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            result = runner.invoke(
                cli,
                ["status"],
                catch_exceptions=False,
            )
        finally:
            os.chdir(old_cwd)

        assert result.exit_code == 0
        assert "Untracked:" in result.output
        assert "demographics/new-file.parquet" in result.output

    @pytest.mark.unit
    def test_status_shows_modified(self, tmp_path: Path) -> None:
        """Status command shows modified files."""
        import os

        # Create catalog with modified file
        catalog_json = tmp_path / "catalog.json"
        catalog_json.write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test-catalog",
                    "stac_version": "1.0.0",
                    "description": "Test catalog",
                    "links": [{"rel": "child", "href": "./imagery/collection.json"}],
                }
            )
        )

        col_dir = tmp_path / "imagery"
        col_dir.mkdir()
        (col_dir / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "imagery",
                    "stac_version": "1.0.0",
                    "description": "Test collection",
                    "license": "proprietary",
                    "extent": {
                        "spatial": {"bbox": [[-180, -90, 180, 90]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                }
            )
        )
        data_file = col_dir / "satellite.tif"
        data_file.write_bytes(b"modified data")

        # versions.json with different checksum
        portolan_dir = col_dir / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "versions.json").write_text(
            json.dumps(
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [
                        {
                            "version": "1.0.0",
                            "created": "2024-01-15T10:30:00Z",
                            "breaking": False,
                            "assets": {
                                "satellite.tif": {
                                    "sha256": "old_hash",
                                    "size_bytes": 9999,
                                    "href": "imagery/satellite.tif",
                                    "mtime": 0.0,
                                }
                            },
                            "changes": ["satellite.tif"],
                        }
                    ],
                }
            )
        )

        runner = CliRunner()
        # Change to catalog directory before running command
        old_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            result = runner.invoke(
                cli,
                ["status"],
                catch_exceptions=False,
            )
        finally:
            os.chdir(old_cwd)

        assert result.exit_code == 0
        assert "Modified:" in result.output
        assert "imagery/satellite.tif" in result.output

    @pytest.mark.unit
    def test_status_no_catalog_error(self, tmp_path: Path) -> None:
        """Status command exits with error when no catalog found."""
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ["status"], catch_exceptions=False)

        assert result.exit_code == 1
        assert "catalog.json not found" in result.output or "No catalog found" in result.output


class TestFileStatus:
    """Tests for the FileStatus dataclass."""

    @pytest.mark.unit
    def test_file_status_path_property(self) -> None:
        """FileStatus.path returns collection_id/filename."""
        fs = FileStatus(collection_id="demographics", filename="census.parquet")
        assert fs.path == "demographics/census.parquet"

    @pytest.mark.unit
    def test_file_status_equality(self) -> None:
        """FileStatus instances with same values are equal."""
        fs1 = FileStatus(collection_id="demographics", filename="census.parquet")
        fs2 = FileStatus(collection_id="demographics", filename="census.parquet")
        assert fs1 == fs2


class TestStatusResult:
    """Tests for the StatusResult dataclass."""

    @pytest.mark.unit
    def test_is_clean_all_empty(self) -> None:
        """is_clean returns True when all lists are empty."""
        result = StatusResult(untracked=[], modified=[], deleted=[])
        assert result.is_clean()

    @pytest.mark.unit
    def test_is_clean_with_untracked(self) -> None:
        """is_clean returns False when untracked files exist."""
        result = StatusResult(
            untracked=[FileStatus("col", "file.parquet")],
            modified=[],
            deleted=[],
        )
        assert not result.is_clean()

    @pytest.mark.unit
    def test_is_clean_with_modified(self) -> None:
        """is_clean returns False when modified files exist."""
        result = StatusResult(
            untracked=[],
            modified=[FileStatus("col", "file.parquet")],
            deleted=[],
        )
        assert not result.is_clean()

    @pytest.mark.unit
    def test_is_clean_with_deleted(self) -> None:
        """is_clean returns False when deleted files exist."""
        result = StatusResult(
            untracked=[],
            modified=[],
            deleted=[FileStatus("col", "file.parquet")],
        )
        assert not result.is_clean()
