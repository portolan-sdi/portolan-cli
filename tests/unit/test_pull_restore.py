"""Unit tests for pull --restore functionality (Issue #325).

Tests for restoring missing files when local versions.json matches remote
but actual data files are missing from disk.

Test categories:
- find_missing_files() helper function
- pull with restore=True when versions match but files missing
- Interaction between --restore and --force flags
- Dry-run behavior with --restore
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def catalog_with_matching_versions(tmp_path: Path) -> Path:
    """Create a catalog where local and remote versions match.

    Used to test the restore scenario where versions match but files are missing.
    """
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()

    # Create catalog.json
    catalog_data = {
        "type": "Catalog",
        "id": "test-catalog",
        "stac_version": "1.0.0",
        "description": "Test catalog",
        "links": [],
    }
    (catalog_root / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    # Create .portolan directory
    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir(parents=True)

    # Create collection directory
    collection_dir = catalog_root / "test-collection"
    collection_dir.mkdir(parents=True)

    # Create versions.json with version 1.0.0
    versions_data = {
        "spec_version": "1.0.0",
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-15T10:00:00Z",
                "breaking": False,
                "message": "Initial version",
                "assets": {
                    "data.parquet": {
                        "sha256": "abc123def456",
                        "size_bytes": 1000,
                        "href": "test-collection/data.parquet",
                    },
                    "metadata.json": {
                        "sha256": "meta789xyz",
                        "size_bytes": 500,
                        "href": "test-collection/metadata.json",
                    },
                },
                "changes": ["data.parquet", "metadata.json"],
            }
        ],
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))

    # Create the actual data files (they exist initially)
    (collection_dir / "data.parquet").write_bytes(b"x" * 1000)
    (collection_dir / "metadata.json").write_text('{"test": true}')

    return catalog_root


@pytest.fixture
def matching_remote_versions() -> dict[str, Any]:
    """Remote versions.json that matches local exactly (same version 1.0.0)."""
    return {
        "spec_version": "1.0.0",
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-15T10:00:00Z",
                "breaking": False,
                "message": "Initial version",
                "assets": {
                    "data.parquet": {
                        "sha256": "abc123def456",
                        "size_bytes": 1000,
                        "href": "test-collection/data.parquet",
                    },
                    "metadata.json": {
                        "sha256": "meta789xyz",
                        "size_bytes": 500,
                        "href": "test-collection/metadata.json",
                    },
                },
                "changes": ["data.parquet", "metadata.json"],
            }
        ],
    }


# =============================================================================
# Tests for find_missing_files()
# =============================================================================


class TestFindMissingFiles:
    """Tests for the find_missing_files helper function."""

    @pytest.mark.unit
    def test_all_files_exist_returns_empty(self, catalog_with_matching_versions: Path) -> None:
        """When all files exist, return empty list."""
        from portolan_cli.pull import find_missing_files

        catalog_root = catalog_with_matching_versions
        collection = "test-collection"

        # All files exist in the fixture
        missing = find_missing_files(catalog_root, collection)

        assert missing == []

    @pytest.mark.unit
    def test_one_file_missing_returns_that_file(self, catalog_with_matching_versions: Path) -> None:
        """When one file is missing, return it in the list."""
        from portolan_cli.pull import find_missing_files

        catalog_root = catalog_with_matching_versions
        collection = "test-collection"

        # Delete one file
        (catalog_root / collection / "data.parquet").unlink()

        missing = find_missing_files(catalog_root, collection)

        assert missing == ["data.parquet"]

    @pytest.mark.unit
    def test_all_files_missing_returns_all(self, catalog_with_matching_versions: Path) -> None:
        """When all files are missing, return all of them."""
        from portolan_cli.pull import find_missing_files

        catalog_root = catalog_with_matching_versions
        collection = "test-collection"

        # Delete all files
        (catalog_root / collection / "data.parquet").unlink()
        (catalog_root / collection / "metadata.json").unlink()

        missing = find_missing_files(catalog_root, collection)

        assert sorted(missing) == ["data.parquet", "metadata.json"]

    @pytest.mark.unit
    def test_no_versions_json_returns_empty(self, tmp_path: Path) -> None:
        """When no versions.json exists, return empty list (nothing to restore)."""
        from portolan_cli.pull import find_missing_files

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        collection_dir = catalog_root / "test-collection"
        collection_dir.mkdir()

        # No versions.json created
        missing = find_missing_files(catalog_root, "test-collection")

        assert missing == []

    @pytest.mark.unit
    def test_empty_versions_returns_empty(self, tmp_path: Path) -> None:
        """When versions.json has no versions, return empty list."""
        from portolan_cli.pull import find_missing_files

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        collection_dir = catalog_root / "test-collection"
        collection_dir.mkdir()

        # Create empty versions.json
        versions_data = {
            "spec_version": "1.0.0",
            "current_version": None,
            "versions": [],
        }
        (collection_dir / "versions.json").write_text(json.dumps(versions_data))

        missing = find_missing_files(catalog_root, "test-collection")

        assert missing == []


# =============================================================================
# Tests for pull with restore=True
# =============================================================================


class TestPullRestore:
    """Tests for pull operation with restore flag."""

    @pytest.mark.unit
    def test_restore_downloads_missing_files_when_versions_match(
        self,
        catalog_with_matching_versions: Path,
        matching_remote_versions: dict[str, Any],
    ) -> None:
        """restore=True should download missing files even when versions match."""
        from portolan_cli.pull import pull
        from portolan_cli.versions import _parse_versions_file

        catalog_root = catalog_with_matching_versions
        collection = "test-collection"

        # Delete the data file (simulating the issue scenario)
        data_file = catalog_root / collection / "data.parquet"
        data_file.unlink()

        # Mock remote fetch to return matching versions
        remote_versions = _parse_versions_file(matching_remote_versions)

        # Track what files get downloaded
        downloaded_files: list[str] = []

        async def mock_download(*args: Any, **kwargs: Any) -> tuple[int, int]:
            files_to_download = kwargs.get("files_to_download", [])
            downloaded_files.extend(files_to_download)
            # Simulate creating the files
            for f in files_to_download:
                (catalog_root / collection / f).write_bytes(b"restored")
            return len(files_to_download), 0

        with (
            patch(
                "portolan_cli.pull._fetch_remote_versions_async",
                new=AsyncMock(return_value=remote_versions),
            ),
            patch(
                "portolan_cli.pull._download_assets_async",
                new=mock_download,
            ),
            patch(
                "portolan_cli.pull._setup_store_and_kwargs",
                return_value=(AsyncMock(), {}),
            ),
        ):
            result = pull(
                remote_url="s3://test-bucket/catalog",
                local_root=catalog_root,
                collection=collection,
                restore=True,
            )

        assert result.success is True
        assert result.files_downloaded == 1
        assert "data.parquet" in downloaded_files
        # metadata.json should NOT be downloaded (it still exists)
        assert "metadata.json" not in downloaded_files

    @pytest.mark.unit
    def test_restore_false_does_not_download_missing_files(
        self,
        catalog_with_matching_versions: Path,
        matching_remote_versions: dict[str, Any],
    ) -> None:
        """Without restore=True, missing files are NOT downloaded (original behavior)."""
        from portolan_cli.pull import pull
        from portolan_cli.versions import _parse_versions_file

        catalog_root = catalog_with_matching_versions
        collection = "test-collection"

        # Delete the data file
        data_file = catalog_root / collection / "data.parquet"
        data_file.unlink()

        remote_versions = _parse_versions_file(matching_remote_versions)

        with (
            patch(
                "portolan_cli.pull._fetch_remote_versions_async",
                new=AsyncMock(return_value=remote_versions),
            ),
            patch(
                "portolan_cli.pull._setup_store_and_kwargs",
                return_value=(AsyncMock(), {}),
            ),
        ):
            result = pull(
                remote_url="s3://test-bucket/catalog",
                local_root=catalog_root,
                collection=collection,
                restore=False,  # Default behavior
            )

        # Without restore, should say "up to date" (versions match)
        assert result.success is True
        assert result.up_to_date is True
        assert result.files_downloaded == 0

    @pytest.mark.unit
    def test_restore_with_all_files_present_is_noop(
        self,
        catalog_with_matching_versions: Path,
        matching_remote_versions: dict[str, Any],
    ) -> None:
        """restore=True with all files present should still be 'up to date'."""
        from portolan_cli.pull import pull
        from portolan_cli.versions import _parse_versions_file

        catalog_root = catalog_with_matching_versions
        collection = "test-collection"

        # All files exist (fixture default)
        remote_versions = _parse_versions_file(matching_remote_versions)

        with (
            patch(
                "portolan_cli.pull._fetch_remote_versions_async",
                new=AsyncMock(return_value=remote_versions),
            ),
            patch(
                "portolan_cli.pull._setup_store_and_kwargs",
                return_value=(AsyncMock(), {}),
            ),
        ):
            result = pull(
                remote_url="s3://test-bucket/catalog",
                local_root=catalog_root,
                collection=collection,
                restore=True,
            )

        # With all files present, still up to date
        assert result.success is True
        assert result.up_to_date is True
        assert result.files_downloaded == 0

    @pytest.mark.unit
    def test_restore_combined_with_version_update(
        self,
        catalog_with_matching_versions: Path,
    ) -> None:
        """restore=True should work alongside normal version updates.

        Note: When a file is deleted locally and remote has a newer version,
        the deleted file is considered an "uncommitted change". We need --force
        to proceed with the pull in this case.
        """
        from portolan_cli.pull import pull
        from portolan_cli.versions import _parse_versions_file

        catalog_root = catalog_with_matching_versions
        collection = "test-collection"

        # Delete one file AND have a newer remote version
        (catalog_root / collection / "data.parquet").unlink()

        # Remote has a newer version 1.1.0 with new_file.txt
        remote_data = {
            "spec_version": "1.0.0",
            "current_version": "1.1.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2024-01-15T10:00:00Z",
                    "breaking": False,
                    "message": "Initial version",
                    "assets": {
                        "data.parquet": {
                            "sha256": "abc123def456",
                            "size_bytes": 1000,
                            "href": "test-collection/data.parquet",
                        },
                        "metadata.json": {
                            "sha256": "meta789xyz",
                            "size_bytes": 500,
                            "href": "test-collection/metadata.json",
                        },
                    },
                    "changes": ["data.parquet", "metadata.json"],
                },
                {
                    "version": "1.1.0",
                    "created": "2024-01-20T10:00:00Z",
                    "breaking": False,
                    "message": "Added new file",
                    "assets": {
                        "data.parquet": {
                            "sha256": "updated_checksum",
                            "size_bytes": 1500,
                            "href": "test-collection/data.parquet",
                        },
                        "metadata.json": {
                            "sha256": "meta789xyz",
                            "size_bytes": 500,
                            "href": "test-collection/metadata.json",
                        },
                        "new_file.txt": {
                            "sha256": "new_file_checksum",
                            "size_bytes": 200,
                            "href": "test-collection/new_file.txt",
                        },
                    },
                    "changes": ["data.parquet", "new_file.txt"],
                },
            ],
        }
        remote_versions = _parse_versions_file(remote_data)

        downloaded_files: list[str] = []

        async def mock_download(*args: Any, **kwargs: Any) -> tuple[int, int]:
            files_to_download = kwargs.get("files_to_download", [])
            downloaded_files.extend(files_to_download)
            for f in files_to_download:
                (catalog_root / collection / f).write_bytes(b"restored")
            return len(files_to_download), 0

        with (
            patch(
                "portolan_cli.pull._fetch_remote_versions_async",
                new=AsyncMock(return_value=remote_versions),
            ),
            patch(
                "portolan_cli.pull._download_assets_async",
                new=mock_download,
            ),
            patch(
                "portolan_cli.pull._setup_store_and_kwargs",
                return_value=(AsyncMock(), {}),
            ),
        ):
            result = pull(
                remote_url="s3://test-bucket/catalog",
                local_root=catalog_root,
                collection=collection,
                restore=True,
                force=True,  # Required because deleted file is an uncommitted change
            )

        assert result.success is True
        # Should download: data.parquet (changed checksum) + new_file.txt (new)
        # restore doesn't change behavior here since diff already includes data.parquet
        assert "data.parquet" in downloaded_files
        assert "new_file.txt" in downloaded_files


class TestPullRestoreDryRun:
    """Tests for --restore with --dry-run."""

    @pytest.mark.unit
    def test_dry_run_with_restore_shows_missing_files(
        self,
        catalog_with_matching_versions: Path,
    ) -> None:
        """dry-run with restore should report missing files without downloading."""
        from portolan_cli.pull import pull

        catalog_root = catalog_with_matching_versions
        collection = "test-collection"

        # Delete file to simulate missing
        (catalog_root / collection / "data.parquet").unlink()

        # dry_run=True should not make network calls (per existing behavior)
        result = pull(
            remote_url="s3://test-bucket/catalog",
            local_root=catalog_root,
            collection=collection,
            restore=True,
            dry_run=True,
        )

        # dry_run returns early with success=True per existing behavior
        assert result.dry_run is True
        assert result.success is True


class TestPullRestoreAndForce:
    """Tests for interaction between --restore and --force flags."""

    @pytest.mark.unit
    def test_restore_and_force_together(
        self,
        catalog_with_matching_versions: Path,
        matching_remote_versions: dict[str, Any],
    ) -> None:
        """--restore and --force can be used together."""
        from portolan_cli.pull import pull
        from portolan_cli.versions import _parse_versions_file

        catalog_root = catalog_with_matching_versions
        collection = "test-collection"

        # Delete file
        (catalog_root / collection / "data.parquet").unlink()

        remote_versions = _parse_versions_file(matching_remote_versions)

        downloaded_files: list[str] = []

        async def mock_download(*args: Any, **kwargs: Any) -> tuple[int, int]:
            files_to_download = kwargs.get("files_to_download", [])
            downloaded_files.extend(files_to_download)
            for f in files_to_download:
                (catalog_root / collection / f).write_bytes(b"restored")
            return len(files_to_download), 0

        with (
            patch(
                "portolan_cli.pull._fetch_remote_versions_async",
                new=AsyncMock(return_value=remote_versions),
            ),
            patch(
                "portolan_cli.pull._download_assets_async",
                new=mock_download,
            ),
            patch(
                "portolan_cli.pull._setup_store_and_kwargs",
                return_value=(AsyncMock(), {}),
            ),
        ):
            result = pull(
                remote_url="s3://test-bucket/catalog",
                local_root=catalog_root,
                collection=collection,
                restore=True,
                force=True,
            )

        assert result.success is True
        assert "data.parquet" in downloaded_files


# =============================================================================
# Tests for PullResult with restore
# =============================================================================


class TestPullResultRestore:
    """Tests for PullResult tracking restored files."""

    @pytest.mark.unit
    def test_pull_result_tracks_files_restored(self) -> None:
        """PullResult should have a files_restored field."""
        from portolan_cli.pull import PullResult

        result = PullResult(
            success=True,
            files_downloaded=3,
            files_skipped=0,
            local_version="1.0.0",
            remote_version="1.0.0",
            files_restored=2,  # New field
        )

        assert result.files_restored == 2

    @pytest.mark.unit
    def test_pull_result_files_restored_defaults_to_zero(self) -> None:
        """files_restored should default to 0 for backward compatibility."""
        from portolan_cli.pull import PullResult

        result = PullResult(
            success=True,
            files_downloaded=0,
            files_skipped=0,
            local_version="1.0.0",
            remote_version="1.0.0",
        )

        assert result.files_restored == 0
