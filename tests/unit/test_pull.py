"""Unit tests for pull module.

Tests for pulling updates from remote cloud storage catalogs.
These tests mock network calls and test pull logic in isolation.

Test categories:
- PullResult dataclass
- Uncommitted change detection
- Version diffing (local vs remote)
- Pull operation with force/dry-run modes
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

if TYPE_CHECKING:
    pass


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def catalog_with_versions(tmp_path: Path) -> Path:
    """Create a catalog with a versions.json file."""
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()

    # Create .portolan directory structure
    portolan_dir = catalog_root / ".portolan" / "collections" / "test-collection"
    portolan_dir.mkdir(parents=True)

    # Create versions.json
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
                        "sha256": "abc123",
                        "size_bytes": 1000,
                        "href": "data.parquet",
                    }
                },
                "changes": ["data.parquet"],
            }
        ],
    }
    (portolan_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))

    # Create the actual data file
    data_file = catalog_root / "data.parquet"
    data_file.write_bytes(b"x" * 1000)

    return catalog_root


@pytest.fixture
def remote_versions_data() -> dict:
    """Remote versions.json with a newer version."""
    return {
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
                        "sha256": "abc123",
                        "size_bytes": 1000,
                        "href": "data.parquet",
                    }
                },
                "changes": ["data.parquet"],
            },
            {
                "version": "1.1.0",
                "created": "2024-01-20T10:00:00Z",
                "breaking": False,
                "message": "Updated data",
                "assets": {
                    "data.parquet": {
                        "sha256": "def456",
                        "size_bytes": 2000,
                        "href": "data.parquet",
                    }
                },
                "changes": ["data.parquet"],
            },
        ],
    }


# =============================================================================
# PullResult Tests
# =============================================================================


class TestPullResult:
    """Tests for PullResult dataclass."""

    @pytest.mark.unit
    def test_pull_result_success(self) -> None:
        """PullResult should track successful pulls."""
        from portolan_cli.pull import PullResult

        result = PullResult(
            success=True,
            files_downloaded=3,
            files_skipped=1,
            local_version="1.0.0",
            remote_version="1.1.0",
            uncommitted_changes=[],
        )

        assert result.success is True
        assert result.files_downloaded == 3
        assert result.files_skipped == 1
        assert result.local_version == "1.0.0"
        assert result.remote_version == "1.1.0"
        assert result.uncommitted_changes == []

    @pytest.mark.unit
    def test_pull_result_with_uncommitted_changes(self) -> None:
        """PullResult should track uncommitted changes that blocked pull."""
        from portolan_cli.pull import PullResult

        result = PullResult(
            success=False,
            files_downloaded=0,
            files_skipped=0,
            local_version="1.0.0",
            remote_version="1.1.0",
            uncommitted_changes=["data.parquet", "other.parquet"],
        )

        assert result.success is False
        assert result.uncommitted_changes == ["data.parquet", "other.parquet"]

    @pytest.mark.unit
    def test_pull_result_already_up_to_date(self) -> None:
        """PullResult should indicate when already up to date."""
        from portolan_cli.pull import PullResult

        result = PullResult(
            success=True,
            files_downloaded=0,
            files_skipped=0,
            local_version="1.1.0",
            remote_version="1.1.0",
            uncommitted_changes=[],
            up_to_date=True,
        )

        assert result.success is True
        assert result.up_to_date is True


# =============================================================================
# Uncommitted Change Detection Tests
# =============================================================================


class TestUncommittedChangeDetection:
    """Tests for detecting uncommitted local changes."""

    @pytest.mark.unit
    def test_detect_modified_file(self, catalog_with_versions: Path) -> None:
        """Should detect when local file differs from versions.json checksum."""
        from portolan_cli.pull import detect_uncommitted_changes

        # Modify the local file (different content = different checksum)
        data_file = catalog_with_versions / "data.parquet"
        data_file.write_bytes(b"modified content")

        changes = detect_uncommitted_changes(
            catalog_root=catalog_with_versions,
            collection="test-collection",
        )

        assert "data.parquet" in changes

    @pytest.mark.unit
    def test_no_changes_when_file_matches(self, catalog_with_versions: Path) -> None:
        """Should return empty list when files match versions.json."""
        from portolan_cli.pull import detect_uncommitted_changes

        # The fixture creates matching file, but we need correct checksum
        # For this test, mock the checksum comparison
        with patch("portolan_cli.pull.compute_checksum") as mock_checksum:
            mock_checksum.return_value = "abc123"  # Matches versions.json

            changes = detect_uncommitted_changes(
                catalog_root=catalog_with_versions,
                collection="test-collection",
            )

        assert changes == []

    @pytest.mark.unit
    def test_detect_missing_file(self, catalog_with_versions: Path) -> None:
        """Should detect when expected file is missing."""
        from portolan_cli.pull import detect_uncommitted_changes

        # Delete the data file
        data_file = catalog_with_versions / "data.parquet"
        data_file.unlink()

        changes = detect_uncommitted_changes(
            catalog_root=catalog_with_versions,
            collection="test-collection",
        )

        assert "data.parquet" in changes

    @pytest.mark.unit
    def test_detect_new_untracked_file(self, catalog_with_versions: Path) -> None:
        """Should detect new files not in versions.json (like git status)."""
        from portolan_cli.pull import detect_uncommitted_changes

        # Create a new file not tracked in versions.json
        new_file = catalog_with_versions / "new_data.parquet"
        new_file.write_bytes(b"new content")

        # Mock checksum for existing file to match
        with patch("portolan_cli.pull.compute_checksum") as mock_checksum:
            mock_checksum.return_value = "abc123"

            changes = detect_uncommitted_changes(
                catalog_root=catalog_with_versions,
                collection="test-collection",
            )

        # New untracked files don't block pull (they won't be overwritten)
        # Only tracked files that differ matter
        assert "new_data.parquet" not in changes


# =============================================================================
# Version Diffing Tests
# =============================================================================


class TestVersionDiffing:
    """Tests for diffing local vs remote versions."""

    @pytest.mark.unit
    def test_diff_versions_newer_remote(self) -> None:
        """Should identify files changed between local and remote versions."""
        from portolan_cli.pull import diff_versions
        from portolan_cli.versions import Asset, Version, VersionsFile

        local_versions = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime(2024, 1, 15, tzinfo=timezone.utc),
                    breaking=False,
                    assets={
                        "data.parquet": Asset(sha256="abc123", size_bytes=1000, href="data.parquet")
                    },
                    changes=["data.parquet"],
                )
            ],
        )

        remote_versions = VersionsFile(
            spec_version="1.0.0",
            current_version="1.1.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime(2024, 1, 15, tzinfo=timezone.utc),
                    breaking=False,
                    assets={
                        "data.parquet": Asset(sha256="abc123", size_bytes=1000, href="data.parquet")
                    },
                    changes=["data.parquet"],
                ),
                Version(
                    version="1.1.0",
                    created=datetime(2024, 1, 20, tzinfo=timezone.utc),
                    breaking=False,
                    assets={
                        "data.parquet": Asset(sha256="def456", size_bytes=2000, href="data.parquet")
                    },
                    changes=["data.parquet"],
                ),
            ],
        )

        diff = diff_versions(local_versions, remote_versions)

        assert diff.local_version == "1.0.0"
        assert diff.remote_version == "1.1.0"
        assert "data.parquet" in diff.files_to_download
        assert diff.is_behind is True

    @pytest.mark.unit
    def test_diff_versions_up_to_date(self) -> None:
        """Should indicate no changes when versions match."""
        from portolan_cli.pull import diff_versions
        from portolan_cli.versions import Asset, Version, VersionsFile

        versions = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime(2024, 1, 15, tzinfo=timezone.utc),
                    breaking=False,
                    assets={
                        "data.parquet": Asset(sha256="abc123", size_bytes=1000, href="data.parquet")
                    },
                    changes=["data.parquet"],
                )
            ],
        )

        diff = diff_versions(versions, versions)

        assert diff.is_behind is False
        assert diff.files_to_download == []

    @pytest.mark.unit
    def test_diff_versions_no_local_versions(self) -> None:
        """Should download all files when local has no versions."""
        from portolan_cli.pull import diff_versions
        from portolan_cli.versions import Asset, Version, VersionsFile

        local_versions = VersionsFile(
            spec_version="1.0.0",
            current_version=None,
            versions=[],
        )

        remote_versions = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime(2024, 1, 15, tzinfo=timezone.utc),
                    breaking=False,
                    assets={
                        "data.parquet": Asset(
                            sha256="abc123", size_bytes=1000, href="data.parquet"
                        ),
                        "metadata.json": Asset(
                            sha256="xyz789", size_bytes=500, href="metadata.json"
                        ),
                    },
                    changes=["data.parquet", "metadata.json"],
                )
            ],
        )

        diff = diff_versions(local_versions, remote_versions)

        assert diff.is_behind is True
        assert set(diff.files_to_download) == {"data.parquet", "metadata.json"}


# =============================================================================
# Pull Operation Tests
# =============================================================================


class TestPullOperation:
    """Tests for the main pull function."""

    @pytest.mark.unit
    def test_pull_refuses_with_uncommitted_changes(
        self, catalog_with_versions: Path, remote_versions_data: dict
    ) -> None:
        """Pull should refuse when local has uncommitted changes."""
        from portolan_cli.pull import pull

        # Modify local file to create uncommitted change
        data_file = catalog_with_versions / "data.parquet"
        data_file.write_bytes(b"modified content - uncommitted")

        with patch("portolan_cli.pull._fetch_remote_versions") as mock_fetch:
            from portolan_cli.versions import _parse_versions_file

            mock_fetch.return_value = _parse_versions_file(remote_versions_data)

            result = pull(
                remote_url="s3://bucket/catalog",
                local_root=catalog_with_versions,
                collection="test-collection",
            )

        assert result.success is False
        assert len(result.uncommitted_changes) > 0
        assert "data.parquet" in result.uncommitted_changes

    @pytest.mark.unit
    def test_pull_force_overwrites_uncommitted(
        self, catalog_with_versions: Path, remote_versions_data: dict
    ) -> None:
        """Pull --force should overwrite uncommitted changes."""
        from portolan_cli.pull import pull

        # Modify local file
        data_file = catalog_with_versions / "data.parquet"
        data_file.write_bytes(b"modified content - will be overwritten")

        with patch("portolan_cli.pull._fetch_remote_versions") as mock_fetch:
            with patch("portolan_cli.pull._download_assets") as mock_download:
                from portolan_cli.versions import _parse_versions_file

                mock_fetch.return_value = _parse_versions_file(remote_versions_data)
                mock_download.return_value = (1, 0)  # 1 downloaded, 0 failed

                result = pull(
                    remote_url="s3://bucket/catalog",
                    local_root=catalog_with_versions,
                    collection="test-collection",
                    force=True,
                )

        assert result.success is True
        mock_download.assert_called_once()

    @pytest.mark.unit
    def test_pull_dry_run_no_downloads(
        self, catalog_with_versions: Path, remote_versions_data: dict
    ) -> None:
        """Pull --dry-run should not download anything."""
        from portolan_cli.pull import pull

        with patch("portolan_cli.pull._fetch_remote_versions") as mock_fetch:
            with patch("portolan_cli.pull._download_assets") as mock_download:
                with patch("portolan_cli.pull.compute_checksum") as mock_checksum:
                    from portolan_cli.versions import _parse_versions_file

                    mock_fetch.return_value = _parse_versions_file(remote_versions_data)
                    mock_checksum.return_value = "abc123"  # Match local version
                    mock_download.return_value = (0, 0)  # dry_run returns (0, 0)

                    result = pull(
                        remote_url="s3://bucket/catalog",
                        local_root=catalog_with_versions,
                        collection="test-collection",
                        dry_run=True,
                    )

        # In dry-run mode, _download_assets is called but doesn't actually download
        # It prints what would be done and returns (0, 0)
        mock_download.assert_called_once()
        # Verify dry_run=True was passed
        call_kwargs = mock_download.call_args.kwargs
        assert call_kwargs.get("dry_run") is True
        assert result.files_downloaded == 0

    @pytest.mark.unit
    def test_pull_already_up_to_date(self, catalog_with_versions: Path) -> None:
        """Pull should indicate when already up to date."""
        from portolan_cli.pull import pull

        # Use same version as local
        same_versions_data = {
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
                            "sha256": "abc123",
                            "size_bytes": 1000,
                            "href": "data.parquet",
                        }
                    },
                    "changes": ["data.parquet"],
                }
            ],
        }

        with patch("portolan_cli.pull._fetch_remote_versions") as mock_fetch:
            with patch("portolan_cli.pull.compute_checksum") as mock_checksum:
                from portolan_cli.versions import _parse_versions_file

                mock_fetch.return_value = _parse_versions_file(same_versions_data)
                mock_checksum.return_value = "abc123"

                result = pull(
                    remote_url="s3://bucket/catalog",
                    local_root=catalog_with_versions,
                    collection="test-collection",
                )

        assert result.success is True
        assert result.up_to_date is True
        assert result.files_downloaded == 0

    @pytest.mark.unit
    def test_pull_updates_local_versions_json(
        self, catalog_with_versions: Path, remote_versions_data: dict
    ) -> None:
        """Pull should update local versions.json after successful download."""
        from portolan_cli.pull import pull

        with patch("portolan_cli.pull._fetch_remote_versions") as mock_fetch:
            with patch("portolan_cli.pull._download_assets") as mock_download:
                with patch("portolan_cli.pull.compute_checksum") as mock_checksum:
                    from portolan_cli.versions import _parse_versions_file

                    mock_fetch.return_value = _parse_versions_file(remote_versions_data)
                    mock_download.return_value = (1, 0)
                    mock_checksum.return_value = "abc123"

                    result = pull(
                        remote_url="s3://bucket/catalog",
                        local_root=catalog_with_versions,
                        collection="test-collection",
                    )

        assert result.success is True

        # Verify local versions.json was updated
        versions_path = (
            catalog_with_versions
            / ".portolan"
            / "collections"
            / "test-collection"
            / "versions.json"
        )
        updated_versions = json.loads(versions_path.read_text())
        assert updated_versions["current_version"] == "1.1.0"


# =============================================================================
# Remote Fetch Tests
# =============================================================================


class TestRemoteFetch:
    """Tests for fetching remote versions.json."""

    @pytest.mark.unit
    def test_fetch_remote_versions_s3(self) -> None:
        """Should fetch versions.json from S3."""
        from portolan_cli.pull import _fetch_remote_versions

        remote_data = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [],
        }

        with patch("portolan_cli.pull.download_file") as mock_download:
            # Simulate download writing the file
            def write_versions(source: str, destination: Path, **kwargs) -> MagicMock:
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_text(json.dumps(remote_data))
                result = MagicMock()
                result.success = True
                return result

            mock_download.side_effect = write_versions

            result = _fetch_remote_versions(
                remote_url="s3://bucket/catalog",
                collection="test-collection",
            )

        assert result.spec_version == "1.0.0"
        assert result.current_version == "1.0.0"

    @pytest.mark.unit
    def test_fetch_remote_versions_not_found(self) -> None:
        """Should raise error when remote versions.json doesn't exist."""
        from portolan_cli.pull import PullError, _fetch_remote_versions

        with patch("portolan_cli.pull.download_file") as mock_download:
            mock_result = MagicMock()
            mock_result.success = False
            mock_result.errors = [(Path("versions.json"), FileNotFoundError("Not found"))]
            mock_download.return_value = mock_result

            with pytest.raises(PullError, match="versions.json"):
                _fetch_remote_versions(
                    remote_url="s3://bucket/catalog",
                    collection="test-collection",
                )


# =============================================================================
# Edge Cases and Error Handling
# =============================================================================


class TestErrorHandling:
    """Tests for error handling in pull operations."""

    @pytest.mark.unit
    def test_pull_handles_download_failure(
        self, catalog_with_versions: Path, remote_versions_data: dict
    ) -> None:
        """Pull should handle download failures gracefully."""
        from portolan_cli.pull import pull

        with patch("portolan_cli.pull._fetch_remote_versions") as mock_fetch:
            with patch("portolan_cli.pull._download_assets") as mock_download:
                with patch("portolan_cli.pull.compute_checksum") as mock_checksum:
                    from portolan_cli.versions import _parse_versions_file

                    mock_fetch.return_value = _parse_versions_file(remote_versions_data)
                    mock_download.return_value = (0, 1)  # 0 downloaded, 1 failed
                    mock_checksum.return_value = "abc123"

                    result = pull(
                        remote_url="s3://bucket/catalog",
                        local_root=catalog_with_versions,
                        collection="test-collection",
                    )

        assert result.success is False

    @pytest.mark.unit
    def test_pull_invalid_remote_url(self, catalog_with_versions: Path) -> None:
        """Pull should reject invalid remote URLs."""
        from portolan_cli.pull import pull

        with pytest.raises(ValueError, match="URL"):
            pull(
                remote_url="invalid://url",
                local_root=catalog_with_versions,
                collection="test-collection",
            )

    @pytest.mark.unit
    def test_pull_missing_local_catalog(self, tmp_path: Path) -> None:
        """Pull should handle missing local catalog gracefully."""
        from portolan_cli.pull import pull

        # Empty directory - no .portolan
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        with patch("portolan_cli.pull._fetch_remote_versions") as mock_fetch:
            with patch("portolan_cli.pull._download_assets") as mock_download:
                from portolan_cli.versions import VersionsFile

                mock_fetch.return_value = VersionsFile(
                    spec_version="1.0.0",
                    current_version="1.0.0",
                    versions=[],
                )
                mock_download.return_value = (0, 0)

                # Should work - creates local structure
                result = pull(
                    remote_url="s3://bucket/catalog",
                    local_root=empty_dir,
                    collection="test-collection",
                )

        assert result.success is True


# =============================================================================
# Security Tests - Path Traversal
# =============================================================================


class TestPathTraversalProtection:
    """Tests for path traversal protection in pull operations."""

    @pytest.mark.unit
    def test_download_assets_rejects_path_traversal(self, tmp_path: Path) -> None:
        """Should reject hrefs containing path traversal sequences."""
        from portolan_cli.pull import _download_assets

        local_root = tmp_path / "catalog"
        local_root.mkdir()

        # Malicious href with path traversal
        remote_assets = {
            "evil.parquet": {
                "sha256": "abc123",
                "size_bytes": 1000,
                "href": "../../../etc/passwd",  # Path traversal attack
            }
        }

        with pytest.raises(ValueError, match="path traversal"):
            _download_assets(
                remote_url="s3://bucket/catalog",
                local_root=local_root,
                files_to_download=["evil.parquet"],
                remote_assets=remote_assets,
            )

    @pytest.mark.unit
    def test_download_assets_rejects_absolute_paths(self, tmp_path: Path) -> None:
        """Should reject absolute hrefs that could write outside catalog."""
        from portolan_cli.pull import _download_assets

        local_root = tmp_path / "catalog"
        local_root.mkdir()

        # Absolute path href
        remote_assets = {
            "evil.parquet": {
                "sha256": "abc123",
                "size_bytes": 1000,
                "href": "/etc/passwd",  # Absolute path attack
            }
        }

        with pytest.raises(ValueError, match="[Aa]bsolute|escapes"):
            _download_assets(
                remote_url="s3://bucket/catalog",
                local_root=local_root,
                files_to_download=["evil.parquet"],
                remote_assets=remote_assets,
            )

    @pytest.mark.unit
    def test_download_assets_accepts_safe_relative_paths(self, tmp_path: Path) -> None:
        """Should accept valid relative paths within catalog."""
        from portolan_cli.pull import _download_assets

        local_root = tmp_path / "catalog"
        local_root.mkdir()

        remote_assets = {
            "data.parquet": {
                "sha256": "abc123",
                "size_bytes": 1000,
                "href": "data/subdir/file.parquet",  # Valid nested path
            }
        }

        with patch("portolan_cli.pull.download_file") as mock_download:
            mock_result = MagicMock()
            mock_result.success = True
            mock_download.return_value = mock_result

            # Should not raise
            downloaded, failed = _download_assets(
                remote_url="s3://bucket/catalog",
                local_root=local_root,
                files_to_download=["data.parquet"],
                remote_assets=remote_assets,
            )

        assert failed == 0


# =============================================================================
# Data Integrity Tests - Local Ahead Detection
# =============================================================================


class TestLocalAheadDetection:
    """Tests for detecting when local is ahead of remote (data loss prevention)."""

    @pytest.mark.unit
    def test_diff_versions_detects_local_ahead(self) -> None:
        """Should detect when local has versions not in remote."""
        from portolan_cli.pull import diff_versions
        from portolan_cli.versions import Asset, Version, VersionsFile

        # Local has v1.0.0 and v1.1.0
        local_versions = VersionsFile(
            spec_version="1.0.0",
            current_version="1.1.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime(2024, 1, 15, tzinfo=timezone.utc),
                    breaking=False,
                    assets={
                        "data.parquet": Asset(sha256="abc123", size_bytes=1000, href="data.parquet")
                    },
                    changes=["data.parquet"],
                ),
                Version(
                    version="1.1.0",
                    created=datetime(2024, 1, 20, tzinfo=timezone.utc),
                    breaking=False,
                    assets={
                        "data.parquet": Asset(sha256="def456", size_bytes=2000, href="data.parquet")
                    },
                    changes=["data.parquet"],
                ),
            ],
        )

        # Remote only has v1.0.0
        remote_versions = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime(2024, 1, 15, tzinfo=timezone.utc),
                    breaking=False,
                    assets={
                        "data.parquet": Asset(sha256="abc123", size_bytes=1000, href="data.parquet")
                    },
                    changes=["data.parquet"],
                ),
            ],
        )

        diff = diff_versions(local_versions, remote_versions)

        # Local is ahead - should NOT download (would lose local changes)
        assert diff.is_local_ahead is True
        assert diff.is_behind is False

    @pytest.mark.unit
    def test_diff_versions_detects_diverged(self) -> None:
        """Should detect when local and remote have diverged."""
        from portolan_cli.pull import diff_versions
        from portolan_cli.versions import Asset, Version, VersionsFile

        # Local has v1.0.0 and v1.1.0-local
        local_versions = VersionsFile(
            spec_version="1.0.0",
            current_version="1.1.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime(2024, 1, 15, tzinfo=timezone.utc),
                    breaking=False,
                    assets={
                        "data.parquet": Asset(sha256="abc123", size_bytes=1000, href="data.parquet")
                    },
                    changes=["data.parquet"],
                ),
                Version(
                    version="1.1.0",
                    created=datetime(2024, 1, 20, tzinfo=timezone.utc),
                    breaking=False,
                    assets={
                        "data.parquet": Asset(
                            sha256="local456", size_bytes=2000, href="data.parquet"
                        )
                    },
                    changes=["data.parquet"],
                ),
            ],
        )

        # Remote has v1.0.0 and v1.0.1-remote (different branch)
        remote_versions = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.1",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime(2024, 1, 15, tzinfo=timezone.utc),
                    breaking=False,
                    assets={
                        "data.parquet": Asset(sha256="abc123", size_bytes=1000, href="data.parquet")
                    },
                    changes=["data.parquet"],
                ),
                Version(
                    version="1.0.1",
                    created=datetime(2024, 1, 18, tzinfo=timezone.utc),
                    breaking=False,
                    assets={
                        "data.parquet": Asset(
                            sha256="remote789", size_bytes=1500, href="data.parquet"
                        )
                    },
                    changes=["data.parquet"],
                ),
            ],
        )

        diff = diff_versions(local_versions, remote_versions)

        # Both have unique versions - diverged
        assert diff.is_diverged is True


# =============================================================================
# Progress Reporting Tests
# =============================================================================


class TestProgressReporting:
    """Tests for progress reporting during downloads."""

    @pytest.mark.unit
    def test_download_assets_shows_progress(self, tmp_path: Path, capsys) -> None:
        """_download_assets should show (1/N) style progress."""
        from portolan_cli.pull import _download_assets

        local_root = tmp_path / "catalog"
        local_root.mkdir()

        remote_assets = {
            "file1.parquet": {
                "sha256": "abc123",
                "size_bytes": 1000,
                "href": "file1.parquet",
            },
            "file2.parquet": {
                "sha256": "def456",
                "size_bytes": 2000,
                "href": "file2.parquet",
            },
        }

        with patch("portolan_cli.pull.download_file") as mock_download:
            mock_result = MagicMock()
            mock_result.success = True
            mock_download.return_value = mock_result

            _download_assets(
                remote_url="s3://bucket/catalog",
                local_root=local_root,
                files_to_download=["file1.parquet", "file2.parquet"],
                remote_assets=remote_assets,
            )

        captured = capsys.readouterr()
        assert "(1/2)" in captured.out
        assert "(2/2)" in captured.out

    @pytest.mark.unit
    def test_download_assets_dry_run_shows_progress(self, tmp_path: Path, capsys) -> None:
        """Dry-run should also show progress indicators."""
        from portolan_cli.pull import _download_assets

        local_root = tmp_path / "catalog"
        local_root.mkdir()

        remote_assets = {
            "data.parquet": {
                "sha256": "abc123",
                "size_bytes": 1000,
                "href": "data.parquet",
            },
        }

        _download_assets(
            remote_url="s3://bucket/catalog",
            local_root=local_root,
            files_to_download=["data.parquet"],
            remote_assets=remote_assets,
            dry_run=True,
        )

        captured = capsys.readouterr()
        assert "(1/1)" in captured.out
        assert "DRY RUN" in captured.out
