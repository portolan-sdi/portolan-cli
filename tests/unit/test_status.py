"""Unit tests for portolan status command.

Tests the status module which shows local vs remote version state,
modified files, and untracked files (Issue #389).
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from portolan_cli.versions import Asset, Version, VersionsFile


def _sha256(data: str) -> str:
    """Generate valid SHA256 hash from string."""
    return hashlib.sha256(data.encode()).hexdigest()


def _create_versions_file(
    version: str = "1.0.0",
    assets: dict[str, tuple[str, int]] | None = None,
) -> VersionsFile:
    """Create a VersionsFile for testing.

    Args:
        version: Version string.
        assets: Dict of filename -> (content_for_hash, size_bytes).
    """
    if assets is None:
        assets = {"data.parquet": ("content1", 1024)}

    asset_objs = {
        name: Asset(
            sha256=_sha256(content),
            size_bytes=size,
            href=f"collection/{name}",
        )
        for name, (content, size) in assets.items()
    }

    ver = Version(
        version=version,
        created=datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
        breaking=False,
        assets=asset_objs,
        changes=list(asset_objs.keys()),
    )

    return VersionsFile(
        spec_version="1.0.0",
        current_version=version,
        versions=[ver],
    )


class TestDetectModifiedFiles:
    """Tests for detecting files modified since last version."""

    @pytest.mark.unit
    def test_no_modifications_when_checksums_match(self, tmp_path: Path) -> None:
        """Files with matching checksums are not reported as modified."""
        from portolan_cli.status import detect_modified_files

        # Create a file with known content
        collection_path = tmp_path / "collection"
        collection_path.mkdir()
        data_file = collection_path / "data.parquet"
        content = b"test content"
        data_file.write_bytes(content)

        # Create versions.json with matching checksum
        checksum = hashlib.sha256(content).hexdigest()
        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime.now(timezone.utc),
                    breaking=False,
                    assets={
                        "data.parquet": Asset(
                            sha256=checksum,
                            size_bytes=len(content),
                            href="collection/data.parquet",
                        )
                    },
                    changes=["data.parquet"],
                )
            ],
        )

        modified = detect_modified_files(collection_path, versions_file)
        assert modified == []

    @pytest.mark.unit
    def test_detects_modified_file(self, tmp_path: Path) -> None:
        """Files with different checksums are reported as modified."""
        from portolan_cli.status import detect_modified_files

        # Create a file with content different from versions.json
        collection_path = tmp_path / "collection"
        collection_path.mkdir()
        data_file = collection_path / "data.parquet"
        data_file.write_bytes(b"new content")

        # Create versions.json with OLD checksum
        old_checksum = hashlib.sha256(b"old content").hexdigest()
        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime.now(timezone.utc),
                    breaking=False,
                    assets={
                        "data.parquet": Asset(
                            sha256=old_checksum,
                            size_bytes=100,
                            href="collection/data.parquet",
                        )
                    },
                    changes=["data.parquet"],
                )
            ],
        )

        modified = detect_modified_files(collection_path, versions_file)
        assert modified == ["data.parquet"]

    @pytest.mark.unit
    def test_deleted_file_not_reported_as_modified(self, tmp_path: Path) -> None:
        """Files in versions.json but missing from disk are not in modified list."""
        from portolan_cli.status import detect_modified_files

        # Create collection without the file
        collection_path = tmp_path / "collection"
        collection_path.mkdir()

        # Create versions.json referencing a file that doesn't exist
        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime.now(timezone.utc),
                    breaking=False,
                    assets={
                        "missing.parquet": Asset(
                            sha256=_sha256("content"),
                            size_bytes=100,
                            href="collection/missing.parquet",
                        )
                    },
                    changes=["missing.parquet"],
                )
            ],
        )

        modified = detect_modified_files(collection_path, versions_file)
        assert modified == []


class TestDetectDeletedFiles:
    """Tests for detecting files deleted since last version."""

    @pytest.mark.unit
    def test_detects_deleted_file(self, tmp_path: Path) -> None:
        """Files in versions.json but missing from disk are reported as deleted."""
        from portolan_cli.status import detect_deleted_files

        collection_path = tmp_path / "collection"
        collection_path.mkdir()

        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime.now(timezone.utc),
                    breaking=False,
                    assets={
                        "deleted.parquet": Asset(
                            sha256=_sha256("content"),
                            size_bytes=100,
                            href="collection/deleted.parquet",
                        )
                    },
                    changes=["deleted.parquet"],
                )
            ],
        )

        deleted = detect_deleted_files(collection_path, versions_file)
        assert deleted == ["deleted.parquet"]

    @pytest.mark.unit
    def test_no_deleted_when_all_files_exist(self, tmp_path: Path) -> None:
        """No deleted files when all versioned files exist on disk."""
        from portolan_cli.status import detect_deleted_files

        collection_path = tmp_path / "collection"
        collection_path.mkdir()
        (collection_path / "data.parquet").write_bytes(b"content")

        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime.now(timezone.utc),
                    breaking=False,
                    assets={
                        "data.parquet": Asset(
                            sha256=_sha256("content"),
                            size_bytes=100,
                            href="collection/data.parquet",
                        )
                    },
                    changes=["data.parquet"],
                )
            ],
        )

        deleted = detect_deleted_files(collection_path, versions_file)
        assert deleted == []


class TestDetectUntrackedFiles:
    """Tests for detecting untracked files."""

    @pytest.mark.unit
    def test_detects_untracked_file(self, tmp_path: Path) -> None:
        """Files on disk but not in versions.json are reported as untracked."""
        from portolan_cli.status import detect_untracked_files

        collection_path = tmp_path / "collection"
        collection_path.mkdir()
        (collection_path / "untracked.parquet").write_bytes(b"content")

        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime.now(timezone.utc),
                    breaking=False,
                    assets={},
                    changes=[],
                )
            ],
        )

        untracked = detect_untracked_files(collection_path, versions_file)
        assert untracked == ["untracked.parquet"]

    @pytest.mark.unit
    def test_no_untracked_when_all_tracked(self, tmp_path: Path) -> None:
        """No untracked files when all files are in versions.json."""
        from portolan_cli.status import detect_untracked_files

        collection_path = tmp_path / "collection"
        collection_path.mkdir()
        (collection_path / "data.parquet").write_bytes(b"content")

        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime.now(timezone.utc),
                    breaking=False,
                    assets={
                        "data.parquet": Asset(
                            sha256=_sha256("content"),
                            size_bytes=100,
                            href="collection/data.parquet",
                        )
                    },
                    changes=["data.parquet"],
                )
            ],
        )

        untracked = detect_untracked_files(collection_path, versions_file)
        assert untracked == []

    @pytest.mark.unit
    def test_excludes_versions_json(self, tmp_path: Path) -> None:
        """versions.json itself is never reported as untracked."""
        from portolan_cli.status import detect_untracked_files

        collection_path = tmp_path / "collection"
        collection_path.mkdir()
        (collection_path / "versions.json").write_text("{}")

        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime.now(timezone.utc),
                    breaking=False,
                    assets={},
                    changes=[],
                )
            ],
        )

        untracked = detect_untracked_files(collection_path, versions_file)
        assert "versions.json" not in untracked

    @pytest.mark.unit
    def test_ignores_subdirectories(self, tmp_path: Path) -> None:
        """Subdirectories are not reported as untracked files."""
        from portolan_cli.status import detect_untracked_files

        collection_path = tmp_path / "collection"
        collection_path.mkdir()
        (collection_path / "subdir").mkdir()

        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime.now(timezone.utc),
                    breaking=False,
                    assets={},
                    changes=[],
                )
            ],
        )

        untracked = detect_untracked_files(collection_path, versions_file)
        assert untracked == []

    @pytest.mark.unit
    def test_all_files_untracked_with_no_versions(self, tmp_path: Path) -> None:
        """All files are untracked when versions list is empty."""
        from portolan_cli.status import detect_untracked_files

        collection_path = tmp_path / "collection"
        collection_path.mkdir()
        (collection_path / "file1.parquet").write_bytes(b"a")
        (collection_path / "file2.parquet").write_bytes(b"b")

        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version=None,
            versions=[],
        )

        untracked = detect_untracked_files(collection_path, versions_file)
        assert sorted(untracked) == ["file1.parquet", "file2.parquet"]


class TestParseVersion:
    """Tests for semver parsing in sync_state calculation."""

    @pytest.mark.unit
    def test_parses_standard_semver(self) -> None:
        """Standard semver strings are parsed correctly."""
        from portolan_cli.status import CollectionStatus

        result = CollectionStatus._parse_version("1.2.3")
        assert result == (1, 2, 3)

    @pytest.mark.unit
    def test_parses_semver_with_prerelease(self) -> None:
        """Prerelease tags are stripped before parsing."""
        from portolan_cli.status import CollectionStatus

        result = CollectionStatus._parse_version("1.2.3-beta.1")
        assert result == (1, 2, 3)

    @pytest.mark.unit
    def test_parses_semver_with_build_metadata(self) -> None:
        """Build metadata is stripped before parsing."""
        from portolan_cli.status import CollectionStatus

        result = CollectionStatus._parse_version("1.2.3+build.456")
        assert result == (1, 2, 3)

    @pytest.mark.unit
    def test_handles_invalid_version(self) -> None:
        """Invalid version strings return (0, 0, 0)."""
        from portolan_cli.status import CollectionStatus

        result = CollectionStatus._parse_version("not-a-version")
        assert result == (0, 0, 0)

    @pytest.mark.unit
    def test_handles_partial_version(self) -> None:
        """Partial version strings return (0, 0, 0)."""
        from portolan_cli.status import CollectionStatus

        result = CollectionStatus._parse_version("1.2")
        assert result == (0, 0, 0)

    @pytest.mark.unit
    def test_handles_empty_string(self) -> None:
        """Empty string returns (0, 0, 0)."""
        from portolan_cli.status import CollectionStatus

        result = CollectionStatus._parse_version("")
        assert result == (0, 0, 0)


class TestCollectionStatus:
    """Tests for the main get_collection_status function."""

    @pytest.mark.unit
    def test_returns_local_version(self, tmp_path: Path) -> None:
        """Status includes the current local version."""
        from portolan_cli.status import CollectionStatus, get_collection_status

        # Set up collection with versions.json
        collection_path = tmp_path / "collection"
        collection_path.mkdir()
        versions_path = collection_path / "versions.json"

        versions_path.write_text(
            json.dumps(
                {
                    "spec_version": "1.0.0",
                    "current_version": "2.1.0",
                    "versions": [
                        {
                            "version": "2.1.0",
                            "created": "2024-01-15T10:30:00Z",
                            "breaking": False,
                            "assets": {},
                            "changes": [],
                        }
                    ],
                }
            )
        )

        status = get_collection_status(
            catalog_root=tmp_path,
            collection="collection",
            offline=True,
        )

        assert isinstance(status, CollectionStatus)
        assert status.local_version == "2.1.0"

    @pytest.mark.unit
    def test_no_versions_json_returns_none_version(self, tmp_path: Path) -> None:
        """Status returns None for local_version when no versions.json exists."""
        from portolan_cli.status import get_collection_status

        collection_path = tmp_path / "collection"
        collection_path.mkdir()

        status = get_collection_status(
            catalog_root=tmp_path,
            collection="collection",
            offline=True,
        )

        assert status.local_version is None

    @pytest.mark.unit
    def test_offline_mode_skips_remote_check(self, tmp_path: Path) -> None:
        """Offline mode does not attempt to fetch remote versions."""
        from portolan_cli.status import get_collection_status

        collection_path = tmp_path / "collection"
        collection_path.mkdir()

        # Should not raise even without remote configured
        status = get_collection_status(
            catalog_root=tmp_path,
            collection="collection",
            offline=True,
        )

        assert status.remote_version is None


class TestStatusOutput:
    """Tests for status output formatting."""

    @pytest.mark.unit
    def test_status_to_dict_for_json_output(self) -> None:
        """CollectionStatus can be serialized to dict for JSON output."""
        from portolan_cli.status import CollectionStatus

        status = CollectionStatus(
            collection="demographics",
            local_version="1.3.0",
            remote_version="1.3.1",
            modified_files=["census-data.parquet"],
            untracked_files=["notes.txt"],
            deleted_files=[],
        )

        data = status.to_dict()

        assert data["collection"] == "demographics"
        assert data["local_version"] == "1.3.0"
        assert data["remote_version"] == "1.3.1"
        assert data["modified_files"] == ["census-data.parquet"]
        assert data["untracked_files"] == ["notes.txt"]
        assert data["deleted_files"] == []

    @pytest.mark.unit
    def test_sync_state_behind(self) -> None:
        """Sync state is 'behind' when remote is ahead."""
        from portolan_cli.status import CollectionStatus

        status = CollectionStatus(
            collection="test",
            local_version="1.0.0",
            remote_version="1.1.0",
            modified_files=[],
            untracked_files=[],
            deleted_files=[],
        )

        assert status.sync_state == "behind"

    @pytest.mark.unit
    def test_sync_state_ahead(self) -> None:
        """Sync state is 'ahead' when local has unpushed versions."""
        from portolan_cli.status import CollectionStatus

        status = CollectionStatus(
            collection="test",
            local_version="1.2.0",
            remote_version="1.0.0",
            modified_files=[],
            untracked_files=[],
            deleted_files=[],
        )

        assert status.sync_state == "ahead"

    @pytest.mark.unit
    def test_sync_state_in_sync(self) -> None:
        """Sync state is 'in_sync' when versions match."""
        from portolan_cli.status import CollectionStatus

        status = CollectionStatus(
            collection="test",
            local_version="1.0.0",
            remote_version="1.0.0",
            modified_files=[],
            untracked_files=[],
            deleted_files=[],
        )

        assert status.sync_state == "in_sync"

    @pytest.mark.unit
    def test_sync_state_unknown_when_offline(self) -> None:
        """Sync state is 'unknown' when remote version is None."""
        from portolan_cli.status import CollectionStatus

        status = CollectionStatus(
            collection="test",
            local_version="1.0.0",
            remote_version=None,
            modified_files=[],
            untracked_files=[],
            deleted_files=[],
        )

        assert status.sync_state == "unknown"
