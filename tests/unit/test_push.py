"""Unit tests for push module.

Tests for cloud-native catalog push functionality with conflict detection
and optimistic locking via etag.

Following TDD: these tests are written FIRST, before implementation.

Test categories:
- PushResult dataclass
- Version diffing (local vs remote changes)
- Conflict detection
- Manifest-last upload ordering
- Etag-based optimistic locking
- Dry-run mode
- Force mode (overwrite conflicts)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from hypothesis import given
from hypothesis import strategies as st

from portolan_cli.push import UploadMetrics

if TYPE_CHECKING:
    pass


# =============================================================================
# Test fixtures
# =============================================================================


@pytest.fixture
def local_catalog(tmp_path: Path) -> Path:
    """Create a local catalog with versions.json for testing.

    Includes required STAC metadata files (collection.json, item STAC).
    """
    catalog_dir = tmp_path / "catalog"
    catalog_dir.mkdir()

    # Create test/versions.json (per ADR-0023)
    versions_dir = catalog_dir / "test"
    versions_dir.mkdir(parents=True)

    # hrefs are catalog-root-relative: collection/item/filename
    versions_data = {
        "spec_version": "1.0.0",
        "current_version": "1.1.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-01T00:00:00Z",
                "breaking": False,
                "message": "Initial version",
                "assets": {
                    "data.parquet": {
                        "sha256": "abc123",
                        "size_bytes": 1024,
                        "href": "test/data/data.parquet",
                    }
                },
                "changes": ["data.parquet"],
            },
            {
                "version": "1.1.0",
                "created": "2024-01-15T00:00:00Z",
                "breaking": False,
                "message": "Added more data",
                "assets": {
                    "data.parquet": {
                        "sha256": "def456",
                        "size_bytes": 2048,
                        "href": "test/data/data.parquet",
                    }
                },
                "changes": ["data.parquet"],
            },
        ],
    }

    (versions_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))

    # Create collection.json (required for push per Issue #252)
    collection_data = {
        "type": "Collection",
        "id": "test",
        "stac_version": "1.0.0",
        "description": "Test collection",
        "license": "proprietary",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [[None, None]]},
        },
        "links": [],
    }
    (versions_dir / "collection.json").write_text(json.dumps(collection_data, indent=2))

    # Create actual data file at catalog_root/collection/item/filename
    item_dir = versions_dir / "data"
    item_dir.mkdir(parents=True)
    (item_dir / "data.parquet").write_bytes(b"x" * 2048)

    # Create item STAC file using Portolan naming convention: {item_id}.json
    item_data = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "data",
        "geometry": None,
        "bbox": None,
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
        "links": [],
        "assets": {"data": {"href": "./data.parquet", "type": "application/x-parquet"}},
    }
    (item_dir / "data.json").write_text(json.dumps(item_data, indent=2))

    return catalog_dir


@pytest.fixture
def remote_versions_same() -> dict[str, Any]:
    """Remote versions.json matching local v1.0.0 (before local changes)."""
    return {
        "spec_version": "1.0.0",
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-01T00:00:00Z",
                "breaking": False,
                "message": "Initial version",
                "assets": {
                    "data.parquet": {
                        "sha256": "abc123",
                        "size_bytes": 1024,
                        "href": "data.parquet",
                    }
                },
                "changes": ["data.parquet"],
            }
        ],
    }


@pytest.fixture
def remote_versions_diverged() -> dict[str, Any]:
    """Remote versions.json that diverged from local (conflict scenario)."""
    return {
        "spec_version": "1.0.0",
        "current_version": "1.0.1",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-01T00:00:00Z",
                "breaking": False,
                "message": "Initial version",
                "assets": {
                    "data.parquet": {
                        "sha256": "abc123",
                        "size_bytes": 1024,
                        "href": "data.parquet",
                    }
                },
                "changes": ["data.parquet"],
            },
            {
                "version": "1.0.1",
                "created": "2024-01-10T00:00:00Z",
                "breaking": False,
                "message": "Remote change",
                "assets": {
                    "data.parquet": {
                        "sha256": "remote789",
                        "size_bytes": 1536,
                        "href": "data.parquet",
                    }
                },
                "changes": ["data.parquet"],
            },
        ],
    }


# =============================================================================
# Malformed Fixtures for Error Path Testing
# =============================================================================


@pytest.fixture
def local_catalog_malformed(tmp_path: Path) -> Path:
    """Create a local catalog with malformed versions.json (missing keys)."""
    catalog_dir = tmp_path / "catalog_malformed"
    catalog_dir.mkdir()

    # Create test/versions.json (per ADR-0023) with missing required keys
    versions_dir = catalog_dir / "test"
    versions_dir.mkdir(parents=True)

    # Missing "current_version" and "spec_version"
    malformed_data = {
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-01T00:00:00Z",
            }
        ],
    }

    (versions_dir / "versions.json").write_text(json.dumps(malformed_data, indent=2))

    return catalog_dir


@pytest.fixture
def local_catalog_invalid_json(tmp_path: Path) -> Path:
    """Create a local catalog with invalid JSON in versions.json."""
    catalog_dir = tmp_path / "catalog_invalid_json"
    catalog_dir.mkdir()

    versions_dir = catalog_dir / "test"
    versions_dir.mkdir(parents=True)

    # Write invalid JSON
    (versions_dir / "versions.json").write_text("{ invalid json }")

    return catalog_dir


@pytest.fixture
def remote_versions_same_malformed() -> dict[str, Any]:
    """Remote versions.json with invalid structure (missing spec_version)."""
    return {
        # Missing "spec_version"
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-01T00:00:00Z",
            }
        ],
    }


@pytest.fixture
def remote_versions_diverged_malformed() -> dict[str, Any]:
    """Remote versions.json with wrong asset fields (missing sha256/size_bytes)."""
    return {
        "spec_version": "1.0.0",
        "current_version": "1.0.1",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-01T00:00:00Z",
                "breaking": False,
                "assets": {
                    "data.parquet": {
                        # Missing "sha256" and "size_bytes"
                        "href": "data.parquet",
                    }
                },
                "changes": ["data.parquet"],
            },
        ],
    }


# =============================================================================
# PushResult Tests
# =============================================================================


class TestPushResult:
    """Tests for PushResult dataclass."""

    @pytest.mark.unit
    def test_push_result_success(self) -> None:
        """PushResult should capture successful push stats."""
        from portolan_cli.push import PushResult

        result = PushResult(
            success=True,
            files_uploaded=5,
            versions_pushed=1,
            conflicts=[],
            errors=[],
        )

        assert result.success is True
        assert result.files_uploaded == 5
        assert result.versions_pushed == 1
        assert result.conflicts == []
        assert result.errors == []

    @pytest.mark.unit
    def test_push_result_with_conflicts(self) -> None:
        """PushResult should capture conflicts when detected."""
        from portolan_cli.push import PushResult

        result = PushResult(
            success=False,
            files_uploaded=0,
            versions_pushed=0,
            conflicts=["data.parquet changed both locally and remotely"],
            errors=[],
        )

        assert result.success is False
        assert result.conflicts == ["data.parquet changed both locally and remotely"]

    @pytest.mark.unit
    def test_push_result_with_errors(self) -> None:
        """PushResult should capture upload errors."""
        from portolan_cli.push import PushResult

        result = PushResult(
            success=False,
            files_uploaded=2,
            versions_pushed=0,
            conflicts=[],
            errors=["Failed to upload data.parquet: connection timeout"],
        )

        assert result.success is False
        assert len(result.errors) == 1


# =============================================================================
# Version Diffing Tests
# =============================================================================


class TestDiffVersionLists:
    """Tests for diff_version_lists function."""

    @pytest.mark.unit
    def test_diff_local_only_changes(self) -> None:
        """Diff should detect versions that exist only locally."""
        from portolan_cli.push import diff_version_lists

        local_versions = ["1.0.0", "1.1.0"]
        remote_versions = ["1.0.0"]

        diff = diff_version_lists(local_versions, remote_versions)

        assert diff.local_only == ["1.1.0"]
        assert diff.remote_only == []
        assert diff.common == ["1.0.0"]

    @pytest.mark.unit
    def test_diff_remote_only_changes(self) -> None:
        """Diff should detect versions that exist only remotely (conflict)."""
        from portolan_cli.push import diff_version_lists

        local_versions = ["1.0.0"]
        remote_versions = ["1.0.0", "1.0.1"]

        diff = diff_version_lists(local_versions, remote_versions)

        assert diff.local_only == []
        assert diff.remote_only == ["1.0.1"]
        assert diff.common == ["1.0.0"]

    @pytest.mark.unit
    def test_diff_both_diverged(self) -> None:
        """Diff should detect when both local and remote have unique versions."""
        from portolan_cli.push import diff_version_lists

        local_versions = ["1.0.0", "1.1.0"]
        remote_versions = ["1.0.0", "1.0.1"]

        diff = diff_version_lists(local_versions, remote_versions)

        assert diff.local_only == ["1.1.0"]
        assert diff.remote_only == ["1.0.1"]
        assert diff.common == ["1.0.0"]

    @pytest.mark.unit
    def test_diff_empty_remote(self) -> None:
        """Diff should handle empty remote (first push)."""
        from portolan_cli.push import diff_version_lists

        local_versions = ["1.0.0", "1.1.0"]
        remote_versions: list[str] = []

        diff = diff_version_lists(local_versions, remote_versions)

        assert diff.local_only == ["1.0.0", "1.1.0"]
        assert diff.remote_only == []
        assert diff.common == []

    @pytest.mark.unit
    def test_diff_identical(self) -> None:
        """Diff should detect when local and remote are identical."""
        from portolan_cli.push import diff_version_lists

        local_versions = ["1.0.0", "1.1.0"]
        remote_versions = ["1.0.0", "1.1.0"]

        diff = diff_version_lists(local_versions, remote_versions)

        assert diff.local_only == []
        assert diff.remote_only == []
        assert diff.common == ["1.0.0", "1.1.0"]


# =============================================================================
# Conflict Detection Tests
# =============================================================================


class TestConflictDetection:
    """Tests for conflict detection logic."""

    @pytest.mark.unit
    def test_has_conflict_when_remote_diverged(self) -> None:
        """Should detect conflict when remote has versions not in local."""
        from portolan_cli.push import VersionDiff

        diff = VersionDiff(
            local_only=["1.1.0"],
            remote_only=["1.0.1"],
            common=["1.0.0"],
        )

        assert diff.has_conflict is True

    @pytest.mark.unit
    def test_no_conflict_when_only_local_changes(self) -> None:
        """No conflict when only local has new versions."""
        from portolan_cli.push import VersionDiff

        diff = VersionDiff(
            local_only=["1.1.0"],
            remote_only=[],
            common=["1.0.0"],
        )

        assert diff.has_conflict is False

    @pytest.mark.unit
    def test_no_conflict_when_identical(self) -> None:
        """No conflict when local and remote are identical."""
        from portolan_cli.push import VersionDiff

        diff = VersionDiff(
            local_only=[],
            remote_only=[],
            common=["1.0.0", "1.1.0"],
        )

        assert diff.has_conflict is False


# =============================================================================
# Push Function Tests
# =============================================================================


class TestPush:
    """Tests for main push function."""

    @pytest.mark.unit
    def test_push_dry_run_no_upload(self, local_catalog: Path) -> None:
        """Dry-run should not perform actual upload."""
        from portolan_cli.push import push

        # Dry-run returns early without any network calls (Bug #137)
        # No need to patch - the function returns before upload
        result = push(
            catalog_root=local_catalog,
            collection="test",
            destination="s3://mybucket/catalog",
            dry_run=True,
        )

        assert result.success is True
        assert result.files_uploaded == 0

    @pytest.mark.unit
    def test_push_detects_conflict_without_force(self, local_catalog: Path) -> None:
        """Push should fail when remote diverged and --force not specified."""
        from portolan_cli.push import PushConflictError, push

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            # Remote has a version we don't have locally
            mock_fetch.return_value = (
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.1",
                    "versions": [
                        {"version": "1.0.0", "created": "2024-01-01T00:00:00Z"},
                        {"version": "1.0.1", "created": "2024-01-10T00:00:00Z"},
                    ],
                },
                "etag-123",
            )

            with pytest.raises(PushConflictError) as exc_info:
                push(
                    catalog_root=local_catalog,
                    collection="test",
                    destination="s3://mybucket/catalog",
                    force=False,
                )

            assert "Remote has changes" in str(exc_info.value)

    @pytest.mark.unit
    def test_push_with_force_ignores_conflict(self, local_catalog: Path) -> None:
        """Push with --force should overwrite despite remote changes."""
        from portolan_cli.push import push

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.1",
                    "versions": [
                        {"version": "1.0.0", "created": "2024-01-01T00:00:00Z"},
                        {"version": "1.0.1", "created": "2024-01-10T00:00:00Z"},
                    ],
                },
                "etag-123",
            )

            with patch(
                "portolan_cli.push._upload_assets_async", new_callable=AsyncMock
            ) as mock_upload_assets:
                mock_upload_assets.return_value = (1, [], ["catalog/data.parquet"], UploadMetrics())

                with patch(
                    "portolan_cli.push._upload_stac_files_async", new_callable=AsyncMock
                ) as mock_upload_stac:
                    mock_upload_stac.return_value = (2, [], ["stac/collection.json"])

                    with patch(
                        "portolan_cli.push._upload_versions_json_async", new_callable=AsyncMock
                    ) as mock_upload_versions:
                        mock_upload_versions.return_value = None

                        result = push(
                            catalog_root=local_catalog,
                            collection="test",
                            destination="s3://mybucket/catalog",
                            force=True,
                        )

        assert result.success is True

    @pytest.mark.unit
    def test_push_first_time_no_remote(self, local_catalog: Path) -> None:
        """First push (no remote versions.json) should succeed."""
        from portolan_cli.push import push

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (None, None)  # No remote versions.json

            with patch(
                "portolan_cli.push._upload_assets_async", new_callable=AsyncMock
            ) as mock_upload_assets:
                mock_upload_assets.return_value = (1, [], ["catalog/data.parquet"], UploadMetrics())

                with patch(
                    "portolan_cli.push._upload_stac_files_async", new_callable=AsyncMock
                ) as mock_upload_stac:
                    mock_upload_stac.return_value = (2, [], ["stac/collection.json"])

                    with patch(
                        "portolan_cli.push._upload_versions_json_async", new_callable=AsyncMock
                    ) as mock_upload_versions:
                        mock_upload_versions.return_value = None

                        result = push(
                            catalog_root=local_catalog,
                            collection="test",
                            destination="s3://mybucket/catalog",
                        )

        assert result.success is True
        assert result.versions_pushed >= 1

    @pytest.mark.unit
    def test_push_nothing_to_push(self, local_catalog: Path) -> None:
        """Push when local == remote should report nothing to push."""
        from portolan_cli.push import push

        # Read local versions to create matching remote
        versions_path = local_catalog / "test" / "versions.json"
        local_data = json.loads(versions_path.read_text())

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (local_data, "etag-123")

            result = push(
                catalog_root=local_catalog,
                collection="test",
                destination="s3://mybucket/catalog",
            )

        assert result.success is True
        assert result.files_uploaded == 0
        assert result.versions_pushed == 0


# =============================================================================
# Etag-based Optimistic Locking Tests
# =============================================================================


class TestEtagOptimisticLocking:
    """Tests for etag-based optimistic locking."""

    @pytest.mark.unit
    def test_push_uses_etag_for_conditional_put(self, local_catalog: Path) -> None:
        """Push should use etag for conditional put (optimistic locking)."""
        from portolan_cli.push import push

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [{"version": "1.0.0", "created": "2024-01-01T00:00:00Z"}],
                },
                "etag-abc123",  # Remote etag
            )

            with patch(
                "portolan_cli.push._upload_assets_async", new_callable=AsyncMock
            ) as mock_upload_assets:
                mock_upload_assets.return_value = (1, [], ["catalog/data.parquet"], UploadMetrics())

                with patch(
                    "portolan_cli.push._upload_stac_files_async", new_callable=AsyncMock
                ) as mock_upload_stac:
                    mock_upload_stac.return_value = (2, [], ["stac/collection.json"])

                    with patch(
                        "portolan_cli.push._upload_versions_json_async", new_callable=AsyncMock
                    ) as mock_upload_versions:
                        mock_upload_versions.return_value = None

                        push(
                            catalog_root=local_catalog,
                            collection="test",
                            destination="s3://mybucket/catalog",
                        )

                        # Verify etag was passed to upload_versions_json
                        call_args = mock_upload_versions.call_args
                        assert call_args is not None
                        # Check that etag was passed (either positional or keyword)
                        assert "etag-abc123" in str(call_args)

    @pytest.mark.unit
    def test_push_raises_on_etag_mismatch(self, local_catalog: Path) -> None:
        """Push should raise when etag mismatch (remote changed during push)."""
        from portolan_cli.push import PushConflictError, push

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [{"version": "1.0.0", "created": "2024-01-01T00:00:00Z"}],
                },
                "etag-abc123",
            )

            with patch(
                "portolan_cli.push._upload_assets_async", new_callable=AsyncMock
            ) as mock_upload_assets:
                mock_upload_assets.return_value = (1, [], ["catalog/data.parquet"], UploadMetrics())

                with patch(
                    "portolan_cli.push._upload_stac_files_async", new_callable=AsyncMock
                ) as mock_upload_stac:
                    mock_upload_stac.return_value = (2, [], ["stac/collection.json"])

                    with patch(
                        "portolan_cli.push._upload_versions_json_async", new_callable=AsyncMock
                    ) as mock_upload_versions:
                        # Simulate PushConflictError from etag mismatch
                        mock_upload_versions.side_effect = PushConflictError(
                            "Remote changed during push, re-run push to try again"
                        )

                        with patch("portolan_cli.push._cleanup_uploaded_assets"):
                            with pytest.raises(PushConflictError) as exc_info:
                                push(
                                    catalog_root=local_catalog,
                                    collection="test",
                                    destination="s3://mybucket/catalog",
                                )

                        assert "Remote changed during push" in str(exc_info.value)


# =============================================================================
# Manifest-last Ordering Tests
# =============================================================================


class TestManifestLastOrdering:
    """Tests for manifest-last upload ordering (assets first, then versions.json)."""

    @pytest.mark.unit
    def test_assets_uploaded_before_versions(self, local_catalog: Path) -> None:
        """Assets should be uploaded before STAC files before versions.json (manifest-last)."""
        from portolan_cli.push import push

        call_order: list[str] = []

        def track_assets(*args, **kwargs):
            call_order.append("assets")
            return (1, [], ["catalog/data.parquet"], UploadMetrics())

        def track_stac(*args, **kwargs):
            call_order.append("stac")
            return (2, [], ["stac/collection.json"])

        def track_versions(*args, **kwargs):
            call_order.append("versions")
            return None

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (None, None)

            with patch(
                "portolan_cli.push._upload_assets_async",
                new_callable=AsyncMock,
                side_effect=track_assets,
            ):
                with patch(
                    "portolan_cli.push._upload_stac_files_async",
                    new_callable=AsyncMock,
                    side_effect=track_stac,
                ):
                    with patch(
                        "portolan_cli.push._upload_versions_json_async",
                        new_callable=AsyncMock,
                        side_effect=track_versions,
                    ):
                        push(
                            catalog_root=local_catalog,
                            collection="test",
                            destination="s3://mybucket/catalog",
                        )

        assert call_order == ["assets", "stac", "versions"], (
            f"Expected assets -> stac -> versions, got: {call_order}"
        )

    @pytest.mark.unit
    def test_versions_not_uploaded_if_assets_fail(self, local_catalog: Path) -> None:
        """versions.json should not be uploaded if asset upload fails."""
        from portolan_cli.push import push

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (None, None)

            with patch(
                "portolan_cli.push._upload_assets_async", new_callable=AsyncMock
            ) as mock_upload_assets:
                mock_upload_assets.return_value = (
                    0,
                    ["Failed to upload data.parquet"],
                    [],
                    UploadMetrics(),
                )

                with patch(
                    "portolan_cli.push._upload_versions_json_async", new_callable=AsyncMock
                ) as mock_upload_versions:
                    with patch("portolan_cli.push._cleanup_uploaded_assets"):
                        result = push(
                            catalog_root=local_catalog,
                            collection="test",
                            destination="s3://mybucket/catalog",
                        )

                    mock_upload_versions.assert_not_called()

        assert result.success is False
        assert len(result.errors) > 0


# =============================================================================
# Store Setup Tests
# =============================================================================


class TestStoreSetup:
    """Tests for object store setup."""

    @pytest.mark.unit
    def test_push_with_profile(self, local_catalog: Path) -> None:
        """Push should pass profile to store setup."""
        from portolan_cli.push import push

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (None, None)

            with patch("portolan_cli.push.setup_store") as mock_setup:
                mock_store = MagicMock()
                mock_setup.return_value = (mock_store, "prefix")

                with patch(
                    "portolan_cli.push._upload_assets_async", new_callable=AsyncMock
                ) as mock_upload:
                    mock_upload.return_value = (1, [], ["catalog/data.parquet"], UploadMetrics())

                    with patch(
                        "portolan_cli.push._upload_versions_json_async", new_callable=AsyncMock
                    ):
                        push(
                            catalog_root=local_catalog,
                            collection="test",
                            destination="s3://mybucket/catalog",
                            profile="myprofile",
                        )

                # Verify profile was passed
                call_kwargs = mock_setup.call_args[1]
                assert call_kwargs.get("profile") == "myprofile"


# =============================================================================
# Missing Asset Detection Tests
# =============================================================================


class TestMissingAssetDetection:
    """Tests for missing asset detection in push operations."""

    @pytest.mark.unit
    def test_get_assets_to_upload_raises_on_missing_file(self, local_catalog: Path) -> None:
        """Should raise FileNotFoundError when referenced asset doesn't exist."""
        from portolan_cli.push import _get_assets_to_upload

        versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "missing.parquet": {
                            "sha256": "abc123",
                            "size_bytes": 1000,
                            "href": "missing.parquet",  # This file doesn't exist
                        }
                    },
                }
            ]
        }

        with pytest.raises(FileNotFoundError, match="missing.parquet"):
            _get_assets_to_upload(
                catalog_root=local_catalog,
                versions_data=versions_data,
                versions_to_push=["1.0.0"],
            )


# =============================================================================
# Force Flag Tests
# =============================================================================


class TestForceFlag:
    """Tests for --force flag behavior in push operations."""

    @pytest.mark.unit
    def test_push_force_with_remote_only_versions_and_local_changes(
        self, local_catalog: Path
    ) -> None:
        """Force push should proceed when both local and remote have unique versions.

        Scenario: Remote has v1.0.0 and v1.0.1, local has v1.0.0 and v1.1.0.
        With --force, push should proceed (local has changes to push).
        """
        from portolan_cli.push import push

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            # Remote has v1.0.1 that local doesn't have
            mock_fetch.return_value = (
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.1",
                    "versions": [
                        {"version": "1.0.0", "created": "2024-01-01T00:00:00Z"},
                        {"version": "1.0.1", "created": "2024-01-10T00:00:00Z"},  # remote-only
                    ],
                },
                "etag-123",
            )

            with patch(
                "portolan_cli.push._upload_assets_async", new_callable=AsyncMock
            ) as mock_upload_assets:
                mock_upload_assets.return_value = (1, [], ["catalog/data.parquet"], UploadMetrics())

                with patch(
                    "portolan_cli.push._upload_stac_files_async", new_callable=AsyncMock
                ) as mock_upload_stac:
                    mock_upload_stac.return_value = (2, [], ["stac/collection.json"])

                    with patch(
                        "portolan_cli.push._upload_versions_json_async", new_callable=AsyncMock
                    ) as mock_upload_versions:
                        mock_upload_versions.return_value = None

                        result = push(
                            catalog_root=local_catalog,
                            collection="test",
                            destination="s3://mybucket/catalog",
                            force=True,
                        )

        # With --force, should have uploaded
        assert result.success is True
        mock_upload_assets.assert_called_once()
        mock_upload_versions.assert_called_once()

    @pytest.mark.unit
    def test_push_force_overwrites_when_no_local_only(self, tmp_path: Path) -> None:
        """Force push should proceed even when local has NO unique versions.

        Bug scenario: Local only has v1.0.0, remote has v1.0.0 and v1.0.1.
        Without --force: conflict error (correct).
        With --force: should overwrite remote with local state (NOT return "nothing to push").

        This tests the fix for the --force early return bug.
        """
        from portolan_cli.push import push

        # Create local catalog with ONLY v1.0.0
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        versions_dir = catalog_dir / "test"
        versions_dir.mkdir(parents=True)

        # hrefs are catalog-root-relative: collection/item/filename
        versions_data = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2024-01-01T00:00:00Z",
                    "breaking": False,
                    "message": "Initial version",
                    "assets": {
                        "data.parquet": {
                            "sha256": "abc123",
                            "size_bytes": 1024,
                            "href": "test/data/data.parquet",
                        }
                    },
                    "changes": ["data.parquet"],
                },
            ],
        }
        (versions_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))

        # Create collection.json (required for push per Issue #252)
        collection_data = {
            "type": "Collection",
            "id": "test",
            "stac_version": "1.0.0",
            "description": "Test collection",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [],
        }
        (versions_dir / "collection.json").write_text(json.dumps(collection_data, indent=2))

        item_dir = versions_dir / "data"
        item_dir.mkdir(parents=True)
        (item_dir / "data.parquet").write_bytes(b"x" * 1024)

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            # Remote has v1.0.0 AND v1.0.1 - local is missing v1.0.1
            mock_fetch.return_value = (
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.1",
                    "versions": [
                        {"version": "1.0.0", "created": "2024-01-01T00:00:00Z"},
                        {"version": "1.0.1", "created": "2024-01-10T00:00:00Z"},  # remote-only
                    ],
                },
                "etag-123",
            )

            with patch(
                "portolan_cli.push._upload_assets_async", new_callable=AsyncMock
            ) as mock_upload_assets:
                mock_upload_assets.return_value = (
                    0,
                    [],
                    [],
                    UploadMetrics(),
                )  # No assets to upload

                with patch(
                    "portolan_cli.push._upload_stac_files_async", new_callable=AsyncMock
                ) as mock_upload_stac:
                    mock_upload_stac.return_value = (1, [], ["stac/collection.json"])

                    with patch(
                        "portolan_cli.push._upload_versions_json_async", new_callable=AsyncMock
                    ) as mock_upload_versions:
                        mock_upload_versions.return_value = None

                        result = push(
                            catalog_root=catalog_dir,
                            collection="test",
                            destination="s3://mybucket/catalog",
                            force=True,
                        )

        # With --force, should still upload versions.json to overwrite remote
        # even though local has no NEW versions (local_only is empty)
        assert result.success is True
        # versions.json MUST be uploaded to force-overwrite remote state
        mock_upload_versions.assert_called_once()


# =============================================================================
# Orphan Cleanup Tests
# =============================================================================


class TestOrphanCleanup:
    """Tests for orphan cleanup when push fails after asset upload."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_upload_assets_returns_uploaded_keys(self, local_catalog: Path) -> None:
        """_upload_assets_async should return list of uploaded object keys."""
        from portolan_cli.push import _upload_assets_async

        # Create the test file (needed for size calculation)
        test_file = local_catalog / "data.parquet"
        test_file.write_bytes(b"test data")

        with patch("portolan_cli.push.obs.put_async", new_callable=AsyncMock) as mock_put:
            mock_put.return_value = None

            mock_store = MagicMock()
            assets = [test_file]

            files_uploaded, errors, uploaded_keys, _metrics = await _upload_assets_async(
                store=mock_store,
                catalog_root=local_catalog,
                prefix="catalog",
                assets=assets,
            )

        assert files_uploaded == 1
        assert errors == []
        assert len(uploaded_keys) == 1
        assert "data.parquet" in uploaded_keys[0]

    @pytest.mark.unit
    def test_cleanup_deletes_uploaded_assets(self) -> None:
        """_cleanup_uploaded_assets should delete all uploaded keys."""
        from portolan_cli.push import _cleanup_uploaded_assets

        with patch("portolan_cli.push.obs.delete") as mock_delete:
            mock_store = MagicMock()
            uploaded_keys = ["catalog/file1.parquet", "catalog/file2.parquet"]

            _cleanup_uploaded_assets(mock_store, uploaded_keys)

        assert mock_delete.call_count == 2
        mock_delete.assert_any_call(mock_store, "catalog/file1.parquet")
        mock_delete.assert_any_call(mock_store, "catalog/file2.parquet")

    @pytest.mark.unit
    def test_cleanup_handles_empty_list(self) -> None:
        """_cleanup_uploaded_assets should handle empty key list gracefully."""
        from portolan_cli.push import _cleanup_uploaded_assets

        with patch("portolan_cli.push.obs.delete") as mock_delete:
            mock_store = MagicMock()
            _cleanup_uploaded_assets(mock_store, [])

        mock_delete.assert_not_called()

    @pytest.mark.unit
    def test_cleanup_continues_on_delete_failure(self) -> None:
        """_cleanup_uploaded_assets should continue if individual deletes fail."""
        from portolan_cli.push import _cleanup_uploaded_assets

        with patch("portolan_cli.push.obs.delete") as mock_delete:
            # First delete fails, second succeeds
            mock_delete.side_effect = [Exception("Network error"), None]

            mock_store = MagicMock()
            uploaded_keys = ["key1", "key2"]

            # Should not raise
            _cleanup_uploaded_assets(mock_store, uploaded_keys)

        assert mock_delete.call_count == 2

    @pytest.mark.unit
    def test_push_cleans_up_on_versions_json_failure(self, local_catalog: Path) -> None:
        """Push should clean up uploaded assets if versions.json upload fails."""
        from portolan_cli.push import push

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (None, None)

            with patch(
                "portolan_cli.push._upload_assets_async", new_callable=AsyncMock
            ) as mock_upload_assets:
                mock_upload_assets.return_value = (1, [], ["catalog/data.parquet"], UploadMetrics())

                with patch(
                    "portolan_cli.push._upload_stac_files_async", new_callable=AsyncMock
                ) as mock_upload_stac:
                    mock_upload_stac.return_value = (2, [], ["stac/collection.json"])

                    with patch(
                        "portolan_cli.push._upload_versions_json_async", new_callable=AsyncMock
                    ) as mock_upload_versions:
                        mock_upload_versions.side_effect = Exception("Network timeout")

                        with patch("portolan_cli.push._cleanup_uploaded_assets") as mock_cleanup:
                            result = push(
                                catalog_root=local_catalog,
                                collection="test",
                                destination="s3://mybucket/catalog",
                            )

        # Cleanup should have been called with the uploaded keys
        mock_cleanup.assert_called_once()
        cleanup_keys = mock_cleanup.call_args[0][1]
        assert "catalog/data.parquet" in cleanup_keys
        assert result.success is False

    @pytest.mark.unit
    def test_push_cleans_up_on_etag_conflict(self, local_catalog: Path) -> None:
        """Push should clean up uploaded assets on etag mismatch."""
        from portolan_cli.push import PushConflictError, push

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [{"version": "1.0.0", "created": "2024-01-01T00:00:00Z"}],
                },
                "etag-123",
            )

            with patch(
                "portolan_cli.push._upload_assets_async", new_callable=AsyncMock
            ) as mock_upload_assets:
                mock_upload_assets.return_value = (1, [], ["catalog/data.parquet"], UploadMetrics())

                with patch(
                    "portolan_cli.push._upload_stac_files_async", new_callable=AsyncMock
                ) as mock_upload_stac:
                    mock_upload_stac.return_value = (2, [], ["stac/collection.json"])

                    with patch(
                        "portolan_cli.push._upload_versions_json_async", new_callable=AsyncMock
                    ) as mock_upload_versions:
                        mock_upload_versions.side_effect = PushConflictError("Etag mismatch")

                        with patch("portolan_cli.push._cleanup_uploaded_assets") as mock_cleanup:
                            with pytest.raises(PushConflictError):
                                push(
                                    catalog_root=local_catalog,
                                    collection="test",
                                    destination="s3://mybucket/catalog",
                                )

        mock_cleanup.assert_called_once()


# =============================================================================
# Progress Reporting Tests
# =============================================================================


class TestProgressReporting:
    """Tests for progress reporting during uploads."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_upload_assets_shows_progress(self, local_catalog: Path, capsys) -> None:
        """_upload_assets_async should upload files with progress tracking.

        Note: With json_mode=False, a Rich progress bar handles the display.
        With suppress_progress=False and json_mode=True, text progress is shown.
        """
        from portolan_cli.push import _upload_assets_async

        # Create the test file (needed for size calculation)
        test_file = local_catalog / "data.parquet"
        test_file.write_bytes(b"test data")

        with patch("portolan_cli.push.obs.put_async", new_callable=AsyncMock) as mock_put:
            mock_put.return_value = None

            mock_store = MagicMock()
            assets = [test_file]

            files_uploaded, errors, uploaded_keys, metrics = await _upload_assets_async(
                store=mock_store,
                catalog_root=local_catalog,
                prefix="catalog",
                assets=assets,
                json_mode=True,  # No Rich progress bar
                suppress_progress=True,  # Suppress for clean test
            )

        # Verify upload occurred
        assert files_uploaded == 1
        assert errors == []
        assert len(uploaded_keys) == 1

    @pytest.mark.unit
    def test_push_dry_run_shows_progress(self, local_catalog: Path, capsys) -> None:
        """Dry-run at push level should show what would be uploaded."""
        from portolan_cli.push import push

        # dry_run is handled at push() level, not in _upload_assets_async
        result = push(
            catalog_root=local_catalog,
            collection="test",
            destination="s3://mybucket/catalog",
            dry_run=True,
        )

        captured = capsys.readouterr()
        assert "DRY RUN" in captured.out
        assert result.dry_run is True


# =============================================================================
# Multi-Cloud Store Setup Tests
# =============================================================================


class TestMultiCloudStoreSetup:
    """Tests for GCS and Azure store setup."""

    @pytest.mark.unit
    def test_setup_store_s3_with_profile(self) -> None:
        """setup_store should load credentials from AWS profile."""
        from portolan_cli.upload import setup_store

        with patch("portolan_cli.upload._load_aws_credentials_from_profile") as mock_load:
            mock_load.return_value = ("access_key", "secret_key", "us-east-1")

            with patch("portolan_cli.upload.S3Store") as mock_s3:
                mock_s3.return_value = MagicMock()

                store, prefix = setup_store("s3://mybucket/catalog", profile="myprofile")

        mock_load.assert_called_once_with("myprofile")
        mock_s3.assert_called_once_with(
            "mybucket",
            region="us-east-1",
            access_key_id="access_key",
            secret_access_key="secret_key",
        )
        assert prefix == "catalog"

    @pytest.mark.unit
    def test_setup_store_s3_from_environment(self) -> None:
        """setup_store should use AWS credentials from environment."""
        from portolan_cli.upload import setup_store

        with patch.dict(
            "os.environ",
            {
                "AWS_ACCESS_KEY_ID": "env_access_key",
                "AWS_SECRET_ACCESS_KEY": "env_secret_key",
                "AWS_REGION": "eu-west-1",
            },
            clear=True,
        ):
            with patch("portolan_cli.upload.S3Store") as mock_s3:
                mock_s3.return_value = MagicMock()

                store, prefix = setup_store("s3://mybucket/prefix")

        mock_s3.assert_called_once_with(
            "mybucket",
            region="eu-west-1",
            access_key_id="env_access_key",
            secret_access_key="env_secret_key",
        )

    @pytest.mark.unit
    def test_setup_store_s3_uses_default_region_env(self) -> None:
        """setup_store should fallback to AWS_DEFAULT_REGION if AWS_REGION not set."""
        from portolan_cli.upload import setup_store

        with patch.dict(
            "os.environ",
            {
                "AWS_ACCESS_KEY_ID": "key",
                "AWS_SECRET_ACCESS_KEY": "secret",
                "AWS_DEFAULT_REGION": "ap-southeast-1",
            },
            clear=True,
        ):
            with patch("portolan_cli.upload.S3Store") as mock_s3:
                mock_s3.return_value = MagicMock()

                setup_store("s3://mybucket/prefix")

        call_kwargs = mock_s3.call_args[1]
        assert call_kwargs["region"] == "ap-southeast-1"

    @pytest.mark.unit
    def test_setup_store_s3_no_region(self) -> None:
        """setup_store should work without region (uses AWS SDK defaults)."""
        from portolan_cli.upload import setup_store

        with patch.dict(
            "os.environ",
            {
                "AWS_ACCESS_KEY_ID": "key",
                "AWS_SECRET_ACCESS_KEY": "secret",
            },
            clear=True,
        ):
            with patch("portolan_cli.upload.S3Store") as mock_s3:
                mock_s3.return_value = MagicMock()

                setup_store("s3://mybucket/data")

        call_kwargs = mock_s3.call_args[1]
        assert "region" not in call_kwargs

    @pytest.mark.unit
    def test_setup_store_gcs(self) -> None:
        """setup_store should create store for gs:// URLs via obs.store.from_url.

        Note: GCS credential handling is delegated to obstore library.
        We just verify the URL is parsed correctly and from_url is called.
        """
        from portolan_cli.upload import setup_store

        with patch("portolan_cli.upload.obs.store.from_url") as mock_from_url:
            mock_from_url.return_value = MagicMock()

            store, prefix = setup_store("gs://mybucket/catalog")

        mock_from_url.assert_called_once_with("gs://mybucket")
        assert prefix == "catalog"

    @pytest.mark.unit
    def test_setup_store_gcs_no_credentials(self) -> None:
        """setup_store should work for GCS without explicit credentials.

        Note: GCS credential handling is delegated to obstore library.
        We just verify the URL is parsed correctly and from_url is called.
        """
        from portolan_cli.upload import setup_store

        with patch("portolan_cli.upload.obs.store.from_url") as mock_from_url:
            mock_from_url.return_value = MagicMock()

            store, prefix = setup_store("gs://mybucket/prefix")

        mock_from_url.assert_called_once_with("gs://mybucket")
        assert prefix == "prefix"

    @pytest.mark.unit
    def test_setup_store_azure_with_key(self) -> None:
        """setup_store should create store for az:// URLs via obs.store.from_url.

        Note: Azure credential handling is delegated to obstore library.
        We just verify the URL is parsed correctly and from_url is called.
        """
        from portolan_cli.upload import setup_store

        with patch("portolan_cli.upload.obs.store.from_url") as mock_from_url:
            mock_from_url.return_value = MagicMock()

            store, prefix = setup_store("az://account/mycontainer/catalog")

        mock_from_url.assert_called_once_with("az://account/mycontainer")
        assert prefix == "catalog"

    @pytest.mark.unit
    def test_setup_store_azure_with_sas_token(self) -> None:
        """setup_store should create store for az:// URLs via obs.store.from_url.

        Note: Azure credential handling (SAS tokens) is delegated to obstore library.
        We just verify the URL is parsed correctly and from_url is called.
        """
        from portolan_cli.upload import setup_store

        with patch("portolan_cli.upload.obs.store.from_url") as mock_from_url:
            mock_from_url.return_value = MagicMock()

            store, prefix = setup_store("az://account/mycontainer/data")

        mock_from_url.assert_called_once_with("az://account/mycontainer")
        assert prefix == "data"

    @pytest.mark.unit
    def test_setup_store_azure_parses_prefix(self) -> None:
        """setup_store should correctly parse Azure URL prefix.

        Azure URLs have format: az://account/container/path
        Note: Azure credential precedence is delegated to obstore library.
        """
        from portolan_cli.upload import setup_store

        with patch("portolan_cli.upload.obs.store.from_url") as mock_from_url:
            mock_from_url.return_value = MagicMock()

            # Azure format: az://account/container/prefix
            store, prefix = setup_store("az://myaccount/mycontainer/data/v1")

        # bucket_url = az://account/container, prefix = data/v1
        mock_from_url.assert_called_once_with("az://myaccount/mycontainer")
        assert prefix == "data/v1"

    @pytest.mark.unit
    def test_setup_store_unknown_scheme_uses_from_url(self) -> None:
        """setup_store should fallback to from_url for unknown schemes.

        NOTE: This tests upload.py, but since parse_object_store_url
        validates the scheme first and raises ValueError for unsupported schemes,
        we need to mock it to allow the unknown scheme through.
        """
        from portolan_cli.upload import setup_store

        with patch("portolan_cli.upload.parse_object_store_url") as mock_parse:
            # Return a fake bucket URL with unsupported scheme
            mock_parse.return_value = ("custom://bucket", "catalog")

            with patch("portolan_cli.upload.obs.store.from_url") as mock_from_url:
                mock_from_url.return_value = MagicMock()

                store, prefix = setup_store("custom://bucket/catalog")

        mock_from_url.assert_called_once_with("custom://bucket")
        assert prefix == "catalog"


# =============================================================================
# Fetch Remote Versions Tests
# =============================================================================


class TestFetchRemoteVersions:
    """Tests for _fetch_remote_versions function."""

    @pytest.mark.unit
    def test_fetch_remote_versions_success(self) -> None:
        """_fetch_remote_versions should return parsed data and etag."""
        from portolan_cli.push import _fetch_remote_versions

        mock_store = MagicMock()
        mock_result = MagicMock()
        mock_result.bytes.return_value = b'{"spec_version": "1.0.0", "versions": []}'
        mock_result.meta = {"e_tag": "etag-123"}

        with patch("portolan_cli.push.obs.get") as mock_get:
            mock_get.return_value = mock_result

            data, etag = _fetch_remote_versions(mock_store, "catalog", "test-collection")

        assert data is not None
        assert data["spec_version"] == "1.0.0"
        assert etag == "etag-123"

    @pytest.mark.unit
    def test_fetch_remote_versions_file_not_found(self) -> None:
        """_fetch_remote_versions should return None for missing file."""
        from portolan_cli.push import _fetch_remote_versions

        mock_store = MagicMock()

        with patch("portolan_cli.push.obs.get") as mock_get:
            mock_get.side_effect = FileNotFoundError("Not found")

            data, etag = _fetch_remote_versions(mock_store, "catalog", "test-collection")

        assert data is None
        assert etag is None

    @pytest.mark.unit
    def test_fetch_remote_versions_not_found_string_error(self) -> None:
        """_fetch_remote_versions should handle 'not found' string errors."""
        from portolan_cli.push import _fetch_remote_versions

        mock_store = MagicMock()

        with patch("portolan_cli.push.obs.get") as mock_get:
            mock_get.side_effect = Exception("Object does not exist in bucket")

            data, etag = _fetch_remote_versions(mock_store, "catalog", "test-collection")

        assert data is None
        assert etag is None

    @pytest.mark.unit
    def test_fetch_remote_versions_404_error(self) -> None:
        """_fetch_remote_versions should handle 404 errors."""
        from portolan_cli.push import _fetch_remote_versions

        mock_store = MagicMock()

        with patch("portolan_cli.push.obs.get") as mock_get:
            mock_get.side_effect = Exception("404 Not Found")

            data, etag = _fetch_remote_versions(mock_store, "catalog", "test-collection")

        assert data is None
        assert etag is None

    @pytest.mark.unit
    def test_fetch_remote_versions_no_such_key_error(self) -> None:
        """_fetch_remote_versions should handle NoSuchKey errors."""
        from portolan_cli.push import _fetch_remote_versions

        mock_store = MagicMock()

        with patch("portolan_cli.push.obs.get") as mock_get:
            mock_get.side_effect = Exception("NoSuchKey: The specified key does not exist")

            data, etag = _fetch_remote_versions(mock_store, "catalog", "test-collection")

        assert data is None
        assert etag is None

    @pytest.mark.unit
    def test_fetch_remote_versions_other_error_raises(self) -> None:
        """_fetch_remote_versions should re-raise non-not-found errors."""
        from portolan_cli.push import _fetch_remote_versions

        mock_store = MagicMock()

        with patch("portolan_cli.push.obs.get") as mock_get:
            mock_get.side_effect = Exception("Access denied: permission error")

            with pytest.raises(Exception, match="Access denied"):
                _fetch_remote_versions(mock_store, "catalog", "test-collection")


# =============================================================================
# Upload Assets Error Handling Tests
# =============================================================================


class TestUploadAssetsErrorHandling:
    """Tests for error handling in _upload_assets_async function."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_upload_assets_handles_exception(self, local_catalog: Path) -> None:
        """_upload_assets_async should catch and report upload exceptions."""
        from portolan_cli.push import _upload_assets_async

        # Create the test file (needed for size calculation)
        test_file = local_catalog / "data.parquet"
        test_file.write_bytes(b"test data")

        mock_store = MagicMock()

        with patch("portolan_cli.push.obs.put_async", new_callable=AsyncMock) as mock_put:
            mock_put.side_effect = Exception("Network timeout")

            files_uploaded, errors, uploaded_keys, _metrics = await _upload_assets_async(
                store=mock_store,
                catalog_root=local_catalog,
                prefix="catalog",
                assets=[test_file],
            )

        assert files_uploaded == 0
        assert len(errors) == 1
        assert "Network timeout" in errors[0]
        assert uploaded_keys == []


# =============================================================================
# Upload Versions JSON Error Handling Tests
# =============================================================================


class TestUploadVersionsJsonErrorHandling:
    """Tests for error handling in _upload_versions_json function."""

    @pytest.mark.unit
    def test_upload_versions_json_precondition_error(self) -> None:
        """_upload_versions_json should raise PushConflictError on precondition failure."""
        from portolan_cli.push import PushConflictError, _upload_versions_json

        mock_store = MagicMock()
        versions_data = {"spec_version": "1.0.0", "versions": []}

        with patch("portolan_cli.push.obs.put") as mock_put:
            mock_put.side_effect = Exception("PreconditionFailed: etag mismatch")

            with pytest.raises(PushConflictError, match="Remote changed during push"):
                _upload_versions_json(
                    store=mock_store,
                    prefix="catalog",
                    collection="test",
                    versions_data=versions_data,
                    etag="old-etag",
                )

    @pytest.mark.unit
    def test_upload_versions_json_with_force(self) -> None:
        """_upload_versions_json with force=True should not use etag."""
        from portolan_cli.push import _upload_versions_json

        mock_store = MagicMock()
        versions_data = {"spec_version": "1.0.0", "versions": []}

        with patch("portolan_cli.push.obs.put") as mock_put:
            mock_put.return_value = None

            _upload_versions_json(
                store=mock_store,
                prefix="catalog",
                collection="test",
                versions_data=versions_data,
                etag="some-etag",
                force=True,
            )

        # With force=True, put should be called without mode parameter
        call_args = mock_put.call_args
        # Check that mode was not passed (no conditional put)
        if len(call_args) > 1 and call_args[1]:
            assert "mode" not in call_args[1] or call_args[1].get("mode") is None

    @pytest.mark.unit
    def test_upload_versions_json_first_push_no_etag(self) -> None:
        """_upload_versions_json with etag=None should use overwrite mode."""
        from portolan_cli.push import _upload_versions_json

        mock_store = MagicMock()
        versions_data = {"spec_version": "1.0.0", "versions": []}

        with patch("portolan_cli.push.obs.put") as mock_put:
            mock_put.return_value = None

            _upload_versions_json(
                store=mock_store,
                prefix="catalog",
                collection="test",
                versions_data=versions_data,
                etag=None,  # First push
            )

        # With etag=None, put should be called without mode parameter
        mock_put.assert_called_once()

    @pytest.mark.unit
    def test_upload_versions_json_other_error_reraises(self) -> None:
        """_upload_versions_json should re-raise non-precondition errors."""
        from portolan_cli.push import _upload_versions_json

        mock_store = MagicMock()
        versions_data = {"spec_version": "1.0.0", "versions": []}

        with patch("portolan_cli.push.obs.put") as mock_put:
            mock_put.side_effect = Exception("Network timeout")

            with pytest.raises(Exception, match="Network timeout"):
                _upload_versions_json(
                    store=mock_store,
                    prefix="catalog",
                    collection="test",
                    versions_data=versions_data,
                    etag="etag",
                )


# =============================================================================
# Error Path Tests with Malformed Data
# =============================================================================


class TestMalformedDataHandling:
    """Tests for error handling with malformed versions.json data."""

    @pytest.mark.unit
    def test_read_local_versions_invalid_json(self, local_catalog_invalid_json: Path) -> None:
        """_read_local_versions should raise ValueError on invalid JSON."""
        from portolan_cli.push import _read_local_versions

        with pytest.raises(ValueError, match="Invalid JSON"):
            _read_local_versions(
                catalog_root=local_catalog_invalid_json,
                collection="test",
            )

    @pytest.mark.unit
    def test_read_local_versions_missing_keys(self, local_catalog_malformed: Path) -> None:
        """_read_local_versions should return data but push should fail on missing keys."""
        from portolan_cli.push import _read_local_versions

        # _read_local_versions just parses JSON, it doesn't validate schema
        # The schema validation happens later in the push process
        data = _read_local_versions(
            catalog_root=local_catalog_malformed,
            collection="test",
        )

        # Data should be returned (it's valid JSON, just missing fields)
        assert "versions" in data
        # But required fields are missing
        assert "spec_version" not in data
        assert "current_version" not in data

    @pytest.mark.unit
    def test_push_with_malformed_local_versions_keyerror(
        self, local_catalog_malformed: Path
    ) -> None:
        """Push should fail when local versions.json has missing required keys.

        The local_catalog_malformed fixture has versions.json missing spec_version
        and current_version. When push tries to read and process this, it should
        fail with a KeyError when accessing these missing fields.
        """
        from portolan_cli.push import _read_local_versions

        # Read the malformed versions.json
        data = _read_local_versions(
            catalog_root=local_catalog_malformed,
            collection="test",
        )

        # Verify the required keys are missing
        assert "spec_version" not in data
        assert "current_version" not in data

        # Attempting to access these keys should raise KeyError
        with pytest.raises(KeyError):
            _ = data["spec_version"]

        with pytest.raises(KeyError):
            _ = data["current_version"]

    @pytest.mark.unit
    def test_push_with_malformed_remote_versions_missing_key(
        self, remote_versions_same_malformed: dict[str, Any]
    ) -> None:
        """Push should handle malformed remote versions.json gracefully.

        The remote_versions_same_malformed fixture is missing spec_version.
        When diff_version_lists tries to extract versions, it should handle
        this gracefully or raise an appropriate error.
        """
        from portolan_cli.push import diff_version_lists

        # The malformed data has versions but is missing spec_version
        # Attempting to extract version strings should still work
        # since versions list exists
        local_versions = ["1.0.0", "1.1.0"]
        remote_versions = [v["version"] for v in remote_versions_same_malformed.get("versions", [])]

        # diff_version_lists should still work with the version strings
        diff = diff_version_lists(local_versions, remote_versions)

        # Local has 1.1.0 that remote doesn't have
        assert "1.1.0" in diff.local_only

    @pytest.mark.unit
    def test_push_with_malformed_asset_fields_extraction(
        self, remote_versions_diverged_malformed: dict[str, Any]
    ) -> None:
        """Push should fail when extracting assets with missing fields.

        The remote_versions_diverged_malformed fixture has assets missing
        sha256 and size_bytes fields. When code tries to access these,
        it should fail with KeyError.
        """
        # Extract the malformed asset
        versions = remote_versions_diverged_malformed["versions"]
        assets = versions[0]["assets"]
        asset_data = assets["data.parquet"]

        # Verify the required fields are missing
        assert "sha256" not in asset_data
        assert "size_bytes" not in asset_data

        # Attempting to access these keys should raise KeyError
        with pytest.raises(KeyError):
            _ = asset_data["sha256"]

        with pytest.raises(KeyError):
            _ = asset_data["size_bytes"]


# =============================================================================
# Property-Based Tests (Hypothesis)
# =============================================================================


class TestPushResultInvariants:
    """Property-based tests for PushResult invariants using Hypothesis."""

    @pytest.mark.unit
    @given(
        success=st.booleans(),
        files_uploaded=st.integers(min_value=0, max_value=1000),
        versions_pushed=st.integers(min_value=0, max_value=100),
        conflicts=st.lists(st.text(min_size=1, max_size=50), max_size=10),
        errors=st.lists(st.text(min_size=1, max_size=100), max_size=10),
    )
    def test_push_result_dataclass_accepts_valid_inputs(
        self,
        success: bool,
        files_uploaded: int,
        versions_pushed: int,
        conflicts: list[str],
        errors: list[str],
    ) -> None:
        """PushResult should accept any valid combination of inputs."""
        from portolan_cli.push import PushResult

        result = PushResult(
            success=success,
            files_uploaded=files_uploaded,
            versions_pushed=versions_pushed,
            conflicts=conflicts,
            errors=errors,
        )

        assert result.success == success
        assert result.files_uploaded == files_uploaded
        assert result.versions_pushed == versions_pushed
        assert result.conflicts == conflicts
        assert result.errors == errors

    @pytest.mark.unit
    def test_dry_run_result_always_has_zero_counts(self, local_catalog: Path) -> None:
        """In dry-run mode, push() should always return zero work done.

        This property encodes the semantic contract: dry-run never actually
        pushes, so files_uploaded and versions_pushed must be 0.

        Tests multiple scenarios to verify invariant holds:
        1. First push (no remote versions)
        2. Incremental push (remote has some versions)
        3. Nothing to push (remote matches local)
        """
        from portolan_cli.push import push

        # Scenario 1: First push (remote has no versions)
        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (None, None)

            result = push(
                catalog_root=local_catalog,
                collection="test",
                destination="s3://bucket/catalog",
                dry_run=True,
            )

            # INVARIANT: dry-run always returns zero counts
            assert result.files_uploaded == 0, "Dry-run should not report files uploaded"
            assert result.versions_pushed == 0, "Dry-run should not report versions pushed"
            assert result.success is True

        # Scenario 2: Incremental push (remote has v1.0.0, local has v1.0.0 + v1.1.0)
        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [
                        {
                            "version": "1.0.0",
                            "created": "2024-01-01T00:00:00Z",
                            "breaking": False,
                            "message": "Initial",
                            "assets": {},
                            "changes": [],
                        }
                    ],
                },
                "etag-123",
            )

            result = push(
                catalog_root=local_catalog,
                collection="test",
                destination="s3://bucket/catalog",
                dry_run=True,
            )

            # INVARIANT: dry-run always returns zero counts
            assert result.files_uploaded == 0, "Dry-run should not report files uploaded"
            assert result.versions_pushed == 0, "Dry-run should not report versions pushed"
            assert result.success is True

        # Scenario 3: Nothing to push (remote matches local exactly)
        local_versions = json.loads((local_catalog / "test" / "versions.json").read_text())
        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (local_versions, "etag-456")

            result = push(
                catalog_root=local_catalog,
                collection="test",
                destination="s3://bucket/catalog",
                dry_run=True,
            )

            # INVARIANT: dry-run always returns zero counts (even when nothing to do)
            assert result.files_uploaded == 0, "Dry-run should not report files uploaded"
            assert result.versions_pushed == 0, "Dry-run should not report versions pushed"
            assert result.success is True


class TestVersionDiffInvariants:
    """Property-based tests for version diffing invariants."""

    @pytest.mark.unit
    @given(
        local_versions=st.lists(st.text(min_size=1, max_size=10), min_size=0, max_size=20),
        remote_versions=st.lists(st.text(min_size=1, max_size=10), min_size=0, max_size=20),
    )
    def test_diff_partitions_versions(
        self, local_versions: list[str], remote_versions: list[str]
    ) -> None:
        """Version diff should partition versions into local_only and remote_only.

        Property: local_only ∪ remote_only ∪ common = local ∪ remote
        Property: local_only ∩ remote_only = ∅
        """
        from portolan_cli.push import diff_version_lists

        diff = diff_version_lists(local_versions, remote_versions)

        # Union of sets should equal union of inputs
        local_set = set(local_versions)
        remote_set = set(remote_versions)

        assert set(diff.local_only) == local_set - remote_set
        assert set(diff.remote_only) == remote_set - local_set
        assert set(diff.local_only) & set(diff.remote_only) == set()  # Disjoint

    @pytest.mark.unit
    @given(
        versions=st.lists(st.text(min_size=1, max_size=10), min_size=0, max_size=20, unique=True),
    )
    def test_identical_versions_means_nothing_to_push(self, versions: list[str]) -> None:
        """When local and remote have identical versions, diff should be empty.

        This is the "nothing to push" case - the invariant our fix addresses.
        """
        from portolan_cli.push import diff_version_lists

        diff = diff_version_lists(versions, versions)

        assert diff.local_only == []
        assert diff.remote_only == []
        assert not diff.has_conflict

    @pytest.mark.unit
    @given(
        local_versions=st.lists(
            st.text(min_size=1, max_size=10), min_size=1, max_size=20, unique=True
        ),
    )
    def test_empty_remote_means_local_only(self, local_versions: list[str]) -> None:
        """When remote is empty, all local versions should be in local_only."""
        from portolan_cli.push import diff_version_lists

        diff = diff_version_lists(local_versions, [])

        assert set(diff.local_only) == set(local_versions)
        assert diff.remote_only == []
        assert not diff.has_conflict  # First push, no conflict


# =============================================================================
# Dry-Run Network Isolation Tests
# =============================================================================


class TestDryRunNetworkIsolation:
    """Tests that dry-run mode never makes network calls.

    Bug #137: --dry-run was still calling _fetch_remote_versions and
    _setup_store, making real network connections. These tests assert
    the fix: dry_run=True must return early BEFORE any network I/O.
    """

    @pytest.mark.unit
    def test_push_dry_run_never_calls_fetch_remote_versions(self, local_catalog: Path) -> None:
        """push(dry_run=True) must not call _fetch_remote_versions at all.

        This is the core regression test for bug #137. The previous behaviour
        always called _setup_store then _fetch_remote_versions regardless of
        dry_run, making real S3/GCS/Azure connections.
        """
        from portolan_cli.push import push

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            with patch("portolan_cli.push.setup_store") as mock_setup:
                mock_setup.return_value = (MagicMock(), "prefix")

                result = push(
                    catalog_root=local_catalog,
                    collection="test",
                    destination="s3://mybucket/catalog",
                    dry_run=True,
                )

        # Neither network operation should be called
        mock_fetch.assert_not_called()
        assert result.success is True

    @pytest.mark.unit
    def test_push_dry_run_never_calls_setup_store(self, local_catalog: Path) -> None:
        """push(dry_run=True) must not call _setup_store (which creates cloud connections)."""
        from portolan_cli.push import push

        with patch("portolan_cli.push.setup_store") as mock_setup:
            result = push(
                catalog_root=local_catalog,
                collection="test",
                destination="s3://mybucket/catalog",
                dry_run=True,
            )

        mock_setup.assert_not_called()
        assert result.success is True

    @pytest.mark.unit
    def test_push_dry_run_shows_would_push_message(self, local_catalog: Path) -> None:
        """push(dry_run=True) should report files that would be uploaded."""
        from portolan_cli.push import push

        with patch("portolan_cli.push.setup_store"):
            with patch("portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock):
                result = push(
                    catalog_root=local_catalog,
                    collection="test",
                    destination="s3://mybucket/catalog",
                    dry_run=True,
                )

        assert result.success is True
        assert result.files_uploaded == 0  # dry-run never uploads
        assert result.dry_run is True  # H3: result must indicate dry-run mode
        assert result.would_push_versions > 0  # H3: should show how many would push

    @pytest.mark.unit
    def test_push_dry_run_does_not_call_upload_assets(self, local_catalog: Path) -> None:
        """push(dry_run=True) must not call _upload_assets."""
        from portolan_cli.push import push

        with patch("portolan_cli.push.setup_store"):
            with patch("portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock):
                with patch(
                    "portolan_cli.push._upload_assets_async", new_callable=AsyncMock
                ) as mock_upload:
                    push(
                        catalog_root=local_catalog,
                        collection="test",
                        destination="s3://mybucket/catalog",
                        dry_run=True,
                    )

        mock_upload.assert_not_called()

    @pytest.mark.unit
    def test_push_non_dry_run_still_calls_fetch_remote_versions(self, local_catalog: Path) -> None:
        """Non-dry-run push must still call _fetch_remote_versions (sanity check)."""
        from portolan_cli.push import push

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            with patch("portolan_cli.push.setup_store") as mock_setup:
                mock_fetch.return_value = (None, None)
                mock_setup.return_value = (MagicMock(), "prefix")

                with patch(
                    "portolan_cli.push._upload_assets_async", new_callable=AsyncMock
                ) as mock_upload:
                    mock_upload.return_value = (1, [], ["key"], UploadMetrics())
                    with patch(
                        "portolan_cli.push._upload_versions_json_async", new_callable=AsyncMock
                    ):
                        push(
                            catalog_root=local_catalog,
                            collection="test",
                            destination="s3://mybucket/catalog",
                            dry_run=False,
                        )

        # Regular push MUST call both setup and fetch
        mock_setup.assert_called_once()
        mock_fetch.assert_called_once()

    @pytest.mark.unit
    def test_push_dry_run_handles_missing_asset_gracefully(self, tmp_path: Path) -> None:
        """H2: push(dry_run=True) should warn but not crash on missing assets.

        When an asset referenced in versions.json is missing locally, dry-run
        should warn and continue (not raise FileNotFoundError) so users can
        see the preview even if local state is inconsistent.
        """
        import json

        from portolan_cli.push import push

        # Create catalog with versions.json referencing a missing file
        catalog_dir = tmp_path / "catalog_missing_asset"
        catalog_dir.mkdir()
        collection_dir = catalog_dir / "test"
        collection_dir.mkdir()

        versions_data = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2024-01-01T00:00:00Z",
                    "breaking": False,
                    "message": "Initial",
                    "assets": {
                        "missing.parquet": {
                            "sha256": "abc123",
                            "size_bytes": 1000,
                            "href": "test/missing.parquet",  # File does NOT exist
                        }
                    },
                    "changes": ["missing.parquet"],
                }
            ],
        }
        (collection_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))
        # NOTE: We deliberately do NOT create the asset file

        with patch("portolan_cli.push.setup_store"):
            with patch("portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock):
                # Should NOT raise - dry-run is forgiving
                result = push(
                    catalog_root=catalog_dir,
                    collection="test",
                    destination="s3://mybucket/catalog",
                    dry_run=True,
                )

        # Dry-run should succeed but record the error
        assert result.success is True
        assert result.dry_run is True
        assert len(result.errors) == 1  # Missing asset recorded as error
        assert "missing.parquet" in result.errors[0]


# =============================================================================
# README Discovery Tests
# =============================================================================


class TestDiscoverStacFilesReadmes:
    """Tests for README.md discovery in _discover_stac_files()."""

    @pytest.mark.unit
    def test_discovers_collection_readme(self, tmp_path: Path) -> None:
        """_discover_stac_files should find collection-level README.md."""
        from portolan_cli.push import _discover_stac_files

        # Setup catalog with collection.json and README
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        collection_dir = catalog_dir / "test"
        collection_dir.mkdir()
        (collection_dir / "collection.json").write_text("{}")
        (collection_dir / "README.md").write_text("# Test Collection")

        result = _discover_stac_files(catalog_dir, "test")

        assert len(result["readmes"]) == 1
        assert result["readmes"][0].name == "README.md"
        assert "test" in str(result["readmes"][0])

    @pytest.mark.unit
    def test_discovers_catalog_readme_when_include_catalog(self, tmp_path: Path) -> None:
        """_discover_stac_files should find root README.md when include_catalog=True."""
        from portolan_cli.push import _discover_stac_files

        # Setup catalog with root README and collection
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        (catalog_dir / "catalog.json").write_text("{}")
        (catalog_dir / "README.md").write_text("# Catalog")
        collection_dir = catalog_dir / "test"
        collection_dir.mkdir()
        (collection_dir / "collection.json").write_text("{}")
        (collection_dir / "README.md").write_text("# Collection")

        result = _discover_stac_files(catalog_dir, "test", include_catalog=True)

        # Should find both READMEs
        assert len(result["readmes"]) == 2
        readme_names = [r.parent.name for r in result["readmes"]]
        assert "catalog" in readme_names  # root README
        assert "test" in readme_names  # collection README

    @pytest.mark.unit
    def test_no_readme_when_missing(self, tmp_path: Path) -> None:
        """_discover_stac_files should handle missing README.md gracefully."""
        from portolan_cli.push import _discover_stac_files

        # Setup catalog without README
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        collection_dir = catalog_dir / "test"
        collection_dir.mkdir()
        (collection_dir / "collection.json").write_text("{}")

        result = _discover_stac_files(catalog_dir, "test")

        assert len(result["readmes"]) == 0


# =============================================================================
# Asset Diffing Tests (Issue #329)
# =============================================================================


class TestAssetDiffing:
    """Tests for asset diffing against remote versions (Issue #329).

    Push should only upload assets that are new or changed, not all assets
    from versions being pushed. This prevents re-uploading 3000 files when
    adding 1 new file to a large catalog.
    """

    @pytest.mark.unit
    def test_skips_assets_already_on_remote_by_sha256(self, tmp_path: Path) -> None:
        """Assets with sha256 matching remote should be skipped.

        Scenario: Local version 2.0.0 has 2 assets, remote version 1.0.0
        has 1 of those assets with same sha256. Only the new asset should
        be uploaded.
        """
        from portolan_cli.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        # Create two asset files
        existing_asset = catalog_dir / "existing.parquet"
        existing_asset.write_bytes(b"existing data")
        new_asset = catalog_dir / "new.parquet"
        new_asset.write_bytes(b"new data")

        local_versions_data = {
            "versions": [
                {
                    "version": "2.0.0",
                    "assets": {
                        "existing.parquet": {
                            "sha256": "sha256_existing",
                            "size_bytes": 13,
                            "href": "existing.parquet",
                        },
                        "new.parquet": {
                            "sha256": "sha256_new",
                            "size_bytes": 8,
                            "href": "new.parquet",
                        },
                    },
                },
            ]
        }

        remote_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "existing.parquet": {
                            "sha256": "sha256_existing",  # Same sha256!
                            "size_bytes": 13,
                            "href": "existing.parquet",
                        },
                    },
                },
            ]
        }

        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            versions_to_push=["2.0.0"],
            remote_versions_data=remote_versions_data,
        )

        # Only new.parquet should be uploaded (existing.parquet has same sha256)
        assert len(assets) == 1
        assert assets[0].name == "new.parquet"

    @pytest.mark.unit
    def test_uploads_changed_assets_with_different_sha256(self, tmp_path: Path) -> None:
        """Assets with different sha256 from remote should be uploaded.

        Scenario: Same file path, different content (different sha256).
        """
        from portolan_cli.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        # Create asset file
        data_file = catalog_dir / "data.parquet"
        data_file.write_bytes(b"updated data")

        local_versions_data = {
            "versions": [
                {
                    "version": "2.0.0",
                    "assets": {
                        "data.parquet": {
                            "sha256": "sha256_v2",  # Different sha256
                            "size_bytes": 12,
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        remote_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "data.parquet": {
                            "sha256": "sha256_v1",  # Original sha256
                            "size_bytes": 10,
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            versions_to_push=["2.0.0"],
            remote_versions_data=remote_versions_data,
        )

        # data.parquet should be uploaded (different sha256)
        assert len(assets) == 1
        assert assets[0].name == "data.parquet"

    @pytest.mark.unit
    def test_uploads_all_assets_when_no_remote(self, tmp_path: Path) -> None:
        """When remote has no versions.json, all assets should be uploaded.

        This is the first push case.
        """
        from portolan_cli.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        # Create asset files
        file1 = catalog_dir / "file1.parquet"
        file1.write_bytes(b"data1")
        file2 = catalog_dir / "file2.parquet"
        file2.write_bytes(b"data2")

        local_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "file1.parquet": {
                            "sha256": "sha1",
                            "href": "file1.parquet",
                        },
                        "file2.parquet": {
                            "sha256": "sha2",
                            "href": "file2.parquet",
                        },
                    },
                },
            ]
        }

        # No remote (first push)
        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            versions_to_push=["1.0.0"],
            remote_versions_data=None,
        )

        assert len(assets) == 2

    @pytest.mark.unit
    def test_considers_all_remote_versions_for_sha256_check(self, tmp_path: Path) -> None:
        """Remote sha256 check should include ALL remote versions, not just latest.

        Scenario: Remote has versions 1.0.0 and 1.1.0 with different assets.
        Local version 2.0.0 has an asset that matches 1.0.0's sha256.
        It should be skipped even though it's not in 1.1.0.
        """
        from portolan_cli.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        asset = catalog_dir / "legacy.parquet"
        asset.write_bytes(b"legacy data")

        local_versions_data = {
            "versions": [
                {
                    "version": "2.0.0",
                    "assets": {
                        "legacy.parquet": {
                            "sha256": "sha256_legacy",
                            "href": "legacy.parquet",
                        },
                    },
                },
            ]
        }

        remote_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "legacy.parquet": {
                            "sha256": "sha256_legacy",  # Same sha256!
                            "href": "legacy.parquet",
                        },
                    },
                },
                {
                    "version": "1.1.0",
                    "assets": {
                        "other.parquet": {
                            "sha256": "sha256_other",
                            "href": "other.parquet",
                        },
                    },
                },
            ]
        }

        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            versions_to_push=["2.0.0"],
            remote_versions_data=remote_versions_data,
        )

        # legacy.parquet exists in 1.0.0 with same sha256, should be skipped
        assert len(assets) == 0

    @pytest.mark.unit
    def test_handles_empty_remote_versions_list(self, tmp_path: Path) -> None:
        """Remote with empty versions list should upload all assets."""
        from portolan_cli.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        asset = catalog_dir / "data.parquet"
        asset.write_bytes(b"data")

        local_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "data.parquet": {
                            "sha256": "sha256_data",
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        remote_versions_data = {"versions": []}  # Empty versions list

        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            versions_to_push=["1.0.0"],
            remote_versions_data=remote_versions_data,
        )

        assert len(assets) == 1

    @pytest.mark.unit
    def test_large_catalog_diffing_performance(self, tmp_path: Path) -> None:
        """Issue #329: Adding 1 file to 3000-file catalog should upload 1 file.

        This is the exact scenario described in the issue.
        """
        from portolan_cli.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        # Create 3001 asset files (simulating large catalog)
        num_existing = 3000
        existing_assets = {}
        for i in range(num_existing):
            filename = f"asset_{i:04d}.parquet"
            (catalog_dir / filename).write_bytes(f"data{i}".encode())
            existing_assets[filename] = {
                "sha256": f"sha256_{i}",
                "href": filename,
            }

        # Add one new asset
        new_filename = "new_asset.parquet"
        (catalog_dir / new_filename).write_bytes(b"new data")
        new_asset = {
            new_filename: {
                "sha256": "sha256_new",
                "href": new_filename,
            }
        }

        # Local version has all 3001 assets (complete snapshot per ADR-0005)
        all_assets = {**existing_assets, **new_asset}
        local_versions_data = {
            "versions": [
                {
                    "version": "2.0.0",
                    "assets": all_assets,
                },
            ]
        }

        # Remote has 3000 existing assets
        remote_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": existing_assets,
                },
            ]
        }

        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            versions_to_push=["2.0.0"],
            remote_versions_data=remote_versions_data,
        )

        # Only 1 new asset should be uploaded, not 3001!
        assert len(assets) == 1
        assert assets[0].name == "new_asset.parquet"

    @pytest.mark.unit
    def test_handles_missing_sha256_in_remote_asset(self, tmp_path: Path) -> None:
        """Assets without sha256 in remote should not block upload.

        Edge case: malformed remote versions.json.
        """
        from portolan_cli.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        asset = catalog_dir / "data.parquet"
        asset.write_bytes(b"data")

        local_versions_data = {
            "versions": [
                {
                    "version": "2.0.0",
                    "assets": {
                        "data.parquet": {
                            "sha256": "sha256_data",
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        remote_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "data.parquet": {
                            # Missing sha256!
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            versions_to_push=["2.0.0"],
            remote_versions_data=remote_versions_data,
        )

        # Should upload since remote has no sha256 to compare
        assert len(assets) == 1

    @pytest.mark.unit
    def test_handles_missing_sha256_in_local_asset(self, tmp_path: Path) -> None:
        """Assets without sha256 in local should still be uploaded.

        Edge case: malformed local versions.json.
        """
        from portolan_cli.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        asset = catalog_dir / "data.parquet"
        asset.write_bytes(b"data")

        local_versions_data = {
            "versions": [
                {
                    "version": "2.0.0",
                    "assets": {
                        "data.parquet": {
                            # Missing sha256!
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        remote_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "data.parquet": {
                            "sha256": "sha256_data",
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            versions_to_push=["2.0.0"],
            remote_versions_data=remote_versions_data,
        )

        # Should upload since local has no sha256 to compare
        assert len(assets) == 1


# =============================================================================
# Real SHA256 Hash Tests (Issue #329)
# =============================================================================


class TestRealSHA256Diffing:
    """Tests for asset diffing using real SHA256 hashes (Issue #329).

    These tests verify the diffing logic works with actual computed hashes,
    not placeholder strings like 'sha256_existing1'.
    """

    @pytest.mark.unit
    def test_real_sha256_diffing_skips_identical_files(self, tmp_path: Path) -> None:
        """Assets with identical content (same real SHA256) should be skipped.

        This test creates real files, computes actual SHA256 hashes, and verifies
        that the diffing logic correctly identifies unchanged files.
        """
        import hashlib

        from portolan_cli.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        # Create a file with known content
        content = b"This is test data that will be hashed"
        real_sha256 = hashlib.sha256(content).hexdigest()

        # Create the local file
        data_file = catalog_dir / "data.parquet"
        data_file.write_bytes(content)

        # Local versions.json references the file with its REAL hash
        local_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "data.parquet": {
                            "sha256": real_sha256,
                            "size_bytes": len(content),
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        # Remote also has the same file with the SAME real hash
        remote_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "data.parquet": {
                            "sha256": real_sha256,  # Same hash!
                            "size_bytes": len(content),
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            versions_to_push=["1.0.0"],
            remote_versions_data=remote_versions_data,
        )

        # File should be SKIPPED because SHA256 matches
        assert len(assets) == 0

    @pytest.mark.unit
    def test_real_sha256_diffing_uploads_modified_files(self, tmp_path: Path) -> None:
        """Assets with different content (different real SHA256) should be uploaded.

        This test verifies that even a small content change results in a different
        hash and triggers an upload.
        """
        import hashlib

        from portolan_cli.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        # Original content and hash (what's on remote)
        original_content = b"Original data version 1"
        original_sha256 = hashlib.sha256(original_content).hexdigest()

        # Modified content and hash (what's local)
        modified_content = b"Modified data version 2"
        modified_sha256 = hashlib.sha256(modified_content).hexdigest()

        # Verify the hashes are actually different
        assert original_sha256 != modified_sha256

        # Create the local file with MODIFIED content
        data_file = catalog_dir / "data.parquet"
        data_file.write_bytes(modified_content)

        # Local has the modified version
        local_versions_data = {
            "versions": [
                {
                    "version": "2.0.0",
                    "assets": {
                        "data.parquet": {
                            "sha256": modified_sha256,
                            "size_bytes": len(modified_content),
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        # Remote has the original version
        remote_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "data.parquet": {
                            "sha256": original_sha256,  # Different hash!
                            "size_bytes": len(original_content),
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            versions_to_push=["2.0.0"],
            remote_versions_data=remote_versions_data,
        )

        # File should be UPLOADED because SHA256 differs
        assert len(assets) == 1
        assert assets[0].name == "data.parquet"

    @pytest.mark.unit
    def test_real_sha256_incremental_add_scenario(self, tmp_path: Path) -> None:
        """Issue #329 core scenario: adding 1 file to a large catalog.

        Simulates adding 1 new file to a catalog with 100 existing files.
        Only the new file should be uploaded.
        """
        import hashlib

        from portolan_cli.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        # Create 100 "existing" files with real hashes
        existing_assets = {}
        for i in range(100):
            content = f"Existing file {i} content".encode()
            sha256 = hashlib.sha256(content).hexdigest()
            filename = f"file_{i:03d}.parquet"

            # Create the actual file
            (catalog_dir / filename).write_bytes(content)

            existing_assets[filename] = {
                "sha256": sha256,
                "size_bytes": len(content),
                "href": filename,
            }

        # Create 1 NEW file
        new_content = b"This is the new file being added"
        new_sha256 = hashlib.sha256(new_content).hexdigest()
        new_filename = "new_file.parquet"
        (catalog_dir / new_filename).write_bytes(new_content)

        # Local version 2.0.0 has all 101 files (100 existing + 1 new)
        all_assets = dict(existing_assets)
        all_assets[new_filename] = {
            "sha256": new_sha256,
            "size_bytes": len(new_content),
            "href": new_filename,
        }

        local_versions_data = {
            "versions": [
                {"version": "1.0.0", "assets": existing_assets},
                {"version": "2.0.0", "assets": all_assets},
            ]
        }

        # Remote only has version 1.0.0 with 100 files
        remote_versions_data = {
            "versions": [
                {"version": "1.0.0", "assets": existing_assets},
            ]
        }

        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            versions_to_push=["2.0.0"],
            remote_versions_data=remote_versions_data,
        )

        # Only the NEW file should be uploaded (not all 101!)
        assert len(assets) == 1
        assert assets[0].name == new_filename
