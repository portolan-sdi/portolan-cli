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
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

if TYPE_CHECKING:
    pass


# =============================================================================
# Test fixtures
# =============================================================================


@pytest.fixture
def local_catalog(tmp_path: Path) -> Path:
    """Create a local catalog with versions.json for testing."""
    catalog_dir = tmp_path / "catalog"
    catalog_dir.mkdir()

    # Create .portolan/collections/test/versions.json
    versions_dir = catalog_dir / ".portolan" / "collections" / "test"
    versions_dir.mkdir(parents=True)

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
                        "href": "data.parquet",
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
                        "href": "data.parquet",
                    }
                },
                "changes": ["data.parquet"],
            },
        ],
    }

    (versions_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))

    # Create actual data file
    (catalog_dir / "data.parquet").write_bytes(b"x" * 2048)

    return catalog_dir


@pytest.fixture
def remote_versions_same() -> dict:
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
def remote_versions_diverged() -> dict:
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

        with patch("portolan_cli.push._fetch_remote_versions") as mock_fetch:
            mock_fetch.return_value = (None, None)  # Empty remote (first push)

            with patch("portolan_cli.push._upload_assets") as mock_upload:
                result = push(
                    catalog_root=local_catalog,
                    collection="test",
                    destination="s3://mybucket/catalog",
                    dry_run=True,
                )

        mock_upload.assert_not_called()
        assert result.success is True
        assert result.files_uploaded == 0

    @pytest.mark.unit
    def test_push_detects_conflict_without_force(self, local_catalog: Path) -> None:
        """Push should fail when remote diverged and --force not specified."""
        from portolan_cli.push import PushConflictError, push

        with patch("portolan_cli.push._fetch_remote_versions") as mock_fetch:
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

        with patch("portolan_cli.push._fetch_remote_versions") as mock_fetch:
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

            with patch("portolan_cli.push._upload_assets") as mock_upload_assets:
                mock_upload_assets.return_value = (1, [], ["catalog/data.parquet"])

                with patch("portolan_cli.push._upload_versions_json") as mock_upload_versions:
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

        with patch("portolan_cli.push._fetch_remote_versions") as mock_fetch:
            mock_fetch.return_value = (None, None)  # No remote versions.json

            with patch("portolan_cli.push._upload_assets") as mock_upload_assets:
                mock_upload_assets.return_value = (1, [], ["catalog/data.parquet"])

                with patch("portolan_cli.push._upload_versions_json") as mock_upload_versions:
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
        versions_path = local_catalog / ".portolan" / "collections" / "test" / "versions.json"
        local_data = json.loads(versions_path.read_text())

        with patch("portolan_cli.push._fetch_remote_versions") as mock_fetch:
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

        with patch("portolan_cli.push._fetch_remote_versions") as mock_fetch:
            mock_fetch.return_value = (
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [{"version": "1.0.0", "created": "2024-01-01T00:00:00Z"}],
                },
                "etag-abc123",  # Remote etag
            )

            with patch("portolan_cli.push._upload_assets") as mock_upload_assets:
                mock_upload_assets.return_value = (1, [], ["catalog/data.parquet"])

                with patch("portolan_cli.push._upload_versions_json") as mock_upload_versions:
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

        with patch("portolan_cli.push._fetch_remote_versions") as mock_fetch:
            mock_fetch.return_value = (
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [{"version": "1.0.0", "created": "2024-01-01T00:00:00Z"}],
                },
                "etag-abc123",
            )

            with patch("portolan_cli.push._upload_assets") as mock_upload_assets:
                mock_upload_assets.return_value = (1, [], ["catalog/data.parquet"])

                with patch("portolan_cli.push._upload_versions_json") as mock_upload_versions:
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
        """Assets should be uploaded before versions.json (manifest-last)."""
        from portolan_cli.push import push

        call_order: list[str] = []

        def track_assets(*args, **kwargs):
            call_order.append("assets")
            return (1, [], ["catalog/data.parquet"])

        def track_versions(*args, **kwargs):
            call_order.append("versions")
            return None

        with patch("portolan_cli.push._fetch_remote_versions") as mock_fetch:
            mock_fetch.return_value = (None, None)

            with patch("portolan_cli.push._upload_assets", side_effect=track_assets):
                with patch("portolan_cli.push._upload_versions_json", side_effect=track_versions):
                    push(
                        catalog_root=local_catalog,
                        collection="test",
                        destination="s3://mybucket/catalog",
                    )

        assert call_order == ["assets", "versions"], (
            f"Expected assets before versions, got: {call_order}"
        )

    @pytest.mark.unit
    def test_versions_not_uploaded_if_assets_fail(self, local_catalog: Path) -> None:
        """versions.json should not be uploaded if asset upload fails."""
        from portolan_cli.push import push

        with patch("portolan_cli.push._fetch_remote_versions") as mock_fetch:
            mock_fetch.return_value = (None, None)

            with patch("portolan_cli.push._upload_assets") as mock_upload_assets:
                mock_upload_assets.return_value = (0, ["Failed to upload data.parquet"], [])

                with patch("portolan_cli.push._upload_versions_json") as mock_upload_versions:
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

        with patch("portolan_cli.push._fetch_remote_versions") as mock_fetch:
            mock_fetch.return_value = (None, None)

            with patch("portolan_cli.push._setup_store") as mock_setup:
                mock_store = MagicMock()
                mock_setup.return_value = (mock_store, "prefix")

                with patch("portolan_cli.push._upload_assets") as mock_upload:
                    mock_upload.return_value = (1, [], ["catalog/data.parquet"])

                    with patch("portolan_cli.push._upload_versions_json"):
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

        with patch("portolan_cli.push._fetch_remote_versions") as mock_fetch:
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

            with patch("portolan_cli.push._upload_assets") as mock_upload_assets:
                mock_upload_assets.return_value = (1, [], ["catalog/data.parquet"])

                with patch("portolan_cli.push._upload_versions_json") as mock_upload_versions:
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
        versions_dir = catalog_dir / ".portolan" / "collections" / "test"
        versions_dir.mkdir(parents=True)

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
                            "href": "data.parquet",
                        }
                    },
                    "changes": ["data.parquet"],
                },
            ],
        }
        (versions_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))
        (catalog_dir / "data.parquet").write_bytes(b"x" * 1024)

        with patch("portolan_cli.push._fetch_remote_versions") as mock_fetch:
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

            with patch("portolan_cli.push._upload_assets") as mock_upload_assets:
                mock_upload_assets.return_value = (0, [], [])  # No assets to upload

                with patch("portolan_cli.push._upload_versions_json") as mock_upload_versions:
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
    def test_upload_assets_returns_uploaded_keys(self, local_catalog: Path) -> None:
        """_upload_assets should return list of uploaded object keys."""
        from portolan_cli.push import _upload_assets

        with patch("portolan_cli.push.obs.put") as mock_put:
            mock_put.return_value = None

            mock_store = MagicMock()
            assets = [local_catalog / "data.parquet"]

            files_uploaded, errors, uploaded_keys = _upload_assets(
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

        with patch("portolan_cli.push._fetch_remote_versions") as mock_fetch:
            mock_fetch.return_value = (None, None)

            with patch("portolan_cli.push._upload_assets") as mock_upload_assets:
                mock_upload_assets.return_value = (1, [], ["catalog/data.parquet"])

                with patch("portolan_cli.push._upload_versions_json") as mock_upload_versions:
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

        with patch("portolan_cli.push._fetch_remote_versions") as mock_fetch:
            mock_fetch.return_value = (
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [{"version": "1.0.0", "created": "2024-01-01T00:00:00Z"}],
                },
                "etag-123",
            )

            with patch("portolan_cli.push._upload_assets") as mock_upload_assets:
                mock_upload_assets.return_value = (1, [], ["catalog/data.parquet"])

                with patch("portolan_cli.push._upload_versions_json") as mock_upload_versions:
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
    def test_upload_assets_shows_progress(self, local_catalog: Path, capsys) -> None:
        """_upload_assets should show (1/N) style progress."""
        from portolan_cli.push import _upload_assets

        with patch("portolan_cli.push.obs.put") as mock_put:
            mock_put.return_value = None

            mock_store = MagicMock()
            assets = [local_catalog / "data.parquet"]

            _upload_assets(
                store=mock_store,
                catalog_root=local_catalog,
                prefix="catalog",
                assets=assets,
            )

        # Check that progress indicator was shown
        captured = capsys.readouterr()
        assert "(1/1)" in captured.out

    @pytest.mark.unit
    def test_upload_assets_dry_run_shows_progress(self, local_catalog: Path, capsys) -> None:
        """Dry-run should also show progress indicators."""
        from portolan_cli.push import _upload_assets

        mock_store = MagicMock()
        assets = [local_catalog / "data.parquet"]

        _upload_assets(
            store=mock_store,
            catalog_root=local_catalog,
            prefix="catalog",
            assets=assets,
            dry_run=True,
        )

        captured = capsys.readouterr()
        assert "(1/1)" in captured.out
        assert "DRY RUN" in captured.out


# =============================================================================
# Multi-Cloud Store Setup Tests
# =============================================================================


class TestMultiCloudStoreSetup:
    """Tests for GCS and Azure store setup."""

    @pytest.mark.unit
    def test_setup_store_gcs(self) -> None:
        """_setup_store should create GCSStore for gs:// URLs."""
        from portolan_cli.push import _setup_store

        with patch.dict(
            "os.environ",
            {"GOOGLE_APPLICATION_CREDENTIALS": "/path/to/service-account.json"},
        ):
            with patch("obstore.store.GCSStore") as mock_gcs:
                mock_gcs.return_value = MagicMock()

                store, prefix = _setup_store("gs://mybucket/catalog")

        mock_gcs.assert_called_once_with(
            "mybucket",
            service_account_path="/path/to/service-account.json",
        )
        assert prefix == "catalog"

    @pytest.mark.unit
    def test_setup_store_gcs_no_credentials(self) -> None:
        """_setup_store should work for GCS without explicit credentials."""
        from portolan_cli.push import _setup_store

        with patch.dict("os.environ", {}, clear=True):
            with patch("obstore.store.GCSStore") as mock_gcs:
                mock_gcs.return_value = MagicMock()

                store, prefix = _setup_store("gs://mybucket/prefix")

        # Called without service_account_path (uses default credentials)
        mock_gcs.assert_called_once_with("mybucket")

    @pytest.mark.unit
    def test_setup_store_azure_with_key(self) -> None:
        """_setup_store should create AzureStore with access key."""
        from portolan_cli.push import _setup_store

        # Azure URL format is az://account/container/path
        # The bucket_url after parse is az://account/container
        with patch.dict(
            "os.environ",
            {
                "AZURE_STORAGE_ACCOUNT": "mystorageaccount",
                "AZURE_STORAGE_KEY": "myaccesskey",
            },
        ):
            with patch("obstore.store.AzureStore") as mock_azure:
                mock_azure.return_value = MagicMock()

                store, prefix = _setup_store("az://account/mycontainer/catalog")

        # Container includes account/container from URL parsing
        mock_azure.assert_called_once_with(
            "account/mycontainer",
            account="mystorageaccount",
            access_key="myaccesskey",
        )
        assert prefix == "catalog"

    @pytest.mark.unit
    def test_setup_store_azure_with_sas_token(self) -> None:
        """_setup_store should create AzureStore with SAS token when no key."""
        from portolan_cli.push import _setup_store

        # Azure URL format is az://account/container/path
        with patch.dict(
            "os.environ",
            {
                "AZURE_STORAGE_ACCOUNT": "mystorageaccount",
                "AZURE_STORAGE_SAS_TOKEN": "sv=2021-12-02&...",
            },
            clear=True,
        ):
            with patch("obstore.store.AzureStore") as mock_azure:
                mock_azure.return_value = MagicMock()

                store, prefix = _setup_store("az://account/mycontainer/data")

        mock_azure.assert_called_once_with(
            "account/mycontainer",
            account="mystorageaccount",
            sas_token="sv=2021-12-02&...",
        )
        assert prefix == "data"

    @pytest.mark.unit
    def test_setup_store_azure_key_takes_precedence(self) -> None:
        """_setup_store should prefer access_key over sas_token."""
        from portolan_cli.push import _setup_store

        with patch.dict(
            "os.environ",
            {
                "AZURE_STORAGE_ACCOUNT": "account",
                "AZURE_STORAGE_KEY": "key",
                "AZURE_STORAGE_SAS_TOKEN": "sas",
            },
        ):
            with patch("obstore.store.AzureStore") as mock_azure:
                mock_azure.return_value = MagicMock()

                _setup_store("az://container/prefix")

        # Should use access_key, not sas_token
        call_kwargs = mock_azure.call_args[1]
        assert "access_key" in call_kwargs
        assert "sas_token" not in call_kwargs
