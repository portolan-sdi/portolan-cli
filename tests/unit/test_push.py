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


class TestDiffVersions:
    """Tests for diff_versions function."""

    @pytest.mark.unit
    def test_diff_local_only_changes(self) -> None:
        """Diff should detect versions that exist only locally."""
        from portolan_cli.push import diff_versions

        local_versions = ["1.0.0", "1.1.0"]
        remote_versions = ["1.0.0"]

        diff = diff_versions(local_versions, remote_versions)

        assert diff.local_only == ["1.1.0"]
        assert diff.remote_only == []
        assert diff.common == ["1.0.0"]

    @pytest.mark.unit
    def test_diff_remote_only_changes(self) -> None:
        """Diff should detect versions that exist only remotely (conflict)."""
        from portolan_cli.push import diff_versions

        local_versions = ["1.0.0"]
        remote_versions = ["1.0.0", "1.0.1"]

        diff = diff_versions(local_versions, remote_versions)

        assert diff.local_only == []
        assert diff.remote_only == ["1.0.1"]
        assert diff.common == ["1.0.0"]

    @pytest.mark.unit
    def test_diff_both_diverged(self) -> None:
        """Diff should detect when both local and remote have unique versions."""
        from portolan_cli.push import diff_versions

        local_versions = ["1.0.0", "1.1.0"]
        remote_versions = ["1.0.0", "1.0.1"]

        diff = diff_versions(local_versions, remote_versions)

        assert diff.local_only == ["1.1.0"]
        assert diff.remote_only == ["1.0.1"]
        assert diff.common == ["1.0.0"]

    @pytest.mark.unit
    def test_diff_empty_remote(self) -> None:
        """Diff should handle empty remote (first push)."""
        from portolan_cli.push import diff_versions

        local_versions = ["1.0.0", "1.1.0"]
        remote_versions: list[str] = []

        diff = diff_versions(local_versions, remote_versions)

        assert diff.local_only == ["1.0.0", "1.1.0"]
        assert diff.remote_only == []
        assert diff.common == []

    @pytest.mark.unit
    def test_diff_identical(self) -> None:
        """Diff should detect when local and remote are identical."""
        from portolan_cli.push import diff_versions

        local_versions = ["1.0.0", "1.1.0"]
        remote_versions = ["1.0.0", "1.1.0"]

        diff = diff_versions(local_versions, remote_versions)

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
                mock_upload_assets.return_value = (1, [])

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
                mock_upload_assets.return_value = (1, [])

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
                mock_upload_assets.return_value = (1, [])

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
                mock_upload_assets.return_value = (1, [])

                with patch("portolan_cli.push._upload_versions_json") as mock_upload_versions:
                    # Simulate PushConflictError from etag mismatch
                    mock_upload_versions.side_effect = PushConflictError(
                        "Remote changed during push, re-run push to try again"
                    )

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
            return (1, [])

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
                mock_upload_assets.return_value = (0, ["Failed to upload data.parquet"])

                with patch("portolan_cli.push._upload_versions_json") as mock_upload_versions:
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
                    mock_upload.return_value = (1, [])

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
