"""Unit tests for sync module - the sync orchestrator.

Tests for the sync command that sequences: Pull -> Init -> Scan -> Check -> Push.
All primitives already exist - sync is orchestration glue.

Following TDD: these tests are written FIRST, before implementation.

Test categories:
- SyncResult dataclass
- Orchestration sequence (all steps called in order)
- Early exit on pull failure
- Skip init if .portolan exists
- Dry-run mode
- Force mode
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest

if TYPE_CHECKING:
    pass


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def managed_catalog(tmp_path: Path) -> Path:
    """Create a managed catalog with .portolan directory structure."""
    catalog_dir = tmp_path / "catalog"
    catalog_dir.mkdir()

    # Create .portolan structure to indicate MANAGED state
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.json").write_text("{}\n")
    (portolan_dir / "state.json").write_text("{}\n")

    # Create collection versions.json
    versions_dir = portolan_dir / "collections" / "test-collection"
    versions_dir.mkdir(parents=True)

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
    (versions_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))

    # Create catalog.json
    catalog_json = {
        "type": "Catalog",
        "id": "catalog",
        "stac_version": "1.0.0",
        "description": "Test catalog",
        "links": [],
    }
    (catalog_dir / "catalog.json").write_text(json.dumps(catalog_json, indent=2))

    # Create data file
    (catalog_dir / "data.parquet").write_bytes(b"x" * 1000)

    return catalog_dir


@pytest.fixture
def fresh_directory(tmp_path: Path) -> Path:
    """Create a fresh directory without .portolan (FRESH state)."""
    fresh_dir = tmp_path / "fresh"
    fresh_dir.mkdir()
    return fresh_dir


# =============================================================================
# SyncResult Tests
# =============================================================================


class TestSyncResult:
    """Tests for SyncResult dataclass."""

    @pytest.mark.unit
    def test_sync_result_success(self) -> None:
        """SyncResult should capture successful sync stats."""
        from portolan_cli.sync import SyncResult

        result = SyncResult(
            success=True,
            pull_result=None,
            init_performed=False,
            scan_result=None,
            check_result=None,
            push_result=None,
            errors=[],
        )

        assert result.success is True
        assert result.errors == []

    @pytest.mark.unit
    def test_sync_result_with_errors(self) -> None:
        """SyncResult should capture errors from any step."""
        from portolan_cli.sync import SyncResult

        result = SyncResult(
            success=False,
            pull_result=None,
            init_performed=False,
            scan_result=None,
            check_result=None,
            push_result=None,
            errors=["Pull failed: network timeout"],
        )

        assert result.success is False
        assert len(result.errors) == 1

    @pytest.mark.unit
    def test_sync_result_with_all_steps(self) -> None:
        """SyncResult should aggregate results from all steps."""
        from portolan_cli.pull import PullResult
        from portolan_cli.push import PushResult
        from portolan_cli.sync import SyncResult

        pull_result = PullResult(
            success=True,
            files_downloaded=2,
            files_skipped=0,
            local_version="1.0.0",
            remote_version="1.1.0",
        )

        push_result = PushResult(
            success=True,
            files_uploaded=1,
            versions_pushed=1,
        )

        result = SyncResult(
            success=True,
            pull_result=pull_result,
            init_performed=True,
            scan_result=None,
            check_result=None,
            push_result=push_result,
            errors=[],
        )

        assert result.success is True
        assert result.pull_result is not None
        assert result.pull_result.files_downloaded == 2
        assert result.push_result is not None
        assert result.push_result.versions_pushed == 1


# =============================================================================
# Orchestration Sequence Tests
# =============================================================================


class TestOrchestrationSequence:
    """Tests for sync orchestration - ensuring steps run in correct order."""

    @pytest.mark.unit
    def test_sync_calls_all_steps_in_order(self, managed_catalog: Path) -> None:
        """Sync should call pull, init, scan, check, push in that order."""
        from portolan_cli.sync import sync

        call_order: list[str] = []

        def track_pull(*args: Any, **kwargs: Any) -> MagicMock:
            call_order.append("pull")
            result = MagicMock()
            result.success = True
            result.up_to_date = True
            result.files_downloaded = 0
            return result

        def track_init(*args: Any, **kwargs: Any) -> tuple[Path, list[str]]:
            call_order.append("init")
            return managed_catalog / "catalog.json", []

        def track_scan(*args: Any, **kwargs: Any) -> MagicMock:
            call_order.append("scan")
            result = MagicMock()
            result.ready = []
            result.has_errors = False
            return result

        def track_check(*args: Any, **kwargs: Any) -> MagicMock:
            call_order.append("check")
            result = MagicMock()
            result.convertible_count = 0
            result.unsupported_count = 0
            return result

        def track_push(*args: Any, **kwargs: Any) -> MagicMock:
            call_order.append("push")
            result = MagicMock()
            result.success = True
            result.files_uploaded = 0
            result.versions_pushed = 0
            return result

        with (
            patch("portolan_cli.sync.pull", side_effect=track_pull),
            patch("portolan_cli.sync.init_catalog", side_effect=track_init),
            patch("portolan_cli.sync.scan_directory", side_effect=track_scan),
            patch("portolan_cli.sync.check_directory", side_effect=track_check),
            patch("portolan_cli.sync.push", side_effect=track_push),
        ):
            sync(
                catalog_root=managed_catalog,
                collection="test-collection",
                destination="s3://bucket/catalog",
            )

        # Verify all steps were called (init may be skipped for managed catalog)
        assert "pull" in call_order
        # init may be skipped if already managed
        assert "scan" in call_order
        assert "check" in call_order
        assert "push" in call_order

        # Verify order: pull must come before scan, scan before check, check before push
        pull_idx = call_order.index("pull")
        scan_idx = call_order.index("scan")
        check_idx = call_order.index("check")
        push_idx = call_order.index("push")

        assert pull_idx < scan_idx, "Pull must come before scan"
        assert scan_idx < check_idx, "Scan must come before check"
        assert check_idx < push_idx, "Check must come before push"


# =============================================================================
# Early Exit Tests
# =============================================================================


class TestEarlyExit:
    """Tests for early exit conditions."""

    @pytest.mark.unit
    def test_sync_exits_early_on_pull_failure(self, managed_catalog: Path) -> None:
        """Sync should exit early if pull fails."""
        from portolan_cli.sync import sync

        def failing_pull(*args: Any, **kwargs: Any) -> MagicMock:
            result = MagicMock()
            result.success = False
            result.uncommitted_changes = ["data.parquet"]
            return result

        with (
            patch("portolan_cli.sync.pull", side_effect=failing_pull),
            patch("portolan_cli.sync.init_catalog"),
            patch("portolan_cli.sync.scan_directory") as mock_scan,
            patch("portolan_cli.sync.check_directory") as mock_check,
            patch("portolan_cli.sync.push") as mock_push,
        ):
            result = sync(
                catalog_root=managed_catalog,
                collection="test-collection",
                destination="s3://bucket/catalog",
            )

        # Pull failed, so subsequent steps should not be called
        mock_scan.assert_not_called()
        mock_check.assert_not_called()
        mock_push.assert_not_called()
        assert result.success is False

    @pytest.mark.unit
    def test_sync_continues_when_pull_up_to_date(self, managed_catalog: Path) -> None:
        """Sync should continue when pull indicates already up to date."""
        from portolan_cli.sync import sync

        def up_to_date_pull(*args: Any, **kwargs: Any) -> MagicMock:
            result = MagicMock()
            result.success = True
            result.up_to_date = True
            result.files_downloaded = 0
            return result

        def successful_push(*args: Any, **kwargs: Any) -> MagicMock:
            result = MagicMock()
            result.success = True
            result.files_uploaded = 0
            result.versions_pushed = 0
            return result

        with (
            patch("portolan_cli.sync.pull", side_effect=up_to_date_pull),
            patch("portolan_cli.sync.init_catalog"),
            patch("portolan_cli.sync.scan_directory") as mock_scan,
            patch("portolan_cli.sync.check_directory") as mock_check,
            patch("portolan_cli.sync.push", side_effect=successful_push),
        ):
            # Set up mock returns
            mock_scan.return_value = MagicMock(ready=[], has_errors=False)
            mock_check.return_value = MagicMock(convertible_count=0, unsupported_count=0)

            result = sync(
                catalog_root=managed_catalog,
                collection="test-collection",
                destination="s3://bucket/catalog",
            )

        # Subsequent steps should be called
        mock_scan.assert_called_once()
        mock_check.assert_called_once()
        assert result.success is True


# =============================================================================
# Init Skip Tests
# =============================================================================


class TestInitSkip:
    """Tests for skipping init when catalog already exists."""

    @pytest.mark.unit
    def test_sync_skips_init_if_managed(self, managed_catalog: Path) -> None:
        """Sync should skip init if catalog is already in MANAGED state."""
        from portolan_cli.sync import sync

        def successful_pull(*args: Any, **kwargs: Any) -> MagicMock:
            result = MagicMock()
            result.success = True
            result.up_to_date = True
            return result

        def successful_push(*args: Any, **kwargs: Any) -> MagicMock:
            result = MagicMock()
            result.success = True
            result.files_uploaded = 0
            result.versions_pushed = 0
            return result

        with (
            patch("portolan_cli.sync.pull", side_effect=successful_pull),
            patch("portolan_cli.sync.init_catalog") as mock_init,
            patch("portolan_cli.sync.scan_directory") as mock_scan,
            patch("portolan_cli.sync.check_directory") as mock_check,
            patch("portolan_cli.sync.push", side_effect=successful_push),
        ):
            mock_scan.return_value = MagicMock(ready=[], has_errors=False)
            mock_check.return_value = MagicMock(convertible_count=0, unsupported_count=0)

            result = sync(
                catalog_root=managed_catalog,
                collection="test-collection",
                destination="s3://bucket/catalog",
            )

        # init_catalog should NOT be called for managed catalog
        mock_init.assert_not_called()
        assert result.init_performed is False

    @pytest.mark.unit
    def test_sync_calls_init_if_fresh(self, fresh_directory: Path) -> None:
        """Sync should call init if directory is in FRESH state."""
        from portolan_cli.sync import sync

        def successful_pull(*args: Any, **kwargs: Any) -> MagicMock:
            result = MagicMock()
            result.success = True
            result.up_to_date = True
            return result

        def successful_push(*args: Any, **kwargs: Any) -> MagicMock:
            result = MagicMock()
            result.success = True
            result.files_uploaded = 0
            result.versions_pushed = 0
            return result

        def mock_init_catalog(path: Path, **kwargs: Any) -> tuple[Path, list[str]]:
            # Simulate init creating the .portolan directory
            portolan_dir = path / ".portolan"
            portolan_dir.mkdir(parents=True, exist_ok=True)
            (portolan_dir / "config.json").write_text("{}\n")
            (portolan_dir / "state.json").write_text("{}\n")
            return path / "catalog.json", []

        with (
            patch("portolan_cli.sync.pull", side_effect=successful_pull),
            patch("portolan_cli.sync.init_catalog", side_effect=mock_init_catalog) as mock_init,
            patch("portolan_cli.sync.scan_directory") as mock_scan,
            patch("portolan_cli.sync.check_directory") as mock_check,
            patch("portolan_cli.sync.push", side_effect=successful_push),
        ):
            mock_scan.return_value = MagicMock(ready=[], has_errors=False)
            mock_check.return_value = MagicMock(convertible_count=0, unsupported_count=0)

            result = sync(
                catalog_root=fresh_directory,
                collection="test-collection",
                destination="s3://bucket/catalog",
            )

        # init_catalog SHOULD be called for fresh directory
        mock_init.assert_called_once()
        assert result.init_performed is True


# =============================================================================
# Dry-Run Mode Tests
# =============================================================================


class TestDryRunMode:
    """Tests for dry-run mode."""

    @pytest.mark.unit
    def test_sync_dry_run_passes_to_push(self, managed_catalog: Path) -> None:
        """Sync --dry-run should pass dry_run=True to push."""
        from portolan_cli.sync import sync

        def successful_pull(*args: Any, **kwargs: Any) -> MagicMock:
            result = MagicMock()
            result.success = True
            result.up_to_date = True
            return result

        with (
            patch("portolan_cli.sync.pull", side_effect=successful_pull),
            patch("portolan_cli.sync.init_catalog"),
            patch("portolan_cli.sync.scan_directory") as mock_scan,
            patch("portolan_cli.sync.check_directory") as mock_check,
            patch("portolan_cli.sync.push") as mock_push,
        ):
            mock_scan.return_value = MagicMock(ready=[], has_errors=False)
            mock_check.return_value = MagicMock(convertible_count=0, unsupported_count=0)
            mock_push.return_value = MagicMock(success=True, files_uploaded=0, versions_pushed=0)

            sync(
                catalog_root=managed_catalog,
                collection="test-collection",
                destination="s3://bucket/catalog",
                dry_run=True,
            )

        # Verify dry_run was passed to push
        mock_push.assert_called_once()
        call_kwargs = mock_push.call_args.kwargs
        assert call_kwargs.get("dry_run") is True

    @pytest.mark.unit
    def test_sync_dry_run_passes_to_pull(self, managed_catalog: Path) -> None:
        """Sync --dry-run should pass dry_run=True to pull."""
        from portolan_cli.sync import sync

        with (
            patch("portolan_cli.sync.pull") as mock_pull,
            patch("portolan_cli.sync.init_catalog"),
            patch("portolan_cli.sync.scan_directory") as mock_scan,
            patch("portolan_cli.sync.check_directory") as mock_check,
            patch("portolan_cli.sync.push") as mock_push,
        ):
            mock_pull.return_value = MagicMock(success=True, up_to_date=True)
            mock_scan.return_value = MagicMock(ready=[], has_errors=False)
            mock_check.return_value = MagicMock(convertible_count=0, unsupported_count=0)
            mock_push.return_value = MagicMock(success=True, files_uploaded=0, versions_pushed=0)

            sync(
                catalog_root=managed_catalog,
                collection="test-collection",
                destination="s3://bucket/catalog",
                dry_run=True,
            )

        # Verify dry_run was passed to pull
        mock_pull.assert_called_once()
        call_kwargs = mock_pull.call_args.kwargs
        assert call_kwargs.get("dry_run") is True


# =============================================================================
# Force Mode Tests
# =============================================================================


class TestForceMode:
    """Tests for force mode."""

    @pytest.mark.unit
    def test_sync_force_passes_to_push(self, managed_catalog: Path) -> None:
        """Sync --force should pass force=True to push."""
        from portolan_cli.sync import sync

        def successful_pull(*args: Any, **kwargs: Any) -> MagicMock:
            result = MagicMock()
            result.success = True
            result.up_to_date = True
            return result

        with (
            patch("portolan_cli.sync.pull", side_effect=successful_pull),
            patch("portolan_cli.sync.init_catalog"),
            patch("portolan_cli.sync.scan_directory") as mock_scan,
            patch("portolan_cli.sync.check_directory") as mock_check,
            patch("portolan_cli.sync.push") as mock_push,
        ):
            mock_scan.return_value = MagicMock(ready=[], has_errors=False)
            mock_check.return_value = MagicMock(convertible_count=0, unsupported_count=0)
            mock_push.return_value = MagicMock(success=True, files_uploaded=0, versions_pushed=0)

            sync(
                catalog_root=managed_catalog,
                collection="test-collection",
                destination="s3://bucket/catalog",
                force=True,
            )

        # Verify force was passed to push
        mock_push.assert_called_once()
        call_kwargs = mock_push.call_args.kwargs
        assert call_kwargs.get("force") is True

    @pytest.mark.unit
    def test_sync_force_passes_to_pull(self, managed_catalog: Path) -> None:
        """Sync --force should pass force=True to pull."""
        from portolan_cli.sync import sync

        with (
            patch("portolan_cli.sync.pull") as mock_pull,
            patch("portolan_cli.sync.init_catalog"),
            patch("portolan_cli.sync.scan_directory") as mock_scan,
            patch("portolan_cli.sync.check_directory") as mock_check,
            patch("portolan_cli.sync.push") as mock_push,
        ):
            mock_pull.return_value = MagicMock(success=True, up_to_date=True)
            mock_scan.return_value = MagicMock(ready=[], has_errors=False)
            mock_check.return_value = MagicMock(convertible_count=0, unsupported_count=0)
            mock_push.return_value = MagicMock(success=True, files_uploaded=0, versions_pushed=0)

            sync(
                catalog_root=managed_catalog,
                collection="test-collection",
                destination="s3://bucket/catalog",
                force=True,
            )

        # Verify force was passed to pull
        mock_pull.assert_called_once()
        call_kwargs = mock_pull.call_args.kwargs
        assert call_kwargs.get("force") is True


# =============================================================================
# Fix Mode Tests
# =============================================================================


class TestFixMode:
    """Tests for fix mode (convert non-cloud-native formats)."""

    @pytest.mark.unit
    def test_sync_fix_passes_to_check(self, managed_catalog: Path) -> None:
        """Sync --fix should pass fix=True to check_directory."""
        from portolan_cli.sync import sync

        def successful_pull(*args: Any, **kwargs: Any) -> MagicMock:
            result = MagicMock()
            result.success = True
            result.up_to_date = True
            return result

        with (
            patch("portolan_cli.sync.pull", side_effect=successful_pull),
            patch("portolan_cli.sync.init_catalog"),
            patch("portolan_cli.sync.scan_directory") as mock_scan,
            patch("portolan_cli.sync.check_directory") as mock_check,
            patch("portolan_cli.sync.push") as mock_push,
        ):
            mock_scan.return_value = MagicMock(ready=[], has_errors=False)
            mock_check.return_value = MagicMock(convertible_count=0, unsupported_count=0)
            mock_push.return_value = MagicMock(success=True, files_uploaded=0, versions_pushed=0)

            sync(
                catalog_root=managed_catalog,
                collection="test-collection",
                destination="s3://bucket/catalog",
                fix=True,
            )

        # Verify fix was passed to check
        mock_check.assert_called_once()
        call_kwargs = mock_check.call_args.kwargs
        assert call_kwargs.get("fix") is True


# =============================================================================
# Profile Passthrough Tests
# =============================================================================


class TestProfilePassthrough:
    """Tests for AWS profile passthrough."""

    @pytest.mark.unit
    def test_sync_profile_passes_to_pull_and_push(self, managed_catalog: Path) -> None:
        """Sync --profile should pass profile to both pull and push."""
        from portolan_cli.sync import sync

        with (
            patch("portolan_cli.sync.pull") as mock_pull,
            patch("portolan_cli.sync.init_catalog"),
            patch("portolan_cli.sync.scan_directory") as mock_scan,
            patch("portolan_cli.sync.check_directory") as mock_check,
            patch("portolan_cli.sync.push") as mock_push,
        ):
            mock_pull.return_value = MagicMock(success=True, up_to_date=True)
            mock_scan.return_value = MagicMock(ready=[], has_errors=False)
            mock_check.return_value = MagicMock(convertible_count=0, unsupported_count=0)
            mock_push.return_value = MagicMock(success=True, files_uploaded=0, versions_pushed=0)

            sync(
                catalog_root=managed_catalog,
                collection="test-collection",
                destination="s3://bucket/catalog",
                profile="production",
            )

        # Verify profile was passed to pull
        pull_kwargs = mock_pull.call_args.kwargs
        assert pull_kwargs.get("profile") == "production"

        # Verify profile was passed to push
        push_kwargs = mock_push.call_args.kwargs
        assert push_kwargs.get("profile") == "production"


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestErrorHandling:
    """Tests for error handling."""

    @pytest.mark.unit
    def test_sync_handles_pull_error_gracefully(self, managed_catalog: Path) -> None:
        """Sync should handle pull errors and not crash."""
        from portolan_cli.pull import PullError
        from portolan_cli.sync import sync

        with patch("portolan_cli.sync.pull") as mock_pull:
            mock_pull.side_effect = PullError("Network timeout")

            result = sync(
                catalog_root=managed_catalog,
                collection="test-collection",
                destination="s3://bucket/catalog",
            )

        assert result.success is False
        assert len(result.errors) > 0
        assert "Network timeout" in str(result.errors)

    @pytest.mark.unit
    def test_sync_handles_push_error_gracefully(self, managed_catalog: Path) -> None:
        """Sync should handle push errors and not crash."""
        from portolan_cli.push import PushConflictError
        from portolan_cli.sync import sync

        def successful_pull(*args: Any, **kwargs: Any) -> MagicMock:
            result = MagicMock()
            result.success = True
            result.up_to_date = True
            return result

        with (
            patch("portolan_cli.sync.pull", side_effect=successful_pull),
            patch("portolan_cli.sync.init_catalog"),
            patch("portolan_cli.sync.scan_directory") as mock_scan,
            patch("portolan_cli.sync.check_directory") as mock_check,
            patch("portolan_cli.sync.push") as mock_push,
        ):
            mock_scan.return_value = MagicMock(ready=[], has_errors=False)
            mock_check.return_value = MagicMock(convertible_count=0, unsupported_count=0)
            mock_push.side_effect = PushConflictError("Remote diverged")

            result = sync(
                catalog_root=managed_catalog,
                collection="test-collection",
                destination="s3://bucket/catalog",
            )

        assert result.success is False
        assert len(result.errors) > 0
        assert "Remote diverged" in str(result.errors) or "conflict" in str(result.errors).lower()

    @pytest.mark.unit
    def test_sync_handles_missing_catalog_root(self, tmp_path: Path) -> None:
        """Sync should handle missing catalog root directory."""
        from portolan_cli.sync import sync

        non_existent = tmp_path / "does_not_exist"

        result = sync(
            catalog_root=non_existent,
            collection="test-collection",
            destination="s3://bucket/catalog",
        )

        assert result.success is False
        assert len(result.errors) > 0


# =============================================================================
# Clone Tests
# =============================================================================


class TestCloneResult:
    """Tests for the CloneResult dataclass."""

    @pytest.mark.unit
    def test_clone_result_stores_success(self, tmp_path: Path) -> None:
        """CloneResult should store success status."""
        from portolan_cli.sync import CloneResult

        result = CloneResult(
            success=True,
            pull_result=None,
            local_path=tmp_path / "cloned",
        )

        assert result.success is True
        assert result.errors == []

    @pytest.mark.unit
    def test_clone_result_stores_errors(self, tmp_path: Path) -> None:
        """CloneResult should store error messages."""
        from portolan_cli.sync import CloneResult

        result = CloneResult(
            success=False,
            pull_result=None,
            local_path=tmp_path / "cloned",
            errors=["Error 1", "Error 2"],
        )

        assert result.success is False
        assert len(result.errors) == 2


class TestCloneFunction:
    """Tests for the clone() function."""

    @pytest.mark.unit
    def test_clone_fails_on_non_empty_directory(self, tmp_path: Path) -> None:
        """Clone should fail if target directory is not empty."""
        from portolan_cli.sync import clone

        # Create non-empty directory
        target = tmp_path / "target"
        target.mkdir()
        (target / "existing_file.txt").write_text("content")

        result = clone(
            remote_url="s3://bucket/catalog",
            local_path=target,
            collection="test",
        )

        assert result.success is False
        assert any("not empty" in err for err in result.errors)

    @pytest.mark.unit
    def test_clone_creates_target_directory(self, tmp_path: Path) -> None:
        """Clone should create target directory if it doesn't exist."""
        from portolan_cli.sync import clone

        target = tmp_path / "new_catalog"

        with (
            patch("portolan_cli.sync.init_catalog") as mock_init,
            patch("portolan_cli.sync.pull") as mock_pull,
        ):
            # Mock successful operations
            mock_init.return_value = None
            mock_pull.return_value = MagicMock(
                success=True,
                files_downloaded=3,
                remote_version="1.0.0",
            )

            result = clone(
                remote_url="s3://bucket/catalog",
                local_path=target,
                collection="test",
            )

        assert target.exists()
        assert result.success is True

    @pytest.mark.unit
    def test_clone_calls_init_catalog(self, tmp_path: Path) -> None:
        """Clone should initialize the catalog."""
        from portolan_cli.sync import clone

        target = tmp_path / "new_catalog"

        with (
            patch("portolan_cli.sync.init_catalog") as mock_init,
            patch("portolan_cli.sync.pull") as mock_pull,
        ):
            mock_pull.return_value = MagicMock(
                success=True,
                files_downloaded=3,
                remote_version="1.0.0",
            )

            clone(
                remote_url="s3://bucket/catalog",
                local_path=target,
                collection="test",
            )

        mock_init.assert_called_once()
        call_args = mock_init.call_args
        assert call_args[0][0] == target  # First positional arg is path

    @pytest.mark.unit
    def test_clone_calls_pull_with_correct_args(self, tmp_path: Path) -> None:
        """Clone should call pull with the correct arguments."""
        from portolan_cli.sync import clone

        target = tmp_path / "new_catalog"

        with (
            patch("portolan_cli.sync.init_catalog"),
            patch("portolan_cli.sync.pull") as mock_pull,
        ):
            mock_pull.return_value = MagicMock(
                success=True,
                files_downloaded=3,
                remote_version="1.0.0",
            )

            clone(
                remote_url="s3://bucket/catalog",
                local_path=target,
                collection="demographics",
                profile="my-profile",
            )

        mock_pull.assert_called_once_with(
            remote_url="s3://bucket/catalog",
            local_root=target,
            collection="demographics",
            force=False,
            dry_run=False,
            profile="my-profile",
        )

    @pytest.mark.unit
    def test_clone_fails_when_pull_fails(self, tmp_path: Path) -> None:
        """Clone should fail if pull fails."""
        from portolan_cli.sync import clone

        target = tmp_path / "new_catalog"

        with (
            patch("portolan_cli.sync.init_catalog"),
            patch("portolan_cli.sync.pull") as mock_pull,
        ):
            mock_pull.return_value = MagicMock(
                success=False,
                remote_version="1.0.0",
                uncommitted_changes=[],
            )

            result = clone(
                remote_url="s3://bucket/catalog",
                local_path=target,
                collection="test",
            )

        assert result.success is False

    @pytest.mark.unit
    def test_clone_fails_when_remote_not_found(self, tmp_path: Path) -> None:
        """Clone should fail with helpful message when remote doesn't exist."""
        from portolan_cli.sync import clone

        target = tmp_path / "new_catalog"

        with (
            patch("portolan_cli.sync.init_catalog"),
            patch("portolan_cli.sync.pull") as mock_pull,
        ):
            mock_pull.return_value = MagicMock(
                success=False,
                remote_version=None,  # Remote doesn't exist
                uncommitted_changes=[],
            )

            result = clone(
                remote_url="s3://bucket/catalog",
                local_path=target,
                collection="test",
            )

        assert result.success is False
        assert any("not found" in err for err in result.errors)

    @pytest.mark.unit
    def test_clone_returns_pull_result(self, tmp_path: Path) -> None:
        """Clone should return the pull result."""
        from portolan_cli.sync import clone

        target = tmp_path / "new_catalog"

        with (
            patch("portolan_cli.sync.init_catalog"),
            patch("portolan_cli.sync.pull") as mock_pull,
        ):
            mock_pull_result = MagicMock(
                success=True,
                files_downloaded=5,
                remote_version="2.0.0",
            )
            mock_pull.return_value = mock_pull_result

            result = clone(
                remote_url="s3://bucket/catalog",
                local_path=target,
                collection="test",
            )

        assert result.pull_result == mock_pull_result
        assert result.pull_result.files_downloaded == 5
