"""Integration tests for sync module.

Tests for CLI command integration and real filesystem operations.
These tests verify:
- CLI command registration and invocation
- Flag passing to underlying sync function
- Error handling and user feedback
- JSON output format
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner


@pytest.fixture
def managed_catalog(tmp_path: Path) -> Path:
    """Create a managed catalog with full .portolan structure."""
    catalog_dir = tmp_path / "catalog"
    catalog_dir.mkdir()

    # Create catalog.json
    catalog_data = {
        "type": "Catalog",
        "id": "test-catalog",
        "description": "Test catalog",
        "stac_version": "1.0.0",
        "links": [],
    }
    (catalog_dir / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    # Create .portolan directory structure (MANAGED state requires both config and state)
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.json").write_text("{}\n")
    (portolan_dir / "state.json").write_text("{}\n")

    # Create collection with versions.json
    collection_dir = portolan_dir / "collections" / "demographics"
    collection_dir.mkdir(parents=True)

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
                    "census.parquet": {
                        "sha256": "abc123def456",
                        "size_bytes": 10240,
                        "href": "collections/demographics/census.parquet",
                    }
                },
                "changes": ["census.parquet"],
            },
        ],
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))

    # Create actual asset file
    asset_dir = catalog_dir / "collections" / "demographics"
    asset_dir.mkdir(parents=True)
    (asset_dir / "census.parquet").write_bytes(b"x" * 10240)

    return catalog_dir


@pytest.fixture
def fresh_directory(tmp_path: Path) -> Path:
    """Create a fresh directory without any catalog structure."""
    fresh_dir = tmp_path / "fresh"
    fresh_dir.mkdir()
    return fresh_dir


# =============================================================================
# CLI Registration Tests
# =============================================================================


class TestSyncCLI:
    """Integration tests for sync CLI command."""

    @pytest.mark.integration
    def test_sync_command_exists(self) -> None:
        """Sync command should be registered in CLI."""
        from portolan_cli.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["sync", "--help"])

        assert result.exit_code == 0
        assert "sync" in result.output.lower() or "Sync" in result.output

    @pytest.mark.integration
    def test_sync_requires_destination(self) -> None:
        """Sync should require destination argument."""
        from portolan_cli.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["sync"])

        # Should show error about missing destination
        assert result.exit_code != 0

    @pytest.mark.integration
    def test_sync_requires_collection(self, managed_catalog: Path) -> None:
        """Sync should require --collection flag."""
        from portolan_cli.cli import cli

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "sync",
                "s3://mybucket/catalog",
                "--catalog",
                str(managed_catalog),
            ],
        )

        # Should show error about missing collection
        assert result.exit_code != 0
        assert "collection" in result.output.lower()

    @pytest.mark.integration
    def test_sync_help_shows_all_options(self) -> None:
        """Sync --help should show all expected options."""
        from portolan_cli.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["sync", "--help"])

        assert result.exit_code == 0
        assert "--collection" in result.output
        assert "--force" in result.output
        assert "--dry-run" in result.output
        assert "--fix" in result.output
        assert "--profile" in result.output
        assert "--catalog" in result.output


# =============================================================================
# Flag Passthrough Tests
# =============================================================================


class TestSyncFlagPassthrough:
    """Tests for CLI flags being passed to sync function."""

    @pytest.mark.integration
    def test_sync_dry_run_flag(self, managed_catalog: Path) -> None:
        """Sync --dry-run should pass dry_run=True to sync function."""
        from portolan_cli.cli import cli
        from portolan_cli.pull import PullResult
        from portolan_cli.push import PushResult
        from portolan_cli.sync import SyncResult

        runner = CliRunner()

        with patch("portolan_cli.sync.sync") as mock_sync:
            mock_sync.return_value = SyncResult(
                success=True,
                pull_result=PullResult(
                    success=True,
                    files_downloaded=0,
                    files_skipped=0,
                    local_version="1.0.0",
                    remote_version="1.0.0",
                    up_to_date=True,
                ),
                init_performed=False,
                scan_result=None,
                check_result=None,
                push_result=PushResult(
                    success=True,
                    files_uploaded=0,
                    versions_pushed=0,
                ),
                errors=[],
            )

            result = runner.invoke(
                cli,
                [
                    "sync",
                    "s3://mybucket/catalog",
                    "--collection",
                    "demographics",
                    "--dry-run",
                    "--catalog",
                    str(managed_catalog),
                ],
                catch_exceptions=False,
            )

            # Verify dry_run=True was passed
            call_kwargs = mock_sync.call_args.kwargs
            assert call_kwargs.get("dry_run") is True
            assert result.exit_code == 0

    @pytest.mark.integration
    def test_sync_force_flag(self, managed_catalog: Path) -> None:
        """Sync --force should pass force=True to sync function."""
        from portolan_cli.cli import cli
        from portolan_cli.pull import PullResult
        from portolan_cli.push import PushResult
        from portolan_cli.sync import SyncResult

        runner = CliRunner()

        with patch("portolan_cli.sync.sync") as mock_sync:
            mock_sync.return_value = SyncResult(
                success=True,
                pull_result=PullResult(
                    success=True,
                    files_downloaded=0,
                    files_skipped=0,
                    local_version="1.0.0",
                    remote_version="1.0.0",
                ),
                init_performed=False,
                scan_result=None,
                check_result=None,
                push_result=PushResult(
                    success=True,
                    files_uploaded=0,
                    versions_pushed=0,
                ),
                errors=[],
            )

            runner.invoke(
                cli,
                [
                    "sync",
                    "s3://mybucket/catalog",
                    "--collection",
                    "demographics",
                    "--force",
                    "--catalog",
                    str(managed_catalog),
                ],
                catch_exceptions=False,
            )

            call_kwargs = mock_sync.call_args.kwargs
            assert call_kwargs.get("force") is True

    @pytest.mark.integration
    def test_sync_fix_flag(self, managed_catalog: Path) -> None:
        """Sync --fix should pass fix=True to sync function."""
        from portolan_cli.cli import cli
        from portolan_cli.pull import PullResult
        from portolan_cli.push import PushResult
        from portolan_cli.sync import SyncResult

        runner = CliRunner()

        with patch("portolan_cli.sync.sync") as mock_sync:
            mock_sync.return_value = SyncResult(
                success=True,
                pull_result=PullResult(
                    success=True,
                    files_downloaded=0,
                    files_skipped=0,
                    local_version="1.0.0",
                    remote_version="1.0.0",
                ),
                init_performed=False,
                scan_result=None,
                check_result=None,
                push_result=PushResult(
                    success=True,
                    files_uploaded=0,
                    versions_pushed=0,
                ),
                errors=[],
            )

            runner.invoke(
                cli,
                [
                    "sync",
                    "s3://mybucket/catalog",
                    "--collection",
                    "demographics",
                    "--fix",
                    "--catalog",
                    str(managed_catalog),
                ],
                catch_exceptions=False,
            )

            call_kwargs = mock_sync.call_args.kwargs
            assert call_kwargs.get("fix") is True

    @pytest.mark.integration
    def test_sync_profile_flag(self, managed_catalog: Path) -> None:
        """Sync --profile should pass AWS profile to sync function."""
        from portolan_cli.cli import cli
        from portolan_cli.pull import PullResult
        from portolan_cli.push import PushResult
        from portolan_cli.sync import SyncResult

        runner = CliRunner()

        with patch("portolan_cli.sync.sync") as mock_sync:
            mock_sync.return_value = SyncResult(
                success=True,
                pull_result=PullResult(
                    success=True,
                    files_downloaded=0,
                    files_skipped=0,
                    local_version="1.0.0",
                    remote_version="1.0.0",
                ),
                init_performed=False,
                scan_result=None,
                check_result=None,
                push_result=PushResult(
                    success=True,
                    files_uploaded=0,
                    versions_pushed=0,
                ),
                errors=[],
            )

            runner.invoke(
                cli,
                [
                    "sync",
                    "s3://mybucket/catalog",
                    "--collection",
                    "demographics",
                    "--profile",
                    "production",
                    "--catalog",
                    str(managed_catalog),
                ],
                catch_exceptions=False,
            )

            call_kwargs = mock_sync.call_args.kwargs
            assert call_kwargs.get("profile") == "production"


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestSyncErrorHandling:
    """Tests for error handling in sync CLI."""

    @pytest.mark.integration
    def test_sync_failure_returns_nonzero(self, managed_catalog: Path) -> None:
        """Sync failure should return non-zero exit code."""
        from portolan_cli.cli import cli
        from portolan_cli.sync import SyncResult

        runner = CliRunner()

        with patch("portolan_cli.sync.sync") as mock_sync:
            mock_sync.return_value = SyncResult(
                success=False,
                pull_result=None,
                init_performed=False,
                scan_result=None,
                check_result=None,
                push_result=None,
                errors=["Pull failed: network timeout"],
            )

            result = runner.invoke(
                cli,
                [
                    "sync",
                    "s3://mybucket/catalog",
                    "--collection",
                    "demographics",
                    "--catalog",
                    str(managed_catalog),
                ],
            )

            assert result.exit_code == 1

    @pytest.mark.integration
    def test_sync_missing_catalog_returns_error(self, tmp_path: Path) -> None:
        """Sync with non-existent catalog should fail."""
        from portolan_cli.cli import cli
        from portolan_cli.sync import SyncResult

        runner = CliRunner()
        non_existent = tmp_path / "does_not_exist"

        with patch("portolan_cli.sync.sync") as mock_sync:
            mock_sync.return_value = SyncResult(
                success=False,
                pull_result=None,
                init_performed=False,
                scan_result=None,
                check_result=None,
                push_result=None,
                errors=["Catalog root not found"],
            )

            result = runner.invoke(
                cli,
                [
                    "sync",
                    "s3://mybucket/catalog",
                    "--collection",
                    "demographics",
                    "--catalog",
                    str(non_existent),
                ],
            )

            assert result.exit_code == 1


# =============================================================================
# JSON Output Tests
# =============================================================================


class TestSyncJSONOutput:
    """Tests for JSON output format."""

    @pytest.mark.integration
    def test_sync_json_output_success(self, managed_catalog: Path) -> None:
        """Sync with --format=json should output valid JSON envelope."""
        from portolan_cli.cli import cli
        from portolan_cli.pull import PullResult
        from portolan_cli.push import PushResult
        from portolan_cli.sync import SyncResult

        runner = CliRunner()

        with patch("portolan_cli.sync.sync") as mock_sync:
            mock_sync.return_value = SyncResult(
                success=True,
                pull_result=PullResult(
                    success=True,
                    files_downloaded=2,
                    files_skipped=0,
                    local_version="1.0.0",
                    remote_version="1.1.0",
                ),
                init_performed=False,
                scan_result=None,
                check_result=None,
                push_result=PushResult(
                    success=True,
                    files_uploaded=1,
                    versions_pushed=1,
                ),
                errors=[],
            )

            result = runner.invoke(
                cli,
                [
                    "--format",
                    "json",
                    "sync",
                    "s3://mybucket/catalog",
                    "--collection",
                    "demographics",
                    "--catalog",
                    str(managed_catalog),
                ],
                catch_exceptions=False,
            )

            assert result.exit_code == 0

            # Parse and validate JSON output
            output = json.loads(result.output)
            assert output["success"] is True
            assert output["command"] == "sync"
            assert "data" in output
            assert "pull" in output["data"]
            assert "push" in output["data"]
            assert output["data"]["pull"]["files_downloaded"] == 2
            assert output["data"]["push"]["versions_pushed"] == 1

    @pytest.mark.integration
    def test_sync_json_output_failure(self, managed_catalog: Path) -> None:
        """Sync failure with --format=json should include errors array."""
        from portolan_cli.cli import cli
        from portolan_cli.sync import SyncResult

        runner = CliRunner()

        with patch("portolan_cli.sync.sync") as mock_sync:
            mock_sync.return_value = SyncResult(
                success=False,
                pull_result=None,
                init_performed=False,
                scan_result=None,
                check_result=None,
                push_result=None,
                errors=["Pull failed: connection refused"],
            )

            result = runner.invoke(
                cli,
                [
                    "--format",
                    "json",
                    "sync",
                    "s3://mybucket/catalog",
                    "--collection",
                    "demographics",
                    "--catalog",
                    str(managed_catalog),
                ],
            )

            assert result.exit_code == 1

            # Parse and validate JSON output
            output = json.loads(result.output)
            assert output["success"] is False
            assert output["command"] == "sync"
            assert "errors" in output
            assert len(output["errors"]) > 0
            assert "Pull failed" in output["errors"][0]["message"]


# =============================================================================
# Combined Flag Tests
# =============================================================================


class TestSyncCombinedFlags:
    """Tests for using multiple flags together."""

    @pytest.mark.integration
    def test_sync_all_flags_together(self, managed_catalog: Path) -> None:
        """Sync should handle all flags simultaneously."""
        from portolan_cli.cli import cli
        from portolan_cli.pull import PullResult
        from portolan_cli.push import PushResult
        from portolan_cli.sync import SyncResult

        runner = CliRunner()

        with patch("portolan_cli.sync.sync") as mock_sync:
            mock_sync.return_value = SyncResult(
                success=True,
                pull_result=PullResult(
                    success=True,
                    files_downloaded=0,
                    files_skipped=0,
                    local_version="1.0.0",
                    remote_version="1.0.0",
                ),
                init_performed=False,
                scan_result=None,
                check_result=None,
                push_result=PushResult(
                    success=True,
                    files_uploaded=0,
                    versions_pushed=0,
                ),
                errors=[],
            )

            result = runner.invoke(
                cli,
                [
                    "sync",
                    "s3://mybucket/catalog",
                    "--collection",
                    "demographics",
                    "--force",
                    "--dry-run",
                    "--fix",
                    "--profile",
                    "production",
                    "--catalog",
                    str(managed_catalog),
                ],
                catch_exceptions=False,
            )

            call_kwargs = mock_sync.call_args.kwargs
            assert call_kwargs.get("force") is True
            assert call_kwargs.get("dry_run") is True
            assert call_kwargs.get("fix") is True
            assert call_kwargs.get("profile") == "production"
            assert result.exit_code == 0
