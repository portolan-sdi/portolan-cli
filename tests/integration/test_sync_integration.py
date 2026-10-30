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
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.sync.core import SyncResult
from portolan_cli.sync.pull import PullResult
from portolan_cli.sync.push import PushResult

if TYPE_CHECKING:
    from pathlib import Path

    from click.testing import Result


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
    (portolan_dir / "config.yaml").write_text("{}\n")

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


def _success_result(
    *,
    files_downloaded: int = 0,
    remote_version: str = "1.0.0",
    up_to_date: bool = False,
    files_uploaded: int = 0,
    versions_pushed: int = 0,
) -> SyncResult:
    """Return a successful SyncResult with the given pull and push counts."""
    return SyncResult(
        success=True,
        pull_result=PullResult(
            success=True,
            files_downloaded=files_downloaded,
            files_skipped=0,
            local_version="1.0.0",
            remote_version=remote_version,
            up_to_date=up_to_date,
        ),
        init_performed=False,
        scan_result=None,
        check_result=None,
        push_result=PushResult(
            success=True,
            files_uploaded=files_uploaded,
            versions_pushed=versions_pushed,
        ),
        errors=[],
    )


def _failure_result(error: str) -> SyncResult:
    """Return a failed SyncResult with one error message."""
    return SyncResult(
        success=False,
        pull_result=None,
        init_performed=False,
        scan_result=None,
        check_result=None,
        push_result=None,
        errors=[error],
    )


def _invoke_sync(
    catalog: Path,
    extra_args: list[str],
    *,
    json_output: bool = False,
    catch_exceptions: bool = True,
) -> Result:
    """Run sync to s3://mybucket/catalog for the demographics collection."""
    prefix = ["--format", "json"] if json_output else []
    return CliRunner().invoke(
        cli,
        [
            *prefix,
            "sync",
            "s3://mybucket/catalog",
            "--collection",
            "demographics",
            *extra_args,
            "--catalog",
            str(catalog),
        ],
        catch_exceptions=catch_exceptions,
    )


# =============================================================================
# CLI Registration Tests
# =============================================================================


class TestSyncCLI:
    """Integration tests for sync CLI command."""

    @pytest.mark.integration
    def test_sync_command_exists(self) -> None:
        """Sync command should be registered in CLI."""
        runner = CliRunner()
        result = runner.invoke(cli, ["sync", "--help"])

        assert result.exit_code == 0
        assert "sync" in result.output.lower() or "Sync" in result.output

    @pytest.mark.integration
    def test_sync_requires_destination(self) -> None:
        """Sync should require destination argument."""
        runner = CliRunner()
        result = runner.invoke(cli, ["sync"])

        # Should show error about missing destination
        assert result.exit_code != 0

    @pytest.mark.integration
    def test_sync_requires_collection(self, managed_catalog: Path) -> None:
        """Sync should require --collection flag."""
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
        with patch("portolan_cli.sync.core.sync") as mock_sync:
            mock_sync.return_value = _success_result(up_to_date=True)

            result = _invoke_sync(managed_catalog, ["--dry-run"], catch_exceptions=False)

            # Verify dry_run=True was passed
            call_kwargs = mock_sync.call_args.kwargs
            assert call_kwargs.get("dry_run") is True
            assert result.exit_code == 0

    @pytest.mark.integration
    def test_sync_force_flag(self, managed_catalog: Path) -> None:
        """Sync --force should pass force=True to sync function."""
        with patch("portolan_cli.sync.core.sync") as mock_sync:
            mock_sync.return_value = _success_result()

            _invoke_sync(managed_catalog, ["--force"], catch_exceptions=False)

            call_kwargs = mock_sync.call_args.kwargs
            assert call_kwargs.get("force") is True

    @pytest.mark.integration
    def test_sync_fix_flag(self, managed_catalog: Path) -> None:
        """Sync --fix should pass fix=True to sync function."""
        with patch("portolan_cli.sync.core.sync") as mock_sync:
            mock_sync.return_value = _success_result()

            _invoke_sync(managed_catalog, ["--fix"], catch_exceptions=False)

            call_kwargs = mock_sync.call_args.kwargs
            assert call_kwargs.get("fix") is True

    @pytest.mark.integration
    def test_sync_profile_flag(self, managed_catalog: Path) -> None:
        """Sync --profile should pass AWS profile to sync function."""
        with patch("portolan_cli.sync.core.sync") as mock_sync:
            mock_sync.return_value = _success_result()

            _invoke_sync(managed_catalog, ["--profile", "production"], catch_exceptions=False)

            call_kwargs = mock_sync.call_args.kwargs
            assert call_kwargs.get("profile") == "production"

    @pytest.mark.integration
    def test_sync_reads_aws_profile_from_env(self, managed_catalog: Path) -> None:
        """Sync should read aws_profile from PORTOLAN_AWS_PROFILE when --profile not specified.

        Note: aws_profile is a sensitive setting and cannot be set in config.yaml (Issue #356).
        """
        import os
        from unittest.mock import patch as mock_patch

        runner = CliRunner()

        with mock_patch("portolan_cli.sync.core.sync") as mock_sync:
            mock_sync.return_value = _success_result()

            # Set profile via env var (Issue #356: sensitive settings)
            with mock_patch.dict(
                os.environ,
                {
                    "PORTOLAN_REMOTE": "s3://test-bucket/catalog",
                    "PORTOLAN_AWS_PROFILE": "env-profile",
                },
            ):
                runner.invoke(
                    cli,
                    [
                        "sync",
                        "--collection",
                        "demographics",
                        "--catalog",
                        str(managed_catalog),
                    ],
                    catch_exceptions=False,
                )

            # Should read profile from env var
            call_kwargs = mock_sync.call_args.kwargs
            assert call_kwargs.get("profile") == "env-profile"


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestSyncErrorHandling:
    """Tests for error handling in sync CLI."""

    @pytest.mark.integration
    def test_sync_failure_returns_nonzero(self, managed_catalog: Path) -> None:
        """Sync failure should return non-zero exit code."""
        with patch("portolan_cli.sync.core.sync") as mock_sync:
            mock_sync.return_value = _failure_result("Pull failed: network timeout")

            result = _invoke_sync(managed_catalog, [])

            assert result.exit_code == 1

    @pytest.mark.integration
    def test_sync_missing_catalog_returns_error(self, tmp_path: Path) -> None:
        """Sync with non-existent catalog should fail."""
        non_existent = tmp_path / "does_not_exist"

        with patch("portolan_cli.sync.core.sync") as mock_sync:
            mock_sync.return_value = _failure_result("Catalog root not found")

            result = _invoke_sync(non_existent, [])

            assert result.exit_code == 1


# =============================================================================
# JSON Output Tests
# =============================================================================


class TestSyncJSONOutput:
    """Tests for JSON output format."""

    @pytest.mark.integration
    def test_sync_json_output_success(self, managed_catalog: Path) -> None:
        """Sync with --format=json should output valid JSON envelope."""
        with patch("portolan_cli.sync.core.sync") as mock_sync:
            mock_sync.return_value = _success_result(
                files_downloaded=2,
                remote_version="1.1.0",
                files_uploaded=1,
                versions_pushed=1,
            )

            result = _invoke_sync(managed_catalog, [], json_output=True, catch_exceptions=False)

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
        with patch("portolan_cli.sync.core.sync") as mock_sync:
            mock_sync.return_value = _failure_result("Pull failed: connection refused")

            result = _invoke_sync(managed_catalog, [], json_output=True)

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
        with patch("portolan_cli.sync.core.sync") as mock_sync:
            mock_sync.return_value = _success_result()

            result = _invoke_sync(
                managed_catalog,
                ["--force", "--dry-run", "--fix", "--profile", "production"],
                catch_exceptions=False,
            )

            call_kwargs = mock_sync.call_args.kwargs
            assert call_kwargs.get("force") is True
            assert call_kwargs.get("dry_run") is True
            assert call_kwargs.get("fix") is True
            assert call_kwargs.get("profile") == "production"
            assert result.exit_code == 0
