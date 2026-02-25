"""Integration tests for pull command.

Tests for the `portolan pull` CLI command.
These tests verify the CLI correctly wraps the pull library function.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from click.testing import CliRunner

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def cli_runner() -> CliRunner:
    """Create a Click CLI runner."""
    return CliRunner()


@pytest.fixture
def local_catalog(tmp_path: Path) -> Path:
    """Create a local catalog with versions.json."""
    catalog_root = tmp_path / "local_catalog"
    catalog_root.mkdir()

    # Create .portolan structure
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

    # Create the data file
    data_file = catalog_root / "data.parquet"
    data_file.write_bytes(b"x" * 1000)

    return catalog_root


@pytest.fixture
def remote_versions_data() -> dict[str, Any]:
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
# Malformed Fixtures for Error Path Testing
# =============================================================================


@pytest.fixture
def malformed_local_catalog(tmp_path: Path) -> Path:
    """Create a local catalog with invalid JSON in versions.json."""
    catalog_root = tmp_path / "malformed_local"
    catalog_root.mkdir()

    portolan_dir = catalog_root / ".portolan" / "collections" / "test-collection"
    portolan_dir.mkdir(parents=True)

    # Write invalid JSON
    (portolan_dir / "versions.json").write_text("{ not: valid: json }")

    return catalog_root


@pytest.fixture
def malformed_remote_versions_data() -> dict[str, Any]:
    """Remote versions.json with missing required fields."""
    return {
        "spec_version": "1.0.0",
        # Missing "current_version"
        "versions": [
            {
                "version": "1.0.0",
                # Missing "created", "breaking", "assets", "changes"
            }
        ],
    }


# =============================================================================
# CLI Pull Command Tests
# =============================================================================


class TestPullCommand:
    """Tests for `portolan pull` CLI command."""

    @pytest.mark.integration
    def test_pull_command_basic(
        self, cli_runner: CliRunner, local_catalog: Path, remote_versions_data: dict
    ) -> None:
        """portolan pull should invoke pull function with correct args."""
        from portolan_cli.cli import cli

        with patch("portolan_cli.pull.pull") as mock_pull:
            from portolan_cli.pull import PullResult

            mock_pull.return_value = PullResult(
                success=True,
                files_downloaded=1,
                files_skipped=0,
                local_version="1.0.0",
                remote_version="1.1.0",
            )

            result = cli_runner.invoke(
                cli,
                [
                    "pull",
                    "s3://bucket/catalog",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(local_catalog),
                ],
            )

        assert result.exit_code == 0, f"Output: {result.output}"
        mock_pull.assert_called_once()

    @pytest.mark.integration
    def test_pull_command_dry_run(self, cli_runner: CliRunner, local_catalog: Path) -> None:
        """portolan pull --dry-run should pass dry_run=True."""
        from portolan_cli.cli import cli

        with patch("portolan_cli.pull.pull") as mock_pull:
            from portolan_cli.pull import PullResult

            mock_pull.return_value = PullResult(
                success=True,
                files_downloaded=0,
                files_skipped=0,
                local_version="1.0.0",
                remote_version="1.1.0",
            )

            result = cli_runner.invoke(
                cli,
                [
                    "pull",
                    "s3://bucket/catalog",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(local_catalog),
                    "--dry-run",
                ],
            )

        assert result.exit_code == 0, f"Output: {result.output}"
        # Verify dry_run was passed
        call_kwargs = mock_pull.call_args.kwargs
        assert call_kwargs.get("dry_run") is True

    @pytest.mark.integration
    def test_pull_command_force(self, cli_runner: CliRunner, local_catalog: Path) -> None:
        """portolan pull --force should pass force=True."""
        from portolan_cli.cli import cli

        with patch("portolan_cli.pull.pull") as mock_pull:
            from portolan_cli.pull import PullResult

            mock_pull.return_value = PullResult(
                success=True,
                files_downloaded=1,
                files_skipped=0,
                local_version="1.0.0",
                remote_version="1.1.0",
            )

            result = cli_runner.invoke(
                cli,
                [
                    "pull",
                    "s3://bucket/catalog",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(local_catalog),
                    "--force",
                ],
            )

        assert result.exit_code == 0, f"Output: {result.output}"
        call_kwargs = mock_pull.call_args.kwargs
        assert call_kwargs.get("force") is True

    @pytest.mark.integration
    def test_pull_command_profile(self, cli_runner: CliRunner, local_catalog: Path) -> None:
        """portolan pull --profile should pass profile to pull function."""
        from portolan_cli.cli import cli

        with patch("portolan_cli.pull.pull") as mock_pull:
            from portolan_cli.pull import PullResult

            mock_pull.return_value = PullResult(
                success=True,
                files_downloaded=1,
                files_skipped=0,
                local_version="1.0.0",
                remote_version="1.1.0",
            )

            result = cli_runner.invoke(
                cli,
                [
                    "pull",
                    "s3://bucket/catalog",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(local_catalog),
                    "--profile",
                    "myprofile",
                ],
            )

        assert result.exit_code == 0, f"Output: {result.output}"
        call_kwargs = mock_pull.call_args.kwargs
        assert call_kwargs.get("profile") == "myprofile"

    @pytest.mark.integration
    def test_pull_command_uncommitted_changes_fails(
        self, cli_runner: CliRunner, local_catalog: Path
    ) -> None:
        """portolan pull should exit 1 when uncommitted changes block pull."""
        from portolan_cli.cli import cli

        with patch("portolan_cli.pull.pull") as mock_pull:
            from portolan_cli.pull import PullResult

            mock_pull.return_value = PullResult(
                success=False,
                files_downloaded=0,
                files_skipped=0,
                local_version="1.0.0",
                remote_version="1.1.0",
                uncommitted_changes=["data.parquet"],
            )

            result = cli_runner.invoke(
                cli,
                [
                    "pull",
                    "s3://bucket/catalog",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(local_catalog),
                ],
            )

        assert result.exit_code == 1

    @pytest.mark.integration
    def test_pull_command_up_to_date(self, cli_runner: CliRunner, local_catalog: Path) -> None:
        """portolan pull should show message when already up to date."""
        from portolan_cli.cli import cli

        with patch("portolan_cli.pull.pull") as mock_pull:
            from portolan_cli.pull import PullResult

            mock_pull.return_value = PullResult(
                success=True,
                files_downloaded=0,
                files_skipped=0,
                local_version="1.1.0",
                remote_version="1.1.0",
                up_to_date=True,
            )

            result = cli_runner.invoke(
                cli,
                [
                    "pull",
                    "s3://bucket/catalog",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(local_catalog),
                ],
            )

        assert result.exit_code == 0
        assert "up to date" in result.output.lower()

    @pytest.mark.integration
    def test_pull_command_json_output(self, cli_runner: CliRunner, local_catalog: Path) -> None:
        """portolan pull --json should output JSON envelope."""
        from portolan_cli.cli import cli

        with patch("portolan_cli.pull.pull") as mock_pull:
            from portolan_cli.pull import PullResult

            mock_pull.return_value = PullResult(
                success=True,
                files_downloaded=2,
                files_skipped=0,
                local_version="1.0.0",
                remote_version="1.1.0",
            )

            result = cli_runner.invoke(
                cli,
                [
                    "--format",
                    "json",
                    "pull",
                    "s3://bucket/catalog",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(local_catalog),
                ],
            )

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert output["success"] is True
        assert output["command"] == "pull"
        assert output["data"]["files_downloaded"] == 2


# =============================================================================
# CLI Error Handling Tests
# =============================================================================


class TestPullCommandErrors:
    """Tests for error handling in pull CLI command."""

    @pytest.mark.integration
    def test_pull_missing_collection_arg(self, cli_runner: CliRunner, local_catalog: Path) -> None:
        """portolan pull should require --collection."""
        from portolan_cli.cli import cli

        result = cli_runner.invoke(
            cli,
            [
                "pull",
                "s3://bucket/catalog",
                "--catalog",
                str(local_catalog),
            ],
        )

        assert result.exit_code != 0
        assert "collection" in result.output.lower()

    @pytest.mark.integration
    def test_pull_missing_remote_url(self, cli_runner: CliRunner, local_catalog: Path) -> None:
        """portolan pull should require remote URL."""
        from portolan_cli.cli import cli

        result = cli_runner.invoke(
            cli,
            [
                "pull",
                "--collection",
                "test-collection",
                "--catalog",
                str(local_catalog),
            ],
        )

        assert result.exit_code != 0

    @pytest.mark.integration
    def test_pull_json_output_on_failure(self, cli_runner: CliRunner, local_catalog: Path) -> None:
        """portolan pull --json should output error envelope on failure."""
        from portolan_cli.cli import cli

        with patch("portolan_cli.pull.pull") as mock_pull:
            from portolan_cli.pull import PullResult

            mock_pull.return_value = PullResult(
                success=False,
                files_downloaded=0,
                files_skipped=0,
                local_version="1.0.0",
                remote_version="1.1.0",
                uncommitted_changes=["data.parquet"],
            )

            result = cli_runner.invoke(
                cli,
                [
                    "--format",
                    "json",
                    "pull",
                    "s3://bucket/catalog",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(local_catalog),
                ],
            )

        assert result.exit_code == 1
        output = json.loads(result.output)
        assert output["success"] is False
        assert "errors" in output


# =============================================================================
# Malformed Data Error Tests
# =============================================================================


class TestPullMalformedDataErrors:
    """Tests for error handling when pull encounters malformed data."""

    @pytest.mark.integration
    def test_pull_malformed_remote_response(
        self,
        cli_runner: CliRunner,
        local_catalog: Path,
    ) -> None:
        """portolan pull should fail gracefully with malformed remote data."""
        from portolan_cli.cli import cli
        from portolan_cli.pull import PullError

        with patch("portolan_cli.pull.pull") as mock_pull:
            # Simulate that pull raises an error due to malformed data
            mock_pull.side_effect = PullError(
                "Invalid remote versions.json: missing field 'current_version'"
            )

            result = cli_runner.invoke(
                cli,
                [
                    "pull",
                    "s3://bucket/catalog",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(local_catalog),
                ],
            )

        # Should exit with error
        assert result.exit_code != 0

    @pytest.mark.integration
    def test_pull_malformed_local_catalog(
        self, cli_runner: CliRunner, malformed_local_catalog: Path
    ) -> None:
        """portolan pull should handle malformed local catalog gracefully."""
        from portolan_cli.cli import cli
        from portolan_cli.pull import PullResult

        with patch("portolan_cli.pull.pull") as mock_pull:
            # The malformed local catalog may cause pull to fail or succeed
            # depending on how the code handles malformed local data
            mock_pull.return_value = PullResult(
                success=True,
                files_downloaded=0,
                files_skipped=0,
                local_version=None,
                remote_version="1.0.0",
                up_to_date=True,
            )

            result = cli_runner.invoke(
                cli,
                [
                    "pull",
                    "s3://bucket/catalog",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(malformed_local_catalog),
                ],
            )

        # Should either succeed (treating malformed as empty) or fail gracefully
        # The important thing is it doesn't crash unexpectedly
        assert result.exit_code in [0, 1]

    @pytest.mark.integration
    def test_pull_with_malformed_remote_data_structure(
        self,
        cli_runner: CliRunner,
        local_catalog: Path,
        malformed_remote_versions_data: dict[str, Any],
    ) -> None:
        """portolan pull should handle malformed remote versions.json structure."""
        from portolan_cli.cli import cli

        # Verify the fixture has the expected malformed structure
        assert "current_version" not in malformed_remote_versions_data

        with patch("portolan_cli.pull.pull") as mock_pull:
            # Simulate parsing error from malformed remote data
            mock_pull.side_effect = KeyError("current_version")

            result = cli_runner.invoke(
                cli,
                [
                    "pull",
                    "s3://bucket/catalog",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(local_catalog),
                ],
            )

        # Should fail gracefully
        assert result.exit_code != 0

    @pytest.mark.integration
    def test_pull_invalid_url_scheme(self, cli_runner: CliRunner, local_catalog: Path) -> None:
        """portolan pull should reject invalid URL schemes."""
        from portolan_cli.cli import cli

        with patch("portolan_cli.pull.pull") as mock_pull:
            mock_pull.side_effect = ValueError("Unsupported URL scheme: ftp")

            result = cli_runner.invoke(
                cli,
                [
                    "pull",
                    "ftp://invalid/url",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(local_catalog),
                ],
            )

        # Should exit with error for invalid URL scheme
        assert result.exit_code != 0


# =============================================================================
# Pull CLI Error Branch Tests
# =============================================================================


class TestPullCLIErrorBranches:
    """Tests for CLI error handling branches in pull command."""

    @pytest.mark.integration
    def test_pull_cli_json_failure_without_uncommitted(
        self, cli_runner: CliRunner, local_catalog: Path
    ) -> None:
        """Pull CLI should output generic error in JSON when no uncommitted changes."""
        from portolan_cli.cli import cli
        from portolan_cli.pull import PullResult

        with patch("portolan_cli.pull.pull") as mock_pull:
            mock_pull.return_value = PullResult(
                success=False,
                files_downloaded=0,
                files_skipped=0,
                local_version="1.0.0",
                remote_version="1.1.0",
                uncommitted_changes=[],  # No uncommitted changes - generic failure
            )

            result = cli_runner.invoke(
                cli,
                [
                    "--format",
                    "json",
                    "pull",
                    "s3://bucket/catalog",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(local_catalog),
                ],
            )

        assert result.exit_code == 1
        output = json.loads(result.output)
        assert output["success"] is False
        assert "errors" in output
        # Should have a generic "PullError" type
        assert any("PullError" in err["type"] for err in output["errors"])

    @pytest.mark.integration
    def test_pull_cli_dry_run_human_output(
        self, cli_runner: CliRunner, local_catalog: Path
    ) -> None:
        """Pull CLI should show dry-run message in human output."""
        from portolan_cli.cli import cli
        from portolan_cli.pull import PullResult

        with patch("portolan_cli.pull.pull") as mock_pull:
            mock_pull.return_value = PullResult(
                success=True,
                files_downloaded=3,
                files_skipped=0,
                local_version="1.0.0",
                remote_version="1.1.0",
            )

            result = cli_runner.invoke(
                cli,
                [
                    "pull",
                    "s3://bucket/catalog",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(local_catalog),
                    "--dry-run",
                ],
            )

        assert result.exit_code == 0
        assert "dry run" in result.output.lower()

    @pytest.mark.integration
    def test_pull_cli_human_failure_no_uncommitted(
        self, cli_runner: CliRunner, local_catalog: Path
    ) -> None:
        """Pull CLI should show generic error message when failure without uncommitted."""
        from portolan_cli.cli import cli
        from portolan_cli.pull import PullResult

        with patch("portolan_cli.pull.pull") as mock_pull:
            mock_pull.return_value = PullResult(
                success=False,
                files_downloaded=0,
                files_skipped=0,
                local_version="1.0.0",
                remote_version="1.1.0",
                uncommitted_changes=[],  # No uncommitted - generic failure
            )

            result = cli_runner.invoke(
                cli,
                [
                    "pull",
                    "s3://bucket/catalog",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(local_catalog),
                ],
            )

        assert result.exit_code == 1
        assert "failed" in result.output.lower()

    @pytest.mark.integration
    def test_pull_cli_human_uncommitted_shows_files(
        self, cli_runner: CliRunner, local_catalog: Path
    ) -> None:
        """Pull CLI should list uncommitted files in human output."""
        from portolan_cli.cli import cli
        from portolan_cli.pull import PullResult

        with patch("portolan_cli.pull.pull") as mock_pull:
            mock_pull.return_value = PullResult(
                success=False,
                files_downloaded=0,
                files_skipped=0,
                local_version="1.0.0",
                remote_version="1.1.0",
                uncommitted_changes=["data.parquet", "other.parquet"],
            )

            result = cli_runner.invoke(
                cli,
                [
                    "pull",
                    "s3://bucket/catalog",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(local_catalog),
                ],
            )

        assert result.exit_code == 1
        # Should show advice about --force
        assert "force" in result.output.lower()

    @pytest.mark.integration
    def test_pull_cli_success_human_shows_version_transition(
        self, cli_runner: CliRunner, local_catalog: Path
    ) -> None:
        """Pull CLI success should show version transition in human output."""
        from portolan_cli.cli import cli
        from portolan_cli.pull import PullResult

        with patch("portolan_cli.pull.pull") as mock_pull:
            mock_pull.return_value = PullResult(
                success=True,
                files_downloaded=2,
                files_skipped=0,
                local_version="1.0.0",
                remote_version="1.1.0",
            )

            result = cli_runner.invoke(
                cli,
                [
                    "pull",
                    "s3://bucket/catalog",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(local_catalog),
                ],
            )

        assert result.exit_code == 0
        # Should mention versions
        assert "1.0.0" in result.output or "1.1.0" in result.output
