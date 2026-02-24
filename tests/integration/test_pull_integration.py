"""Integration tests for pull command.

Tests for the `portolan pull` CLI command.
These tests verify the CLI correctly wraps the pull library function.
"""

from __future__ import annotations

import json
from pathlib import Path
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

        with patch("portolan_cli.cli.pull") as mock_pull:
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

        with patch("portolan_cli.cli.pull") as mock_pull:
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

        with patch("portolan_cli.cli.pull") as mock_pull:
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

        with patch("portolan_cli.cli.pull") as mock_pull:
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

        with patch("portolan_cli.cli.pull") as mock_pull:
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

        with patch("portolan_cli.cli.pull") as mock_pull:
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

        with patch("portolan_cli.cli.pull") as mock_pull:
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

        with patch("portolan_cli.cli.pull") as mock_pull:
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
