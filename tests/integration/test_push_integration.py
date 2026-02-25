"""Integration tests for push module.

Tests for real filesystem operations and CLI command integration.
These tests verify:
- Local versions.json reading
- Version diffing with real files
- CLI command integration with Click
- Error handling and user feedback
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner


@pytest.fixture
def catalog_with_versions(tmp_path: Path) -> Path:
    """Create a catalog with versions.json for integration tests."""
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

    # Create .portolan directory structure
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()

    # Create collection with versions.json
    collection_dir = portolan_dir / "collections" / "demographics"
    collection_dir.mkdir(parents=True)

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
                    "census.parquet": {
                        "sha256": "abc123def456",
                        "size_bytes": 10240,
                        "href": "collections/demographics/census.parquet",
                    }
                },
                "changes": ["census.parquet"],
            },
            {
                "version": "1.1.0",
                "created": "2024-02-01T00:00:00Z",
                "breaking": False,
                "message": "Updated census data",
                "assets": {
                    "census.parquet": {
                        "sha256": "ghi789jkl012",
                        "size_bytes": 15360,
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
    (asset_dir / "census.parquet").write_bytes(b"x" * 15360)

    return catalog_dir


@pytest.fixture
def catalog_with_versions_malformed(tmp_path: Path) -> Path:
    """Create a catalog with invalid JSON in versions.json."""
    catalog_dir = tmp_path / "catalog_malformed"
    catalog_dir.mkdir()

    # Create catalog.json
    catalog_data = {
        "type": "Catalog",
        "id": "test-catalog",
        "stac_version": "1.0.0",
        "links": [],
    }
    (catalog_dir / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    # Create .portolan directory structure
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()

    # Create collection with invalid JSON in versions.json
    collection_dir = portolan_dir / "collections" / "demographics"
    collection_dir.mkdir(parents=True)

    # Write invalid JSON
    (collection_dir / "versions.json").write_text("{ this is not valid json }")

    return catalog_dir


@pytest.fixture
def catalog_missing_versions_file(tmp_path: Path) -> Path:
    """Create a catalog without versions.json file."""
    catalog_dir = tmp_path / "catalog_no_versions"
    catalog_dir.mkdir()

    # Create catalog.json
    catalog_data = {
        "type": "Catalog",
        "id": "test-catalog",
        "stac_version": "1.0.0",
        "links": [],
    }
    (catalog_dir / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    # Create .portolan directory but no versions.json
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()

    collection_dir = portolan_dir / "collections" / "demographics"
    collection_dir.mkdir(parents=True)
    # Intentionally NOT creating versions.json

    return catalog_dir


# =============================================================================
# Local Versions Reading Tests
# =============================================================================


class TestLocalVersionsReading:
    """Tests for reading local versions.json."""

    @pytest.mark.integration
    def test_read_local_versions_json(self, catalog_with_versions: Path) -> None:
        """Should correctly read local versions.json."""
        from portolan_cli.push import _read_local_versions

        versions_data = _read_local_versions(
            catalog_root=catalog_with_versions,
            collection="demographics",
        )

        assert versions_data["current_version"] == "1.1.0"
        assert len(versions_data["versions"]) == 2

    @pytest.mark.integration
    def test_read_nonexistent_collection_raises(self, catalog_with_versions: Path) -> None:
        """Should raise FileNotFoundError for nonexistent collection."""
        from portolan_cli.push import _read_local_versions

        with pytest.raises(FileNotFoundError, match="versions.json not found"):
            _read_local_versions(
                catalog_root=catalog_with_versions,
                collection="nonexistent",
            )


# =============================================================================
# Version Diffing Integration Tests
# =============================================================================


class TestVersionDiffingIntegration:
    """Integration tests for version diffing with real data."""

    @pytest.mark.integration
    def test_diff_with_real_version_lists(self) -> None:
        """Diff should work with realistic version lists."""
        from portolan_cli.push import diff_version_lists

        local = ["1.0.0", "1.1.0", "1.2.0", "2.0.0"]
        remote = ["1.0.0", "1.1.0"]

        diff = diff_version_lists(local, remote)

        assert diff.local_only == ["1.2.0", "2.0.0"]
        assert diff.remote_only == []
        assert "1.0.0" in diff.common
        assert "1.1.0" in diff.common

    @pytest.mark.integration
    def test_diff_preserves_version_order(self) -> None:
        """Diff should preserve version order in results."""
        from portolan_cli.push import diff_version_lists

        local = ["1.0.0", "1.1.0", "1.2.0"]
        remote = ["1.0.0"]

        diff = diff_version_lists(local, remote)

        # Local-only should be in original order
        assert diff.local_only == ["1.1.0", "1.2.0"]


# =============================================================================
# CLI Integration Tests
# =============================================================================


class TestPushCLI:
    """Integration tests for push CLI command."""

    @pytest.mark.integration
    def test_push_command_exists(self) -> None:
        """Push command should be registered in CLI."""
        from portolan_cli.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["push", "--help"])

        assert result.exit_code == 0
        assert "push" in result.output.lower() or "Push" in result.output

    @pytest.mark.integration
    def test_push_requires_destination(self, catalog_with_versions: Path) -> None:
        """Push should require destination argument."""
        from portolan_cli.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["push"], catch_exceptions=False)

        # Should show error about missing destination
        assert result.exit_code != 0

    @pytest.mark.integration
    def test_push_dry_run_flag(self, catalog_with_versions: Path) -> None:
        """Push --dry-run should show what would be uploaded."""
        from portolan_cli.cli import cli
        from portolan_cli.push import PushResult

        runner = CliRunner()

        with patch("portolan_cli.push.push") as mock_push:
            mock_push.return_value = PushResult(
                success=True,
                files_uploaded=0,
                versions_pushed=0,
                conflicts=[],
                errors=[],
            )

            runner.invoke(
                cli,
                [
                    "push",
                    "s3://mybucket/catalog",
                    "--collection",
                    "demographics",
                    "--dry-run",
                    "--catalog",
                    str(catalog_with_versions),
                ],
                catch_exceptions=False,
            )

            # Verify dry_run=True was passed
            call_kwargs = mock_push.call_args[1]
            assert call_kwargs.get("dry_run") is True

    @pytest.mark.integration
    def test_push_force_flag(self, catalog_with_versions: Path) -> None:
        """Push --force should enable force mode."""
        from portolan_cli.cli import cli
        from portolan_cli.push import PushResult

        runner = CliRunner()

        with patch("portolan_cli.push.push") as mock_push:
            mock_push.return_value = PushResult(
                success=True,
                files_uploaded=1,
                versions_pushed=1,
                conflicts=[],
                errors=[],
            )

            runner.invoke(
                cli,
                [
                    "push",
                    "s3://mybucket/catalog",
                    "--collection",
                    "demographics",
                    "--force",
                    "--catalog",
                    str(catalog_with_versions),
                ],
                catch_exceptions=False,
            )

            call_kwargs = mock_push.call_args[1]
            assert call_kwargs.get("force") is True

    @pytest.mark.integration
    def test_push_profile_flag(self, catalog_with_versions: Path) -> None:
        """Push --profile should pass AWS profile."""
        from portolan_cli.cli import cli
        from portolan_cli.push import PushResult

        runner = CliRunner()

        with patch("portolan_cli.push.push") as mock_push:
            mock_push.return_value = PushResult(
                success=True,
                files_uploaded=1,
                versions_pushed=1,
                conflicts=[],
                errors=[],
            )

            runner.invoke(
                cli,
                [
                    "push",
                    "s3://mybucket/catalog",
                    "--collection",
                    "demographics",
                    "--profile",
                    "myprofile",
                    "--catalog",
                    str(catalog_with_versions),
                ],
                catch_exceptions=False,
            )

            call_kwargs = mock_push.call_args[1]
            assert call_kwargs.get("profile") == "myprofile"

    @pytest.mark.integration
    def test_push_conflict_shows_error(self, catalog_with_versions: Path) -> None:
        """Push conflict should show error message to user."""
        from portolan_cli.cli import cli
        from portolan_cli.push import PushConflictError

        runner = CliRunner()

        with patch("portolan_cli.push.push") as mock_push:
            mock_push.side_effect = PushConflictError(
                "Remote has changes not present locally. Use --force to overwrite."
            )

            result = runner.invoke(
                cli,
                [
                    "push",
                    "s3://mybucket/catalog",
                    "--collection",
                    "demographics",
                    "--catalog",
                    str(catalog_with_versions),
                ],
            )

            assert result.exit_code == 1
            assert "conflict" in result.output.lower() or "force" in result.output.lower()


# =============================================================================
# JSON Output Tests
# =============================================================================


class TestPushJSONOutput:
    """Tests for JSON output mode."""

    @pytest.mark.integration
    def test_push_json_success(self, catalog_with_versions: Path) -> None:
        """Push with --format=json should output JSON envelope."""
        from portolan_cli.cli import cli
        from portolan_cli.push import PushResult

        runner = CliRunner()

        with patch("portolan_cli.push.push") as mock_push:
            mock_push.return_value = PushResult(
                success=True,
                files_uploaded=1,
                versions_pushed=1,
                conflicts=[],
                errors=[],
            )

            result = runner.invoke(
                cli,
                [
                    "--format",
                    "json",
                    "push",
                    "s3://mybucket/catalog",
                    "--collection",
                    "demographics",
                    "--catalog",
                    str(catalog_with_versions),
                ],
                catch_exceptions=False,
            )

            # Should be valid JSON
            output_data = json.loads(result.output)
            assert output_data["success"] is True
            assert "data" in output_data

    @pytest.mark.integration
    def test_push_json_error(self, catalog_with_versions: Path) -> None:
        """Push errors with --format=json should output JSON error envelope."""
        from portolan_cli.cli import cli
        from portolan_cli.push import PushConflictError

        runner = CliRunner()

        with patch("portolan_cli.push.push") as mock_push:
            mock_push.side_effect = PushConflictError("Remote diverged")

            result = runner.invoke(
                cli,
                [
                    "--format",
                    "json",
                    "push",
                    "s3://mybucket/catalog",
                    "--collection",
                    "demographics",
                    "--catalog",
                    str(catalog_with_versions),
                ],
            )

            output_data = json.loads(result.output)
            assert output_data["success"] is False
            assert "errors" in output_data


# =============================================================================
# Dry-run Behavior Tests
# =============================================================================


class TestDryRunBehavior:
    """Tests for dry-run mode behavior."""

    @pytest.mark.integration
    def test_dry_run_shows_files_to_upload(self, catalog_with_versions: Path) -> None:
        """Dry-run should show which files would be uploaded."""
        from portolan_cli.push import push

        with patch("portolan_cli.push._fetch_remote_versions") as mock_fetch:
            mock_fetch.return_value = (None, None)  # First push

            result = push(
                catalog_root=catalog_with_versions,
                collection="demographics",
                destination="s3://mybucket/catalog",
                dry_run=True,
            )

        assert result.success is True
        assert result.files_uploaded == 0  # Dry-run doesn't upload

    @pytest.mark.integration
    def test_dry_run_detects_conflicts(self, catalog_with_versions: Path) -> None:
        """Dry-run should still detect and report conflicts."""
        from portolan_cli.push import PushConflictError, push

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

            # Should raise conflict even in dry-run
            with pytest.raises(PushConflictError):
                push(
                    catalog_root=catalog_with_versions,
                    collection="demographics",
                    destination="s3://mybucket/catalog",
                    dry_run=True,
                )


# =============================================================================
# Asset Path Resolution Tests
# =============================================================================


class TestAssetPathResolution:
    """Tests for resolving asset paths from versions.json."""

    @pytest.mark.integration
    def test_resolve_asset_paths(self, catalog_with_versions: Path) -> None:
        """Should resolve asset paths relative to catalog root."""
        from portolan_cli.push import _get_assets_to_upload

        # Read local versions
        versions_path = (
            catalog_with_versions / ".portolan" / "collections" / "demographics" / "versions.json"
        )
        versions_data = json.loads(versions_path.read_text())

        assets = _get_assets_to_upload(
            catalog_root=catalog_with_versions,
            versions_data=versions_data,
            versions_to_push=["1.1.0"],
        )

        # Should return list of asset paths
        assert len(assets) >= 1
        # Asset paths should be absolute
        for asset_path in assets:
            assert asset_path.is_absolute()


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestErrorHandling:
    """Tests for error handling in push operations."""

    @pytest.mark.integration
    def test_invalid_destination_url(self, catalog_with_versions: Path) -> None:
        """Push with invalid URL should fail gracefully."""
        from portolan_cli.push import push

        with pytest.raises(ValueError, match="Unsupported URL scheme"):
            push(
                catalog_root=catalog_with_versions,
                collection="demographics",
                destination="ftp://invalid/path",
            )

    @pytest.mark.integration
    def test_missing_catalog_root(self, tmp_path: Path) -> None:
        """Push with nonexistent catalog should fail."""
        from portolan_cli.push import push

        nonexistent = tmp_path / "nonexistent"

        with pytest.raises(FileNotFoundError):
            push(
                catalog_root=nonexistent,
                collection="demographics",
                destination="s3://mybucket/catalog",
            )

    @pytest.mark.integration
    def test_push_with_invalid_json(self, catalog_with_versions_malformed: Path) -> None:
        """Push should fail with clear error on invalid JSON in versions.json."""
        from portolan_cli.push import push

        with pytest.raises(ValueError, match="Invalid JSON"):
            push(
                catalog_root=catalog_with_versions_malformed,
                collection="demographics",
                destination="s3://mybucket/catalog",
            )

    @pytest.mark.integration
    def test_push_with_missing_versions_file(self, catalog_missing_versions_file: Path) -> None:
        """Push should fail with clear error when versions.json doesn't exist."""
        from portolan_cli.push import push

        with pytest.raises(FileNotFoundError, match="versions.json"):
            push(
                catalog_root=catalog_missing_versions_file,
                collection="demographics",
                destination="s3://mybucket/catalog",
            )


# =============================================================================
# CLI Error Branch Tests
# =============================================================================


class TestPushCLIErrorBranches:
    """Tests for CLI error handling branches in push command."""

    @pytest.mark.integration
    def test_push_cli_filenotfound_json_output(
        self, catalog_missing_versions_file: Path
    ) -> None:
        """Push CLI with --format=json should output JSON error on FileNotFoundError."""
        from portolan_cli.cli import cli

        runner = CliRunner()

        result = runner.invoke(
            cli,
            [
                "--format",
                "json",
                "push",
                "s3://mybucket/catalog",
                "--collection",
                "demographics",
                "--catalog",
                str(catalog_missing_versions_file),
            ],
        )

        assert result.exit_code == 1
        output_data = json.loads(result.output)
        assert output_data["success"] is False
        assert any("FileNotFoundError" in err["type"] for err in output_data["errors"])

    @pytest.mark.integration
    def test_push_cli_filenotfound_human_output(
        self, catalog_missing_versions_file: Path
    ) -> None:
        """Push CLI should show human-readable error on FileNotFoundError."""
        from portolan_cli.cli import cli

        runner = CliRunner()

        result = runner.invoke(
            cli,
            [
                "push",
                "s3://mybucket/catalog",
                "--collection",
                "demographics",
                "--catalog",
                str(catalog_missing_versions_file),
            ],
        )

        assert result.exit_code == 1
        assert "versions.json" in result.output.lower() or "not found" in result.output.lower()

    @pytest.mark.integration
    def test_push_cli_valueerror_json_output(self, catalog_with_versions: Path) -> None:
        """Push CLI with --format=json should output JSON error on ValueError."""
        from portolan_cli.cli import cli
        from portolan_cli.push import PushResult

        runner = CliRunner()

        with patch("portolan_cli.push.push") as mock_push:
            mock_push.side_effect = ValueError("Invalid destination URL format")

            result = runner.invoke(
                cli,
                [
                    "--format",
                    "json",
                    "push",
                    "invalid://url",
                    "--collection",
                    "demographics",
                    "--catalog",
                    str(catalog_with_versions),
                ],
            )

        assert result.exit_code == 1
        output_data = json.loads(result.output)
        assert output_data["success"] is False
        assert any("ValueError" in err["type"] for err in output_data["errors"])

    @pytest.mark.integration
    def test_push_cli_valueerror_human_output(self, catalog_with_versions: Path) -> None:
        """Push CLI should show human-readable error on ValueError."""
        from portolan_cli.cli import cli

        runner = CliRunner()

        with patch("portolan_cli.push.push") as mock_push:
            mock_push.side_effect = ValueError("Unsupported URL scheme: invalid")

            result = runner.invoke(
                cli,
                [
                    "push",
                    "invalid://url",
                    "--collection",
                    "demographics",
                    "--catalog",
                    str(catalog_with_versions),
                ],
            )

        assert result.exit_code == 1
        # Error message should be shown

    @pytest.mark.integration
    def test_push_cli_result_errors_human_output(self, catalog_with_versions: Path) -> None:
        """Push CLI should show errors from PushResult and exit 1."""
        from portolan_cli.cli import cli
        from portolan_cli.push import PushResult

        runner = CliRunner()

        with patch("portolan_cli.push.push") as mock_push:
            mock_push.return_value = PushResult(
                success=False,
                files_uploaded=0,
                versions_pushed=0,
                conflicts=[],
                errors=["Network timeout uploading file1.parquet"],
            )

            result = runner.invoke(
                cli,
                [
                    "push",
                    "s3://mybucket/catalog",
                    "--collection",
                    "demographics",
                    "--catalog",
                    str(catalog_with_versions),
                ],
            )

        assert result.exit_code == 1

    @pytest.mark.integration
    def test_push_cli_conflict_human_advice(self, catalog_with_versions: Path) -> None:
        """Push CLI should show advice about --force on conflict."""
        from portolan_cli.cli import cli
        from portolan_cli.push import PushConflictError

        runner = CliRunner()

        with patch("portolan_cli.push.push") as mock_push:
            mock_push.side_effect = PushConflictError("Remote has diverged")

            result = runner.invoke(
                cli,
                [
                    "push",
                    "s3://mybucket/catalog",
                    "--collection",
                    "demographics",
                    "--catalog",
                    str(catalog_with_versions),
                ],
            )

        assert result.exit_code == 1
        assert "force" in result.output.lower() or "pull" in result.output.lower()
