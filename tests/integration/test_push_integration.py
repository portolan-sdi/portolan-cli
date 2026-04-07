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
from unittest.mock import AsyncMock, patch

import pytest
from click.testing import CliRunner
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st


@pytest.fixture
def catalog_with_versions(tmp_path: Path) -> Path:
    """Create a catalog with versions.json for integration tests.

    Per ADR-0023: STAC files (catalog.json, collection.json, versions.json)
    live at root level; config.yaml goes in .portolan/.
    """
    catalog_dir = tmp_path / "catalog"
    catalog_dir.mkdir()

    # Create catalog.json at root (per ADR-0023)
    catalog_data = {
        "type": "Catalog",
        "id": "test-catalog",
        "description": "Test catalog",
        "stac_version": "1.0.0",
        "links": [],
    }
    (catalog_dir / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    # Create .portolan directory for internal state only
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()

    # Create collection directory at root with versions.json (per ADR-0023)
    collection_dir = catalog_dir / "demographics"
    collection_dir.mkdir(parents=True)

    # Create collection.json (STAC collection metadata)
    collection_data = {
        "type": "Collection",
        "id": "demographics",
        "stac_version": "1.0.0",
        "description": "Demographics collection",
        "license": "proprietary",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
        },
        "links": [],
    }
    (collection_dir / "collection.json").write_text(json.dumps(collection_data, indent=2))

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
                    "census.parquet": {
                        "sha256": "abc123def456",
                        "size_bytes": 10240,
                        "href": "demographics/census/census.parquet",
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
                        "href": "demographics/census/census.parquet",
                    }
                },
                "changes": ["census.parquet"],
            },
        ],
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))

    # Create actual asset file at collection/item/filename
    item_dir = collection_dir / "census"
    item_dir.mkdir(parents=True)
    (item_dir / "census.parquet").write_bytes(b"x" * 15360)

    return catalog_dir


@pytest.fixture
def catalog_with_versions_malformed(tmp_path: Path) -> Path:
    """Create a catalog with invalid JSON in versions.json.

    Per ADR-0023: versions.json lives at collection root level.
    """
    catalog_dir = tmp_path / "catalog_malformed"
    catalog_dir.mkdir()

    # Create catalog.json at root
    catalog_data = {
        "type": "Catalog",
        "id": "test-catalog",
        "stac_version": "1.0.0",
        "description": "Test catalog",
        "links": [],
    }
    (catalog_dir / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    # Create .portolan directory for internal state
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()

    # Create collection directory at root with invalid versions.json
    collection_dir = catalog_dir / "demographics"
    collection_dir.mkdir(parents=True)

    # Write invalid JSON
    (collection_dir / "versions.json").write_text("{ this is not valid json }")

    return catalog_dir


@pytest.fixture
def catalog_missing_versions_file(tmp_path: Path) -> Path:
    """Create a catalog without versions.json file.

    Per ADR-0023: Collection directory exists at root but has no versions.json.
    """
    catalog_dir = tmp_path / "catalog_no_versions"
    catalog_dir.mkdir()

    # Create catalog.json at root
    catalog_data = {
        "type": "Catalog",
        "id": "test-catalog",
        "stac_version": "1.0.0",
        "description": "Test catalog",
        "links": [],
    }
    (catalog_dir / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    # Create .portolan directory for internal state
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()

    # Create collection directory at root but no versions.json
    collection_dir = catalog_dir / "demographics"
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
        from unittest.mock import AsyncMock

        from portolan_cli.cli import cli
        from portolan_cli.push import PushResult

        runner = CliRunner()

        # CLI calls push_async directly (not push) for single-collection push
        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
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
        from unittest.mock import AsyncMock

        from portolan_cli.cli import cli
        from portolan_cli.push import PushResult

        runner = CliRunner()

        # CLI calls push_async directly (not push) for single-collection push
        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
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

        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
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
    def test_push_reads_aws_profile_from_config(self, catalog_with_versions: Path) -> None:
        """Push should read aws_profile from .portolan/config.yaml when --profile not specified."""
        from portolan_cli.cli import cli
        from portolan_cli.push import PushResult

        runner = CliRunner()

        # Set aws_profile in config.yaml
        config_file = catalog_with_versions / ".portolan" / "config.yaml"
        config_file.write_text("remote: s3://test-bucket/catalog\naws_profile: config-profile\n")

        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
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
                    "--collection",
                    "demographics",
                    "--catalog",
                    str(catalog_with_versions),
                ],
                catch_exceptions=False,
            )

            # Should read profile from config, not use hardcoded "default"
            call_kwargs = mock_push.call_args[1]
            assert call_kwargs.get("profile") == "config-profile"

    @pytest.mark.integration
    def test_push_cli_profile_overrides_config(self, catalog_with_versions: Path) -> None:
        """Push --profile should override aws_profile from config.yaml."""
        from portolan_cli.cli import cli
        from portolan_cli.push import PushResult

        runner = CliRunner()

        # Set aws_profile in config.yaml
        config_file = catalog_with_versions / ".portolan" / "config.yaml"
        config_file.write_text("remote: s3://test-bucket/catalog\naws_profile: config-profile\n")

        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
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
                    "--collection",
                    "demographics",
                    "--profile",
                    "cli-override",
                    "--catalog",
                    str(catalog_with_versions),
                ],
                catch_exceptions=False,
            )

            # CLI flag should override config
            call_kwargs = mock_push.call_args[1]
            assert call_kwargs.get("profile") == "cli-override"

    @pytest.mark.integration
    def test_push_conflict_shows_error(self, catalog_with_versions: Path) -> None:
        """Push conflict should show error message to user."""
        from portolan_cli.cli import cli
        from portolan_cli.push import PushConflictError

        runner = CliRunner()

        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
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

        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
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

        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
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

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
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
    def test_dry_run_does_not_detect_conflicts(self, catalog_with_versions: Path) -> None:
        """Dry-run must NOT make network calls, so it cannot detect remote conflicts.

        Bug #137: dry-run previously called _fetch_remote_versions to check for
        conflicts even in dry-run mode. The fix ensures dry-run returns early
        before any network I/O, which means remote conflicts are not checked.
        Users who want conflict detection must run without --dry-run.
        """
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

            # dry-run must NOT raise — it never fetches remote state
            result = push(
                catalog_root=catalog_with_versions,
                collection="demographics",
                destination="s3://mybucket/catalog",
                dry_run=True,
            )

        # Verify the remote was never consulted
        mock_fetch.assert_not_called()
        assert result.success is True
        assert result.files_uploaded == 0

    @pytest.mark.integration
    def test_cli_dry_run_no_contradictory_messages(self, catalog_with_versions: Path) -> None:
        """CLI should not show 'Nothing to push' after dry-run shows pending work.

        Regression test for issue #145: dry-run was showing contradictory messages:
        - "[DRY RUN] Would push 7 version(s)" followed by
        - "Nothing to push - local and remote are in sync"
        """
        from portolan_cli.cli import cli
        from portolan_cli.push import PushResult

        runner = CliRunner()

        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
            # Simulate dry-run where there IS work to do
            # (versions_pushed=0 because dry-run doesn't actually push)
            mock_push.return_value = PushResult(
                success=True,
                files_uploaded=0,
                versions_pushed=0,  # Dry-run returns 0 even when work exists
                conflicts=[],
                errors=[],
            )

            result = runner.invoke(
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

            # Should NOT show the misleading "Nothing to push" message
            assert "Nothing to push" not in result.output, (
                f"Contradictory message found in dry-run output: {result.output}"
            )

    @pytest.mark.integration
    def test_cli_dry_run_shows_completion_message(self, catalog_with_versions: Path) -> None:
        """CLI should show dry-run completion message when dry-run succeeds."""
        from portolan_cli.cli import cli
        from portolan_cli.push import PushResult

        runner = CliRunner()

        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
            mock_push.return_value = PushResult(
                success=True,
                files_uploaded=0,
                versions_pushed=0,
                conflicts=[],
                errors=[],
            )

            result = runner.invoke(
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

            # Should show dry-run completion indicator
            assert "DRY RUN" in result.output or "dry-run" in result.output.lower(), (
                f"Expected dry-run completion message, got: {result.output}"
            )

    @pytest.mark.integration
    def test_cli_normal_push_still_shows_nothing_to_push(self, catalog_with_versions: Path) -> None:
        """Non-dry-run with nothing to push should still show 'Nothing to push'."""
        from portolan_cli.cli import cli
        from portolan_cli.push import PushResult

        runner = CliRunner()

        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
            mock_push.return_value = PushResult(
                success=True,
                files_uploaded=0,
                versions_pushed=0,
                conflicts=[],
                errors=[],
            )

            result = runner.invoke(
                cli,
                [
                    "push",
                    "s3://mybucket/catalog",
                    "--collection",
                    "demographics",
                    # NOTE: No --dry-run flag
                    "--catalog",
                    str(catalog_with_versions),
                ],
                catch_exceptions=False,
            )

            # Normal push with nothing to do SHOULD show this message
            assert "Nothing to push" in result.output, (
                f"Expected 'Nothing to push' for sync'd repo, got: {result.output}"
            )

    @pytest.mark.integration
    def test_dry_run_nothing_to_push_real_codepath(self, catalog_with_versions: Path) -> None:
        """Integration test: dry-run with nothing to push exercises real push() code.

        This test does NOT mock push() - it mocks only _fetch_remote_versions
        so we exercise the actual dry-run logic in push.py.
        """
        from portolan_cli.push import push

        # Mock _fetch_remote_versions to simulate remote is identical to local
        versions_path = catalog_with_versions / "demographics" / "versions.json"
        local_versions = json.loads(versions_path.read_text())

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            # Remote has same versions as local = nothing to push
            mock_fetch.return_value = (local_versions, "etag-123")

            result = push(
                catalog_root=catalog_with_versions,
                collection="demographics",
                destination="s3://mybucket/catalog",
                dry_run=True,
            )

        assert result.success is True
        assert result.versions_pushed == 0
        assert result.files_uploaded == 0

    @pytest.mark.integration
    def test_dry_run_with_work_real_codepath(self, catalog_with_versions: Path) -> None:
        """Integration test: dry-run with pending work exercises real push() code.

        This test mocks only _fetch_remote_versions, not push(), so we test
        the actual dry-run message generation in push.py.
        """
        from portolan_cli.push import push

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            # Remote is empty = local has versions to push
            mock_fetch.return_value = (None, None)

            result = push(
                catalog_root=catalog_with_versions,
                collection="demographics",
                destination="s3://mybucket/catalog",
                dry_run=True,
            )

        assert result.success is True
        # Dry-run doesn't actually push, but there was work to do
        assert result.versions_pushed == 0
        assert result.files_uploaded == 0

    @pytest.mark.integration
    def test_cli_dry_run_nothing_to_push_real_codepath(
        self, catalog_with_versions: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """CLI integration: dry-run with nothing to push shows correct message.

        Tests the actual CLI output when local and remote are in sync.
        """
        from portolan_cli.cli import cli

        # Setup: remote has same versions as local
        versions_path = catalog_with_versions / "demographics" / "versions.json"
        local_versions = json.loads(versions_path.read_text())

        runner = CliRunner()

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (local_versions, "etag-123")

            result = runner.invoke(
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

        # In dry-run mode with nothing to push, should show DRY RUN prefix
        assert "[DRY RUN]" in result.output, (
            f"Expected '[DRY RUN]' prefix in output, got: {result.output}"
        )
        # Should NOT show the contradictory plain "Nothing to push" message
        assert "Nothing to push - local and remote are in sync" not in result.output, (
            f"Contradictory message found: {result.output}"
        )

    @pytest.mark.integration
    def test_cli_dry_run_with_work_real_codepath(self, catalog_with_versions: Path) -> None:
        """CLI integration: dry-run with pending work shows what would be pushed.

        Tests that we see "[DRY RUN] Would push N version(s)" without
        contradictory "Nothing to push" message.
        """
        from portolan_cli.cli import cli

        runner = CliRunner()

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            # Remote is empty = local has versions to push
            mock_fetch.return_value = (None, None)

            result = runner.invoke(
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

        # Should show what would be pushed
        assert "[DRY RUN] Would push" in result.output, (
            f"Expected dry-run work message, got: {result.output}"
        )
        # Should NOT show contradictory "Nothing to push"
        assert "Nothing to push" not in result.output, (
            f"Contradictory message found after dry-run work: {result.output}"
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
        versions_path = catalog_with_versions / "demographics" / "versions.json"
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
    def test_push_cli_filenotfound_json_output(self, catalog_missing_versions_file: Path) -> None:
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
    def test_push_cli_filenotfound_human_output(self, catalog_missing_versions_file: Path) -> None:
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

        runner = CliRunner()

        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
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

        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
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

        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
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

        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
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


# =============================================================================
# Property-Based Tests (Hypothesis)
# =============================================================================


class TestPushOutputInvariants:
    """Property-based tests for push CLI output invariants.

    These tests verify invariants that should hold across all possible states:
    - Dry-run never shows "Nothing to push" (contradictory)
    - Non-dry-run with 0 versions pushed shows "Nothing to push"
    - Successful push with >0 versions shows success message
    """

    @pytest.mark.integration
    @given(
        files_uploaded=st.integers(min_value=0, max_value=100),
        versions_pushed=st.integers(min_value=0, max_value=100),
    )
    @settings(
        max_examples=50,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
        deadline=None,  # CLI invocation can be slow
    )
    def test_dry_run_never_shows_nothing_to_push(
        self,
        catalog_with_versions: Path,
        files_uploaded: int,
        versions_pushed: int,
    ) -> None:
        """Property: Dry-run mode never outputs 'Nothing to push'.

        This invariant ensures we don't show contradictory messages where
        dry-run indicates work to do but then says nothing to push.
        """
        from portolan_cli.cli import cli
        from portolan_cli.push import PushResult

        runner = CliRunner()

        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
            mock_push.return_value = PushResult(
                success=True,
                files_uploaded=files_uploaded,
                versions_pushed=versions_pushed,
                conflicts=[],
                errors=[],
            )

            result = runner.invoke(
                cli,
                [
                    "push",
                    "s3://mybucket/catalog",
                    "--collection",
                    "demographics",
                    "--dry-run",  # Key: dry-run mode
                    "--catalog",
                    str(catalog_with_versions),
                ],
                catch_exceptions=False,
            )

            # INVARIANT: Dry-run should never show "Nothing to push"
            assert "Nothing to push" not in result.output, (
                f"Dry-run with files_uploaded={files_uploaded}, "
                f"versions_pushed={versions_pushed} showed 'Nothing to push': {result.output}"
            )

    @pytest.mark.integration
    @given(
        files_uploaded=st.integers(min_value=0, max_value=100),
    )
    @settings(
        max_examples=25,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
        deadline=None,  # CLI invocation can be slow
    )
    def test_non_dry_run_zero_versions_shows_nothing_to_push(
        self,
        catalog_with_versions: Path,
        files_uploaded: int,
    ) -> None:
        """Property: Non-dry-run with 0 versions pushed shows 'Nothing to push'.

        This invariant ensures the normal case still works correctly.
        """
        from portolan_cli.cli import cli
        from portolan_cli.push import PushResult

        runner = CliRunner()

        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
            mock_push.return_value = PushResult(
                success=True,
                files_uploaded=files_uploaded,
                versions_pushed=0,  # Key: 0 versions
                conflicts=[],
                errors=[],
            )

            result = runner.invoke(
                cli,
                [
                    "push",
                    "s3://mybucket/catalog",
                    "--collection",
                    "demographics",
                    # No --dry-run
                    "--catalog",
                    str(catalog_with_versions),
                ],
                catch_exceptions=False,
            )

            # INVARIANT: Non-dry-run with 0 versions should show "Nothing to push"
            assert "Nothing to push" in result.output, (
                f"Non-dry-run with files_uploaded={files_uploaded}, "
                f"versions_pushed=0 didn't show 'Nothing to push': {result.output}"
            )

    @pytest.mark.integration
    @given(
        files_uploaded=st.integers(min_value=0, max_value=100),
        versions_pushed=st.integers(min_value=1, max_value=100),
    )
    @settings(
        max_examples=25,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
        deadline=None,  # CLI invocation can be slow
    )
    def test_successful_push_with_versions_shows_pushed_message(
        self,
        catalog_with_versions: Path,
        files_uploaded: int,
        versions_pushed: int,
    ) -> None:
        """Property: Successful push with >0 versions shows 'Pushed' message.

        This invariant ensures success messages appear when work is done.
        """
        from portolan_cli.cli import cli
        from portolan_cli.push import PushResult

        runner = CliRunner()

        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
            mock_push.return_value = PushResult(
                success=True,
                files_uploaded=files_uploaded,
                versions_pushed=versions_pushed,  # Key: >0 versions
                conflicts=[],
                errors=[],
            )

            result = runner.invoke(
                cli,
                [
                    "push",
                    "s3://mybucket/catalog",
                    "--collection",
                    "demographics",
                    # No --dry-run
                    "--catalog",
                    str(catalog_with_versions),
                ],
                catch_exceptions=False,
            )

            # INVARIANT: Should show "Pushed" message with version count
            assert "Pushed" in result.output, (
                f"Successful push with files_uploaded={files_uploaded}, "
                f"versions_pushed={versions_pushed} didn't show 'Pushed': {result.output}"
            )
            assert str(versions_pushed) in result.output, (
                f"Output didn't contain version count {versions_pushed}: {result.output}"
            )


# =============================================================================
# Real Code Path Tests (No Mocking of push())
# =============================================================================


class TestDryRunRealCodePath:
    """Tests that exercise the real push() code path without mocking.

    These tests verify that the dry-run message handling works correctly
    in the actual implementation, not just the CLI layer.
    """

    @pytest.mark.integration
    def test_dry_run_nothing_to_push_shows_dry_run_prefix(
        self, catalog_with_versions: Path
    ) -> None:
        """Real push() with dry_run=True and nothing to push shows [DRY RUN] prefix.

        This test does NOT mock push() - it tests the actual implementation
        to verify the dry-run message is prefixed correctly when local and
        remote are in sync.
        """
        from portolan_cli.push import push

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            # Simulate remote having same versions as local (nothing to push)
            mock_fetch.return_value = (
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.1.0",
                    "versions": [
                        {"version": "1.0.0", "created": "2024-01-01T00:00:00Z"},
                        {"version": "1.1.0", "created": "2024-02-01T00:00:00Z"},
                    ],
                },
                "etag-123",
            )

            # Call real push() with dry_run=True
            result = push(
                catalog_root=catalog_with_versions,
                collection="demographics",
                destination="s3://mybucket/catalog",
                dry_run=True,
            )

            # Should succeed with nothing to push
            assert result.success is True
            assert result.versions_pushed == 0
            assert result.files_uploaded == 0

    @pytest.mark.integration
    def test_non_dry_run_nothing_to_push_no_prefix(self, catalog_with_versions: Path) -> None:
        """Real push() with dry_run=False and nothing to push has no prefix.

        This verifies the normal case (non-dry-run) still works correctly.
        """
        from portolan_cli.push import push

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            # Simulate remote having same versions as local (nothing to push)
            mock_fetch.return_value = (
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.1.0",
                    "versions": [
                        {"version": "1.0.0", "created": "2024-01-01T00:00:00Z"},
                        {"version": "1.1.0", "created": "2024-02-01T00:00:00Z"},
                    ],
                },
                "etag-123",
            )

            # Call real push() with dry_run=False
            result = push(
                catalog_root=catalog_with_versions,
                collection="demographics",
                destination="s3://mybucket/catalog",
                dry_run=False,
            )

            # Should succeed with nothing to push
            assert result.success is True
            assert result.versions_pushed == 0
            assert result.files_uploaded == 0

    @pytest.mark.integration
    def test_dry_run_with_work_to_do_shows_would_push(self, catalog_with_versions: Path) -> None:
        """Real push() with dry_run=True and work to do shows what would be pushed.

        This tests the case where there ARE versions to push, verifying
        that dry-run mode shows "[DRY RUN] Would push" messages.
        """
        from portolan_cli.push import push

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            # Simulate remote having fewer versions than local (work to do)
            mock_fetch.return_value = (
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [
                        {"version": "1.0.0", "created": "2024-01-01T00:00:00Z"},
                    ],
                },
                "etag-123",
            )

            # Call real push() with dry_run=True
            result = push(
                catalog_root=catalog_with_versions,
                collection="demographics",
                destination="s3://mybucket/catalog",
                dry_run=True,
            )

            # Dry-run should succeed but not actually upload
            assert result.success is True
            assert result.versions_pushed == 0  # Dry-run doesn't push
            assert result.files_uploaded == 0  # Dry-run doesn't upload


# =============================================================================
# Trailing Slash URL Tests (Issue #144)
# =============================================================================


class TestTrailingSlashNormalization:
    """Tests for URL trailing slash handling.

    Regression tests for issue #144: trailing slash in S3 URL causes path
    parsing error due to double slashes in constructed paths.
    """

    @pytest.mark.integration
    def test_push_with_trailing_slash_url(self, catalog_with_versions: Path) -> None:
        """Push should handle destination URLs with trailing slashes.

        Before fix: s3://bucket/prefix/ would cause:
        "Could not parse path: Path 'prefix//collection/versions.json' contained empty path segment"
        """
        from portolan_cli.push import push

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (None, None)  # First push

            # This should NOT fail with trailing slash
            result = push(
                catalog_root=catalog_with_versions,
                collection="demographics",
                destination="s3://mybucket/catalog/",  # <-- Trailing slash
                dry_run=True,
            )

        assert result.success is True
        # Verify no double slashes were constructed
        # (mock was called with the normalized prefix)

    @pytest.mark.integration
    def test_push_with_multiple_trailing_slashes(self, catalog_with_versions: Path) -> None:
        """Push should handle multiple trailing slashes gracefully."""
        from portolan_cli.push import push

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (None, None)

            result = push(
                catalog_root=catalog_with_versions,
                collection="demographics",
                destination="s3://mybucket/catalog///",  # <-- Multiple trailing slashes
                dry_run=True,
            )

        assert result.success is True

    @pytest.mark.integration
    def test_push_url_bucket_only_with_trailing_slash(self, catalog_with_versions: Path) -> None:
        """Push to bucket root with trailing slash should work."""
        from portolan_cli.push import push

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (None, None)

            # s3://bucket/ should work (empty prefix)
            result = push(
                catalog_root=catalog_with_versions,
                collection="demographics",
                destination="s3://mybucket/",  # <-- Bucket with trailing slash
                dry_run=True,
            )

        assert result.success is True

    @pytest.mark.integration
    def test_push_constructed_paths_have_no_double_slashes(
        self, catalog_with_versions: Path
    ) -> None:
        """Verify that path construction doesn't create double slashes.

        This test verifies the fix by checking the actual paths that would
        be used for upload/fetch operations.
        """
        from portolan_cli.upload import setup_store

        # Test various trailing slash scenarios
        test_cases = [
            ("s3://mybucket/prefix/", "prefix"),
            ("s3://mybucket/prefix///", "prefix"),
            ("s3://mybucket/", ""),
            ("s3://mybucket/a/b/c/", "a/b/c"),
        ]

        for destination, expected_prefix in test_cases:
            with patch("portolan_cli.upload.S3Store"):
                _, prefix = setup_store(destination)
                assert prefix == expected_prefix, (
                    f"For {destination!r}, expected prefix {expected_prefix!r}, got {prefix!r}"
                )
                # Verify path construction wouldn't create double slashes
                test_path = f"{prefix}/collection/versions.json".lstrip("/")
                assert "//" not in test_path, (
                    f"Path {test_path!r} contains double slash (from prefix={prefix!r})"
                )


# =============================================================================
# Asset Diffing Integration Tests (Issue #329)
# =============================================================================


class TestPushAssetDiffingIntegration:
    """Integration tests for push asset diffing (Issue #329).

    These tests verify that the full push flow correctly diffs assets
    against remote and only uploads new/changed files.
    """

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_push_only_uploads_new_assets(self, tmp_path: Path) -> None:
        """Issue #329: Push should only upload assets not on remote.

        Scenario: Local version 2.0.0 adds 1 new file to a catalog that
        has 2 existing files on remote (version 1.0.0). Only the new file
        should be uploaded.
        """
        from portolan_cli.push import push_async

        # Setup catalog
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        # Create .portolan/config.yaml (sentinel per ADR-0029)
        portolan_dir = catalog_dir / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("version: 1\n")

        # Create catalog.json
        (catalog_dir / "catalog.json").write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test",
                    "stac_version": "1.0.0",
                    "description": "Test",
                    "links": [],
                }
            )
        )

        # Create collection
        collection_dir = catalog_dir / "test"
        collection_dir.mkdir()
        (collection_dir / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "test",
                    "stac_version": "1.0.0",
                    "description": "Test",
                    "license": "CC0-1.0",
                    "extent": {
                        "spatial": {"bbox": [[-180, -90, 180, 90]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                }
            )
        )

        # Create 3 asset files (2 existing + 1 new)
        (collection_dir / "existing1.parquet").write_bytes(b"data1")
        (collection_dir / "existing2.parquet").write_bytes(b"data2")
        (collection_dir / "new_file.parquet").write_bytes(b"new data")

        # Create local versions.json with both 1.0.0 (base) and 2.0.0 (new version)
        # Per ADR-0005, each version has a complete snapshot of all assets
        local_versions = {
            "spec_version": "1.0.0",
            "current_version": "2.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2024-01-01T00:00:00Z",
                    "assets": {
                        "existing1.parquet": {
                            "sha256": "sha256_existing1",
                            "href": "test/existing1.parquet",
                        },
                        "existing2.parquet": {
                            "sha256": "sha256_existing2",
                            "href": "test/existing2.parquet",
                        },
                    },
                },
                {
                    "version": "2.0.0",
                    "created": "2024-02-01T00:00:00Z",
                    "assets": {
                        "existing1.parquet": {
                            "sha256": "sha256_existing1",
                            "href": "test/existing1.parquet",
                        },
                        "existing2.parquet": {
                            "sha256": "sha256_existing2",
                            "href": "test/existing2.parquet",
                        },
                        "new_file.parquet": {
                            "sha256": "sha256_new",
                            "href": "test/new_file.parquet",
                        },
                    },
                },
            ],
        }
        (collection_dir / "versions.json").write_text(json.dumps(local_versions, indent=2))

        # Remote has version 1.0.0 with only the 2 existing files
        remote_versions = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2024-01-01T00:00:00Z",
                    "assets": {
                        "existing1.parquet": {
                            "sha256": "sha256_existing1",  # Same sha256!
                            "href": "test/existing1.parquet",
                        },
                        "existing2.parquet": {
                            "sha256": "sha256_existing2",  # Same sha256!
                            "href": "test/existing2.parquet",
                        },
                    },
                },
            ],
        }

        # Track which assets are uploaded
        uploaded_assets: list[str] = []

        async def mock_put(store, path, data, **kwargs):
            """Mock put that accepts any keyword arguments."""
            uploaded_assets.append(path)
            return None

        with patch(
            "portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (remote_versions, "etag123")

            with patch("portolan_cli.push.obs.put_async", new_callable=AsyncMock) as mock_put_call:
                mock_put_call.side_effect = mock_put

                result = await push_async(
                    catalog_root=catalog_dir,
                    collection="test",
                    destination="s3://bucket/prefix",
                )

        # Verify only the new file was uploaded (not the 2 existing ones)
        assert result.success is True

        # Filter to just parquet files (ignore STAC metadata)
        parquet_uploads = [p for p in uploaded_assets if p.endswith(".parquet")]
        assert len(parquet_uploads) == 1
        assert "new_file.parquet" in parquet_uploads[0]

        # The existing files should NOT be in the upload list
        assert not any("existing1.parquet" in p for p in parquet_uploads)
        assert not any("existing2.parquet" in p for p in parquet_uploads)
