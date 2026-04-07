"""Tests for push command using configured remote as default destination.

GitHub Issue #143: portolan push should use configured remote as default.

These tests verify:
1. Push uses configured remote when no destination provided
2. Explicit destination overrides configured remote
3. Clear error message when neither provided
4. Config hierarchy (CLI > env > collection > catalog) is respected
5. Pull command parity (if pull has similar behavior)

Following TDD: tests written FIRST before implementation.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from click.testing import CliRunner
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from portolan_cli.cli import cli

# =============================================================================
# Test fixtures
# =============================================================================


@pytest.fixture
def runner() -> CliRunner:
    """Create a Click test runner."""
    return CliRunner()


@pytest.fixture
def catalog_with_remote_config(tmp_path: Path) -> Path:
    """Create a catalog with remote configured in config.yaml."""
    catalog_dir = tmp_path / "catalog"
    catalog_dir.mkdir()

    # Create catalog.json
    (catalog_dir / "catalog.json").write_text(
        json.dumps({"type": "Catalog", "id": "test-catalog", "description": "Test"})
    )

    # Create .portolan directory with config
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()

    config = {"remote": "s3://configured-bucket/catalog"}
    (portolan_dir / "config.yaml").write_text(f"remote: {config['remote']}\n")

    # Create a test collection with versions.json
    collection_dir = catalog_dir / "test-collection"
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
                "assets": {},
                "changes": [],
            }
        ],
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions_data))

    return catalog_dir


@pytest.fixture
def catalog_without_remote_config(tmp_path: Path) -> Path:
    """Create a catalog without remote configured."""
    catalog_dir = tmp_path / "catalog"
    catalog_dir.mkdir()

    # Create catalog.json
    (catalog_dir / "catalog.json").write_text(
        json.dumps({"type": "Catalog", "id": "test-catalog", "description": "Test"})
    )

    # Create .portolan directory without remote
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("# No remote configured\n")

    # Create a test collection
    collection_dir = catalog_dir / "test-collection"
    collection_dir.mkdir()

    versions_data = {
        "spec_version": "1.0.0",
        "current_version": "1.0.0",
        "versions": [],
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions_data))

    return catalog_dir


# =============================================================================
# Unit tests - Push uses configured remote
# =============================================================================


class TestPushDefaultRemote:
    """Tests for push using configured remote as default destination."""

    @pytest.mark.unit
    def test_push_uses_configured_remote_when_no_destination(
        self, runner: CliRunner, catalog_with_remote_config: Path
    ) -> None:
        """Push should use configured remote when no DESTINATION provided."""
        from portolan_cli.push import PushResult

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
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(catalog_with_remote_config),
                ],
                catch_exceptions=False,
            )

            assert result.exit_code == 0, f"Failed: {result.output}"
            # Verify the configured remote was used
            call_kwargs = mock_push.call_args[1]
            assert call_kwargs["destination"] == "s3://configured-bucket/catalog"

    @pytest.mark.unit
    def test_push_explicit_destination_overrides_config(
        self, runner: CliRunner, catalog_with_remote_config: Path
    ) -> None:
        """Explicit DESTINATION argument should override configured remote."""
        from portolan_cli.push import PushResult

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
                    "s3://explicit-bucket/path",  # Explicit destination
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(catalog_with_remote_config),
                ],
                catch_exceptions=False,
            )

            assert result.exit_code == 0, f"Failed: {result.output}"
            call_kwargs = mock_push.call_args[1]
            assert call_kwargs["destination"] == "s3://explicit-bucket/path"

    @pytest.mark.unit
    def test_push_error_when_no_destination_and_no_config(
        self, runner: CliRunner, catalog_without_remote_config: Path
    ) -> None:
        """Push should fail with clear error when no destination and no remote configured."""
        result = runner.invoke(
            cli,
            [
                "push",
                "--collection",
                "test-collection",
                "--catalog",
                str(catalog_without_remote_config),
            ],
        )

        assert result.exit_code != 0
        # Should have a clear error message about missing destination
        assert "destination" in result.output.lower() or "remote" in result.output.lower()

    @pytest.mark.unit
    def test_push_help_shows_destination_is_optional(self, runner: CliRunner) -> None:
        """Push --help should indicate DESTINATION is optional when remote is configured."""
        result = runner.invoke(cli, ["push", "--help"])

        assert result.exit_code == 0
        # The help text should indicate the argument exists
        # After implementation, it should show it's optional
        assert "DESTINATION" in result.output or "destination" in result.output.lower()


class TestPushRemoteConfigPrecedence:
    """Tests for config precedence when resolving remote."""

    @pytest.mark.unit
    def test_env_var_overrides_config_file(
        self, runner: CliRunner, catalog_with_remote_config: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """PORTOLAN_REMOTE env var should override config file."""
        from portolan_cli.push import PushResult

        monkeypatch.setenv("PORTOLAN_REMOTE", "s3://env-bucket/catalog")

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
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(catalog_with_remote_config),
                ],
                catch_exceptions=False,
            )

            assert result.exit_code == 0, f"Failed: {result.output}"
            call_kwargs = mock_push.call_args[1]
            assert call_kwargs["destination"] == "s3://env-bucket/catalog"

    @pytest.mark.unit
    def test_explicit_arg_overrides_env_var(
        self, runner: CliRunner, catalog_with_remote_config: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Explicit DESTINATION should override PORTOLAN_REMOTE env var."""
        from portolan_cli.push import PushResult

        monkeypatch.setenv("PORTOLAN_REMOTE", "s3://env-bucket/catalog")

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
                    "s3://explicit-bucket/catalog",  # CLI arg wins
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(catalog_with_remote_config),
                ],
                catch_exceptions=False,
            )

            assert result.exit_code == 0, f"Failed: {result.output}"
            call_kwargs = mock_push.call_args[1]
            assert call_kwargs["destination"] == "s3://explicit-bucket/catalog"


class TestPushDefaultRemoteJSONOutput:
    """Tests for JSON output mode with default remote."""

    @pytest.mark.unit
    def test_push_json_output_uses_configured_remote(
        self, runner: CliRunner, catalog_with_remote_config: Path
    ) -> None:
        """Push with --json should work with configured remote."""
        from portolan_cli.push import PushResult

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
                    "--format",
                    "json",
                    "push",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(catalog_with_remote_config),
                ],
                catch_exceptions=False,
            )

            assert result.exit_code == 0, f"Failed: {result.output}"
            output = json.loads(result.output)
            assert output["success"] is True

    @pytest.mark.unit
    def test_push_json_error_when_no_remote(
        self, runner: CliRunner, catalog_without_remote_config: Path
    ) -> None:
        """Push with --json should return error envelope when no remote configured."""
        result = runner.invoke(
            cli,
            [
                "--format",
                "json",
                "push",
                "--collection",
                "test-collection",
                "--catalog",
                str(catalog_without_remote_config),
            ],
        )

        assert result.exit_code != 0
        output = json.loads(result.output)
        assert output["success"] is False
        # Error should mention missing destination/remote
        error_messages = [e.get("message", "") for e in output.get("errors", [])]
        assert any(
            "destination" in msg.lower() or "remote" in msg.lower() for msg in error_messages
        )


class TestPushDefaultRemoteDryRun:
    """Tests for dry-run mode with default remote."""

    @pytest.mark.unit
    def test_push_dry_run_with_configured_remote(
        self, runner: CliRunner, catalog_with_remote_config: Path
    ) -> None:
        """Push --dry-run should work with configured remote."""
        from portolan_cli.push import PushResult

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
                    "--collection",
                    "test-collection",
                    "--dry-run",
                    "--catalog",
                    str(catalog_with_remote_config),
                ],
                catch_exceptions=False,
            )

            assert result.exit_code == 0, f"Failed: {result.output}"
            call_kwargs = mock_push.call_args[1]
            assert call_kwargs["dry_run"] is True
            assert call_kwargs["destination"] == "s3://configured-bucket/catalog"


# =============================================================================
# Property-based tests (Hypothesis)
# =============================================================================


class TestPushDefaultRemoteInvariants:
    """Property-based tests for push default remote behavior."""

    @pytest.mark.unit
    @given(
        remote_url=st.from_regex(r"s3://[a-z][a-z0-9-]{2,20}/[a-z][a-z0-9-/]{2,30}", fullmatch=True)
    )
    @settings(max_examples=20, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_configured_remote_used_when_no_arg(self, remote_url: str, tmp_path: Path) -> None:
        """Any valid remote URL in config should be used when no arg provided."""
        from portolan_cli.push import PushResult

        # Set up catalog with this remote
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir(exist_ok=True)

        (catalog_dir / "catalog.json").write_text(
            json.dumps({"type": "Catalog", "id": "test", "description": "Test"})
        )

        portolan_dir = catalog_dir / ".portolan"
        portolan_dir.mkdir(exist_ok=True)
        (portolan_dir / "config.yaml").write_text(f"remote: {remote_url}\n")

        collection_dir = catalog_dir / "test"
        collection_dir.mkdir(exist_ok=True)
        (collection_dir / "versions.json").write_text(
            json.dumps({"spec_version": "1.0.0", "current_version": "1.0.0", "versions": []})
        )

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
                ["push", "--collection", "test", "--catalog", str(catalog_dir)],
                catch_exceptions=False,
            )

            assert result.exit_code == 0, f"Failed for {remote_url}: {result.output}"
            call_kwargs = mock_push.call_args[1]
            assert call_kwargs["destination"] == remote_url

    @pytest.mark.unit
    @given(
        explicit_url=st.from_regex(
            r"s3://[a-z][a-z0-9-]{2,20}/explicit-[a-z0-9]{2,10}", fullmatch=True
        ),
        config_url=st.from_regex(
            r"s3://[a-z][a-z0-9-]{2,20}/config-[a-z0-9]{2,10}", fullmatch=True
        ),
    )
    @settings(max_examples=20, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_explicit_always_overrides_config(
        self, explicit_url: str, config_url: str, tmp_path: Path
    ) -> None:
        """Explicit destination should always override config, regardless of values."""
        from portolan_cli.push import PushResult

        # Set up catalog with config remote
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir(exist_ok=True)

        (catalog_dir / "catalog.json").write_text(
            json.dumps({"type": "Catalog", "id": "test", "description": "Test"})
        )

        portolan_dir = catalog_dir / ".portolan"
        portolan_dir.mkdir(exist_ok=True)
        (portolan_dir / "config.yaml").write_text(f"remote: {config_url}\n")

        collection_dir = catalog_dir / "test"
        collection_dir.mkdir(exist_ok=True)
        (collection_dir / "versions.json").write_text(
            json.dumps({"spec_version": "1.0.0", "current_version": "1.0.0", "versions": []})
        )

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
                    explicit_url,  # Explicit should win
                    "--collection",
                    "test",
                    "--catalog",
                    str(catalog_dir),
                ],
                catch_exceptions=False,
            )

            assert result.exit_code == 0
            call_kwargs = mock_push.call_args[1]
            assert call_kwargs["destination"] == explicit_url
            assert call_kwargs["destination"] != config_url or explicit_url == config_url


# =============================================================================
# Integration tests - Full workflow
# =============================================================================


class TestPushDefaultRemoteIntegration:
    """Integration tests for push with default remote."""

    @pytest.mark.integration
    def test_push_workflow_with_config_set(self, runner: CliRunner, tmp_path: Path) -> None:
        """Full workflow: init -> config set remote -> push (no destination)."""
        from portolan_cli.push import PushResult

        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Initialize catalog
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0, f"Init failed: {result.output}"

            # Set remote config
            result = runner.invoke(cli, ["config", "set", "remote", "s3://workflow-bucket/catalog"])
            assert result.exit_code == 0, f"Config set failed: {result.output}"

            # Create a minimal collection for push
            Path("test-collection").mkdir()
            Path("test-collection/versions.json").write_text(
                json.dumps(
                    {
                        "spec_version": "1.0.0",
                        "current_version": "1.0.0",
                        "versions": [],
                    }
                )
            )

            # Push without destination - should use config
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
                    ["push", "--collection", "test-collection"],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0, f"Push failed: {result.output}"
                call_kwargs = mock_push.call_args[1]
                assert call_kwargs["destination"] == "s3://workflow-bucket/catalog"

    @pytest.mark.integration
    def test_push_error_message_is_actionable(
        self, runner: CliRunner, catalog_without_remote_config: Path
    ) -> None:
        """Error message should tell user how to fix the issue."""
        result = runner.invoke(
            cli,
            [
                "push",
                "--collection",
                "test-collection",
                "--catalog",
                str(catalog_without_remote_config),
            ],
        )

        assert result.exit_code != 0
        # Error should be actionable - mention how to fix
        output_lower = result.output.lower()
        assert (
            "config set remote" in output_lower
            or "portolan config" in output_lower
            or "destination" in output_lower
            or "provide" in output_lower
        )


# =============================================================================
# Sync command - Same default remote behavior
# =============================================================================


class TestSyncDefaultRemote:
    """Tests for sync using configured remote as default destination.

    The sync command should have the same default remote behavior as push,
    since sync is essentially pull + push.
    """

    @pytest.mark.unit
    def test_sync_uses_configured_remote_when_no_destination(
        self, runner: CliRunner, catalog_with_remote_config: Path
    ) -> None:
        """Sync should use configured remote when no DESTINATION provided."""
        from portolan_cli.sync import SyncResult

        with patch("portolan_cli.sync.sync") as mock_sync:
            mock_sync.return_value = SyncResult(
                success=True,
                init_performed=False,
                pull_result=None,
                scan_result=None,
                check_result=None,
                push_result=None,
                errors=[],
            )

            result = runner.invoke(
                cli,
                [
                    "sync",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(catalog_with_remote_config),
                ],
                catch_exceptions=False,
            )

            assert result.exit_code == 0, f"Failed: {result.output}"
            call_kwargs = mock_sync.call_args[1]
            assert call_kwargs["destination"] == "s3://configured-bucket/catalog"

    @pytest.mark.unit
    def test_sync_explicit_destination_overrides_config(
        self, runner: CliRunner, catalog_with_remote_config: Path
    ) -> None:
        """Explicit DESTINATION argument should override configured remote."""
        from portolan_cli.sync import SyncResult

        with patch("portolan_cli.sync.sync") as mock_sync:
            mock_sync.return_value = SyncResult(
                success=True,
                init_performed=False,
                pull_result=None,
                scan_result=None,
                check_result=None,
                push_result=None,
                errors=[],
            )

            result = runner.invoke(
                cli,
                [
                    "sync",
                    "s3://explicit-bucket/path",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(catalog_with_remote_config),
                ],
                catch_exceptions=False,
            )

            assert result.exit_code == 0, f"Failed: {result.output}"
            call_kwargs = mock_sync.call_args[1]
            assert call_kwargs["destination"] == "s3://explicit-bucket/path"

    @pytest.mark.unit
    def test_sync_error_when_no_destination_and_no_config(
        self, runner: CliRunner, catalog_without_remote_config: Path
    ) -> None:
        """Sync should fail with clear error when no destination and no remote configured."""
        result = runner.invoke(
            cli,
            [
                "sync",
                "--collection",
                "test-collection",
                "--catalog",
                str(catalog_without_remote_config),
            ],
        )

        assert result.exit_code != 0
        assert "destination" in result.output.lower() or "remote" in result.output.lower()


class TestSyncDefaultRemoteJSONOutput:
    """Tests for JSON output mode with default remote for sync command."""

    @pytest.mark.unit
    def test_sync_json_output_uses_configured_remote(
        self, runner: CliRunner, catalog_with_remote_config: Path
    ) -> None:
        """Sync with --json should work with configured remote."""
        from portolan_cli.sync import SyncResult

        with patch("portolan_cli.sync.sync") as mock_sync:
            mock_sync.return_value = SyncResult(
                success=True,
                init_performed=False,
                pull_result=None,
                scan_result=None,
                check_result=None,
                push_result=None,
                errors=[],
            )

            result = runner.invoke(
                cli,
                [
                    "--format",
                    "json",
                    "sync",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(catalog_with_remote_config),
                ],
                catch_exceptions=False,
            )

            assert result.exit_code == 0, f"Failed: {result.output}"
            output = json.loads(result.output)
            assert output["success"] is True

    @pytest.mark.unit
    def test_sync_json_error_when_no_remote(
        self, runner: CliRunner, catalog_without_remote_config: Path
    ) -> None:
        """Sync with --json should return error envelope when no remote configured."""
        result = runner.invoke(
            cli,
            [
                "--format",
                "json",
                "sync",
                "--collection",
                "test-collection",
                "--catalog",
                str(catalog_without_remote_config),
            ],
        )

        assert result.exit_code != 0
        output = json.loads(result.output)
        assert output["success"] is False
        # Error should mention missing destination/remote
        error_messages = [e.get("message", "") for e in output.get("errors", [])]
        assert any(
            "destination" in msg.lower() or "remote" in msg.lower() for msg in error_messages
        )
