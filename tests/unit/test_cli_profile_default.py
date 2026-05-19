"""Tests for CLI --profile flag default behavior.

Tests verify:
1. Default fallback to 'default' profile when no config or CLI arg
2. Environment variable PORTOLAN_AWS_PROFILE is used
3. CLI --profile overrides env var values

Note: aws_profile is a sensitive setting and cannot be set in config.yaml (Issue #356).
Use PORTOLAN_PROFILE or PORTOLAN_AWS_PROFILE env vars instead.
"""

import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

# Remote URL for tests - set via env var (Issue #356: sensitive settings)
TEST_REMOTE = "s3://test-bucket/test-catalog"


class TestProfileDefaultBehavior:
    """Test that --profile defaults to 'default' for S3 commands."""

    @pytest.fixture
    def mock_catalog(self, tmp_path: Path) -> Path:
        """Create a minimal catalog structure for testing.

        Note: remote must be set via PORTOLAN_REMOTE env var (Issue #356).
        """
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / "catalog.json").write_text('{"type": "Catalog", "id": "test"}')

        collection_dir = catalog_root / "test-collection"
        collection_dir.mkdir()
        (collection_dir / "collection.json").write_text(
            '{"type": "Collection", "id": "test-collection"}'
        )
        (collection_dir / "versions.json").write_text('{"spec_version": "1.0.0", "versions": []}')

        portolan_dir = catalog_root / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# No sensitive settings\n")

        return catalog_root

    @pytest.mark.unit
    def test_push_defaults_to_default_profile(self, mock_catalog: Path) -> None:
        """portolan push should use 'default' profile when --profile not specified."""
        runner = CliRunner()

        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
            mock_push.return_value = MagicMock(
                success=True,
                files_uploaded=0,
                versions_pushed=0,
                conflicts=[],
                errors=[],
            )

            # Clear profile env vars to ensure default is used
            with patch.dict(
                os.environ,
                {
                    "PORTOLAN_REMOTE": TEST_REMOTE,
                    "PORTOLAN_AWS_PROFILE": "",
                    "PORTOLAN_PROFILE": "",
                },
                clear=False,
            ):
                runner.invoke(
                    cli,
                    ["push", "--collection", "test-collection", "--catalog", str(mock_catalog)],
                )

            # Verify push was called with profile="default"
            assert mock_push.called
            call_kwargs = mock_push.call_args.kwargs
            assert call_kwargs["profile"] == "default"

    @pytest.mark.unit
    def test_push_respects_explicit_profile(self, mock_catalog: Path) -> None:
        """portolan push should use explicit --profile when provided."""
        runner = CliRunner()

        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
            mock_push.return_value = MagicMock(
                success=True,
                files_uploaded=0,
                versions_pushed=0,
                conflicts=[],
                errors=[],
            )

            with patch.dict(os.environ, {"PORTOLAN_REMOTE": TEST_REMOTE}):
                runner.invoke(
                    cli,
                    [
                        "push",
                        "--collection",
                        "test-collection",
                        "--profile",
                        "custom-profile",
                        "--catalog",
                        str(mock_catalog),
                    ],
                )

            # Verify push was called with profile="custom-profile"
            assert mock_push.called
            call_kwargs = mock_push.call_args.kwargs
            assert call_kwargs["profile"] == "custom-profile"

    @pytest.mark.unit
    def test_pull_defaults_to_default_profile(self, mock_catalog: Path) -> None:
        """portolan pull should use 'default' profile when --profile not specified."""
        from portolan_cli.pull import PullResult

        runner = CliRunner()

        with patch("portolan_cli.pull.pull") as mock_pull:
            mock_pull.return_value = PullResult(
                success=True,
                files_downloaded=0,
                files_skipped=0,
                local_version="1.0.0",
                remote_version="1.0.0",
            )

            runner.invoke(
                cli,
                [
                    "pull",
                    "s3://test-bucket/catalog",
                    "--collection",
                    "test-collection",
                    "--catalog",
                    str(mock_catalog),
                ],
            )

            # Verify pull was called with profile="default"
            assert mock_pull.called
            call_kwargs = mock_pull.call_args.kwargs
            assert call_kwargs["profile"] == "default"

    @pytest.mark.unit
    def test_sync_defaults_to_default_profile(self, mock_catalog: Path) -> None:
        """portolan sync should use 'default' profile when --profile not specified."""
        runner = CliRunner()

        with patch("portolan_cli.sync.sync") as mock_sync:
            mock_sync.return_value = MagicMock(
                success=True,
                files_pulled=0,
                files_pushed=0,
                errors=[],
            )

            with patch.dict(os.environ, {"PORTOLAN_REMOTE": TEST_REMOTE}):
                runner.invoke(
                    cli,
                    ["sync", "--collection", "test-collection", "--catalog", str(mock_catalog)],
                )

            # Verify sync was called with profile="default"
            assert mock_sync.called
            call_kwargs = mock_sync.call_args.kwargs
            assert call_kwargs["profile"] == "default"

    @pytest.mark.unit
    def test_clone_defaults_to_default_profile(self, tmp_path: Path) -> None:
        """portolan clone should use 'default' profile when --profile not specified."""
        runner = CliRunner()
        clone_target = tmp_path / "cloned-catalog"

        with patch("portolan_cli.sync.clone") as mock_clone:
            mock_clone.return_value = MagicMock(
                success=True,
                local_path=clone_target,
                collections_cloned=["test-collection"],
                total_files_downloaded=0,
                errors=[],
                pull_result=None,
            )

            with runner.isolated_filesystem(temp_dir=tmp_path):
                result = runner.invoke(
                    cli,
                    ["clone", "s3://test-bucket/test-catalog", str(clone_target)],
                )

            # Verify command succeeded
            assert result.exit_code == 0

            # Verify clone was called with profile="default"
            assert mock_clone.called
            call_kwargs = mock_clone.call_args.kwargs
            assert call_kwargs["profile"] == "default"


class TestProfileConfigResolution:
    """Test that PORTOLAN_AWS_PROFILE env var is used when --profile not specified.

    Note: aws_profile is a sensitive setting and cannot be set in config.yaml (Issue #356).
    These tests verify env var behavior instead.
    """

    @pytest.fixture
    def basic_catalog(self, tmp_path: Path) -> Path:
        """Create a basic catalog without profile configured.

        Note: remote and profile must be set via env vars (Issue #356).
        """
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / "catalog.json").write_text('{"type": "Catalog", "id": "test"}')

        collection_dir = catalog_root / "test-collection"
        collection_dir.mkdir()
        (collection_dir / "collection.json").write_text(
            '{"type": "Collection", "id": "test-collection"}'
        )
        (collection_dir / "versions.json").write_text('{"spec_version": "1.0.0", "versions": []}')

        portolan_dir = catalog_root / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# No sensitive settings\n")

        return catalog_root

    @pytest.mark.unit
    def test_push_uses_aws_profile_from_env(self, basic_catalog: Path) -> None:
        """portolan push should use PORTOLAN_AWS_PROFILE env var when --profile not specified."""
        runner = CliRunner()

        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
            mock_push.return_value = MagicMock(
                success=True,
                files_uploaded=0,
                versions_pushed=0,
                conflicts=[],
                errors=[],
            )

            with patch.dict(
                os.environ,
                {"PORTOLAN_REMOTE": TEST_REMOTE, "PORTOLAN_AWS_PROFILE": "env-profile"},
            ):
                runner.invoke(
                    cli,
                    ["push", "--collection", "test-collection", "--catalog", str(basic_catalog)],
                )

            # Verify push was called with profile from env
            assert mock_push.called
            call_kwargs = mock_push.call_args.kwargs
            assert call_kwargs["profile"] == "env-profile"

    @pytest.mark.unit
    def test_push_cli_profile_overrides_env(self, basic_catalog: Path) -> None:
        """CLI --profile should override PORTOLAN_AWS_PROFILE env var."""
        runner = CliRunner()

        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
            mock_push.return_value = MagicMock(
                success=True,
                files_uploaded=0,
                versions_pushed=0,
                conflicts=[],
                errors=[],
            )

            with patch.dict(
                os.environ,
                {"PORTOLAN_REMOTE": TEST_REMOTE, "PORTOLAN_AWS_PROFILE": "env-profile"},
            ):
                runner.invoke(
                    cli,
                    [
                        "push",
                        "--collection",
                        "test-collection",
                        "--profile",
                        "cli-override",
                        "--catalog",
                        str(basic_catalog),
                    ],
                )

            # CLI profile should win
            assert mock_push.called
            call_kwargs = mock_push.call_args.kwargs
            assert call_kwargs["profile"] == "cli-override"

    @pytest.mark.unit
    def test_pull_uses_aws_profile_from_env(self, basic_catalog: Path) -> None:
        """portolan pull should use PORTOLAN_AWS_PROFILE env var when --profile not specified."""
        from portolan_cli.pull import PullResult

        runner = CliRunner()

        with patch("portolan_cli.pull.pull") as mock_pull:
            mock_pull.return_value = PullResult(
                success=True,
                files_downloaded=0,
                files_skipped=0,
                local_version="1.0.0",
                remote_version="1.0.0",
            )

            with patch.dict(os.environ, {"PORTOLAN_AWS_PROFILE": "env-profile"}):
                runner.invoke(
                    cli,
                    [
                        "pull",
                        "s3://test-bucket/catalog",
                        "--collection",
                        "test-collection",
                        "--catalog",
                        str(basic_catalog),
                    ],
                )

            # Verify pull was called with profile from env
            assert mock_pull.called
            call_kwargs = mock_pull.call_args.kwargs
            assert call_kwargs["profile"] == "env-profile"

    @pytest.mark.unit
    def test_sync_uses_aws_profile_from_env(self, basic_catalog: Path) -> None:
        """portolan sync should use PORTOLAN_AWS_PROFILE env var when --profile not specified."""
        runner = CliRunner()

        with patch("portolan_cli.sync.sync") as mock_sync:
            mock_sync.return_value = MagicMock(
                success=True,
                files_pulled=0,
                files_pushed=0,
                errors=[],
            )

            with patch.dict(
                os.environ,
                {"PORTOLAN_REMOTE": TEST_REMOTE, "PORTOLAN_AWS_PROFILE": "env-profile"},
            ):
                runner.invoke(
                    cli,
                    ["sync", "--collection", "test-collection", "--catalog", str(basic_catalog)],
                )

            # Verify sync was called with profile from env
            assert mock_sync.called
            call_kwargs = mock_sync.call_args.kwargs
            assert call_kwargs["profile"] == "env-profile"

    @pytest.mark.unit
    def test_clone_uses_aws_profile_from_env(self, tmp_path: Path) -> None:
        """portolan clone should use PORTOLAN_AWS_PROFILE env var when --profile not specified."""
        runner = CliRunner()
        clone_target = tmp_path / "cloned-catalog"

        with patch("portolan_cli.sync.clone") as mock_clone:
            mock_clone.return_value = MagicMock(
                success=True,
                local_path=clone_target,
                collections_cloned=["test-collection"],
                total_files_downloaded=0,
                errors=[],
                pull_result=None,
            )

            # Clone uses env var since there's no local catalog to read config from
            with patch.dict(os.environ, {"PORTOLAN_AWS_PROFILE": "env-profile"}):
                with runner.isolated_filesystem(temp_dir=tmp_path):
                    result = runner.invoke(
                        cli,
                        ["clone", "s3://test-bucket/test-catalog", str(clone_target)],
                    )

                # Verify command succeeded
                assert result.exit_code == 0

                # Verify clone was called with profile from env
                assert mock_clone.called
                call_kwargs = mock_clone.call_args.kwargs
                assert call_kwargs["profile"] == "env-profile"

    @pytest.mark.unit
    def test_push_uses_env_var_when_no_cli(self, tmp_path: Path) -> None:
        """PORTOLAN_AWS_PROFILE env var should be used when no CLI arg."""
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / "catalog.json").write_text('{"type": "Catalog", "id": "test"}')

        collection_dir = catalog_root / "test-collection"
        collection_dir.mkdir()
        (collection_dir / "collection.json").write_text(
            '{"type": "Collection", "id": "test-collection"}'
        )
        (collection_dir / "versions.json").write_text('{"spec_version": "1.0.0", "versions": []}')

        portolan_dir = catalog_root / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# No sensitive settings\n")

        runner = CliRunner()

        with patch("portolan_cli.push.push_async", new_callable=AsyncMock) as mock_push:
            mock_push.return_value = MagicMock(
                success=True,
                files_uploaded=0,
                versions_pushed=0,
                conflicts=[],
                errors=[],
            )

            with patch.dict(
                os.environ,
                {"PORTOLAN_REMOTE": TEST_REMOTE, "PORTOLAN_AWS_PROFILE": "env-profile"},
            ):
                runner.invoke(
                    cli,
                    ["push", "--collection", "test-collection", "--catalog", str(catalog_root)],
                )

            # Verify push was called with profile from env var
            assert mock_push.called
            call_kwargs = mock_push.call_args.kwargs
            assert call_kwargs["profile"] == "env-profile"
