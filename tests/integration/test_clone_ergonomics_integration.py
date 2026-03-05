"""Integration tests for clone command git-style ergonomics (Issue #146).

These tests verify the CLI command works end-to-end with mocked S3.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

if TYPE_CHECKING:
    from click.testing import CliRunner


class TestCloneCLIIntegration:
    """Integration tests for the clone CLI command."""

    @pytest.fixture
    def cli_runner(self) -> CliRunner:
        """Create Click CLI test runner."""
        from click.testing import CliRunner

        return CliRunner()

    @pytest.mark.integration
    def test_clone_infers_directory_from_url(
        self,
        cli_runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Clone should infer directory name from remote URL."""
        import os

        from portolan_cli.cli import clone

        # Change to tmp_path to avoid cluttering the repo
        original_cwd = os.getcwd()
        os.chdir(tmp_path)

        try:
            with (
                patch("portolan_cli.sync.list_remote_collections") as mock_list,
                patch("portolan_cli.sync.init_catalog") as mock_init,
                patch("portolan_cli.sync.pull") as mock_pull,
            ):
                mock_list.return_value = ["collection-a"]
                mock_init.return_value = None
                mock_pull.return_value = MagicMock(
                    success=True,
                    files_downloaded=5,
                    remote_version="1.0.0",
                )

                # No LOCAL_PATH specified - should infer from URL
                result = cli_runner.invoke(
                    clone,
                    ["s3://mybucket/my-test-catalog"],
                )

                # Check that it tried to create "my-test-catalog" directory
                assert result.exit_code == 0
                # Verify pull was called with the inferred path
                call_kwargs = mock_pull.call_args.kwargs
                assert "my-test-catalog" in str(call_kwargs["local_root"])
        finally:
            os.chdir(original_cwd)

    @pytest.mark.integration
    def test_clone_all_collections_when_none_specified(
        self,
        cli_runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Clone without -c should clone all collections."""
        from portolan_cli.cli import clone

        target = tmp_path / "catalog"

        with (
            patch("portolan_cli.sync.list_remote_collections") as mock_list,
            patch("portolan_cli.sync.init_catalog") as mock_init,
            patch("portolan_cli.sync.pull") as mock_pull,
        ):
            mock_list.return_value = ["demographics", "imagery", "boundaries"]
            mock_init.return_value = None
            mock_pull.return_value = MagicMock(
                success=True,
                files_downloaded=3,
                remote_version="1.0.0",
            )

            result = cli_runner.invoke(
                clone,
                ["s3://mybucket/catalog", str(target)],  # No -c flag
            )

            assert result.exit_code == 0
            # Should have called pull for each collection
            assert mock_pull.call_count == 3
            collections_pulled = [call.kwargs["collection"] for call in mock_pull.call_args_list]
            assert "demographics" in collections_pulled
            assert "imagery" in collections_pulled
            assert "boundaries" in collections_pulled

    @pytest.mark.integration
    def test_clone_specific_collection_with_c_flag(
        self,
        cli_runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Clone with -c should clone only specified collection."""
        from portolan_cli.cli import clone

        target = tmp_path / "catalog"

        with (
            patch("portolan_cli.sync.list_remote_collections") as mock_list,
            patch("portolan_cli.sync.init_catalog") as mock_init,
            patch("portolan_cli.sync.pull") as mock_pull,
        ):
            mock_init.return_value = None
            mock_pull.return_value = MagicMock(
                success=True,
                files_downloaded=10,
                remote_version="1.0.0",
            )

            result = cli_runner.invoke(
                clone,
                ["s3://mybucket/catalog", str(target), "-c", "demographics"],
            )

            assert result.exit_code == 0
            # Should NOT list remote collections (skip discovery)
            mock_list.assert_not_called()
            # Should call pull once with specific collection
            mock_pull.assert_called_once()
            assert mock_pull.call_args.kwargs["collection"] == "demographics"

    @pytest.mark.integration
    def test_clone_to_current_directory(
        self,
        cli_runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Clone to '.' should work if directory is empty."""
        import os

        from portolan_cli.cli import clone

        # Create empty target directory
        target = tmp_path / "empty_target"
        target.mkdir()

        original_cwd = os.getcwd()
        os.chdir(target)

        try:
            with (
                patch("portolan_cli.sync.init_catalog") as mock_init,
                patch("portolan_cli.sync.pull") as mock_pull,
            ):
                mock_init.return_value = None
                mock_pull.return_value = MagicMock(
                    success=True,
                    files_downloaded=5,
                    remote_version="1.0.0",
                )

                result = cli_runner.invoke(
                    clone,
                    ["s3://mybucket/catalog", ".", "-c", "test"],
                )

                assert result.exit_code == 0
        finally:
            os.chdir(original_cwd)

    @pytest.mark.integration
    def test_clone_fails_on_invalid_url(
        self,
        cli_runner: CliRunner,
    ) -> None:
        """Clone should fail with helpful message for bucket-only URL."""
        from portolan_cli.cli import clone

        result = cli_runner.invoke(
            clone,
            ["s3://mybucket"],  # No catalog name
        )

        assert result.exit_code == 1
        assert "Cannot infer" in result.output or "local path" in result.output.lower()

    @pytest.mark.integration
    def test_clone_partial_failure_reports_errors(
        self,
        cli_runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Clone should report which collections failed in partial failure."""
        from portolan_cli.cli import clone

        target = tmp_path / "catalog"

        with (
            patch("portolan_cli.sync.list_remote_collections") as mock_list,
            patch("portolan_cli.sync.init_catalog") as mock_init,
            patch("portolan_cli.sync.pull") as mock_pull,
        ):
            mock_list.return_value = ["good", "bad", "also-good"]
            mock_init.return_value = None
            # Second call fails
            mock_pull.side_effect = [
                MagicMock(success=True, files_downloaded=3, remote_version="1.0.0"),
                MagicMock(success=False, remote_version="1.0.0", uncommitted_changes=[]),
                MagicMock(success=True, files_downloaded=2, remote_version="1.0.0"),
            ]

            result = cli_runner.invoke(
                clone,
                ["s3://mybucket/catalog", str(target)],
            )

            # Should exit with error (partial failure)
            assert result.exit_code == 1
            # Should mention the failed collection
            assert "bad" in result.output
