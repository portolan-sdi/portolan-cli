"""Tests for GitHub issue #323: --concurrency flag ignored for catalog-wide operations.

The bug: When doing catalog-wide push/pull (no --collection specified), the
--concurrency flag (which controls file-level parallelism within each collection)
was completely ignored. It was only passed to single-collection operations.

The fix: Pass file_concurrency through to push_all_collections/pull_all_collections
so that individual collection operations also respect file-level concurrency limits.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.pull import PullAllResult, PullResult, pull_all_collections
from portolan_cli.push import PushAllResult, PushResult, push_all_collections

# Mark all tests in this module as unit tests
pytestmark = pytest.mark.unit


def _setup_valid_catalog(catalog_root: Path) -> None:
    """Helper to create a valid catalog with .portolan/config.yaml."""
    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir(parents=True, exist_ok=True)
    (portolan_dir / "config.yaml").write_text("version: '1.0'\nremote: s3://test/catalog\n")


def _create_collection(catalog_root: Path, name: str) -> None:
    """Helper to create a collection with versions.json."""
    collection_dir = catalog_root / name
    collection_dir.mkdir()
    (collection_dir / "versions.json").write_text(json.dumps({"versions": []}))


# =============================================================================
# Tests for push_all_collections file_concurrency parameter
# =============================================================================


class TestPushAllCollectionsFileConcurrency:
    """Tests for file_concurrency parameter in push_all_collections()."""

    @patch("portolan_cli.push.push_async", new_callable=AsyncMock)
    def test_accepts_file_concurrency_parameter(self, mock_push: AsyncMock, tmp_path: Path) -> None:
        """push_all_collections accepts file_concurrency parameter."""
        _setup_valid_catalog(tmp_path)
        _create_collection(tmp_path, "col1")

        mock_push.return_value = PushResult(
            success=True,
            files_uploaded=1,
            versions_pushed=1,
            conflicts=[],
            errors=[],
        )

        # Should not raise - file_concurrency parameter is accepted
        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            file_concurrency=10,
        )

        assert result.success is True

    @patch("portolan_cli.push.push_async", new_callable=AsyncMock)
    def test_file_concurrency_passed_to_push_async(
        self, mock_push: AsyncMock, tmp_path: Path
    ) -> None:
        """file_concurrency is passed through to push_async() calls."""
        _setup_valid_catalog(tmp_path)
        _create_collection(tmp_path, "col1")
        _create_collection(tmp_path, "col2")

        mock_push.return_value = PushResult(
            success=True,
            files_uploaded=1,
            versions_pushed=1,
            conflicts=[],
            errors=[],
        )

        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            file_concurrency=5,
        )

        assert result.success is True
        assert mock_push.call_count == 2

        # Verify file_concurrency was passed to each push_async call
        for call in mock_push.call_args_list:
            assert call.kwargs.get("concurrency") == 5

    @patch("portolan_cli.push.push_async", new_callable=AsyncMock)
    def test_file_concurrency_none_uses_default(self, mock_push: AsyncMock, tmp_path: Path) -> None:
        """file_concurrency=None uses the default concurrency (8 per Issue #344)."""
        _setup_valid_catalog(tmp_path)
        _create_collection(tmp_path, "col1")

        mock_push.return_value = PushResult(
            success=True,
            files_uploaded=1,
            versions_pushed=1,
            conflicts=[],
            errors=[],
        )

        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            file_concurrency=None,  # Explicitly pass None
        )

        assert result.success is True
        # When file_concurrency is None, the default (8) is applied
        # per Issue #344 (lowered from 50 for home network safety)
        call_kwargs = mock_push.call_args.kwargs
        assert call_kwargs.get("concurrency") == 8


# =============================================================================
# Tests for pull_all_collections file_concurrency parameter
# =============================================================================


class TestPullAllCollectionsFileConcurrency:
    """Tests for file_concurrency parameter in pull_all_collections()."""

    @patch("portolan_cli.pull.pull_async", new_callable=AsyncMock)
    def test_accepts_file_concurrency_parameter(self, mock_pull: AsyncMock, tmp_path: Path) -> None:
        """pull_all_collections accepts file_concurrency parameter."""
        _setup_valid_catalog(tmp_path)
        _create_collection(tmp_path, "col1")

        mock_pull.return_value = PullResult(
            success=True,
            files_downloaded=1,
            files_skipped=0,
            local_version="1.0.0",
            remote_version="1.0.1",
        )

        # Should not raise - file_concurrency parameter is accepted
        result = pull_all_collections(
            remote_url="s3://bucket/catalog",
            local_root=tmp_path,
            file_concurrency=10,
        )

        assert result.success is True

    @patch("portolan_cli.pull.pull_async", new_callable=AsyncMock)
    def test_file_concurrency_passed_to_pull_async(
        self, mock_pull: AsyncMock, tmp_path: Path
    ) -> None:
        """file_concurrency is passed through to pull_async() calls."""
        _setup_valid_catalog(tmp_path)
        _create_collection(tmp_path, "col1")
        _create_collection(tmp_path, "col2")

        mock_pull.return_value = PullResult(
            success=True,
            files_downloaded=1,
            files_skipped=0,
            local_version="1.0.0",
            remote_version="1.0.1",
        )

        result = pull_all_collections(
            remote_url="s3://bucket/catalog",
            local_root=tmp_path,
            file_concurrency=5,
        )

        assert result.success is True
        assert mock_pull.call_count == 2

        # Verify file_concurrency was passed to each pull_async call
        for call in mock_pull.call_args_list:
            assert call.kwargs.get("concurrency") == 5

    @patch("portolan_cli.pull.pull_async", new_callable=AsyncMock)
    def test_file_concurrency_none_uses_default(self, mock_pull: AsyncMock, tmp_path: Path) -> None:
        """file_concurrency=None allows pull_async to use its default."""
        _setup_valid_catalog(tmp_path)
        _create_collection(tmp_path, "col1")

        mock_pull.return_value = PullResult(
            success=True,
            files_downloaded=1,
            files_skipped=0,
            local_version="1.0.0",
            remote_version="1.0.1",
        )

        result = pull_all_collections(
            remote_url="s3://bucket/catalog",
            local_root=tmp_path,
            file_concurrency=None,  # Explicitly pass None
        )

        assert result.success is True
        # When file_concurrency is None, we should NOT pass concurrency
        call_kwargs = mock_pull.call_args.kwargs
        assert call_kwargs.get("concurrency") is None


# =============================================================================
# Tests for CLI --concurrency flag with catalog-wide operations
# =============================================================================


class TestCliConcurrencyFlagCatalogWide:
    """Tests that --concurrency flag is passed to catalog-wide operations."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @patch("portolan_cli.push.push_all_collections")
    def test_push_concurrency_passed_to_push_all_collections(
        self, mock_push_all: MagicMock, runner: CliRunner, tmp_path: Path
    ) -> None:
        """--concurrency is passed to push_all_collections for catalog-wide push."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _setup_valid_catalog(Path("."))
            _create_collection(Path("."), "col1")

            mock_push_all.return_value = PushAllResult(
                success=True,
                total_collections=1,
                successful_collections=1,
                failed_collections=0,
                total_files_uploaded=1,
                total_versions_pushed=1,
            )

            result = runner.invoke(cli, ["push", "--catalog", ".", "--concurrency", "10"])

            assert result.exit_code == 0, f"Failed: {result.output}"
            mock_push_all.assert_called_once()
            call_kwargs = mock_push_all.call_args.kwargs
            assert call_kwargs["file_concurrency"] == 10

    @patch("portolan_cli.push.push_all_collections")
    def test_push_concurrency_default_passed_to_push_all_collections(
        self, mock_push_all: MagicMock, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Default --concurrency (8 per Issue #344) is passed to push_all_collections."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _setup_valid_catalog(Path("."))
            _create_collection(Path("."), "col1")

            mock_push_all.return_value = PushAllResult(
                success=True,
                total_collections=1,
                successful_collections=1,
                failed_collections=0,
                total_files_uploaded=1,
                total_versions_pushed=1,
            )

            # Don't specify --concurrency, should use default of 8 (Issue #344)
            result = runner.invoke(cli, ["push", "--catalog", "."])

            assert result.exit_code == 0, f"Failed: {result.output}"
            mock_push_all.assert_called_once()
            call_kwargs = mock_push_all.call_args.kwargs
            assert call_kwargs["file_concurrency"] == 8

    @patch("portolan_cli.pull.pull_all_collections")
    def test_pull_concurrency_passed_to_pull_all_collections(
        self, mock_pull_all: MagicMock, runner: CliRunner, tmp_path: Path
    ) -> None:
        """--concurrency is passed to pull_all_collections for catalog-wide pull."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _setup_valid_catalog(Path("."))
            _create_collection(Path("."), "col1")

            mock_pull_all.return_value = PullAllResult(
                success=True,
                total_collections=1,
                successful_collections=1,
                failed_collections=0,
                total_files_downloaded=1,
            )

            result = runner.invoke(
                cli, ["pull", "s3://bucket/catalog", "--catalog", ".", "--concurrency", "10"]
            )

            assert result.exit_code == 0, f"Failed: {result.output}"
            mock_pull_all.assert_called_once()
            call_kwargs = mock_pull_all.call_args.kwargs
            assert call_kwargs["file_concurrency"] == 10

    @patch("portolan_cli.pull.pull_all_collections")
    def test_pull_concurrency_default_passed_to_pull_all_collections(
        self, mock_pull_all: MagicMock, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Default --concurrency (50) is passed to pull_all_collections."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _setup_valid_catalog(Path("."))
            _create_collection(Path("."), "col1")

            mock_pull_all.return_value = PullAllResult(
                success=True,
                total_collections=1,
                successful_collections=1,
                failed_collections=0,
                total_files_downloaded=1,
            )

            # Don't specify --concurrency, should use default of 50
            result = runner.invoke(cli, ["pull", "s3://bucket/catalog", "--catalog", "."])

            assert result.exit_code == 0, f"Failed: {result.output}"
            mock_pull_all.assert_called_once()
            call_kwargs = mock_pull_all.call_args.kwargs
            assert call_kwargs["file_concurrency"] == 50


# =============================================================================
# Tests for combined workers + file_concurrency
# =============================================================================


class TestCombinedConcurrencyParameters:
    """Tests for using both --workers and --concurrency together."""

    @patch("portolan_cli.push.push_async", new_callable=AsyncMock)
    def test_push_both_workers_and_file_concurrency(
        self, mock_push: AsyncMock, tmp_path: Path
    ) -> None:
        """push_all_collections respects both workers and file_concurrency."""
        _setup_valid_catalog(tmp_path)
        for name in ["col1", "col2", "col3"]:
            _create_collection(tmp_path, name)

        mock_push.return_value = PushResult(
            success=True,
            files_uploaded=1,
            versions_pushed=1,
            conflicts=[],
            errors=[],
        )

        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            workers=2,  # Collection-level concurrency
            file_concurrency=5,  # File-level concurrency
        )

        assert result.success is True
        assert mock_push.call_count == 3

        # All push_async calls should have concurrency=5
        for call in mock_push.call_args_list:
            assert call.kwargs.get("concurrency") == 5

    @patch("portolan_cli.pull.pull_async", new_callable=AsyncMock)
    def test_pull_both_workers_and_file_concurrency(
        self, mock_pull: AsyncMock, tmp_path: Path
    ) -> None:
        """pull_all_collections respects both workers and file_concurrency."""
        _setup_valid_catalog(tmp_path)
        for name in ["col1", "col2", "col3"]:
            _create_collection(tmp_path, name)

        mock_pull.return_value = PullResult(
            success=True,
            files_downloaded=1,
            files_skipped=0,
            local_version="1.0.0",
            remote_version="1.0.1",
        )

        result = pull_all_collections(
            remote_url="s3://bucket/catalog",
            local_root=tmp_path,
            workers=2,  # Collection-level concurrency
            file_concurrency=5,  # File-level concurrency
        )

        assert result.success is True
        assert mock_pull.call_count == 3

        # All pull_async calls should have concurrency=5
        for call in mock_pull.call_args_list:
            assert call.kwargs.get("concurrency") == 5

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @patch("portolan_cli.push.push_all_collections")
    def test_cli_push_both_workers_and_concurrency(
        self, mock_push_all: MagicMock, runner: CliRunner, tmp_path: Path
    ) -> None:
        """CLI push passes both --workers and --concurrency."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _setup_valid_catalog(Path("."))
            _create_collection(Path("."), "col1")

            mock_push_all.return_value = PushAllResult(
                success=True,
                total_collections=1,
                successful_collections=1,
                failed_collections=0,
                total_files_uploaded=1,
                total_versions_pushed=1,
            )

            result = runner.invoke(
                cli,
                ["push", "--catalog", ".", "--workers", "2", "--concurrency", "10"],
            )

            assert result.exit_code == 0, f"Failed: {result.output}"
            mock_push_all.assert_called_once()
            call_kwargs = mock_push_all.call_args.kwargs
            assert call_kwargs["workers"] == 2
            assert call_kwargs["file_concurrency"] == 10

    @patch("portolan_cli.pull.pull_all_collections")
    def test_cli_pull_both_workers_and_concurrency(
        self, mock_pull_all: MagicMock, runner: CliRunner, tmp_path: Path
    ) -> None:
        """CLI pull passes both --workers and --concurrency."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _setup_valid_catalog(Path("."))
            _create_collection(Path("."), "col1")

            mock_pull_all.return_value = PullAllResult(
                success=True,
                total_collections=1,
                successful_collections=1,
                failed_collections=0,
                total_files_downloaded=1,
            )

            result = runner.invoke(
                cli,
                [
                    "pull",
                    "s3://bucket/catalog",
                    "--catalog",
                    ".",
                    "--workers",
                    "2",
                    "--concurrency",
                    "10",
                ],
            )

            assert result.exit_code == 0, f"Failed: {result.output}"
            mock_pull_all.assert_called_once()
            call_kwargs = mock_pull_all.call_args.kwargs
            assert call_kwargs["workers"] == 2
            assert call_kwargs["file_concurrency"] == 10
