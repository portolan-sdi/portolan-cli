"""Tests for `portolan push --workers` CLI option (issue #229).

These tests verify the CLI behavior of the --workers flag for parallel push.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.push import PushAllResult, PushResult


def _setup_catalog_with_collections(path: Path, collections: list[str]) -> None:
    """Helper to create a catalog with multiple collections."""
    # Create .portolan/config.yaml
    portolan_dir = path / ".portolan"
    portolan_dir.mkdir(parents=True, exist_ok=True)
    (portolan_dir / "config.yaml").write_text("version: '1.0'\nremote: s3://test/catalog\n")

    # Create collections with versions.json
    for name in collections:
        coll_dir = path / name
        coll_dir.mkdir()
        (coll_dir / "versions.json").write_text(json.dumps({"versions": []}))


class TestPushWorkersFlag:
    """Tests for `portolan push --workers` option."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    @patch("portolan_cli.push.push_all_collections")
    def test_workers_flag_exists(
        self, mock_push_all: MagicMock, runner: CliRunner, tmp_path: Path
    ) -> None:
        """push command accepts --workers flag."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _setup_catalog_with_collections(Path("."), ["col1"])

            mock_push_all.return_value = PushAllResult(
                success=True,
                total_collections=1,
                successful_collections=1,
                failed_collections=0,
                total_files_uploaded=1,
                total_versions_pushed=1,
            )

            result = runner.invoke(cli, ["push", "--catalog", ".", "--workers", "4"])

            # Should not fail due to unknown option
            assert result.exit_code == 0, f"Failed: {result.output}"

    @pytest.mark.unit
    @patch("portolan_cli.push.push_all_collections")
    def test_workers_flag_shorthand(
        self, mock_push_all: MagicMock, runner: CliRunner, tmp_path: Path
    ) -> None:
        """push command accepts -w shorthand for --workers."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _setup_catalog_with_collections(Path("."), ["col1"])

            mock_push_all.return_value = PushAllResult(
                success=True,
                total_collections=1,
                successful_collections=1,
                failed_collections=0,
                total_files_uploaded=1,
                total_versions_pushed=1,
            )

            result = runner.invoke(cli, ["push", "--catalog", ".", "-w", "4"])

            assert result.exit_code == 0, f"Failed: {result.output}"

    @pytest.mark.unit
    @patch("portolan_cli.push.push_all_collections")
    def test_workers_value_passed_to_push_all(
        self, mock_push_all: MagicMock, runner: CliRunner, tmp_path: Path
    ) -> None:
        """--workers value is passed to push_all_collections."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _setup_catalog_with_collections(Path("."), ["col1"])

            mock_push_all.return_value = PushAllResult(
                success=True,
                total_collections=1,
                successful_collections=1,
                failed_collections=0,
                total_files_uploaded=1,
                total_versions_pushed=1,
            )

            result = runner.invoke(cli, ["push", "--catalog", ".", "--workers", "8"])

            assert result.exit_code == 0, f"Failed: {result.output}"
            # Verify workers=8 was passed
            mock_push_all.assert_called_once()
            call_kwargs = mock_push_all.call_args.kwargs
            assert call_kwargs["workers"] == 8

    @pytest.mark.unit
    @patch("portolan_cli.push.push_all_collections")
    def test_workers_default_is_none(
        self, mock_push_all: MagicMock, runner: CliRunner, tmp_path: Path
    ) -> None:
        """--workers defaults to None (auto-detect) when not specified."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _setup_catalog_with_collections(Path("."), ["col1"])

            mock_push_all.return_value = PushAllResult(
                success=True,
                total_collections=1,
                successful_collections=1,
                failed_collections=0,
                total_files_uploaded=1,
                total_versions_pushed=1,
            )

            result = runner.invoke(cli, ["push", "--catalog", "."])

            assert result.exit_code == 0, f"Failed: {result.output}"
            # Verify workers=None was passed (auto-detect)
            mock_push_all.assert_called_once()
            call_kwargs = mock_push_all.call_args.kwargs
            assert call_kwargs["workers"] is None

    @pytest.mark.unit
    @patch("portolan_cli.push.push")
    def test_workers_ignored_for_single_collection(
        self, mock_push: MagicMock, runner: CliRunner, tmp_path: Path
    ) -> None:
        """--workers is ignored when pushing a specific collection (--collection)."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _setup_catalog_with_collections(Path("."), ["col1"])

            mock_push.return_value = PushResult(
                success=True,
                files_uploaded=1,
                versions_pushed=1,
                conflicts=[],
                errors=[],
            )

            # When --collection is specified, single push is called
            result = runner.invoke(
                cli, ["push", "--catalog", ".", "--collection", "col1", "--workers", "4"]
            )

            assert result.exit_code == 0, f"Failed: {result.output}"
            # Single collection push doesn't use workers
            mock_push.assert_called_once()
            call_kwargs = mock_push.call_args.kwargs
            assert "workers" not in call_kwargs

    @pytest.mark.unit
    def test_workers_requires_positive_integer(self, runner: CliRunner, tmp_path: Path) -> None:
        """--workers requires a positive integer value (>= 1)."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _setup_catalog_with_collections(Path("."), ["col1"])

            # Negative value should fail with IntRange validation
            result = runner.invoke(cli, ["push", "--workers", "-1"])
            assert result.exit_code != 0
            assert "1" in result.output or "range" in result.output.lower()

            # Zero should also fail (IntRange min=1)
            result = runner.invoke(cli, ["push", "--workers", "0"])
            assert result.exit_code != 0
            assert "1" in result.output or "range" in result.output.lower()

            # Non-integer should fail
            result = runner.invoke(cli, ["push", "--workers", "abc"])
            assert result.exit_code != 0

    @pytest.mark.unit
    @patch("portolan_cli.push.push_all_collections")
    def test_workers_1_for_sequential(
        self, mock_push_all: MagicMock, runner: CliRunner, tmp_path: Path
    ) -> None:
        """--workers 1 explicitly requests sequential execution."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _setup_catalog_with_collections(Path("."), ["col1", "col2"])

            mock_push_all.return_value = PushAllResult(
                success=True,
                total_collections=2,
                successful_collections=2,
                failed_collections=0,
                total_files_uploaded=2,
                total_versions_pushed=2,
            )

            result = runner.invoke(cli, ["push", "--catalog", ".", "--workers", "1"])

            assert result.exit_code == 0, f"Failed: {result.output}"
            mock_push_all.assert_called_once()
            call_kwargs = mock_push_all.call_args.kwargs
            assert call_kwargs["workers"] == 1
