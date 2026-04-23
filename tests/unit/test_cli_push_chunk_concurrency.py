"""Tests for `portolan push --chunk-concurrency` CLI option (Issue #344).

These tests verify the CLI behavior of the --chunk-concurrency flag for
controlling per-file multipart upload parallelism.

TDD: These tests are written FIRST, before implementation.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.push import PushAllResult, PushResult

# Remote URL for tests - set via env var (Issue #356: sensitive settings)
TEST_REMOTE = "s3://test/catalog"


def _setup_catalog_with_collections(path: Path, collections: list[str]) -> None:
    """Helper to create a catalog with multiple collections.

    Note: remote must be set via PORTOLAN_REMOTE env var (Issue #356).
    """
    # Create .portolan/config.yaml (no sensitive settings)
    portolan_dir = path / ".portolan"
    portolan_dir.mkdir(parents=True, exist_ok=True)
    (portolan_dir / "config.yaml").write_text("version: '1.0'\n")

    # Create collections with versions.json
    for name in collections:
        coll_dir = path / name
        coll_dir.mkdir()
        (coll_dir / "versions.json").write_text(json.dumps({"versions": []}))


class TestPushChunkConcurrencyFlag:
    """Tests for `portolan push --chunk-concurrency` option."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    @patch("portolan_cli.push.push_all_collections")
    def test_chunk_concurrency_flag_exists(
        self, mock_push_all: MagicMock, runner: CliRunner, tmp_path: Path
    ) -> None:
        """push command accepts --chunk-concurrency flag."""
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

            with patch.dict(os.environ, {"PORTOLAN_REMOTE": TEST_REMOTE}):
                result = runner.invoke(cli, ["push", "--catalog", ".", "--chunk-concurrency", "4"])

            # Should not fail due to unknown option
            assert result.exit_code == 0, f"Failed: {result.output}"

    @pytest.mark.unit
    @patch("portolan_cli.push.push_all_collections")
    def test_chunk_concurrency_value_passed_to_push_all(
        self, mock_push_all: MagicMock, runner: CliRunner, tmp_path: Path
    ) -> None:
        """--chunk-concurrency value is passed to push_all_collections."""
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

            with patch.dict(os.environ, {"PORTOLAN_REMOTE": TEST_REMOTE}):
                result = runner.invoke(cli, ["push", "--catalog", ".", "--chunk-concurrency", "6"])

            assert result.exit_code == 0, f"Failed: {result.output}"
            mock_push_all.assert_called_once()
            call_kwargs = mock_push_all.call_args.kwargs
            assert call_kwargs.get("chunk_concurrency") == 6

    @pytest.mark.unit
    @patch("portolan_cli.push.push_all_collections")
    def test_chunk_concurrency_default_is_4(
        self, mock_push_all: MagicMock, runner: CliRunner, tmp_path: Path
    ) -> None:
        """--chunk-concurrency defaults to 4 when not specified."""
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

            with patch.dict(os.environ, {"PORTOLAN_REMOTE": TEST_REMOTE}):
                result = runner.invoke(cli, ["push", "--catalog", "."])

            assert result.exit_code == 0, f"Failed: {result.output}"
            mock_push_all.assert_called_once()
            call_kwargs = mock_push_all.call_args.kwargs
            assert call_kwargs.get("chunk_concurrency") == 4

    @pytest.mark.unit
    def test_chunk_concurrency_rejects_zero(self, runner: CliRunner, tmp_path: Path) -> None:
        """--chunk-concurrency rejects 0 (must be >= 1)."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _setup_catalog_with_collections(Path("."), ["col1"])

            result = runner.invoke(cli, ["push", "--catalog", ".", "--chunk-concurrency", "0"])

            assert result.exit_code != 0
            assert "Invalid value" in result.output or "Error" in result.output

    @pytest.mark.unit
    def test_chunk_concurrency_rejects_negative(self, runner: CliRunner, tmp_path: Path) -> None:
        """--chunk-concurrency rejects negative values."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _setup_catalog_with_collections(Path("."), ["col1"])

            result = runner.invoke(cli, ["push", "--catalog", ".", "--chunk-concurrency", "-1"])

            assert result.exit_code != 0

    @pytest.mark.unit
    @patch("portolan_cli.push.push_async")
    def test_chunk_concurrency_passed_to_single_collection_push(
        self, mock_push_async: MagicMock, runner: CliRunner, tmp_path: Path
    ) -> None:
        """--chunk-concurrency is passed to push_async for single collection."""
        from unittest.mock import AsyncMock

        with runner.isolated_filesystem(temp_dir=tmp_path):
            _setup_catalog_with_collections(Path("."), ["col1"])

            # Create an AsyncMock that returns the result
            mock_push_async.return_value = PushResult(
                success=True,
                files_uploaded=1,
                versions_pushed=1,
            )
            # Make it awaitable
            mock_push_async.side_effect = AsyncMock(
                return_value=PushResult(
                    success=True,
                    files_uploaded=1,
                    versions_pushed=1,
                )
            )

            with patch.dict(os.environ, {"PORTOLAN_REMOTE": TEST_REMOTE}):
                result = runner.invoke(
                    cli,
                    [
                        "push",
                        "--catalog",
                        ".",
                        "--collection",
                        "col1",
                        "--chunk-concurrency",
                        "8",
                    ],
                )

            assert result.exit_code == 0, f"Failed: {result.output}"
            mock_push_async.assert_called_once()
            call_kwargs = mock_push_async.call_args.kwargs
            assert call_kwargs.get("chunk_concurrency") == 8


class TestConcurrencyInteraction:
    """Tests for interaction between --concurrency and --chunk-concurrency."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    @patch("portolan_cli.push.push_all_collections")
    def test_both_concurrency_flags_can_be_set(
        self, mock_push_all: MagicMock, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Both --concurrency and --chunk-concurrency can be set together."""
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

            with patch.dict(os.environ, {"PORTOLAN_REMOTE": TEST_REMOTE}):
                result = runner.invoke(
                    cli,
                    [
                        "push",
                        "--catalog",
                        ".",
                        "--concurrency",
                        "10",
                        "--chunk-concurrency",
                        "6",
                    ],
                )

            assert result.exit_code == 0, f"Failed: {result.output}"
            mock_push_all.assert_called_once()
            call_kwargs = mock_push_all.call_args.kwargs
            assert call_kwargs.get("file_concurrency") == 10
            assert call_kwargs.get("chunk_concurrency") == 6

    @pytest.mark.unit
    @patch("portolan_cli.push.push_all_collections")
    def test_high_concurrency_shows_warning(
        self, mock_push_all: MagicMock, runner: CliRunner, tmp_path: Path
    ) -> None:
        """High concurrency values (> 100 connections) show a warning."""
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

            # 50 files × 12 chunks = 600 connections (way over safe limit)
            with patch.dict(os.environ, {"PORTOLAN_REMOTE": TEST_REMOTE}):
                result = runner.invoke(
                    cli,
                    [
                        "push",
                        "--catalog",
                        ".",
                        "--concurrency",
                        "50",
                        "--chunk-concurrency",
                        "12",
                    ],
                )

            assert result.exit_code == 0, f"Failed: {result.output}"
            # Should warn about high connection count
            assert "connections" in result.output.lower() or "warning" in result.output.lower()
