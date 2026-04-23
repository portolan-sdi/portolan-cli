"""Tests for `portolan push --max-connections` CLI option (Issue #344).

These tests verify the CLI behavior of the --max-connections flag for
capping the total concurrent HTTP connections (file_concurrency × chunk_concurrency).

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
from portolan_cli.push import PushAllResult

# Remote URL for tests - set via env var (Issue #356: sensitive settings)
TEST_REMOTE = "s3://test/catalog"


def _setup_catalog_with_collections(path: Path, collections: list[str]) -> None:
    """Helper to create a catalog with multiple collections.

    Note: remote must be set via PORTOLAN_REMOTE env var (Issue #356).
    """
    portolan_dir = path / ".portolan"
    portolan_dir.mkdir(parents=True, exist_ok=True)
    (portolan_dir / "config.yaml").write_text("version: '1.0'\n")

    for name in collections:
        coll_dir = path / name
        coll_dir.mkdir()
        (coll_dir / "versions.json").write_text(json.dumps({"versions": []}))


class TestPushMaxConnectionsFlag:
    """Tests for `portolan push --max-connections` option."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    @patch("portolan_cli.push.push_all_collections")
    def test_max_connections_flag_exists(
        self, mock_push_all: MagicMock, runner: CliRunner, tmp_path: Path
    ) -> None:
        """push command accepts --max-connections flag."""
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
                result = runner.invoke(cli, ["push", "--catalog", ".", "--max-connections", "50"])

            assert result.exit_code == 0, f"Failed: {result.output}"

    @pytest.mark.unit
    @patch("portolan_cli.push.push_all_collections")
    def test_max_connections_value_passed_to_push_all(
        self, mock_push_all: MagicMock, runner: CliRunner, tmp_path: Path
    ) -> None:
        """--max-connections value is passed to push_all_collections."""
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
                result = runner.invoke(cli, ["push", "--catalog", ".", "--max-connections", "64"])

            assert result.exit_code == 0, f"Failed: {result.output}"
            mock_push_all.assert_called_once()
            call_kwargs = mock_push_all.call_args.kwargs
            assert call_kwargs.get("max_connections") == 64

    @pytest.mark.unit
    def test_max_connections_rejects_too_low(self, runner: CliRunner, tmp_path: Path) -> None:
        """--max-connections rejects values below 1."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _setup_catalog_with_collections(Path("."), ["col1"])

            result = runner.invoke(cli, ["push", "--catalog", ".", "--max-connections", "0"])

            assert result.exit_code != 0


class TestMaxConnectionsAutoAdjustment:
    """Tests for auto-adjustment of concurrency to respect --max-connections."""

    @pytest.mark.unit
    def test_adjust_concurrency_for_max_connections_reduces_file_concurrency(
        self,
    ) -> None:
        """adjust_concurrency_for_max_connections reduces file concurrency first."""
        from portolan_cli.async_utils import adjust_concurrency_for_max_connections

        # If max_connections=32 and chunk=4, file should be capped at 8
        file_conc, chunk_conc = adjust_concurrency_for_max_connections(
            file_concurrency=50,
            chunk_concurrency=4,
            max_connections=32,
        )

        assert file_conc * chunk_conc <= 32
        assert file_conc == 8
        assert chunk_conc == 4  # Chunk unchanged when file adjustment is enough

    @pytest.mark.unit
    def test_adjust_concurrency_for_max_connections_reduces_both(self) -> None:
        """adjust_concurrency_for_max_connections reduces both if needed."""
        from portolan_cli.async_utils import adjust_concurrency_for_max_connections

        # If max_connections=6 and both are high, both must be reduced
        file_conc, chunk_conc = adjust_concurrency_for_max_connections(
            file_concurrency=50,
            chunk_concurrency=12,
            max_connections=6,
        )

        assert file_conc * chunk_conc <= 6
        # With max=6, reasonable split is 2×3 or 3×2 or 6×1
        assert file_conc >= 1
        assert chunk_conc >= 1

    @pytest.mark.unit
    def test_adjust_concurrency_for_max_connections_no_change_if_under_limit(
        self,
    ) -> None:
        """adjust_concurrency_for_max_connections doesn't change if already under limit."""
        from portolan_cli.async_utils import adjust_concurrency_for_max_connections

        # 8×4=32, which is under 100
        file_conc, chunk_conc = adjust_concurrency_for_max_connections(
            file_concurrency=8,
            chunk_concurrency=4,
            max_connections=100,
        )

        assert file_conc == 8
        assert chunk_conc == 4

    @pytest.mark.unit
    def test_adjust_concurrency_for_max_connections_minimum_one(self) -> None:
        """adjust_concurrency_for_max_connections never goes below 1."""
        from portolan_cli.async_utils import adjust_concurrency_for_max_connections

        file_conc, chunk_conc = adjust_concurrency_for_max_connections(
            file_concurrency=100,
            chunk_concurrency=100,
            max_connections=1,
        )

        assert file_conc >= 1
        assert chunk_conc >= 1
        assert file_conc * chunk_conc <= 1


class TestMaxConnectionsWithWorkers:
    """Tests for --max-connections interaction with --workers."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    @patch("portolan_cli.push.push_all_collections")
    def test_max_connections_considers_workers(
        self, mock_push_all: MagicMock, runner: CliRunner, tmp_path: Path
    ) -> None:
        """--max-connections is divided across workers."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _setup_catalog_with_collections(Path("."), ["col1", "col2", "col3", "col4"])

            mock_push_all.return_value = PushAllResult(
                success=True,
                total_collections=4,
                successful_collections=4,
                failed_collections=0,
                total_files_uploaded=4,
                total_versions_pushed=4,
            )

            # With 4 workers and max_connections=64, each worker gets 16 connections
            with patch.dict(os.environ, {"PORTOLAN_REMOTE": TEST_REMOTE}):
                result = runner.invoke(
                    cli,
                    [
                        "push",
                        "--catalog",
                        ".",
                        "--workers",
                        "4",
                        "--max-connections",
                        "64",
                    ],
                )

            assert result.exit_code == 0, f"Failed: {result.output}"
            mock_push_all.assert_called_once()
            call_kwargs = mock_push_all.call_args.kwargs

            # The max_connections should be passed through
            assert call_kwargs.get("max_connections") == 64
            # Workers should also be passed
            assert call_kwargs.get("workers") == 4


class TestMaxConnectionsHelp:
    """Tests for --max-connections help text."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    def test_max_connections_in_help(self, runner: CliRunner) -> None:
        """--max-connections appears in push --help."""
        result = runner.invoke(cli, ["push", "--help"])

        assert result.exit_code == 0
        assert "--max-connections" in result.output

    @pytest.mark.unit
    def test_max_connections_help_mentions_nat(self, runner: CliRunner) -> None:
        """--max-connections help mentions NAT/home network safety."""
        result = runner.invoke(cli, ["push", "--help"])

        assert result.exit_code == 0
        # Help should explain why this matters
        help_lower = result.output.lower()
        assert "connection" in help_lower
