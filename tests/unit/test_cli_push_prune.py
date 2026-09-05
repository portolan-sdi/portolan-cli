"""Tests for `portolan push --prune` safety guards and output contract (Issue #753)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.sync.prune import PruneGroup, PrunePlan
from portolan_cli.sync.push import PushAllResult

TEST_REMOTE = "s3://test/catalog"


def _setup_catalog(path: Path, collections: list[str]) -> None:
    portolan_dir = path / ".portolan"
    portolan_dir.mkdir(parents=True, exist_ok=True)
    (portolan_dir / "config.yaml").write_text("version: '1.0'\n")
    for name in collections:
        coll_dir = path / name
        coll_dir.mkdir()
        (coll_dir / "versions.json").write_text(json.dumps({"versions": []}))


def _successful_push(collections: int = 1) -> PushAllResult:
    return PushAllResult(
        success=True,
        total_collections=collections,
        successful_collections=collections,
        failed_collections=0,
        total_files_uploaded=collections,
        total_versions_pushed=collections,
    )


class TestPruneEmptyLocalCollectionsGuard:
    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    @pytest.mark.unit
    @patch("portolan_cli.sync.upload.setup_store")
    @patch("portolan_cli.sync.push.discover_collections")
    @patch("portolan_cli.sync.push.push_all_collections")
    def test_refuses_to_prune_with_zero_local_collections(
        self,
        mock_push_all: MagicMock,
        mock_discover: MagicMock,
        mock_setup_store: MagicMock,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _setup_catalog(Path("."), ["col1"])
            mock_push_all.return_value = _successful_push()
            mock_discover.return_value = []

            with patch.dict(os.environ, {"PORTOLAN_REMOTE": TEST_REMOTE}):
                result = runner.invoke(cli, ["push", "--catalog", ".", "--prune", "--yes"])

            assert result.exit_code == 0, f"Failed: {result.output}"
            mock_setup_store.assert_not_called()
            assert "Refusing to prune" in result.output


class TestPruneJsonOutputContract:
    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    @pytest.mark.unit
    @patch("portolan_cli.sync.prune.delete_prune_plan")
    @patch("portolan_cli.sync.prune.build_prune_plan")
    @patch("portolan_cli.sync.upload.setup_store")
    @patch("portolan_cli.sync.push.discover_collections")
    @patch("portolan_cli.sync.push.push_all_collections")
    def test_json_mode_emits_one_envelope_with_no_styled_text(
        self,
        mock_push_all: MagicMock,
        mock_discover: MagicMock,
        mock_setup_store: MagicMock,
        mock_build_plan: MagicMock,
        mock_delete_plan: MagicMock,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _setup_catalog(Path("."), ["col1"])
            mock_push_all.return_value = _successful_push()
            mock_discover.return_value = ["col1"]
            mock_setup_store.return_value = (MagicMock(), "cat")
            mock_build_plan.return_value = PrunePlan(
                delete=[PruneGroup(prefix="gone", keys=["cat/gone/a"])]
            )
            mock_delete_plan.return_value = (1, [])

            with patch.dict(os.environ, {"PORTOLAN_REMOTE": TEST_REMOTE}):
                result = runner.invoke(
                    cli, ["push", "--catalog", ".", "--prune", "--yes", "--json"]
                )

            assert result.exit_code == 0, f"Failed: {result.output}"
            assert "Would delete" not in result.output
            # A second, unparseable envelope would raise json.JSONDecodeError here.
            envelope = json.loads(result.output)
            assert envelope["success"] is True
            assert envelope["data"]["prune"]["pruned"] == 1
