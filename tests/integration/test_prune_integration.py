"""Integration tests for portolan_cli.sync.prune against a real object store.

Exercises build_prune_plan/delete_prune_plan against obstore's MemoryStore,
not mocks, and the CLI-level --prune/--collection guard (Issue #753).
"""

from __future__ import annotations

import json
from pathlib import Path

import obstore as obs
import pytest
from click.testing import CliRunner
from obstore.store import MemoryStore

from portolan_cli.cli import cli
from portolan_cli.sync.prune import build_prune_plan, delete_prune_plan


@pytest.mark.integration
def test_build_and_delete_plan_against_memory_store(tmp_path: Path) -> None:
    store = MemoryStore()

    # A still-live collection: one tracked asset, plus one the local build
    # never regenerated (missing/incomplete, must be refused not deleted).
    live_dir = tmp_path / "live"
    live_dir.mkdir()
    (live_dir / "versions.json").write_text(
        json.dumps(
            {"versions": [{"version": "1.0.0", "assets": {"a": {"href": "live/a.parquet"}}}]}
        )
    )
    obs.put(store, "cat/live/versions.json", b"{}")
    obs.put(store, "cat/live/a.parquet", b"x")
    obs.put(store, "cat/live/orphan_asset.parquet", b"x")

    # A collection removed locally in full (e.g. via `portolan rm --force`).
    obs.put(store, "cat/gone/versions.json", b"{}")
    obs.put(store, "cat/gone/b.parquet", b"x")

    plan = build_prune_plan(store, "cat", tmp_path, local_collections=["live"])

    assert plan.delete_count == 2
    assert {g.prefix for g in plan.delete} == {"gone"}
    assert plan.refuse_count == 1
    assert plan.refuse[0].keys == ["cat/live/orphan_asset.parquet"]

    deleted, errors = delete_prune_plan(store, plan)
    assert deleted == 2
    assert errors == []

    remaining = {str(meta["path"]) for batch in obs.list(store, prefix="cat") for meta in batch}
    assert remaining == {
        "cat/live/versions.json",
        "cat/live/a.parquet",
        "cat/live/orphan_asset.parquet",
    }


@pytest.mark.integration
class TestPruneCliGuard:
    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    def test_prune_with_collection_is_rejected(self, runner: CliRunner, tmp_path: Path) -> None:
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("version: '1.0'\n")

        result = runner.invoke(
            cli,
            [
                "push",
                "s3://test/catalog",
                "--collection",
                "demographics",
                "--prune",
                "--catalog",
                str(tmp_path),
            ],
        )

        assert result.exit_code == 1
        assert "--prune" in result.output
        assert "--collection" in result.output
