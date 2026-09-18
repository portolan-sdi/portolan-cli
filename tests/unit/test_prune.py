"""Unit tests for portolan_cli.sync.prune (Issue #753).

TDD: written before the corresponding cli.py wiring was exercised end to end.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from portolan_cli.sync.prune import (
    PrunePlan,
    build_prune_plan,
    delete_prune_plan,
    print_prune_plan,
)


def _write_versions(catalog_root: Path, collection: str, hrefs: list[str]) -> None:
    coll_dir = catalog_root / collection
    coll_dir.mkdir(parents=True, exist_ok=True)
    assets = {href.rsplit("/", 1)[-1]: {"href": href, "sha256": "x"} for href in hrefs}
    (coll_dir / "versions.json").write_text(
        json.dumps({"versions": [{"version": "1.0.0", "assets": assets}]})
    )


def _remote_meta(*paths: str) -> list[list[dict[str, object]]]:
    return [[{"path": p, "size": 1} for p in paths]]


@pytest.mark.unit
class TestBuildPrunePlan:
    def test_deletes_keys_under_unowned_prefix(self, tmp_path: Path) -> None:
        store = MagicMock()
        with patch(
            "portolan_cli.sync.prune.obs.list",
            return_value=_remote_meta("cat/stale/v1/versions.json", "cat/stale/v1/data.parquet"),
        ):
            plan = build_prune_plan(store, "cat", tmp_path, local_collections=["live"])

        assert plan.delete_count == 2
        assert plan.delete[0].prefix == "stale/v1"
        assert plan.refuse_count == 0

    def test_deletes_stray_key_under_no_known_collection(self, tmp_path: Path) -> None:
        store = MagicMock()
        with patch(
            "portolan_cli.sync.prune.obs.list",
            return_value=_remote_meta("cat/orphan.txt"),
        ):
            plan = build_prune_plan(store, "cat", tmp_path, local_collections=["live"])

        assert plan.delete_count == 1
        assert plan.delete[0].prefix == "(unrecognized)"

    def test_refuses_key_missing_locally_under_live_collection(self, tmp_path: Path) -> None:
        _write_versions(tmp_path, "live", ["live/data.parquet"])
        store = MagicMock()
        with patch(
            "portolan_cli.sync.prune.obs.list",
            return_value=_remote_meta(
                "cat/live/versions.json", "cat/live/data.parquet", "cat/live/extra.parquet"
            ),
        ):
            plan = build_prune_plan(store, "cat", tmp_path, local_collections=["live"])

        assert plan.delete_count == 0
        assert plan.refuse_count == 1
        assert plan.refuse[0].keys == ["cat/live/extra.parquet"]

    def test_ignores_metadata_basenames_under_live_collection(self, tmp_path: Path) -> None:
        _write_versions(tmp_path, "live", ["live/data.parquet"])
        store = MagicMock()
        with patch(
            "portolan_cli.sync.prune.obs.list",
            return_value=_remote_meta(
                "cat/live/data.parquet", "cat/live/versions.json", "cat/live/collection.json"
            ),
        ):
            plan = build_prune_plan(store, "cat", tmp_path, local_collections=["live"])

        assert plan.delete_count == 0
        assert plan.refuse_count == 0

    def test_agents_md_is_a_metadata_basename(self, tmp_path: Path) -> None:
        _write_versions(tmp_path, "live", ["live/data.parquet"])
        store = MagicMock()
        with patch(
            "portolan_cli.sync.prune.obs.list",
            return_value=_remote_meta(
                "cat/live/data.parquet", "cat/live/versions.json", "cat/live/AGENTS.md"
            ),
        ):
            plan = build_prune_plan(store, "cat", tmp_path, local_collections=["live"])

        assert plan.delete_count == 0
        assert plan.refuse_count == 0

    def test_nested_collection_never_matches_unrelated_sibling(self, tmp_path: Path) -> None:
        # "live" must not match a remote key under "live-other/...".
        store = MagicMock()
        with patch(
            "portolan_cli.sync.prune.obs.list",
            return_value=_remote_meta(
                "cat/live-other/versions.json", "cat/live-other/data.parquet"
            ),
        ):
            plan = build_prune_plan(store, "cat", tmp_path, local_collections=["live"])

        assert plan.delete_count == 2
        assert plan.delete[0].prefix == "live-other"

    def test_empty_local_collections_returns_empty_plan_without_listing(
        self, tmp_path: Path
    ) -> None:
        store = MagicMock()
        with patch("portolan_cli.sync.prune.obs.list") as mock_list:
            plan = build_prune_plan(store, "cat", tmp_path, local_collections=[])

        assert plan.delete_count == 0
        assert plan.refuse_count == 0
        mock_list.assert_not_called()

    def test_root_and_subcatalog_metadata_not_deleted(self, tmp_path: Path) -> None:
        _write_versions(tmp_path, "region/live", ["region/live/data.parquet"])
        store = MagicMock()
        with patch(
            "portolan_cli.sync.prune.obs.list",
            return_value=_remote_meta(
                "cat/catalog.json",
                "cat/README.md",
                "cat/region/catalog.json",
                "cat/region/README.md",
                "cat/region/live/versions.json",
                "cat/region/live/data.parquet",
            ),
        ):
            plan = build_prune_plan(store, "cat", tmp_path, local_collections=["region/live"])

        assert plan.delete_count == 0
        assert plan.refuse_count == 0

    def test_root_metadata_survives_alongside_root_level_versions_json(
        self, tmp_path: Path
    ) -> None:
        # A root versions.json is catalog-level state, not a collection prefix.
        _write_versions(tmp_path, "region/live", ["region/live/data.parquet"])
        store = MagicMock()
        with patch(
            "portolan_cli.sync.prune.obs.list",
            return_value=_remote_meta(
                "cat/AGENTS.md",
                "cat/README.md",
                "cat/catalog.json",
                "cat/versions.json",
                "cat/region/catalog.json",
                "cat/region/README.md",
                "cat/region/live/versions.json",
                "cat/region/live/data.parquet",
            ),
        ):
            plan = build_prune_plan(store, "cat", tmp_path, local_collections=["region/live"])

        assert plan.delete_count == 0
        assert plan.refuse_count == 0

    def test_collection_missing_remote_versions_json_is_not_wiped(self, tmp_path: Path) -> None:
        # push uploads versions.json last, so a live collection can have assets
        # remote already with no remote versions.json yet.
        _write_versions(tmp_path, "live", ["live/data.parquet"])
        store = MagicMock()
        with patch(
            "portolan_cli.sync.prune.obs.list",
            return_value=_remote_meta("cat/live/data.parquet"),
        ):
            plan = build_prune_plan(store, "cat", tmp_path, local_collections=["live"])

        assert plan.delete_count == 0
        assert plan.refuse_count == 0

    def test_nested_collection_prefix_matches_most_specific(self, tmp_path: Path) -> None:
        _write_versions(tmp_path, "region/subregion", ["region/subregion/data.parquet"])
        store = MagicMock()
        with patch(
            "portolan_cli.sync.prune.obs.list",
            return_value=_remote_meta(
                "cat/region/versions.json",
                "cat/region/subregion/versions.json",
                "cat/region/subregion/data.parquet",
            ),
        ):
            plan = build_prune_plan(store, "cat", tmp_path, local_collections=["region/subregion"])

        assert plan.refuse_count == 0
        assert plan.delete_count == 1
        assert plan.delete[0].prefix == "region"

    def test_local_versions_json_read_once_per_collection(self, tmp_path: Path) -> None:
        _write_versions(tmp_path, "live", ["live/a.parquet", "live/b.parquet"])
        store = MagicMock()
        with (
            patch(
                "portolan_cli.sync.prune.obs.list",
                return_value=_remote_meta(
                    "cat/live/versions.json", "cat/live/a.parquet", "cat/live/b.parquet"
                ),
            ),
            patch("portolan_cli.sync.prune._local_asset_hrefs") as mock_hrefs,
        ):
            mock_hrefs.return_value = {"live/a.parquet", "live/b.parquet"}
            plan = build_prune_plan(store, "cat", tmp_path, local_collections=["live"])

        assert plan.delete_count == 0
        assert plan.refuse_count == 0
        mock_hrefs.assert_called_once_with(tmp_path, "live")

    def test_corrupted_local_versions_json_does_not_crash(self, tmp_path: Path) -> None:
        coll_dir = tmp_path / "live"
        coll_dir.mkdir()
        (coll_dir / "versions.json").write_text("{not valid json")
        store = MagicMock()
        with patch(
            "portolan_cli.sync.prune.obs.list",
            return_value=_remote_meta("cat/live/versions.json", "cat/live/data.parquet"),
        ):
            plan = build_prune_plan(store, "cat", tmp_path, local_collections=["live"])

        assert plan.delete_count == 0
        assert plan.refuse_count == 1


@pytest.mark.unit
def test_list_remote_keys_uses_trailing_slash_boundary() -> None:
    from portolan_cli.sync.prune import _list_remote_keys

    store = MagicMock()
    with patch("portolan_cli.sync.prune.obs.list", return_value=_remote_meta()) as mock_list:
        _list_remote_keys(store, "acme")

    assert mock_list.call_args.kwargs["prefix"] == "acme/"


@pytest.mark.unit
class TestDeletePrunePlan:
    def test_deletes_only_delete_group(self) -> None:
        store = MagicMock()
        plan = PrunePlan()
        from portolan_cli.sync.prune import PruneGroup

        plan.delete = [PruneGroup(prefix="stale", keys=["cat/stale/a.parquet"])]
        plan.refuse = [PruneGroup(prefix="live", keys=["cat/live/extra.parquet"])]

        with patch("portolan_cli.sync.prune.obs.delete") as mock_delete:
            deleted, errors = delete_prune_plan(store, plan)

        assert deleted == 1
        assert errors == []
        mock_delete.assert_called_once_with(store, "cat/stale/a.parquet")

    def test_continues_on_individual_delete_failure(self) -> None:
        store = MagicMock()
        plan = PrunePlan()
        from portolan_cli.sync.prune import PruneGroup

        plan.delete = [PruneGroup(prefix="stale", keys=["a", "b"])]

        with patch("portolan_cli.sync.prune.obs.delete", side_effect=[Exception("boom"), None]):
            deleted, errors = delete_prune_plan(store, plan)

        assert deleted == 1
        assert errors == ["Failed to delete a: boom"]


@pytest.mark.unit
def test_print_prune_plan_empty_reports_nothing_to_prune() -> None:
    with patch("portolan_cli.sync.prune.info") as mock_info:
        print_prune_plan(PrunePlan())

    mock_info.assert_called_once()
    assert "Nothing to prune" in mock_info.call_args[0][0]
