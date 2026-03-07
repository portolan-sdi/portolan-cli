"""Integration tests for ``portolan list`` showing all assets grouped by item.

Verifies the CLI output through Click's CliRunner with a realistic STAC catalog
structure on the filesystem.  Tests that all assets appear in hierarchical output.

Fixes: https://github.com/portolan-sdi/portolan-cli/issues/196
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.fixture()
def runner() -> CliRunner:
    """Create a Click test runner."""
    return CliRunner()


def _create_stac_catalog(root: Path) -> None:
    """Create a minimal STAC catalog.json at root."""
    (root / "catalog.json").write_text(
        json.dumps(
            {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "test-catalog",
                "description": "Test catalog",
                "links": [],
            },
            indent=2,
        )
    )


def _create_collection(
    root: Path,
    collection_id: str,
    item_ids: list[str],
) -> None:
    """Create a collection directory with collection.json linking to items."""
    col_dir = root / collection_id
    col_dir.mkdir(exist_ok=True)

    links = [{"rel": "item", "href": f"./{item_id}/{item_id}.json"} for item_id in item_ids]

    (col_dir / "collection.json").write_text(
        json.dumps(
            {
                "type": "Collection",
                "stac_version": "1.0.0",
                "id": collection_id,
                "description": f"Collection {collection_id}",
                "extent": {
                    "spatial": {"bbox": [[-180, -90, 180, 90]]},
                    "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
                },
                "links": links,
            },
            indent=2,
        )
    )


def _create_item(
    root: Path,
    collection_id: str,
    item_id: str,
    asset_files: dict[str, str],
) -> None:
    """Create an item directory with item.json and actual asset files.

    Args:
        root: Catalog root directory.
        collection_id: Collection ID.
        item_id: Item ID.
        asset_files: Mapping of asset_key -> filename (e.g., {"data": "data.parquet"}).
    """
    item_dir = root / collection_id / item_id
    item_dir.mkdir(parents=True, exist_ok=True)

    assets = {}
    for key, filename in asset_files.items():
        # Create the actual file with some content for file size
        asset_path = item_dir / filename
        asset_path.write_text(f"fake data for {filename}")
        assets[key] = {"href": f"./{filename}"}

    (item_dir / f"{item_id}.json").write_text(
        json.dumps(
            {
                "type": "Feature",
                "stac_version": "1.0.0",
                "id": item_id,
                "geometry": None,
                "bbox": [0, 0, 1, 1],
                "properties": {"datetime": "2020-01-01T00:00:00Z"},
                "assets": assets,
                "links": [],
            },
            indent=2,
        )
    )


@pytest.fixture()
def multi_asset_catalog(tmp_path: Path) -> Path:
    """Create a catalog with multiple collections, items, and assets.

    Structure mirrors the censo-argentino test data from issue #196:
      censo-2010/
        data/  -> metadata.parquet, census.parquet, overview.pmtiles
        radios/ -> radios.parquet
      censo-2022/
        data/  -> data.parquet, summary.pmtiles
    """
    _create_stac_catalog(tmp_path)

    # censo-2010 with 2 items
    _create_collection(tmp_path, "censo-2010", ["data", "radios"])
    _create_item(
        tmp_path,
        "censo-2010",
        "data",
        {
            "metadata": "metadata.parquet",
            "census": "census.parquet",
            "overview": "overview.pmtiles",
        },
    )
    _create_item(
        tmp_path,
        "censo-2010",
        "radios",
        {"radios": "radios.parquet"},
    )

    # censo-2022 with 1 item
    _create_collection(tmp_path, "censo-2022", ["data"])
    _create_item(
        tmp_path,
        "censo-2022",
        "data",
        {"data": "data.parquet", "summary": "summary.pmtiles"},
    )

    return tmp_path


# =============================================================================
# Core: All assets displayed through CLI
# =============================================================================


@pytest.mark.integration
class TestListAllAssets:
    """CLI integration tests verifying all assets appear in list output."""

    def test_all_assets_visible(self, runner: CliRunner, multi_asset_catalog: Path) -> None:
        """All 6 assets across 3 items are displayed."""
        result = runner.invoke(cli, ["list", "--catalog", str(multi_asset_catalog)])

        assert result.exit_code == 0
        # All asset filenames must appear
        assert "metadata.parquet" in result.output
        assert "census.parquet" in result.output
        assert "overview.pmtiles" in result.output
        assert "radios.parquet" in result.output
        assert "data.parquet" in result.output
        assert "summary.pmtiles" in result.output

    def test_second_asset_not_hidden(self, runner: CliRunner, multi_asset_catalog: Path) -> None:
        """Regression test: assets beyond the first one are NOT hidden (#196)."""
        result = runner.invoke(cli, ["list", "--catalog", str(multi_asset_catalog)])

        assert result.exit_code == 0
        # These are NOT the first asset in their item -- they must still appear
        assert "census.parquet" in result.output
        assert "overview.pmtiles" in result.output
        assert "summary.pmtiles" in result.output

    def test_item_directories_shown(self, runner: CliRunner, multi_asset_catalog: Path) -> None:
        """Item directories appear as headers with asset counts."""
        result = runner.invoke(cli, ["list", "--catalog", str(multi_asset_catalog)])

        assert result.exit_code == 0
        # Item directories should be visible
        assert "data/" in result.output
        assert "radios/" in result.output

    def test_collection_headers_shown(self, runner: CliRunner, multi_asset_catalog: Path) -> None:
        """Collection headers are present."""
        result = runner.invoke(cli, ["list", "--catalog", str(multi_asset_catalog)])

        assert result.exit_code == 0
        assert "censo-2010/" in result.output
        assert "censo-2022/" in result.output

    def test_asset_counts_in_item_headers(
        self, runner: CliRunner, multi_asset_catalog: Path
    ) -> None:
        """Item headers show correct asset counts."""
        result = runner.invoke(cli, ["list", "--catalog", str(multi_asset_catalog)])

        assert result.exit_code == 0
        assert "3 assets" in result.output  # censo-2010/data
        assert "1 asset)" in result.output  # censo-2010/radios (singular)
        assert "2 assets" in result.output  # censo-2022/data

    def test_format_types_per_asset(self, runner: CliRunner, multi_asset_catalog: Path) -> None:
        """Each asset shows its own format type."""
        result = runner.invoke(cli, ["list", "--catalog", str(multi_asset_catalog)])

        assert result.exit_code == 0
        assert "GeoParquet" in result.output
        assert "PMTiles" in result.output

    def test_collections_alphabetically_sorted(
        self, runner: CliRunner, multi_asset_catalog: Path
    ) -> None:
        """Collections are sorted alphabetically."""
        result = runner.invoke(cli, ["list", "--catalog", str(multi_asset_catalog)])

        assert result.exit_code == 0
        pos_2010 = result.output.find("censo-2010/")
        pos_2022 = result.output.find("censo-2022/")
        assert pos_2010 < pos_2022

    def test_collection_filter(self, runner: CliRunner, multi_asset_catalog: Path) -> None:
        """--collection flag filters to one collection."""
        result = runner.invoke(
            cli,
            ["list", "--catalog", str(multi_asset_catalog), "--collection", "censo-2022"],
        )

        assert result.exit_code == 0
        # Should show censo-2022 assets
        assert "data.parquet" in result.output
        assert "summary.pmtiles" in result.output
        # Should NOT show censo-2010 assets
        assert "radios.parquet" not in result.output


# =============================================================================
# Edge cases
# =============================================================================


@pytest.mark.integration
class TestListEdgeCases:
    """Edge cases for the list CLI command."""

    def test_empty_catalog(self, runner: CliRunner, tmp_path: Path) -> None:
        """Empty catalog shows helpful message."""
        _create_stac_catalog(tmp_path)

        result = runner.invoke(cli, ["list", "--catalog", str(tmp_path)])

        assert result.exit_code == 0
        assert (
            "no tracked item" in result.output.lower() or "portolan scan" in result.output.lower()
        )

    def test_single_item_single_asset(self, runner: CliRunner, tmp_path: Path) -> None:
        """Minimal catalog with one item and one asset."""
        _create_stac_catalog(tmp_path)
        _create_collection(tmp_path, "boundaries", ["regions"])
        _create_item(tmp_path, "boundaries", "regions", {"data": "regions.parquet"})

        result = runner.invoke(cli, ["list", "--catalog", str(tmp_path)])

        assert result.exit_code == 0
        assert "boundaries/" in result.output
        assert "regions/" in result.output
        assert "regions.parquet" in result.output
        assert "1 asset" in result.output

    def test_json_output_still_works(self, runner: CliRunner, multi_asset_catalog: Path) -> None:
        """JSON output mode is unaffected by the tree output changes."""
        result = runner.invoke(
            cli,
            ["list", "--catalog", str(multi_asset_catalog), "--json"],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["success"] is True
        assert data["command"] == "list"

        # All items should be in JSON output
        items = data["data"]["items"]
        assert len(items) == 3  # 3 items total

        # Check that assets are included
        all_assets = []
        for item in items:
            all_assets.extend(item.get("assets", []))
        assert len(all_assets) == 6  # 6 total assets
