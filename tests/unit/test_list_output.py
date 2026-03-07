"""Unit tests for _list_tree_output showing all assets grouped by item.

Tests verify that:
1. All assets are displayed (not just the first one)
2. Assets are grouped by item under each collection
3. Item directories show asset counts
4. Each asset shows format and file size
5. Collections are sorted alphabetically
6. Items within collections are sorted alphabetically

Fixes: https://github.com/portolan-sdi/portolan-cli/issues/196

Expected output format:
    -> censo-2010/
        data/ (3 assets)
          metadata.parquet (GeoParquet, 1.2MB)
          census-data.parquet (GeoParquet, 4.5MB)
          overview.pmtiles (PMTiles, 800KB)
        radios/ (1 asset)
          radios.parquet (GeoParquet, 2.1MB)
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from portolan_cli.dataset import DatasetInfo
from portolan_cli.formats import FormatType

# =============================================================================
# Helpers
# =============================================================================

_CATALOG_ROOT = Path("/tmp/test-catalog")


def _make_dataset(
    item_id: str,
    collection_id: str,
    asset_paths: list[str],
    format_type: FormatType = FormatType.VECTOR,
) -> DatasetInfo:
    """Create a DatasetInfo with the given asset paths."""
    return DatasetInfo(
        item_id=item_id,
        collection_id=collection_id,
        format_type=format_type,
        bbox=[0, 0, 1, 1],
        asset_paths=asset_paths,
    )


def _capture_list_output(
    datasets: list[DatasetInfo],
    catalog_path: Path = _CATALOG_ROOT,
) -> str:
    """Call _list_tree_output and capture printed output.

    Patches all output functions (info, detail) and the file size lookup
    to avoid filesystem access.
    """
    from portolan_cli.cli import _list_tree_output

    lines: list[str] = []

    def _collect(msg: str) -> None:
        lines.append(str(msg))

    with (
        patch("portolan_cli.cli.info_output", side_effect=_collect),
        patch("portolan_cli.cli.detail", side_effect=_collect),
        patch("portolan_cli.cli._get_asset_file_size", return_value=None),
    ):
        _list_tree_output(datasets, catalog_path)

    return "\n".join(lines)


def _capture_list_output_with_sizes(
    datasets: list[DatasetInfo],
    size_map: dict[str, int],
    catalog_path: Path = _CATALOG_ROOT,
) -> str:
    """Like _capture_list_output but with a file size mapping.

    Args:
        datasets: List of DatasetInfo objects.
        size_map: Mapping of asset href substring to size in bytes.
        catalog_path: Catalog root path.
    """
    from portolan_cli.cli import _list_tree_output

    lines: list[str] = []

    def _collect(msg: str) -> None:
        lines.append(str(msg))

    def _fake_size(_cat_path: Path, _col_id: str, asset_href: str) -> int | None:
        for key, size in size_map.items():
            if key in asset_href:
                return size
        return None

    with (
        patch("portolan_cli.cli.info_output", side_effect=_collect),
        patch("portolan_cli.cli.detail", side_effect=_collect),
        patch("portolan_cli.cli._get_asset_file_size", side_effect=_fake_size),
    ):
        _list_tree_output(datasets, catalog_path)

    return "\n".join(lines)


# =============================================================================
# Core: All assets displayed (issue #196 regression tests)
# =============================================================================


@pytest.mark.unit
class TestAllAssetsDisplayed:
    """Verify that ALL assets per item are shown, not just the first."""

    def test_single_asset_shown(self) -> None:
        """An item with one asset displays that asset."""
        ds = _make_dataset("data", "censo-2010", ["./data.parquet"])
        output = _capture_list_output([ds])

        assert "data.parquet" in output

    def test_multiple_assets_all_shown(self) -> None:
        """An item with multiple assets displays ALL of them."""
        ds = _make_dataset(
            "data",
            "censo-2010",
            ["./metadata.parquet", "./census-data.parquet", "./overview.pmtiles"],
        )
        output = _capture_list_output([ds])

        assert "metadata.parquet" in output
        assert "census-data.parquet" in output
        assert "overview.pmtiles" in output

    def test_first_asset_not_only_asset(self) -> None:
        """Regression: second and third assets must also appear (issue #196)."""
        ds = _make_dataset(
            "data",
            "censo-2010",
            ["./first.parquet", "./second.parquet", "./third.pmtiles"],
        )
        output = _capture_list_output([ds])

        assert "second.parquet" in output
        assert "third.pmtiles" in output

    def test_many_assets_all_shown(self) -> None:
        """Even items with many assets show all of them."""
        paths = [f"./file_{i}.parquet" for i in range(10)]
        ds = _make_dataset("data", "collection-a", paths)
        output = _capture_list_output([ds])

        for i in range(10):
            assert f"file_{i}.parquet" in output


# =============================================================================
# Grouping: Assets grouped by item within collection
# =============================================================================


@pytest.mark.unit
class TestItemGrouping:
    """Assets must be grouped under their parent item directory."""

    def test_item_directory_shown_as_header(self) -> None:
        """Each item appears as a directory header line."""
        ds = _make_dataset("data", "censo-2010", ["./data.parquet"])
        output = _capture_list_output([ds])

        # Item "data" should appear as a directory header
        assert "data/" in output

    def test_multiple_items_each_get_header(self) -> None:
        """Multiple items in same collection each get their own header."""
        ds1 = _make_dataset("data", "censo-2010", ["./metadata.parquet"])
        ds2 = _make_dataset("radios", "censo-2010", ["./radios.parquet"])
        output = _capture_list_output([ds1, ds2])

        assert "data/" in output
        assert "radios/" in output

    def test_item_header_shows_asset_count_plural(self) -> None:
        """Item header shows count of assets (plural form)."""
        ds = _make_dataset(
            "data",
            "censo-2010",
            ["./a.parquet", "./b.parquet", "./c.pmtiles"],
        )
        output = _capture_list_output([ds])

        assert "3 assets" in output

    def test_item_header_shows_asset_count_singular(self) -> None:
        """Item header shows '1 asset' (singular) for single asset."""
        ds = _make_dataset("radios", "censo-2010", ["./radios.parquet"])
        output = _capture_list_output([ds])

        assert "1 asset" in output

    def test_assets_indented_under_item(self) -> None:
        """Assets are indented deeper than their item header."""
        ds = _make_dataset("data", "censo-2010", ["./data.parquet"])
        output = _capture_list_output([ds])
        lines = output.split("\n")

        # Find the item header line and the asset line
        item_line = next(line for line in lines if "data/" in line and "asset" in line)
        asset_line = next(line for line in lines if "data.parquet" in line)

        # Asset line should have more leading spaces than item header
        item_indent = len(item_line) - len(item_line.lstrip())
        asset_indent = len(asset_line) - len(asset_line.lstrip())
        assert asset_indent > item_indent


# =============================================================================
# Per-asset format display
# =============================================================================


@pytest.mark.unit
class TestAssetFormatDisplay:
    """Each asset shows its own format type based on file extension."""

    def test_parquet_shows_geoparquet(self) -> None:
        """A .parquet asset is displayed as GeoParquet."""
        ds = _make_dataset("data", "col-a", ["./data.parquet"])
        output = _capture_list_output([ds])

        assert "GeoParquet" in output

    def test_pmtiles_shows_pmtiles(self) -> None:
        """A .pmtiles asset is displayed as PMTiles."""
        ds = _make_dataset("data", "col-a", ["./overview.pmtiles"])
        output = _capture_list_output([ds])

        assert "PMTiles" in output

    def test_tif_shows_cog(self) -> None:
        """A .tif asset is displayed as COG."""
        ds = _make_dataset("data", "col-a", ["./image.tif"], FormatType.RASTER)
        output = _capture_list_output([ds])

        assert "COG" in output

    def test_mixed_formats_each_asset_correct(self) -> None:
        """An item with mixed asset types shows the correct format for each."""
        ds = _make_dataset(
            "data",
            "col-a",
            ["./data.parquet", "./tiles.pmtiles", "./thumb.png"],
        )
        output = _capture_list_output([ds])

        # Each asset line should have the correct format
        assert "GeoParquet" in output
        assert "PMTiles" in output
        # .png is not a recognized geo format
        assert "thumb.png" in output

    def test_unknown_extension_shows_extension(self) -> None:
        """An asset with unknown extension shows the extension as format."""
        ds = _make_dataset("data", "col-a", ["./readme.txt"])
        output = _capture_list_output([ds])

        assert "readme.txt" in output


# =============================================================================
# File size display
# =============================================================================


@pytest.mark.unit
class TestFileSizeDisplay:
    """Each asset shows its file size when available."""

    def test_size_shown_when_available(self) -> None:
        """File size appears in parentheses when available."""
        ds = _make_dataset("data", "col-a", ["./data.parquet"])
        output = _capture_list_output_with_sizes([ds], {"data.parquet": 1_200_000})

        assert "1.1MB" in output

    def test_size_omitted_when_unavailable(self) -> None:
        """No size suffix when file size is not available."""
        ds = _make_dataset("data", "col-a", ["./data.parquet"])
        output = _capture_list_output([ds])

        # Should show format but no size
        assert "GeoParquet" in output
        # There should be no comma+size after format if size is None
        assert "GeoParquet)" in output

    def test_multiple_assets_each_with_size(self) -> None:
        """Each asset gets its own file size."""
        ds = _make_dataset(
            "data",
            "col-a",
            ["./small.parquet", "./large.parquet"],
        )
        output = _capture_list_output_with_sizes(
            [ds],
            {"small.parquet": 500, "large.parquet": 5_000_000},
        )

        assert "500B" in output
        assert "4.8MB" in output


# =============================================================================
# Collection sorting and structure
# =============================================================================


@pytest.mark.unit
class TestCollectionStructure:
    """Collections are sorted alphabetically with items nested inside."""

    def test_collections_sorted_alphabetically(self) -> None:
        """Collections appear in alphabetical order."""
        ds1 = _make_dataset("data", "zebra", ["./data.parquet"])
        ds2 = _make_dataset("data", "alpha", ["./data.parquet"])
        output = _capture_list_output([ds1, ds2])

        alpha_pos = output.find("alpha/")
        zebra_pos = output.find("zebra/")
        assert alpha_pos < zebra_pos

    def test_items_sorted_within_collection(self) -> None:
        """Items within a collection are sorted alphabetically."""
        ds1 = _make_dataset("radios", "censo-2010", ["./radios.parquet"])
        ds2 = _make_dataset("data", "censo-2010", ["./data.parquet"])
        output = _capture_list_output([ds1, ds2])

        data_pos = output.find("data/")
        radios_pos = output.find("radios/")
        assert data_pos < radios_pos

    def test_multiple_collections_with_multiple_items(self) -> None:
        """Full hierarchical output with multiple collections and items."""
        datasets = [
            _make_dataset(
                "data",
                "censo-2010",
                ["./metadata.parquet", "./census.parquet", "./tiles.pmtiles"],
            ),
            _make_dataset(
                "radios",
                "censo-2010",
                ["./radios.parquet"],
            ),
            _make_dataset(
                "data",
                "censo-2022",
                ["./data.parquet", "./overview.pmtiles"],
            ),
        ]
        output = _capture_list_output(datasets)

        # Collections sorted
        assert output.find("censo-2010/") < output.find("censo-2022/")

        # All assets present
        assert "metadata.parquet" in output
        assert "census.parquet" in output
        assert "tiles.pmtiles" in output
        assert "radios.parquet" in output
        assert "data.parquet" in output
        assert "overview.pmtiles" in output

        # Item headers with counts
        assert "3 assets" in output
        assert "1 asset" in output
        assert "2 assets" in output


# =============================================================================
# Edge cases
# =============================================================================


@pytest.mark.unit
class TestListEdgeCases:
    """Edge cases for the list tree output."""

    def test_empty_datasets_produces_no_output(self) -> None:
        """No datasets means no output."""
        output = _capture_list_output([])

        assert output.strip() == ""

    def test_item_with_no_assets(self) -> None:
        """An item with empty asset_paths still displays (shows 0 assets)."""
        ds = _make_dataset("empty", "col-a", [])
        output = _capture_list_output([ds])

        assert "empty/" in output
        assert "0 assets" in output

    def test_asset_href_with_subdirectory(self) -> None:
        """Asset hrefs with subdirectories show only the filename."""
        ds = _make_dataset("data", "col-a", ["./subdir/data.parquet"])
        output = _capture_list_output([ds])

        assert "data.parquet" in output

    def test_asset_href_without_dot_slash_prefix(self) -> None:
        """Asset hrefs without './' prefix are handled correctly."""
        ds = _make_dataset("data", "col-a", ["data.parquet"])
        output = _capture_list_output([ds])

        assert "data.parquet" in output

    def test_single_collection_single_item_single_asset(self) -> None:
        """Minimal case: one collection, one item, one asset."""
        ds = _make_dataset("data", "boundaries", ["./borders.parquet"])
        output = _capture_list_output([ds])

        assert "boundaries/" in output
        assert "data/" in output
        assert "1 asset" in output
        assert "borders.parquet" in output
        assert "GeoParquet" in output


# =============================================================================
# Hypothesis property-based tests
# =============================================================================


@pytest.mark.unit
class TestListOutputProperties:
    """Property-based tests using Hypothesis."""

    @pytest.fixture(autouse=True)
    def _import_hypothesis(self) -> None:
        """Import hypothesis lazily to keep test collection fast."""
        pass

    def test_all_assets_appear_in_output(self) -> None:
        """Every asset path from every item must appear in the output."""
        from hypothesis import given, settings
        from hypothesis import strategies as st

        @given(
            n_collections=st.integers(min_value=1, max_value=5),
            n_items_per=st.integers(min_value=1, max_value=4),
            n_assets_per=st.integers(min_value=1, max_value=6),
        )
        @settings(max_examples=50, deadline=2000)
        def _check(n_collections: int, n_items_per: int, n_assets_per: int) -> None:
            datasets: list[DatasetInfo] = []
            all_asset_names: list[str] = []
            for c in range(n_collections):
                col_id = f"col_{c}"
                for i in range(n_items_per):
                    item_id = f"item_{c}_{i}"
                    paths = []
                    for a in range(n_assets_per):
                        name = f"asset_{c}_{i}_{a}.parquet"
                        paths.append(f"./{name}")
                        all_asset_names.append(name)
                    datasets.append(_make_dataset(item_id, col_id, paths))

            output = _capture_list_output(datasets)

            for name in all_asset_names:
                assert name in output, f"Asset {name!r} not found in output"

        _check()

    def test_collection_count_matches_input(self) -> None:
        """The number of collection headers matches distinct collection IDs."""
        from hypothesis import given, settings
        from hypothesis import strategies as st

        @given(
            n_collections=st.integers(min_value=1, max_value=8),
        )
        @settings(max_examples=30, deadline=2000)
        def _check(n_collections: int) -> None:
            datasets = [
                _make_dataset("data", f"col_{c}", ["./data.parquet"]) for c in range(n_collections)
            ]
            output = _capture_list_output(datasets)

            for c in range(n_collections):
                assert f"col_{c}/" in output

        _check()

    def test_item_asset_count_in_header(self) -> None:
        """Each item header shows the correct asset count."""
        from hypothesis import given, settings
        from hypothesis import strategies as st

        @given(
            n_assets=st.integers(min_value=0, max_value=10),
        )
        @settings(max_examples=30, deadline=2000)
        def _check(n_assets: int) -> None:
            paths = [f"./file_{a}.parquet" for a in range(n_assets)]
            ds = _make_dataset("data", "col-a", paths)
            output = _capture_list_output([ds])

            expected = f"{n_assets} asset" if n_assets != 1 else "1 asset"
            assert expected in output

        _check()
