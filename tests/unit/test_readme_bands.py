"""Tests for the README Bands section (#713).

The CLI writes the STAC v1.1.0 unified ``bands`` array on the data asset
(``stac.py:_set_bands_on_data_assets``), item-level for rasters. The Bands
section used to read ``summaries["eo:bands"]`` / ``["raster:bands"]``, which
nothing writes, so it never rendered. These tests pin the asset-level read,
the item walk that reaches it, and the columns that drop when unpopulated.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

# Band shape the CLI actually writes: name + data_type from metadata/cog.py,
# statistics merged in by preparation.py.
CLI_BAND: dict[str, Any] = {
    "name": "band_1",
    "data_type": "int16",
    "nodata": 0,
    "statistics": {"minimum": 302.0, "maximum": 1015.0, "mean": 512.4, "stddev": 88.1},
}

# Band shape the portolan-spec reference collection carries: no name, and a
# valid_percent the CLI does not currently compute.
SPEC_BAND: dict[str, Any] = {
    "data_type": "uint8",
    "statistics": {
        "minimum": 1.0,
        "maximum": 255.0,
        "mean": 44.396,
        "stddev": 58.4784,
        "valid_percent": 67.4572,
    },
}


def _bands_block(readme: str) -> str:
    """Return the Bands section, from its heading to the next heading."""
    assert "## Bands" in readme, f"no Bands section in:\n{readme}"
    after = readme.split("## Bands", 1)[1]
    return after.split("\n## ", 1)[0]


class TestBandsFromAssets:
    """Bands render from the unified array on the data asset."""

    @pytest.mark.unit
    def test_collection_level_data_asset_renders(self) -> None:
        """A single-file raster collection carries bands on collection.assets."""
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "sample-cog",
            "assets": {
                "data": {
                    "href": "sample-cog.tif",
                    "roles": ["data"],
                    "bands": [SPEC_BAND],
                }
            },
        }

        block = _bands_block(generate_readme(stac=stac, metadata={}))

        assert "| Band | Data Type | Min | Max | Mean | Std Dev | Valid % |" in block
        assert "| 1 | uint8 | 1.0 | 255.0 | 44.396 | 58.4784 | 67.4572 |" in block

    @pytest.mark.unit
    def test_item_level_data_asset_renders(self) -> None:
        """Raster scenes sit on items, so the item walk must reach their bands."""
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "imagery",
            "items": [
                {
                    "id": "scene-a",
                    "assets": {
                        "data": {
                            "href": "./scene-a.tif",
                            "roles": ["data"],
                            "bands": [CLI_BAND],
                        }
                    },
                }
            ],
        }

        block = _bands_block(generate_readme(stac=stac, metadata={}))

        assert "| Band | Name | Data Type | Nodata | Min | Max | Mean | Std Dev |" in block
        assert "| 1 | band_1 | int16 | 0 | 302.0 | 1015.0 | 512.4 | 88.1 |" in block

    @pytest.mark.unit
    def test_data_role_asset_wins_over_thumbnail(self) -> None:
        """A thumbnail carrying bands must not shadow the data asset."""
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "imagery",
            "assets": {
                "thumbnail": {
                    "href": "thumb.png",
                    "roles": ["thumbnail"],
                    "bands": [{"name": "rgb", "data_type": "uint8"}],
                },
                "data": {
                    "href": "scene.tif",
                    "roles": ["data"],
                    "bands": [CLI_BAND],
                },
            },
        }

        block = _bands_block(generate_readme(stac=stac, metadata={}))

        assert "band_1" in block
        assert "rgb" not in block

    @pytest.mark.unit
    def test_every_band_gets_a_row(self) -> None:
        """A four-band COG renders four rows, numbered from one."""
        from portolan_cli.readme import generate_readme

        bands = [dict(CLI_BAND, name=f"band_{i}") for i in range(1, 5)]
        stac = {
            "type": "Collection",
            "id": "imagery",
            "assets": {"data": {"href": "s.tif", "roles": ["data"], "bands": bands}},
        }

        block = _bands_block(generate_readme(stac=stac, metadata={}))

        for i in range(1, 5):
            assert f"| {i} | band_{i} |" in block


class TestBandsColumnSelection:
    """Columns no band populates are dropped rather than rendered empty."""

    @pytest.mark.unit
    def test_no_statistics_drops_stat_columns(self) -> None:
        """Bands without statistics render a narrow identity table."""
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "imagery",
            "assets": {
                "data": {
                    "href": "s.tif",
                    "roles": ["data"],
                    "bands": [{"name": "band_1", "data_type": "int16", "nodata": 0}],
                }
            },
        }

        block = _bands_block(generate_readme(stac=stac, metadata={}))

        assert "| Band | Name | Data Type | Nodata |" in block
        assert "Min" not in block
        assert "Std Dev" not in block

    @pytest.mark.unit
    def test_no_nodata_drops_nodata_column(self) -> None:
        """rapidai4eo-sample.tif reports no nodata, so the column must vanish."""
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "imagery",
            "assets": {
                "data": {
                    "href": "s.tif",
                    "roles": ["data"],
                    "bands": [{"name": "band_1", "data_type": "int16"}],
                }
            },
        }

        block = _bands_block(generate_readme(stac=stac, metadata={}))

        assert "| Band | Name | Data Type |" in block
        assert "Nodata" not in block

    @pytest.mark.unit
    def test_zero_nodata_keeps_the_column(self) -> None:
        """nodata=0 is a real value, so falsiness must not drop the column."""
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "imagery",
            "assets": {
                "data": {
                    "href": "s.tif",
                    "roles": ["data"],
                    "bands": [{"name": "band_1", "data_type": "int16", "nodata": 0}],
                }
            },
        }

        block = _bands_block(generate_readme(stac=stac, metadata={}))

        assert "Nodata" in block
        assert "| 1 | band_1 | int16 | 0 |" in block

    @pytest.mark.unit
    def test_partial_column_renders_dash_for_missing(self) -> None:
        """One band with nodata keeps the column; the other shows a dash."""
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "imagery",
            "assets": {
                "data": {
                    "href": "s.tif",
                    "roles": ["data"],
                    "bands": [
                        {"name": "band_1", "data_type": "int16", "nodata": -9999},
                        {"name": "band_2", "data_type": "int16"},
                    ],
                }
            },
        }

        block = _bands_block(generate_readme(stac=stac, metadata={}))

        assert "| 1 | band_1 | int16 | -9999 |" in block
        assert "| 2 | band_2 | int16 | - |" in block


class TestBandsAbsentAndLegacy:
    """No bands means no section; legacy summaries still render."""

    @pytest.mark.unit
    def test_no_bands_omits_section(self) -> None:
        """A vector collection gets no Bands heading."""
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "boundaries",
            "assets": {"data": {"href": "b.parquet", "roles": ["data"]}},
        }

        assert "## Bands" not in generate_readme(stac=stac, metadata={})

    @pytest.mark.unit
    def test_empty_bands_array_omits_section(self) -> None:
        """An empty array is not a band list."""
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "imagery",
            "assets": {"data": {"href": "s.tif", "roles": ["data"], "bands": []}},
        }

        assert "## Bands" not in generate_readme(stac=stac, metadata={})

    @pytest.mark.unit
    def test_legacy_eo_bands_summaries_still_render(self) -> None:
        """Hand-authored catalogs predating STAC 1.1.0 keep working."""
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "imagery",
            "summaries": {
                "eo:bands": [
                    {"name": "B04", "common_name": "red", "description": "Red band"},
                ]
            },
        }

        block = _bands_block(generate_readme(stac=stac, metadata={}))

        assert "| 1 | B04 | red | Red band |" in block

    @pytest.mark.unit
    def test_asset_bands_win_over_legacy_summaries(self) -> None:
        """The array the CLI writes takes precedence over a stale summary."""
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "imagery",
            "summaries": {"raster:bands": [{"name": "stale"}]},
            "assets": {"data": {"href": "s.tif", "roles": ["data"], "bands": [CLI_BAND]}},
        }

        block = _bands_block(generate_readme(stac=stac, metadata={}))

        assert "band_1" in block
        assert "stale" not in block


class TestItemAssetHrefs:
    """Item asset hrefs are rebased onto the collection the README sits in."""

    @pytest.mark.unit
    def test_item_relative_href_gains_the_item_directory(self) -> None:
        """An item's ./scene-a.tif must render as scene-a/scene-a.tif."""
        from portolan_cli.readme import _collection_relative_href

        assert _collection_relative_href("./scene-a.tif", "scene-a") == "scene-a/scene-a.tif"

    @pytest.mark.unit
    def test_flat_layout_keeps_the_bare_name(self) -> None:
        """An item JSON beside collection.json has no directory to prepend."""
        from portolan_cli.readme import _collection_relative_href

        assert _collection_relative_href("./scene-a.tif", ".") == "scene-a.tif"

    @pytest.mark.unit
    def test_url_is_untouched(self) -> None:
        """A remote href already resolves and must not be prefixed."""
        from portolan_cli.readme import _collection_relative_href

        url = "https://data.example.org/scene-a.tif"
        assert _collection_relative_href(url, "scene-a") == url

    @pytest.mark.unit
    def test_absolute_path_is_untouched(self) -> None:
        """An absolute path must not be turned into a relative one."""
        from portolan_cli.readme import _collection_relative_href

        assert _collection_relative_href("/data/scene-a.tif", "scene-a") == "/data/scene-a.tif"

    @pytest.mark.unit
    def test_collection_asset_href_is_untouched(self) -> None:
        """Collection-level hrefs are already collection-relative."""
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "boundaries",
            "assets": {"data": {"href": "boundaries.parquet", "file:size": 10}},
        }

        readme = generate_readme(stac=stac, metadata={})

        assert "| boundaries.parquet |" in readme


def _write_raster_collection(catalog_root: Path) -> Path:
    """Write a catalog whose one collection holds a banded scene item on disk."""
    catalog_root.mkdir(parents=True, exist_ok=True)
    (catalog_root / ".portolan").mkdir(exist_ok=True)
    (catalog_root / ".portolan" / "config.yaml").write_text("version: 1\n", encoding="utf-8")
    (catalog_root / "catalog.json").write_text(
        json.dumps(
            {
                "type": "Catalog",
                "stac_version": "1.1.0",
                "id": "test-catalog",
                "description": "Test catalog",
                "links": [{"rel": "child", "href": "./imagery/collection.json"}],
            }
        ),
        encoding="utf-8",
    )

    collection_dir = catalog_root / "imagery"
    item_dir = collection_dir / "scene-a"
    item_dir.mkdir(parents=True)
    (item_dir / "scene-a.json").write_text(
        json.dumps(
            {
                "type": "Feature",
                "stac_version": "1.1.0",
                "id": "scene-a",
                "properties": {"datetime": "2024-01-01T00:00:00Z"},
                "assets": {
                    "data": {
                        "href": "./scene-a.tif",
                        "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                        "roles": ["data"],
                        "file:size": 205061,
                        "bands": [CLI_BAND],
                    }
                },
                "links": [],
            }
        ),
        encoding="utf-8",
    )
    (collection_dir / "collection.json").write_text(
        json.dumps(
            {
                "type": "Collection",
                "stac_version": "1.1.0",
                "id": "imagery",
                "description": "Raster imagery",
                "license": "CC0-1.0",
                "extent": {
                    "spatial": {"bbox": [[-1.0, -1.0, 1.0, 1.0]]},
                    "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
                },
                "links": [{"rel": "item", "href": "./scene-a/scene-a.json"}],
            }
        ),
        encoding="utf-8",
    )
    return collection_dir


class TestBandsFromDisk:
    """generate_readme_for_collection must load items off disk to see bands."""

    @pytest.mark.unit
    def test_item_bands_render_from_disk(self, tmp_path: Path) -> None:
        """The regression: bands on disk reached no README before #713."""
        from portolan_cli.readme import generate_readme_for_collection

        catalog_root = tmp_path / "catalog"
        collection_dir = _write_raster_collection(catalog_root)

        readme = generate_readme_for_collection(collection_dir, catalog_root)

        assert "| 1 | band_1 | int16 | 0 | 302.0 | 1015.0 | 512.4 | 88.1 |" in _bands_block(readme)

    @pytest.mark.unit
    def test_item_data_file_reaches_files_table(self, tmp_path: Path) -> None:
        """The same dead item walk kept the COG out of the Files table."""
        from portolan_cli.readme import generate_readme_for_collection

        catalog_root = tmp_path / "catalog"
        collection_dir = _write_raster_collection(catalog_root)

        readme = generate_readme_for_collection(collection_dir, catalog_root)

        assert "| scene-a/scene-a.tif | 200.3 KB |" in readme
