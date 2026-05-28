"""Regression tests for STAC v1.1.0 raster bands placement (issue #437).

STAC v1.1.0 unified the bands model: ``bands`` is an **asset-level** field.
The core item schema explicitly forbids ``bands`` on ``item.properties``, so
raster items that put it there fail STAC schema validation.

These tests lock in the fix: the unified ``bands`` array (carrying per-band
``data_type``, ``nodata`` and ``statistics``) must live on the data asset, and
a freshly added raster item must pass schema validation via the same engine
``portolan check`` uses (``stac-check``).
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pytest
import yaml
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.fixture
def raster_catalog(tmp_path: Path) -> Path:
    """Catalog with statistics enabled, so emitted bands carry a statistics dict."""
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()
    (catalog_root / "catalog.json").write_text(
        json.dumps(
            {
                "type": "Catalog",
                "stac_version": "1.1.0",
                "id": "test-catalog",
                "description": "Test catalog",
                "links": [],
            }
        )
    )
    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text(
        yaml.dump({"version": 1, "statistics": {"enabled": True}})
    )
    (catalog_root / "imagery").mkdir()
    return catalog_root


@pytest.fixture
def float32_raster(tmp_path: Path) -> Path:
    """A small 2-band float32 raster (reproduces the dtype from the bug report)."""
    pytest.importorskip("rasterio")
    import numpy as np
    import rasterio
    from rasterio.transform import from_bounds

    path = tmp_path / "elevation.tif"
    data = np.random.default_rng(0).random((2, 16, 16), dtype="float32")
    transform = from_bounds(-75.2, 39.9, -75.1, 40.0, 16, 16)
    profile = {
        "driver": "GTiff",
        "dtype": "float32",
        "width": 16,
        "height": 16,
        "count": 2,
        "crs": "EPSG:4326",
        "transform": transform,
        "nodata": -9999.0,
    }
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data)
    return path


def _add_raster(catalog_root: Path, raster: Path) -> dict[str, Any]:
    """Run `portolan add` on a raster and return the parsed item.json."""
    item_dir = catalog_root / "imagery" / "dem-x"
    item_dir.mkdir(parents=True)
    dest = item_dir / "elevation.tif"
    shutil.copy(raster, dest)

    result = CliRunner().invoke(
        cli,
        ["add", "--portolan-dir", str(catalog_root), str(dest)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    item_json = item_dir / "dem-x.json"
    assert item_json.exists(), f"Expected {item_json}"
    item: dict[str, Any] = json.loads(item_json.read_text())
    return item


@pytest.mark.integration
class TestRasterBandsPlacement:
    """Bands must live on the data asset, never on item.properties (STAC v1.1.0)."""

    def test_bands_not_on_item_properties(self, raster_catalog: Path, float32_raster: Path) -> None:
        item = _add_raster(raster_catalog, float32_raster)
        assert "bands" not in item["properties"], "STAC v1.1.0 forbids `bands` on item.properties"

    def test_bands_on_data_asset(self, raster_catalog: Path, float32_raster: Path) -> None:
        item = _add_raster(raster_catalog, float32_raster)
        bands = item["assets"]["data"].get("bands")
        assert isinstance(bands, list) and len(bands) == 2, f"Expected 2 asset bands, got {bands}"
        for band in bands:
            assert "name" in band and "data_type" in band

    def test_band_statistics_preserved_on_asset(
        self, raster_catalog: Path, float32_raster: Path
    ) -> None:
        item = _add_raster(raster_catalog, float32_raster)
        bands = item["assets"]["data"]["bands"]
        assert any("statistics" in band for band in bands), (
            "per-band statistics must survive relocation onto the asset"
        )

    def test_raster_item_passes_stac_schema_validation(
        self, raster_catalog: Path, float32_raster: Path
    ) -> None:
        from portolan_cli.validation.stac_rules import StacSchemaRule

        _add_raster(raster_catalog, float32_raster)
        result = StacSchemaRule().check(raster_catalog)
        assert result.passed, f"STAC schema validation failed: {result.message}"
