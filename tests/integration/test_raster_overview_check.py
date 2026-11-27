"""A raster collection with an overview COG and no style passes `check` (issue #1141).

The spec asks a collection of two or more rasters that together cover an area
much larger than any one item to provide an overview COG, as a collection-level
asset with the ``visual`` and ``overview`` roles (PORTO-FMT-067). The same spec
says a display-ready COG is self-rendering and needs no separate style file
(PORTO-CORE-067).

rashid 0.3.x and older read the overview as a visualization derivative and
asked for a ``style`` asset, so `check --fix --metadata` failed with
PTL-VIZ-002. A pipeline that follows the spec could not pass the gate. rashid
0.4.0 exempts the COG overview.

The catalog comes from the real `init` + `add` path, with four tiles cut from
the real rapidai4eo scene, and the overview is an 8-bit RGB COG of the whole
scene.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

import numpy as np
import pytest
import rasterio
from rasterio.windows import Window
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

if TYPE_CHECKING:
    from pathlib import Path

    from tests.catalog_invariants import RunCli

pytestmark = [pytest.mark.integration, pytest.mark.realdata]

COG_TYPE = "image/tiff; application=geotiff; profile=cloud-optimized"

METADATA_YAML = """\
license: CC-BY-4.0
contact:
  name: Example Data Team
  email: data@example.org
source_url: https://example.org/source
providers:
  - name: Example Statistics Agency
    roles: [producer, licensor]
  - name: Example Municipal Open Data
    roles: [host]
    url: https://example.org
"""


def _write_tiles(scene: Path, collection: Path) -> None:
    """Cut the scene into a 2x2 grid of items, each a quarter of its area."""
    with rasterio.open(scene) as src:
        half_w, half_h = src.width // 2, src.height // 2
        for row in range(2):
            for col in range(2):
                name = f"tile-r{row}c{col}"
                window = Window(col * half_w, row * half_h, half_w, half_h)
                profile = dict(
                    src.profile,
                    width=half_w,
                    height=half_h,
                    transform=src.window_transform(window),
                )
                (collection / name).mkdir(parents=True)
                with rasterio.open(collection / name / f"{name}.tif", "w", **profile) as dst:
                    dst.write(src.read(window=window))


def _write_overview(scene: Path, output: Path) -> None:
    """Write an 8-bit RGB COG of the whole scene, with embedded band statistics."""
    with rasterio.open(scene) as src:
        rgb = src.read([1, 2, 3]).astype("float64")
        low, high = np.percentile(rgb, 2), np.percentile(rgb, 98)
        rgb8 = np.clip((rgb - low) / (high - low) * 255, 0, 255).astype("uint8")
        plain = output.with_name("overview-plain.tif")
        with rasterio.open(
            plain,
            "w",
            driver="GTiff",
            width=src.width,
            height=src.height,
            count=3,
            dtype="uint8",
            crs=src.crs,
            transform=src.transform,
            photometric="RGB",
        ) as dst:
            dst.write(rgb8)
            for band in (1, 2, 3):
                stats = dst.stats(indexes=[band], approx=False)[0]
                dst.update_tags(
                    band,
                    STATISTICS_MINIMUM=stats.min,
                    STATISTICS_MAXIMUM=stats.max,
                    STATISTICS_MEAN=stats.mean,
                    STATISTICS_STDDEV=stats.std,
                    STATISTICS_VALID_PERCENT=100,
                )
    cog_translate(
        str(plain),
        str(output),
        cog_profiles.get("deflate"),
        overview_level=2,
        forward_band_tags=True,
        quiet=True,
    )
    plain.unlink()


def _register_overview(collection: Path, overview: Path) -> None:
    """Add the overview to collection.json as the spec example does, with no style asset."""
    data = overview.read_bytes()
    path = collection / "collection.json"
    stac = json.loads(path.read_text())
    stac["assets"]["overview"] = {
        "href": f"./{overview.name}",
        "type": COG_TYPE,
        "title": "Overview",
        "description": "8-bit RGB overview of all tiles in the collection.",
        "roles": ["visual", "overview"],
        "start_datetime": "2018-01-01T00:00:00Z",
        "end_datetime": "2018-12-31T23:59:59Z",
        "file:size": len(data),
        "file:checksum": "1220" + hashlib.sha256(data).hexdigest(),
    }
    path.write_text(json.dumps(stac, indent=2) + "\n")


@pytest.fixture
def overview_catalog(run_cli: RunCli, tmp_path: Path, fixtures_dir: Path) -> Path:
    """A four-tile raster collection with an overview COG and no style asset."""
    scene = fixtures_dir / "realdata" / "rapidai4eo-sample.tif"
    root = tmp_path / "cat"
    (root / ".portolan").mkdir(parents=True)
    (root / ".portolan" / "metadata.yaml").write_text(METADATA_YAML)
    run_cli("init", "--auto", str(root), "--license", "CC-BY-4.0", "--id", "overview")

    collection = root / "imagery"
    _write_tiles(scene, collection)
    for tile in sorted(collection.iterdir()):
        run_cli("add", str(tile / f"{tile.name}.tif"), "--portolan-dir", str(root))

    _write_overview(scene, collection / "overview.tif")
    _register_overview(collection, collection / "overview.tif")
    return root


def _rule_ids(stdout: str, severity: str) -> list[str]:
    findings = json.loads(stdout)["data"]["findings"]
    return [f["rule_id"] for f in findings if f["severity"] == severity]


class TestCollectionOverviewWithoutStyle:
    """PORTO-CORE-067 exempts a display-ready COG from the style requirement."""

    def test_check_fix_metadata_passes(self, run_cli: RunCli, overview_catalog: Path) -> None:
        """`check --fix --metadata`, the pipeline gate, exits 0 with no error.

        Under rashid 0.3.0 this run exited 1 with PTL-VIZ-002.
        """
        result = run_cli(
            "check", str(overview_catalog), "--fix", "--metadata", "--json", expect=None
        )

        assert _rule_ids(result.stdout, "error") == []
        assert result.exit_code == 0

    def test_overview_survives_fix_and_check_still_passes(
        self, run_cli: RunCli, overview_catalog: Path
    ) -> None:
        """`--fix` keeps the overview, and a later `check --metadata` raises no PTL-VIZ-002."""
        run_cli("check", str(overview_catalog), "--fix", "--metadata", expect=None)

        result = run_cli("check", str(overview_catalog), "--metadata", "--json", expect=None)

        stac = json.loads((overview_catalog / "imagery" / "collection.json").read_text())
        assert stac["assets"]["overview"]["roles"] == ["visual", "overview"]
        assert "PTL-VIZ-002" not in _rule_ids(result.stdout, "error")
        assert result.exit_code == 0
