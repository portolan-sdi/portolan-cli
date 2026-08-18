"""Regression tests for raster bbox reprojection (issue #708, PTL-BBX-001).

`_extract_geometry_from_file` returned a COG's bbox in the source CRS. STAC
requires bbox in WGS84 regardless of the asset's own CRS, so rashid flagged
`PTL-BBX-001` and it cascaded into `PTL-DAT-005` through the collection extent
union. `add` was unaffected, it reprojects via `preparation._extract_bbox_wgs84`;
`metadata.update.update_item_metadata` and `item.create_item` both consume this
function directly, so both wrote the projected bbox.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.item import _extract_geometry_from_file
from portolan_cli.metadata.update import update_item_metadata

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).parent.parent / "fixtures"

# tests/fixtures/realdata/rapidai4eo-sample.tif is UTM zone 33N (EPSG:32633).
UTM_RASTER = FIXTURES / "realdata" / "rapidai4eo-sample.tif"
# The projected bbox the issue saw land in the item after a refresh.
SOURCE_CRS_BBOX = [364800.0, 4823400.0, 365400.0, 4824000.0]
# The WGS84 equivalent, matching the "before the refresh" value in the issue.
WGS84_BBOX = [13.326095176877459, 43.55130444246275, 13.333669492311913, 43.55681316579193]

WGS84_RASTER = FIXTURES / "raster" / "valid" / "singleband.tif"


@pytest.mark.realdata
def test_projected_raster_bbox_is_reprojected_to_wgs84() -> None:
    # Pre-fix this returned SOURCE_CRS_BBOX, the raw UTM easting/northing.
    bbox, _geometry = _extract_geometry_from_file(UTM_RASTER)

    assert bbox != SOURCE_CRS_BBOX
    assert bbox == pytest.approx(WGS84_BBOX)


@pytest.mark.realdata
def test_projected_raster_geometry_is_reprojected_to_wgs84() -> None:
    """The polygon is derived from the bbox, so it must be WGS84 too."""
    _bbox, geometry = _extract_geometry_from_file(UTM_RASTER)

    lons = [pt[0] for pt in geometry["coordinates"][0]]
    lats = [pt[1] for pt in geometry["coordinates"][0]]
    assert all(-180.0 <= lon <= 180.0 for lon in lons), lons
    assert all(-90.0 <= lat <= 90.0 for lat in lats), lats


def test_wgs84_raster_bbox_is_not_transformed_twice() -> None:
    """A source already in EPSG:4326 must pass through unchanged."""
    bbox, _geometry = _extract_geometry_from_file(WGS84_RASTER)

    assert bbox == pytest.approx([-122.5, 37.7, -122.35, 37.85])


@pytest.mark.realdata
def test_metadata_refresh_keeps_the_bbox_in_wgs84(tmp_path: Path) -> None:
    """The issue's reproduction: a refresh must not overwrite WGS84 with UTM."""
    raster = tmp_path / "scene-a.tif"
    raster.write_bytes(UTM_RASTER.read_bytes())

    item_path = tmp_path / "scene-a.json"
    item_path.write_text(
        json.dumps(
            {
                "type": "Feature",
                "stac_version": "1.1.0",
                "id": "scene-a",
                "collection": "imagery",
                "bbox": WGS84_BBOX,
                "geometry": {"type": "Polygon", "coordinates": [[]]},
                "properties": {"datetime": "2024-01-01T00:00:00Z"},
                "assets": {"data": {"href": "scene-a.tif", "roles": ["data"]}},
                "links": [],
            }
        )
    )

    update_item_metadata(item_path, raster)

    refreshed = json.loads(item_path.read_text())["bbox"]
    assert refreshed != SOURCE_CRS_BBOX, "the refresh reintroduced the source CRS"
    assert refreshed == pytest.approx(WGS84_BBOX)
