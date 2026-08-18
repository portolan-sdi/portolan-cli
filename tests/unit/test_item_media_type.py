"""Regression tests for item media-type derivation (issue #736).

`item._get_media_type` carried its own hardcoded extension map, so a COG came
out as `image/tiff; application=geotiff` without `profile=cloud-optimized`.
rashid uses `is_cog_media_type` as a detection gate, so the missing profile made
`PTL-MIR-001` silently never fire instead of flagging the collection. `add` was
unaffected because it uses the registry-backed map in `preparation`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli import extension_registry as _reg
from portolan_cli.item import _get_media_type, create_item

pytestmark = pytest.mark.unit

COG_MEDIA_TYPE = "image/tiff; application=geotiff; profile=cloud-optimized"


@pytest.mark.parametrize("filename", ["scene.tif", "scene.tiff"])
def test_geotiff_media_type_carries_cog_profile(filename: str) -> None:
    # Pre-fix, the hardcoded map dropped "; profile=cloud-optimized".
    assert _get_media_type(Path(filename)) == COG_MEDIA_TYPE


def test_media_types_match_the_registry() -> None:
    """Every known extension agrees with the registry, so the two cannot drift."""
    for ext, expected in _reg.field_map("media_type").items():
        assert _get_media_type(Path(f"sample{ext}")) == expected, ext


def test_unknown_extension_still_falls_back() -> None:
    assert _get_media_type(Path("sample.unknown")) == "application/octet-stream"


def test_geoparquet_alias_tracks_parquet() -> None:
    """The registry keys only .parquet, but this module accepts either suffix."""
    assert _get_media_type(Path("data.geoparquet")) == _get_media_type(Path("data.parquet"))


@pytest.mark.parametrize("suffix", [".tif", ".tiff"])
def test_create_item_emits_cog_media_type(tmp_path: Path, suffix: str) -> None:
    """The issue's reproduction: create_item's data asset must name the profile."""
    source = Path(__file__).parent.parent / "fixtures" / "raster" / "valid" / "singleband.tif"
    data_path = tmp_path / f"scene1{suffix}"
    data_path.write_bytes(source.read_bytes())

    item = create_item("scene1", data_path, "imagery")

    assert item.to_dict()["assets"]["data"]["type"] == COG_MEDIA_TYPE
