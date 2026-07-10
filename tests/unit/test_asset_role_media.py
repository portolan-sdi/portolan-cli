"""Regression tests for asset role/media-type assignment (issue #558).

`.webp` and `.gif` were documented and scan-classified as thumbnails but were
absent from add.py's `_ROLE_MAP` / `_MEDIA_TYPE_MAP`, so such assets were
mis-typed as role "data" / "application/octet-stream". `.svg` was already a
thumbnail in the role map but missing from the scan image set.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.add import _get_asset_role, _get_media_type
from portolan_cli.scan.classify import IMAGE_EXTENSIONS

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("filename", "expected_media"),
    [
        ("preview.webp", "image/webp"),
        ("preview.gif", "image/gif"),
        ("preview.svg", "image/svg+xml"),
    ],
)
def test_image_media_types(filename: str, expected_media: str) -> None:
    # Pre-fix, .webp/.gif fell through to "application/octet-stream".
    assert _get_media_type(Path(filename)) == expected_media


@pytest.mark.parametrize("filename", ["preview.webp", "preview.gif", "preview.svg"])
def test_image_role_is_thumbnail(filename: str) -> None:
    # Pre-fix, .webp/.gif fell through to the "data" default.
    assert _get_asset_role(Path(filename)) == "thumbnail"


def test_svg_is_a_scan_image_extension() -> None:
    # .svg now classified as an image by the scanner, matching the role map.
    assert ".svg" in IMAGE_EXTENSIONS
