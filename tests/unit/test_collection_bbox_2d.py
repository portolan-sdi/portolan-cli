"""Regression tests for 2D reduction of 3D (6-element) STAC bboxes.

STAC allows 6-element bboxes for 3D extents:
``[west, south, min_z, east, north, max_z]``. Collapsing these to a 2D
``[west, south, east, north]`` extent requires selecting indices ``[0, 1, 3, 4]``
- a naive ``bbox[:4]`` prefix slice yields ``[west, south, min_z, east]``,
which corrupts the eastern coordinate and drops the northern one.

See finding B in the adversarial review of PR #588.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.collection import (
    _get_metadata_yaml_bbox,
    _get_sibling_collection_bboxes,
)


@pytest.mark.unit
def test_metadata_yaml_bbox_reduces_3d_to_2d(tmp_path: Path) -> None:
    """A 6-element metadata.yaml bbox collapses to [west, south, east, north]."""
    (tmp_path / "metadata.yaml").write_text(
        "bbox: [-10.0, -20.0, 0.0, 30.0, 40.0, 100.0]\n",
        encoding="utf-8",
    )

    result = _get_metadata_yaml_bbox(tmp_path)

    # min_z=0.0 and max_z=100.0 must be dropped, east=30.0 / north=40.0 kept.
    assert result == [-10.0, -20.0, 30.0, 40.0]


@pytest.mark.unit
def test_metadata_yaml_bbox_keeps_2d_unchanged(tmp_path: Path) -> None:
    """A 4-element metadata.yaml bbox is returned as-is."""
    (tmp_path / "metadata.yaml").write_text(
        "bbox: [-10.0, -20.0, 30.0, 40.0]\n",
        encoding="utf-8",
    )

    assert _get_metadata_yaml_bbox(tmp_path) == [-10.0, -20.0, 30.0, 40.0]


@pytest.mark.unit
def test_sibling_collection_bbox_reduces_3d_to_2d(tmp_path: Path) -> None:
    """A sibling collection with a 6-element extent collapses to 2D correctly."""
    (tmp_path / "catalog.json").write_text(
        json.dumps(
            {
                "links": [
                    {"rel": "child", "href": "./sibling/collection.json"},
                ],
            }
        ),
        encoding="utf-8",
    )
    sibling_dir = tmp_path / "sibling"
    sibling_dir.mkdir()
    (sibling_dir / "collection.json").write_text(
        json.dumps(
            {
                "extent": {
                    "spatial": {"bbox": [[-10.0, -20.0, 0.0, 30.0, 40.0, 100.0]]},
                },
            }
        ),
        encoding="utf-8",
    )

    result = _get_sibling_collection_bboxes(tmp_path)

    assert result == [[-10.0, -20.0, 30.0, 40.0]]
