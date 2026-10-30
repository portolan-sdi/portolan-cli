"""Helpers for add_files properties that stub out item preparation.

The CSV and tabular Parquet skip properties check that add_files does not
raise on a directory that mixes GeoJSON with non-geo files, and that it
hands every candidate file to prepare_item once. They stub prepare_item and finalize_items, so no
geoparquet-io conversion runs.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from portolan_cli.add import add_files

if TYPE_CHECKING:
    from pathlib import Path


def write_point_geojson(path: Path, index: int) -> None:
    """Write a FeatureCollection with one point, offset by ``index`` degrees."""
    path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [-122.4 + index, 37.8]},
                        "properties": {"name": f"Point {index}"},
                    }
                ],
            }
        )
    )


def add_mixed_directory_without_raising(
    collection_dir: Path, catalog_root: Path, item_id: str
) -> list[Path]:
    """Run add_files on ``collection_dir`` with item preparation stubbed.

    Per issue #281, add_files calls prepare_item and then finalize_items.

    Returns:
        The paths that add_files passed to prepare_item, sorted.

    Raises:
        pytest.fail.Exception: If add_files raises.
    """
    with (
        patch("portolan_cli.add.prepare_item") as mock_prepare,
        patch("portolan_cli.add.finalize_items") as mock_finalize,
    ):
        mock_prepare.return_value = MagicMock(item_id=item_id, collection_id="collection")
        mock_finalize.return_value = []
        try:
            add_files(
                paths=[collection_dir],
                catalog_root=catalog_root,
                collection_id="collection",
            )
        except Exception as e:
            pytest.fail(f"add_files raised for mixed directory: {e}")
    return sorted(call.kwargs["path"] for call in mock_prepare.call_args_list)
