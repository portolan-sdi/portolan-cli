"""Tests for detect_changes: each signal reports only when it can compare (#839)."""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.metadata.detection import detect_changes
from portolan_cli.metadata.models import FileMetadataState

pytestmark = pytest.mark.unit

WGS84 = [13.3, 43.5, 13.4, 43.6]


def _state(
    *,
    current_bbox: list[float] | None = None,
    stored_bbox: list[float] | None = None,
    current_count: int | None = None,
    stored_count: int | None = None,
) -> FileMetadataState:
    return FileMetadataState(
        file_path=Path("a.tif"),
        current_mtime=2.0,
        stored_mtime=2.0,
        current_bbox=current_bbox,
        stored_bbox=stored_bbox,
        current_feature_count=current_count,
        stored_feature_count=stored_count,
        current_schema_fingerprint="s",
        stored_schema_fingerprint="s",
    )


def test_a_file_bbox_in_another_crs_is_no_bbox_change() -> None:
    """The file bbox of a UTM raster is None, so it never differs from WGS84."""
    assert detect_changes(_state(current_bbox=None, stored_bbox=WGS84)) == []


def test_a_new_wgs84_bbox_is_a_change() -> None:
    assert detect_changes(_state(current_bbox=WGS84, stored_bbox=None)) == ["bbox"]


def test_a_moved_wgs84_bbox_is_a_change() -> None:
    moved = [13.3, 43.5, 13.5, 43.6]
    assert detect_changes(_state(current_bbox=moved, stored_bbox=WGS84)) == ["bbox"]


def test_no_count_on_either_side_is_no_count_change() -> None:
    assert detect_changes(_state(current_count=None, stored_count=None)) == []


def test_a_new_count_is_a_change() -> None:
    assert detect_changes(_state(current_count=5, stored_count=None)) == ["feature_count"]
