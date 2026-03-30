"""Tests for CRS validation and mismatch detection.

These tests address Issue #4 from TESTING-NOTES.md:
CRS mislabeling where files declare EPSG:28992 but contain WGS84 coordinates.
"""

from __future__ import annotations

import logging

import pytest

from portolan_cli.crs import (
    CRSMismatchWarning,
    is_likely_wgs84_bbox,
    transform_bbox_to_wgs84,
    validate_bbox_crs,
)
from portolan_cli.errors import CRSMismatchError

pytestmark = pytest.mark.unit


class TestIsLikelyWgs84Bbox:
    """Tests for WGS84 coordinate detection heuristic."""

    def test_typical_wgs84_coordinates_detected(self) -> None:
        """Coordinates in valid lon/lat range are detected as WGS84-like."""
        # Den Haag, Netherlands (from the bug report)
        bbox = (4.29, 52.07, 4.35, 52.10)
        assert is_likely_wgs84_bbox(bbox) is True

    def test_european_wgs84_coordinates_detected(self) -> None:
        """European WGS84 coordinates are detected."""
        # Amsterdam area
        bbox = (4.7, 52.2, 5.1, 52.5)
        assert is_likely_wgs84_bbox(bbox) is True

    def test_global_extent_wgs84(self) -> None:
        """Global extent is WGS84-like."""
        bbox = (-180.0, -90.0, 180.0, 90.0)
        assert is_likely_wgs84_bbox(bbox) is True

    def test_projected_coordinates_not_wgs84(self) -> None:
        """RD New (EPSG:28992) coordinates are NOT WGS84-like."""
        # Den Haag in RD New meters
        bbox = (81000.0, 454000.0, 85000.0, 458000.0)
        assert is_likely_wgs84_bbox(bbox) is False

    def test_utm_coordinates_not_wgs84(self) -> None:
        """UTM coordinates (meters) are NOT WGS84-like."""
        # UTM zone 31N
        bbox = (580000.0, 5770000.0, 590000.0, 5780000.0)
        assert is_likely_wgs84_bbox(bbox) is False

    def test_web_mercator_coordinates_not_wgs84(self) -> None:
        """Web Mercator (EPSG:3857) coordinates are NOT WGS84-like."""
        bbox = (477000.0, 6810000.0, 486000.0, 6840000.0)
        assert is_likely_wgs84_bbox(bbox) is False

    def test_edge_case_longitude_boundary(self) -> None:
        """Coordinates at longitude extremes are still WGS84-like."""
        # Pacific crossing antimeridian
        bbox = (170.0, -10.0, -170.0, 10.0)  # Note: west > east for antimeridian
        assert is_likely_wgs84_bbox(bbox) is True

    def test_southern_hemisphere_wgs84(self) -> None:
        """Southern hemisphere WGS84 coordinates are detected."""
        # Sydney, Australia
        bbox = (150.9, -34.0, 151.3, -33.7)
        assert is_likely_wgs84_bbox(bbox) is True

    def test_negative_longitude_wgs84(self) -> None:
        """Negative longitude (Western hemisphere) WGS84 coordinates are detected."""
        # New York area
        bbox = (-74.3, 40.5, -73.7, 41.0)
        assert is_likely_wgs84_bbox(bbox) is True


class TestValidateBboxCrs:
    """Tests for CRS mismatch detection."""

    def test_no_warning_when_crs_is_wgs84(self) -> None:
        """No warning when CRS is WGS84 and coordinates are WGS84."""
        bbox = (4.29, 52.07, 4.35, 52.10)
        result = validate_bbox_crs(bbox, "EPSG:4326")
        assert result is None  # No warning

    def test_no_warning_when_crs_is_none(self) -> None:
        """No warning when CRS is None (assumed WGS84)."""
        bbox = (4.29, 52.07, 4.35, 52.10)
        result = validate_bbox_crs(bbox, None)
        assert result is None

    def test_no_warning_when_projected_crs_and_projected_coords(self) -> None:
        """No warning when projected CRS matches projected coordinates."""
        # RD New coordinates (large meters values)
        bbox = (81000.0, 454000.0, 85000.0, 458000.0)
        result = validate_bbox_crs(bbox, "EPSG:28992")
        assert result is None

    def test_warning_when_projected_crs_but_wgs84_coords(self) -> None:
        """Warning when CRS is projected but coordinates look like WGS84.

        This is the exact bug from TESTING-NOTES.md Issue #4:
        - File declares EPSG:28992 (RD New)
        - But coordinates are actually WGS84 (4.29, 52.07)
        """
        bbox = (4.29, 52.07, 4.35, 52.10)  # WGS84 coords
        result = validate_bbox_crs(bbox, "EPSG:28992")

        assert result is not None
        assert isinstance(result, CRSMismatchWarning)
        assert result.declared_crs == "EPSG:28992"
        assert result.likely_actual_crs == "EPSG:4326"
        assert "WGS84" in result.message or "4326" in result.message

    def test_warning_message_includes_context(self) -> None:
        """Warning message includes helpful context for debugging."""
        bbox = (4.29, 52.07, 4.35, 52.10)
        result = validate_bbox_crs(bbox, "EPSG:28992")

        assert result is not None
        # Should mention the mismatch
        assert "mismatch" in result.message.lower() or "declared" in result.message.lower()

    def test_warning_for_utm_with_wgs84_coords(self) -> None:
        """Warning when UTM CRS but coordinates are WGS84."""
        bbox = (4.29, 52.07, 4.35, 52.10)  # WGS84 coords
        result = validate_bbox_crs(bbox, "EPSG:32631")  # UTM zone 31N

        assert result is not None
        assert isinstance(result, CRSMismatchWarning)
        assert result.declared_crs == "EPSG:32631"

    def test_no_warning_when_wgs84_with_wgs84_coords(self) -> None:
        """No warning when WGS84 CRS matches WGS84 coordinates."""
        bbox = (4.29, 52.07, 4.35, 52.10)
        # Test various WGS84 representations
        for crs in ["EPSG:4326", "WGS84", "urn:ogc:def:crs:OGC:1.3:CRS84"]:
            result = validate_bbox_crs(bbox, crs)
            assert result is None, f"Unexpected warning for CRS {crs}"


class TestTransformBboxToWgs84WithValidation:
    """Tests for CRS mismatch detection during bbox transformation."""

    def test_raises_error_on_crs_mismatch_by_default(self) -> None:
        """transform_bbox_to_wgs84 raises CRSMismatchError by default.

        When coordinates look like WGS84 but CRS declares projected system,
        we should fail-fast with a clear exception to avoid propagating
        invalid coordinates.
        """
        bbox = (4.29, 52.07, 4.35, 52.10)  # WGS84 coords

        with pytest.raises(CRSMismatchError) as exc_info:
            transform_bbox_to_wgs84(bbox, "EPSG:28992")

        assert exc_info.value.source_crs == "EPSG:28992"
        assert exc_info.value.bbox == bbox
        assert exc_info.value.likely_actual_crs == "EPSG:4326"

    def test_logs_warning_on_crs_mismatch_with_allow_guess(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """transform_bbox_to_wgs84 logs warning when allow_guess=True.

        When coordinates look like WGS84 but CRS declares projected system,
        and allow_guess=True, we should warn but still return the coordinates
        (assuming they're already WGS84).
        """
        bbox = (4.29, 52.07, 4.35, 52.10)  # WGS84 coords

        with caplog.at_level(logging.WARNING):
            result = transform_bbox_to_wgs84(bbox, "EPSG:28992", allow_guess=True)

        # Result should be unchanged (mismatch detected)
        assert result == bbox

        # Should log a warning about CRS mismatch
        assert any("mismatch" in record.message.lower() for record in caplog.records), (
            f"Expected CRS mismatch warning in logs. Got: {[r.message for r in caplog.records]}"
        )

    def test_returns_unchanged_bbox_on_mismatch_with_allow_guess(self) -> None:
        """When CRS mismatch detected with allow_guess=True, return bbox unchanged.

        If coordinates are already WGS84 but CRS claims projected, attempting
        the transformation would produce garbage. Better to return unchanged.
        """
        bbox = (4.29, 52.07, 4.35, 52.10)  # WGS84 coords

        result = transform_bbox_to_wgs84(bbox, "EPSG:28992", allow_guess=True)

        # Should return coordinates unchanged (they're already WGS84)
        assert result == bbox

    def test_normal_transformation_when_no_mismatch(self) -> None:
        """Normal CRS transformation when no mismatch detected."""
        # RD New coordinates (large meters values)
        bbox = (81000.0, 454000.0, 85000.0, 458000.0)

        result = transform_bbox_to_wgs84(bbox, "EPSG:28992")

        # Result should be in WGS84 (small degree values)
        minx, miny, maxx, maxy = result
        assert -180 <= minx <= 180
        assert -90 <= miny <= 90
        assert -180 <= maxx <= 180
        assert -90 <= maxy <= 90

        # Should be roughly in Netherlands area
        assert 3.0 < minx < 8.0  # Longitude
        assert 50.0 < miny < 54.0  # Latitude
