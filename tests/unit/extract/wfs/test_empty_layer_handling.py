"""Unit tests for empty WFS layer handling.

Tests Issue #450: Graceful handling of empty WFS layers.

When a WFS layer has 0 features, gpio raises WFSError with message
"No features returned from WFS service for layer '{typename}'."

We should:
1. Detect this specific error
2. Mark the layer as "empty" (not "failed")
3. Continue with the next layer
4. Report empty layers in the summary
"""

from __future__ import annotations

import pytest

from portolan_cli.extract.common.report import ExtractionSummary, LayerResult
from portolan_cli.extract.wfs.orchestrator import (
    _build_summary,
    _is_empty_layer_error,
)

pytestmark = [pytest.mark.unit]


class TestIsEmptyLayerError:
    """Tests for _is_empty_layer_error detection function."""

    def test_detects_no_features_error(self) -> None:
        """Detects gpio's 'No features returned' error message."""
        error_msg = (
            "No features returned from WFS service for layer 'apl:s9422_sewerappurtenance'.\n"
            "Check that the layer exists and is not empty."
        )
        assert _is_empty_layer_error(error_msg) is True

    def test_detects_no_features_from_tiles(self) -> None:
        """Detects gpio's 'No features returned from any spatial tile' error."""
        error_msg = "No features returned from any spatial tile."
        assert _is_empty_layer_error(error_msg) is True

    def test_rejects_other_wfs_errors(self) -> None:
        """Does not match other WFS errors."""
        error_messages = [
            "Layer 'nonexistent' not found in WFS service.",
            "Authentication required. WFS server requires credentials.",
            "HTTP error 500: Internal server error",
            "Could not connect to WFS service: https://example.com/wfs",
            "Request failed after 3 attempts: timeout",
        ]
        for msg in error_messages:
            assert _is_empty_layer_error(msg) is False, f"Should reject: {msg}"

    def test_handles_none_error(self) -> None:
        """Handles None error message gracefully."""
        assert _is_empty_layer_error(None) is False

    def test_handles_empty_string(self) -> None:
        """Handles empty string error message."""
        assert _is_empty_layer_error("") is False


class TestBuildSummaryWithEmpty:
    """Tests for _build_summary including empty layer counts."""

    def test_counts_empty_layers(self) -> None:
        """Empty layers are counted separately from failed."""
        results = [
            LayerResult(
                id=0,
                name="layer_a",
                status="success",
                features=100,
                size_bytes=1000,
                duration_seconds=1.0,
                output_path="layer_a/layer_a.parquet",
                warnings=[],
                error=None,
                attempts=1,
            ),
            LayerResult(
                id=1,
                name="layer_b",
                status="empty",
                features=0,
                size_bytes=0,
                duration_seconds=0.5,
                output_path=None,
                warnings=[],
                error="No features returned from WFS service for layer 'layer_b'.",
                attempts=1,
            ),
            LayerResult(
                id=2,
                name="layer_c",
                status="failed",
                features=None,
                size_bytes=None,
                duration_seconds=None,
                output_path=None,
                warnings=[],
                error="HTTP error 500",
                attempts=3,
            ),
        ]

        summary = _build_summary(results)

        assert summary.total_layers == 3
        assert summary.succeeded == 1
        assert summary.empty == 1
        assert summary.failed == 1
        assert summary.skipped == 0

    def test_empty_field_in_summary(self) -> None:
        """ExtractionSummary has empty field."""
        summary = ExtractionSummary(
            total_layers=10,
            succeeded=7,
            failed=1,
            skipped=0,
            empty=2,
            total_features=7000,
            total_size_bytes=70000,
            total_duration_seconds=10.0,
        )

        assert summary.empty == 2

        # Verify serialization
        d = summary.to_dict()
        assert d["empty"] == 2

        # Verify deserialization
        restored = ExtractionSummary.from_dict(d)
        assert restored.empty == 2

    def test_summary_backward_compatible(self) -> None:
        """from_dict handles missing empty field (backward compatibility)."""
        old_format = {
            "total_layers": 10,
            "succeeded": 8,
            "failed": 2,
            "skipped": 0,
            "total_features": 8000,
            "total_size_bytes": 80000,
            "total_duration_seconds": 10.0,
            # Note: no "empty" field (old report format)
        }

        summary = ExtractionSummary.from_dict(old_format)

        assert summary.empty == 0  # Default to 0 for old reports


class TestLayerResultEmptyStatus:
    """Tests for LayerResult with 'empty' status."""

    def test_empty_status_serializes(self) -> None:
        """LayerResult with 'empty' status serializes correctly."""
        result = LayerResult(
            id=1,
            name="empty_layer",
            status="empty",
            features=0,
            size_bytes=0,
            duration_seconds=0.3,
            output_path=None,
            warnings=["Layer has no features"],
            error="No features returned from WFS service for layer 'empty_layer'.",
            attempts=1,
        )

        d = result.to_dict()
        assert d["status"] == "empty"
        assert d["features"] == 0

        # Verify round-trip
        restored = LayerResult.from_dict(d)
        assert restored.status == "empty"
        assert restored.features == 0
