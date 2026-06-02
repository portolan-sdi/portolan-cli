"""Tests for common resume logic."""

from __future__ import annotations

import pytest

from portolan_cli.extract.common.report import (
    ExtractionReport,
    ExtractionSummary,
    LayerResult,
    MetadataExtracted,
)
from portolan_cli.extract.common.resume import (
    ResumeState,
    get_resume_state,
    should_process_layer,
)

pytestmark = pytest.mark.unit


def _make_layer_result(id: int, name: str, status: str) -> LayerResult:
    """Helper to create a LayerResult."""
    # Empty layers have 0 features but no output file
    if status == "empty":
        return LayerResult(
            id=id,
            name=name,
            status=status,
            features=0,
            size_bytes=0,
            duration_seconds=0.0,
            output_path=None,
            warnings=["Layer has no features"],
            error="No features returned from WFS service for layer 'test'.",
            attempts=1,
        )
    return LayerResult(
        id=id,
        name=name,
        status=status,
        features=100 if status == "success" else None,
        size_bytes=1000 if status == "success" else None,
        duration_seconds=1.0 if status == "success" else None,
        output_path=f"{name}/{name}.parquet" if status == "success" else None,
        warnings=[],
        error="test error" if status == "failed" else None,
        attempts=1,
    )


def _make_report(layers: list[LayerResult]) -> ExtractionReport:
    """Helper to create an ExtractionReport."""
    return ExtractionReport(
        extraction_date="2024-01-01T00:00:00Z",
        source_url="https://example.com/wfs",
        portolan_version="1.0.0",
        gpio_version="1.0.0",
        metadata_extracted=MetadataExtracted(
            source_url="https://example.com/wfs",
            description=None,
            attribution=None,
            keywords=None,
            contact_name=None,
            processing_notes=None,
            known_issues=None,
            license_info_raw=None,
        ),
        layers=layers,
        summary=ExtractionSummary(
            total_layers=len(layers),
            succeeded=sum(1 for lyr in layers if lyr.status == "success"),
            failed=sum(1 for lyr in layers if lyr.status == "failed"),
            skipped=sum(1 for lyr in layers if lyr.status == "skipped"),
            empty=sum(1 for lyr in layers if lyr.status == "empty"),
            total_features=sum(lyr.features or 0 for lyr in layers),
            total_size_bytes=sum(lyr.size_bytes or 0 for lyr in layers),
            total_duration_seconds=sum(lyr.duration_seconds or 0.0 for lyr in layers),
        ),
    )


class TestGetResumeState:
    """Tests for get_resume_state."""

    def test_success_layers_tracked(self) -> None:
        """Succeeded layers are tracked in succeeded_layers set."""
        layers = [
            _make_layer_result(0, "layer_a", "success"),
            _make_layer_result(1, "layer_b", "success"),
        ]
        report = _make_report(layers)

        state = get_resume_state(report)

        assert state.succeeded_layers == {0, 1}
        assert state.failed_layers == set()
        assert state.succeeded_names == {"layer_a", "layer_b"}
        assert state.failed_names == set()

    def test_failed_layers_tracked(self) -> None:
        """Failed layers are tracked in failed_layers set."""
        layers = [
            _make_layer_result(0, "layer_a", "success"),
            _make_layer_result(1, "layer_b", "failed"),
        ]
        report = _make_report(layers)

        state = get_resume_state(report)

        assert state.succeeded_layers == {0}
        assert state.failed_layers == {1}
        assert state.succeeded_names == {"layer_a"}
        assert state.failed_names == {"layer_b"}

    def test_skipped_treated_as_success(self) -> None:
        """Skipped layers are treated as succeeded."""
        layers = [
            _make_layer_result(0, "layer_a", "skipped"),
        ]
        report = _make_report(layers)

        state = get_resume_state(report)

        assert state.succeeded_layers == {0}
        assert state.failed_layers == set()
        assert state.succeeded_names == {"layer_a"}
        assert state.failed_names == set()

    def test_empty_treated_as_success(self) -> None:
        """Empty layers (0 features) are treated as succeeded.

        Issue #450: Empty layers should not be re-extracted on resume
        because they will remain empty - no point retrying.
        """
        layers = [
            _make_layer_result(0, "layer_a", "success"),
            _make_layer_result(1, "layer_b", "empty"),
            _make_layer_result(2, "layer_c", "failed"),
        ]
        report = _make_report(layers)

        state = get_resume_state(report)

        # Empty layers go into succeeded set, not failed
        assert state.succeeded_layers == {0, 1}
        assert state.failed_layers == {2}
        assert state.succeeded_names == {"layer_a", "layer_b"}
        assert state.failed_names == {"layer_c"}


class TestShouldProcessLayer:
    """Tests for should_process_layer."""

    def test_no_resume_state_process_all(self) -> None:
        """With no resume state, process all layers."""
        assert should_process_layer(0, None)
        assert should_process_layer(1, None)

    def test_succeeded_layer_skipped(self) -> None:
        """Succeeded layers should be skipped."""
        state = ResumeState(
            succeeded_layers={0, 1},
            failed_layers=set(),
            succeeded_names={"layer_a", "layer_b"},
            failed_names=set(),
        )

        assert not should_process_layer(0, state)
        assert not should_process_layer(1, state)

    def test_failed_layer_retried(self) -> None:
        """Failed layers should be retried."""
        state = ResumeState(
            succeeded_layers=set(),
            failed_layers={0},
            succeeded_names=set(),
            failed_names={"layer_a"},
        )

        assert should_process_layer(0, state)

    def test_new_layer_processed(self) -> None:
        """New layers (not in report) should be processed."""
        state = ResumeState(
            succeeded_layers={0},
            failed_layers={1},
            succeeded_names={"layer_a"},
            failed_names={"layer_b"},
        )

        assert should_process_layer(2, state)  # Not in either set

    def test_name_based_lookup_preferred(self) -> None:
        """Name-based lookup is preferred when layer_name is provided."""
        state = ResumeState(
            succeeded_layers={0},
            failed_layers=set(),
            succeeded_names={"layer_a"},
            failed_names=set(),
        )

        # By name - should skip
        assert not should_process_layer(99, state, layer_name="layer_a")
        # By name - should process (new name)
        assert should_process_layer(0, state, layer_name="layer_b")

    def test_empty_layer_skipped_on_resume(self) -> None:
        """Empty layers should be skipped on resume.

        Issue #450: Previously empty layers were treated as "new" and
        re-extracted on every resume. Now they're in succeeded_names
        and get skipped.
        """
        state = ResumeState(
            succeeded_layers={0, 1},  # includes empty layer
            failed_layers={2},
            succeeded_names={"layer_a", "empty_layer"},  # empty_layer is here
            failed_names={"failed_layer"},
        )

        # Empty layer should be skipped (not retried)
        assert not should_process_layer(1, state, layer_name="empty_layer")
        # Failed layer should be retried
        assert should_process_layer(2, state, layer_name="failed_layer")
