"""Unit tests for ArcGIS extraction resume logic.

Tests the resume functionality for interrupted extractions:
- ResumeState: Tracks succeeded/failed layer IDs
- get_resume_state: Builds resume state from report
- should_process_layer: Determines if layer needs processing

Following TDD: tests written before implementation.
"""

from __future__ import annotations

from portolan_cli.extract.arcgis.report import (
    ExtractionReport,
    ExtractionSummary,
    LayerResult,
    MetadataExtracted,
)
from portolan_cli.extract.arcgis.resume import (
    ResumeState,
    get_resume_state,
    should_process_layer,
)


class TestResumeState:
    """Tests for ResumeState dataclass."""

    def test_creation(self) -> None:
        """Create resume state with succeeded and failed layers."""
        state = ResumeState(
            succeeded_layers={0, 1, 2},
            failed_layers={3, 4},
        )

        assert state.succeeded_layers == {0, 1, 2}
        assert state.failed_layers == {3, 4}

    def test_empty_state(self) -> None:
        """Create empty resume state."""
        state = ResumeState(
            succeeded_layers=set(),
            failed_layers=set(),
        )

        assert len(state.succeeded_layers) == 0
        assert len(state.failed_layers) == 0


class TestGetResumeState:
    """Tests for get_resume_state function."""

    def test_all_succeeded(self) -> None:
        """Resume state from report where all layers succeeded."""
        report = _create_report_with_layers(
            [
                ("success", 0),
                ("success", 1),
                ("success", 2),
            ]
        )

        state = get_resume_state(report)

        assert state.succeeded_layers == {0, 1, 2}
        assert state.failed_layers == set()

    def test_all_failed(self) -> None:
        """Resume state from report where all layers failed."""
        report = _create_report_with_layers(
            [
                ("failed", 0),
                ("failed", 1),
            ]
        )

        state = get_resume_state(report)

        assert state.succeeded_layers == set()
        assert state.failed_layers == {0, 1}

    def test_mixed_results(self) -> None:
        """Resume state from report with mixed success/failure."""
        report = _create_report_with_layers(
            [
                ("success", 0),
                ("failed", 1),
                ("success", 2),
                ("failed", 3),
                ("success", 4),
            ]
        )

        state = get_resume_state(report)

        assert state.succeeded_layers == {0, 2, 4}
        assert state.failed_layers == {1, 3}

    def test_skipped_layers_treated_as_succeeded(self) -> None:
        """Skipped layers should be in succeeded set (already done)."""
        report = _create_report_with_layers(
            [
                ("success", 0),
                ("skipped", 1),
                ("failed", 2),
            ]
        )

        state = get_resume_state(report)

        # Skipped layers were already processed successfully before
        assert state.succeeded_layers == {0, 1}
        assert state.failed_layers == {2}

    def test_empty_report(self) -> None:
        """Resume state from empty report."""
        report = _create_report_with_layers([])

        state = get_resume_state(report)

        assert state.succeeded_layers == set()
        assert state.failed_layers == set()


class TestShouldProcessLayer:
    """Tests for should_process_layer function."""

    def test_no_resume_state_always_process(self) -> None:
        """Without resume state, all layers should be processed."""
        assert should_process_layer(0, None) is True
        assert should_process_layer(5, None) is True
        assert should_process_layer(100, None) is True

    def test_skip_succeeded_layers(self) -> None:
        """Succeeded layers should be skipped."""
        state = ResumeState(
            succeeded_layers={0, 1, 2},
            failed_layers={3},
        )

        assert should_process_layer(0, state) is False
        assert should_process_layer(1, state) is False
        assert should_process_layer(2, state) is False

    def test_retry_failed_layers(self) -> None:
        """Failed layers should be retried."""
        state = ResumeState(
            succeeded_layers={0, 1},
            failed_layers={2, 3},
        )

        assert should_process_layer(2, state) is True
        assert should_process_layer(3, state) is True

    def test_process_new_layers(self) -> None:
        """New layers (not in report) should be processed."""
        state = ResumeState(
            succeeded_layers={0, 1},
            failed_layers={2},
        )

        # Layer 5 wasn't in the previous extraction
        assert should_process_layer(5, state) is True
        assert should_process_layer(10, state) is True

    def test_comprehensive_scenario(self) -> None:
        """Comprehensive test with all layer types."""
        state = ResumeState(
            succeeded_layers={0, 2, 4, 6},
            failed_layers={1, 3},
        )

        # Succeeded - skip
        assert should_process_layer(0, state) is False
        assert should_process_layer(4, state) is False

        # Failed - retry
        assert should_process_layer(1, state) is True
        assert should_process_layer(3, state) is True

        # New - process
        assert should_process_layer(5, state) is True
        assert should_process_layer(7, state) is True


def _create_report_with_layers(layer_specs: list[tuple[str, int]]) -> ExtractionReport:
    """Create a report with specified layer statuses.

    Args:
        layer_specs: List of (status, id) tuples.

    Returns:
        ExtractionReport with the specified layers.
    """
    layers = []
    for status, layer_id in layer_specs:
        if status == "success" or status == "skipped":
            layers.append(
                LayerResult(
                    id=layer_id,
                    name=f"Layer_{layer_id}",
                    status=status,
                    features=100,
                    size_bytes=50000,
                    duration_seconds=5.0,
                    output_path=f"layer_{layer_id}/data.parquet",
                    warnings=[],
                    error=None,
                    attempts=1,
                )
            )
        else:  # failed
            layers.append(
                LayerResult(
                    id=layer_id,
                    name=f"Layer_{layer_id}",
                    status=status,
                    features=None,
                    size_bytes=None,
                    duration_seconds=None,
                    output_path=None,
                    warnings=[],
                    error="Test error",
                    attempts=3,
                )
            )

    succeeded = sum(1 for s, _ in layer_specs if s in ("success", "skipped"))
    failed = sum(1 for s, _ in layer_specs if s == "failed")

    metadata = MetadataExtracted(
        source_url="https://test.com/FeatureServer",
        description=None,
        attribution=None,
        keywords=None,
        contact_name=None,
        processing_notes=None,
        known_issues=None,
        license_info_raw=None,
    )

    summary = ExtractionSummary(
        total_layers=len(layers),
        succeeded=succeeded,
        failed=failed,
        skipped=0,
        total_features=100 * succeeded,
        total_size_bytes=50000 * succeeded,
        total_duration_seconds=5.0 * succeeded,
    )

    return ExtractionReport(
        extraction_date="2026-03-30T14:30:00Z",
        source_url="https://test.com/FeatureServer",
        portolan_version="0.4.0",
        gpio_version="0.2.0",
        metadata_extracted=metadata,
        layers=layers,
        summary=summary,
    )
