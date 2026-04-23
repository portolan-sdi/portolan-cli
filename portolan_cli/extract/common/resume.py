"""Resume logic for interrupted extractions.

This module provides functionality to resume extractions that were
interrupted or partially failed:

- ResumeState: Tracks which layers succeeded/failed in previous run
- get_resume_state: Builds resume state from an extraction report
- should_process_layer: Determines if a layer needs processing

Usage:
    report = load_report(Path(".portolan/extraction-report.json"))
    resume_state = get_resume_state(report)

    for layer_id in service_layers:
        if should_process_layer(layer_id, resume_state):
            # Extract this layer
            ...
"""

from __future__ import annotations

from dataclasses import dataclass

from portolan_cli.extract.common.report import ExtractionReport


@dataclass
class ResumeState:
    """State for resuming an interrupted extraction.

    Tracks which layers have already been processed, enabling
    the extraction to skip succeeded layers and retry failed ones.

    Attributes:
        succeeded_layers: Set of layer IDs that succeeded (to skip).
        failed_layers: Set of layer IDs that failed (to retry).
    """

    succeeded_layers: set[int]
    failed_layers: set[int]


def get_resume_state(report: ExtractionReport) -> ResumeState:
    """Build resume state from an extraction report.

    Analyzes the previous extraction results to determine which
    layers should be skipped (already succeeded) and which should
    be retried (previously failed).

    Note: "skipped" status layers are treated as succeeded because
    they were already successfully extracted in a prior run.

    Args:
        report: The extraction report from a previous run.

    Returns:
        ResumeState with succeeded and failed layer IDs.
    """
    succeeded_layers: set[int] = set()
    failed_layers: set[int] = set()

    for layer in report.layers:
        if layer.status in ("success", "skipped"):
            succeeded_layers.add(layer.id)
        elif layer.status == "failed":
            failed_layers.add(layer.id)

    return ResumeState(
        succeeded_layers=succeeded_layers,
        failed_layers=failed_layers,
    )


def should_process_layer(layer_id: int, resume_state: ResumeState | None) -> bool:
    """Determine if a layer should be processed.

    Decision logic:
    - If no resume state: process all layers
    - If layer succeeded previously: skip (return False)
    - If layer failed previously: retry (return True)
    - If layer is new (not in report): process (return True)

    Args:
        layer_id: The layer ID.
        resume_state: Resume state from previous extraction, or None.

    Returns:
        True if the layer should be processed, False if it should be skipped.
    """
    if resume_state is None:
        return True

    if layer_id in resume_state.succeeded_layers:
        return False

    return True
