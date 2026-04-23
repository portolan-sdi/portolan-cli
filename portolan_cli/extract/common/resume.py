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
        succeeded_names: Set of layer names that succeeded (stable lookup).
        failed_names: Set of layer names that failed (stable lookup).
    """

    succeeded_layers: set[int]
    failed_layers: set[int]
    succeeded_names: set[str]
    failed_names: set[str]


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
        ResumeState with succeeded and failed layer IDs and names.
    """
    succeeded_layers: set[int] = set()
    failed_layers: set[int] = set()
    succeeded_names: set[str] = set()
    failed_names: set[str] = set()

    for layer in report.layers:
        if layer.status in ("success", "skipped"):
            succeeded_layers.add(layer.id)
            succeeded_names.add(layer.name)
        elif layer.status == "failed":
            failed_layers.add(layer.id)
            failed_names.add(layer.name)

    return ResumeState(
        succeeded_layers=succeeded_layers,
        failed_layers=failed_layers,
        succeeded_names=succeeded_names,
        failed_names=failed_names,
    )


def should_process_layer(
    layer_id: int,
    resume_state: ResumeState | None,
    layer_name: str | None = None,
) -> bool:
    """Determine if a layer should be processed.

    Decision logic:
    - If no resume state: process all layers
    - If layer succeeded previously: skip (return False)
    - If layer failed previously: retry (return True)
    - If layer is new (not in report): process (return True)

    When layer_name is provided, uses name-based lookup (more stable than
    ID-based lookup since layer IDs can change if discovery order changes).

    Args:
        layer_id: The layer ID (fallback for backwards compatibility).
        resume_state: Resume state from previous extraction, or None.
        layer_name: The layer name (preferred, stable identifier).

    Returns:
        True if the layer should be processed, False if it should be skipped.
    """
    if resume_state is None:
        return True

    # Prefer name-based lookup (stable across discovery order changes)
    if layer_name is not None:
        if layer_name in resume_state.succeeded_names:
            return False
        if layer_name in resume_state.failed_names:
            return True  # Retry failed layers
        return True  # New layer

    # Fallback to ID-based lookup (backwards compatibility)
    if layer_id in resume_state.succeeded_layers:
        return False

    if layer_id in resume_state.failed_layers:
        return True  # Retry failed layers

    return True
