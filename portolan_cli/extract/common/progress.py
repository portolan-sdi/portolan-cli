"""Shared progress plumbing for extract orchestrators.

Every extract backend (arcgis, wfs, carto) reports layer-by-layer progress
through the same callback contract: an :class:`ExtractionProgress` event is
constructed and handed to an optional ``on_progress`` callback. This module
holds the single definition of that event and the :func:`emit_progress` helper
so the orchestrators no longer each carry a near-identical copy (Issue #621).

Note: ``ExtractionOptions`` intentionally lives in each orchestrator, since the
backends expose materially different option sets and defaults.

Typical usage:
    from portolan_cli.extract.common.progress import (
        ExtractionProgress,
        emit_progress,
    )

    emit_progress(on_progress, index, total, layer.name, "starting")
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


@dataclass
class ExtractionProgress:
    """Progress callback data for extraction.

    Attributes:
        layer_index: Current layer index (0-based).
        total_layers: Total number of layers to extract.
        layer_name: Name of current layer.
        status: Current status ("starting", "extracting", "success", "failed", "skipped").
        error: Error message when status is "failed" (Issue #504).
    """

    layer_index: int
    total_layers: int
    layer_name: str
    status: str
    error: str | None = None


def emit_progress(
    on_progress: Callable[[ExtractionProgress], None] | None,
    layer_index: int,
    total_layers: int,
    layer_name: str,
    status: str,
    error: str | None = None,
) -> None:
    """Emit a progress event if a callback is provided."""
    if on_progress:
        on_progress(
            ExtractionProgress(
                layer_index=layer_index,
                total_layers=total_layers,
                layer_name=layer_name,
                status=status,
                error=error,
            )
        )
