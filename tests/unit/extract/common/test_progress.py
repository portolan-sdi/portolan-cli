"""Tests for shared extract progress plumbing."""

from __future__ import annotations

import pytest

from portolan_cli.extract.common.progress import (
    ExtractionProgress,
    emit_progress,
)

pytestmark = pytest.mark.unit


def test_progress_defaults_error_to_none() -> None:
    progress = ExtractionProgress(
        layer_index=0,
        total_layers=1,
        layer_name="layer",
        status="starting",
    )

    assert progress.error is None


def test_emit_progress_invokes_callback_with_event() -> None:
    events: list[ExtractionProgress] = []

    emit_progress(events.append, 2, 5, "roads", "extracting")

    assert len(events) == 1
    assert events[0] == ExtractionProgress(
        layer_index=2,
        total_layers=5,
        layer_name="roads",
        status="extracting",
        error=None,
    )


def test_emit_progress_forwards_error_message() -> None:
    events: list[ExtractionProgress] = []

    emit_progress(events.append, 0, 1, "parcels", "failed", error="boom")

    assert events[0].status == "failed"
    assert events[0].error == "boom"


def test_emit_progress_is_noop_without_callback() -> None:
    # Must not raise when no callback is registered.
    emit_progress(None, 0, 1, "parcels", "starting")
