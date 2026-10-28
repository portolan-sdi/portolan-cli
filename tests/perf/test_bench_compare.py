"""The nightly wall-time compare must follow the speed of the runner, not its luck."""

from __future__ import annotations

import copy
from typing import Any

import pytest

from tests.perf.bench import compare

pytestmark = pytest.mark.unit


def _results(wall: float, calibration: float) -> dict[str, Any]:
    return {
        "machine": "x86_64",
        "calibration_s": calibration,
        "scenarios": {"check-metadata": {"n": 10, "wall_s": wall, "peak_rss_mb": 600.0}},
    }


def test_a_slower_runner_does_not_fail_unchanged_code() -> None:
    """The 2026-10-28 nightly ran the baseline code 1.5x to 2x slower on another runner."""
    baseline = _results(wall=100.0, calibration=2.0)
    assert compare(_results(wall=190.0, calibration=4.0), baseline) == []


def test_a_slower_scenario_on_the_same_runner_speed_fails() -> None:
    baseline = _results(wall=100.0, calibration=2.0)
    failures = compare(_results(wall=140.0, calibration=2.0), baseline)
    assert [line.split(":")[0] for line in failures] == ["check-metadata wall_s"]


def test_peak_rss_does_not_scale_with_the_runner_speed() -> None:
    baseline = _results(wall=100.0, calibration=2.0)
    now = _results(wall=200.0, calibration=4.0)
    now["scenarios"]["check-metadata"]["peak_rss_mb"] = 700.0
    failures = compare(now, baseline)
    assert [line.split(":")[0] for line in failures] == ["check-metadata peak_rss_mb"]


def test_a_baseline_without_calibration_asks_for_a_record() -> None:
    baseline = copy.deepcopy(_results(wall=100.0, calibration=2.0))
    del baseline["calibration_s"]
    assert compare(_results(wall=100.0, calibration=2.0), baseline) == [
        "the baseline has no calibration_s. Record a new baseline."
    ]
