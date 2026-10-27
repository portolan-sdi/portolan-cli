"""Fail any change that adds work per file, per collection, or per run (#992).

Each test runs one scenario of ``scenarios.py`` at size n and 2n in cold child
processes and compares the slope and intercept of every counter with the
constants in ``tests/perf/expected/``. ``gates.py`` explains the arithmetic.
The constants encode the costs of today, the bad ones included. A lane that
removes work lowers its own constants.

An ``add`` scenario then runs at 2n with ``--workers 4``. The thread pool must
not add work, so each counter must stay at or below its ``--workers 1`` value.

The counts come from the code under test, not from machine load, so the gate
is exact. A failure prints the pairs to change. When the change is a real
improvement, rerecord with ``uv run python -m tests.perf.gates --record --write``
and commit only the file of your lane.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from tests.perf.gates import COUNTER_OWNER, SCALE_SIZES, expected, measure, scale
from tests.perf.scenarios import SCENARIOS

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.integration


def _ran_clean(record: dict[str, Any], exit_code: int) -> None:
    assert record["network"] == [], f"{record['argv']} reached the network"
    assert record["exit_code"] == exit_code, record["stdout"][-2000:]


@pytest.mark.parametrize("name", sorted(SCALE_SIZES))
def test_counters_scale_as_recorded(name: str, tmp_path: Path) -> None:
    scenario = SCENARIOS[name]
    n = SCALE_SIZES[name]

    small = measure(scenario, tmp_path, n)
    large = measure(scenario, tmp_path, 2 * n)

    _ran_clean(small, scenario.exit_code)
    _ran_clean(large, scenario.exit_code)
    measured = scale(small, large)
    changed = {
        counter: {"expected": expected(name)[counter], "measured": pair}
        for counter, pair in measured.items()
        if pair != expected(name)[counter]
    }
    assert changed == {}, (
        f"{name}: (slope, intercept) changed. Owner files: "
        f"{sorted({COUNTER_OWNER[c] for c in changed})}. "
        f"Rerecord: uv run python -m tests.perf.gates --record --write {name}"
    )

    if scenario.runs_phase_two:
        parallel = measure(scenario, tmp_path, 2 * n, workers=4)
        _ran_clean(parallel, scenario.exit_code)
        grown = {
            counter: (parallel["counters"][counter], large["counters"][counter])
            for counter in COUNTER_OWNER
            if parallel["counters"][counter] > large["counters"][counter]
        }
        assert grown == {}, f"{name}: --workers 4 does more work than --workers 1"
