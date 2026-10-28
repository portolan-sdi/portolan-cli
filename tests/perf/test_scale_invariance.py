"""Fail any change that adds work per file, per collection, or per run (#992).

Each test runs one scenario of ``scenarios.py`` at size n and 2n in cold child
processes and compares the slope and intercept of every counter with the
constants in ``tests/perf/expected/``. ``gates.py`` explains the arithmetic.
The constants encode the costs of today, the bad ones included. A lane that
removes work lowers its own constants.

Each scenario that accepts ``--workers`` then runs at 2n with ``--workers 4``.
The thread pool must not change the work, so each counter must equal its
``--workers 1`` value. The process pool for ``check --fix`` additionally allows
one config load per worker, since each process has its own run state. A race
that loads one file twice raises a count. A race that drops a file lowers one.
The gate sees a race only when the threads meet in it during the run, so
``tests/unit/test_run_state.py`` holds the deterministic tests of each lock.

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
        # check --fix spawns processes with separate config memos. Each used
        # worker may parse and merge the root config once more than the parent.
        process_config = (
            {"config.yaml_parses", "config.merges"} if scenario.command[0] == "check" else set()
        )
        for counter in process_config:
            assert (
                large["counters"][counter]
                <= parallel["counters"][counter]
                <= (large["counters"][counter] + 4)
            ), f"{name}: {counter} exceeded one extra config load per worker"
        moved = {
            counter: {"workers 1": large["counters"][counter], "workers 4": count}
            for counter in COUNTER_OWNER
            if counter not in process_config
            if (count := parallel["counters"][counter]) != large["counters"][counter]
        }
        assert moved == {}, f"{name}: --workers 4 does other work than --workers 1"
