"""Delegated work must be counted in spawned conversion processes too."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from tests.perf.gates import COUNTER_OWNER, measure
from tests.perf.scenarios import SCENARIOS

if TYPE_CHECKING:
    from types import FrameType
    from typing import Any

pytestmark = pytest.mark.integration


def test_check_fix_counts_spawned_conversion_work(tmp_path: Path) -> None:
    scenario = SCENARIOS["check-fix-geo-assets"]
    serial = measure(scenario, tmp_path, 2)
    parallel = measure(scenario, tmp_path, 2, workers=2)
    for record in (serial, parallel):
        assert record["exit_code"] == scenario.exit_code, record["stdout"]
        assert record["network"] == []
    assert serial["counters"]["duckdb.connects"] == 10
    process_config = {"config.yaml_parses", "config.merges"}
    for name in process_config:
        assert (
            serial["counters"][name] <= parallel["counters"][name] <= serial["counters"][name] + 2
        )
    assert {
        name: parallel["counters"][name] for name in COUNTER_OWNER if name not in process_config
    } == {name: serial["counters"][name] for name in COUNTER_OWNER if name not in process_config}


def test_check_fix_has_a_worker_comparison() -> None:
    assert SCENARIOS["check-fix-geo-assets"].runs_phase_two


def _rewrite_and_audit(source: Path) -> tuple[dict[str, int], int, int]:
    import sys

    import pyarrow.parquet as pq

    from portolan_cli.formats import in_run
    from portolan_cli.preparation import convert_vector
    from portolan_cli.run_state import RunState
    from portolan_cli.trace import counters, tracing

    actual_opens = 0
    actual_connections = 0

    def audit(frame: FrameType, event: str, arg: Any) -> None:
        nonlocal actual_opens, actual_connections
        if (
            event == "call"
            and frame.f_globals.get("__name__") == "pyarrow.parquet.core"
            and frame.f_code.co_name == "__init__"
            and isinstance(frame.f_locals.get("self"), pq.ParquetFile)
        ):
            actual_opens += 1
        if (
            event == "c_call"
            and getattr(arg, "__module__", None) == "_duckdb"
            and getattr(arg, "__name__", None) == "connect"
        ):
            actual_connections += 1

    with tracing(), in_run(RunState.open(source.parent)):
        previous = sys.getprofile()
        sys.setprofile(audit)
        try:
            convert_vector(source, source.parent)
        finally:
            sys.setprofile(previous)
        reported = counters()
    assert "bbox" in pq.read_schema(source).names
    return reported, actual_opens, actual_connections


def test_spawned_worker_counts_delegated_footer_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import multiprocessing
    from concurrent.futures import ProcessPoolExecutor

    from portolan_cli.convert import _init_worker_state

    fixture = Path(__file__).resolve().parents[1] / "fixtures/vector/valid/points.parquet"
    source = tmp_path / "points.parquet"
    source.write_bytes(fixture.read_bytes())
    monkeypatch.setenv("PORTOLAN_PERF_WORKER_INIT", "tests.perf.runner:install_worker_counters")
    with ProcessPoolExecutor(
        max_workers=1,
        mp_context=multiprocessing.get_context("spawn"),
        initializer=_init_worker_state,
        initargs=(tmp_path, True),
    ) as pool:
        reported, actual_opens, actual_connections = pool.submit(
            _rewrite_and_audit, source
        ).result()
    assert actual_opens > 2  # Exercise delegated opens as well as the two core footers.
    assert actual_connections > 0
    assert reported["parquet.opens"] == actual_opens
    assert reported["duckdb.connects"] == actual_connections
