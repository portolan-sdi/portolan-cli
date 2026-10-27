"""Unit tests for the tracing module (spans and counters, issue #991)."""

from __future__ import annotations

import asyncio
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from portolan_cli import trace

pytestmark = pytest.mark.unit

_MIB = 1024 * 1024


def _names(nodes: list[dict[str, Any]]) -> list[str]:
    return [node["name"] for node in nodes]


class TestDisabled:
    def test_disabled_span_records_nothing(self) -> None:
        assert not trace.is_enabled()
        with trace.span("work", files=3) as s:
            s.set(bytes=10)
        assert trace.meta() is None
        assert trace.chrome_trace_events() == []

    def test_disabled_span_is_shared_null_object(self) -> None:
        assert trace.span("a") is trace.span("b")

    def test_disabled_count_records_nothing(self) -> None:
        trace.count("checksum.files")
        trace.count("checksum.bytes", 100)
        assert trace.counters() == {}


class TestSpans:
    def test_nested_spans_form_a_tree(self) -> None:
        trace.enable()
        with trace.span("root"):
            with trace.span("child", files=2) as child:
                child.set(bytes=7)
            with trace.span("sibling"):
                pass
        meta = trace.meta()
        assert meta is not None
        (root,) = meta["timings"]
        assert root["name"] == "root"
        assert _names(root["children"]) == ["child", "sibling"]
        assert root["children"][0]["attrs"] == {"files": 2, "bytes": 7}

    def test_span_records_wall_and_cpu_time(self) -> None:
        trace.enable()
        with trace.span("busy"):
            sum(range(200_000))
        meta = trace.meta()
        assert meta is not None
        node = meta["timings"][0]
        assert node["wall_s"] > 0
        assert node["cpu_s"] >= 0
        assert "open" not in node

    def test_open_span_reports_duration_so_far(self) -> None:
        trace.enable()
        with trace.span("running"):
            meta = trace.meta()
        assert meta is not None
        node = meta["timings"][0]
        assert node["open"] is True
        assert node["wall_s"] >= 0

    @pytest.mark.skipif(trace.resource is None, reason="Windows has no resource module")
    def test_span_records_rss_high_water(self) -> None:
        trace.enable()
        with trace.span("rss"):
            pass
        meta = trace.meta()
        assert meta is not None
        assert meta["timings"][0]["rss_peak_mb"] > 0
        assert meta["peak_rss_mb"] > 0

    def test_missing_resource_module_records_no_rss(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(trace, "resource", None)
        trace.enable()
        with trace.span("windows"):
            pass
        meta = trace.meta()
        assert meta is not None
        assert meta["timings"][0]["rss_peak_mb"] is None
        assert meta["peak_rss_mb"] is None

    def test_mem_mode_records_tracemalloc_growth(self) -> None:
        trace.enable(mem=True)
        with trace.span("outer"), trace.span("alloc"):
            block = bytearray(4 * _MIB)
            del block
        meta = trace.meta()
        assert meta is not None
        outer = meta["timings"][0]
        inner = outer["children"][0]
        assert inner["mem_growth_mb"] >= 3.9
        assert outer["mem_growth_mb"] >= inner["mem_growth_mb"]

    def test_mem_growth_leaves_out_memory_held_before_the_span(self) -> None:
        """Memory held at span entry is not the span's own cost.

        A module imported before the span and one imported inside the
        previous span must give the same number for the span.
        """
        trace.enable(mem=True)
        held = bytearray(8 * _MIB)
        with trace.span("alloc"):
            block = bytearray(4 * _MIB)
            del block
        del held
        meta = trace.meta()
        assert meta is not None
        growth = meta["timings"][0]["mem_growth_mb"]
        assert 3.9 <= growth <= 4.5

    def test_mem_growth_of_a_parent_keeps_its_peak_before_a_child(self) -> None:
        """A child span resets the tracemalloc peak. The parent keeps its own."""
        trace.enable(mem=True)
        held = bytearray(8 * _MIB)
        with trace.span("parent"):
            first = bytearray(6 * _MIB)
            del first
            with trace.span("child"):
                second = bytearray(2 * _MIB)
                del second
        del held
        meta = trace.meta()
        assert meta is not None
        parent = meta["timings"][0]
        child = parent["children"][0]
        assert 5.9 <= parent["mem_growth_mb"] <= 6.5
        assert 1.9 <= child["mem_growth_mb"] <= 2.5

    def test_mem_growth_absent_without_mem_mode(self) -> None:
        trace.enable()
        with trace.span("plain"):
            pass
        meta = trace.meta()
        assert meta is not None
        assert "mem_growth_mb" not in meta["timings"][0]


class TestCounters:
    def test_counters_are_seeded_with_documented_names(self) -> None:
        trace.enable()
        assert set(trace.KNOWN_COUNTERS) <= set(trace.counters())
        assert all(v == 0 for v in trace.counters().values())

    def test_known_counters_name_every_write_choke_point(self) -> None:
        """The perf gates in tests/perf read these names (#992)."""
        assert trace.KNOWN_COUNTERS == (
            "checksum.files",
            "checksum.bytes",
            "versions_json.reads",
            "versions_json.writes",
            "collection_json.writes",
            "items_parquet.writes",
            "config.yaml_parses",
            "config.merges",
            "versions.published",
            "thumbnail.draws",
            "tiles.fetched",
            "raster.opens",
            "parquet.opens",
            "duckdb.connects",
        )

    def test_tracing_enables_then_resets(self) -> None:
        with trace.tracing():
            assert trace.is_enabled()
            trace.count("config.merges", 2)
            assert trace.counters()["config.merges"] == 2
        assert not trace.is_enabled()
        assert trace.counters() == {}

    def test_tracing_resets_after_an_error(self) -> None:
        with pytest.raises(KeyError), trace.tracing():
            trace.count("config.merges")
            raise KeyError("boom")
        assert not trace.is_enabled()
        assert trace.counters() == {}

    def test_count_adds(self) -> None:
        trace.enable()
        trace.count("checksum.files")
        trace.count("checksum.bytes", 512)
        trace.count("checksum.bytes", 512)
        assert trace.counters()["checksum.files"] == 1
        assert trace.counters()["checksum.bytes"] == 1024

    def test_counters_are_thread_safe(self) -> None:
        trace.enable()

        def work() -> None:
            for _ in range(5_000):
                trace.count("parquet.opens")

        threads = [threading.Thread(target=work) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert trace.counters()["parquet.opens"] == 40_000

    def test_counters_returns_a_copy(self) -> None:
        trace.enable()
        snapshot = trace.counters()
        snapshot["raster.opens"] = 99
        assert trace.counters()["raster.opens"] == 0


class TestContextPropagation:
    def test_submit_keeps_parent_span_in_worker_thread(self) -> None:
        trace.enable()

        def task(i: int) -> int:
            with trace.span("worker", i=i):
                return threading.get_native_id()

        with trace.span("pool"), ThreadPoolExecutor(max_workers=2) as pool:
            futures = [trace.submit(pool, task, i) for i in range(4)]
            worker_tids = {f.result() for f in futures}
        meta = trace.meta()
        assert meta is not None
        (pool_node,) = meta["timings"]
        assert _names(pool_node["children"]) == ["worker"] * 4
        assert {c["tid"] for c in pool_node["children"]} == worker_tids

    def test_plain_submit_loses_parent(self) -> None:
        trace.enable()
        with trace.span("pool"), ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(lambda: trace.span("orphan").__enter__().__exit__(None, None, None))
        meta = trace.meta()
        assert meta is not None
        assert _names(meta["timings"]) == ["pool", "orphan"]

    def test_asyncio_tasks_inherit_parent(self) -> None:
        trace.enable()

        async def child(i: int) -> None:
            with trace.span("task", i=i):
                await asyncio.sleep(0)

        async def main() -> None:
            with trace.span("loop"):
                await asyncio.gather(*(child(i) for i in range(3)))

        asyncio.run(main())
        meta = trace.meta()
        assert meta is not None
        (loop_node,) = meta["timings"]
        assert _names(loop_node["children"]) == ["task"] * 3


class TestOutput:
    def test_meta_fields(self) -> None:
        trace.enable()
        meta = trace.meta()
        assert meta is not None
        assert set(meta) == {"timings", "counters", "peak_rss_mb", "version", "run_id"}
        assert meta["run_id"] == trace.run_id()
        assert isinstance(meta["version"], str)

    def test_chrome_trace_events(self, tmp_path: Path) -> None:
        trace.enable()
        with trace.span("root", files=1), ThreadPoolExecutor(max_workers=1) as pool:
            trace.submit(
                pool, lambda: trace.span("w").__enter__().__exit__(None, None, None)
            ).result()
        out = tmp_path / "trace.json"
        trace.write_chrome_trace(out)
        doc = json.loads(out.read_text())
        events = doc["traceEvents"]
        complete = [e for e in events if e["ph"] == "X"]
        assert {e["name"] for e in complete} == {"root", "w"}
        for event in complete:
            assert {"ts", "dur", "pid", "tid", "args"} <= set(event)
            assert event["dur"] >= 0
        # Perfetto reads tid as a 32-bit integer.
        assert all(0 < e["tid"] < 2**31 for e in complete)
        root = next(e for e in complete if e["name"] == "root")
        assert root["args"] == {"files": 1}
        thread_names = [e for e in events if e["ph"] == "M"]
        assert {e["tid"] for e in thread_names} == {e["tid"] for e in complete}

    def test_format_table_lists_top_level_spans(self) -> None:
        trace.enable()
        with trace.span("portolan.add"):
            with trace.span("add.collect"), trace.span("deep"):
                pass
            with trace.span("add.process"):
                pass
        table = trace.format_table()
        assert "add.collect" in table
        assert "add.process" in table
        assert "deep" not in table

    def test_attrs_are_json_safe(self) -> None:
        trace.enable()
        with trace.span("path", path=Path("/x/y")):
            pass
        meta = trace.meta()
        json.dumps(meta)


class TestEnableFromEnv:
    @pytest.mark.parametrize(
        ("env", "flag", "enabled", "mem"),
        [
            ({}, False, False, False),
            ({}, True, True, False),
            ({"PORTOLAN_TIMINGS": "1"}, False, True, False),
            ({"PORTOLAN_TIMINGS": "mem"}, False, True, True),
            ({"PORTOLAN_TIMINGS": "0"}, False, False, False),
            ({"PORTOLAN_TRACE_FILE": "t.json"}, False, True, False),
        ],
    )
    def test_env_and_flag(
        self,
        monkeypatch: pytest.MonkeyPatch,
        env: dict[str, str],
        flag: bool,
        enabled: bool,
        mem: bool,
    ) -> None:
        for key, value in env.items():
            monkeypatch.setenv(key, value)
        trace.enable_from_env(timings_flag=flag)
        assert trace.is_enabled() is enabled
        assert trace.mem_enabled() is mem
