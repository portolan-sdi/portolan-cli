"""Integration tests for the logging flags and the ``meta`` block (issue #991)."""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

pytestmark = [pytest.mark.integration, pytest.mark.realdata]

REALDATA = Path(__file__).parent.parent / "fixtures" / "realdata"

ADD_STAGES = [
    "add.collect",
    "add.licenses",
    "add.process",
    "add.finalize",
    "add.deferred_non_geo",
    "add.enrich",
]

SECTION_4_COUNTERS = {
    "checksum.files",
    "checksum.bytes",
    "versions_json.reads",
    "config.yaml_parses",
    "thumbnail.draws",
    "raster.opens",
    "parquet.opens",
}


@pytest.fixture
def catalog_with_data(conformant_catalog: Path) -> Path:
    """A conformant catalog with one vector and one raster file to add.

    The vector file already declares a bbox covering column. A file without
    one makes ``add`` print a rewrite notice to stdout, also in JSON mode.
    """
    vectors = conformant_catalog / "districts"
    vectors.mkdir()
    shutil.copy(
        REALDATA / "philadelphia-housing" / "council_districts_2024.parquet",
        vectors / "council_districts_2024.parquet",
    )
    # Raster data needs an item folder inside the collection.
    rasters = conformant_catalog / "imagery" / "sample"
    rasters.mkdir(parents=True)
    shutil.copy(REALDATA / "rapidai4eo-sample.tif", rasters / "rapidai4eo-sample.tif")
    return conformant_catalog


def _add_args(catalog: Path) -> list[str]:
    return ["add", "--portolan-dir", str(catalog), str(catalog)]


def _walk(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for node in nodes:
        out.append(node)
        out.extend(_walk(node["children"]))
    return out


class TestTracingOff:
    def test_envelope_unchanged_when_tracing_off(self, catalog_with_data: Path) -> None:
        result = CliRunner().invoke(cli, ["--format", "json", *_add_args(catalog_with_data)])
        assert result.exit_code == 0, result.output
        envelope = json.loads(result.stdout)
        assert set(envelope) == {"success", "command", "data"}

    def test_error_envelope_unchanged_when_tracing_off(self, tmp_path: Path) -> None:
        result = CliRunner().invoke(
            cli, ["--format", "json", "add", "--portolan-dir", str(tmp_path), str(tmp_path)]
        )
        assert result.exit_code != 0
        envelope = json.loads(result.stdout)
        assert set(envelope) == {"success", "command", "data", "errors"}

    def test_no_timing_table_without_tracing(self, catalog_with_data: Path) -> None:
        result = CliRunner().invoke(cli, _add_args(catalog_with_data))
        assert result.exit_code == 0, result.output
        assert "wall s" not in result.stderr


class TestTimings:
    def test_meta_block_holds_add_stages_and_counters(
        self, catalog_with_data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PORTOLAN_TIMINGS", "1")
        result = CliRunner().invoke(cli, ["--format", "json", *_add_args(catalog_with_data)])
        assert result.exit_code == 0, result.output
        envelope = json.loads(result.stdout)
        meta = envelope["meta"]
        assert set(meta) == {"timings", "counters", "peak_rss_mb", "version", "run_id"}

        (root,) = meta["timings"]
        assert root["name"] == "portolan.add"
        names = [node["name"] for node in _walk(meta["timings"])]
        for stage in ADD_STAGES:
            assert stage in names
        assert "add.thumbnails" in names
        assert "thumbnail.collection" in names

        counters = meta["counters"]
        assert set(counters) >= SECTION_4_COUNTERS
        assert counters["checksum.files"] > 0
        assert counters["checksum.bytes"] > 0
        assert counters["versions_json.reads"] > 0
        assert counters["config.yaml_parses"] > 0
        assert counters["parquet.opens"] > 0
        assert counters["raster.opens"] > 0
        assert counters["thumbnail.draws"] > 0

    def test_timings_flag_turns_tracing_on(self, catalog_with_data: Path) -> None:
        result = CliRunner().invoke(
            cli, ["--timings", "--format", "json", *_add_args(catalog_with_data)]
        )
        assert result.exit_code == 0, result.output
        assert "meta" in json.loads(result.stdout)

    def test_error_envelope_carries_meta(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PORTOLAN_TIMINGS", "1")
        result = CliRunner().invoke(
            cli, ["--format", "json", "add", "--portolan-dir", str(tmp_path), str(tmp_path)]
        )
        assert result.exit_code != 0
        envelope = json.loads(result.stdout)
        assert envelope["success"] is False
        assert envelope["meta"]["timings"][0]["name"] == "portolan.add"

    def test_text_mode_prints_table_to_stderr(
        self, catalog_with_data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PORTOLAN_TIMINGS", "1")
        result = CliRunner().invoke(cli, _add_args(catalog_with_data))
        assert result.exit_code == 0, result.output
        assert "wall s" in result.stderr
        assert "add.process" in result.stderr
        assert "wall s" not in result.stdout


class TestRunIsolation:
    """Each invocation of ``cli`` in one process starts with fresh trace state."""

    @staticmethod
    def _failing_add(tmp_path: Path, *root_flags: str) -> dict[str, Any]:
        result = CliRunner().invoke(
            cli,
            [
                *root_flags,
                "--format",
                "json",
                "add",
                "--portolan-dir",
                str(tmp_path),
                str(tmp_path),
            ],
        )
        assert result.exit_code != 0
        envelope: dict[str, Any] = json.loads(result.stdout)
        return envelope

    def test_run_without_timings_after_a_timed_run_has_no_meta(self, tmp_path: Path) -> None:
        assert "meta" in self._failing_add(tmp_path, "--timings")
        assert set(self._failing_add(tmp_path)) == {"success", "command", "data", "errors"}

    def test_timed_run_reports_only_its_own_spans(self, tmp_path: Path) -> None:
        first = self._failing_add(tmp_path, "--timings")["meta"]
        second = self._failing_add(tmp_path, "--timings")["meta"]
        assert [node["name"] for node in second["timings"]] == ["portolan.add"]
        assert second["run_id"] != first["run_id"]


class TestTraceFile:
    def test_trace_file_holds_chrome_events(
        self, catalog_with_data: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = tmp_path / "trace.json"
        monkeypatch.setenv("PORTOLAN_TRACE_FILE", str(path))
        result = CliRunner().invoke(cli, ["--format", "json", *_add_args(catalog_with_data)])
        assert result.exit_code == 0, result.output
        events = json.loads(path.read_text())["traceEvents"]
        complete = [e for e in events if e["ph"] == "X"]
        assert "portolan.add" in {e["name"] for e in complete}
        for event in complete:
            assert isinstance(event["ts"], (int, float))
            assert event["dur"] >= 0
            assert {"pid", "tid"} <= set(event)


class TestVerbosity:
    def test_vv_logs_debug_to_stderr_only(self, catalog_with_data: Path) -> None:
        result = CliRunner().invoke(cli, ["-vv", *_add_args(catalog_with_data)])
        assert result.exit_code == 0, result.output
        assert re.search(r"^\+\d+\.\d{3}s DEBUG portolan_cli\.", result.stderr, re.MULTILINE)
        assert not re.search(r"^\+\d+\.\d{3}s ", result.stdout, re.MULTILINE)

    def test_vv_json_stdout_stays_parseable(self, catalog_with_data: Path) -> None:
        result = CliRunner().invoke(cli, ["-vv", "--format", "json", *_add_args(catalog_with_data)])
        assert result.exit_code == 0, result.output
        json.loads(result.stdout)

    def test_default_hides_debug(self, catalog_with_data: Path) -> None:
        result = CliRunner().invoke(cli, _add_args(catalog_with_data))
        assert result.exit_code == 0, result.output
        assert " DEBUG portolan_cli." not in result.stderr

    def test_root_help_separates_log_flag_from_subcommand_verbose(self) -> None:
        root_help = CliRunner().invoke(cli, ["--help"]).output
        assert "-v, --verbose" in root_help
        check_help = CliRunner().invoke(cli, ["check", "--help"]).output
        assert "portolan -v" in check_help
