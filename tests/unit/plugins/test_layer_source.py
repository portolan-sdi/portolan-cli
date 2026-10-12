"""Tests for LayerSource, run_layer_extraction, and extractor_from (#947)."""

from __future__ import annotations

import json
import threading
import time
from typing import TYPE_CHECKING, Any

import pytest
from click.testing import CliRunner

from portolan_cli.extract.common.source import (
    Discovery,
    EmptyLayerError,
    LayerMetadata,
    LayerRef,
    LayerSource,
    LayerSourceOptions,
    SourceLink,
    extractor_from,
    run_layer_extraction,
)
from portolan_cli.metadata_extraction import ExtractedMetadata
from portolan_cli.plugins.extractors import Extractor

if TYPE_CHECKING:
    from pathlib import Path

    from portolan_cli.extract.common.progress import ExtractionProgress

pytestmark = pytest.mark.integration

_LAYERS = {"points": "points.geojson", "lines": "lines.geojson"}


class _FixtureSource:
    """Two fixture GeoJSON files as layers.

    `fail` names layers whose fetch raises, `empty` names layers with no
    features, and `slow` holds per-layer sleeps in seconds.
    """

    name = "fixtures"
    summary = "Layers from the test fixture GeoJSON files."

    def __init__(
        self,
        fixtures_dir: Path,
        fail: set[str] | None = None,
        empty: set[str] | None = None,
        slow: dict[str, float] | None = None,
    ) -> None:
        self._dir = fixtures_dir / "vector" / "valid"
        self.fail = fail or set()
        self.empty = empty or set()
        self.slow = slow or {}
        self.fetched: list[str] = []
        self.threads: set[str] = set()

    def discover(self, source: str, options: LayerSourceOptions) -> Discovery:
        return Discovery(
            source_url=source,
            layers=[LayerRef(id=i, name=name, data=name) for i, name in enumerate(_LAYERS)],
            catalog_seed=ExtractedMetadata(
                source_type="fixtures", source_url=source, description="Fixture layers."
            ),
            root_metadata=("Fixture catalog", "Fixture layers."),
        )

    def fetch(self, source: str, layer: LayerRef, dest: Path) -> int:
        import geoparquet_io as gpio  # type: ignore[import-untyped]

        self.fetched.append(layer.name)
        self.threads.add(threading.current_thread().name)
        time.sleep(self.slow.get(layer.name, 0.0))
        if layer.name in self.fail:
            raise ConnectionError(f"{layer.name} is down")
        if layer.name in self.empty:
            raise EmptyLayerError(f"No features returned for {layer.name}")
        table = gpio.convert(str(self._dir / _LAYERS[layer.name]))
        table.write(str(dest))
        return int(table.num_rows)

    def layer_metadata(self, source: str, layer: LayerRef) -> LayerMetadata:
        return LayerMetadata(title=f"Fixture {layer.name}", description=f"The {layer.name} layer.")

    def styles(self, source: str, layer: LayerRef, collection_dir: Path) -> None:
        return None

    def source_link(self, source: str, layer: LayerRef) -> SourceLink:
        return SourceLink(href=f"{source}/{layer.name}", title=f"Fixture layer: {layer.name}")


def _options(**fields: Any) -> LayerSourceOptions:
    return LayerSourceOptions(**{"license": "CC0-1.0", "retry_delay": 0.0, **fields})


def _report(output_dir: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(
        (output_dir / ".portolan" / "extraction-report.json").read_text(encoding="utf-8")
    )
    return data


def test_source_is_a_layer_source(fixtures_dir: Path) -> None:
    assert isinstance(_FixtureSource(fixtures_dir), LayerSource)


def test_run_writes_layers_report_and_catalog(fixtures_dir: Path, tmp_path: Path) -> None:
    """A run writes each layer, the report, and a catalog with provenance links."""
    out = tmp_path / "out"

    report = run_layer_extraction(
        _FixtureSource(fixtures_dir), "https://example.org/src", out, _options()
    )

    assert [(r.name, r.status, r.output_path) for r in report.layers] == [
        ("points", "success", "points/points.parquet"),
        ("lines", "success", "lines/lines.parquet"),
    ]
    assert report.summary.succeeded == 2
    assert _report(out)["summary"]["succeeded"] == 2
    collection = json.loads((out / "points" / "collection.json").read_text(encoding="utf-8"))
    vias = [(link["href"], link["title"]) for link in collection["links"] if link["rel"] == "via"]
    assert vias == [("https://example.org/src/points", "Fixture layer: points")]
    assert collection["title"] == "Fixture points"
    catalog = json.loads((out / "catalog.json").read_text(encoding="utf-8"))
    assert (catalog["title"], catalog["description"]) == ("Fixture catalog", "Fixture layers.")


def test_layer_path_sets_the_collection_directory(fixtures_dir: Path, tmp_path: Path) -> None:
    """A LayerRef path nests the collection; the file takes the last segment."""

    class _Nested(_FixtureSource):
        def discover(self, source: str, options: LayerSourceOptions) -> Discovery:
            found = super().discover(source, options)
            found.layers = [LayerRef(id=0, name="points", path="service/points_layer")]
            return found

    report = run_layer_extraction(
        _Nested(fixtures_dir), "src", tmp_path / "out", _options(raw=True)
    )

    assert report.layers[0].output_path == "service/points_layer/points_layer.parquet"


def test_filters_select_layers(fixtures_dir: Path, tmp_path: Path) -> None:
    report = run_layer_extraction(
        _FixtureSource(fixtures_dir),
        "src",
        tmp_path / "out",
        _options(layer_filter=["p*"], dry_run=True),
    )

    assert [r.name for r in report.layers] == ["points"]


def test_dry_run_writes_nothing(fixtures_dir: Path, tmp_path: Path) -> None:
    source = _FixtureSource(fixtures_dir)
    out = tmp_path / "out"

    report = run_layer_extraction(source, "src", out, LayerSourceOptions(dry_run=True))

    assert [(r.status, r.output_path, r.attempts) for r in report.layers] == [
        ("pending", "", 0),
        ("pending", "", 0),
    ]
    assert source.fetched == []
    assert not out.exists()


def test_resume_reuses_succeeded_layers(fixtures_dir: Path, tmp_path: Path) -> None:
    """A resumed run keeps the earlier result and fetches only what failed."""
    out = tmp_path / "out"
    first = run_layer_extraction(
        _FixtureSource(fixtures_dir, fail={"lines"}), "src", out, _options(retries=1, raw=True)
    )
    second = _FixtureSource(fixtures_dir)
    seen: list[tuple[str, str]] = []

    report = run_layer_extraction(
        second,
        "src",
        out,
        _options(resume=True, raw=True),
        on_progress=lambda p: seen.append((p.layer_name, p.status)),
    )

    assert second.fetched == ["lines"]
    assert seen[0] == ("points", "skipped")
    assert report.layers[0] == first.layers[0]
    assert [r.status for r in report.layers] == ["success", "success"]


def test_resume_does_not_retry_an_empty_layer(fixtures_dir: Path, tmp_path: Path) -> None:
    out = tmp_path / "out"
    run_layer_extraction(
        _FixtureSource(fixtures_dir, empty={"points"}, fail={"lines"}),
        "src",
        out,
        _options(retries=1, raw=True),
    )
    second = _FixtureSource(fixtures_dir)

    run_layer_extraction(second, "src", out, _options(resume=True, raw=True))

    assert second.fetched == ["lines"]


def test_failure_is_retried_then_recorded(fixtures_dir: Path, tmp_path: Path) -> None:
    source = _FixtureSource(fixtures_dir, fail={"lines"})

    report = run_layer_extraction(source, "src", tmp_path / "out", _options(retries=2, raw=True))

    lines = report.layers[1]
    assert (lines.status, lines.attempts, lines.error, lines.features, lines.output_path) == (
        "failed",
        2,
        "lines is down",
        None,
        None,
    )
    assert source.fetched == ["points", "lines", "lines"]


def test_empty_layer_is_reported_as_empty(fixtures_dir: Path, tmp_path: Path) -> None:
    source = _FixtureSource(fixtures_dir, empty={"points"})

    report = run_layer_extraction(source, "src", tmp_path / "out", _options(retries=1, raw=True))

    points = report.layers[0]
    assert (points.status, points.features, points.warnings) == (
        "empty",
        0,
        ["Layer has no features"],
    )
    assert report.summary.empty == 1


def test_progress_reports_each_step(fixtures_dir: Path, tmp_path: Path) -> None:
    seen: list[ExtractionProgress] = []

    run_layer_extraction(
        _FixtureSource(fixtures_dir, fail={"lines"}),
        "src",
        tmp_path / "out",
        _options(retries=1, raw=True),
        on_progress=seen.append,
    )

    assert [(p.layer_index, p.total_layers, p.layer_name, p.status, p.error) for p in seen] == [
        (0, 2, "points", "starting", None),
        (0, 2, "points", "extracting", None),
        (0, 2, "points", "success", None),
        (1, 2, "lines", "starting", None),
        (1, 2, "lines", "extracting", None),
        (1, 2, "lines", "failed", "lines is down"),
    ]


def test_workers_fetch_layers_in_parallel(fixtures_dir: Path, tmp_path: Path) -> None:
    """With two workers, both layers run on pool threads and keep their order."""
    source = _FixtureSource(fixtures_dir, slow={"points": 0.2})

    report = run_layer_extraction(source, "src", tmp_path / "out", _options(workers=2, raw=True))

    assert [r.name for r in report.layers] == ["points", "lines"]
    assert len(source.threads) == 2
    assert all(name != threading.main_thread().name for name in source.threads)


def test_layer_timeout_fails_a_slow_layer(fixtures_dir: Path, tmp_path: Path) -> None:
    # The fast layer gets a full second, so a loaded test machine cannot time it out.
    source = _FixtureSource(fixtures_dir, slow={"points": 3.0})

    report = run_layer_extraction(
        source, "src", tmp_path / "out", _options(workers=2, layer_timeout=1.0, raw=True)
    )

    points = report.layers[0]
    assert (points.status, points.error) == ("failed", "Timeout after 1.0s")
    assert report.layers[1].status == "success"


def test_raw_builds_no_catalog(fixtures_dir: Path, tmp_path: Path) -> None:
    out = tmp_path / "out"

    run_layer_extraction(_FixtureSource(fixtures_dir), "src", out, _options(raw=True))

    assert (out / "points" / "points.parquet").is_file()
    assert not (out / "catalog.json").exists()


def test_extractor_from_wraps_the_source(fixtures_dir: Path, tmp_path: Path) -> None:
    """The wrapper is an Extractor, and its command runs the driver."""
    extractor = extractor_from(_FixtureSource(fixtures_dir))
    out = tmp_path / "out"

    command = extractor.cli_command()
    result = CliRunner().invoke(
        command,
        [
            "https://example.org/src",
            str(out),
            "--auto",
            "--license",
            "CC0-1.0",
            "--layers",
            "points",
        ],
    )

    assert isinstance(extractor, Extractor)
    assert (extractor.name, command.name) == ("fixtures", "fixtures")
    assert result.exit_code == 0, result.output
    assert "Extracted 1/1 layers" in result.output
    assert (out / "points" / "collection.json").is_file()


def test_extractor_from_command_reports_a_missing_license(
    fixtures_dir: Path, tmp_path: Path
) -> None:
    command = extractor_from(_FixtureSource(fixtures_dir)).cli_command()

    result = CliRunner().invoke(command, ["src", str(tmp_path / "out"), "--auto", "--json"])

    assert result.exit_code == 1
    envelope = json.loads(result.output)
    assert envelope["command"] == "extract-fixtures"
    assert envelope["errors"][0]["type"] == "MissingLicenseError"
