"""Tests for LayerSource, run_layer_extraction, and extractor_from (#947)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest
from click.testing import CliRunner

from portolan_cli.extract.common.source import (
    LayerMetadata,
    LayerRef,
    LayerSource,
    LayerSourceOptions,
    extractor_from,
    run_layer_extraction,
)
from portolan_cli.metadata_extraction import ExtractedMetadata
from portolan_cli.plugins.extractors import Extractor

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.integration

_LAYERS = {"points": "points.geojson", "lines": "lines.geojson"}


class _FixtureSource:
    """Two fixture GeoJSON files as layers. `fail` names layers whose fetch raises."""

    name = "fixtures"
    summary = "Layers from the test fixture GeoJSON files."

    def __init__(
        self, fixtures_dir: Path, fail: set[str] | None = None, empty: set[str] | None = None
    ) -> None:
        self._dir = fixtures_dir / "vector" / "valid"
        self.fail = fail or set()
        self.empty = empty or set()
        self.fetched: list[str] = []

    def discover(self, source: str) -> list[LayerRef]:
        return [LayerRef(id=i, name=name) for i, name in enumerate(_LAYERS)]

    def fetch(self, source: str, layer: LayerRef, dest: Path) -> int:
        import geoparquet_io as gpio  # type: ignore[import-untyped]

        self.fetched.append(layer.name)
        if layer.name in self.fail:
            raise ConnectionError(f"{layer.name} is down")
        if layer.name in self.empty:
            return 0
        table = gpio.convert(str(self._dir / _LAYERS[layer.name]))
        table.write(str(dest))
        return int(table.num_rows)

    def service_metadata(self, source: str) -> ExtractedMetadata:
        return ExtractedMetadata(
            source_type="fixtures", source_url=source, description="Fixture layers."
        )

    def layer_metadata(self, source: str, layer: LayerRef) -> LayerMetadata:
        return LayerMetadata(title=f"Fixture {layer.name}", description=f"The {layer.name} layer.")

    def styles(self, source: str, layer: LayerRef, collection_dir: Path) -> None:
        return None

    def source_link(self, source: str, layer: LayerRef) -> str:
        return f"{source}/{layer.name}"


_OPTIONS = LayerSourceOptions(license="CC0-1.0")


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
        _FixtureSource(fixtures_dir), "https://example.org/src", out, _OPTIONS
    )

    assert [(r.name, r.status) for r in report.layers] == [
        ("points", "success"),
        ("lines", "success"),
    ]
    assert report.summary.succeeded == 2
    assert (out / "points" / "points.parquet").is_file()
    assert _report(out)["summary"]["succeeded"] == 2
    collection = json.loads((out / "points" / "collection.json").read_text(encoding="utf-8"))
    vias = [link["href"] for link in collection["links"] if link["rel"] == "via"]
    assert vias == ["https://example.org/src/points"]
    assert collection["title"] == "Fixture points"
    assert (out / "catalog.json").is_file()


def test_filters_select_layers(fixtures_dir: Path, tmp_path: Path) -> None:
    options = LayerSourceOptions(license="CC0-1.0", layer_filter=["p*"], dry_run=True)

    report = run_layer_extraction(_FixtureSource(fixtures_dir), "src", tmp_path / "out", options)

    assert [r.name for r in report.layers] == ["points"]


def test_dry_run_writes_nothing(fixtures_dir: Path, tmp_path: Path) -> None:
    source = _FixtureSource(fixtures_dir)
    out = tmp_path / "out"

    report = run_layer_extraction(source, "src", out, LayerSourceOptions(dry_run=True))

    assert [r.status for r in report.layers] == ["pending", "pending"]
    assert source.fetched == []
    assert not out.exists()


def test_resume_skips_succeeded_layers(fixtures_dir: Path, tmp_path: Path) -> None:
    out = tmp_path / "out"
    first = _FixtureSource(fixtures_dir, fail={"lines"})
    run_layer_extraction(
        first, "src", out, LayerSourceOptions(license="CC0-1.0", retries=1, raw=True)
    )
    second = _FixtureSource(fixtures_dir)
    seen: list[tuple[str, str]] = []

    report = run_layer_extraction(
        second,
        "src",
        out,
        LayerSourceOptions(license="CC0-1.0", resume=True, raw=True),
        on_progress=lambda p: seen.append((p.layer_name, p.status)),
    )

    assert second.fetched == ["lines"]
    assert ("points", "skipped") in seen
    assert [(r.name, r.status) for r in report.layers] == [
        ("points", "success"),
        ("lines", "success"),
    ]


def test_failure_is_retried_then_recorded(fixtures_dir: Path, tmp_path: Path) -> None:
    source = _FixtureSource(fixtures_dir, fail={"lines"})
    options = LayerSourceOptions(license="CC0-1.0", retries=2, raw=True, retry_delay=0.0)

    report = run_layer_extraction(source, "src", tmp_path / "out", options)

    lines = report.layers[1]
    assert (lines.status, lines.attempts, lines.error) == ("failed", 2, "lines is down")
    assert source.fetched == ["points", "lines", "lines"]


def test_empty_layer_is_reported_as_empty(fixtures_dir: Path, tmp_path: Path) -> None:
    source = _FixtureSource(fixtures_dir, empty={"points"})

    report = run_layer_extraction(source, "src", tmp_path / "out", LayerSourceOptions(raw=True))

    assert report.layers[0].status == "empty"
    assert report.summary.empty == 1


def test_raw_builds_no_catalog(fixtures_dir: Path, tmp_path: Path) -> None:
    out = tmp_path / "out"

    run_layer_extraction(_FixtureSource(fixtures_dir), "src", out, LayerSourceOptions(raw=True))

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
