"""Extraction output separates skipped layers from extracted layers."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from portolan_cli.extract.common.cli_helpers import _print_dry_run, output_extract_result
from portolan_cli.extract.common.report import ExtractionReport, ExtractionSummary


def _layer(layer_id: int, status: str, error: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(id=layer_id, name=f"layer{layer_id}", status=status, error=error)


@pytest.mark.unit
def test_skipped_layers_excluded_from_extract_count(capsys: pytest.CaptureFixture[str]) -> None:
    report = SimpleNamespace(
        layers=[
            _layer(1, "pending"),
            _layer(2, "skipped", "no geometry"),
            _layer(3, "skipped", "table"),
        ]
    )
    _print_dry_run(report)
    out = capsys.readouterr().out
    assert "would extract 1 layer:" in out
    assert "Would skip 2 layers:" in out
    assert "layer2 (ID: 2): no geometry" in out
    assert "layer3 (ID: 3): table" in out


@pytest.mark.unit
def test_all_skipped_reports_zero(capsys: pytest.CaptureFixture[str]) -> None:
    report = SimpleNamespace(layers=[_layer(1, "skipped", "r1"), _layer(2, "skipped", "r2")])
    _print_dry_run(report)
    out = capsys.readouterr().out
    assert "would extract 0 layers:" in out
    assert "Would skip 2 layers:" in out


@pytest.mark.unit
@pytest.mark.parametrize("reason", [None, "", "no query capability"])
def test_single_skipped_layer(capsys: pytest.CaptureFixture[str], reason: str | None) -> None:
    report = SimpleNamespace(layers=[_layer(1, "skipped", reason)])
    _print_dry_run(report)
    suffix = f": {reason}" if reason else ""
    assert capsys.readouterr().out.splitlines() == [
        "→ ",
        "Dry run - would extract 0 layers:",
        "→ ",
        "Would skip 1 layer:",
        f"  • layer1 (ID: 1){suffix}",
    ]


@pytest.mark.unit
@pytest.mark.parametrize("layer_count", [0, 1, 2])
def test_dry_run_without_skipped_layers(
    capsys: pytest.CaptureFixture[str], layer_count: int
) -> None:
    report = SimpleNamespace(layers=[_layer(i, "pending") for i in range(layer_count)])
    _print_dry_run(report)
    noun = "layer" if layer_count == 1 else "layers"
    assert capsys.readouterr().out.splitlines() == [
        "→ ",
        f"Dry run - would extract {layer_count} {noun}:",
        *(f"  • layer{i} (ID: {i})" for i in range(layer_count)),
    ]


@pytest.mark.unit
def test_success_summary_includes_skipped_count(capsys: pytest.CaptureFixture[str]) -> None:
    report = Mock(
        spec=ExtractionReport,
        summary=ExtractionSummary(
            total_layers=4,
            succeeded=1,
            failed=0,
            skipped=3,
            empty=0,
            total_features=10,
            total_size_bytes=100,
            total_duration_seconds=1.0,
        ),
        folder_coverage=None,
    )
    output_dir = Path("output")
    output_extract_result(report, output_dir, use_json=False, dry_run=False)
    assert capsys.readouterr().out.splitlines() == [
        "",
        "✓ Extracted 1/4 layers (3 skipped)",
        f"→ Output: {output_dir}",
        f"→ Report: {output_dir}/.portolan/extraction-report.json",
    ]
