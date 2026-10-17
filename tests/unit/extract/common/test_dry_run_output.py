"""Dry-run output separates skipped layers from layers it would extract."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from portolan_cli.extract.common.cli_helpers import _print_dry_run


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
