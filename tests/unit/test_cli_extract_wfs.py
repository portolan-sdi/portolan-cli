"""CLI wiring tests for `extract wfs` page-size and auto-tile flags (Issue #529)."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.extract.common.report import ExtractionReport
from portolan_cli.extract.wfs.orchestrator import (
    ExtractionOptions,
    _build_dry_run_report,
)


def _make_capturing_fake(
    captured: dict[str, object],
) -> Callable[..., ExtractionReport]:
    """Build a fake extract_wfs_catalog that records the resolved options."""

    def fake_extract(
        url: str,
        output_dir: Path,
        *,
        layer_filter: list[str] | None = None,
        layer_exclude: list[str] | None = None,
        options: ExtractionOptions | None = None,
        on_progress: object = None,
    ) -> ExtractionReport:
        assert options is not None
        captured["page_size"] = options.page_size
        captured["auto_tile"] = options.auto_tile
        return _build_dry_run_report(url=url, layers=[], discovery_result=None)

    return fake_extract


@pytest.mark.unit
def test_extract_wfs_has_page_size_and_auto_tile_flags() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["extract", "wfs", "--help"])
    assert result.exit_code == 0
    for flag in ("--page-size", "--auto-tile", "--no-auto-tile"):
        assert flag in result.output


@pytest.mark.unit
def test_extract_wfs_defaults_thread_gpio_13_values(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default invocation builds options with page_size=100000 and auto_tile=True."""
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "portolan_cli.extract.wfs.orchestrator.extract_wfs_catalog",
        _make_capturing_fake(captured),
    )
    runner = CliRunner()
    result = runner.invoke(
        cli, ["extract", "wfs", "https://example.com/wfs", "--dry-run", "--auto"]
    )
    assert result.exit_code == 0
    assert captured["page_size"] == 100000
    assert captured["auto_tile"] is True


@pytest.mark.unit
def test_extract_wfs_no_auto_tile_threads_false(monkeypatch: pytest.MonkeyPatch) -> None:
    """--no-auto-tile threads auto_tile=False into ExtractionOptions."""
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "portolan_cli.extract.wfs.orchestrator.extract_wfs_catalog",
        _make_capturing_fake(captured),
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["extract", "wfs", "https://example.com/wfs", "--no-auto-tile", "--dry-run", "--auto"],
    )
    assert result.exit_code == 0
    assert captured["auto_tile"] is False
