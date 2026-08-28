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
        captured["catalog_id"] = options.catalog_id
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


@pytest.mark.unit
def test_extract_wfs_id_threads_into_options(monkeypatch: pytest.MonkeyPatch) -> None:
    """--id threads catalog_id into ExtractionOptions (issue #821)."""
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "portolan_cli.extract.wfs.orchestrator.extract_wfs_catalog",
        _make_capturing_fake(captured),
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["extract", "wfs", "https://example.com/wfs", "--id", "phl-housing", "--dry-run", "--auto"],
    )
    assert result.exit_code == 0
    assert captured["catalog_id"] == "phl-housing"


@pytest.mark.unit
def test_extract_wfs_id_defaults_to_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without --id the orchestrator keeps deriving the id from the directory name."""
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
    assert captured["catalog_id"] is None


@pytest.mark.unit
def test_extract_wfs_rejects_bad_id_before_extraction(monkeypatch: pytest.MonkeyPatch) -> None:
    """A bad --id fails before the download starts, not after it (issue #821)."""
    calls: list[str] = []

    def _never_called(*args: object, **kwargs: object) -> None:
        calls.append("extract")

    monkeypatch.setattr("portolan_cli.extract.wfs.orchestrator.extract_wfs_catalog", _never_called)
    runner = CliRunner()
    result = runner.invoke(
        cli, ["extract", "wfs", "https://example.com/wfs", "--id", "bad id", "--auto"]
    )

    assert result.exit_code == 1
    assert calls == []
    assert "Invalid catalog ID" in result.output


@pytest.mark.unit
def test_extract_wfs_warns_that_raw_ignores_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """--raw writes no catalog, so --id does nothing and says so (issue #821)."""
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "portolan_cli.extract.wfs.orchestrator.extract_wfs_catalog",
        _make_capturing_fake(captured),
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "extract",
            "wfs",
            "https://example.com/wfs",
            "--id",
            "phl-housing",
            "--raw",
            "--dry-run",
            "--auto",
        ],
    )

    assert result.exit_code == 0
    assert "Ignored --id 'phl-housing'" in result.output
