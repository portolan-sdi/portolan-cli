"""CLI wiring tests for `extract carto`."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.extract.carto.orchestrator import (
    ExtractionOptions,
    _build_dry_run_report,
)
from portolan_cli.extract.common.report import ExtractionReport

pytestmark = [pytest.mark.unit]


def _make_capturing_fake(captured: dict[str, object]) -> Callable[..., ExtractionReport]:
    """Build a fake extract_carto_catalog that records resolved options + filters."""

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
        captured["where"] = options.where
        captured["limit"] = options.limit
        captured["api_key"] = options.api_key
        captured["workers"] = options.workers
        captured["layer_filter"] = layer_filter
        captured["layer_exclude"] = layer_exclude
        return _build_dry_run_report(url, [])

    return fake_extract


def test_extract_carto_help_lists_flags() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["extract", "carto", "--help"])
    assert result.exit_code == 0
    for flag in ("--tables", "--exclude-tables", "--where", "--bbox", "--limit", "--api-key"):
        assert flag in result.output


def test_extract_carto_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "portolan_cli.extract.carto.orchestrator.extract_carto_catalog",
        _make_capturing_fake(captured),
    )
    runner = CliRunner()
    result = runner.invoke(
        cli, ["extract", "carto", "https://phl.carto.com", "--dry-run", "--auto"]
    )
    assert result.exit_code == 0
    assert captured["where"] is None
    assert captured["limit"] is None
    assert captured["workers"] == 1
    assert captured["layer_filter"] is None


def test_extract_carto_threads_carto_options(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "portolan_cli.extract.carto.orchestrator.extract_carto_catalog",
        _make_capturing_fake(captured),
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "extract",
            "carto",
            "https://phl.carto.com",
            "--tables",
            "vacant_*,zoning",
            "--where",
            "updated_at > '2026-01-01'",
            "--limit",
            "100",
            "--api-key",
            "secret",
            "--dry-run",
            "--auto",
        ],
    )
    assert result.exit_code == 0
    assert captured["where"] == "updated_at > '2026-01-01'"
    assert captured["limit"] == 100
    assert captured["api_key"] == "secret"
    assert captured["layer_filter"] == ["vacant_*", "zoning"]


def test_extract_carto_rejects_bad_bbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "portolan_cli.extract.carto.orchestrator.extract_carto_catalog",
        _make_capturing_fake({}),
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["extract", "carto", "https://phl.carto.com", "--bbox", "1,2,3", "--auto"],
    )
    assert result.exit_code == 1
