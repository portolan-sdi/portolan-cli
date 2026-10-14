"""Live URL usage errors must preserve the CLI's text and JSON contracts."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("source", ["--url", "publish.public_url"])
@pytest.mark.parametrize("use_json", [False, True])
def test_http_live_url_reports_usage_error(
    conformant_catalog: Path, source: str, use_json: bool
) -> None:
    """Neither URL source can leak rashid's ValueError or replace JSON with text."""
    args = ["check", str(conformant_catalog), "--live"]
    if source == "--url":
        args.extend(["--url", "http://example.org/demo/"])
    else:
        config = conformant_catalog / ".portolan" / "config.yaml"
        config.write_text(config.read_text() + "publish:\n  public_url: http://example.org/demo/\n")
    if use_json:
        args.append("--json")

    result = CliRunner().invoke(cli, args)

    assert result.exit_code == 2
    assert isinstance(result.exception, SystemExit)
    message = f"--live needs an https base URL. Set {source} to an https URL."
    if use_json:
        envelope = json.loads(result.output)
        assert envelope["success"] is False
        assert envelope["command"] == "check"
        assert envelope["errors"] == [
            {
                "type": "InvalidLiveUrlError",
                "message": f"[PRTLN-VAL005] {message}",
                "code": "PRTLN-VAL005",
            }
        ]
    else:
        assert " ".join(result.output.split()) == f"✗ [PRTLN-VAL005] {message}"


@pytest.mark.parametrize("dry_run", [False, True])
def test_invalid_live_url_stops_before_fixes(conformant_catalog: Path, dry_run: bool) -> None:
    """A usage error must not mutate the catalog before it reaches the live pass."""
    args = ["check", str(conformant_catalog), "--fix", "--live", "--url", "http://example.org/"]
    if dry_run:
        args.append("--dry-run")
    with patch("portolan_cli.cli._run_fix_and_repairs") as repair:
        result = CliRunner().invoke(cli, args)
    assert result.exit_code == 2
    repair.assert_not_called()
