"""Regression tests for the mutation-test configuration."""

from __future__ import annotations

from pathlib import Path

import pytest
import tomllib

pytestmark = [pytest.mark.unit, pytest.mark.source_scan]

REPO_ROOT = Path(__file__).resolve().parents[2]
PYPROJECT = REPO_ROOT / "pyproject.toml"


def test_mutmut_excludes_static_source_scans() -> None:
    """Instrumented mutation alternatives must not poison source-tree guards."""
    config = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    pytest_config = config["tool"]["pytest"]["ini_options"]
    mutmut_args = config["tool"]["mutmut"]["pytest_add_cli_args"]

    assert any(str(marker).startswith("source_scan:") for marker in pytest_config["markers"])

    marker_expression = mutmut_args[mutmut_args.index("-m") + 1]
    assert "not source_scan" in marker_expression
