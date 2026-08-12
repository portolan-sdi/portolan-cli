"""Regression tests for the PR-scoped mutation-test configuration."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest
import tomllib

pytestmark = [pytest.mark.unit, pytest.mark.source_scan]

REPO_ROOT = Path(__file__).resolve().parents[2]
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"
PYPROJECT = REPO_ROOT / "pyproject.toml"
MUTATION_PATHSPEC = ":(glob)portolan_cli/**/*.py"


def test_pr_mutation_pathspec_includes_top_level_and_nested_modules() -> None:
    """The changed-file filter must reach both package-root and nested modules."""
    workflow = CI_WORKFLOW.read_text(encoding="utf-8")
    command_pattern = re.compile(
        r'git diff --name-only --diff-filter=d "\$BASE_SHA" HEAD -- \\\n\s*'
        + re.escape(f"'{MUTATION_PATHSPEC}'")
    )
    assert command_pattern.search(workflow)

    result = subprocess.run(
        ["git", "ls-files", MUTATION_PATHSPEC],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    matched = set(result.stdout.splitlines())

    assert "portolan_cli/add.py" in matched
    assert "portolan_cli/metadata/update.py" in matched


def test_mutmut_excludes_static_source_scans() -> None:
    """Instrumented mutation alternatives must not poison source-tree guards."""
    config = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    pytest_config = config["tool"]["pytest"]["ini_options"]
    mutmut_args = config["tool"]["mutmut"]["pytest_add_cli_args"]

    assert any(str(marker).startswith("source_scan:") for marker in pytest_config["markers"])

    marker_expression = mutmut_args[mutmut_args.index("-m") + 1]
    assert "not source_scan" in marker_expression
