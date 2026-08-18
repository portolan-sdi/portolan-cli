"""Regression tests for the nightly mutation-test configuration."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest
import tomllib

pytestmark = [pytest.mark.unit, pytest.mark.source_scan]

REPO_ROOT = Path(__file__).resolve().parents[2]
NIGHTLY_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "nightly.yml"
PYPROJECT = REPO_ROOT / "pyproject.toml"
SCRIPTS_DIR = REPO_ROOT / "scripts"

# The sweep mutates whatever scripts/shard_select.py walks. Read the root and the
# shard count out of the workflow rather than repeating them here, so a change to
# either one is measured instead of silently passing an outdated expectation.
SELECTOR_INVOCATION = re.compile(
    r"scripts/shard_select\.py \\\n\s*--root (?P<root>\S+) "
    r'--num-shards "\$NUM_SHARDS" --shard "\$SHARD"'
)
NUM_SHARDS_ASSIGNMENT = re.compile(r"^\s*NUM_SHARDS=(?P<count>\d+)\s*$", re.MULTILINE)


def _selector_config() -> tuple[str, int]:
    """Return the package root and shard count the nightly sweep runs with."""
    workflow = NIGHTLY_WORKFLOW.read_text(encoding="utf-8")

    invocation = SELECTOR_INVOCATION.search(workflow)
    assert invocation, "nightly.yml no longer runs scripts/shard_select.py"

    count = NUM_SHARDS_ASSIGNMENT.search(workflow)
    assert count, "nightly.yml no longer sets NUM_SHARDS"

    return invocation.group("root"), int(count.group("count"))


def test_nightly_sweep_covers_top_level_and_nested_modules() -> None:
    """Every shard together must mutate package-root and nested modules.

    A selector that walks one directory level, or a root pointed at a
    subpackage, drops whole regions of the tree. The sweep still exits zero,
    so the gap shows up only as mutants that no night ever tests (#612).
    """
    root_name, num_shards = _selector_config()
    if str(SCRIPTS_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPTS_DIR))
    from shard_select import select, shard_key

    root = REPO_ROOT / root_name
    keys = [shard_key(root, path) for path in sorted(root.rglob("*.py"))]
    swept = {key for shard in range(num_shards) for key in select(keys, num_shards, shard)}

    assert "portolan_cli/add.py" in swept
    assert "portolan_cli/metadata/update.py" in swept


def test_mutmut_excludes_static_source_scans() -> None:
    """Instrumented mutation alternatives must not poison source-tree guards."""
    config = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    pytest_config = config["tool"]["pytest"]["ini_options"]
    mutmut_args = config["tool"]["mutmut"]["pytest_add_cli_args"]

    assert any(str(marker).startswith("source_scan:") for marker in pytest_config["markers"])

    marker_expression = mutmut_args[mutmut_args.index("-m") + 1]
    assert "not source_scan" in marker_expression
