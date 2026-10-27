"""Iceberg uses the built-in plugin form (#947).

Its backend comes from a `portolan_backends` hook module, and its `version
rollback` and `version prune` commands come from `portolan.commands` entry
points that portolan-cli declares.
"""

from __future__ import annotations

import importlib
import subprocess
import sys
from importlib.metadata import entry_points
from typing import Any

import pytest
from click.testing import CliRunner

from portolan_cli.plugins import manager

pytestmark = pytest.mark.unit


def _cli() -> Any:
    return importlib.import_module("portolan_cli.cli").cli


def test_backend_comes_from_a_builtin_hook_module() -> None:
    """`builtin:iceberg` registers the `iceberg` name."""
    manager.reset_plugin_manager()

    results = dict(manager.call_each("portolan_backends"))

    assert "iceberg" in results["builtin:iceberg"]


def test_missing_extra_keeps_the_install_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without pyiceberg, `get_backend` names the extra to install."""
    from portolan_cli.backends import get_backend

    monkeypatch.setitem(sys.modules, "portolan_cli.backends.iceberg.backend", None)

    with pytest.raises(ValueError) as excinfo:
        get_backend("iceberg")

    assert str(excinfo.value) == (
        "The 'iceberg' backend requires the iceberg extra. "
        "Install it with: pip install portolan-cli[iceberg]"
    )


def test_commands_are_entry_points_of_portolan_cli() -> None:
    commands = {
        ep.name: (ep.value, ep.dist.name if ep.dist else None)
        for ep in entry_points(group="portolan.commands")
    }

    assert commands["version.rollback"] == (
        "portolan_cli.backends.iceberg.cli:rollback",
        "portolan-cli",
    )
    assert commands["version.prune"] == ("portolan_cli.backends.iceberg.cli:prune", "portolan-cli")


def test_version_help_shows_builtin_plugin_help_text() -> None:
    """A command that portolan-cli declares shows its own short help."""
    result = CliRunner().invoke(_cli(), ["version", "--help"])

    assert result.exit_code == 0
    assert "rollback  Rollback a collection to a previous version." in result.output
    assert "prune     Remove old versions, keeping the N most recent." in result.output
    assert "has the name of a built-in command" not in result.output


def test_none_keeps_the_iceberg_commands() -> None:
    result = CliRunner().invoke(
        _cli(), ["version", "rollback", "--help"], env={"PORTOLAN_PLUGINS": "none"}
    )

    assert result.exit_code == 0
    assert "version rollback [OPTIONS] COLLECTION TARGET_VERSION" in result.output


def test_command_and_hook_modules_load_without_pyiceberg() -> None:
    """The hook and command modules import neither pyiceberg nor pyarrow."""
    code = (
        "import sys\n"
        "import portolan_cli.backends.iceberg.cli, portolan_cli.backends.iceberg.hooks\n"
        "print(sorted(m for m in ('pyiceberg', 'pyarrow') if m in sys.modules))\n"
    )

    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )

    assert result.stdout.strip() == "[]"
