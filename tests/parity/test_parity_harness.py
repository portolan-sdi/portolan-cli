"""The command-parity ratchet, and the ignore paths of the harness.

`PARITY_PENDING` lists each command that has no parity test yet. A family
agent that moves a command into the library adds its parity test and removes
the command from this list in the same commit. The list only shrinks.
`tests/parity/harness.py` gives the full convention.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click
import pytest

from tests.parity.harness import drop_path

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.unit

PARITY_DIR = Path(__file__).parent

# One command path per line, sorted, so parallel removals merge cleanly.
PARITY_PENDING = frozenset(
    {
        "add",
        "add-external",
        "check",
        "clean",
        "clone",
        "config get",
        "config list",
        "config set",
        "config unset",
        "extract arcgis",
        "extract carto",
        "extract wfs",
        "init",
        "list",
        "logo",
        "metadata init",
        "metadata validate",
        "partition",
        "pull",
        "push",
        "readme",
        "rm",
        "scan",
        "skills list",
        "skills show",
        "stac-geoparquet",
        "status",
        "sync",
        "version bump",
        "version current",
        "version list",
        "version prune",
        "version rollback",
    }
)


def _walk(group: click.Group, prefix: tuple[str, ...], ctx: click.Context) -> Iterator[str]:
    for name in group.list_commands(ctx):
        command = group.get_command(ctx, name)
        if isinstance(command, click.Group):
            yield from _walk(command, (*prefix, name), click.Context(command, ctx, name))
        # A third-party plugin owns its commands. CI installs the example
        # plugin in one lane, so the core list must not depend on it.
        elif (
            command is not None
            and command.callback is not None
            and command.callback.__module__.startswith("portolan_cli.")
        ):
            yield " ".join((*prefix, name))


def cli_command_paths() -> set[str]:
    """Return the path of every leaf command that `portolan_cli` itself defines."""
    from portolan_cli.cli import cli

    return set(_walk(cli, (), click.Context(cli, info_name="portolan")))


def covered_command_paths() -> set[str]:
    """Return each command that a `parity(...)` success call in `tests/parity/` names.

    `parity.error(...)` does not count. A failure case alone does not prove that
    the command and the library function return the same result.
    """
    covered: set[str] = set()
    for path in sorted(PARITY_DIR.glob("test_*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "parity"
            ):
                continue
            first = node.args[0] if node.args else None
            assert isinstance(first, ast.Constant) and isinstance(first.value, str), (
                f"{path.name}:{node.lineno}: the first argument of parity() must be a "
                "string literal, so this test can read the command path"
            )
            covered.add(first.value)
    return covered


def test_every_command_is_covered_or_pending() -> None:
    """A command with no parity test must be in `PARITY_PENDING`."""
    missing = cli_command_paths() - covered_command_paths() - PARITY_PENDING
    assert not missing, (
        f"commands with no parity test: {sorted(missing)}. Add a parity test in "
        "tests/parity/ (see tests/parity/harness.py)"
    )


def test_a_covered_command_leaves_pending() -> None:
    """The ratchet: a command with a parity test must leave `PARITY_PENDING`."""
    stale = covered_command_paths() & PARITY_PENDING
    assert not stale, f"remove these from PARITY_PENDING, they have parity tests: {sorted(stale)}"


def test_every_entry_names_a_real_command() -> None:
    """A removed or renamed command cannot linger in the list or in a parity call."""
    unknown = (covered_command_paths() | PARITY_PENDING) - cli_command_paths()
    assert not unknown, f"not a portolan command: {sorted(unknown)}"


def test_the_walk_reaches_groups_and_plugin_commands() -> None:
    """The walk sees subcommands of groups and the commands of the built-in plugins."""
    paths = cli_command_paths()
    assert {"config set", "version bump", "metadata init", "extract arcgis"} <= paths
    assert {"version rollback", "version prune"} <= paths
    assert "config" not in paths


def _sample() -> dict[str, Any]:
    return {
        "created": "2026-11-26T00:00:00",
        "collections": [{"id": "a", "updated": 1}, {"id": "b", "updated": 2}],
        "nested": {"duration": 0.5, "kept": True},
    }


def test_drop_path_removes_a_nested_key() -> None:
    """A dotted path removes the key at its end and keeps the rest."""
    data = _sample()
    drop_path(data, "nested.duration")
    drop_path(data, "created")
    assert data == {
        "collections": [{"id": "a", "updated": 1}, {"id": "b", "updated": 2}],
        "nested": {"kept": True},
    }


def test_drop_path_star_removes_from_each_list_element() -> None:
    """`*` applies the rest of the path to each element of a list."""
    data = _sample()
    drop_path(data, "collections.*.updated")
    assert data["collections"] == [{"id": "a"}, {"id": "b"}]


def test_drop_path_index_removes_one_list_element() -> None:
    """A number selects one element of a list."""
    data = _sample()
    drop_path(data, "collections.1")
    assert data["collections"] == [{"id": "a", "updated": 1}]


def test_drop_path_fails_on_a_missing_key() -> None:
    """An ignore path that is not in the result fails, so a typo cannot hide a field."""
    with pytest.raises(AssertionError, match="nested.missing"):
        drop_path(_sample(), "nested.missing")
