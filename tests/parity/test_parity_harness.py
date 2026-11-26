"""The command-parity ratchet, and the ignore paths of the harness.

`PARITY_PENDING` lists each command that has no parity test yet. A family
agent that moves a command into the library adds its parity test and removes
the command from this list in the same commit. The list only shrinks.
`tests/parity/harness.py` gives the full convention.
"""

from __future__ import annotations

import ast
import json
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click
import pytest

from tests.parity.harness import drop_path

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from portolan_cli.inspect import CatalogInfo
    from tests.parity.harness import Parity

pytestmark = pytest.mark.unit

PARITY_DIR = Path(__file__).parent

# One command path per line, sorted, so parallel removals merge cleanly.
PARITY_PENDING = frozenset(
    {
        "partition",
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
        covered |= parity_calls(path.read_text(encoding="utf-8"), path.name)
    return covered


def parity_calls(source: str, name: str) -> set[str]:
    """Return the command path of each `parity("...")` call in a test module that runs.

    A call inside a function or class with a skip, skipif, or xfail decorator
    does not count, because that test does not prove parity.
    """
    covered: set[str] = set()
    nodes: list[ast.AST] = [ast.parse(source)]
    while nodes:
        node = nodes.pop()
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef) and any(
            word in ast.unparse(d) for d in node.decorator_list for word in ("skip", "xfail")
        ):
            continue
        nodes.extend(ast.iter_child_nodes(node))
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "parity"
        ):
            continue
        first = node.args[0] if node.args else None
        assert isinstance(first, ast.Constant) and isinstance(first.value, str), (
            f"{name}:{node.lineno}: the first argument of parity() must be a "
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


def test_a_skipped_or_xfailed_parity_call_is_not_coverage() -> None:
    """A parity test that does not run does not prove parity."""
    source = """
import pytest

@pytest.mark.skip(reason="not yet")
def test_a(parity):
    parity("logo", [])

@pytest.mark.xfail(reason="not yet", strict=True)
def test_b(parity):
    parity("rm", [])

@pytest.mark.skipif(True, reason="not yet")
class TestC:
    def test_c(self, parity):
        parity("scan", [])

def test_d(parity):
    parity("info", [])
"""
    assert parity_calls(source, "test_sample.py") == {"info"}


@pytest.mark.integration
def test_a_command_that_breaks_its_catalog_fails(
    parity: Parity,
    build_conformant_catalog: Callable[..., Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The harness checks the catalog invariants on the command side, before the rebuild.

    The stub writes an absolute path into catalog.json on the first call only,
    which is the command side. The two results stay equal.
    """
    from portolan_cli import inspect

    real = inspect.inspect_catalog
    calls: list[Path] = []

    def breaking(root: Path) -> CatalogInfo:
        result = real(root)
        if not calls:
            catalog = root / "catalog.json"
            data = json.loads(catalog.read_text(encoding="utf-8"))
            data["description"] = str(catalog.resolve())
            catalog.write_text(json.dumps(data), encoding="utf-8")
        calls.append(root)
        return result

    monkeypatch.setattr(inspect, "inspect_catalog", breaking)
    # parity.__call__ and not parity(...): a test of the harness is not coverage of info.
    with pytest.raises(pytest.fail.Exception, match="P-NO-LOCAL-PATH"):
        parity.__call__(
            "info", [], catalog=build_conformant_catalog, call=lambda root: breaking(Path())
        )
    assert len(calls) == 1
    shutil.rmtree(parity.root)


class _Loud:
    """A result whose ``to_dict()`` prints."""

    def __init__(self, inner: CatalogInfo) -> None:
        self.inner = inner

    def to_dict(self) -> dict[str, Any]:
        print("loud")  # noqa: T201 - the test needs a to_dict that prints
        return self.inner.to_dict()


@pytest.mark.integration
def test_a_to_dict_that_prints_fails(
    parity: Parity, build_conformant_catalog: Callable[..., Path]
) -> None:
    """``to_dict()`` is part of the library result, so it must not print either."""
    from portolan_cli.inspect import inspect_catalog

    with pytest.raises(AssertionError, match="printed output"):
        parity.__call__(
            "info",
            [],
            catalog=build_conformant_catalog,
            call=lambda root: _Loud(inspect_catalog(Path())),
        )


@pytest.mark.integration
def test_a_library_log_record_is_not_printed_output(
    parity: Parity, build_conformant_catalog: Callable[..., Path]
) -> None:
    """The command run leaves its stderr log handler. The library side must not inherit it.

    A library function logs a notice when its caller passes no callback. The
    harness counts only what the library prints, not what the CLI handler writes.
    """
    import logging

    from portolan_cli.inspect import inspect_catalog

    def call(root: Path) -> CatalogInfo:
        logging.getLogger("portolan_cli.parity_probe").warning("library notice")
        return inspect_catalog(Path())

    parity.__call__("info", [], catalog=build_conformant_catalog, call=call)
