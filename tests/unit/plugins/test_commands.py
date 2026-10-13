"""Tests for the lazy plugin command group (#947)."""

from __future__ import annotations

import sys
from importlib.metadata import EntryPoint
from types import SimpleNamespace
from typing import cast

import click
import pytest
from click.testing import CliRunner

from portolan_cli.plugins import manager
from portolan_cli.plugins.commands import PluginGroup

pytestmark = pytest.mark.unit

_FIXTURES = "tests.unit.plugin_fixtures"


def _command(name: str, target: str, dist: str = "portolan-example") -> EntryPoint:
    ep = EntryPoint(name=name, value=f"{_FIXTURES}.{target}", group="portolan.commands")
    # `_for` attaches a distribution, as importlib.metadata does on discovery.
    attach = getattr(ep, "_for")  # noqa: B009 - private, absent from the stubs
    return cast("EntryPoint", attach(SimpleNamespace(name=dist)))


def _root() -> click.Group:
    """Build a small CLI with the same shape as `portolan`."""

    @click.group(cls=PluginGroup)
    def root() -> None:
        """Root."""

    @root.command()
    def add() -> None:
        """Built-in add."""
        click.echo("built-in add")

    @root.group()
    def version() -> None:
        """Version commands."""

    @version.command(name="list")
    def list_versions() -> None:
        """List versions."""

    return root


def test_help_lists_plugin_command_without_importing_it(
    installed: list[EntryPoint],
) -> None:
    """`--help` reads the name from metadata and imports nothing."""
    installed.append(_command("lazy", "lazy_commands:lazy"))
    sys.modules.pop(f"{_FIXTURES}.lazy_commands", None)

    result = CliRunner().invoke(_root(), ["--help"])

    assert result.exit_code == 0
    assert "lazy     (plugin: portolan-example)" in result.output
    assert f"{_FIXTURES}.lazy_commands" not in sys.modules


def test_plugin_group_runs(installed: list[EntryPoint]) -> None:
    """A plugin command group runs under its entry point name."""
    installed.append(_command("example", "commands:group"))

    result = CliRunner().invoke(_root(), ["example", "hello"])

    assert result.exit_code == 0
    assert result.output == "hello from the plugin\n"


def test_dotted_name_attaches_to_a_builtin_group(installed: list[EntryPoint]) -> None:
    """`version.rollback` becomes `portolan version rollback`."""
    installed.append(_command("version.rollback", "commands:rollback"))
    root = _root()

    help_result = CliRunner().invoke(root, ["version", "--help"])
    run_result = CliRunner().invoke(root, ["version", "rollback"])

    assert "rollback  (plugin: portolan-example)" in help_result.output
    assert "list      List versions." in help_result.output
    assert run_result.output == "plugin rollback\n"
    assert "version.rollback" not in CliRunner().invoke(root, ["--help"]).output


def test_builtin_wins_a_name_collision_and_help_warns(installed: list[EntryPoint]) -> None:
    """A plugin cannot replace a built-in command."""
    installed.append(_command("add", "commands:add"))
    root = _root()

    run_result = CliRunner().invoke(root, ["add"])
    help_result = CliRunner().invoke(root, ["--help"])

    assert run_result.output == "built-in add\n"
    assert "Plugin command 'add' from portolan-example has the name of a built-in" in (
        help_result.output
    )


def test_portolan_cli_entry_point_wins_a_name_collision(installed: list[EntryPoint]) -> None:
    """An outside plugin cannot replace a command that portolan-cli declares.

    The outside entry point comes first in discovery order, so the first-found
    rule alone would pick it.
    """
    installed.append(_command("version.rollback", "commands:add"))
    installed.append(_command("version.rollback", "commands:rollback", dist="portolan-cli"))
    root = _root()

    run_result = CliRunner().invoke(root, ["version", "rollback"])
    help_result = CliRunner().invoke(root, ["version", "--help"])

    assert run_result.output == "plugin rollback\n"
    assert "Plugin command 'rollback' from portolan-example has the name of a built-in" in (
        help_result.output
    )
    assert "(plugin: portolan-example)" not in help_result.output


def test_load_failure_warns_and_leaves_no_command(installed: list[EntryPoint]) -> None:
    """A command that fails to import produces a warning, then Click's usual error."""
    installed.append(_command("broken", "broken:command"))

    result = CliRunner().invoke(_root(), ["broken"])

    assert result.exit_code == 2
    assert "Plugin command 'broken' from portolan-example did not load" in result.output
    assert "No such command 'broken'" in result.output


def test_non_command_object_warns(installed: list[EntryPoint]) -> None:
    """An entry point must name a Click command."""
    installed.append(_command("odd", "commands:not_a_command"))

    result = CliRunner().invoke(_root(), ["odd"])

    assert result.exit_code == 2
    assert "Plugin command 'odd' from portolan-example is not a Click command" in result.output


def test_none_hides_plugin_commands(
    installed: list[EntryPoint], monkeypatch: pytest.MonkeyPatch
) -> None:
    """`PORTOLAN_PLUGINS=none` lists no plugin command."""
    installed.append(_command("example", "commands:group"))
    monkeypatch.setenv(manager.ENV_VAR, "none")

    result = CliRunner().invoke(_root(), ["--help"])

    assert "example" not in result.output
    assert "add" in result.output


def test_builtin_distribution_ignores_none(
    installed: list[EntryPoint], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A command that portolan-cli declares loads with `PORTOLAN_PLUGINS=none`."""
    installed.append(_command("version.rollback", "commands:rollback", dist="portolan-cli"))
    monkeypatch.setenv(manager.ENV_VAR, "none")

    result = CliRunner().invoke(_root(), ["version", "rollback"])

    assert result.output == "plugin rollback\n"


def test_portolan_cli_uses_plugin_groups() -> None:
    """The root group and every built-in subgroup accept plugin commands."""
    from portolan_cli.cli import cli

    groups = [cli, *(c for c in cli.commands.values() if isinstance(c, click.Group))]

    assert len(groups) == 6
    assert all(isinstance(group, PluginGroup) for group in groups)


def test_cli_shows_plugin_warnings_as_output_warnings(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The CLI prints a PluginWarning like any other warning line, without a source path."""
    import importlib

    cli_module = importlib.import_module("portolan_cli.cli")

    cli_module.show_warning(
        manager.PluginWarning("Plugin 'x' failed"), manager.PluginWarning, "/src/m.py", 3
    )
    cli_module.show_warning(UserWarning("other"), UserWarning, "/src/m.py", 3)

    err = capsys.readouterr().err
    assert err.startswith("⚠ Plugin 'x' failed\n")
    assert "/src/m.py:3: UserWarning: other" in err


def test_importing_the_cli_installs_show_warning() -> None:
    """`import portolan_cli.cli` routes warnings through `show_warning`."""
    import subprocess

    code = (
        "import importlib, warnings\n"
        "c = importlib.import_module('portolan_cli.cli')\n"
        "print(warnings.showwarning is c.show_warning)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )

    assert result.stdout.strip() == "True"


def test_shell_completion_lists_plugin_command_without_importing_it(
    installed: list[EntryPoint],
) -> None:
    """Tab completion reads plugin names from metadata, like `--help`."""
    from click.shell_completion import ShellComplete

    installed.append(_command("lazy", "lazy_commands:lazy"))
    sys.modules.pop(f"{_FIXTURES}.lazy_commands", None)

    completions = ShellComplete(_root(), {}, "portolan", "_PORTOLAN_COMPLETE").get_completions(
        [], "la"
    )

    assert [(item.value, item.help) for item in completions] == [
        ("lazy", "(plugin: portolan-example)")
    ]
    assert f"{_FIXTURES}.lazy_commands" not in sys.modules


def test_shell_completion_lists_builtin_commands(installed: list[EntryPoint]) -> None:
    """Built-in names keep their short help in completion."""
    from click.shell_completion import ShellComplete

    completions = ShellComplete(_root(), {}, "portolan", "_PORTOLAN_COMPLETE").get_completions(
        [], ""
    )

    assert [(item.value, item.help) for item in completions] == [
        ("add", "Built-in add."),
        ("version", "Version commands."),
    ]


def test_disabled_plugin_command_stops_running_after_a_reset(
    installed: list[EntryPoint], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A command loaded once does not outlive `PORTOLAN_PLUGINS=none`."""
    installed.append(_command("example", "commands:group"))
    root = _root()
    assert CliRunner().invoke(root, ["example", "hello"]).exit_code == 0

    monkeypatch.setenv(manager.ENV_VAR, "none")
    manager.reset_plugin_manager()
    result = CliRunner().invoke(root, ["example", "hello"])

    assert result.exit_code == 2
    assert "No such command 'example'" in result.output


def test_shipped_commands_leave_out_outside_plugins(installed: list[EntryPoint]) -> None:
    """The generated reference documents portolan-cli's own commands only."""
    installed.append(_command("version.rollback", "commands:rollback", dist="portolan-cli"))
    installed.append(_command("version.extra", "commands:rollback"))
    root = _root()
    version = root.commands["version"]
    assert isinstance(version, PluginGroup)
    ctx = click.Context(version, info_name="version", parent=click.Context(root))

    shipped = version.shipped_commands(ctx)

    assert sorted(shipped) == ["list", "rollback"]
