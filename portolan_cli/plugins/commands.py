"""A Click group that adds commands from the ``portolan.commands`` group (#947).

Command names come from entry point metadata. The group imports a command
module only when the user runs that command, so ``portolan --help`` imports no
plugin. Help lists a plugin command with its distribution name, not its help
text, because the help text lives in the module it would have to import.

A dotted name attaches to a built-in group: ``version.rollback`` becomes
``portolan version rollback``. A plugin command never replaces a built-in
command. Help warns about the collision.

This module is part of the CLI layer.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import click

from portolan_cli.output import warn
from portolan_cli.plugins.manager import COMMANDS_ENTRY_POINT_GROUP, enabled_entry_points

if TYPE_CHECKING:
    from importlib.metadata import EntryPoint


def _group_path(ctx: click.Context) -> str:
    """Return the dotted path of the group below the root, or ``""`` for the root."""
    parts: list[str] = []
    current: click.Context | None = ctx
    while current is not None and current.parent is not None:
        parts.append(current.info_name or "")
        current = current.parent
    return ".".join(reversed(parts))


def _distribution(ep: EntryPoint) -> str:
    dist = getattr(ep, "dist", None)
    return dist.name if dist is not None else "an unknown distribution"


class PluginGroup(click.Group):
    """A group whose commands include plugin entry points.

    ``group_class = type`` makes every subgroup a ``PluginGroup`` too.
    """

    group_class = type

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._loaded: dict[str, click.Command | None] = {}

    def _plugin_entries(self, ctx: click.Context) -> dict[str, EntryPoint]:
        """Return the plugin commands for this group, keyed by command name."""
        path = _group_path(ctx)
        prefix = f"{path}." if path else ""
        entries: dict[str, EntryPoint] = {}
        for ep in enabled_entry_points(COMMANDS_ENTRY_POINT_GROUP):
            if not ep.name.startswith(prefix):
                continue
            name = ep.name[len(prefix) :]
            if name and "." not in name:
                entries.setdefault(name, ep)
        return entries

    def list_commands(self, ctx: click.Context) -> list[str]:
        """List built-in commands and plugin commands."""
        return sorted(set(super().list_commands(ctx)) | set(self._plugin_entries(ctx)))

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        """Return a built-in command, or load the plugin command of that name."""
        builtin = super().get_command(ctx, cmd_name)
        if builtin is not None:
            return builtin
        if cmd_name in self._loaded:
            return self._loaded[cmd_name]
        ep = self._plugin_entries(ctx).get(cmd_name)
        if ep is None:
            return None
        command = self._load(cmd_name, ep)
        self._loaded[cmd_name] = command
        return command

    @staticmethod
    def _load(cmd_name: str, ep: EntryPoint) -> click.Command | None:
        try:
            loaded = ep.load()
        except Exception as err:  # noqa: BLE001 - any import failure skips the command
            warn(f"Plugin command '{cmd_name}' from {_distribution(ep)} did not load: {err}")
            return None
        if not isinstance(loaded, click.Command):
            warn(f"Plugin command '{cmd_name}' from {_distribution(ep)} is not a Click command.")
            return None
        return loaded

    def format_commands(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        """Write the command list without importing a plugin.

        Click's version calls ``get_command`` for each name to read the short
        help, which would import every plugin command.
        """
        plugins = self._plugin_entries(ctx)
        rows: list[tuple[str, click.Command | EntryPoint]] = []
        for name in self.list_commands(ctx):
            builtin = super().get_command(ctx, name)
            if builtin is None:
                rows.append((name, plugins[name]))
                continue
            if name in plugins:
                warn(
                    f"Plugin command '{name}' from {_distribution(plugins[name])} has the "
                    "name of a built-in command. The built-in command runs."
                )
            if not builtin.hidden:
                rows.append((name, builtin))
        if not rows:
            return
        limit = formatter.width - 6 - max(len(name) for name, _ in rows)
        lines = [
            (
                name,
                item.get_short_help_str(limit)
                if isinstance(item, click.Command)
                else f"(plugin: {_distribution(item)})",
            )
            for name, item in rows
        ]
        with formatter.section("Commands"):
            formatter.write_dl(lines)
