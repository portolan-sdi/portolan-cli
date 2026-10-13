"""A Click group that adds commands from the ``portolan.commands`` group (#947).

Command names come from entry point metadata. The group imports a command
module only when the user runs that command. ``portolan --help`` and shell
completion import no plugin. Help lists a plugin command with its distribution
name, not its help text, because the help text lives in the module it would
have to import. A command that portolan-cli itself declares shows its help text.

A dotted name attaches to a built-in group: ``version.rollback`` becomes
``portolan version rollback``. A plugin command never replaces a built-in
command. Help warns about the collision.

This module is part of the CLI layer.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import click

from portolan_cli.output import warn
from portolan_cli.plugins.manager import (
    COMMANDS_ENTRY_POINT_GROUP,
    enabled_entry_points,
    is_builtin,
    shadowed_entry_points,
)

if TYPE_CHECKING:
    from importlib.metadata import EntryPoint

    from click.shell_completion import CompletionItem


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
        # Keyed by entry point. A command that PORTOLAN_PLUGINS disables after
        # a reset_plugin_manager() is no longer found, so it does not run.
        self._loaded: dict[EntryPoint, click.Command | None] = {}

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
        return sorted(set(self._builtin_names(ctx)) | set(self._plugin_entries(ctx)))

    def _builtin_names(self, ctx: click.Context) -> list[str]:
        """Names of the commands that win over a ``portolan.commands`` entry."""
        return super().list_commands(ctx)

    def _builtin_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        """Return a command that wins over a ``portolan.commands`` entry."""
        return super().get_command(ctx, cmd_name)

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        """Return a built-in command, or load the plugin command of that name."""
        builtin = self._builtin_command(ctx, cmd_name)
        if builtin is not None:
            return builtin
        ep = self._plugin_entries(ctx).get(cmd_name)
        if ep is None:
            return None
        if ep not in self._loaded:
            self._loaded[ep] = self._load(cmd_name, ep)
        return self._loaded[ep]

    def shipped_commands(self, ctx: click.Context) -> dict[str, click.Command]:
        """Return the commands portolan-cli ships in this group.

        The result holds the built-in commands and the entry point commands
        that portolan-cli declares. Commands from outside packages are left
        out, so a generated reference does not depend on installed plugins.

        Args:
            ctx: Context of this group.

        Returns:
            Command name to command.
        """
        shipped = dict(self.commands)
        for name, ep in self._plugin_entries(ctx).items():
            if name not in shipped and is_builtin(ep):
                command = self.get_command(ctx, name)
                if command is not None:
                    shipped[name] = command
        return shipped

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

    def _rows(self, ctx: click.Context) -> list[tuple[str, str | click.Command]]:
        """Return each visible command with its built-in or its plugin label.

        A plugin row holds ``(plugin: <distribution>)`` in place of the help
        text. The method imports no plugin. Click's own help and completion
        call ``get_command`` for each name, which would import every plugin.
        """
        plugins = self._plugin_entries(ctx)
        rows: list[tuple[str, str | click.Command]] = []
        for name in self.list_commands(ctx):
            builtin = self._builtin_command(ctx, name)
            if builtin is None and is_builtin(plugins[name]):
                # portolan-cli's own command modules import only Click and the
                # public API, so loading one for its help text costs little.
                own = self.get_command(ctx, name)
                if own is None:
                    rows.append((name, f"(plugin: {_distribution(plugins[name])})"))
                elif not own.hidden:
                    rows.append((name, own))
                continue
            if builtin is None:
                rows.append((name, f"(plugin: {_distribution(plugins[name])})"))
            elif not builtin.hidden:
                rows.append((name, builtin))
        return rows

    def format_commands(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        """Write the command list without importing a plugin."""
        plugins = self._plugin_entries(ctx)
        collisions = [
            (name, plugins[name]) for name in plugins.keys() & set(self._builtin_names(ctx))
        ]
        # An outside entry point can also lose its name to a portolan-cli one.
        path = _group_path(ctx)
        prefix = f"{path}." if path else ""
        collisions += [
            (ep.name[len(prefix) :], ep)
            for ep in shadowed_entry_points(COMMANDS_ENTRY_POINT_GROUP)
            if ep.name.startswith(prefix) and "." not in ep.name[len(prefix) :]
        ]
        for name, ep in sorted(collisions, key=lambda pair: pair[0]):
            warn(
                f"Plugin command '{name}' from {_distribution(ep)} has the "
                "name of a built-in command. The built-in command runs."
            )
        rows = self._rows(ctx)
        if not rows:
            return
        limit = formatter.width - 6 - max(len(name) for name, _ in rows)
        lines = [
            (name, item.get_short_help_str(limit) if isinstance(item, click.Command) else item)
            for name, item in rows
        ]
        with formatter.section("Commands"):
            formatter.write_dl(lines)

    def shell_complete(self, ctx: click.Context, incomplete: str) -> list[CompletionItem]:
        """Complete command names without importing a plugin, then options."""
        from click.shell_completion import CompletionItem

        results = [
            CompletionItem(
                name, help=item.get_short_help_str() if isinstance(item, click.Command) else item
            )
            for name, item in self._rows(ctx)
            if name.startswith(incomplete)
        ]
        # click.Command.shell_complete completes the options of this group.
        results.extend(click.Command.shell_complete(self, ctx, incomplete))
        return results


class ExtractGroup(PluginGroup):
    """The ``portolan extract`` group. Its subcommands come from extractors.

    The ``portolan_extractors`` hook supplies the extractors, the built-in
    ones included. The group calls the hook on first use, so ``portolan
    --help`` builds no plugin manager. An extractor command counts as a
    built-in command of this group.
    """

    def shipped_commands(self, ctx: click.Context) -> dict[str, click.Command]:
        """Add the built-in extractor commands. The plugin manager does not load."""
        from portolan_cli.plugins.builtin.extractors import portolan_extractors

        commands = super().shipped_commands(ctx)
        for extractor in portolan_extractors():
            commands.setdefault(extractor.name, extractor.cli_command())
        return commands

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Keyed by id(extractor). The value keeps the extractor alive, so its id
        # stays unique. A pluggable extractor need not be hashable. None records
        # a command that did not load, so help and the run warn about it once.
        self._extractor_commands: dict[int, tuple[object, click.Command | None]] = {}

    def _builtin_names(self, ctx: click.Context) -> list[str]:
        from portolan_cli.plugins.extractors import registered_extractors

        names = set(super()._builtin_names(ctx))
        names |= {name for name in registered_extractors() if self._builtin_command(ctx, name)}
        return sorted(names)

    def _builtin_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        command = super()._builtin_command(ctx, cmd_name)
        if command is not None:
            return command
        from portolan_cli.plugins.extractors import extractor_owner, registered_extractors

        extractor = registered_extractors().get(cmd_name)
        if extractor is None:
            return None
        if id(extractor) not in self._extractor_commands:
            try:
                loaded: click.Command | None = extractor.cli_command()
            except Exception as err:  # noqa: BLE001 - one broken extractor must not break the group
                warn(
                    f"Extractor '{cmd_name}' from {extractor_owner(cmd_name)} did not load "
                    f"its command: {err}"
                )
                loaded = None
            self._extractor_commands[id(extractor)] = (extractor, loaded)
        return self._extractor_commands[id(extractor)][1]
