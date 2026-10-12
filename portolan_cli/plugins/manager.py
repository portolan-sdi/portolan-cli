"""The plugin manager, built on the first hook call (#947).

Importing this module creates nothing. ``get_plugin_manager`` builds one
``pluggy.PluginManager`` and caches it. It registers the built-in hook modules
first, then each outside plugin from the ``portolan`` entry point group. Pluggy
calls the plugin registered last first, so outside plugins come before the
built-ins in hook results.

``PORTOLAN_PLUGINS`` controls discovery of outside plugins:

- unset: load every installed plugin.
- ``none``: load no outside plugin.
- ``a,b``: load only the entry points named ``a`` and ``b``.

Built-in plugins always load. A plugin that fails to load produces a
``PluginLoadWarning``, and the CLI keeps running.

The loop reads entry points itself rather than through
``PluginManager.load_setuptools_entrypoints``, which stops at the first
import error and has no filter.
"""

from __future__ import annotations

import functools
import importlib
import os
import warnings
from importlib.metadata import EntryPoint, entry_points
from typing import TYPE_CHECKING, Any

import pluggy

from portolan_cli import hookspecs

if TYPE_CHECKING:
    from collections.abc import Callable

ENTRY_POINT_GROUP = "portolan"
COMMANDS_ENTRY_POINT_GROUP = "portolan.commands"
# Deprecated in #947. Read by plugins.builtin.legacy_backends.
LEGACY_BACKENDS_ENTRY_POINT_GROUP = "portolan.backends"
ENV_VAR = "PORTOLAN_PLUGINS"
BUILTIN_DISTRIBUTION = "portolan-cli"
BUILTIN_PREFIX = "builtin:"

# Built-in hook modules. They implement the same hooks an outside plugin does.
_BUILTIN_MODULES = (
    "portolan_cli.plugins.builtin.backends",
    "portolan_cli.plugins.builtin.legacy_backends",
)

_manager: pluggy.PluginManager | None = None


class PluginWarning(UserWarning):
    """A plugin misbehaved. The CLI keeps running without its contribution."""


class PluginLoadWarning(PluginWarning):
    """A plugin failed to load. The CLI runs without it."""


@functools.cache
def _scan(group: str) -> tuple[EntryPoint, ...]:
    """Read one entry point group once per process.

    The scan reads every installed distribution. One ``portolan --help`` asks
    for the command group several times, and each scan costs about 8 ms with
    230 distributions installed. ``reset_plugin_manager`` clears the cache.
    """
    return tuple(entry_points(group=group))


def _requested_names() -> set[str] | None:
    """Parse ``PORTOLAN_PLUGINS``. None means every installed plugin."""
    raw = os.environ.get(ENV_VAR)
    if raw is None:
        return None
    value = raw.strip()
    if value.lower() == "none":
        return set()
    return {name.strip() for name in value.split(",") if name.strip()}


def _distribution(ep: EntryPoint) -> str | None:
    dist = getattr(ep, "dist", None)
    return dist.name if dist is not None else None


def _describe(ep: EntryPoint) -> str:
    return _distribution(ep) or "an unknown distribution"


def is_builtin(ep: EntryPoint) -> bool:
    """True for an entry point that portolan-cli itself declares."""
    return _distribution(ep) == BUILTIN_DISTRIBUTION


def plugin_names(ep: EntryPoint) -> set[str]:
    """Return the names ``PORTOLAN_PLUGINS`` uses to select ``ep``.

    A hook entry point is selected by its own name. A command is selected by
    the hook entry point names of its distribution, so one name enables or
    disables everything a package adds. A distribution with commands and no
    hooks is selected by the first segment of the command name.

    Args:
        ep: Entry point from either group.

    Returns:
        Names that select the entry point.
    """
    if ep.group == ENTRY_POINT_GROUP:
        return {ep.name}
    dist = _distribution(ep)
    hook_names = {
        hook.name
        for hook in _scan(ENTRY_POINT_GROUP)
        if dist is not None and _distribution(hook) == dist
    }
    return hook_names or {ep.name.split(".", 1)[0]}


def enabled_entry_points(group: str) -> list[EntryPoint]:
    """Return the entry points in ``group`` that ``PORTOLAN_PLUGINS`` enables.

    Entry points that portolan-cli declares always load. Two distributions can
    declare the same name. The first one wins.

    Args:
        group: Entry point group name.

    Returns:
        Entry points in discovery order, one per name.
    """
    requested = _requested_names()
    selected: dict[str, EntryPoint] = {}
    for ep in _scan(group):
        if requested is not None and not is_builtin(ep) and not plugin_names(ep) & requested:
            continue
        selected.setdefault(ep.name, ep)
    return list(selected.values())


def _warn(message: str) -> None:
    warnings.warn(message, PluginLoadWarning, stacklevel=3)


def _warn_unknown_requested_names() -> None:
    requested = _requested_names()
    if not requested:
        return
    known: set[str] = set()
    for group in (ENTRY_POINT_GROUP, COMMANDS_ENTRY_POINT_GROUP, LEGACY_BACKENDS_ENTRY_POINT_GROUP):
        for ep in _scan(group):
            known |= plugin_names(ep)
    for name in sorted(requested - known):
        _warn(f"{ENV_VAR} names '{name}', but no installed plugin has that name.")


def _register_outside_plugins(pm: pluggy.PluginManager) -> None:
    for ep in enabled_entry_points(ENTRY_POINT_GROUP):
        if ep.name.startswith(BUILTIN_PREFIX):
            _warn(
                f"Plugin '{ep.name}' from {_describe(ep)} uses the reserved '{BUILTIN_PREFIX}' prefix."
            )
            continue
        try:
            plugin = ep.load()
        except Exception as err:  # noqa: BLE001 - any import failure skips the plugin
            _warn(f"Plugin '{ep.name}' from {_describe(ep)} did not load: {err}")
            continue
        try:
            pm.register(plugin, name=ep.name)
        except (ValueError, pluggy.PluginValidationError) as err:
            # pluggy adds the hooks one by one and keeps the ones it added
            # before the invalid one. Remove them, so the plugin adds nothing.
            if isinstance(err, pluggy.PluginValidationError) and pm.is_registered(plugin):
                pm.unregister(plugin)
            _warn(f"Plugin '{ep.name}' from {_describe(ep)} did not register: {err}")


def _drop_plugins_with_unknown_hooks(pm: pluggy.PluginManager) -> None:
    """Unregister each plugin whose hook name no hookspec declares."""
    while True:
        try:
            pm.check_pending()
        except pluggy.PluginValidationError as err:
            name = pm.get_name(err.plugin)
            pm.unregister(err.plugin)
            _warn(f"Plugin '{name}' did not register: {err}")
        else:
            return


def get_plugin_manager() -> pluggy.PluginManager:
    """Return the plugin manager. Build it on the first call.

    Returns:
        The cached manager with built-in and enabled outside plugins.
    """
    global _manager
    if _manager is not None:
        return _manager

    pm = pluggy.PluginManager(hookspecs.PROJECT_NAME)
    pm.add_hookspecs(hookspecs)
    for module_name in _BUILTIN_MODULES:
        pm.register(
            importlib.import_module(module_name),
            name=f"{BUILTIN_PREFIX}{module_name.rsplit('.', 1)[1]}",
        )
    _warn_unknown_requested_names()
    _register_outside_plugins(pm)
    _drop_plugins_with_unknown_hooks(pm)
    _manager = pm
    return pm


def call_each(hook_name: str, **kwargs: Any) -> list[tuple[str, Any]]:
    """Call one hook plugin by plugin and keep the plugin name of each result.

    A plugin whose hook raises produces a ``PluginWarning``, and the call
    continues with the next plugin. A None result is dropped.

    Args:
        hook_name: Name of a hook in ``hookspecs``.
        **kwargs: Hook arguments.

    Returns:
        ``(plugin_name, result)`` pairs in registration order, built-ins first.
    """
    results: list[tuple[str, Any]] = []
    for plugin_name, call in hook_calls(hook_name):
        try:
            result = call(**kwargs)
        except Exception as err:  # noqa: BLE001 - one broken plugin must not stop the CLI
            warn_hook_failure(plugin_name, hook_name, err)
            continue
        if result is not None:
            results.append((plugin_name, result))
    return results


def hook_calls(hook_name: str) -> list[tuple[str, Callable[..., Any]]]:
    """Return one callable per plugin that implements ``hook_name``.

    Each callable takes the full hook arguments as keywords and passes on the
    ones the implementation names. The built-ins come first, then the outside
    plugins in pluggy order. ``tryfirst`` and ``trylast`` cannot move an
    outside plugin before a built-in. A hook wrapper produces a
    ``PluginWarning`` and is skipped, because a plain call of a wrapper only
    creates a generator. An empty list means no plugin implements the hook.

    Args:
        hook_name: Name of a hook in ``hookspecs``.

    Returns:
        ``(plugin_name, call)`` pairs.
    """
    pm = get_plugin_manager()
    calls: list[tuple[str, Callable[..., Any]]] = []
    for impl in getattr(pm.hook, hook_name).get_hookimpls():
        if impl.wrapper or impl.hookwrapper:
            warnings.warn(
                f"Plugin '{impl.plugin_name}' implements {hook_name} as a hook wrapper. "
                "Portolan calls each implementation on its own and skips wrappers.",
                PluginWarning,
                stacklevel=2,
            )
            continue

        def _call(_impl: Any = impl, **kwargs: Any) -> Any:
            return _impl.function(**{arg: kwargs[arg] for arg in _impl.argnames})

        calls.append((impl.plugin_name, _call))
    # sorted() is stable, so each side keeps its pluggy order.
    return sorted(calls, key=lambda pair: not pair[0].startswith(BUILTIN_PREFIX))


def warn_hook_failure(plugin_name: str, hook_name: str, err: Exception) -> None:
    """Warn that a plugin's hook raised."""
    warnings.warn(
        f"Plugin '{plugin_name}' failed in {hook_name}: {err}", PluginWarning, stacklevel=3
    )


def reset_plugin_manager() -> None:
    """Drop the cached manager and entry points. The next call reads them again."""
    global _manager
    _manager = None
    _scan.cache_clear()
