"""The plugin manager, built on the first hook call."""

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
LEGACY_BACKENDS_ENTRY_POINT_GROUP = "portolan.backends"
ENV_VAR = "PORTOLAN_PLUGINS"
BUILTIN_DISTRIBUTION = "portolan-cli"
BUILTIN_PREFIX = "builtin:"

# Built-in hook modules, registered as ``builtin:<name>``. They implement the
# same hooks an outside plugin does.
_BUILTIN_MODULES = {
    "backends": "portolan_cli.plugins.builtin.backends",
    "extractors": "portolan_cli.plugins.builtin.extractors",
    "formats": "portolan_cli.plugins.builtin.formats",
    "iceberg": "portolan_cli.backends.iceberg.hooks",
    "legacy_backends": "portolan_cli.plugins.builtin.legacy_backends",
}
# Built-in modules that rank after the outside plugins. The deprecated backend
# group holds third-party backends, so a hook plugin wins a name over it.
_TRAILING_BUILTINS = frozenset({f"{BUILTIN_PREFIX}legacy_backends"})


class PluginWarning(UserWarning):
    """A plugin misbehaved. The CLI keeps running without its contribution."""


class PluginLoadWarning(PluginWarning):
    """A plugin failed to load. The CLI runs without it."""


@functools.cache
def _scan(group: str) -> tuple[EntryPoint, ...]:
    """Read one entry point group once per process."""
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
    """Return the names ``PORTOLAN_PLUGINS`` uses to select ``ep``."""
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
    """Return the entry points in ``group`` that ``PORTOLAN_PLUGINS`` enables."""
    selected: dict[str, EntryPoint] = {}
    for ep in _enabled(group):
        if ep.name not in selected or (is_builtin(ep) and not is_builtin(selected[ep.name])):
            selected[ep.name] = ep
    return list(selected.values())


def shadowed_entry_points(group: str) -> list[EntryPoint]:
    """Return the enabled outside entry points that lose a name to portolan-cli."""
    enabled = _enabled(group)
    own = {ep.name for ep in enabled if is_builtin(ep)}
    return [ep for ep in enabled if not is_builtin(ep) and ep.name in own]


def _enabled(group: str) -> list[EntryPoint]:
    """Return every entry point in ``group`` that ``PORTOLAN_PLUGINS`` enables."""
    requested = _requested_names()
    return [
        ep
        for ep in _scan(group)
        if requested is None or is_builtin(ep) or plugin_names(ep) & requested
    ]


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
        except Exception as err:  # any import failure skips the plugin
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


@functools.cache
def get_plugin_manager() -> pluggy.PluginManager:
    """Return the plugin manager. Build it on the first call."""
    pm = pluggy.PluginManager(hookspecs.PROJECT_NAME)
    pm.add_hookspecs(hookspecs)
    for name, module_name in _BUILTIN_MODULES.items():
        pm.register(importlib.import_module(module_name), name=f"{BUILTIN_PREFIX}{name}")
    _warn_unknown_requested_names()
    _register_outside_plugins(pm)
    _drop_plugins_with_unknown_hooks(pm)
    return pm


def call_each(hook_name: str, **kwargs: Any) -> list[tuple[str, Any]]:
    """Call one hook plugin by plugin and keep the plugin name of each result."""
    results: list[tuple[str, Any]] = []
    for plugin_name, call in hook_calls(hook_name):
        try:
            result = call(**kwargs)
        except Exception as err:  # one broken plugin must not stop the CLI
            warn_hook_failure(plugin_name, hook_name, err)
            continue
        if result is not None:
            results.append((plugin_name, result))
    return results


def hook_calls(hook_name: str) -> list[tuple[str, Callable[..., Any]]]:
    """Return one callable per plugin that implements ``hook_name``."""
    pm = get_plugin_manager()
    ranked: list[tuple[tuple[int, int], str, Callable[..., Any]]] = []
    # get_hookimpls() lists trylast, then registration order, then tryfirst.
    # Pluggy calls the list from the end.
    for index, impl in enumerate(getattr(pm.hook, hook_name).get_hookimpls()):
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

        ranked.append((_rank(impl.plugin_name, index), impl.plugin_name, _call))
    return [(name, call) for _, name, call in sorted(ranked, key=lambda entry: entry[0])]


def _rank(plugin_name: str, index: int) -> tuple[int, int]:
    """Order built-ins by registration and outside plugins by pluggy call order."""
    if plugin_name in _TRAILING_BUILTINS:
        return (2, index)
    if plugin_name.startswith(BUILTIN_PREFIX):
        return (0, index)
    return (1, -index)


def warn_hook_failure(plugin_name: str, hook_name: str, err: Exception) -> None:
    """Warn that a plugin's hook raised."""
    warnings.warn(
        f"Plugin '{plugin_name}' failed in {hook_name}: {err}", PluginWarning, stacklevel=3
    )


def reset_plugin_manager() -> None:
    """Drop the cached manager and entry points. The next call reads them again."""
    get_plugin_manager.cache_clear()
    _scan.cache_clear()
