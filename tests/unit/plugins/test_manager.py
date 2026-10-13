"""Tests for the plugin manager (#947)."""

from __future__ import annotations

import subprocess
import sys
import warnings
from contextlib import contextmanager
from importlib.metadata import EntryPoint
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from portolan_cli.hookspecs import EnrichContext, hookimpl
from portolan_cli.plugins import manager

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.unit

_FIXTURES = "tests.unit.plugin_fixtures"


def _ep(name: str, module: str, group: str = "portolan") -> EntryPoint:
    return EntryPoint(name=name, value=f"{_FIXTURES}.{module}", group=group)


@contextmanager
def _no_warnings() -> Iterator[None]:
    """Fail the test if the block emits a PluginLoadWarning."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", manager.PluginLoadWarning)
        yield


def _context() -> EnrichContext:
    return EnrichContext(
        catalog_root=Path("/catalog"),
        collection_id="roads",
        document_path=Path("/catalog/roads/a/a.json"),
        item_id="a",
    )


def _enrich(item: dict[str, object]) -> dict[str, object]:
    manager.get_plugin_manager().hook.portolan_enrich_item(item=item, context=_context())
    return item


def _plugin_names() -> set[str]:
    return {name for name, _ in manager.get_plugin_manager().list_name_plugin()}


def test_manager_is_built_once(installed: list[EntryPoint]) -> None:
    """Every call returns the same manager until a reset."""
    first = manager.get_plugin_manager()

    assert manager.get_plugin_manager() is first
    manager.reset_plugin_manager()
    assert manager.get_plugin_manager() is not first


def test_entry_point_plugin_runs_its_hook(installed: list[EntryPoint]) -> None:
    """A plugin from the `portolan` group runs under its entry point name."""
    installed.append(_ep("good", "good"))

    item = _enrich({"properties": {}})

    assert item == {"properties": {"fixture:good": "roads"}}
    assert "good" in _plugin_names()


def test_builtins_register_with_no_outside_plugin(installed: list[EntryPoint]) -> None:
    """Built-in hook modules register under a `builtin:` name."""
    names = _plugin_names()

    assert "builtin:backends" in names
    assert all(name.startswith("builtin:") for name in names)


@pytest.mark.parametrize("value", ["none", "NONE", " none "])
def test_none_disables_outside_plugins(
    installed: list[EntryPoint], monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    """`PORTOLAN_PLUGINS=none` loads built-ins only."""
    installed.append(_ep("good", "good"))
    monkeypatch.setenv(manager.ENV_VAR, value)

    assert _enrich({"properties": {}}) == {"properties": {}}
    assert "builtin:backends" in _plugin_names()


def test_list_enables_only_named_plugins(
    installed: list[EntryPoint], monkeypatch: pytest.MonkeyPatch
) -> None:
    """`PORTOLAN_PLUGINS=a,b` loads only those entry point names."""
    installed.extend([_ep("good", "good"), _ep("other", "broken")])
    monkeypatch.setenv(manager.ENV_VAR, "good")

    with _no_warnings():
        names = _plugin_names()

    assert "good" in names
    assert "other" not in names


def test_unknown_name_in_list_warns(
    installed: list[EntryPoint], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A name that no entry point declares produces a warning."""
    installed.append(_ep("good", "good"))
    monkeypatch.setenv(manager.ENV_VAR, "good, gone")

    with pytest.warns(manager.PluginLoadWarning, match="PORTOLAN_PLUGINS names 'gone'"):
        manager.get_plugin_manager()


def test_import_failure_warns_and_keeps_other_plugins(installed: list[EntryPoint]) -> None:
    """A plugin that fails to import is skipped with a warning."""
    installed.extend([_ep("broken", "broken"), _ep("good", "good")])

    with pytest.warns(manager.PluginLoadWarning, match="'broken'.*cannot import its dependency"):
        item = _enrich({"properties": {}})

    assert item == {"properties": {"fixture:good": "roads"}}
    assert "broken" not in _plugin_names()


def test_unknown_hook_warns_and_unregisters_plugin(installed: list[EntryPoint]) -> None:
    """A hook implementation that matches no hookspec is reported, not run."""
    installed.extend([_ep("misspelled", "misspelled"), _ep("good", "good")])

    with pytest.warns(manager.PluginLoadWarning, match="'misspelled'.*portolan_enrich_itme"):
        names = _plugin_names()

    assert "misspelled" not in names
    assert "good" in names


def test_duplicate_entry_points_load_once(installed: list[EntryPoint]) -> None:
    """Two distributions that declare one name load the plugin once."""
    installed.extend([_ep("good", "good"), _ep("good", "good")])

    with _no_warnings():
        item = _enrich({"properties": {}})

    assert item == {"properties": {"fixture:good": "roads"}}


def test_importing_the_cli_builds_no_manager() -> None:
    """The CLI module loads without creating the plugin manager."""
    code = (
        "import portolan_cli.cli\n"
        "from portolan_cli.plugins import manager\n"
        "print(manager._manager is None)\n"
    )

    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )

    assert result.stdout.strip() == "True"


def test_failed_registration_leaves_no_hook_behind(installed: list[EntryPoint]) -> None:
    """A plugin that fails validation contributes no hook at all.

    pluggy adds hook implementations one by one and stops at the first
    invalid one. The valid ones before it stay unless the manager removes them.
    """
    installed.append(_ep("half", "half"))

    with pytest.warns(manager.PluginLoadWarning, match="'half'.*did not register"):
        names = _plugin_names()

    assert "half" not in names
    assert [name for name, _ in manager.call_each("portolan_backends")] == [
        "builtin:backends",
        "builtin:iceberg",
    ]


class _Hooks:
    """Plugin objects with pluggy ordering options."""

    class TryLast:
        @hookimpl(trylast=True)
        def portolan_backends(self) -> dict[str, object]:
            return {"file": object}

    class TryFirst:
        @hookimpl(tryfirst=True)
        def portolan_backends(self) -> dict[str, object]:
            return {"first": object}

    class Plain:
        def __init__(self, name: str) -> None:
            self._name = name

        @hookimpl
        def portolan_backends(self) -> dict[str, object]:
            return {self._name: object}

    class Wrapper:
        @hookimpl(wrapper=True)
        def portolan_enrich_item(self, item: dict[str, object]) -> Iterator[None]:
            item["wrapped"] = True
            return (yield)

    class OldWrapper:
        @hookimpl(hookwrapper=True)
        def portolan_enrich_item(self, item: dict[str, object]) -> Iterator[None]:
            item["wrapped"] = True
            yield


def test_hook_calls_list_builtins_first(installed: list[EntryPoint]) -> None:
    """`trylast` and `tryfirst` cannot move a plugin before the built-ins."""
    pm = manager.get_plugin_manager()
    pm.register(_Hooks.TryLast(), name="last")
    pm.register(_Hooks.TryFirst(), name="first")

    names = [name for name, _ in manager.hook_calls("portolan_backends")]

    # Outside plugins follow pluggy call order: tryfirst, then trylast. The
    # deprecated backend group comes after every outside plugin.
    assert names == [
        "builtin:backends",
        "builtin:iceberg",
        "first",
        "last",
        "builtin:legacy_backends",
    ]


@pytest.mark.parametrize("wrapper", [_Hooks.Wrapper, _Hooks.OldWrapper])
def test_hook_calls_skip_wrappers_with_a_warning(
    installed: list[EntryPoint], wrapper: type
) -> None:
    """A hook wrapper is not a plain call, so the manager skips it."""
    manager.get_plugin_manager().register(wrapper(), name="wrapping")

    with pytest.warns(manager.PluginWarning, match="'wrapping'.*wrapper"):
        calls = manager.hook_calls("portolan_enrich_item")

    assert calls == []


def test_outside_plugin_cannot_take_the_builtin_prefix(installed: list[EntryPoint]) -> None:
    """An entry point named `builtin:...` would sort before the built-ins."""
    installed.append(_ep("builtin:good", "good"))

    with pytest.warns(manager.PluginLoadWarning, match="'builtin:good'.*reserved"):
        names = _plugin_names()

    assert "builtin:good" not in names


def test_outside_plugins_follow_pluggy_call_order(installed: list[EntryPoint]) -> None:
    """Pluggy calls the plugin registered last first. hook_calls does the same."""
    pm = manager.get_plugin_manager()
    pm.register(_Hooks.Plain("a"), name="registered-first")
    pm.register(_Hooks.Plain("b"), name="registered-second")

    names = [name for name, _ in manager.hook_calls("portolan_backends")]

    assert names == [
        "builtin:backends",
        "builtin:iceberg",
        "registered-second",
        "registered-first",
        "builtin:legacy_backends",
    ]
