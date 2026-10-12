"""Tests for the extractor hook and the `portolan extract` group (#947)."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import click
import pytest
from click.testing import CliRunner

from portolan_cli.hookspecs import hookimpl
from portolan_cli.plugins.extractors import Extractor, registered_extractors
from portolan_cli.plugins.manager import PluginWarning

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

pytestmark = pytest.mark.unit


@click.command("fake")
def _fake_command() -> None:
    """Extract from a fake source."""
    click.echo("fake extract ran")


class _FakeExtractor:
    name = "fake"
    summary = "Extract from a fake source."

    def extract(self, source: str, output_dir: Path, **options: Any) -> Any:
        return {"source": source, "output_dir": output_dir, **options}

    def cli_command(self) -> click.Command:
        return _fake_command


class _Plugin:
    def __init__(self, extractors: list[object]) -> None:
        self._extractors = extractors

    @hookimpl
    def portolan_extractors(self) -> list[object]:
        return self._extractors


def _cli() -> click.Group:
    group: click.Group = importlib.import_module("portolan_cli.cli").cli
    return group


def test_builtin_extractors_are_registered(register_plugin: Callable[..., None]) -> None:
    """ArcGIS, Carto, and WFS come through the same hook as a plugin."""
    extractors = registered_extractors()

    assert sorted(extractors) == ["arcgis", "carto", "wfs"]
    for name, extractor in extractors.items():
        assert isinstance(extractor, Extractor)
        assert extractor.cli_command().name == name


def test_builtin_extract_delegates_to_the_orchestrator(
    register_plugin: Callable[..., None], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """`extract()` calls the source's catalog function with the same arguments."""
    sentinel = object()
    orchestrator = MagicMock(return_value=sentinel)
    monkeypatch.setattr("portolan_cli.extract.wfs.orchestrator.extract_wfs_catalog", orchestrator)

    result = registered_extractors()["wfs"].extract(
        "https://example.org/wfs", tmp_path, layer_filter=["roads"]
    )

    assert result is sentinel
    orchestrator.assert_called_once_with(
        "https://example.org/wfs", tmp_path, layer_filter=["roads"]
    )


def test_plugin_extractor_appears_and_runs(register_plugin: Callable[..., None]) -> None:
    """A plugin extractor becomes `portolan extract <name>`."""
    register_plugin(_Plugin([_FakeExtractor()]), "fake-plugin")

    help_result = CliRunner().invoke(_cli(), ["extract", "--help"])
    run_result = CliRunner().invoke(_cli(), ["extract", "fake"])

    assert "  fake    Extract from a fake source.\n" in help_result.output
    assert run_result.output == "fake extract ran\n"


def test_builtin_wins_a_name_collision(register_plugin: Callable[..., None]) -> None:
    """A plugin cannot replace a built-in extractor."""
    shadow = _FakeExtractor()
    shadow.name = "wfs"
    register_plugin(_Plugin([shadow]), "shadow-plugin")

    with pytest.warns(PluginWarning, match=r"'shadow-plugin' registers extractor 'wfs'"):
        extractors = registered_extractors()

    assert extractors["wfs"] is not shadow


def test_non_extractor_is_dropped_with_a_warning(register_plugin: Callable[..., None]) -> None:
    """A hook result that is not an Extractor names the plugin and is ignored."""
    register_plugin(_Plugin([object()]), "odd-plugin")

    with pytest.warns(PluginWarning, match=r"'odd-plugin' returned object, which is not"):
        extractors = registered_extractors()

    assert sorted(extractors) == ["arcgis", "carto", "wfs"]


def test_extract_help_lists_the_builtin_sources(register_plugin: Callable[..., None]) -> None:
    result = CliRunner().invoke(_cli(), ["extract", "--help"])

    assert result.exit_code == 0
    commands = result.output.split("Commands:")[1].split()
    assert [w for w in commands if w in {"arcgis", "carto", "wfs"}] == ["arcgis", "carto", "wfs"]


def test_shipped_commands_list_the_builtin_sources_without_plugins(
    register_plugin: Callable[..., None],
) -> None:
    """The reference docs read the shipped commands. No plugin manager builds."""
    from portolan_cli.plugins import manager

    register_plugin(_Plugin([_FakeExtractor()]), "fake-plugin")
    manager.reset_plugin_manager()
    extract_group = _cli().commands["extract"]

    root = click.Context(_cli(), info_name="portolan")
    ctx = click.Context(extract_group, info_name="extract", parent=root)

    shipped = extract_group.shipped_commands(ctx)  # type: ignore[attr-defined]

    assert sorted(shipped) == ["arcgis", "carto", "wfs"]
    assert manager._manager is None
