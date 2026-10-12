"""Tests for format handler discovery and precedence (#947)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from portolan_cli.errors import AmbiguousFormatError, UnknownFormatHandlerError
from portolan_cli.hookspecs import hookimpl
from portolan_cli.plugins.formats import FormatHandler, format_handlers, handler_for
from portolan_cli.plugins.manager import PluginWarning
from tests.unit.plugins.stub_handler import StubHandler

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

pytestmark = pytest.mark.unit


class _Plugin:
    def __init__(self, *handlers: object) -> None:
        self._handlers = list(handlers)

    @hookimpl
    def portolan_formats(self) -> list[object]:
        return self._handlers


def _touch(tmp_path: Path, name: str) -> Path:
    path = tmp_path / name
    path.write_text("x", encoding="utf-8")
    return path


def _pin(tmp_path: Path, handler: str) -> None:
    (tmp_path / ".portolan").mkdir()
    (tmp_path / ".portolan" / "config.yaml").write_text(
        f"conversion:\n  handlers:\n    .tif: {handler}\n", encoding="utf-8"
    )


def test_builtin_handlers_route_vector_and_raster(
    register_plugin: Callable[..., None], tmp_path: Path
) -> None:
    gpkg = handler_for(_touch(tmp_path, "a.gpkg"))
    tif = handler_for(_touch(tmp_path, "a.tif"))

    assert gpkg is not None
    assert tif is not None
    assert (gpkg.name, gpkg.family) == ("geoparquet", "vector")
    assert (tif.name, tif.family) == ("cog", "raster")


def test_unknown_extension_has_no_handler(
    register_plugin: Callable[..., None], tmp_path: Path
) -> None:
    assert handler_for(_touch(tmp_path, "notes.txt")) is None


def test_stac_json_is_not_geojson(register_plugin: Callable[..., None], tmp_path: Path) -> None:
    path = tmp_path / "item.json"
    path.write_text('{"type": "Feature", "stac_version": "1.1.0"}', encoding="utf-8")

    assert handler_for(path) is None


def test_plugin_adds_a_new_extension(register_plugin: Callable[..., None], tmp_path: Path) -> None:
    xyz = StubHandler("xyz", frozenset({".xyz"}))
    register_plugin(_Plugin(xyz), "points-plugin")

    assert handler_for(_touch(tmp_path, "a.xyz")) is xyz


def test_plugin_comes_before_a_builtin(
    register_plugin: Callable[..., None], tmp_path: Path
) -> None:
    better_tif = StubHandler("better-tif", frozenset({".tif"}), family="raster")
    register_plugin(_Plugin(better_tif), "tif-plugin")

    assert handler_for(_touch(tmp_path, "a.tif")) is better_tif


def test_two_plugins_for_one_extension_need_a_pin(
    register_plugin: Callable[..., None], tmp_path: Path
) -> None:
    register_plugin(_Plugin(StubHandler("one", frozenset({".tif"}))), "one-plugin")
    register_plugin(_Plugin(StubHandler("two", frozenset({".tif"}))), "two-plugin")

    with pytest.raises(AmbiguousFormatError, match=r"conversion\.handlers") as err:
        handler_for(_touch(tmp_path, "a.tif"))

    assert "'one'" in str(err.value)
    assert "'two'" in str(err.value)


def test_config_pin_selects_the_handler(
    register_plugin: Callable[..., None], tmp_path: Path
) -> None:
    register_plugin(_Plugin(StubHandler("one", frozenset({".tif"}))), "one-plugin")
    register_plugin(_Plugin(StubHandler("two", frozenset({".tif"}))), "two-plugin")
    _pin(tmp_path, "cog")

    handler = handler_for(_touch(tmp_path, "a.tif"), catalog_root=tmp_path)

    assert handler is not None
    assert handler.name == "cog"


def test_pin_to_a_missing_handler_is_an_error(
    register_plugin: Callable[..., None], tmp_path: Path
) -> None:
    _pin(tmp_path, "nope")

    with pytest.raises(UnknownFormatHandlerError, match="'nope'"):
        handler_for(_touch(tmp_path, "a.tif"), catalog_root=tmp_path)


def test_non_handler_result_is_dropped_with_a_warning(
    register_plugin: Callable[..., None],
) -> None:
    register_plugin(_Plugin(object()), "odd-plugin")

    with pytest.warns(PluginWarning, match="'odd-plugin'.*not a FormatHandler"):
        handlers = format_handlers()

    assert handlers
    assert all(isinstance(h, FormatHandler) for _, h in handlers)
