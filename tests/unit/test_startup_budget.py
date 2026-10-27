"""Startup budget: plugins cost nothing until an operation uses them (#947).

Each check runs in a fresh interpreter, because the test session has already
imported most of the package.
"""

from __future__ import annotations

import os
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest

from tests.perf.expected.imports import HEAVY_MODULES
from tests.support.fake_dist import write_fake_dist

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.unit

_HOOKS = """
from portolan_cli.hookspecs import hookimpl

@hookimpl
def portolan_enrich_item(item, context):
    item["properties"]["fake"] = True
"""

_COMMAND = '''
import click

@click.command()
def command():
    """Fake plugin command."""
'''

_HELP_PROBE = """
import importlib, sys
cli = importlib.import_module("portolan_cli.cli").cli
try:
    cli(["--help"], prog_name="portolan")
except SystemExit:
    pass
from portolan_cli.plugins import manager
print("PLUGIN_MODULES", sorted(m for m in sys.modules if m.startswith("fake_plugin")))
print("MANAGER_BUILT", manager._manager is not None)
"""


def _run(code: str, site: Path | None = None) -> str:
    env = dict(os.environ)
    env.pop("PORTOLAN_PLUGINS", None)
    if site is not None:
        env["PYTHONPATH"] = os.pathsep.join([str(site), env.get("PYTHONPATH", "")])
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True, env=env
    )
    return result.stdout


def test_help_imports_no_plugin_module(tmp_path: Path) -> None:
    """`portolan --help` lists an installed plugin command and imports none of it."""
    write_fake_dist(
        tmp_path,
        "fake-plugin",
        {
            "portolan": {"fake": "fake_plugin_hooks"},
            "portolan.commands": {"fake": "fake_plugin_cli:command"},
        },
        {"fake_plugin_hooks": _HOOKS, "fake_plugin_cli": _COMMAND},
    )

    output = _run(_HELP_PROBE, site=tmp_path)

    assert "(plugin: fake-plugin)" in output
    assert "PLUGIN_MODULES []" in output
    assert "MANAGER_BUILT False" in output


def test_api_import_loads_no_heavy_dependency() -> None:
    """`import portolan_cli.api` loads no Click and no geospatial library.

    A plugin imports the API at module level, so its cost lands on every
    hook call that loads the plugin.
    """
    heavy = ["click", "rasterio", "pyarrow", "pyiceberg", "pystac", "geopandas"]
    code = (
        f"import sys, portolan_cli.api\nprint(sorted(m for m in {heavy!r} if m in sys.modules))\n"
    )

    assert _run(code).strip() == "[]"


def test_metadata_package_import_loads_no_heavy_dependency() -> None:
    """`import portolan_cli.metadata` loads no format extractor (#994).

    Python runs the package `__init__` before any submodule, so an eager
    re-export there loads rasterio and pyogrio for `metadata.models` too.
    `tests/perf/test_cold_imports.py` checks the help commands themselves.
    """
    code = (
        "import sys, portolan_cli.metadata.models\n"
        f"print(sorted(m for m in {list(HEAVY_MODULES)!r} if m in sys.modules))\n"
    )

    assert _run(code).strip() == "[]"


def test_validation_import_loads_no_heavy_dependency() -> None:
    """`import portolan_cli.validation` loads no format extractor (#994).

    `portolan check` imports the package before it reads a file. Its chain
    runs through `validation.fixers`, `metadata.fix`, and `metadata.update`.
    A module-level rasterio import on that chain loads rasterio and boto3
    for every check, also when the catalog holds no raster.
    """
    code = (
        "import sys, portolan_cli.validation\n"
        f"print(sorted(m for m in {list(HEAVY_MODULES)!r} if m in sys.modules))\n"
    )

    assert _run(code).strip() == "[]"


def test_metadata_package_resolves_every_public_name() -> None:
    """Each name in `portolan_cli.metadata.__all__` resolves to its definition."""
    import portolan_cli.metadata as metadata

    for name in metadata.__all__:
        value = getattr(metadata, name)
        assert value.__module__.startswith("portolan_cli.metadata."), name
        assert value.__name__ == name
    assert set(metadata.__all__) <= set(dir(metadata))


_EXTRACT_HELP_PROBE = """
import importlib, sys
cli = importlib.import_module("portolan_cli.cli").cli
try:
    cli(["extract", "--help"], prog_name="portolan")
except SystemExit:
    pass
sources = ("arcgis", "carto", "wfs")
print("SOURCE_MODULES", sorted(
    m for m in sys.modules
    if m.startswith("portolan_cli.extract.") and m.split(".")[2] in sources
))
print("HTTP_CLIENTS", sorted(m for m in ("httpx", "requests") if m in sys.modules))
"""


def test_extract_help_imports_no_extractor_command() -> None:
    """`portolan extract --help` shows each summary and loads no source package.

    An extractor imports its command and its HTTP client inside
    `cli_command()`, so the help must not call it.
    """
    output = _run(_EXTRACT_HELP_PROBE)

    assert "OGC Web Feature Service layers." in output
    assert "SOURCE_MODULES []" in output
    assert "HTTP_CLIENTS []" in output
