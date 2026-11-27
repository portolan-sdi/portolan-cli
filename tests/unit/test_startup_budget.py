"""Startup budget: plugins cost nothing until an operation uses them."""

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
print("MANAGER_BUILT", manager.get_plugin_manager.cache_info().currsize > 0)
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
    heavy = ["click", "rasterio", "pyarrow", "pyiceberg", "pystac", "geopandas"]
    code = (
        f"import sys, portolan_cli.api\nprint(sorted(m for m in {heavy!r} if m in sys.modules))\n"
    )

    assert _run(code).strip() == "[]"


def test_metadata_package_import_loads_no_heavy_dependency() -> None:
    code = (
        "import sys, portolan_cli.metadata.models\n"
        f"print(sorted(m for m in {list(HEAVY_MODULES)!r} if m in sys.modules))\n"
    )

    assert _run(code).strip() == "[]"


def test_validation_import_loads_no_heavy_dependency() -> None:
    code = (
        "import sys, portolan_cli.validation\n"
        f"print(sorted(m for m in {list(HEAVY_MODULES)!r} if m in sys.modules))\n"
    )

    assert _run(code).strip() == "[]"


def test_metadata_package_resolves_every_public_name() -> None:
    """Each name in `portolan_cli.metadata.__all__` resolves to its definition."""
    from portolan_cli import metadata

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
    output = _run(_EXTRACT_HELP_PROBE)

    assert "OGC Web Feature Service layers." in output
    assert "SOURCE_MODULES []" in output
    assert "HTTP_CLIENTS []" in output
