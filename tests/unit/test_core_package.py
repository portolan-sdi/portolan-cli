"""The portolan-core distribution imports without the CLI or the geospatial stack."""

from __future__ import annotations

import json
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[2]
FORBIDDEN = ["click", "rasterio", "pyarrow", "geopandas", "duckdb", "pystac", "portolan_cli"]

_PROBE = f"""
import importlib, json, pkgutil, sys
import portolan_core
modules = [m.name for m in pkgutil.walk_packages(portolan_core.__path__, "portolan_core.")]
for name in modules:
    importlib.import_module(name)
print(json.dumps({{
    "file": portolan_core.__file__,
    "modules": modules,
    "loaded": sorted(m for m in {FORBIDDEN!r} if m in sys.modules),
}}))
"""


def test_core_import_loads_no_cli_or_geospatial_module() -> None:
    """Every portolan_core module imports without loading a forbidden module."""
    result = subprocess.run(
        [sys.executable, "-c", _PROBE], capture_output=True, text=True, check=True
    )
    probe = json.loads(result.stdout)

    expected = REPO_ROOT / "packages" / "portolan-core" / "src" / "portolan_core" / "__init__.py"
    assert Path(probe["file"]).resolve() == expected
    assert "portolan_core.versions" in probe["modules"]
    assert probe["loaded"] == []


def test_the_versions_agree() -> None:
    """portolan-cli pins portolan-core to its own version, so the three fields move together.

    The release workflow runs this test before it pushes the tag.
    """
    root = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    core = tomllib.loads(
        (REPO_ROOT / "packages" / "portolan-core" / "pyproject.toml").read_text(encoding="utf-8")
    )
    pins = [
        dep.removeprefix("portolan-core==")
        for dep in root["project"]["dependencies"]
        if dep.startswith("portolan-core")
    ]

    assert [core["project"]["version"], *pins] == [root["project"]["version"]] * 2
