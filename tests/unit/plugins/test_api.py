"""Tests for the public plugin API (#947).

`portolan_cli.api`, `portolan_cli.hookspecs`, and `portolan_cli.testing` follow
semantic versioning. `public_api.txt` records the three surfaces, so a change
shows up in review.
"""

from __future__ import annotations

import importlib
import inspect
import subprocess
import sys
from pathlib import Path

import pytest

from portolan_cli import api, hookspecs, testing

_GOLDEN = Path(__file__).with_name("public_api.txt")


def _surface() -> str:
    lines = ["# portolan_cli.api"]
    lines += sorted(api.__all__)
    lines.append("")
    lines.append("# portolan_cli.hookspecs")
    for name in sorted(dir(hookspecs)):
        value = getattr(hookspecs, name)
        if name.startswith("portolan_") and callable(value):
            lines.append(f"{name}{inspect.signature(value)}")
    lines.append("")
    lines.append("# portolan_cli.testing")
    for name in sorted(testing.__all__):
        value = getattr(testing, name)
        lines.append(f"{name}{inspect.signature(value)}" if inspect.isfunction(value) else name)
    return "\n".join(lines) + "\n"


@pytest.mark.snapshot
def test_public_surface_matches_the_record() -> None:
    """A change to the API or a hookspec must update public_api.txt.

    Removing or changing a name breaks plugins and needs a major release.
    """
    assert _surface() == _GOLDEN.read_text(encoding="utf-8"), (
        "The public plugin surface changed. If the change is intended, write "
        "the new surface to tests/unit/plugins/public_api.txt."
    )


@pytest.mark.unit
@pytest.mark.parametrize("name", sorted(api.__all__))
def test_every_export_resolves_to_its_source(name: str) -> None:
    """Each name loads, and it is the object its source module defines."""
    value = getattr(api, name)

    source = importlib.import_module(api._EXPORTS[name])
    assert value is getattr(source, name)


@pytest.mark.unit
def test_unknown_name_raises_attribute_error() -> None:
    with pytest.raises(AttributeError, match="has no attribute 'nope'"):
        api.nope  # noqa: B018


@pytest.mark.unit
def test_importing_the_api_loads_no_heavy_dependency() -> None:
    """`import portolan_cli.api` stays light for a plugin's module-level import."""
    heavy = ["click", "rasterio", "pyarrow", "pyiceberg", "pystac", "geopandas"]
    code = (
        f"import sys, portolan_cli.api\nprint(sorted(m for m in {heavy!r} if m in sys.modules))\n"
    )

    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )

    assert result.stdout.strip() == "[]"
