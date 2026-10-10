"""The library imports without the CLI shell (issue #944).

The subprocess checks run in a fresh interpreter. The test process has already imported
click and the geospatial stack, so ``sys.modules`` here proves nothing.
"""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

# Modules a library caller must not pay for on import.
_HEAVY = ("click", "rasterio", "geopandas", "pyarrow", "portolan_cli.cli", "portolan_cli.add")


def _loaded_after(statement: str) -> list[str]:
    """Run ``statement`` in a fresh interpreter and return the heavy modules it loaded."""
    script = (
        "import json, sys\n"
        f"{statement}\n"
        f"print(json.dumps([m for m in {_HEAVY!r} if m in sys.modules]))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=True,
    )
    return list(json.loads(result.stdout.strip().splitlines()[-1]))


@pytest.mark.integration
@pytest.mark.parametrize(
    "statement",
    [
        "import portolan_cli",
        "import portolan_cli.stac",
        "import portolan_cli.status",
        "import portolan_cli.query",
        "import portolan_cli.catalog_list",
        "import portolan_cli.models",
        "import portolan_cli.stac_links",
        "import portolan_cli.inspect",
        "from portolan_cli import FormatType, detect_format",
    ],
)
def test_import_loads_no_heavy_module(statement: str) -> None:
    """A library import loads neither Click, the geospatial stack, nor cli.py."""
    assert _loaded_after(statement) == []


@pytest.mark.integration
def test_cli_export_is_the_group_after_the_submodule_loads() -> None:
    """Importing ``portolan_cli.cli`` first must not rebind the export to the module.

    The import system binds a loaded submodule to the package attribute of the
    same name. Without the guard, this order returns the module object.
    """
    script = "import portolan_cli.cli\nfrom portolan_cli import cli\nprint(type(cli).__name__)\n"
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "Group"


@pytest.mark.unit
def test_catalog_export_loads_on_access() -> None:
    """The top-level ``Catalog`` export resolves to the class in ``catalog.py``."""
    import portolan_cli
    from portolan_cli.catalog import Catalog

    assert portolan_cli.Catalog is Catalog
    assert "Catalog" in dir(portolan_cli)


@pytest.mark.unit
def test_unknown_attribute_raises() -> None:
    """A name outside the export list raises AttributeError, not ImportError."""
    import portolan_cli

    with pytest.raises(AttributeError, match="no_such_name"):
        getattr(portolan_cli, "no_such_name")  # noqa: B009 - the name is the test input
