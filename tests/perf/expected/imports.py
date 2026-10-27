"""S7 constants: the heavy modules that each help command loads (#994).

Lane F owns this file. ``cli.py`` imports ``portolan_cli.add`` and
``portolan_cli.metadata.fix`` at the top, and they load the read stack.
duckdb, matplotlib, contextily, and geoparquet_io load later already.
"""

from __future__ import annotations

#: The modules that a help command should not need, in the order the test reports.
HEAVY_MODULES: tuple[str, ...] = (
    "rasterio",
    "pyogrio",
    "geopandas",
    "pandas",
    "pyarrow.compute",
    "duckdb",
    "matplotlib",
    "contextily",
    "boto3",
    "geoparquet_io",
)

EXPECTED: dict[tuple[str, ...], list[str]] = {
    # cli.py:28 add_files and cli.py:49 metadata.fix load these.
    ("--help",): ["rasterio", "pyogrio", "geopandas", "pandas", "pyarrow.compute", "boto3"],
    # cli.py:28 add_files and cli.py:49 metadata.fix load these.
    ("init", "--help"): ["rasterio", "pyogrio", "geopandas", "pandas", "pyarrow.compute", "boto3"],
    # cli.py:28 add_files and cli.py:49 metadata.fix load these.
    ("check", "--help"): ["rasterio", "pyogrio", "geopandas", "pandas", "pyarrow.compute", "boto3"],
}
