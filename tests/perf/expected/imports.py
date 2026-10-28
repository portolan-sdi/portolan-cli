"""S7 constants: the heavy modules that each help command loads (#994).

Lane F owns this file. ``cli.py`` imports the add, fix, and check stacks
inside the commands that use them, and the metadata package loads each
export on first access. A help command loads none of the heavy modules.
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
    ("--help",): [],
    ("init", "--help"): [],
    ("check", "--help"): [],
}
