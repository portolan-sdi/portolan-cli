"""S7 constants: the heavy modules that each help command loads.
``cli.py`` imports each stack inside the command that uses it, so the set is empty.
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
