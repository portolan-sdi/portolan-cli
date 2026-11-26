"""The Portolan library without the command line or the geospatial stack (#1133).

portolan-core holds the catalog logic that a library caller needs without the
CLI shell. The ``portolan-cli`` distribution depends on it and adds Click and
the format conversion stack.

This package must not import ``click``, the geospatial stack (rasterio,
pyarrow, geopandas, duckdb, pystac, pyproj, shapely, numpy), or
``portolan_cli``. The import-linter contracts ``core-no-cli`` and
``core-no-heavy`` in the root ``pyproject.toml`` enforce this.
"""
