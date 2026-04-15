"""Integration test fixtures for portolan CLI-to-Iceberg pipeline.

These fixtures create full portolan catalogs with the iceberg backend
configured, using SQLite + local filesystem (no Docker, no cloud).
"""

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner
from pyiceberg.catalog import load_catalog

from portolan_cli.cli import cli

_SKIP_WINDOWS = pytest.mark.skipif(
    sys.platform == "win32",
    reason="PyIceberg SQL catalog warehouse paths broken on Windows"
    " (https://github.com/apache/iceberg-python/issues/1005)",
)

pytestmark = [pytest.mark.integration, _SKIP_WINDOWS]

# Minimal valid GeoJSON FeatureCollection with point geometries.
# Used as the primary test input because portolan-cli's add pipeline
# expects geospatial files (GeoJSON, GeoParquet, Shapefile, etc.)
# and converts GeoJSON to GeoParquet automatically.
_POINTS_GEOJSON = json.dumps(
    {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [2.3522, 48.8566]},
                "properties": {"id": 1, "name": "Paris"},
            },
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-73.9857, 40.7484]},
                "properties": {"id": 2, "name": "NYC"},
            },
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [139.6917, 35.6895]},
                "properties": {"id": 3, "name": "Tokyo"},
            },
        ],
    }
)

_POINTS_GEOJSON_V2 = json.dumps(
    {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-0.1276, 51.5074]},
                "properties": {"id": 4, "name": "London"},
            },
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [12.4964, 41.9028]},
                "properties": {"id": 5, "name": "Rome"},
            },
        ],
    }
)


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


@pytest.fixture
def initialized_iceberg_catalog(tmp_path: Path, runner: CliRunner) -> Path:
    """Create a full portolan catalog initialized with iceberg backend.

    Patches _get_external_config to isolate from ~/.pyiceberg.yaml.
    Skips on Windows due to PyIceberg SQL catalog path bug.
    """
    if sys.platform == "win32":
        pytest.skip(
            "PyIceberg SQL catalog warehouse paths broken on Windows"
            " (https://github.com/apache/iceberg-python/issues/1005)"
        )
    with patch("portolan_cli.backends.iceberg.config._get_external_config", return_value=None):
        result = runner.invoke(
            cli,
            ["init", str(tmp_path), "--auto", "--backend", "iceberg"],
            catch_exceptions=False,
        )
    assert result.exit_code == 0, f"Init failed: {result.output}"
    assert (tmp_path / ".portolan" / "config.yaml").exists()
    return tmp_path


def load_test_catalog(catalog_root: Path):
    """Load the SQLite Iceberg catalog from a portolan catalog root.

    Uses catalog name "portolake" to match the name used by
    portolan_cli.backends.iceberg.config.create_catalog() (CATALOG_NAME = "portolake").
    """
    return load_catalog(
        "portolake",
        **{
            "type": "sql",
            "uri": f"sqlite:///{catalog_root}/.portolan/iceberg.db",
            "warehouse": (catalog_root / ".portolan" / "warehouse").as_uri(),
        },
    )


def place_geojson_in_collection(
    catalog_root: Path,
    collection: str,
    geojson_content: str | None = None,
    filename: str = "data.geojson",
) -> Path:
    """Write a GeoJSON file into the expected catalog directory structure.

    For vector data (GeoJSON), portolan-cli uses collection-level assets
    (ADR-0031): parent directory = collection. So:
      catalog_root/collection/data.geojson -> collection = "collection"

    Returns the file path.
    """
    if geojson_content is None:
        geojson_content = _POINTS_GEOJSON
    collection_dir = catalog_root / collection
    collection_dir.mkdir(parents=True, exist_ok=True)
    dest = collection_dir / filename
    dest.write_text(geojson_content)
    return dest


def invoke_add(runner: CliRunner, catalog_root: Path, file_path: Path) -> object:
    """Invoke `portolan add` with the iceberg backend, isolated from external config."""
    with patch("portolan_cli.backends.iceberg.config._get_external_config", return_value=None):
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_root), str(file_path)],
            catch_exceptions=False,
        )
    return result
