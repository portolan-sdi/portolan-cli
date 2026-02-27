"""Shared fixtures for documentation example tests.

These fixtures provide the infrastructure needed to test README
and documentation examples in isolation. They ensure that documented
workflows actually work.

Philosophy: If it's in the docs, it must be tested. Examples that
cannot be tested (e.g., installation commands) are marked with
`# notest` comments in the source documentation.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

if TYPE_CHECKING:
    from collections.abc import Iterator


# =============================================================================
# Minimal Test Data
# =============================================================================

# Minimal valid GeoJSON for testing - 2 point features
MINIMAL_GEOJSON = """{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {"type": "Point", "coordinates": [-73.9857, 40.7484]},
      "properties": {"name": "Empire State Building", "id": 1}
    },
    {
      "type": "Feature",
      "geometry": {"type": "Point", "coordinates": [-74.0445, 40.6892]},
      "properties": {"name": "Statue of Liberty", "id": 2}
    }
  ]
}"""


# =============================================================================
# CLI Runner Fixtures
# =============================================================================


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner with isolated filesystem support."""
    return CliRunner()


# =============================================================================
# Catalog Fixtures
# =============================================================================


@pytest.fixture
def empty_catalog(tmp_path: Path, runner: CliRunner) -> Iterator[Path]:
    """An initialized but empty Portolan catalog.

    Yields a temporary directory with `portolan init --auto` already run.
    Useful for testing workflows that start from a clean catalog.
    """
    result = runner.invoke(cli, ["init", str(tmp_path), "--auto"])
    assert result.exit_code == 0, f"Init failed: {result.output}"
    yield tmp_path


@pytest.fixture
def catalog_with_geojson(empty_catalog: Path, valid_points_geojson: Path) -> Iterator[Path]:
    """A catalog with a demographics collection containing GeoJSON.

    Sets up the standard demo structure used in README examples:
    - demographics/
      - sample.geojson (copied from test fixtures)
    """
    demographics_dir = empty_catalog / "demographics"
    demographics_dir.mkdir()
    shutil.copy(valid_points_geojson, demographics_dir / "sample.geojson")
    yield empty_catalog


@pytest.fixture
def catalog_with_minimal_data(empty_catalog: Path) -> Iterator[Path]:
    """A catalog with minimal inline GeoJSON data.

    Does not depend on external fixtures - creates data inline.
    Useful for self-contained documentation tests.
    """
    demographics_dir = empty_catalog / "demographics"
    demographics_dir.mkdir()
    (demographics_dir / "sample.geojson").write_text(MINIMAL_GEOJSON)
    yield empty_catalog


# =============================================================================
# Real Fixture Access (Delegated to Main conftest.py)
# =============================================================================

# The following fixtures are inherited from tests/conftest.py:
# - valid_points_geojson
# - valid_polygons_geojson
# - fixtures_dir
#
# No need to redefine them here - pytest's fixture discovery handles it.
