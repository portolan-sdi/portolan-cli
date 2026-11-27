"""Fixtures for the documentation example tests."""

from __future__ import annotations

import shutil
from typing import TYPE_CHECKING

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

if TYPE_CHECKING:
    from pathlib import Path


# Minimal test data

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


# CLI runner fixtures


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner with isolated filesystem support."""
    return CliRunner()


# Catalog fixtures


@pytest.fixture
def empty_catalog(tmp_path: Path, runner: CliRunner) -> Path:
    """Yield a temporary catalog after ``portolan init --auto``."""
    result = runner.invoke(cli, ["init", str(tmp_path), "--auto", "--license", "CC-BY-4.0"])
    assert result.exit_code == 0, f"Init failed: {result.output}"
    return tmp_path


@pytest.fixture
def catalog_with_geojson(empty_catalog: Path, valid_points_geojson: Path) -> Path:
    """Yield a catalog with ``demographics/sample.geojson``, as the README shows."""
    demographics_dir = empty_catalog / "demographics"
    demographics_dir.mkdir()
    shutil.copy(valid_points_geojson, demographics_dir / "sample.geojson")
    return empty_catalog


@pytest.fixture
def catalog_with_minimal_data(empty_catalog: Path) -> Path:
    """Yield a catalog with inline GeoJSON and no external fixture."""
    demographics_dir = empty_catalog / "demographics"
    demographics_dir.mkdir()
    (demographics_dir / "sample.geojson").write_text(MINIMAL_GEOJSON)
    return empty_catalog


# Real fixtures come from tests/conftest.py.
