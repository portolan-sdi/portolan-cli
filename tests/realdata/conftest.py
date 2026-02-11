"""Fixtures for real-world data tests.

These tests verify Portolan's orchestration layer with production data.
They do NOT test geometry validity or format conversion (upstream's job).
"""

from pathlib import Path

import pytest

# Path to real-world fixtures
REALDATA_FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "realdata"


@pytest.fixture
def realdata_path() -> Path:
    """Return path to realdata fixtures directory."""
    return REALDATA_FIXTURES_DIR


@pytest.fixture
def nwi_wetlands_path() -> Path:
    """NWI Wetlands - complex polygons with holes (1,000 features)."""
    return REALDATA_FIXTURES_DIR / "nwi-wetlands.parquet"


@pytest.fixture
def open_buildings_path() -> Path:
    """Open Buildings - bulk polygon ingestion (1,000 features)."""
    return REALDATA_FIXTURES_DIR / "open-buildings.parquet"


@pytest.fixture
def road_detections_path() -> Path:
    """Road Detections - LineString geometries (1,000 features)."""
    return REALDATA_FIXTURES_DIR / "road-detections.parquet"


@pytest.fixture
def fieldmaps_boundaries_path() -> Path:
    """FieldMaps Boundaries - antimeridian crossing (3 features)."""
    return REALDATA_FIXTURES_DIR / "fieldmaps-boundaries.parquet"


@pytest.fixture
def rapidai4eo_path() -> Path:
    """RapidAI4EO - Cloud-Optimized GeoTIFF raster."""
    return REALDATA_FIXTURES_DIR / "rapidai4eo-sample.tif"
