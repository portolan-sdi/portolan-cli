"""Shared pytest fixtures for Portolan CLI tests."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator


# =============================================================================
# Fixture Directory Access
# =============================================================================


@pytest.fixture
def fixtures_dir() -> Path:
    """Return the path to the test fixtures directory."""
    return Path(__file__).parent / "fixtures"


# =============================================================================
# Vector Fixtures (GeoJSON, GeoParquet)
# =============================================================================


@pytest.fixture
def valid_points_geojson(fixtures_dir: Path) -> Path:
    """Path to valid points GeoJSON fixture (10 point features)."""
    return fixtures_dir / "vector" / "valid" / "points.geojson"


@pytest.fixture
def valid_polygons_geojson(fixtures_dir: Path) -> Path:
    """Path to valid polygons GeoJSON fixture (5 polygon features)."""
    return fixtures_dir / "vector" / "valid" / "polygons.geojson"


@pytest.fixture
def valid_lines_geojson(fixtures_dir: Path) -> Path:
    """Path to valid lines GeoJSON fixture (5 linestring features)."""
    return fixtures_dir / "vector" / "valid" / "lines.geojson"


@pytest.fixture
def valid_multigeom_geojson(fixtures_dir: Path) -> Path:
    """Path to valid mixed geometry GeoJSON fixture."""
    return fixtures_dir / "vector" / "valid" / "multigeom.geojson"


@pytest.fixture
def valid_large_properties_geojson(fixtures_dir: Path) -> Path:
    """Path to valid GeoJSON with 20+ property columns."""
    return fixtures_dir / "vector" / "valid" / "large_properties.geojson"


@pytest.fixture
def valid_points_parquet(fixtures_dir: Path) -> Path:
    """Path to valid GeoParquet fixture (real-world Open Buildings data).

    This uses real-world data from Open Buildings (1000 building footprints).
    Previously pointed to synthetic fixture at vector/valid/points.parquet.
    """
    return fixtures_dir / "realdata" / "open-buildings.parquet"


@pytest.fixture
def projected_parquet(fixtures_dir: Path) -> Path:
    """Path to GeoParquet with projected CRS (EPSG:32631 UTM Zone 31N)."""
    return fixtures_dir / "vector" / "open-buildings-utm31n.parquet"


# Invalid vector fixtures


@pytest.fixture
def invalid_no_geometry_json(fixtures_dir: Path) -> Path:
    """Path to JSON file with no geometry field."""
    return fixtures_dir / "vector" / "invalid" / "no_geometry.json"


@pytest.fixture
def invalid_malformed_geojson(fixtures_dir: Path) -> Path:
    """Path to malformed (truncated) GeoJSON file."""
    return fixtures_dir / "vector" / "invalid" / "malformed.geojson"


@pytest.fixture
def invalid_empty_geojson(fixtures_dir: Path) -> Path:
    """Path to empty FeatureCollection GeoJSON file."""
    return fixtures_dir / "vector" / "invalid" / "empty.geojson"


@pytest.fixture
def invalid_null_geometries_geojson(fixtures_dir: Path) -> Path:
    """Path to GeoJSON with null geometry features."""
    return fixtures_dir / "vector" / "invalid" / "null_geometries.geojson"


# =============================================================================
# Raster Fixtures (COG)
# =============================================================================


@pytest.fixture
def valid_rgb_cog(fixtures_dir: Path) -> Path:
    """Path to valid COG fixture (real-world RapidAI4EO satellite imagery).

    This uses real-world data from ESA's RapidAI4EO program (100x100 Sentinel-2 sample).
    Previously pointed to synthetic fixture at raster/valid/rgb.tif.
    """
    return fixtures_dir / "realdata" / "rapidai4eo-sample.tif"


@pytest.fixture
def valid_singleband_cog(fixtures_dir: Path) -> Path:
    """Path to valid single-band COG fixture (64x64)."""
    return fixtures_dir / "raster" / "valid" / "singleband.tif"


@pytest.fixture
def valid_float32_cog(fixtures_dir: Path) -> Path:
    """Path to valid float32 COG fixture (elevation-like data)."""
    return fixtures_dir / "raster" / "valid" / "float32.tif"


@pytest.fixture
def valid_nodata_cog(fixtures_dir: Path) -> Path:
    """Path to valid COG with nodata value set."""
    return fixtures_dir / "raster" / "valid" / "nodata.tif"


# Invalid raster fixtures


@pytest.fixture
def invalid_not_georeferenced_tif(fixtures_dir: Path) -> Path:
    """Path to TIFF without CRS or geotransform."""
    return fixtures_dir / "raster" / "invalid" / "not_georeferenced.tif"


@pytest.fixture
def invalid_truncated_tif(fixtures_dir: Path) -> Path:
    """Path to truncated (corrupted) TIFF file."""
    return fixtures_dir / "raster" / "invalid" / "truncated.tif"


@pytest.fixture
def non_cog_tif(fixtures_dir: Path) -> Path:
    """Path to non-cloud-optimized GeoTIFF (Natural Earth data)."""
    return fixtures_dir / "raster" / "natural-earth-non-cog.tif"


# =============================================================================
# Edge Case Fixtures
# =============================================================================


@pytest.fixture
def edge_unicode_geojson(fixtures_dir: Path) -> Path:
    """Path to GeoJSON with Unicode property values."""
    return fixtures_dir / "edge" / "unicode_properties.geojson"


@pytest.fixture
def edge_special_filename_geojson(fixtures_dir: Path) -> Path:
    """Path to GeoJSON with spaces in filename."""
    return fixtures_dir / "edge" / "special_filename spaces.geojson"


@pytest.fixture
def edge_antimeridian_geojson(fixtures_dir: Path) -> Path:
    """Path to GeoJSON crossing the antimeridian."""
    return fixtures_dir / "edge" / "antimeridian.geojson"


# =============================================================================
# Real-World Data Fixtures
# =============================================================================


@pytest.fixture
def nwi_wetlands_path(fixtures_dir: Path) -> Path:
    """NWI Wetlands - complex polygons with holes (1,000 features)."""
    return fixtures_dir / "realdata" / "nwi-wetlands.parquet"


@pytest.fixture
def open_buildings_path(fixtures_dir: Path) -> Path:
    """Open Buildings - bulk polygon ingestion (1,000 features)."""
    return fixtures_dir / "realdata" / "open-buildings.parquet"


@pytest.fixture
def road_detections_path(fixtures_dir: Path) -> Path:
    """Road Detections - LineString geometries (1,000 features)."""
    return fixtures_dir / "realdata" / "road-detections.parquet"


@pytest.fixture
def fieldmaps_boundaries_path(fixtures_dir: Path) -> Path:
    """FieldMaps Boundaries - antimeridian crossing (3 features)."""
    return fixtures_dir / "realdata" / "fieldmaps-boundaries.parquet"


@pytest.fixture
def rapidai4eo_path(fixtures_dir: Path) -> Path:
    """RapidAI4EO - Cloud-Optimized GeoTIFF raster."""
    return fixtures_dir / "realdata" / "rapidai4eo-sample.tif"


# =============================================================================
# Temporary Catalog Fixtures
# =============================================================================


@pytest.fixture
def temp_catalog_dir(tmp_path: Path) -> Iterator[Path]:
    """Create a temporary directory for catalog operations.

    Yields the path to a clean temporary directory that will be
    automatically cleaned up after the test.
    """
    catalog_dir = tmp_path / "test-catalog"
    catalog_dir.mkdir()
    yield catalog_dir
