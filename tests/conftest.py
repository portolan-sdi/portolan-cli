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


# =============================================================================
# Catalog State Fixtures (for push/pull/sync tests)
# =============================================================================


@pytest.fixture
def catalog_with_versions_for_dry_run(tmp_path: Path) -> Path:
    """Catalog with ADR-0023-compliant structure for dry-run tests.

    This fixture is used by TestDryRunNetworkIsolation classes across
    test_pull.py, test_push.py, and test_sync.py.

    Structure:
    - <catalog_root>/catalog.json
    - <catalog_root>/.portolan/config.yaml
    (Note: state.json removed per issue #290 - config.yaml alone is sufficient)
    - <catalog_root>/<collection>/versions.json
    - <catalog_root>/<collection>/data.parquet
    """
    import json

    catalog_dir = tmp_path / "catalog_dry_run"
    catalog_dir.mkdir()

    # Create catalog.json at root (per ADR-0023)
    catalog_data = {
        "type": "Catalog",
        "id": "test-catalog",
        "stac_version": "1.0.0",
        "description": "Test catalog for dry-run tests",
        "links": [],
    }
    (catalog_dir / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    # .portolan sentinel: config.yaml alone is sufficient for MANAGED state (per issue #290)
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("{}\n")

    # Per ADR-0023: versions.json at <catalog_root>/<collection>/versions.json
    collection_dir = catalog_dir / "test-collection"
    collection_dir.mkdir()
    versions_data = {
        "spec_version": "1.0.0",
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-15T10:00:00Z",
                "breaking": False,
                "message": "Initial version",
                "assets": {
                    "data.parquet": {
                        "sha256": "abc123",
                        "size_bytes": 1000,
                        "href": "test-collection/data.parquet",
                    }
                },
                "changes": ["data.parquet"],
            }
        ],
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))
    (collection_dir / "data.parquet").write_bytes(b"x" * 1000)

    return catalog_dir


@pytest.fixture
def fresh_catalog_no_versions(tmp_path: Path) -> Path:
    """Catalog WITHOUT versions.json (fresh state for initial pull/push).

    This tests the edge case where dry-run is called on a catalog that
    has never been versioned before.
    """
    import json

    catalog_dir = tmp_path / "catalog_fresh"
    catalog_dir.mkdir()

    # Create catalog.json at root (per ADR-0023)
    catalog_data = {
        "type": "Catalog",
        "id": "test-catalog",
        "stac_version": "1.0.0",
        "description": "Fresh catalog without versions",
        "links": [],
    }
    (catalog_dir / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    # .portolan sentinel: config.yaml alone is sufficient (per issue #290)
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("{}\n")

    # Collection directory exists but NO versions.json
    collection_dir = catalog_dir / "test-collection"
    collection_dir.mkdir()

    return catalog_dir


@pytest.fixture
def catalog_with_multiple_versions(tmp_path: Path) -> Path:
    """Catalog with 3 versions for divergence and accumulation testing.

    Version history:
    - v1.0.0: base.parquet (initial)
    - v1.1.0: base.parquet + second.parquet (add)
    - v1.2.0: base.parquet + second.parquet + third.parquet (add)

    Useful for testing snapshot model accumulation per ADR-0005.
    """
    import json

    catalog_dir = tmp_path / "catalog_multi_version"
    catalog_dir.mkdir()

    # .portolan sentinel
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("catalog_id: test-catalog\n")

    # catalog.json
    catalog_data = {
        "type": "Catalog",
        "id": "test-catalog",
        "stac_version": "1.0.0",
        "description": "Catalog with multiple versions",
        "links": [],
    }
    (catalog_dir / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    # Collection with 3 versions
    collection_dir = catalog_dir / "test-collection"
    collection_dir.mkdir()

    base_hash = "base123def456789012345678901234567890123456789012345678901234base"
    second_hash = "sec0nd3def456789012345678901234567890123456789012345678901234sec"
    third_hash = "third3def456789012345678901234567890123456789012345678901234thi"

    versions_data = {
        "spec_version": "1.0.0",
        "current_version": "1.2.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2026-01-15T10:00:00Z",
                "breaking": False,
                "assets": {
                    "base.parquet": {
                        "sha256": base_hash,
                        "size_bytes": 1000,
                        "href": "base.parquet",
                    }
                },
                "changes": ["base.parquet"],
            },
            {
                "version": "1.1.0",
                "created": "2026-01-16T10:00:00Z",
                "breaking": False,
                "assets": {
                    "base.parquet": {
                        "sha256": base_hash,
                        "size_bytes": 1000,
                        "href": "base.parquet",
                    },
                    "second.parquet": {
                        "sha256": second_hash,
                        "size_bytes": 2000,
                        "href": "second.parquet",
                    },
                },
                "changes": ["second.parquet"],
            },
            {
                "version": "1.2.0",
                "created": "2026-01-17T10:00:00Z",
                "breaking": False,
                "assets": {
                    "base.parquet": {
                        "sha256": base_hash,
                        "size_bytes": 1000,
                        "href": "base.parquet",
                    },
                    "second.parquet": {
                        "sha256": second_hash,
                        "size_bytes": 2000,
                        "href": "second.parquet",
                    },
                    "third.parquet": {
                        "sha256": third_hash,
                        "size_bytes": 3000,
                        "href": "third.parquet",
                    },
                },
                "changes": ["third.parquet"],
            },
        ],
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))

    # Create actual asset files
    (collection_dir / "base.parquet").write_bytes(b"x" * 1000)
    (collection_dir / "second.parquet").write_bytes(b"y" * 2000)
    (collection_dir / "third.parquet").write_bytes(b"z" * 3000)

    return catalog_dir


@pytest.fixture
def catalog_with_100_assets(tmp_path: Path) -> Path:
    """Catalog with 100 assets for scale testing.

    Tests issue #339 scenario where many files are added.
    """
    import hashlib
    import json

    catalog_dir = tmp_path / "catalog_100_assets"
    catalog_dir.mkdir()

    # .portolan sentinel
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("catalog_id: test-catalog\n")

    # catalog.json
    catalog_data = {
        "type": "Catalog",
        "id": "test-catalog",
        "stac_version": "1.0.0",
        "description": "Catalog with 100 assets",
        "links": [],
    }
    (catalog_dir / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    # Collection with 100 assets
    collection_dir = catalog_dir / "test-collection"
    collection_dir.mkdir()

    assets = {}
    changes = []
    for i in range(100):
        name = f"asset_{i:03d}.parquet"
        sha = hashlib.sha256(name.encode()).hexdigest()
        size = 1024 * (i + 1)
        assets[name] = {
            "sha256": sha,
            "size_bytes": size,
            "href": name,
        }
        changes.append(name)
        # Create actual file
        (collection_dir / name).write_bytes(b"x" * size)

    versions_data = {
        "spec_version": "1.0.0",
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2026-01-15T10:00:00Z",
                "breaking": False,
                "assets": assets,
                "changes": changes,
            }
        ],
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))

    return catalog_dir
