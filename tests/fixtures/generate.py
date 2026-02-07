#!/usr/bin/env python3
"""Generate test fixtures for geospatial formats.

Run with: uv run python tests/fixtures/generate.py

All fixtures are deterministic (fixed coordinates, no random data).
Regenerate anytime to ensure consistency.
"""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path
from typing import Any

import numpy as np

# Lazy imports for optional dependencies
# These are imported inside functions to allow the script to be parsed
# even if dependencies aren't installed yet.

FIXTURES_DIR = Path(__file__).parent

# San Francisco Bay Area bounds (for all fixtures)
SF_BOUNDS = {
    "west": -122.5,
    "east": -122.35,
    "south": 37.7,
    "north": 37.85,
}


def generate_points_geojson() -> dict[str, Any]:
    """Generate 10 point features in San Francisco area."""
    # Deterministic coordinates in a grid pattern
    lons = np.linspace(SF_BOUNDS["west"], SF_BOUNDS["east"], 5)
    lats = np.linspace(SF_BOUNDS["south"], SF_BOUNDS["north"], 2)

    features = []
    names = [
        "Ferry Building",
        "Coit Tower",
        "Golden Gate Park",
        "Fisherman's Wharf",
        "Alcatraz View",
        "Mission District",
        "Castro",
        "SOMA",
        "Financial District",
        "Embarcadero",
    ]
    categories = ["landmark", "park", "neighborhood", "tourist", "business"]

    idx = 0
    for lat in lats:
        for lon in lons:
            if idx >= 10:
                break
            features.append(
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [float(lon), float(lat)]},
                    "properties": {
                        "id": idx + 1,
                        "name": names[idx],
                        "category": categories[idx % len(categories)],
                        "rating": round(3.5 + (idx % 3) * 0.5, 1),
                        "open": idx % 2 == 0,
                        "visitors": (idx + 1) * 1000 if idx % 3 != 0 else None,
                    },
                }
            )
            idx += 1

    return {
        "type": "FeatureCollection",
        "features": features,
    }


def generate_polygons_geojson() -> dict[str, Any]:
    """Generate 5 polygon features (neighborhood-like shapes)."""
    features = []
    neighborhoods = [
        ("Mission", -122.42, 37.76),
        ("Castro", -122.435, 37.762),
        ("SOMA", -122.4, 37.78),
        ("Marina", -122.44, 37.8),
        ("Nob Hill", -122.415, 37.79),
    ]

    for idx, (name, center_lon, center_lat) in enumerate(neighborhoods):
        # Create a simple rectangle around the center
        size = 0.01 + (idx * 0.002)  # Vary sizes slightly
        coords = [
            [center_lon - size, center_lat - size],
            [center_lon + size, center_lat - size],
            [center_lon + size, center_lat + size],
            [center_lon - size, center_lat + size],
            [center_lon - size, center_lat - size],  # Close the ring
        ]
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [coords]},
                "properties": {
                    "id": idx + 1,
                    "name": name,
                    "population": 10000 + idx * 5000,
                    "median_income": 50000 + idx * 10000,
                    "area_sq_km": round(size * size * 111 * 111, 2),  # Rough conversion
                },
            }
        )

    return {
        "type": "FeatureCollection",
        "features": features,
    }


def generate_lines_geojson() -> dict[str, Any]:
    """Generate 5 linestring features (street-like shapes)."""
    features = []
    streets = [
        ("Market Street", [(-122.42, 37.77), (-122.4, 37.79), (-122.39, 37.79)]),
        ("Mission Street", [(-122.42, 37.76), (-122.4, 37.78)]),
        ("Van Ness Ave", [(-122.42, 37.75), (-122.42, 37.82)]),
        ("Embarcadero", [(-122.39, 37.79), (-122.385, 37.8), (-122.4, 37.81)]),
        ("Geary Blvd", [(-122.48, 37.78), (-122.42, 37.785), (-122.4, 37.787)]),
    ]

    for idx, (name, coords) in enumerate(streets):
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[lon, lat] for lon, lat in coords],
                },
                "properties": {
                    "id": idx + 1,
                    "name": name,
                    "type": "major" if idx < 2 else "minor",
                    "lanes": 4 if idx < 2 else 2,
                    "one_way": idx == 3,
                },
            }
        )

    return {
        "type": "FeatureCollection",
        "features": features,
    }


def generate_multigeom_geojson() -> dict[str, Any]:
    """Generate features with mixed geometry types."""
    features = [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-122.4, 37.78]},
            "properties": {"id": 1, "type": "point"},
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "MultiPoint",
                "coordinates": [[-122.41, 37.78], [-122.42, 37.79]],
            },
            "properties": {"id": 2, "type": "multipoint"},
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [[-122.4, 37.77], [-122.41, 37.78]],
            },
            "properties": {"id": 3, "type": "linestring"},
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "MultiLineString",
                "coordinates": [
                    [[-122.42, 37.77], [-122.43, 37.78]],
                    [[-122.42, 37.78], [-122.43, 37.79]],
                ],
            },
            "properties": {"id": 4, "type": "multilinestring"},
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-122.45, 37.76],
                        [-122.44, 37.76],
                        [-122.44, 37.77],
                        [-122.45, 37.77],
                        [-122.45, 37.76],
                    ]
                ],
            },
            "properties": {"id": 5, "type": "polygon"},
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "MultiPolygon",
                "coordinates": [
                    [
                        [
                            [-122.46, 37.76],
                            [-122.455, 37.76],
                            [-122.455, 37.765],
                            [-122.46, 37.765],
                            [-122.46, 37.76],
                        ]
                    ],
                    [
                        [
                            [-122.46, 37.77],
                            [-122.455, 37.77],
                            [-122.455, 37.775],
                            [-122.46, 37.775],
                            [-122.46, 37.77],
                        ]
                    ],
                ],
            },
            "properties": {"id": 6, "type": "multipolygon"},
        },
    ]

    return {
        "type": "FeatureCollection",
        "features": features,
    }


def generate_large_properties_geojson() -> dict[str, Any]:
    """Generate features with 20+ property columns of diverse types."""
    features = []

    for idx in range(5):
        props = {
            "id": idx + 1,
            # Strings
            "name": f"Feature {idx + 1}",
            "description": f"This is a test feature number {idx + 1}",
            "category": ["A", "B", "C", "D", "E"][idx],
            "status": "active" if idx % 2 == 0 else "inactive",
            "code": f"CODE-{idx:04d}",
            # Integers
            "count": idx * 100,
            "rank": idx + 1,
            "year": 2020 + idx,
            "population": 10000 * (idx + 1),
            "elevation_m": 50 + idx * 10,
            # Floats
            "rating": round(3.0 + idx * 0.4, 2),
            "percentage": round(idx * 20.5, 2),
            "area_sq_km": round(1.5 + idx * 0.3, 3),
            "temperature_c": round(15.0 + idx * 2.1, 1),
            "price": round(99.99 + idx * 50.0, 2),
            # Booleans
            "active": idx % 2 == 0,
            "verified": idx % 3 == 0,
            "public": True,
            # Nulls (some fields)
            "optional_field": f"value_{idx}" if idx % 2 == 0 else None,
            "nullable_int": idx * 10 if idx % 3 != 0 else None,
        }
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [-122.4 + idx * 0.01, 37.78 + idx * 0.01],
                },
                "properties": props,
            }
        )

    return {
        "type": "FeatureCollection",
        "features": features,
    }


def generate_invalid_fixtures() -> None:
    """Generate invalid fixtures for error handling tests."""
    invalid_dir = FIXTURES_DIR / "vector" / "invalid"
    invalid_dir.mkdir(parents=True, exist_ok=True)

    # no_geometry.json - Valid JSON but no geometry
    no_geom = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"name": "Missing geometry"},
                # No geometry field at all
            }
        ],
    }
    (invalid_dir / "no_geometry.json").write_text(json.dumps(no_geom, indent=2))

    # malformed.geojson - Truncated JSON
    valid_start = '{"type": "FeatureCollection", "features": [{"type": "Feature", '
    (invalid_dir / "malformed.geojson").write_text(valid_start)

    # empty.geojson - Empty FeatureCollection
    empty = {"type": "FeatureCollection", "features": []}
    (invalid_dir / "empty.geojson").write_text(json.dumps(empty, indent=2))

    # null_geometries.geojson - Features with null geometry
    null_geom = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": None,
                "properties": {"name": "Null geometry feature"},
            },
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-122.4, 37.78]},
                "properties": {"name": "Valid geometry"},
            },
        ],
    }
    (invalid_dir / "null_geometries.geojson").write_text(json.dumps(null_geom, indent=2))


def generate_edge_cases() -> None:
    """Generate edge case fixtures."""
    edge_dir = FIXTURES_DIR / "edge"
    edge_dir.mkdir(parents=True, exist_ok=True)

    # unicode_properties.geojson - Non-ASCII in properties
    unicode_feat = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-122.4, 37.78]},
                "properties": {
                    "name_en": "San Francisco",
                    "name_zh": "旧金山",
                    "name_ja": "サンフランシスコ",
                    "name_ar": "سان فرانسيسكو",
                    "name_ru": "Сан-Франциско",
                    "emoji": "🌉",
                    "description": "A city with many names — très cosmopolite!",
                },
            }
        ],
    }
    (edge_dir / "unicode_properties.geojson").write_text(
        json.dumps(unicode_feat, indent=2, ensure_ascii=False)
    )

    # special_filename spaces.geojson - Spaces in filename
    special_feat = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-122.4, 37.78]},
                "properties": {"name": "Test for filename with spaces"},
            }
        ],
    }
    (edge_dir / "special_filename spaces.geojson").write_text(json.dumps(special_feat, indent=2))

    # antimeridian.geojson - Crosses date line
    # Per RFC 7946 §3.1.9, geometries crossing the antimeridian SHOULD be split
    # into a MultiPolygon with separate parts on each side of the date line.
    # This represents a narrow strip crossing the antimeridian (10° on each side).
    antimeridian_feat = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "MultiPolygon",
                    "coordinates": [
                        # Eastern polygon (170° to 180°)
                        [
                            [
                                [170.0, 50.0],
                                [180.0, 50.0],
                                [180.0, 60.0],
                                [170.0, 60.0],
                                [170.0, 50.0],
                            ]
                        ],
                        # Western polygon (-180° to -170°)
                        [
                            [
                                [-180.0, 50.0],
                                [-170.0, 50.0],
                                [-170.0, 60.0],
                                [-180.0, 60.0],
                                [-180.0, 50.0],
                            ]
                        ],
                    ],
                },
                "properties": {
                    "name": "Antimeridian crossing polygon",
                    "note": "Crosses the date line (+-180 longitude), split per RFC 7946 §3.1.9",
                },
            }
        ],
    }
    (edge_dir / "antimeridian.geojson").write_text(json.dumps(antimeridian_feat, indent=2))


def generate_vector_fixtures() -> None:
    """Generate all vector (GeoJSON) fixtures."""
    valid_dir = FIXTURES_DIR / "vector" / "valid"
    valid_dir.mkdir(parents=True, exist_ok=True)

    fixtures = {
        "points.geojson": generate_points_geojson(),
        "polygons.geojson": generate_polygons_geojson(),
        "lines.geojson": generate_lines_geojson(),
        "multigeom.geojson": generate_multigeom_geojson(),
        "large_properties.geojson": generate_large_properties_geojson(),
    }

    for filename, data in fixtures.items():
        filepath = valid_dir / filename
        filepath.write_text(json.dumps(data, indent=2))
        print(f"Generated {filepath}")


def convert_to_geoparquet() -> None:
    """Convert GeoJSON fixtures to GeoParquet using geoparquet-io."""
    import geoparquet_io as gpio  # type: ignore[import-untyped]

    valid_dir = FIXTURES_DIR / "vector" / "valid"
    source = valid_dir / "points.geojson"
    output = valid_dir / "points.parquet"

    # geoparquet-io uses fluent API
    gpio.convert(str(source)).write(str(output))
    print(f"Generated {output}")


def generate_raster_fixtures() -> None:
    """Generate raster (COG) fixtures using rasterio and rio-cogeo."""
    import rasterio  # type: ignore[import-untyped]
    from rasterio.transform import from_bounds  # type: ignore[import-untyped]
    from rio_cogeo.cogeo import cog_translate
    from rio_cogeo.profiles import cog_profiles

    valid_dir = FIXTURES_DIR / "raster" / "valid"
    invalid_dir = FIXTURES_DIR / "raster" / "invalid"
    valid_dir.mkdir(parents=True, exist_ok=True)
    invalid_dir.mkdir(parents=True, exist_ok=True)

    # Common parameters
    width, height = 64, 64
    transform = from_bounds(
        SF_BOUNDS["west"],
        SF_BOUNDS["south"],
        SF_BOUNDS["east"],
        SF_BOUNDS["north"],
        width,
        height,
    )
    crs = "EPSG:4326"
    profile = cog_profiles.get("deflate")  # type: ignore[no-untyped-call]

    # --- RGB COG (3-band uint8) ---
    # Deterministic pattern: red gradient left-to-right, green top-to-bottom
    rgb_data = np.zeros((3, height, width), dtype=np.uint8)
    for i in range(width):
        rgb_data[0, :, i] = int(255 * i / width)  # Red gradient
    for j in range(height):
        rgb_data[1, j, :] = int(255 * j / height)  # Green gradient
    rgb_data[2, :, :] = 128  # Constant blue

    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        with rasterio.open(
            tmp_path,
            "w",
            driver="GTiff",
            height=height,
            width=width,
            count=3,
            dtype="uint8",
            crs=crs,
            transform=transform,
        ) as dst:
            dst.write(rgb_data)
        cog_translate(tmp_path, str(valid_dir / "rgb.tif"), profile, quiet=True)
    finally:
        Path(tmp_path).unlink()
    print(f"Generated {valid_dir / 'rgb.tif'}")

    # --- Singleband COG (1-band uint8) ---
    # Deterministic: checkerboard pattern
    single_data = np.zeros((1, height, width), dtype=np.uint8)
    for i in range(height):
        for j in range(width):
            single_data[0, i, j] = 255 if (i // 8 + j // 8) % 2 == 0 else 0

    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        with rasterio.open(
            tmp_path,
            "w",
            driver="GTiff",
            height=height,
            width=width,
            count=1,
            dtype="uint8",
            crs=crs,
            transform=transform,
        ) as dst:
            dst.write(single_data)
        cog_translate(tmp_path, str(valid_dir / "singleband.tif"), profile, quiet=True)
    finally:
        Path(tmp_path).unlink()
    print(f"Generated {valid_dir / 'singleband.tif'}")

    # --- Float32 COG (elevation-like) ---
    # Deterministic: gradient representing "elevation"
    float_data = np.zeros((1, height, width), dtype=np.float32)
    for i in range(height):
        for j in range(width):
            float_data[0, i, j] = 100.0 + i * 2.0 + j * 1.5  # Gradient

    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        with rasterio.open(
            tmp_path,
            "w",
            driver="GTiff",
            height=height,
            width=width,
            count=1,
            dtype="float32",
            crs=crs,
            transform=transform,
        ) as dst:
            dst.write(float_data)
        cog_translate(tmp_path, str(valid_dir / "float32.tif"), profile, quiet=True)
    finally:
        Path(tmp_path).unlink()
    print(f"Generated {valid_dir / 'float32.tif'}")

    # --- NoData COG ---
    # Deterministic: circle with nodata outside
    nodata_value = 255
    nodata_data = np.full((1, height, width), nodata_value, dtype=np.uint8)
    center_x, center_y = width // 2, height // 2
    radius = min(width, height) // 3
    for i in range(height):
        for j in range(width):
            if (i - center_y) ** 2 + (j - center_x) ** 2 <= radius**2:
                nodata_data[0, i, j] = 128  # Value inside circle

    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        with rasterio.open(
            tmp_path,
            "w",
            driver="GTiff",
            height=height,
            width=width,
            count=1,
            dtype="uint8",
            crs=crs,
            transform=transform,
            nodata=nodata_value,
        ) as dst:
            dst.write(nodata_data)
        cog_translate(tmp_path, str(valid_dir / "nodata.tif"), profile, quiet=True)
    finally:
        Path(tmp_path).unlink()
    print(f"Generated {valid_dir / 'nodata.tif'}")

    # --- Invalid: not_georeferenced.tif ---
    # A regular TIFF with no CRS
    non_geo_data = np.zeros((1, 32, 32), dtype=np.uint8)
    non_geo_data[0, :16, :16] = 255  # Simple pattern

    with rasterio.open(
        str(invalid_dir / "not_georeferenced.tif"),
        "w",
        driver="GTiff",
        height=32,
        width=32,
        count=1,
        dtype="uint8",
        # No CRS or transform!
    ) as dst:
        dst.write(non_geo_data)
    print(f"Generated {invalid_dir / 'not_georeferenced.tif'}")

    # --- Invalid: truncated.tif ---
    # Create a valid file then truncate it
    truncated_path = invalid_dir / "truncated.tif"
    shutil.copy(valid_dir / "singleband.tif", truncated_path)
    # Truncate to 50% of original size
    original_size = truncated_path.stat().st_size
    with open(truncated_path, "r+b") as f:
        f.truncate(original_size // 2)
    print(f"Generated {truncated_path}")


def main() -> None:
    """Generate all fixtures."""
    print("Generating test fixtures...")
    print("=" * 50)

    print("\n--- Vector fixtures (GeoJSON) ---")
    generate_vector_fixtures()

    print("\n--- Invalid vector fixtures ---")
    generate_invalid_fixtures()

    print("\n--- Edge case fixtures ---")
    generate_edge_cases()

    print("\n--- GeoParquet conversion ---")
    convert_to_geoparquet()

    print("\n--- Raster fixtures (COG) ---")
    generate_raster_fixtures()

    # Note: Shapefile fixtures deferred - GeoJSON/GeoParquet sufficient for now
    # See issue #30 for real-world data fixtures including Shapefiles

    print("\n" + "=" * 50)
    print("Done! All fixtures generated.")


if __name__ == "__main__":
    main()
