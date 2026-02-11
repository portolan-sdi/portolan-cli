# Scan False Positives - Needed Fixes

**Date:** 2025-02-11
**Status:** TODO
**Related PR:** #63

## Problem

The `portolan scan` command produces false positive warnings for valid catalog structures.

## Issue 1: All .parquet treated as "primary geospatial assets"

**Current behavior:** Any `.parquet` file is considered a primary asset.

**Reality:** Only GeoParquet files (with geometry) are primary. Regular Parquet files are auxiliary data.

**Example from `arg-censo-portolan/output/2010/`:**
```
2010/
├── radios.parquet       # GeoParquet (HAS geometry) → PRIMARY
├── census-data.parquet  # Regular Parquet (NO geometry) → AUXILIARY
├── metadata.parquet     # Regular Parquet (NO geometry) → AUXILIARY
└── *.pmtiles            # Overview/visualization derivative
```

The scan warns "Directory has 3 primary assets" but there's only 1 geospatial asset.

**Fix:** Check for GeoParquet metadata before classifying as primary:

```python
import pyarrow.parquet as pq

def is_geoparquet(path: Path) -> bool:
    """Check if a parquet file has GeoParquet metadata."""
    try:
        metadata = pq.read_metadata(path)
        schema_metadata = metadata.schema.metadata or {}
        return b'geo' in schema_metadata
    except Exception:
        return False  # Assume not geo if we can't read it
```

## Issue 2: PMTiles not recognized as overview/derivative

**Current behavior:** `.pmtiles` files may be treated as unknown or incorrectly categorized.

**Fix:** Add `.pmtiles` to recognized extensions with role `overview` (like thumbnails).

## Issue 3: Duplicate basenames across sibling directories

**Current behavior:** Warns about `2010/radios.parquet` and `2022/radios.parquet` having duplicate basenames.

**Reality:** This is **intentional** — STAC collections within a catalog SHOULD have consistent naming across years/versions. Only flag duplicates that would conflict during import (same directory or same target ID).

**Fix:** Change duplicate detection logic:
- Only warn about duplicates within the SAME directory
- OR warn if they would produce conflicting dataset IDs during import
- Don't warn about sibling directories with matching filenames

## Priority

| Fix | Priority | Complexity |
|-----|----------|------------|
| GeoParquet detection | P1 | Medium (needs pyarrow read) |
| Duplicate basename logic | P1 | Low (logic change only) |
| PMTiles as overview | P2 | Low (add to extension map) |

## Test Cases Needed

1. Directory with 1 GeoParquet + 2 regular Parquet → no "multiple primaries" warning
2. Sibling directories with same filenames → no "duplicate basename" warning
3. Same directory with duplicate basenames → SHOULD warn
4. Directory with .pmtiles → recognized as overview, not primary

## Dependencies

- `pyarrow` is already a project dependency (via geoparquet-io)
