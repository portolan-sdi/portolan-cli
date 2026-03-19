# Feature: Scan Nested Catalogs (ADR-0032)

This specification defines test cases for nested catalog support in `portolan scan`.

## Key Concepts

| Concept | Meaning |
|---------|---------|
| **Collection ID** | Flat identifier in STAC (`hittekaart`), NOT a path |
| **Display path** | `/`-separated path showing hierarchy (`climate/hittekaart`) |
| **Primary geo-asset** | GeoParquet, GeoJSON, Shapefile, etc. (has geometry) |
| **Companion** | Plain Parquet without geometry (attribute tables, lookups) |

**Critical distinction**: Scan discovers files and infers structure. Add creates STAC files. Missing intermediate `catalog.json` is NOT a scan error—`add` creates those automatically.

## Happy Path

### Nested Collection ID Inference

- [ ] Single-level structure returns simple ID
  ```
  catalog-root/demographics/census.parquet
  ```
  Collection ID: `demographics`

- [ ] Two-level nested structure returns path ID
  ```
  catalog-root/climate/hittekaart/data.parquet
  ```
  Collection ID: `climate/hittekaart`

- [ ] Three-level nested structure returns full path ID
  ```
  catalog-root/environment/air/quality/pm25.parquet
  ```
  Collection ID: `environment/air/quality`

- [ ] Mixed depths in same catalog reports both correctly
  ```
  catalog-root/simple/data.parquet
  catalog-root/nested/deep/data.parquet
  ```
  Collection IDs: `simple`, `nested/deep`

### Valid Structure (No Warnings)

- [ ] One GeoParquet + multiple plain Parquet companions = VALID
  ```
  my-collection/
  ├── data.parquet           <- GeoParquet (primary)
  ├── attributes.parquet     <- Plain Parquet (companion)
  └── lookup.parquet         <- Plain Parquet (companion)
  ```
  Collection ID: `my-collection`, no structural warnings.
  Uses `is_geoparquet()` to distinguish primary from companions.

- [ ] Valid ADR-0032 structure with intermediate catalogs
  ```
  catalog-root/
  ├── catalog.json
  └── theme/
      ├── catalog.json
      └── collection-a/
          ├── collection.json
          └── data.parquet
  ```
  0 structural issues, collection ID: `theme/collection-a`

- [ ] Deep nesting without data at intermediate levels
  ```
  catalog-root/theme/subtheme/collection/data.parquet
  ```
  Collection ID: `theme/subtheme/collection`, no warnings

- [ ] Existing flat catalog (backward compatible)
  ```
  catalog-root/
  ├── catalog.json
  ├── collection-a/data.parquet
  └── collection-b/data.parquet
  ```
  Collection IDs: `collection-a`, `collection-b`

## Edge Cases

### Multiple Primary Geo-Assets

- [ ] Multiple GeoParquet in same directory = WARNING
  ```
  my-collection/
  ├── boundaries.parquet     <- GeoParquet
  └── points.parquet         <- GeoParquet (SECOND primary)
  ```
  Issue type: `MULTIPLE_GEO_PRIMARIES`
  Severity: WARNING
  Suggestion: "Reorganize into separate collections or use partitioned structure"

- [ ] Multi-format same basename = WARN (cloud-native + legacy copy)
  ```
  my-collection/
  ├── data.parquet           <- GeoParquet
  └── data.shp               <- Shapefile (legacy copy)
  ```
  WARNING: "Cloud-native and legacy copy detected"

- [ ] Multi-format different basenames = ERROR with hint
  ```
  my-collection/
  ├── boundaries.parquet     <- GeoParquet
  └── points.shp             <- Shapefile (different name)
  ```
  ERROR: Multiple geo-primaries with naming convention hint

### Structural Issues

- [ ] Data files at intermediate level = WARNING
  ```
  theme/
  ├── stray-file.parquet     <- Data at intermediate level
  └── collection-a/
      └── data.parquet
  ```
  Both files reported as ready (scan is a discovery tool).
  Issue: "Directory 'theme' contains both data files and subdirectories with data"
  Severity: WARNING (existing `MIXED_FLAT_MULTIITEM` check)
  Suggestion: "Move into subdirectory or remove subdirectories"

- [ ] Missing intermediate catalog.json = NOT an error for scan
  ```
  catalog-root/
  ├── catalog.json
  └── theme/                    <- No catalog.json yet
      └── collection-a/
          └── data.parquet
  ```
  Collection ID reported: `theme/collection-a`
  No error—`add` creates intermediate catalogs automatically.

### Deep Nesting

- [ ] Five+ level nesting works without depth limits
  ```
  catalog-root/a/b/c/d/e/data.parquet
  ```
  Collection ID: `a/b/c/d/e`

- [ ] Path separators consistent on all platforms
  Collection ID uses forward slashes (`theme/subtheme/collection`)
  Never backslashes, even on Windows

### Raster vs Vector Detection

- [ ] Vector collection (files at collection level)
  ```
  vectors/boundaries/municipalities.parquet
  ```
  `boundaries` is a collection, collection ID: `vectors/boundaries`

- [ ] Raster collection (items in subdirectories)
  ```
  rasters/landsat/2024-01-15/scene.tif
  ```
  `landsat` is a collection (contains item subdirs)
  `2024-01-15` is an item (contains raster assets)
  Collection ID: `rasters/landsat`

## Strict Mode

- [ ] `--strict` makes warnings become errors
  ```bash
  portolan scan path/with/warnings --strict
  ```
  Exit code: 1 (instead of 0 with warnings)

- [ ] `add` calls `scan` internally with `--strict`
  Ensures structural issues block add operations

## JSON Output

- [ ] JSON includes `inferred_collection_id` per file
- [ ] JSON includes `format_status` per file (CLOUD_NATIVE/CONVERTIBLE/UNSUPPORTED)
- [ ] JSON includes `recommended_structure` object
- [ ] JSON includes `fix_commands` array (structured for agent consumption)
  ```json
  {
    "fix_commands": [
      {"command": "add", "args": ["climate/hittekaart"], "reason": "Collection not tracked"},
      {"command": "convert", "args": ["data.tif"], "options": {"output": "data.tif"}, "reason": "GeoTIFF is not COG"}
    ]
  }
  ```

## Invariants

- [ ] Scan never creates or modifies STAC files (discovery only)
- [ ] Scan never auto-restructures directories (report issues, don't fix)
- [ ] Collection IDs always use forward slashes regardless of OS
- [ ] One geo-primary per leaf directory is the golden rule (companions OK)
- [ ] Corrupted files are flagged but don't error scan (error on `add`)
- [ ] Non-geo Parquet files are never counted as primaries

## Non-Goals (Explicitly Out of Scope)

These belong to other commands, NOT scan:

| Out of Scope | Belongs To |
|--------------|------------|
| Creating STAC files | `add` |
| Validating STAC metadata | `check --metadata` |
| Auto-restructuring directories | `scan --fix` (report only) |
| Creating intermediate catalogs | `add` |
| Validating existing STAC links | `check --metadata` |

## Test Fixtures

| Fixture | Purpose |
|---------|---------|
| `tests/fixtures/scan/nested/` | Basic nested collection ID inference |
| `tests/fixtures/scan/geoparquet_with_companions/` | GeoParquet + plain Parquet companions |
| `tests/fixtures/scan/multiple_geoparquet/` | Two GeoParquet in same dir (warning) |
| `tests/fixtures/scan/deep_nested/` | 5+ levels of nesting |
| `tests/fixtures/scan/mixed_depths/` | Shallow + nested in same catalog |
| `tests/fixtures/scan/three_level_nested/` | 3+ level nesting |

## References

- [ADR-0031](../../context/shared/adr/0031-collection-level-assets-for-vector-data.md): Collection-Level Assets for Vector Data
- [ADR-0032](../../context/shared/adr/0032-nested-catalogs-with-flat-collections.md): Nested Catalogs with Flat Collections
- [Plan](../../context/shared/plans/enhanced-scan-nested-catalogs.md): Implementation plan
