# Scope: convert-command

## Current Focus

> **Phase 3: Architecture Dialogue** -- COMPLETE. Ready for task decomposition.

---

## Research Summary

### Codebase Context

**Existing Infrastructure (80% Complete):**

| Component | Location | Status |
|-----------|----------|--------|
| convert_vector() | portolan_cli/dataset.py:202 | Exists, wraps geoparquet-io |
| convert_raster() | portolan_cli/dataset.py:227 | Exists, wraps rio-cogeo |
| Format detection | portolan_cli/formats.py | Complete 3-tier system |
| Output utilities | portolan_cli/output.py | Standardized messaging |
| JSON envelopes | portolan_cli/json_output.py | Dual output mode ready |
| Metadata extraction | portolan_cli/metadata/ | GeoParquet + COG done |

**Format Classification (from formats.py):**

- CLOUD_NATIVE: GeoParquet, COG, FlatGeobuf, PMTiles, Zarr, COPC
- CONVERTIBLE: Shapefile, GeoJSON, GeoPackage, CSV, JP2, non-COG TIFF
- UNSUPPORTED: NetCDF, HDF5, LAS/LAZ (non-COPC)

**Gaps to Fill:**

1. No standalone convert_file() API function
2. No ConversionResult dataclass
3. No CLI command registration
4. No format-specific error types in errors.py

**Key ADRs:**

- ADR-0007: CLI wraps API (all logic in library layer)
- ADR-0010: Delegate conversion to geoparquet-io/rio-cogeo
- ADR-0014: Accept non-cloud-native with warnings

### Domain Research

**Library APIs:**

| Library | API Pattern | Key Features |
|---------|-------------|--------------|
| geoparquet-io | gpio.read(input).add_bbox().sort_hilbert().write(output) | Fluent API, cloud storage, spatial indexing |
| rio-cogeo | cog_translate(src, dst, profile, in_memory=True) | Profiles (deflate/jpeg/webp), auto-overviews, validation |

**CLI Design Patterns (from GDAL, rio, fio):**

- Simple: command INPUT OUTPUT
- Options: --format, --profile, --quiet, --dry-run, --overwrite
- Progress: click.progressbar() for long operations
- Dual output: Human-readable + JSON

**Best Practices:**

- Batch processing (122,880 rows optimal for DuckDB)
- Memory-mapping for large files
- Auto in-memory for <120M pixels (rio-cogeo)
- Spatial ordering before compression (Hilbert sort)

---

## Decision Tree

```mermaid
graph TD
    A[Input file] --> B{Cloud-native?}
    B -->|No| C{Format type?}
    B -->|Yes| D{Optimized?}
    C -->|Vector| E[geoparquet-io convert]
    C -->|Raster| F[rio-cogeo create]
    D -->|No vector| G[geoparquet-io check --fix]
    D -->|No raster| H[rio-cogeo create to temp + replace]
    D -->|Yes| I[Skip - already good]
    E --> J[Validate output]
    F --> J
    G --> J
    H --> J
    J --> K[Update versions.json]
```

**Note on rio-cogeo:** Unlike geoparquet-io, rio-cogeo has **no `--fix` or in-place optimization mode**. It only has `create` (make new COG) and `validate` (check existing). To "fix" a suboptimal COG, we must create a new file and replace the original. See [rio-cogeo CLI docs](https://cogeotiff.github.io/rio-cogeo/CLI/).

## Task Decomposition

### Phase 1: Core Data Structures

| ID | Task | Files | Dependencies | Acceptance Criteria |
|----|------|-------|--------------|---------------------|
| 1.1 | **Test spec**: ConversionStatus enum values and behavior | `tests/specs/convert_status.md` | None | Spec defines all 4 states (SUCCESS, SKIPPED, FAILED, INVALID) with examples |
| 1.2 | **Test**: ConversionStatus enum | `tests/unit/test_convert.py` | 1.1 | Tests for enum values, string representation, comparison |
| 1.3 | **Implement**: ConversionStatus enum | `portolan_cli/convert.py` | 1.2 | Enum passes all tests |
| 1.4 | **Test**: ConversionResult dataclass | `tests/unit/test_convert.py` | 1.3 | Tests for creation, fields (source, output, format_from, format_to, status, error, duration_ms) |
| 1.5 | **Implement**: ConversionResult dataclass | `portolan_cli/convert.py` | 1.4 | Dataclass passes all tests, includes `to_dict()` for JSON serialization |

### Phase 2: Single-File Conversion API

| ID | Task | Files | Dependencies | Acceptance Criteria |
|----|------|-------|--------------|---------------------|
| 2.1 | **Test spec**: convert_file() behavior for all scenarios | `tests/specs/convert_file.md` | 1.5 | Spec covers: vector conversion, raster conversion, already cloud-native (skip), unsupported format, conversion failure, validation failure |
| 2.2 | **Test**: convert_file() with cloud-native input (skip case) | `tests/unit/test_convert.py` | 2.1 | Returns SKIPPED status, no file operations |
| 2.3 | **Test**: convert_file() with vector input | `tests/unit/test_convert.py` | 2.1 | Wraps existing `convert_vector()`, returns SUCCESS, output path correct |
| 2.4 | **Test**: convert_file() with raster input | `tests/unit/test_convert.py` | 2.1 | Wraps existing `convert_raster()`, returns SUCCESS, uses COG defaults from plan |
| 2.5 | **Test**: convert_file() handles conversion exception | `tests/unit/test_convert.py` | 2.1 | Returns FAILED status, error message captured, original file preserved |
| 2.6 | **Test**: convert_file() handles validation failure | `tests/unit/test_convert.py` | 2.1 | Returns INVALID status, output file preserved for inspection |
| 2.7 | **Implement**: convert_file() function | `portolan_cli/convert.py` | 2.2-2.6 | All tests pass; uses `get_cloud_native_status()` from formats.py; wraps `convert_vector()`/`convert_raster()` from dataset.py |
| 2.8 | **Refactor**: Update convert_raster() with plan's COG defaults | `portolan_cli/dataset.py` | 2.7 | DEFLATE compression, predictor=2, 512x512 tiles, nearest resampling |

### Phase 3: Batch Conversion with Callbacks

| ID | Task | Files | Dependencies | Acceptance Criteria |
|----|------|-------|--------------|---------------------|
| 3.1 | **Test spec**: ConversionReport and convert_directory() | `tests/specs/convert_directory.md` | 2.7 | Spec covers: report aggregation, callback invocation, partial failure handling, idempotent re-runs |
| 3.2 | **Test**: ConversionReport dataclass | `tests/unit/test_convert.py` | 3.1 | Tests for `succeeded`, `failed`, `skipped`, `invalid` counts; `to_dict()` |
| 3.3 | **Implement**: ConversionReport dataclass | `portolan_cli/convert.py` | 3.2 | Aggregates list of ConversionResult, exposes counts |
| 3.4 | **Test**: convert_directory() basic functionality | `tests/unit/test_convert.py` | 3.3 | Converts multiple files, returns ConversionReport |
| 3.5 | **Test**: convert_directory() callback invocation | `tests/unit/test_convert.py` | 3.4 | Callback called for each file with ConversionResult |
| 3.6 | **Test**: convert_directory() continues after failure | `tests/unit/test_convert.py` | 3.4 | One file fails, others still processed |
| 3.7 | **Test**: convert_directory() skips already-converted | `tests/unit/test_convert.py` | 3.4 | Re-run on same directory skips cloud-native files |
| 3.8 | **Implement**: convert_directory() function | `portolan_cli/convert.py` | 3.4-3.7 | All tests pass; signature: `convert_directory(path, on_progress=None) -> ConversionReport` |

### Phase 4: Conversion Error Types

| ID | Task | Files | Dependencies | Acceptance Criteria |
|----|------|-------|--------------|---------------------|
| 4.1 | **Test**: ConversionError base class | `tests/unit/test_errors.py` | None | Follows PortolanError pattern with PRTLN-CNV* code |
| 4.2 | **Test**: UnsupportedFormatError (PRTLN-CNV001) | `tests/unit/test_errors.py` | 4.1 | Has path, format_type context |
| 4.3 | **Test**: ConversionFailedError (PRTLN-CNV002) | `tests/unit/test_errors.py` | 4.1 | Has path, original_error context |
| 4.4 | **Test**: ValidationFailedError (PRTLN-CNV003) | `tests/unit/test_errors.py` | 4.1 | Has path, validation_errors context |
| 4.5 | **Implement**: Conversion error types | `portolan_cli/errors.py` | 4.1-4.4 | All error types follow PortolanError pattern |
| 4.6 | **Refactor**: convert_file() to use new error types | `portolan_cli/convert.py` | 4.5, 2.7 | Raises/catches specific error types |

### Phase 5: Source Tracking in versions.json

| ID | Task | Files | Dependencies | Acceptance Criteria |
|----|------|-------|--------------|---------------------|
| 5.1 | **Test spec**: Source tracking fields | `tests/specs/versions_source_tracking.md` | None | Spec defines `source_path`, `source_mtime` fields on Asset |
| 5.2 | **Test**: Asset with source tracking fields | `tests/unit/test_versions.py` | 5.1 | Asset accepts optional `source_path`, `source_mtime` |
| 5.3 | **Implement**: Add source tracking to Asset dataclass | `portolan_cli/versions.py` | 5.2 | Optional fields: `source_path: str | None`, `source_mtime: float | None` |
| 5.4 | **Test**: _serialize_versions_file includes source fields | `tests/unit/test_versions.py` | 5.3 | JSON output includes source fields when present |
| 5.5 | **Test**: _parse_versions_file handles source fields | `tests/unit/test_versions.py` | 5.3 | Reads source fields from JSON, defaults to None |
| 5.6 | **Implement**: Update serialize/parse for source tracking | `portolan_cli/versions.py` | 5.4-5.5 | Backward compatible: old versions.json still works |

### Phase 6: Integration with check --fix Workflow

| ID | Task | Files | Dependencies | Acceptance Criteria |
|----|------|-------|--------------|---------------------|
| 6.1 | **Test spec**: check --fix integration | `tests/specs/check_fix_integration.md` | 3.8, 5.6 | Spec covers: scan → convert → validate → update versions.json |
| 6.2 | **Test**: check command detects convertible files | `tests/integration/test_check_command.py` | 6.1 | Uses scan result, identifies CONVERTIBLE files |
| 6.3 | **Test**: check --fix converts and validates | `tests/integration/test_check_command.py` | 6.2 | Calls convert_directory(), validates outputs |
| 6.4 | **Test**: check --fix updates versions.json | `tests/integration/test_check_command.py` | 6.3 | Assets have source tracking fields |
| 6.5 | **Test**: check --fix with --dry-run | `tests/integration/test_check_command.py` | 6.3 | Shows what would happen, no file changes |
| 6.6 | **Test**: check --fix with partial failure | `tests/integration/test_check_command.py` | 6.3 | Reports summary, continues after failures |
| 6.7 | **Implement**: check command with --fix flag | `portolan_cli/cli.py`, `portolan_cli/check.py` | 6.2-6.6 | Wires convert_directory() into check workflow |
| 6.8 | **Implement**: Progress output via callback | `portolan_cli/cli.py` | 6.7 | Uses `click.progressbar()` in CLI layer |

### Phase 7: Edge Cases and Hardening

| ID | Task | Files | Dependencies | Acceptance Criteria |
|----|------|-------|--------------|---------------------|
| 7.1 | **Test**: Shapefile with missing sidecars | `tests/unit/test_convert.py` | 2.7 | Warns, attempts conversion (delegates to geoparquet-io) |
| 7.2 | **Test**: Permission error during conversion | `tests/unit/test_convert.py` | 2.7 | Returns FAILED with clear error message |
| 7.3 | **Test**: Output file already exists (cloud-native) | `tests/unit/test_convert.py` | 2.7 | Skips if optimized |
| 7.4 | **Test**: Output file already exists (not optimized) | `tests/unit/test_convert.py` | 2.7 | Re-optimizes vector with geoparquet-io; re-creates raster COG |
| 7.5 | **Test**: Raster "fix" (non-COG to COG replacement) | `tests/unit/test_convert.py` | 2.7 | Creates temp file, validates, replaces original |
| 7.6 | **Test**: Source file changed (mtime detection) | `tests/unit/test_convert.py` | 5.6 | Warns user when source mtime differs from recorded |
| 7.7 | **Integration test**: Full convert workflow with real files | `tests/integration/test_convert_workflow.py` | All above | Uses `tests/fixtures/` data, end-to-end conversion |

### Implementation Order (Recommended)

```
Phase 1 (Core) ──────────────────────────────────────────────▶
    │
    ▼
Phase 2 (Single File) ──────────────────────────────────────▶
    │
    ├── Phase 4 (Errors) ──────────────▶
    │
    ▼
Phase 3 (Batch) ────────────────────────────────────────────▶
    │
    ▼
Phase 5 (Source Tracking) ──────────────────────────────────▶
    │
    ▼
Phase 6 (Integration) ──────────────────────────────────────▶
    │
    ▼
Phase 7 (Edge Cases) ───────────────────────────────────────▶
```

**Estimated effort:** 3-4 sessions (each phase ~30-60 min)

**Key files created:**
- `portolan_cli/convert.py` (new) - Core conversion API
- `portolan_cli/check.py` (new) - Check command implementation
- `tests/specs/convert_*.md` (new) - Test specifications
- `tests/unit/test_convert.py` (new) - Unit tests
- `tests/integration/test_convert_workflow.py` (new) - Integration tests

**Key files modified:**
- `portolan_cli/errors.py` - Add PRTLN-CNV* error types
- `portolan_cli/versions.py` - Add source tracking fields
- `portolan_cli/dataset.py` - Update COG defaults
- `portolan_cli/cli.py` - Wire check --fix command

## Dependency Order

### Existing Infrastructure (Already Built)

| Module | What It Does | Convert Integration |
|--------|--------------|---------------------|
| `versions.py` | `Asset`, `Version`, `VersionsFile` dataclasses; read/write versions.json | Store source mtime + conversion metadata |
| `formats.py` | `get_cloud_native_status()` returns `CLOUD_NATIVE/CONVERTIBLE/UNSUPPORTED` | Use to decide if conversion needed |
| `scan.py` | `scan_directory()` returns `ScanResult` with ready files | Provides file list to convert |
| `catalog.py` | `Catalog.init()` creates `.portolan/` directory | Convert runs after init |
| `dataset.py` | `convert_vector()`, `convert_raster()` already exist | Wrap these in new API |
| `cli.py` | `init` command creates catalog | Wire convert into workflow |

### What Needs to Be Built

```mermaid
graph TD
    A[1. ConversionResult dataclass] --> B[2. Refactor convert_file]
    B --> C[3. ConversionReport dataclass]
    C --> D[4. convert_directory with callback]
    D --> E[5. versions.json source tracking]
    E --> F[6. Wire into init workflow]
```

| Step | What | Notes |
|------|------|-------|
| 1 | `ConversionResult` dataclass | source, output, format_from, format_to, status, error |
| 2 | Refactor existing `convert_vector/convert_raster` into unified `convert_file()` | Already have the logic, just need wrapper |
| 3 | `ConversionReport` dataclass | Aggregates results, has `succeeded`, `failed`, `skipped` counts |
| 4 | `convert_directory()` with callback | Calls convert_file, invokes callback, returns report |
| 5 | versions.json source tracking | Add `source_path`, `source_mtime` fields to Asset |
| 6 | Wire into init/check workflow | After scan, if convertible files found, convert them |

## Happy Path

### Catalog Structure (ADR-0012: Flat Hierarchy)

```
.portolan/                    <- catalog root
├── catalog.json
├── {collection}/             <- first-level subdirectory = collection
│   ├── collection.json
│   ├── versions.json
│   └── {item}/               <- dataset within collection
│       ├── data.parquet      <- cloud-native file
│       └── item.json         <- STAC item
└── {collection}/
```

### Command Workflow (Build Pieces First, Compose Later)

```mermaid
graph TD
    A[portolan init] --> B[portolan scan]
    B --> C[portolan check --fix]
    C --> D[portolan push]
```

| Command | Purpose | Analogy |
|---------|---------|---------|
| `portolan init` | Create .portolan/ catalog | `git init` |
| `portolan scan` | Find files, detect structure issues | `ruff format --check` |
| `portolan scan --fix` | Fix filenames (invalid chars, long paths) | `ruff format` |
| `portolan check` | Validate cloud-native status, metadata | `ruff check` |
| `portolan check --fix` | Convert + optimize + generate metadata | `ruff check --fix` |
| `portolan push` | Sync to remote | `git push` |

**Key insight:**
- `scan --fix` = formatter (structure/naming)
- `check --fix` = linter (cloud-native/metadata)
- `convert` is internal to `check --fix`

### Example Session

```bash
$ portolan init /data/gis
✓ Initialized catalog in /data/gis/.portolan/

$ portolan scan
Found 4 geospatial files:
  - 3 vector (roads.shp, parcels.geojson, buildings.gpkg)
  - 1 raster (elevation.tif)
Cloud-native status:
  - 0 already optimized
  - 4 need conversion (use `portolan check --fix`)

$ portolan check --fix
Converting...
  ✓ roads.shp → roads.parquet (in-place)
  ✓ parcels.geojson → parcels.parquet
  ✓ buildings.gpkg → buildings.parquet
  ✓ elevation.tif → elevation.tif (COG, in-place)
Validating...
  ✓ 4/4 files pass cloud-native checks
Generating metadata...
  ✓ 4 STAC items created

$ portolan push
Syncing to s3://my-bucket/...
  ✓ Uploaded 4 datasets
```

**Ending state:**
```
/data/gis/
├── .portolan/
│   ├── catalog.json
│   └── versions.json
├── roads.parquet        <- converted (original .shp deleted with --replace)
├── parcels.parquet      <- converted
├── buildings.parquet    <- converted
└── elevation.tif        <- COG (in-place conversion)
```

## Edge Cases

| Scenario | Expected Behavior | Notes |
|----------|-------------------|-------|
| Conversion fails midway | Keep original, flag output as INVALID | Never delete - let user inspect and decide |
| One file in batch fails | Continue with others, report summary at end | "3/4 succeeded, 1 failed: parcels.shp" |
| Re-run after partial failure | Skip already-converted files | Idempotent - only process what's needed |
| Shapefile with missing sidecars | Warn, attempt conversion anyway | geoparquet-io may handle it |
| Already cloud-native | Skip, no-op | Don't re-convert GeoParquet to GeoParquet |
| Output file already exists | Skip if optimized, re-optimize if not | Portolan is opinionated - enforce standards |
| Cloud-native but not optimized (vector) | Re-optimize in-place | Use `geoparquet-io check --fix` |
| Cloud-native but not optimized (raster) | Re-create COG, replace original | rio-cogeo has no `--fix`; must create new file |
| Permission error | FAIL LOUDLY | Don't silently skip — error with clear message |
| Format edge cases | Delegate to upstream | geoparquet-io/rio-cogeo handle projections, multi-geometry, etc. |

## Resolved Assumptions

| Assumption | Decision | Rationale |
|------------|----------|-----------|
| User-facing command? | NO | Convert is internal plumbing, not a CLI command. Users pass directories; Portolan handles conversion automatically. For single files, users go directly to geoparquet-io/rio-cogeo. |
| Trigger mode | AUTO + MANUAL | Automatic by default during workflow, but can be invoked explicitly. `--skip-conversion` flag to opt out. |
| Output location | SIDE-BY-SIDE (vectors), IN-PLACE (rasters) | Vectors: keep original, add .parquet. Rasters: overwrite .tif with COG (same extension). `--replace` to delete original vectors after conversion. |
| Deletion safety | AFTER success | Only delete original after conversion succeeds + validates. |
| Dry-run | YES | `--dry-run` shows what would happen without doing it. |
| Design philosophy | OPINIONATED | Portolan enforces best practices by default. Users opt-out, not opt-in. |
| Failure mode | CONTINUE + REPORT | Process all files, summarize failures at end. Not atomic. |
| Re-run behavior | INCREMENTAL | Skip already-converted. Cloud-native files become ground truth. |
| State tracking | USE versions.json | Existing infrastructure: semantic version + timestamp + SHA + manifest. Convert integrates with this. |
| API design | TWO FUNCTIONS + CALLBACK | `convert_file()` for single file, `convert_directory()` with `on_progress` callback for streaming feedback. Returns `ConversionReport` for summary. |
| Progress reporting | CALLBACK-BASED | Library layer uses callback; CLI wires to click.progressbar(). CI can log or pass None. |
| Success criteria | EXISTS + VALID + BEST PRACTICES | File exists, passes validation, passes `check --fix` (geoparquet-io handles opinions). |
| Primary user | LOCAL GOV / MUNICIPAL | Employee batch-converting hard drive of legacy files to cloud-optimized catalog on S3. |
| Source change detection | MTIME (MVP) | Check mtime first; if changed, warn user. Later: add heuristic (bbox/count) → hash for reliability. |

## Open Questions

~~All resolved - see Resolved Assumptions table above.~~

## Parking Lot

~~All items resolved.~~

## Resolved Technical Decisions

### COG Optimization Settings

**Single opinionated default** (no per-data-type profiles):

| Setting | Value | Rationale |
|---------|-------|-----------|
| Compression | DEFLATE | Universal compatibility, lossless, works everywhere |
| Predictor | 2 (horizontal differencing) | Improves compression for all data types |
| Tile size | 512x512 | Matches rio-cogeo default; fewer tiles = fewer HTTP requests |
| Overview resampling | nearest | Safe for all data types (categorical, imagery, elevation) |

**Power users:** For fine-tuned control (WEBP for imagery, LERC for elevation), use `rio_cogeo.cog_translate()` directly. Portolan is for batch workflows, not per-file optimization.

**Sources:**
- [Cloud Native Geo Guide](https://guide.cloudnativegeo.org/cloud-optimized-geotiffs/cogs-details.html)
- [Koko Alberti's compression guide](https://kokoalberti.com/articles/geotiff-compression-optimization-guide/)
- Vincent Sarago's COG analysis (rio-cogeo author)

### GeoParquet Optimization Settings

**Delegate to geoparquet-io** - it's opinionated by default (bbox columns, Hilbert sort, etc.).

### Partial Write / Corruption Handling

**Strategy: Post-hoc validation, flag invalid files**

| Step | Action |
|------|--------|
| 1. Convert | Let rio-cogeo/geoparquet-io write directly (no wrapper) |
| 2. Validate | Run `cog_validate()` for COGs, check parquet footer for GeoParquet |
| 3. On failure | **Flag as INVALID** in ConversionReport - do NOT delete |
| 4. User decides | Manual inspection, retry, or use upstream tools |

**Why not atomic write wrappers:**
- Wrapping doesn't prevent corruption *during* the library's write
- Adds I/O overhead (write twice for large files)
- Post-hoc validation catches corruption from any cause (crashes, disk errors, OOM)

**Atomic writes for metadata only:** versions.json, catalog.json use temp+rename pattern (small files we control).

### ConversionStatus Values

```python
class ConversionStatus(Enum):
    SUCCESS = "success"    # Converted and validated
    SKIPPED = "skipped"    # Already cloud-native
    FAILED = "failed"      # Conversion threw exception
    INVALID = "invalid"    # Converted but failed validation (file kept for inspection)
```

### rio-cogeo Memory Handling

**Delegate to rio-cogeo's auto in-memory threshold** (~120M pixels / ~360MB for RGB uint8).

- Below threshold: in-memory processing (faster)
- Above threshold: temp file on disk (safer)
- Environment variable `IN_MEMORY_THRESHOLD` available for override
- Document `--no-in-memory` equivalent for very large rasters if needed

### rio-cogeo Has No `--fix` Mode

**Confirmed via [rio-cogeo documentation](https://cogeotiff.github.io/rio-cogeo/CLI/):**

| Command | Purpose |
|---------|---------|
| `rio cogeo create` | Create new COG from any GeoTIFF |
| `rio cogeo validate` | Check if file is valid COG |
| `rio cogeo info` | Show raster metadata |

**No `--fix` or optimize command exists.** To "fix" a suboptimal COG:

1. Run `rio cogeo create existing.tif temp_optimized.tif`
2. Validate the output
3. Replace original with optimized version

This differs from geoparquet-io which has `check --fix` for in-place optimization.

**Implication for Portolan:** Raster optimization always requires creating a temporary file and replacing the original. This is fine - rio-cogeo already uses temp files internally during `cog_translate()`.
