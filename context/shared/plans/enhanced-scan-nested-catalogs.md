# Enhanced Scan: Nested Catalogs + Structure Recommendations

## Context

Issues #241 and #234 both enhance `portolan scan` to understand ADR-0031/0032 catalog structures and provide intelligent guidance. This plan combines them into a single implementation.

**Problem**: Scan currently assumes flat structure and gives vague warnings like "Multiple primary assets" without explaining what's wrong or how to fix it.

**Goal**: Make scan an intelligent guide that:
1. Detects nested catalog structures (ADR-0032)
2. Recommends correct organization based on data patterns
3. Surfaces format status (GeoParquet vs Parquet, COG vs GeoTIFF)
4. Provides actionable suggestions with commands to run
5. Works well for both humans (interactive) and agents (JSON output)

## Key Rules (from discussion)

| Rule | Source |
|------|--------|
| ONE primary geo-asset per leaf directory | ADR-0031 |
| Multiple non-geo Parquet files OK as companions | User clarification |
| Nested catalogs organize collections thematically | ADR-0032 |
| Scan WARNS, add ERRORS (--strict makes scan error) | User decision |
| Dry-run preview outputs JSON for agent restructuring | User decision |
| No caching for now (YAGNI) | User decision |

## Files to Modify

### Core Changes

| File | Changes |
|------|---------|
| `portolan_cli/scan.py` | Import format detection, update structure checks, add --strict flag |
| `portolan_cli/scan_output.py` | Enhanced output with nested IDs, format status, recommendations |
| `portolan_cli/cli.py` | Add --strict flag to scan command |
| `portolan_cli/scan.py` | Remove duplicate `is_geoparquet()`, import from `formats.py` |

### New/Updated Types

| File | Changes |
|------|---------|
| `portolan_cli/scan.py` | Add `IssueType.MULTIPLE_GEO_PRIMARIES` (distinct from generic multiple primaries) |
| `portolan_cli/scan.py` | Add `ScannedFile.format_status` field (CLOUD_NATIVE/CONVERTIBLE/UNSUPPORTED) |
| `portolan_cli/scan.py` | Add `ScannedFile.inferred_collection_id` field (nested path) |

### Tests

| File | Changes |
|------|---------|
| `tests/unit/test_scan_nested.py` | New: Tests for nested collection ID inference |
| `tests/unit/test_scan_structure.py` | New: Tests for structure validation (one geo per leaf) |
| `tests/unit/test_scan_strict.py` | New: Tests for --strict flag behavior |
| `tests/specs/scan_nested_catalogs.md` | Update: Fix section 2.4 (multiple files rule) |

## Implementation Steps

### Phase 1: Foundation (collection ID + format detection)

1. **Remove duplicate `is_geoparquet()` from scan.py**
   - Delete lines 459-485 in `scan.py`
   - Import from `formats.py` instead
   - This consolidates format detection

2. **Add format status to ScannedFile**
   ```python
   @dataclass(frozen=True)
   class ScannedFile:
       # ... existing fields ...
       format_status: CloudNativeStatus  # NEW
       format_display_name: str  # NEW: "GeoParquet", "GeoTIFF (not COG)", etc.
   ```

3. **Add nested collection ID inference**
   - Import `infer_nested_collection_id()` from `dataset.py`
   - Call during `_process_file()` to compute collection path
   - Store in `ScannedFile.inferred_collection_id`

### Phase 2: Structure Validation

4. **Update `_check_mixed_structure()` for nested context**
   - Currently only checks root level
   - Extend to check ALL directories with geo-assets
   - Flag: "Directory has both data files AND subdirectories with data"

5. **Add `_check_multiple_geo_primaries()`**
   - For each directory, count GEO-assets (using `is_geoparquet()`, extension checks)
   - Ignore non-geo Parquet files (they're companions)
   - Warn if >1 geo-asset in same directory
   - Suggestion: "Move to separate subdirectories or reorganize as partitioned data"

6. **Add `_check_nested_structure_validity()`**
   - Detect intermediate directories (contain only subdirs)
   - Detect leaf directories (contain data files)
   - Flag ambiguous cases (data at intermediate level)

### Phase 3: Enhanced Output

7. **Update `_print_scan_summary_enhanced()`**
   - Show nested collection IDs: `climate/hittekaart`
   - Show format status per file: `data.parquet (GeoParquet)` vs `data.parquet (Parquet, no geometry)`
   - Group by inferred collection

8. **Add structure recommendations**
   - Pattern detection: vector collection, raster items, partitioned, mixed
   - Suggested structure (ASCII tree)
   - Commands to implement: `portolan add ...`
   - Link to portolan-spec for details

9. **Add --verbose flag enhancements**
   - Basic: issue + why it matters
   - Verbose: + recommended commands + spec reference

### Phase 4: Strict Mode + Agent Support

10. **Add --strict flag**
    - All warnings become errors (exit code 1)
    - Integrate with `add` command (add calls scan internally with --strict)

11. **Enhance --json output**
    - Include `inferred_collection_id` per file
    - Include `format_status` per file
    - Include `recommended_structure` object
    - Include `fix_commands` array for agent consumption

12. **Add dry-run preview for restructuring**
    - `scan --recommend --dry-run` shows what reorganization would look like
    - JSON output perfect for Claude to execute

## Reusable Functions (Don't Reinvent)

| Function | Location | Use For |
|----------|----------|---------|
| `is_geoparquet()` | `formats.py:165` | Distinguish geo vs non-geo Parquet |
| `is_cloud_optimized_geotiff()` | `formats.py:196` | COG vs regular GeoTIFF |
| `get_cloud_native_status()` | `formats.py:224` | Full format classification |
| `infer_nested_collection_id()` | `dataset.py:1238` | Compute nested collection path |
| `create_intermediate_catalogs()` | `catalog.py:506` | (for add, not scan) |

## Test Cases

### From existing fixtures (`tests/fixtures/scan/`)

| Fixture | Tests |
|---------|-------|
| `nested/` | Nested collection ID inference, multi-level structure |
| `multiple_primaries/` | Multiple geo-assets warning |
| `mixed_formats/` | Vector + raster in same dir |
| `clean_flat/` | Valid flat structure (no warnings) |

### New test cases needed

| Case | Expected |
|------|----------|
| One GeoParquet + multiple plain Parquet | Valid (no warning) |
| Two GeoParquet in same dir | Warning: multiple geo primaries |
| Deep nesting (5+ levels) | Correct nested collection ID |
| --strict with warnings | Exit code 1 |
| JSON output with nested IDs | Correct structure |

## Verification

1. **Unit tests**: `uv run pytest tests/unit/test_scan*.py -v`
2. **Integration**: `uv run pytest tests/integration/test_scan*.py -v`
3. **Manual testing**:
   ```bash
   # Test nested structure
   portolan scan tests/fixtures/scan/nested/ --json | jq

   # Test multiple primaries detection
   portolan scan tests/fixtures/scan/multiple_primaries/

   # Test strict mode
   portolan scan tests/fixtures/scan/multiple_primaries/ --strict; echo "Exit: $?"

   # Test with real data
   portolan scan ~/portolan-test-data/den-haag-test-nested/
   ```

4. **Full suite**: `uv run pytest`

### Phase 5: Progress Reporting

13. **Add simple progress indicator**
    - Quick pre-count of directories via `os.walk` (fast, < 100ms for typical trees)
    - During scan: `Scanning... (12/42 directories)`
    - Use `rich` progress bar or simple stderr output
    - Suppress in `--json` mode (agent/batch usage)
    - Show total at end: `Scanned 42 directories in 1.2s`

## Out of Scope (Future Work)

- Caching scan results (YAGNI for now)
- HTML report generation
- Auto-restructuring (destructive, use dry-run + agent instead)
- Schema validation for partitions (belongs in `check`)

## References

- ADR-0031: Collection-Level Assets for Vector Data
- ADR-0032: Nested Catalogs with Flat Collections
- Issue #234: Enhanced Scan UX
- Issue #241: Nested catalog support in scan
- Spec: https://github.com/portolan-sdi/portolan-spec

---

# Appendix: Test Specification

## Overview

This section defines detailed test cases for integrating ADR-0032 nested catalog support into `portolan scan`.

## UX Philosophy (from #234)

Scan should be a **helpful guide**, not a cryptic error generator. Key principles:

1. **Explain why** — Not just "missing catalog.json" but why it matters for STAC
2. **Actionable suggestions** — Tell users what command to run to fix
3. **Clear severity** — Info (FYI), Warning (suboptimal), Error (invalid)
4. **Consistent output** — Use `portolan_cli/output.py` patterns

## ADR-0032 Structure Rules

| Directory type | Contains | Expected STAC file |
|---------------|----------|-------------------|
| Root | subdirs only | `catalog.json` |
| Theme/domain dir | subdirs only | `catalog.json` (intermediate) |
| Vector data dir | `.parquet`, `.shp`, etc. | `collection.json` |
| Raster collection | item subdirs | `collection.json` |
| Raster item dir | `.tif` files | `item.json` |

**Key heuristic**: A directory containing only subdirectories (no data files) is a **catalog**. A directory containing data files is a **collection** (vector) or **item** (raster).

## Detailed Test Cases

### 1. Nested Collection ID Inference

**1.1 Single-level structure returns simple ID**
```
catalog-root/
└── demographics/
    └── census.parquet
```
- Scan should report collection ID: `demographics`

**1.2 Two-level nested structure returns path ID**
```
catalog-root/
└── climate/
    └── hittekaart/
        └── hittekaart.parquet
```
- Scan should report collection ID: `climate/hittekaart`

**1.3 Three-level nested structure returns full path ID**
```
catalog-root/
└── environment/
    └── air/
        └── quality/
            └── pm25.parquet
```
- Scan should report collection ID: `environment/air/quality`

**1.4 Mixed depths in same catalog**
```
catalog-root/
├── simple/
│   └── data.parquet
└── nested/
    └── deep/
        └── data.parquet
```
- Scan should report:
  - `simple` (collection ID)
  - `nested/deep` (collection ID)

### 2. Structure Validation

**2.1 Valid ADR-0032 structure (no issues)**
```
catalog-root/
├── catalog.json
└── theme/
    ├── catalog.json          <- intermediate catalog
    └── collection-a/
        ├── collection.json   <- leaf collection
        └── data.parquet
```
- Scan should report 0 structural issues
- Ready files show nested collection ID: `theme/collection-a`

**2.2 Missing intermediate catalog.json (info, not blocking)**
```
catalog-root/
├── catalog.json
└── theme/                    <- No catalog.json yet
    └── collection-a/
        └── data.parquet
```
- Scan should report nested collection ID: `theme/collection-a`
- Note: Missing intermediate `catalog.json` is NOT an error for scan
- The `add` command creates intermediate catalogs automatically
- Scan focuses on file discovery; `add` handles STAC structure

**2.3 Data files at intermediate level (structural issue)**
```
catalog-root/
└── theme/
    ├── stray-file.parquet    <- Data at intermediate level
    └── collection-a/
        └── data.parquet
```
- Scan should report BOTH files as ready (it's a discovery tool)
- Issue: "Directory 'theme' contains both data files and subdirectories with data"
- Severity: WARNING (ambiguous - is `theme` a collection or organizational?)
- Suggestion: "Move 'stray-file.parquet' into a subdirectory, or remove subdirectories"
- This is essentially the existing `MIXED_FLAT_MULTIITEM` check

**2.4 One GeoParquet + multiple plain Parquet (VALID)**
```
catalog-root/
└── my-collection/
    ├── data.parquet           <- GeoParquet (primary geo-asset)
    ├── attributes.parquet     <- Plain Parquet (companion, no geo metadata)
    └── lookup.parquet         <- Plain Parquet (companion, no geo metadata)
```
- This is VALID! One geo-asset + multiple non-geo companions
- Collection ID: `my-collection`
- No structural warnings
- Key: Use `is_geoparquet()` to distinguish primary from companions

**2.5 Multiple GeoParquet in same dir (INVALID)**
```
catalog-root/
└── my-collection/
    ├── boundaries.parquet     <- GeoParquet (primary)
    └── points.parquet         <- GeoParquet (SECOND primary - INVALID)
```
- WARNING: Multiple primary geo-assets in same directory
- Suggestion: "Reorganize into separate collections or use partitioned structure"

**2.6 Deep nesting without data at intermediate levels (valid)**
```
catalog-root/
└── theme/
    └── subtheme/
        └── collection/
            └── data.parquet
```
- Valid nested structure
- Collection ID: `theme/subtheme/collection`
- No warnings (intermediate dirs are organizational)

### 3. Raster vs Vector Detection

**3.1 Vector collection (files at collection level)**
```
catalog-root/
└── vectors/
    └── boundaries/
        └── municipalities.parquet
```
- `boundaries` is a collection (contains vector data directly)
- Collection ID: `vectors/boundaries`

**3.2 Raster collection (items in subdirectories)**
```
catalog-root/
└── rasters/
    └── landsat/
        └── 2024-01-15/
            └── scene.tif
```
- `landsat` is a collection (contains item subdirectories)
- `2024-01-15` is an item (contains raster assets)
- Collection ID: `rasters/landsat`

### 4. Edge Cases

**4.1 Deep nesting (5+ levels)**
```
catalog-root/
└── a/b/c/d/e/
    └── data.parquet
```
- Collection ID: `a/b/c/d/e`
- Should work without depth limits

**4.2 Collection ID with path separators in display**
- Nested IDs should use forward slashes consistently: `theme/subtheme/collection`
- NOT backslashes on Windows

**4.3 Existing flat catalog (backward compatible)**
```
catalog-root/
├── catalog.json
├── collection-a/
│   └── data.parquet
└── collection-b/
    └── data.parquet
```
- Should still work as before
- Collection IDs: `collection-a`, `collection-b`

## Non-Goals (Out of Scope for Scan)

- **Creating STAC files**: `scan --fix` doesn't create `catalog.json` or `collection.json`
- **Validating STAC metadata**: That's `portolan check --metadata`
- **Auto-restructuring directories**: Structural issues are reported, not auto-fixed
- **Checking for missing intermediate catalogs**: That's `add`'s job to create them
- **Validating existing STAC links**: That's `check --metadata`

## Key Insight: Scan's Role

Scan is a **discovery and validation tool for files**, not STAC structure:
- Discovers geospatial files and their formats
- Validates file-level issues (naming, paths, completeness)
- Infers what collection each file would belong to (nested paths)
- Reports ambiguous directory structures (mixed flat/nested)

The `add` command handles STAC structure creation (catalogs, collections, items).

## Implementation Notes

- Use `infer_nested_collection_id()` from `dataset.py` for collection ID inference
- Use `is_geoparquet()` from `formats.py` to distinguish geo vs non-geo Parquet
- Add new `IssueType.MULTIPLE_GEO_PRIMARIES` for multiple primary geo-assets
- Existing `_check_mixed_structure()` needs updates for nested context
