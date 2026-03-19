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

## Out of Scope (Future Work)

- Caching scan results (YAGNI for now)
- HTML report generation
- Auto-restructuring (destructive, use dry-run + agent instead)
- Schema validation for partitions (belongs in `check`)
- Progress indicators (can add later if needed)

## References

- ADR-0031: Collection-Level Assets for Vector Data
- ADR-0032: Nested Catalogs with Flat Collections
- Issue #234: Enhanced Scan UX
- Issue #241: Nested catalog support in scan
- Spec: https://github.com/portolan-sdi/portolan-spec
