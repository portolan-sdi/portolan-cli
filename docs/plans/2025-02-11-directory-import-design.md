# Directory Import Design

**Date:** 2025-02-11
**Status:** Draft
**Related Issues:** [#12](https://github.com/portolan-sdi/portolan-cli/issues/12), [#17](https://github.com/portolan-sdi/portolan-cli/issues/17)

## Problem Statement

Users have messy, inconsistent directory structures ranging from clean hierarchies to "20GB accumulated on a hard drive." The current `portolan dataset add` command handles single files well but lacks robust directory handling.

We need to support:

1. **Flat directories** — 50 independent files → 50 datasets
2. **Hierarchical organization** — `census/2020/tracts.parquet` → catalog structure
3. **Partitioned data** — 100 files that are ONE dataset
4. **Mixed messes** — All of the above, poorly organized

## Design Principles

1. **Separation of concerns** — Scanning/validation separate from import (like ruff check vs ruff format)
2. **Sane defaults with escape hatches** — Works out of the box, flexible when needed
3. **Fail-safe over fail-fast** — Warn and continue rather than abort on first issue
4. **CLI-first** — All functionality accessible via command line

## Command Structure

### Three-Stage Workflow

```
scan → (optional fix) → dataset add
```

| Command | Purpose | Analogy |
|---------|---------|---------|
| `portolan scan <path>` | Analyze structure, report issues | `ruff check` |
| `portolan scan <path> --fix` | Apply safe fixes | `ruff check --fix` |
| `portolan scan <path> --unsafe-fix` | Apply destructive fixes | `ruff check --unsafe-fix` |
| `portolan dataset add <path>` | Import files into catalog | `git add` |

### `portolan scan`

```bash
portolan scan /data/                    # Report only (human-readable)
portolan scan /data/ --json             # Machine-readable output
portolan scan /data/ --fix              # Safe fixes (rename invalid chars)
portolan scan /data/ --unsafe-fix       # Destructive fixes (move/split dirs)
```

**Output example:**

```
$ portolan scan /data/messy-drive/

📁 Scanned 147 files across 23 directories

⚠️  Structure Issues (12):
   • /census/2010/ has 3 primary assets (tracts.parquet, blocks.parquet, counties.parquet)
     → Suggest: Split into separate directories, or use --bundle flag
   • /downloads/map (copy).shp has invalid characters in filename
     → Suggest: Rename to map_copy.shp
   • /projects/old/ contains mix of raster and vector in same directory
     → Suggest: Separate by type

✓ Ready to Import (89 files):
   • 45 GeoParquet files
   • 32 COG files
   • 12 shapefiles (will convert to GeoParquet)

⏭️  Skipped (46 files):
   • 23 .xml sidecar files (metadata, will attach automatically)
   • 15 .dbf/.shx/.prj (shapefile components)
   • 8 unsupported formats (.gdb, .mxd)

Run `portolan scan /data/messy-drive/ --fix` to fix safe issues.
Run `portolan dataset add /data/messy-drive/` to import ready files.
```

### `portolan dataset add` (Enhanced)

```bash
portolan dataset add /data/              # Flat import (each file = dataset)
portolan dataset add /data/ --dry-run    # Preview what would happen
portolan dataset add /data/ --depth=2    # Map directory levels to STAC hierarchy
portolan dataset add /data/ --force      # Import despite warnings
```

**Key behavior:** `dataset add` calls `scan` internally and warns if issues are detected:

```
$ portolan dataset add /data/messy/

⚠️  Structure issues detected (run `portolan scan /data/messy/` for details):
   • 3 directories have multiple primary assets
   • 2 files have invalid characters

Proceeding with 47 of 52 importable files...
Use --force to import all, or fix issues first with `portolan scan --fix`
```

## File Discovery Rules

### Recognized Extensions

| Extension | Type | Notes |
|-----------|------|-------|
| `.parquet` | Vector | GeoParquet |
| `.geojson` | Vector | — |
| `.shp` | Vector | Requires sidecar files |
| `.gpkg` | Vector | GeoPackage |
| `.fgb` | Vector | FlatGeobuf |
| `.tif`, `.tiff` | Raster | GeoTIFF/COG |
| `.jp2` | Raster | JPEG2000 |

**Override:** `--include-ext=.gdb,.kml`

### Handling Rules

| Category | Default Behavior | Override Flag |
|----------|------------------|---------------|
| Hidden files (`.*`) | Skip silently | `--include-hidden` |
| Symlinks | Skip (avoids loops) | `--follow-symlinks` |
| Zero-byte files | Report as error, skip | — |
| Sidecar files (`.xml`, `.prj`, `.dbf`, `.shx`) | Auto-attach to parent | — |

### Recursion

| Flag | Behavior |
|------|----------|
| (default) | Recursive scan |
| `--no-recursive` | Only immediate children |
| `--max-depth=N` | Limit recursion depth |

## Structure Detection

### Issue Severity Levels

| Level | Meaning | Blocks Import? |
|-------|---------|----------------|
| Error | Cannot process file | Yes (that file) |
| Warning | Ambiguous structure | No (warns) |
| Info | Suggestion | No |

### Detected Issues

| Issue | Severity | `--fix` Action | `--unsafe-fix` Action |
|-------|----------|----------------|----------------------|
| Invalid characters in filename | Warning | Rename | Rename |
| Multiple primary assets in one dir | Warning | — | Split into subdirs |
| Mixed raster/vector in same dir | Info | — | Separate by type |
| Incomplete shapefile (missing .dbf) | Error | — | — |
| Very long paths (200+ chars) | Warning | Truncate/hash | Truncate/hash |
| Duplicate basenames across dirs | Info | — | Add path prefix to ID |
| Zero-byte file | Error | — | — |
| Symlink loop detected | Error | — | — |

### Asset Role Detection

Within a directory, files are classified by role:

| Role | Detection | Notes |
|------|-----------|-------|
| Primary data | `.parquet`, `.geojson`, `.gpkg`, `.fgb`, `.tif` | Max 1 per directory (warn if multiple) |
| Overview/preview | `*.pmtiles`, `*-overview.*` | Derivative of primary |
| Thumbnail | `thumbnail.*` | Preview image |
| Style | `style.json` | Mapbox/MapLibre style |
| Metadata | `metadata.*` | Auxiliary metadata |

**Multiple primary assets warning:**

```
⚠️  /census/2010/ has 3 primary assets:
    • tracts.parquet
    • blocks.parquet
    • counties.parquet

    This is usually a mistake. Consider:
    1. Split into separate directories (census/2010-tracts/, etc.)
    2. Use `portolan dataset add /census/2010/ --bundle` to treat as one dataset
```

## Depth-Based Hierarchy (`--depth`)

The `--depth=N` flag maps directory levels to STAC hierarchy:

```
--depth=2 with structure:
/data/
├── census/           # Level 1 → Subcatalog
│   ├── 2020/         # Level 2 → Collection
│   │   └── tracts.parquet  # Level 3+ → Assets
│   └── 2022/
└── imagery/
    └── sentinel/
```

| Level | Maps To |
|-------|---------|
| 0 | Catalog root |
| 1 | Subcatalog |
| 2 | Collection |
| 3+ | Assets within collection |

### Edge Cases

| Scenario | Behavior |
|----------|----------|
| File at wrong depth | Warn, place in nearest valid container |
| Deeper than `--depth` | Flatten into collection at depth N |
| Shallower than `--depth` | Create collection at actual depth |

## Manifest File (Escape Hatch)

For complex structures that don't fit conventions, users can create a manifest:

```yaml
# portolan-import.yaml
structure:
  census:
    type: subcatalog
    children:
      2020: { type: collection }
      2022: { type: collection }
  imagery:
    type: subcatalog
  boundaries:
    type: collection

ignore:
  - "*.tmp"
  - "old_backups/"
```

**Usage:**

```bash
portolan dataset add /data/ --manifest=portolan-import.yaml
```

## Performance Considerations

### Current Implementation

Uses `pathlib.rglob()` — 2-3x slower than `os.walk()` for large directories.

### Recommended Changes

1. **Switch to `os.walk()`** for scan implementation
2. **Lazy iteration** — yield files as found, don't collect all paths in memory
3. **Early termination** — `--max-depth` should stop recursion, not filter after
4. **Progress indicator** — Simple counter for large scans

### Benchmark Targets

| Directory Size | Target Scan Time |
|----------------|------------------|
| 1K files | < 1 second |
| 10K files | < 10 seconds |
| 100K files | < 2 minutes |

## Test Scenarios

### File Discovery

1. Directory with only hidden files → empty result, no error
2. Symlink loop (`a/ → b/`, `b/ → a/`) → doesn't hang, reports error
3. `.shp` with missing `.dbf` → warns about incomplete shapefile
4. Mixed valid/invalid files → processes valid, reports invalid
5. `--max-depth=1` with files at depth 3 → only finds depth-1 files
6. Zero-byte file → error on that file, continues with others

### Structure Detection

7. `tracts.parquet` + `blocks.parquet` in same dir → warns "multiple primary assets"
8. Filename with spaces/special chars → warns, `--fix` renames
9. Duplicate basenames (`a/data.parquet`, `b/data.parquet`) → unique IDs generated
10. Very long filename (200+ chars) → warns about potential truncation

### Depth Handling

11. `--depth=2` with mixed depths → appropriate warnings/placement
12. `--depth=3` but max actual depth is 1 → graceful handling

### Integration

13. `dataset add` calls `scan` and shows summary
14. `--force` bypasses warnings
15. `--dry-run` shows plan without executing

## Prior Art

### stac-cat-utils

[EOEPCA/stac-cat-utils](https://github.com/EOEPCA/stac-cat-utils) provides similar functionality:

**Patterns we're adopting:**
- `collection_paths`, `item_paths`, `ignore_paths` configuration
- Glob pattern support for path specification
- Fallback handling (try format-specific, fall back to generic)

**Where we differ:**
- CLI-first (they're API-only)
- Structure validation with warnings (they trust user config)
- `--fix` capability (they don't modify files)
- General-purpose (they're satellite-focused)

### Other Tools

| Tool | Relevance |
|------|-----------|
| [rio-stac](https://github.com/developmentseed/rio-stac) | Single-file STAC item creation |
| [stactools](https://github.com/stac-utils/stactools) | CLI patterns, format-specific packages |

## Implementation Phases

### Phase 1: `portolan scan` (MVP)

- Basic file discovery with extension filtering
- Issue detection (multiple primaries, invalid chars, etc.)
- Human-readable output
- `--json` flag for scripting

### Phase 2: `--fix` Support

- Safe fixes: rename invalid characters
- `--unsafe-fix`: move/split directories

### Phase 3: Enhanced `dataset add`

- Integration with `scan` (automatic pre-check)
- `--depth` flag for hierarchy mapping
- `--force` to bypass warnings

### Phase 4: Manifest Support

- YAML manifest parsing
- Full structure control for complex cases

## Open Questions

1. **Should `scan` create a suggested manifest?** — e.g., `portolan scan /data/ --suggest-manifest > import.yaml`
2. **Interactive mode?** — `portolan scan /data/ --interactive` to walk through decisions
3. **Remote paths?** — Should `scan` work on S3/GCS paths? (Probably Phase 2+)
