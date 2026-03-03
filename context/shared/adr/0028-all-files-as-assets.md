# ADR-0028: Track All Files in Item Directories as Assets

## Status
Accepted

## Context

### Current Behavior

Portolan's scan pipeline (`scan.py`) currently tracks only files whose extensions
appear in `GEOSPATIAL_EXTENSIONS` (`.geojson`, `.parquet`, `.shp`, `.gpkg`,
`.fgb`, `.csv`, `.tif`, `.tiff`, `.jp2`, `.pmtiles`).  All other files are
silently skipped and placed in the `skipped` list of `ScanResult`.

A typical real-world item directory contains more than a single geo file:

```text
demographics/census-2020/
├── data.parquet          # primary GeoParquet asset (tracked)
├── codebook.pdf          # documentation (skipped)
├── thumbnail.png         # preview image (skipped)
├── LICENSE.txt           # licensing terms (skipped)
└── metadata.json         # supplemental metadata (skipped)
```

STAC fully supports non-geospatial assets (`"type": "application/pdf"`, etc.).
Portolan's narrow scan means these companion files are invisible to version
tracking, checksumming, and sync—a significant gap in catalog completeness.

### Issue

GitHub issue #133 requires tracking ALL files in item directories as assets so
that:

1. Checksums are computed for every file (documentation, thumbnails, licenses).
2. `versions.json` accurately represents the full on-disk state.
3. Files removed between versions are detected and surfaced in diffs.
4. The catalog is a complete, self-describing archive.

### Forces

- **Asset breadth vs. noise**: System-generated junk (`.DS_Store`, `Thumbs.db`,
  `*.tmp`) should not appear in catalogs.
- **STAC compatibility**: STAC items are permitted to hold assets of any MIME
  type—there is no spec-level restriction to geospatial files.
- **Backward compatibility**: Existing catalogs were built tracking only
  geospatial files; changing the scan scope is a breaking change for those catalogs.
- **User control**: What counts as "noise" is subjective; macOS users may not
  generate `Thumbs.db` and vice-versa. A hard-coded exclusion list cannot cover
  all cases.
- **ADR-0010**: Format conversion/validation is delegated to upstream libraries.
  The decision here is purely about *which* files to track, not how to convert
  them.

## Decision

**Track ALL files within item directories as STAC assets, with a configurable
exclusion list (`ignored_files`) to filter out system junk.**

### Scope of Tracking

The scan pipeline will include every file in an item directory as a `ScannedFile`
with an appropriate `FormatType` (`VECTOR`, `RASTER`, or the new `OTHER`
category for non-geospatial files).

Files matching any pattern in `ignored_files` are silently skipped and never
appear as assets.

### `ignored_files` Config Key

A new `ignored_files` key is added to the hierarchical config system (ADR-0024).
It holds a list of glob patterns:

```yaml
# .portolan/config.yaml
ignored_files:
  - ".DS_Store"
  - "Thumbs.db"
  - "desktop.ini"
  - "*.tmp"
  - "*.temp"
  - "~*"
  - ".git*"
  - "*.pyc"
  - "__pycache__"
```

If `ignored_files` is absent from config, `DEFAULT_IGNORED_FILES` (defined in
`portolan_cli/config.py`) applies.  A user-supplied list **replaces** the
defaults entirely — there is no merge.  Users who want to extend the defaults
should copy the default list and add their patterns.

The matching is performed with Python's `fnmatch` (filename-only matching, not
full-path matching).  This is consistent with `.gitignore` filename patterns and
avoids the complexity of path-based glob matching.

### API

```python
from portolan_cli.config import get_ignored_files, DEFAULT_IGNORED_FILES

# Returns DEFAULT_IGNORED_FILES if no config or key absent
patterns: list[str] = get_ignored_files(catalog_path)

# Example usage in scan pipeline
import fnmatch
def is_ignored(filename: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(filename, p) for p in patterns)
```

### Backward Compatibility

This is a **breaking change** for existing catalogs:

- Previously scanned catalogs tracked only geospatial files.
- After this change, a re-scan will surface companion files as new additions.
- `versions.json` diffs will show these files as newly added (not as deletions
  of old content).
- **Migration path**: Re-run `portolan scan` after upgrading.  The first scan
  post-upgrade will detect the additional files as new assets.  No data is lost.

Portolan is pre-v1.0 with a small user base; breaking changes of this nature
are acceptable per established project norms (see ADR-0027 precedent).

## Consequences

### Benefits

- **Complete version tracking**: Checksums capture the full item directory state.
- **STAC compliance**: Catalogs accurately reflect all item assets.
- **Auditable history**: File additions, removals, and changes are visible in
  `versions.json` regardless of file type.
- **User trust**: Nothing in an item directory silently disappears from the
  catalog.

### Trade-offs

- **Increased catalog size**: More assets per item means larger `collection.json`
  and `versions.json` files.
- **Breaking change**: Existing catalogs require a re-scan to pick up companion
  files.
- **MIME-type inference**: Non-geospatial files need MIME types inferred from
  extension for STAC asset objects.  This is handled via Python's `mimetypes`
  stdlib module.
- **Replace-not-merge default semantics**: A user who sets `ignored_files` must
  explicitly include all patterns they want to exclude.  This is intentional
  (predictable) but may surprise users who expect additive behavior.

## Alternatives Considered

### Alternative A: Opt-in via `include_all_files` flag
Have an explicit `--all-files` scan option rather than changing the default.

**Rejected**: The goal of issue #133 is *complete* tracking.  An opt-in flag
would leave the incomplete behavior as the default, requiring users to remember
the flag on every scan.  Changing the default is the only way to guarantee
complete catalogs.

### Alternative B: Merge user patterns with defaults
Provide a separate `extra_ignored_files` key that extends `DEFAULT_IGNORED_FILES`
rather than replacing it.

**Rejected**: Two keys (`ignored_files` + `extra_ignored_files`) create
confusion.  Replace semantics are simpler to reason about and consistent with
how similar tools (ruff, eslint) handle user-supplied exclusion lists.

### Alternative C: Path-based glob matching (gitignore style)
Use full-path patterns rather than filename-only patterns.

**Rejected**: Filename-only `fnmatch` is simpler to explain and sufficient for
the common junk-file use case.  Full-path patterns are a future extension if
needed.

### Alternative D: Hard-code the exclusion list, no user config
Keep `DEFAULT_IGNORED_FILES` as an immutable constant with no user override.

**Rejected**: What counts as junk is environment-dependent.  A CI pipeline
may need to ignore `.pytest_cache/`; a Windows user may not encounter `.DS_Store`
at all.  User control is necessary.

## References

- [ADR-0024: Hierarchical Config System](0024-hierarchical-config-system.md)
- [ADR-0022: Git-Style Implicit Tracking](0022-git-style-implicit-tracking.md)
- [ADR-0010: Delegate Conversion/Validation](0010-delegate-conversion-validation.md)
- [ADR-0005: versions.json as Source of Truth](0005-versions-json-source-of-truth.md)
- [GitHub Issue #133](https://github.com/portolan-sdi/portolan-cli/issues/133)
