---
paths:
  - "portolan_cli/scan/**"
  - "portolan_cli/validation/**"
  - "portolan_cli/clean.py"
  - "portolan_cli/metadata/**"
---

# Scan, check, and metadata freshness

Two related pipelines live here. `scan` discovers and classifies files and
reports issues (read-only by default). `check` validates the catalog, including
STAC metadata freshness, and with `--fix` reconciles it. They have a long bug
history around **reporting problems that `--fix` cannot resolve** and
**mistaking collection-level rollup assets for missing items**. The rules below
are what keep the two halves in agreement.

## The flow

```mermaid
flowchart TD
    SCLI["scan CLI (cli.py:1840)"] --> SDIR["scan_directory (scan/core.py:1367)"]
    SDIR --> DISC["_discover_files (scan/core.py:1070)"]
    DISC --> PROC["_process_file (scan/core.py:1180)"]
    PROC --> CLS{"classify_file (scan/classify.py:232)"}
    CLS -->|geo asset| READY[ready ScannedFile]
    CLS -->|tabular / sidecar / metadata / junk| SKIP[skipped]
    READY --> MULTI["multi-file checks (scan/core.py:1412)"]
    SKIP --> MULTI
    MULTI --> ISSUES[ScanIssue list]
    ISSUES --> SFIX{--fix?}
    SFIX -->|yes| SAFE["apply_safe_fixes (scan/fix.py:630): rename/sanitize only"]
    SFIX -->|no| SOUT[report]
    SAFE --> SOUT

    CCLI["check CLI (cli.py:1094)"] --> RUN["validation.runner.run_check (runner.py:91)"]
    RUN --> RASHID["rashid.validate: metadata + structural + data passes"]
    RUN --> GEO["scan.check.check_directory (scan/check.py:306), --geo-assets"]
    RASHID --> FIND["PTL-* findings"]
    GEO --> GSTAT{cloud-native status}
    FIND --> PAY["build_check_payload (validation/report.py:56)"]
    GSTAT --> PAY
    PAY --> CFIX{--fix?}
    CFIX -->|no| CREPORT[report + exit code]
    CFIX -->|yes| FIXR["cli._run_fix_and_repairs (cli.py:1570)"]
    FIXR --> AUTO["validation.fixers.apply_fixers: AUTO-bucket registry"]
    FIXR --> LEG["scan.check.run_fix_workflow (scan/check.py:572)"]
    LEG --> MSCAN["scan_catalog_metadata (metadata/scan.py:79)"]
    LEG --> FIXG[convert CONVERTIBLE only]
    MSCAN --> STAT{status per asset}
    STAT --> FRESH
    STAT --> MISSING
    STAT --> STALE[STALE or BREAKING]
    STAT --> ORPHANED
    FRESH --> FIXM["fix_metadata (metadata/fix.py:108)"]
    MISSING --> FIXM
    STALE --> FIXM
    ORPHANED --> FIXM
    AUTO --> RECHK["re-check once, annotate_survivors"]
    FIXM --> RECHK
    FIXG --> RECHK
    RECHK --> CREPORT
```

## `check` delegates conformance to rashid

`portolan_cli/validation/` is an adapter, not a rule engine. `run_check` builds
rashid's config, calls `rashid.validate`, and returns a `CheckOutcome` of plain
data; rule ids are rashid's `PTL-*`, shown verbatim. Do not reintroduce
Portolan-side conformance rules, the old `validation/rules.py` registry is gone.
What stays on our side is the half rashid cannot see: source files on disk that
are not yet catalog assets (`scan.check.check_directory`) and item freshness
against the filesystem (`run_fix_workflow`).

`--fix` is **check → fix → re-check exactly once**, never a loop
(`cli._execute_check_workflow`, cli.py:1387). The first check's findings pick fixers:
`validation.remediation` sorts each rule into AUTO / INSTRUCT / EXTERNAL, and
only AUTO rows name a `fixer` key in `validation.fixers.FIXERS`. A fixer takes
`(catalog_root, dry_run)` and sweeps the whole catalog, so findings decide
*whether* it runs, never *where* it looks. `annotate_survivors` then marks any
AUTO finding that outlived the re-check, which is what stops an agent from
calling `--fix` forever.

## The STAC manifest is the canonical scan source for freshness

`check --metadata` and `check --metadata --fix` MUST consume the **same**
`scan_catalog_metadata(catalog_path)` (`metadata/scan.py`). It walks the STAC
**manifest tree** (catalog -> collection.assets -> item dirs), not a filesystem
extension glob. Do not reintroduce a parallel filesystem-walk scanner, that is
exactly what produced the bug where `check` reported MISSING for files `--fix`
never saw, and `--fix` then said "already fresh" forever (issues #345, #384).

## The five statuses, and which are auto-fixable

`MetadataStatus` is FRESH / MISSING / STALE / BREAKING / ORPHANED.

- **FRESH**: registered, mtime unchanged or heuristics equal. `--fix` skips it.
- **MISSING**: registered in STAC but the file is gone, or an item dir whose data
  file exists but `item.json` does not. `--fix` creates the item. Counts as an
  ERROR.
- **STALE / BREAKING**: mtime changed and bbox/feature-count heuristics changed
  (STALE), or the schema fingerprint changed (BREAKING). `--fix` updates the item
  and versions tracking. BREAKING is an ERROR, STALE is part of the freshness
  message.
- **ORPHANED**: a data file on disk under a collection that is not registered in
  any manifest. Reported as a WARNING and **explicitly not auto-fixable**, `--fix`
  SKIPS it with a message rather than fabricating a wrong `item.json`.

The invariant: **`check` must never report an issue without saying who resolves
it.** On the freshness half it holds structurally, both paths read the same
`MetadataReport` and the non-fixable cases (ORPHANED, collection-level assets)
are reported but marked SKIPPED. On the rashid half the same job is done by the
remediation bucket: every finding is AUTO (a fixer owns it), INSTRUCT (the
output tells the operator what to write), or EXTERNAL (an optional dependency or
a hosting change). If you add a status or a fixer, preserve this, either make it
fixable or label the residue clearly in both halves.

## Collection-level assets are NOT items

A registered collection-level asset (e.g. `items.parquet`, or an single
vector file) has no companion `item.json`. Freshness-check it against
`versions.json` directly (`_check_collection_level_asset`), never route it
through the per-file item-JSON lookup, or it falsely reports MISSING. In
`fix_metadata`, STALE/BREAKING on a collection-level asset is SKIPPED with a
"re-run portolan add" message (regenerating it is `add`'s job, not `check`'s).

## mtime + heuristics fast gate

Freshness uses a cheap gate before any expensive hashing. stored mtime is None
means new. mtime unchanged means FRESH (fast path). mtime changed then compare
schema fingerprint (BREAKING) and bbox/feature-count heuristics (STALE), and if
those are equal it is touched-but-unchanged, so FRESH. The fast path requires
mtime within tolerance **and** size unchanged, a fast `convert + mv` otherwise
looks unchanged and gets wrongly skipped.

## scan `--fix` sanitizes names, it does NOT convert formats

`scan --fix` (`apply_safe_fixes`) only renames and sanitizes: lowercase, spaces
to dashes (including the extension), Windows reserved names get an underscore
prefix, long paths get hash-truncated, invalid collection ids get fixed. It is
gated to the FIX_FLAG set. **Format conversion is exclusively
`check --fix --geo-assets`** (vectors to GeoParquet, rasters to COG via
`convert_directory`, only `CONVERTIBLE` files). Keep these separate (
scan-before-import). Shapefile renames move all sidecars together with rollback.

## Classification and discovery gotchas

- A FileGDB `.gdb` directory is one asset, yielded whole and not recursed into.
- Shapefile sidecars (`.dbf`/`.shx`/`.prj`/...) are tracked then skipped, never
  imported directly. An incomplete shapefile (missing `.dbf`/`.shx`) is an ERROR.
- A `.parquet` with no `geo` schema-metadata key is tabular, not GeoParquet, skip
  it as `TABULAR_DATA`/`NOT_GEOSPATIAL` (the spec derives tabular from a geometry-less Parquet).
- An image under 1 MiB is a thumbnail, larger images are raster data.
- Catalogs reach 25k+ item dirs. Any per-directory or per-asset check must be
  O(n), the `_check_mixed_structure` O(n^2) bug hung for minutes on 27k dirs.
  Read bbox and counts from file metadata (O(1)), never parse geometry.
- Detect Hive partitions by the `key=value/` pattern and strip those segments
  before inferring a collection id (see `conversion-and-visualization.md`).

## `--geo-assets` is a different check

`check --geo-assets` (`check_directory`) classifies each geospatial file as
`CLOUD_NATIVE` / `CONVERTIBLE` / `UNSUPPORTED` via `get_cloud_native_status`.
This is "is each asset already cloud-native", distinct from the STAC validation
rules in `validation/`. `--remove-legacy` requires `--fix` and deletes only the
sources of successful conversions whose output exists, plus their sidecars.

## Where to investigate further

- `context/shared/documentation/catalog-layout-contract.md` for the layout the
  scanner assumes and the two boundaries it refuses to cross.
- `metadata/scan.py` module docstring, it spells out the MISSING/ORPHANED split.
- `portolan_cli/validation/`, the adapter in front of rashid, and
  rashid itself for the rules. Rule ids are rashid's `PTL-*`, shown verbatim;
  each cites the `PORTO-*` spec requirements it enforces.
