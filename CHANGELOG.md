## v1.0.0b0 (2026-08-31)

### BREAKING CHANGE

- --strict now fails the exit code on warnings. It used
to escalate a fixed subset of native rules to errors during validation;
rashid owns rule severity, and per-rule escalation is expressible as
`check.severity` in config.yaml.
- check --json replaces `results` with rashid `findings`
carrying PTL-* rule ids, and adds mode, spec_version, validator, and
counts_by_remediation; --fix adds a fix section with fixers, applied, and
survivors.
- --strict now affects only the exit code. Findings keep the
severity the validator declares.
- the reported spec_version is derived from the schemas
rashid bundles. It is 0.1.0; the CLI previously claimed 0.1.1.

### Feat

- **convert**: generate conforming GeoParquet by default (#808)
- **spec**: mandate AGENTS.md (rel=agents) — enforce + generate it (#640)
- emit rel=pmtiles collection link via the web-map-links extension (#586)
- **extract**: add `portolan extract carto` for Carto SQL API sources (#492) (#535)
- **check**: enforce tabular collection rules (RULE-0090–0094) (#481) (#533)
- **wfs**: adopt gpio 1.3+ defaults (100K page_size, auto_tile) (#529) (#532)
- **check**: re-optimize valid COGs via --force, parallelize conversion (#530) (#531)
- **viz**: punchy data-aware matplotlib thumbnail floor (#518) (#525)
- **extract**: ArcGIS folder recursion, folder URLs, token auth (#493) (#499)
- **stac**: populate file:size and aggregate statistics on STAC assets (#501) (#517)
- **bbox**: comprehensive bbox validation for inf/nan and anti-meridian (#516) (#520)
- **extract**: extract styles from WMS/WFS and ESRI REST endpoints (#491)
- **stac**: require human-readable titles and descriptions (#502) (#509)
- **spec**: consolidate spec into CLI as single source of truth (#474)
- **ci**: add CLI→Spec schema sync workflow (#472)
- **partitioning**: support arbitrary Hive partition column names (#443) (#462)

### Fix

- **check**: --fix no longer rewrites untouched items or writes a second one (#841)
- **docs**: use the canonical mark and bring the docs site onto the brand (#831)
- apply the root metadata.yaml title to catalog.json (#815) (#829)
- **bbox**: keep an antimeridian crossing in the measured extent
- **bbox**: measure the collection extent from data, not the bbox rectangle
- **viz**: render thumbnails from the GeoParquet, PMTiles as fallback
- **convert**: repair shapefile encoding sidecars before conversion
- **crs**: scope the WGS84 range guard and cover the add path
- **crs**: fail fast when a bbox assumed WGS84 exceeds lon/lat range
- **formats**: list GeoPackage layers from gpkg_contents, features only
- **viz**: write loadable pmtiles:// style sources with the archive zoom range
- **pmtiles**: make --force-pmtiles generate tiles and drop gpio-pmtiles (#782)
- **ci**: skip mutation shards that hold no mutable code (#717)
- **ci**: run the checks on every pull request, not only those onto main (#697)
- **ci**: gate the nightly sweep on per-shard mutation baselines (#612) (#672)
- **ci**: filter mutmut by dotted mutant names, not file paths (#612) (#671)
- **ci**: repair mutation-testing baseline and bound the run (#612) (#664)
- **test**: stabilize 1000-file versioning stress fixtures (#663)
- **convert**: retry transient DuckDB interrupt; unflake nightly perf/slow jobs (#643)
- **push**: report returned push failures as errors in JSON mode (#627)
- **add**: scan collection-level assets in O(n), not O(n²) (#465) (#608)
- lint linked collections/items, not just root catalog (#604) (#607)
- check --metadata validates linked collections/items, not just the root (#543) (#605)
- collection-level asset freshness — check reports STALE that --fix/add cannot clear (#512) (#606)
- rm deletes the item dir by parent, not file stem (#602) (#603)
- rm href-matches item-level assets in versions.json; drop dead remove_item (#589) (#601)
- catalog-versions.json writer conforms to its own schema (#561) (#596)
- **push**: upload intermediate catalogs and refresh metadata on --force (#496, #547, #552) (#553)
- **readme**: prefix collection links with catalog dir name (#549) (#550)
- **readme**: use metadata.yaml title/description in READMEs (#534) (#537)
- **stac**: stop PMTiles 3857 from clobbering source CRS in proj:epsg (#488) (#528)
- **extract**: anchor WFS per-layer timeout to execution start (#526) (#527)
- **readme**: suppress technical-slug keyword badge wall (#515) (#524)
- **viz**: track generated thumbnails in versions.json (#519) (#523)
- **stac**: stop is_technical_name clobbering single-word titles (#513) (#522)
- **extract**: enforce WFS timeout and show error details inline (#510)
- **check**: tolerate remote/extension assets and unknown STAC extensions (#460)
- **thumbnail**: apply plot-before-basemap fix to PMTiles path (#468)
- **spec**: add complete spec content from portolan-spec (#475)
- **benchmark**: use minimal valid TIFF/Parquet fixtures to avoid error paths (#467)
- **deps**: upgrade starlette 1.0.0 → 1.2.1 (PYSEC-2026-161) (#466)
- **ci**: separate security gate from notification (#457)

### Refactor

- **bbox**: split the measurement to stay under the complexity ceiling
- rename the validation extraction seam from reis to rashid (#676)
- rename the validation extraction seam from reis to rashid (#675)
- group flat root modules into scan/ sync/ viz/ subpackages (#625) (#641)
- **add**: extract finalization.py + split add_files into explicit phases (#624) (#639)
- **add**: extract preparation.py (prepare_item + conversion routing) (#623) (#637)
- **extract**: add common orchestrator base for arcgis/wfs/carto lifecycle (#622) (#636)
- **extract**: hoist ExtractionProgress/emit_progress into extract/common/progress.py (#635)
- **cli**: move post-add and check orchestration helpers into library (#620) (#628)
- **cli**: collapse duplicated JSON envelope blocks into emit helpers (#619) (#626)
- single-source the recognized-extension vocabulary (#558); drop dead .raquet (#487) (#593)
- restore reis seam — main is un-committable (import-linter) (#594)
- sweep "dataset" terminology and decompose the add pipeline (#560, #567) (#588)
- **skills**: extract skills to external repository (#511)
- **spec**: consolidate DECISIONS.md into CLI ADRs (#476)

### Perf

- **ci**: halve the CI test-matrix wall time (#845)
- prune FileGDB subtrees during discovery traversal (#590) (#600)

## v1.0.0a0 (2026-06-03)

### BREAKING CHANGE

- `metadata init`, `metadata validate`, and `readme`
are now recursive by default, matching `scan` behavior.

### Feat

- **add**: merge strategy for preserving human-authored metadata (#452)
- **add**: non-geo tabular data support (#432) (#449)
- **cli**: make metadata and readme commands recursive by default (#442)
- **backends**: merge portolake into portolan-cli as [iceberg] extra (#342)
- **readme**: make collections list collapsible for large catalogs (#424) (#429)
- **skills**: add bootstrap skill for end-to-end catalog creation (#425)
- **skills**: add consume skill for querying Portolan catalogs (closes #121) (#421)
- **style**: register style assets in collection.json
- **style**: add discover_styles and build_styles_manifest
- **style**: add build_full_style, write_style_file, and write_default_style functions
- **check**: add STAC schema and lint validation rules (#397) (#419)

### Fix

- **add**: correct incremental add asset accounting bugs (#454)
- **scan**: strip Hive partition dirs from collection ID inference (#453)
- **extract**: gracefully handle empty WFS layers (#450) (#451)
- **stac**: place raster bands on data asset per STAC v1.1.0 (#441)
- renumber styles ADR to 0045 to resolve conflict with consumption guides, increase test coverage
- **push**: sync all catalog files to remote (closes #426) (#430)
- **thumbnail**: resolve CRS mismatch and improve large file performance (#427)
- **style**: address review findings for styles-as-stac-assets
- **scan**: classify files in styles/ directories as style metadata
- **deps**: remove git dependency from optional-deps for PyPI compatibility (#418)

### Refactor

- **style**: remove pmtiles:style inline approach

### Perf

- **iceberg**: read table:row_count from snapshot metadata (#440)

## v0.7.0 (2026-05-07)

### Feat

- **style**: add Mapbox GL style and thumbnail generation for vector/raster assets (closes #13) (#414)
- **partition**: add STAC partition extension support (#413)
- **version**: add git-like version management UX (closes #389) (#406)
- **convert**: add configurable vector spatial optimization via geoparquet-io (#404)
- **readme**: add --verbose flag for detailed output (#403)
- **add**: add --force flag for re-tracking files (closes #386) (#400)
- **partition**: add spatial partitioning for large GeoParquet files (#399)
- **stac**: normalize asset keys and titles for well-known roles (#375)
- **convert**: auto-generate JPEG thumbnail for COG items (#373)
- **extract**: add CSW/ISO 19139 metadata seeding for WFS (#366)
- **extract**: add WFS extraction support (#363)
- **extract**: flatten single-layer ArcGIS services (Issue #358) (#362)
- **pmtiles**: add PMTiles generation from GeoParquet (#115) (#346)
- **skills**: Add Source Cooperative upload skill (#331)
- Metadata extractor abstraction design (#312, #316) (#328)
- **stac-geoparquet**: Generate items.parquet for large collections (#319) (#327)
- **progress**: Unified progress output model (ADR-0040) (#320)
- **async**: Complete async migration for push/pull operations (#321)
- Integrate Iceberg backend plugin routing and remote mode (#302)
- **imageserver**: add ArcGIS ImageServer raster extraction support (#308)
- **metadata**: add data defaults support in metadata.yaml (#310)
- **metadata**: add --recursive flag to metadata init (#299)
- **convert**: add multi-layer format support for GeoPackage and FileGDB (#287)
- **push**: add live progress bar with upload speed (#284)
- **extract**: add portolan extract arcgis command (#288)
- **convert**: make COG conversion settings configurable via config.yaml (#283)
- metadata.yaml enrichment, README generation, batch versioning, S3 fixes (#278)
- **stac**: implement Wave 2 extended STAC metadata & statistics (#277)
- **stac**: implement STAC extension foundation (Wave 1) (#273)
- **deps**: upgrade menard to 0.3.0 with brevity support (#266)
- **ci**: add menard for documentation drift detection (#264)
- **deps**: add deptry for dependency management (#263)
- add GitHub PR and issue templates from geoparquet-io (#260)
- **push**: upload STAC metadata files for full catalog sync (#254)

### Fix

- **crs**: skip mismatch validation for geographic CRSes (#415)
- **hooks**: restore SKILL.md accidentally deleted in PR #399 (#411)
- **init**: add trailing slash to pystac normalize_hrefs (fixes #401) (#405)
- **partition**: correct path mismatch in versions.json and glob patterns (#402)
- Collection-level assets for single-file vectors (#350, #383) (#398)
- **check**: unify metadata scan via STAC manifest (closes #345, #384) (#396)
- **add**: detect file changes within mtime tolerance (#394)
- **pmtiles**: clean up partial output files on generation failure (#393)
- **deps**: update geoparquet-io to v1.1.1 for bbox coordinate fix (#392)
- **ci**: update nightly workflow for mutmut 3.x API (#391)
- **config**: resolve dotted keys from nested YAML (#382)
- **extract**: clean collection names and filter boilerplate (#381)
- **extract**: propagate rich metadata to STAC from WFS/ArcGIS (#371)
- **add**: support PMTiles and FlatGeobuf without conversion (#370)
- **stac**: collection-level vector assets skip item.json (#365)
- **extract**: add via link provenance and fix asset path doubling (#361)
- **push**: upload root README.md and versions.json when pushing all collections (Issue #357) (#360)
- **config**: move credentials to env vars (Issue #356) (#359)
- **push**: conservative concurrency defaults (Issue #344) (#348)
- **versions**: implement catalog-level versioning with stress tests (fixes #339) (#347)
- **imageserver**: improve UX for bbox, tile size, and error handling (#338)
- **stac**: declare raster extension and aggregate collection extensions (#337)
- Clean up dead code, duplicates, and doc/code mismatches (#333)
- sync/push/pull improvements (#323, #324, #325, #329) (#330)
- **stac**: use STAC_VERSION constant and wire up Table extension (#307)
- **info**: handle catalog root and subcatalogs when using dot path (#298)
- **deps**: add pygments CVE-2026-4539 security constraint (#297)
- **deps**: resolve security audit failures (#286)
- **add**: format-aware collection inference per ADR-0031 (#269)
- **add**: treat FileGDB directories as data assets for collection ID inference (#262)
- **scan**: detect GeoJSON content in .json files (#261)
- **push**: recursive collection discovery for nested catalogs (#255)
- **dataset**: correct asset paths for collection-level assets (#253)

### Refactor

- **hooks**: migrate from pre-commit to prek (#408)
- remove global AI tool config from project (#334)
- **catalog**: remove state.json sentinel - config.yaml alone is sufficient (#301)

### Perf

- **tests**: enable parallel test execution with pytest-xdist (closes #410) (#412)
- **scan**: Fix O(n²) performance in _check_mixed_structure (#314) (#326)

## v0.6.0 (2026-03-19)

### BREAKING CHANGE

- Collection IDs now use nested paths (e.g., "climate/hittekaart")

### Feat

- **push**: add parallel push with --workers flag (#244)
- **cli**: git-style command scoping for list, push, pull (#243)
- **scan**: nested catalog support with structure recommendations (#242)
- nested catalog support (#237) (#240)
- **push**: add catalog-wide push without --collection flag (#228)
- **cli**: agent-native improvements with JSON output and input hardening (#223)

### Fix

- **cli**: default AWS profile to 'default' for S3 commands (#227)
- **output**: improve detail text readability across terminal themes (#219)

### Refactor

- **catalog**: remove dead write/read_catalog_json helpers (#217)

## v0.5.0 (2026-03-08)

### Feat

- **cli**: unify list and status commands (#215)
- **cli**: remove deprecated dataset command group (#214)
- **cli**: add file-level progress output for add and check --fix (#213)
- **check**: add --remove-legacy flag to delete source files after conversion (#212)

### Fix

- **scan**: normalize filenames to lowercase with dashes (#211)

## v0.4.4 (2026-03-07)

### Feat

- **scan**: batch and summarize repeated warnings (#194)
- **add**: accept multiple paths like git add (#189)
- **add**: support tabular parquet as auxiliary assets (#190)

### Fix

- **list**: show all assets grouped by item (#196) (#204)
- **add**: batch repetitive failures by error message (#199) (#205)
- **add**: accept PMTiles as cloud-native primary asset (#201)
- **status**: treat FileGDB directories as single assets (#187)
- **status**: detect and skip symlink cycles (#188)
- **add**: continue on errors and report all failures at end (#191)
- **scan**: list specific unrecognized files instead of just count (#185)
- **scan**: make PATH argument optional, default to current directory (#184)
- **scan**: remove non-existent --bundle flag reference from suggestion (#183)

## v0.4.3 (2026-03-07)

### BREAKING CHANGE

- Removes --fix-metadata flag; use --metadata --fix instead.

### Feat

- **cli**: add portolan clean command to remove metadata (#172)
- **cli**: add --item-id flag to portolan add command (#171)
- **check**: redesign --fix to work orthogonally with --metadata/--geo-assets (#164)

### Fix

- **dataset**: make add_dataset atomic and track files in-place (#170)
- **catalog**: unify catalog root detection (#162) (#169)
- **status**: detect untracked files in uninitialized collections (#167)
- **dry-run**: prevent network calls in dry-run mode (#168)
- **add**: support recursive add at catalog root (#166)
- **list**: add guidance when no items found (#165)

## v0.4.2 (2026-03-06)

### Feat

- full FileGDB support for scan and add workflows (#157)
- **clone**: add git-style ergonomics to clone command (#156)
- **assets**: track ALL files in item directories, not just geo files (#135)
- **validation**: add collection ID validation and auto-fix (#132)
- **ci**: add duplicate code detection with pylint R0801 (#130)
- **config**: add conversion config for format handling overrides (#128)

### Fix

- detect FileGDB directories during scan (#153)
- warn and skip non-geospatial CSV files during add (#152)
- **cli**: remove contradictory dry-run output messages (#150)
- normalize trailing slashes in S3 URLs (#151)
- **versions**: merge assets with previous version for snapshot model (#149)
- **versions**: use catalog-root-relative hrefs in versions.json (#126)

### Refactor

- **catalog**: unify config.json and config.yaml as sentinel (ADR-0027) (#131)

## v0.4.1 (2026-02-27)

### BREAKING CHANGE

- Catalog structure changed. Collections now live at root
level, not inside `.portolan/collections/`.

### Feat

- Wave 1 documentation alignment (Phases 1-3) (#122)
- **cli**: promote add/rm to top-level commands (#106)
- **check**: add --metadata and --geo-assets flags (#105)
- **config**: add config command and hierarchical configuration system (#104)
- **sync**: add sync and clone commands for catalog synchronization (#93)
- **sync**: implement push and pull commands (#92)
- **backends**: wire JsonFileBackend versioning methods (#90)
- **download**: add download primitive for cloud object storage (#91)
- **upload**: port upload primitive from geoparquet-io (#46)
- **metadata**: implement check metadata handling (#87)
- **convert**: implement check --fix conversion workflow (#85)
- **init**: implement portolan init with state detection (#83)
- **tooling**: add grepai MCP integration for semantic code search (#79)
- **models**: implement STAC metadata models for catalog, collection, item, schema (#78)
- **scan**: implement --fix flag for safe auto-fixes (#76)
- **scan**: add --manual flag for tree-structured manual-resolution output (#72)
- add global --format=json output option (#70)
- **backends**: implement VersioningBackend protocol and plugin discovery (#71)
- **scan**: implement portolan scan command (Phase 1 MVP) (#63)
- **tests**: add directory scan fixtures for portolan scan command (#61)
- **tests**: consolidate fixtures, migrate to real-world data (#58)
- **tests**: add real-world test fixtures for orchestration testing (#55)

### Fix

- **scan**: add .parquet to GEO_ASSET_EXTENSIONS (#124)
- **catalog**: write versions.json to root per ADR-0023 (#123)
- **structure**: STAC at root level per ADR-0023 (#102)
- emit warnings for permission errors and broken symlinks in scan (#65)
- **ci**: add pythonpath to pytest config for scripts imports (#56)
- **ci**: enable workflow_dispatch for release recovery (#52)

## v0.4.0 (2026-02-09)

### Feat

- **validation**: PMTiles recommended, not required (#49)
- warn on non-cloud-native formats (#48)
- **workflow**: add speckit for specification-driven development (#47)
- **hooks**: add auto-fetch for core dependency docs via gitingest (#44)

### Fix

- **ci**: extract only project version from pyproject.toml (#45)

## v0.3.0 (2026-02-07)

### Feat

- **dataset**: implement dataset CRUD operations (#39)
- v0.4 - Metadata extraction and validation framework (#37)
- v0.3 format conversion foundation (#36)
- **output**: add dry-run and verbose modes to output functions (#32)
- **test**: add geospatial test fixtures for vector and raster formats (#31)

### Fix

- **ci**: add retry for Python install and suppress hypothesis flaky test warning (#40)
- **ci**: update codecov configuration with token and slug (#28)
- **ci**: update nightly workflow for mutmut 3.x API (#25)
- **ci**: use mutmut junitxml instead of non-existent --json flag (#23)
- **ci**: repair failing workflows with tag-based releases and placeholder tests (#22)
- **docs**: update GitHub organization from portolan to portolan-sdi (#21)

## v0.2.0 (2026-02-05)

### Feat

- **cli**: add `portolan init` command (#20)

## v0.1.3 (2026-02-05)

### Fix

- **docs**: use absolute GitHub URL for ADR link in roadmap

## v0.1.2 (2026-02-04)

### Fix

- **ci**: handle commitizen exit code 16 (NO_PATTERN_MAP)
- **ci**: use commit SHA for GitHub release target

## v0.1.1 (2026-02-04)

### Fix

- **ci**: add --yes flag to commitizen dry-run for first tag
- **ci**: handle commitizen NO_COMMITS_TO_BUMP exit code gracefully
