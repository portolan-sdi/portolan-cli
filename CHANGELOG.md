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
