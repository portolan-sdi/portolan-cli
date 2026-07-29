<!-- ops-sync:begin — synced from portolan-sdi/portolan-ops. Edit there, not here. -->
# Portolan agent norms

Canonical rules for AI agents working in any portolan-sdi repo. Downstream repos carry this text verbatim as a synced block at the top of their own `AGENTS.md`, so the rules are in context rather than a link away. Repo-specific instructions live below that block. When a repo-specific rule conflicts with this file, the repo-specific rule wins for that repo.

Claude Code does not read `AGENTS.md`. Each repo therefore carries a one-line `CLAUDE.md` that imports it. Put repo-specific instructions in `AGENTS.md`, never in `CLAUDE.md`, which the sync overwrites.

## Voice and prose

- All collective public-facing copy (website, announcements, docs, presentations) follows [VOICE.md](https://github.com/portolan-sdi/portolan-ops/blob/main/VOICE.md). Read it before writing any of those.
- How Portolan is described comes from [copy/messaging.md](https://github.com/portolan-sdi/portolan-ops/blob/main/copy/messaging.md) alone. That file is provisional but authoritative: it distills the working messaging document and wins over any older copy anywhere in the org. Never describe Portolan from memory or from copy that predates it.
- All written artifacts (READMEs, PR and issue bodies, docs, commit message bodies, lasting code comments) follow [STYLE.md](https://github.com/portolan-sdi/portolan-ops/blob/main/STYLE.md). Apply it while drafting, not as a cleanup pass.
- Both are mandatory. "Agents MUST abide" is the operative phrase in each.

## Writing issues and pull requests

A reviewer should finish a pull request body in under a minute and know what changed, why, and that it works. Two rules make that possible, and CI checks both on every push and edit.

- **200 words outside code blocks, no section longer than six lines.** Fenced blocks are uncapped, so evidence never competes with the budget. Say the thing once. Do not restate the diff, do not summarize your own summary, and do not explain the approach at a level the code already shows.
- **Show that it works on real data.** Paste the command and the output you got, and name the data it read: a URL or a catalog path. Green tests are not verification. A change that alters no behavior waives this by ticking the waiver checkbox in the template.

Issues carry the same budget. A bug report needs the reproduction that triggered it, a feature request needs the transcript showing where current behavior falls short, and a task needs the command that will prove it done. Every repo runs these forms, and blank issues are off.

The check fails the pull request. On an issue it applies `needs-rewrite` and comments once.

## Documentation

Agents writing or restructuring documentation, READMEs above all, MUST follow the two guidance sources named in [norms/docs.md](https://github.com/portolan-sdi/portolan-ops/blob/main/norms/docs.md):

1. **[obstore](https://github.com/developmentseed/obstore)** is the exemplar. Before drafting, fetch and study its README and docs layout. Match its shape: what belongs on a landing page, how quick-start is separated from deep documentation and API reference, how much each layer says.
2. **[scaffold-docs-skill](https://github.com/dbreunig/scaffold-docs-skill)** is the method. Draft top-down in layers: section structure first, then headers, then topic sentences, then paragraphs, pausing for human review between layers rather than emitting finished pages in one pass.

Do not draft a README from a generic template or from memory of "what READMEs look like." Consult both sources first, every time.

## Org-wide facts

- License is Apache-2.0 in every repo. Never introduce code under another license without a human decision recorded in `norms/repos.md`.
- The canonical homepage is https://www.portolan-sdi.org/. Canonical URLs live in [copy/urls.md](https://github.com/portolan-sdi/portolan-ops/blob/main/copy/urls.md). Do not hardcode variants.
- Community discussion happens in the [Portolan Google Group](https://groups.google.com/g/portolan) and the [Portolan channel](https://cloudnativegeo.slack.com/archives/C0A1JBH9529) in the Cloud-Native Geo Slack. Planning lives in [org-level GitHub projects](https://github.com/orgs/portolan-sdi/projects/1).
- The [portolan-spec](https://github.com/portolan-sdi/portolan-spec) repo is the ground truth for the Portolan standard. The CLI, the validator, the registry, and every other tool implement the spec and are downstream of it. Never describe the CLI as the source of truth for the spec. Propose spec changes in portolan-spec.

## Contribution rules

- The [AI policy](https://github.com/portolan-sdi/portolan-ops/blob/main/policies/AI_POLICY.md) applies to every contribution. A human must have read, reviewed, and understood any change before review is requested. Agents never open PRs, post comments, or take action in shared spaces without human approval.
- Follow the [contributing guide](https://github.com/portolan-sdi/portolan-ops/blob/main/policies/CONTRIBUTING.md) and the [code of conduct](https://github.com/portolan-sdi/portolan-ops/blob/main/policies/CODE_OF_CONDUCT.md).
- Conventional commits. Squash-merge means the PR title is the commit message. Write it in conventional form.
- Never bypass pre-commit hooks or CI gates. Green means green.

## Ground truth discipline

- One canonical home per fact. Link, don't duplicate. If a value (a color, a URL, a policy line) exists in this repo, reference it rather than copying it.
- Shared files reach downstream repos through `sync/manifest.yml` and the sync workflow, never by hand-copying. To change a synced file in a downstream repo, change it here.
- Brand values come from `brand/brand.json`. Regenerate derived files (`brand/emit_css.py`) rather than editing them.
<!-- ops-sync:end -->

# Portolan CLI - Development Guide

## What is Portolan?

Portolan is a CLI for publishing and managing **cloud-native geospatial data catalogs**. It orchestrates format conversion (GeoParquet, COG), versioning, and sync to object storage (S3, GCS, Azure)—no running servers, just static files.

**Key concepts:**
- **STAC** (SpatioTemporal Asset Catalog) — The catalog metadata spec
- **GeoParquet** — Cloud-optimized vector data (columnar, spatial indexing)
- **COG** (Cloud-Optimized GeoTIFF) — Cloud-optimized raster data (HTTP range requests)
- **versions.json** — Single source of truth for version history, sync state, and checksums

Portolan doesn't do the heavy lifting—it orchestrates libraries like `geoparquet-io` and `rio-cogeo`.

## Path-scoped rules

Detailed, context-specific guidance lives in `.claude/rules/*.md`. Each file
declares `paths:` globs and Claude Code loads it automatically when you open a
matching file, so this root keeps only always-relevant global facts. Update or
add a rule when a convention changes, the code moves, or a new subsystem needs
its own guidance.

## Terminology (ENFORCED)

**Use STAC terminology exclusively.** Do NOT use "dataset" — it's ambiguous and not part of the STAC spec.

| Term | Meaning | Example |
|------|---------|---------|
| **Catalog** | Root container with metadata | `catalog.json` at repo root |
| **Collection** | Group of related items | `demographics/collection.json` |
| **Item** | Single spatiotemporal entity | `demographics/census-2020/item.json` |
| **Asset** | Actual data file | `demographics/census-2020/data.parquet` |

**Correct:** "Add files to a collection", "Track items", "Push a collection"
**Wrong:** "Add a dataset", "Import datasets", "Dataset management"

## Guiding Principle

AI agents will write most of the code. Human review does not scale to match AI output volume. Therefore: every quality gate must be automated, every convention must be enforceable, and tests must be verified to actually test something.

## Quick Reference

| Resource | Location |
|----------|----------|
| **Roadmap** | [GitHub Issues](https://github.com/portolan-sdi/portolan-cli/issues?q=label%3Aroadmap%3Amvp%2Croadmap%3Anext%2Croadmap%3Afuture) |
| Contributing guide | `docs/contributing.md` |
| Architecture | `pyproject.toml` [tool.importlinter] + [ADR-0025](context/shared/adr/0025-architecture-as-code.md) |
| CI/CD documentation | `context/shared/documentation/ci.md` |
| **Real-world test fixtures** | `context/shared/documentation/test-fixtures.md` |
| ADRs | `context/shared/adr/` |
| Plans & research | `context/shared/` |

**Target Python version:** 3.10+ (matches geoparquet-io dependency)

**CLI entry point:** `portolan` → `portolan_cli:cli` (defined in pyproject.toml)

### ADR Index

| ADR | Decision |
|-----|----------|
| [0001](context/shared/adr/0001-agentic-first-development.md) | Agentic-first: automate all quality gates, TDD mandatory |
| [0002](context/shared/adr/0002-click-for-cli.md) | Click for CLI framework |
| [0003](context/shared/adr/0003-plugin-architecture.md) | Plugin architecture for formats (GeoParquet/COG core, others optional) |
| [0004](context/shared/adr/0004-iceberg-as-plugin.md) | ~~Iceberg as plugin~~ Superseded by ADR-0046 |
| [0005](context/shared/adr/0005-versions-json-source-of-truth.md) | versions.json as single source of truth |
| [0006](context/shared/adr/0006-remote-ownership-model.md) | Portolan owns bucket contents (no external edits) |
| [0007](context/shared/adr/0007-cli-wraps-api.md) | CLI wraps Python API (all logic in library layer) |
| [0008](context/shared/adr/0008-pipx-for-installation.md) | pipx for global installation, uv for development |
| [0009](context/shared/adr/0009-output-dry-run-and-verbose-modes.md) | Dry-run and verbose modes in output functions |
| [0010](context/shared/adr/0010-delegate-conversion-validation.md) | Delegate conversion/validation to upstream libraries |
| [0011](context/shared/adr/0011-mvp-validation-framework.md) | MVP validation framework for format handlers |
| [0012](context/shared/adr/0012-flat-catalog-hierarchy.md) | Flat catalog hierarchy (no nested collections) |
| [0013](context/shared/adr/0013-gitingest-auto-fetch.md) | Auto-fetch dependency docs via gitingest |
| [0014](context/shared/adr/0014-accept-non-cloud-native-formats.md) | Accept non-cloud-native formats with warnings |
| [0015](context/shared/adr/0015-two-tier-versioning-architecture.md) | Two-tier versioning: simple MVP + `[iceberg]` extra for enterprise |
| [0016](context/shared/adr/0016-scan-before-import.md) | Scan-before-import: separate validation from import (like ruff check/fix) |
| [0017](context/shared/adr/0017-mtime-heuristics-change-detection.md) | MTIME + heuristics for change detection (fast gate, O(1) metadata check) |
| [0018](context/shared/adr/0018-metadata-generation-tiers.md) | Metadata generation tiers: auto-extractable → derivable → defaults → human-enrichable |
| [0019](context/shared/adr/0019-cog-optimization-defaults.md) | COG defaults: DEFLATE, predictor=2, 512×512 tiles, nearest resampling |
| [0020](context/shared/adr/0020-conversion-output-location.md) | Conversion output: side-by-side for vectors, in-place for rasters |
| [0021](context/shared/adr/0021-catalog-json-root-level.md) | catalog.json at root level (STAC standard) |
| [0022](context/shared/adr/0022-git-style-implicit-tracking.md) | Git-style implicit tracking (subdir = collection, delete = untrack) |
| [0023](context/shared/adr/0023-stac-structure-separation.md) | STAC at root, Portolan internals in .portolan/ (supersedes 0012, 0021) |
| [0024](context/shared/adr/0024-hierarchical-config-system.md) | Hierarchical config system (YAML) |
| [0025](context/shared/adr/0025-architecture-as-code.md) | Architecture as code with import-linter |
| [0026](context/shared/adr/0026-conversion-config-design.md) | Conversion config: extension/path overrides, precedence rules |
| [0027](context/shared/adr/0027-unified-config-yaml-sentinel.md) | Unified config.yaml as sentinel and user config (eliminates config.json) |
| [0028](context/shared/adr/0028-all-files-as-assets.md) | Track ALL files in item directories as assets |
| [0029](context/shared/adr/0029-unified-catalog-root-detection.md) | Unified catalog root detection via .portolan/config.yaml |
| [0030](context/shared/adr/0030-agent-native-cli-design.md) | Agent-native CLI design with JSON output and input hardening |
| [0031](context/shared/adr/0031-collection-level-assets-for-vector-data.md) | Collection-level assets for vector data (GeoParquet, Shapefile, GeoPackage) |
| [0032](context/shared/adr/0032-nested-catalogs-with-flat-collections.md) | Nested catalogs with flat collections (supersedes ADR-0012) |
| [0033](context/shared/adr/0033-esri-gdb-raster-gdal-requirement.md) | ESRI GDB rasters require external GDAL (no bundled support) |
| [0034](context/shared/adr/0034-statistics-computation-defaults.md) | Stats: approx raster, PyArrow parquet, enabled by default, configurable |
| [0035](context/shared/adr/0035-temporal-extent-handling.md) | Temporal: default null (open interval), mark provisional, flag in check |
| [0036](context/shared/adr/0036-collection-summaries-strategy.md) | Summaries: hybrid field detection, categorical only, no numeric aggregation |
| [0037](context/shared/adr/0037-experimental-extension-policy.md) | Use experimental extensions, accept migration cost, no fallback prefixes |
| [0038](context/shared/adr/0038-metadata-yaml-enrichment.md) | metadata.yaml as human enrichment layer (supplements STAC, generates README) |
| [0039](context/shared/adr/0039-hierarchical-portolan-folders.md) | Hierarchical .portolan/ at collection/subcatalog levels |
| [0040](context/shared/adr/0040-unified-progress-output.md) | Progress + summary model: Rich progress bars, immediate errors, batched warnings |
| [0041](context/shared/adr/0041-stac-manifest-as-canonical-scan-source.md) | STAC manifest as canonical scan source for metadata_fresh; unifies check/--fix; adds ORPHANED status |
| [0042](context/shared/adr/0042-partition-stac-extension.md) | Standalone `partition:` STAC extension for Hive-style partitioned datasets |
| [0043](context/shared/adr/0043-style-and-thumbnail-architecture.md) | Style/thumbnail: inline in STAC, Mapbox GL spec, basemaps for vectors only |
| [0044](context/shared/adr/0044-consumption-guides-architecture.md) | Consumption guides: DuckDB + Python in README, skill for advanced cases |
| [0045](context/shared/adr/0045-styles-as-stac-assets.md) | Styles as standalone STAC assets (supersedes ADR-0043 style storage) |
| [0046](context/shared/adr/0046-iceberg-as-optional-extra.md) | Iceberg as optional `[iceberg]` extra, not separate package (supersedes 0004) |
| [0047](context/shared/adr/0047-non-geo-tabular-data-support.md) | Non-geo tabular data: opt-in support, GPIO routing, AOI inheritance |
| [0048](context/shared/adr/0048-cli-as-spec-source.md) | CLI repo as spec source of truth; portolan-spec becomes read-only mirror |
| [0049](context/shared/adr/0049-stac-geoparquet-scalability.md) | STAC-GeoParquet required for collections >1000 items |
| [0050](context/shared/adr/0050-pmtiles-visualization-requirement.md) | PMTiles required for vector datasets >100 MB |
| [0051](context/shared/adr/0051-self-contained-catalog-type.md) | SELF_CONTAINED catalog type (relative links, portable) |
| [0052](context/shared/adr/0052-llms-txt-requirement.md) | Require AGENTS.md for AI/LLM integration at catalog and collection levels (supersedes llms.txt); schema-required link + RULE-0080/0081 enforcement + generation |
| [0053](context/shared/adr/0053-mandatory-human-readable-titles.md) | Mandatory human-readable titles/descriptions; auto-humanize slugs; title on child/item links |
| [0054](context/shared/adr/0054-arcgis-folder-recursion-and-structure.md) | ArcGIS folder recursion (default-on), folder URLs, token auth pass-through, nested-folder subcatalogs |
| [0055](context/shared/adr/0055-extension-registry-single-source.md) | Single-source the recognized-extension vocabulary via a typed in-package registry (derives the formats/constants/scan_classify/add maps; drops dead `.raquet`) |
| [0056](context/shared/adr/0056-hermetic-shipped-schema-validation.md) | Validate compliance against the shipped STAC schemas via a vendored STAC v1.1.0 `$ref` closure + `referencing.Registry` (hermetic); `format` off pending href policy (#573); deletes inline stubs (#557) |

## Common Commands

```bash
# Environment setup
uv sync --all-extras                    # Install all dependencies
prek install                            # Install git hooks (requires: uv tool install prek)

# Development
uv run pytest                           # Run tests
uv run pytest -m unit                   # Run only unit tests
uv run pytest --cov-report=html         # Coverage report
uv run ruff check .                     # Lint
uv run ruff format .                    # Format
uv run mypy portolan_cli                # Type check
uv run deptry .                         # Check dependencies (unused, missing, transitive)
uv run vulture portolan_cli tests       # Dead code
uv run xenon --max-absolute=C portolan_cli  # Complexity
uv run pylint --disable=all --enable=duplicate-code portolan_cli/  # Duplicate code

# Iceberg backend development
uv sync --extra iceberg --extra dev     # Install with iceberg deps
uv run pytest tests/iceberg/ -m unit    # Run iceberg unit tests
uv run pytest tests/iceberg/ -m "not e2e and not e2e_slow"  # All iceberg tests (no Docker)

# Commits (use commitizen for conventional commits)
uv run cz commit                        # Interactive commit
uv run cz bump --dry-run                # Preview version bump

# Docs
uv run mkdocs serve                     # Local docs server
uv run mkdocs build                     # Build docs
```

## Project Structure

```
portolan-cli/
├── portolan_cli/          # Source code
│   └── backends/
│       ├── json_file.py   # MVP file-based backend (always available)
│       ├── protocol.py    # VersioningBackend protocol definition
│       └── iceberg/       # Iceberg backend (requires [iceberg] extra)
├── tests/                 # Test suite
│   ├── fixtures/          # Test data files
│   ├── specs/             # Human-written test specifications
│   ├── unit/              # Fast, isolated unit tests
│   ├── integration/       # Multi-component tests
│   ├── network/           # Tests requiring network (mocked locally)
│   ├── benchmark/         # Performance measurements
│   ├── snapshot/          # Snapshot tests
│   └── iceberg/           # Iceberg backend tests (unit, integration, e2e)
├── docs/                  # PUBLIC documentation (mkdocs) - tutorials, user guides
├── context/               # AI/INTERNAL development context
│   └── shared/            # Plans, research, reports
│       ├── adr/           # Architectural decisions
│       ├── documentation/ # CI, tooling docs
│       ├── plans/         # Architecture plans and design docs
│       └── known-issues/  # Tracked issues
└── .github/workflows/     # CI/CD pipelines
```

`docs/` is public (mkdocs); `context/` is internal AI-oriented context. See
`.claude/rules/documentation.md` for the full distinction and where to file things.

## Before Writing Code

Always research before implementing:

1. **Understand the request** — Ask clarifying questions if ambiguous
2. **Search for patterns** — Check if similar functionality exists
3. **Check utilities** — Review `portolan_cli/` first
4. **Review existing tests** — Look at tests for the area you're modifying
5. **Check ADRs** — Read `context/shared/adr/` to understand past decisions

## Testing

**TDD is MANDATORY** (write tests first). Full workflow and fixture rules:
`.claude/rules/testing.md`.

### Test Markers

```python
@pytest.mark.unit        # Fast, isolated, no I/O (< 100ms each)
@pytest.mark.integration # Multi-component, may touch filesystem
@pytest.mark.network     # Requires network (mocked locally, real in CI nightly)
@pytest.mark.realdata    # Uses real-world fixtures from tests/fixtures/realdata/ (tests orchestration, not geometry)
@pytest.mark.snapshot    # Compares output against golden files
@pytest.mark.benchmark   # Performance measurement, tracked over time
@pytest.mark.slow        # Takes > 5 seconds
@pytest.mark.e2e         # End-to-end tests requiring Docker (REST catalog + MinIO)
@pytest.mark.e2e_slow    # Extended E2E tests (concurrency stress, large datasets) - nightly only
```

## Development Rules

- **ALL** code must have type annotations (`mypy --strict`)
- **ALL** new features require tests FIRST (TDD)
- **ALL** non-obvious decisions require an ADR in `context/shared/adr/`
- **NO** new dependencies without discussion (document in ADR)

<!-- freshness: last-verified: 2026-07-29 -->
## Design Principles

| Principle | Meaning | ADR |
|-----------|---------|-----|
| **Don't duplicate** | Orchestrate libraries (geoparquet-io, rio-cogeo), never reimplement | — |
| **YAGNI** | No speculative features; complexity is expensive | — |
| **Interactive + automatable** | Every prompt has `--auto` fallback | — |
| **versions.json is truth** | Drives sync, validation, history | [ADR-0005](context/shared/adr/0005-versions-json-source-of-truth.md) |
| **Plugin interface early** | Handlers follow consistent interface for future plugins | [ADR-0003](context/shared/adr/0003-plugin-architecture.md) |
| **CLI wraps API** | All logic in library; CLI is thin Click layer | [ADR-0007](context/shared/adr/0007-cli-wraps-api.md) |
<!-- /freshness -->

## Known Issues

See `context/shared/known-issues/` for tracked issues. Key ones:

| Issue | Impact |
|-------|--------|
| [PyArrow v22+ ABI](context/shared/known-issues/pyarrow-abseil-abi.md) | Import failures on Ubuntu 22.04; pinned to `<22.0.0` |
| [geoparquet-io Windows segfault](context/shared/known-issues/geoparquet-io-windows-segfault.md) | Crashes on malformed input; test skipped on Windows |
| [geoparquet-io macOS abort](context/shared/known-issues/geoparquet-io-macos-abort.md) | Aborts on multilayer conversion; test skipped on macOS |
| [PySTAC absolute paths](context/shared/known-issues/pystac-absolute-paths.md) | Leaks local paths in output; use manual JSON construction |
| [DuckDB "Query interrupted" transient](context/shared/known-issues/duckdb-query-interrupted-transient.md) | Rare (~1/1000) transient interrupt during bulk conversion; bounded retry in `_convert_vector` |
