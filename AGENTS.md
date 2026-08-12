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

- License is Apache-2.0 in every repo. Never introduce code under another license without a human decision recorded in [norms/repos.md](https://github.com/portolan-sdi/portolan-ops/blob/main/norms/repos.md).
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
- Shared files reach downstream repos through [sync/manifest.yml](https://github.com/portolan-sdi/portolan-ops/blob/main/sync/manifest.yml) and the sync workflow, never by hand-copying. To change a synced file in a downstream repo, change it here.
- Brand values come from [brand/brand.json](https://github.com/portolan-sdi/portolan-ops/blob/main/brand/brand.json). Regenerate derived files ([brand/emit_css.py](https://github.com/portolan-sdi/portolan-ops/blob/main/brand/emit_css.py)) rather than editing them.
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
| Architecture | `pyproject.toml` [tool.importlinter] + `.claude/rules/architecture.md` |
| CI/CD documentation | `context/shared/documentation/ci.md` |
| **Real-world test fixtures** | `context/shared/documentation/test-fixtures.md` |
| Maintainer rationale | `context/shared/documentation/` |
| Plans & research | `context/shared/` |

**Target Python version:** 3.10+ (matches geoparquet-io dependency)

**CLI entry point:** `portolan` → `portolan_cli:cli` (defined in pyproject.toml)

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
│       ├── documentation/ # CI, tooling, maintainer rationale
│       └── known-issues/  # Tracked issues and environment constraints
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
5. **Check the rationale** — Read `context/shared/documentation/` and the
   relevant `.claude/rules/*.md` to understand past decisions

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
- **ALL** non-obvious decisions are recorded where they apply (see `.claude/rules/documentation.md`)
- **NO** new dependencies without discussion

<!-- freshness: last-verified: 2026-08-12 -->
## Design Principles

| Principle | Meaning |
|-----------|---------|
| **Don't duplicate** | Orchestrate libraries (geoparquet-io, rio-cogeo), never reimplement |
| **YAGNI** | No speculative features; complexity is expensive |
| **Interactive + automatable** | Every prompt has `--auto` fallback |
| **versions.json is truth** | Drives sync, validation, history |
| **Plugin interface early** | Handlers follow consistent interface for future plugins |
| **CLI wraps API** | All logic in library; CLI is thin Click layer |
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
| [ESRI GDB rasters](context/shared/known-issues/esri-gdb-rasters.md) | Unreadable without GDAL and undetected; `.gdb` routes to the vector pipeline |
