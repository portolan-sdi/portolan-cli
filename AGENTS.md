<!-- ops-sync:begin — synced from portolan-sdi/portolan-ops. Edit there, not here. -->
# Portolan agent norms

All agents MUST read this document before starting work, with no exceptions, ever. (Claude Code does not read `AGENTS.md` but imports this file via a one-line `CLAUDE.md`.) Most of this content is synced downstream from `portolan-ops`. Put repo-specific instructions in `AGENTS.md`, never in `CLAUDE.md`, which the sync overwrites.

## Ground rules

Agents MUST read the complete contents of [norms](https://github.com/portolan-sdi/portolan-ops/blob/main/norms) and [policies](https://github.com/portolan-sdi/portolan-ops/blob/main/policies/) before starting work, with no exceptions, ever.

The [portolan-spec](https://github.com/portolan-sdi/portolan-spec) repo is the absolute ground truth for the Portolan spec. All other tools are downstream of it.

Before documenting any command, flag, or API, verify that it exists.

Use conventional commits always.

All tickets and PRs MUST follow the [templates](https://github.com/portolan-sdi/portolan-ops/blob/main/templates/).

GitHub applies issue forms only in the web UI. With `gh issue create`, write each form field as a `### <field label>` heading, in the form's order. Include `### Priority` with one of `P0` to `P3` from the [priority rubric](https://github.com/portolan-sdi/portolan-ops/blob/main/norms/repos.md#priority). Pass the form's type label with `--label`, and pass the body with `--body-file`. The writing hook denies a body that skips its form, and issue governance flags one.

## Org-wide facts

The canonical homepage is <https://www.portolan-sdi.org/>. Canonical URLs live in [copy/urls.md](https://github.com/portolan-sdi/portolan-ops/blob/main/copy/urls.md). Do not hardcode variants.

Community discussion happens in the [Portolan Google Group](https://groups.google.com/g/portolan) and the [Portolan channel](https://cloudnativegeo.slack.com/archives/C0A1JBH9529) in Cloud-Native Geo Slack. Planning happens in [org-level GitHub projects](https://github.com/orgs/portolan-sdi/projects/1).

Files between `ops-sync` markers are synced from [portolan-ops](https://github.com/portolan-sdi/portolan-ops). They are overwritten on every sync run. To change one, edit it in portolan-ops.

One canonical home per fact. If a value like a color, URL, or policy line exists in portolan-ops, link to it rather than copying it.
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
| CI/CD | `.github/workflows/` and `prek.toml`, with rationale in comments |
| **Real-world test fixtures** | `tests/fixtures/realdata/README.md` |
| Maintainer rationale | Module docstrings and comments at the enforcing site |

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
uv run python scripts/affected_tests.py --run -n 4  # Run tests this change can affect
uv run pytest --cov-report=html         # Coverage report
uv run ruff check .                     # Lint
uv run ruff format .                    # Format
uv run mypy portolan_cli                # Type check
uv run deptry .                         # Check dependencies (unused, missing, transitive)
uv run vulture portolan_cli tests       # Dead code
uvx jscpd@5.2.1 portolan_cli tests \
  --baseline .jscpd-baseline.json \
  --fail-on-new-clones 0            # Duplicate code

# Iceberg backend development
uv sync --extra iceberg --extra dev     # Install with iceberg deps
uv run pytest tests/iceberg/ -m unit    # Run iceberg unit tests
uv run pytest tests/iceberg/ -m "not e2e and not e2e_slow"  # All iceberg tests (no Docker)

# Commits (use commitizen for conventional commits)
uv run cz commit                        # Interactive commit
uv run cz bump --dry-run                # Preview version bump

# Docs
uv run zensical serve                   # Local docs server
uv run zensical build --strict --clean  # Build docs
uv run pytest tests/docs/test_site_build.py  # Verify the built site
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
├── docs/                  # PUBLIC documentation (Zensical) - tutorials, user guides
└── .github/workflows/     # CI/CD pipelines
```

`docs/` is public (Zensical). Docstrings and comments next to the code record
the internal rationale. See `.claude/rules/documentation.md` for
where to file things.

## Synced brand files

The sync in portolan-ops owns these four files. They carry no `ops-sync` marker,
because a CSS file and an SVG file cannot hold a Markdown comment in a place
that a reader sees. Do not edit them here. Edit the source in portolan-ops and
let the sync run.

- `docs/assets/stylesheets/_brand-vars.css`
- `docs/assets/images/portolan-logomark-4163cc.svg`
- `docs/assets/images/portolan-logomark-fcfcfa.svg`
- `docs/assets/images/portolan-logo-horizontal-light.svg`

Comments in `mkdocs.yml` and `docs/assets/stylesheets/extra.css` record how
the site applies them.

## Before Writing Code

Always research before implementing:

1. **Understand the request** — Ask clarifying questions if ambiguous
2. **Search for patterns** — Check if similar functionality exists
3. **Check utilities** — Review `portolan_cli/` first
4. **Review existing tests** — Look at tests for the area you're modifying
5. **Check the rationale** — Read the module docstrings and the relevant
   `.claude/rules/*.md` to understand past decisions

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
@pytest.mark.plugin      # Needs the example plugin installed (tests/plugins/portolan_example)
@pytest.mark.benchmark   # Performance measurement, tracked over time
@pytest.mark.slow        # Takes > 5 seconds
@pytest.mark.e2e         # End-to-end tests requiring Docker (REST catalog + RustFS)
@pytest.mark.e2e_slow    # Extended E2E tests (concurrency stress, large datasets) - nightly only
```

## Development Rules

- **ALL** code must have type annotations (`mypy --strict`)
- **ALL** new features require tests FIRST (TDD)
- **ALL** non-obvious decisions are recorded where they apply (see `.claude/rules/documentation.md`)
- **NO** new dependencies without discussion

<!-- freshness: last-verified: 2026-10-14 -->
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

Each upstream bug and accepted limit is recorded at its workaround, with the
upstream issue URL where one exists. The record is a dependency pin comment in
`pyproject.toml`, a comment or docstring at the workaround in `portolan_cli/`, a
`skipif` reason, or an `xfail(strict=True)` reason. A strict `xfail` fails CI once
upstream fixes the bug. List them with:

```bash
git grep -nE "xfail\(|skipif\(|upstream" -- \
  pyproject.toml portolan_cli tests
```
