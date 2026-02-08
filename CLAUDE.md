# Portolan CLI - Development Guide

## What is Portolan?

Portolan is a CLI for publishing and managing **cloud-native geospatial data catalogs**. It orchestrates format conversion (GeoParquet, COG), versioning, and sync to object storage (S3, GCS, Azure)—no running servers, just static files.

**Key concepts:**
- **STAC** (SpatioTemporal Asset Catalog) — The catalog metadata spec
- **GeoParquet** — Cloud-optimized vector data (columnar, spatial indexing)
- **COG** (Cloud-Optimized GeoTIFF) — Cloud-optimized raster data (HTTP range requests)
- **versions.json** — Single source of truth for version history, sync state, and checksums

Portolan doesn't do the heavy lifting—it orchestrates libraries like `geoparquet-io` and `rio-cogeo`.

**Key dependencies (check these repos for API docs):**
- [geoparquet-io](https://github.com/geoparquet/geoparquet-io) — Vector format conversion
- [gpio-pmtiles](https://github.com/geoparquet-io/gpio-pmtiles) — PMTiles generation from GeoParquet
- [rio-cogeo](https://github.com/cogeotiff/rio-cogeo) — Raster conversion to COG

## Guiding Principle

AI agents will write most of the code. Human review does not scale to match AI output volume. Therefore: every quality gate must be automated, every convention must be enforceable, and tests must be verified to actually test something.

## Quick Reference

| Resource | Location |
|----------|----------|
| **Roadmap** | `ROADMAP.md` |
| Contributing guide | `docs/contributing.md` |
| Architecture | `context/architecture.md` |
| CI/CD documentation | `context/shared/documentation/ci.md` |
| Distill MCP tools | `context/shared/documentation/distill-mcp.md` |
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
| [0004](context/shared/adr/0004-iceberg-as-plugin.md) | Iceberg as plugin, STAC remains catalog layer |
| [0005](context/shared/adr/0005-versions-json-source-of-truth.md) | versions.json as single source of truth |
| [0006](context/shared/adr/0006-remote-ownership-model.md) | Portolan owns bucket contents (no external edits) |
| [0007](context/shared/adr/0007-cli-wraps-api.md) | CLI wraps Python API (all logic in library layer) |
| [0008](context/shared/adr/0008-pipx-for-installation.md) | pipx for global installation, uv for development |
| [0009](context/shared/adr/0009-output-dry-run-and-verbose-modes.md) | Dry-run and verbose modes in output functions |
| [0010](context/shared/adr/0010-delegate-conversion-validation.md) | Delegate conversion/validation to upstream libraries |
| [0011](context/shared/adr/0011-mvp-validation-framework.md) | MVP validation framework for format handlers |
| [0012](context/shared/adr/0012-flat-catalog-hierarchy.md) | Flat catalog hierarchy (no nested collections) |
| [0013](context/shared/adr/0013-gitingest-auto-fetch.md) | Auto-fetch dependency docs via gitingest |

## Common Commands

```bash
# Environment setup
uv sync --all-extras                    # Install all dependencies
uv run pre-commit install               # Install git hooks

# Development
uv run pytest                           # Run tests
uv run pytest -m unit                   # Run only unit tests
uv run pytest --cov-report=html         # Coverage report
uv run ruff check .                     # Lint
uv run ruff format .                    # Format
uv run mypy portolan_cli                # Type check
uv run vulture portolan_cli tests       # Dead code
uv run xenon --max-absolute=C portolan_cli  # Complexity

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
├── tests/                 # Test suite
│   ├── fixtures/          # Test data files
│   ├── specs/             # Human-written test specifications
│   ├── unit/              # Fast, isolated unit tests
│   ├── integration/       # Multi-component tests
│   ├── network/           # Tests requiring network (mocked locally)
│   ├── benchmark/         # Performance measurements
│   └── snapshot/          # Snapshot tests
├── docs/                  # Documentation (mkdocs)
├── context/               # AI development context
│   └── shared/            # Plans, research, reports
│       ├── adr/           # Architectural decisions
│       ├── documentation/ # CI, tooling docs
│       └── known-issues/  # Tracked issues
└── .github/workflows/     # CI/CD pipelines
```

## Before Writing Code

Always research before implementing:

1. **Understand the request** — Ask clarifying questions if ambiguous
2. **Search for patterns** — Check if similar functionality exists
3. **Check utilities** — Review `portolan_cli/` first
4. **Review existing tests** — Look at tests for the area you're modifying
5. **Check ADRs** — Read `context/shared/adr/` to understand past decisions

## Test-Driven Development (MANDATORY)

**YOU MUST USE TDD. NO EXCEPTIONS.** Unless the user explicitly says "skip tests":

1. **WRITE TESTS FIRST** — Before ANY implementation code
2. **RUN TESTS** — Verify they fail with `uv run pytest`
3. **IMPLEMENT** — Minimal code to pass tests
4. **RUN TESTS AGAIN** — Verify they pass
5. **ADD EDGE CASES** — Test error conditions

### Test Markers

```python
@pytest.mark.unit        # Fast, isolated, no I/O (< 100ms)
@pytest.mark.integration # Multi-component, may touch filesystem
@pytest.mark.network     # Requires network (mocked locally)
@pytest.mark.benchmark   # Performance measurement
@pytest.mark.slow        # Takes > 5 seconds
```

### Defending Against Tautological Tests

Three layers of defense (see `context/shared/documentation/ci.md` for details):

1. **Mutation testing** — Nightly `mutmut` runs verify tests catch real bugs
2. **Property-based testing** — Use `hypothesis` for invariant verification
3. **Human test specs** — `tests/specs/` defines what matters; AI implements

### Test Fixtures

Store small, representative data files in `tests/fixtures/`. Fixtures should be:

- **Small** — a few rows/pixels, enough to test behavior
- **Committed to git** — they're small enough, and reproducibility matters
- **Paired with invalid variants** — every valid fixture should have a corresponding invalid one
- **Documented** — each subdirectory gets a README.md

## CI Pipeline

**Source of truth:** `context/shared/documentation/ci.md`

| Tier | When | What |
|------|------|------|
| Tier 1 | Pre-commit | ruff, vulture, xenon, mypy, fast tests |
| Tier 2 | Every PR | lint, mypy, security, full tests, docs build |
| Tier 3 | Nightly | mutation testing, benchmarks, live network tests |

**All checks are strict** — no `continue-on-error`. Fix issues or they block.

### Pre-commit Hooks

Install: `uv run pre-commit install`. All hooks block—no `--no-verify`. See `.pre-commit-config.yaml` for full list.

## Code Quality

- **ruff** — Linting and formatting
- **mypy** — Type checking (`strict = true`)
- **vulture** — Dead code detection
- **xenon** — Complexity monitoring (max C function, B module, A average)
- **bandit** — Security scanning
- **pip-audit** — Dependency vulnerabilities

## Git Workflow

### Branch Naming

```
feature/description    # New features
fix/description        # Bug fixes
docs/description       # Documentation
refactor/description   # Code restructuring
```

### Conventional Commits

Use `uv run cz commit` for interactive commit creation:

```
feat(scope): add new feature      # Minor version bump
fix(scope): fix bug               # Patch version bump
docs(scope): update documentation
refactor(scope): restructure code
test(scope): add tests
BREAKING CHANGE: ...              # Major version bump
```

### Merge Policy

**Squash-merge** all PRs to main. This ensures:
- Clean history (one commit per PR)
- PR title becomes the commit message (enforce conventional format)
- Commitizen can analyze commits cleanly for versioning

### Release Automation

Portolan uses a **tag-based release workflow**. See `.github/workflows/release.yml`.

**To release:**
1. Create a PR that runs `uv run cz bump --changelog`
2. Merge the bump PR
3. Release workflow detects the bump commit and creates tag + publishes

**What happens automatically:**
1. Version extracted from `pyproject.toml`
2. Git tag created (e.g., `v0.3.0`)
3. Package built and published to PyPI
4. GitHub Release created

See `docs/contributing.md` for the full release process.

## Development Rules

- **ALL** code must have type annotations (`mypy --strict`)
- **ALL** new features require tests FIRST (TDD)
- **ALL** non-obvious decisions require an ADR in `context/shared/adr/`
- **NO** new dependencies without discussion (document in ADR)

## Documentation Bias

**Bias toward documenting everything.** AI agents work best with rich context.

### What to Document

| What | Where | When |
|------|-------|------|
| Architectural decisions | `context/shared/adr/` | Any non-obvious design choice |
| Known bugs/issues | `context/shared/known-issues/` | When a bug is identified but not yet fixed |
| Non-obvious code | Inline comments | Code that would confuse a future reader |
| API contracts | Docstrings | All public functions/classes |
| Gotchas/quirks | CLAUDE.md or inline | Anything that surprised you |

### ADR Guidelines

Create an ADR (`context/shared/adr/NNNN-title.md`) when:

- Choosing between multiple valid approaches
- Adopting a new dependency
- Establishing a pattern that others should follow
- Making a trade-off that isn't obvious

Use the template at `context/shared/adr/0000-template.md`.

### Two Documentation Audiences

| Audience | Location | Purpose |
|----------|----------|---------|
| **Humans** | `docs/` (mkdocs) | *How to use* — tutorials, visual guides |
| **AI agents** | Docstrings, CLAUDE.md, ADRs | *How to modify* — dense, structured, co-located with code |

### Validating AI Guidance

**When possible, back AI guidance with automated validation.** Documentation drifts; code doesn't lie.

If CLAUDE.md says "all ADRs must be listed in the index," enforce it with a script. If it says "use `output.py` for terminal messages," add a lint rule. The goal: make it impossible for guidance to become stale.

**Pattern:**
1. Write guidance in CLAUDE.md
2. Ask: "Can I validate this automatically?"
3. If yes, write a script in `scripts/` and add a pre-commit hook

**Example:** The ADR index in this file is validated by `scripts/validate_claude_md.py`:

```python
# Checks that all ADRs in context/shared/adr/ are listed in CLAUDE.md
missing = actual_adrs - linked_adrs
if missing:
    fail(f"ADRs not in CLAUDE.md index: {missing}")
```

This runs as a pre-commit hook—commits that add ADRs without updating CLAUDE.md are blocked.

**Validation scripts:**

| Script | Validates |
|--------|-----------|
| `scripts/validate_claude_md.py` | ADR index, known issues table, link validity |

When adding new guidance to CLAUDE.md, consider: can this be validated? If so, add a check.

## Standardized Terminal Output

Use `portolan_cli/output.py` for all user-facing messages:

```python
from portolan_cli.output import success, info, warn, error, detail

success("Wrote output.parquet (1.2 MB)")  # ✓ Green checkmark
info("Reading data.shp (4,231 features)")  # → Blue arrow
warn("Missing thumbnail (recommended)")    # ⚠ Yellow warning
error("No geometry column (required)")     # ✗ Red X
detail("Processing chunk 3/10...")         # Dimmed text
```

## Design Principles

| Principle | Meaning | ADR |
|-----------|---------|-----|
| **Don't duplicate** | Orchestrate libraries (geoparquet-io, rio-cogeo), never reimplement | — |
| **YAGNI** | No speculative features; complexity is expensive | — |
| **Interactive + automatable** | Every prompt has `--auto` fallback | — |
| **versions.json is truth** | Drives sync, validation, history | [ADR-0005](context/shared/adr/0005-versions-json-source-of-truth.md) |
| **Plugin interface early** | Handlers follow consistent interface for future plugins | [ADR-0003](context/shared/adr/0003-plugin-architecture.md) |
| **CLI wraps API** | All logic in library; CLI is thin Click layer | [ADR-0007](context/shared/adr/0007-cli-wraps-api.md) |

## Tool Usage

| Tool | Purpose | Documentation |
|------|---------|---------------|
| context7 | Up-to-date library docs (official API) | — |
| gitingest | Source code exploration (implementation details) | `https://github.com/cyclotruc/gitingest` |
| distill | Token-efficient operations | `context/shared/documentation/distill-mcp.md` |
| worktrunk | Worktree management | — |

### Dependency Research Workflow

**geoparquet-io** and **rio-cogeo** are core foundation libraries. When working with these, Claude should be proactive about checking actual implementation—not just API docs—when the tools are available.

For core dependencies (**geoparquet-io**, **rio-cogeo**, **gpio-pmtiles**), use this workflow:

1. **Official API** → Use Context7 first (up-to-date, authoritative)
   ```
   resolve-library-id("geoparquet-io") → query-docs(libraryId, "your question")
   ```

2. **Implementation details** → Use Gitingest to explore source (if available)
   - For geoparquet-io and rio-cogeo: check source when investigating edge cases or debugging
   - These are core libraries—understanding their actual behavior (not just API surface) helps catch subtle issues
   ```
   gitingest https://github.com/geoparquet/geoparquet-io
   gitingest https://github.com/cogeotiff/rio-cogeo
   # Copy output, paste into Claude for code-level analysis
   ```

3. **Large outputs** → Use Distill to compress (if available)
   ```
   mcp__distill__auto_optimize(gitingest_output, hint="code")
   # Reduces tokens by 50-70%
   ```

**Example**: "How does geoparquet-io handle missing geometry?"
- Step 1: Context7 → official API docs
- Step 2: Gitingest → search source for geometry validation (recommended for edge cases)
- Step 3: Distill → compress the source exploration for token efficiency

**When tools aren't available**: If gitingest isn't installed or MCP tools aren't configured:
- Use GitHub's web interface to browse source files directly
- Use `gh api` to fetch specific files from repos
- Clone the repo locally and use standard file reading
- WebFetch can retrieve raw GitHub file contents

**Claude's Responsibility**: When working with geoparquet-io or rio-cogeo:
- Don't assume API behavior without checking source (when tools permit)
- Search for edge cases, error handling, and validation logic
- Ask implementation-level questions ("How does it actually...?" not just "What's the API?")

## Known Issues

See `context/shared/known-issues/` for tracked issues. Key ones:

| Issue | Impact |
|-------|--------|
| [PyArrow v22+ ABI](context/shared/known-issues/pyarrow-abseil-abi.md) | Import failures on Ubuntu 22.04; pinned to `<22.0.0` |
| [geoparquet-io Windows segfault](context/shared/known-issues/geoparquet-io-windows-segfault.md) | Crashes on malformed input; test skipped on Windows |
| [PySTAC absolute paths](context/shared/known-issues/pystac-absolute-paths.md) | Leaks local paths in output; use manual JSON construction |
