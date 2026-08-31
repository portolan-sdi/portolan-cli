<!-- ops-sync:begin — synced from portolan-sdi/portolan-ops. Edit there, not here. -->
# Portolan Agent Norms

These rules apply to AI agents working in any portolan-sdi repo. Every downstream repo carries this text verbatim as a synced block at the top of its own `AGENTS.md`. Repo-specific instructions live below the block and override the canonical rules in that repo only.

Claude Code does not read `AGENTS.md`. Each repo carries a one-line `CLAUDE.md` that imports it instead. Put repo-specific instructions in `AGENTS.md`, never in `CLAUDE.md`, which the sync overwrites.

## Ground Rules

The [portolan-spec](https://github.com/portolan-sdi/portolan-spec) repo is ground truth for the Portolan standard. The CLI, validator, registry, and every other tool implement the spec. They are downstream of it. Never describe the CLI as the source of truth. Propose spec changes in portolan-spec.

Before documenting any command, flag, or API, verify it exists in the shipped tool. A fabricated example persists beyond the session that wrote it.

Every repo uses Apache-2.0 except portolan-browser and portolan-nl-demo, which are ISC forks. See [norms/repos.md](https://github.com/portolan-sdi/portolan-ops/blob/main/norms/repos.md) for the record. Never introduce code under another license without a human decision recorded there.

Never bypass pre-commit hooks or CI gates. Green means green.

Write commits in conventional form. Squash-merge makes the pull request title become the commit message.

## Pull Requests and Issues

Write every issue and pull request in two layers. The human layer comes first: what is wrong or missing, why it matters, and what should happen instead. Someone who did not follow your investigation should understand it in about a minute. The agent layer comes after: evidence, implementation detail, constraints, edge cases, and verification.

There is no word limit. A 700-word issue is good when its first 150 words make the outcome obvious. A 150-word issue is bad when it compresses the meaning into prose the reader has to unpack. Optimize for fast comprehension, not for short tickets.

Write them in Simplified Technical English (ASD-STE100). The rules are an output style, `.claude/output-styles/simplified-technical-english.md`, which every repo carries. A hook prints it at session start. Sentences stay under 20 words and hold one idea. Use the active voice and simple verb forms only, so no gerund, no present participle, and no perfect tense. Use a verb rather than a noun made from a verb. Keep the technical content exactly as precise as it was, and simplify only the language around it. Describe the design as it stands now rather than the approaches you discarded.

The structural contract CI enforces on a pull request:

- The sections `## What changed`, `## Why`, and `## Verification` exist and are not empty.
- The prose references the issue the change resolves, as `#N` or its URL.
- Verification pastes the command you ran and its output in a fenced block under `## Verification`. It names the data it read, as a URL or catalog path.
- A change that alters no behavior ticks the waiver checkbox instead. Keep its wording intact because the check matches the phrase "does not alter behavior".

Good evidence shows the fix works against real data. Proving a command exits zero is not enough. Take the failing command from the issue, run it against the same catalog, and show it now succeeds. A wall of pytest output does not count.

Issues follow the same shape. A bug report shows the failure and names the data. A feature request shows where the current tool falls short, or what the workaround costs. A task states the outcome and the command that proves it is done.

Every repo uses the org issue template. The language itself is checked before a body is ever filed: `.claude/hooks/writing_check.py` runs on `gh issue create` and `gh pr create`, and reports the specific problems it found. Run `writing_check.py --print-rules` to read the rules. When it is wrong about a line, say so in the body with `<!-- ste-ok: RULE_ID why this is correct -->`. Dependabot is exempt from the CI check.

That check matches words and punctuation. It cannot see tone, padding, or prose that spends its length arguing for the work it describes, so passing it proves nothing about how the body reads. Read what you wrote before you file it, and cut the sentences that exist to make the change sound good.

## Documentation

Agents writing or restructuring documentation follow two exemplars named in [norms/docs.md](https://github.com/portolan-sdi/portolan-ops/blob/main/norms/docs.md). [obstore](https://github.com/developmentseed/obstore) demonstrates a concise, human-readable README that delegates to good docs elsewhere. [scaffold-docs-skill](https://github.com/dbreunig/scaffold-docs-skill) shows how to build docs that have a clear human-facing surface, maintain examples via tests so they never drift, and auto-generate API docs instead of duplicating them. Both keep documentation maintainable and robust. Draft top-down with human review between layers. Do not draft a README from a generic template or from memory.

Three rules apply to every docs change. Use title-case headings without emoji. Use absolute dates like "in July 2026", never "recently". Command examples must have been actually run against the shipped tool.

## Voice and Messaging

Every written artifact follows [VOICE.md](https://github.com/portolan-sdi/portolan-ops/blob/main/VOICE.md). This includes READMEs, PR and issue bodies, commit message bodies, docs, and lasting code comments. Apply it while drafting, not as cleanup.

Before drafting substantial public copy like a README, a docs page, or an announcement, fetch and read [VOICE.md](https://github.com/portolan-sdi/portolan-ops/blob/main/VOICE.md) and [copy/messaging.md](https://github.com/portolan-sdi/portolan-ops/blob/main/copy/messaging.md) in full. If you cannot fetch them, say so and stop. Write from the actual files, not from memory.

How Portolan is described comes from [copy/messaging.md](https://github.com/portolan-sdi/portolan-ops/blob/main/copy/messaging.md) alone.

## Org-Wide Facts

The canonical homepage is https://www.portolan-sdi.org/. Canonical URLs live in [copy/urls.md](https://github.com/portolan-sdi/portolan-ops/blob/main/copy/urls.md). Do not hardcode variants.

Community discussion happens in the [Portolan Google Group](https://groups.google.com/g/portolan) and the [Portolan channel](https://cloudnativegeo.slack.com/archives/C0A1JBH9529) in Cloud-Native Geo Slack. Planning lives in [org-level GitHub projects](https://github.com/orgs/portolan-sdi/projects/1).

## Contribution Rules

The [AI policy](https://github.com/portolan-sdi/portolan-ops/blob/main/policies/AI_POLICY.md) applies to every contribution. An agent may draft the diff and the pull request body. A human must read, understand, and approve both before review is requested. Agents never open PRs, post comments, or take action in shared spaces without human approval.

Follow the [contributing guide](https://github.com/portolan-sdi/portolan-ops/blob/main/policies/CONTRIBUTING.md) and the [code of conduct](https://github.com/portolan-sdi/portolan-ops/blob/main/policies/CODE_OF_CONDUCT.md).

## Sync Discipline

Files between `ops-sync` markers are synced from [portolan-ops](https://github.com/portolan-sdi/portolan-ops). They are overwritten on every sync run. To change one, edit it in portolan-ops, never in place.

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

<!-- freshness: last-verified: 2026-08-27 -->
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
| [geoparquet-io drops CRS on write](context/shared/known-issues/geoparquet-io-write-drops-crs.md) | Resolved in geoparquet-io 1.4.0. The rewrite fidelity gate stays, because it guards a destructive operation |
| [DuckDB 1.5.5 ST_Read_Meta segfault](context/shared/known-issues/duckdb-155-st-read-meta-segfault.md) | Kills the process on malformed vector input, with no catchable error; pinned to `duckdb<1.5.5` |
| [geoparquet-io S2 unavailable](context/shared/known-issues/geoparquet-io-s2-unavailable.md) | `add s2` and `partition s2` stop with an explanation. The DuckDB `geography` extension is not published for `duckdb>=1.5.2`. Use `a5` |
