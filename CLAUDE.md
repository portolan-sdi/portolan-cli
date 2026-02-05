# Portolan CLI - Development Guide

## Guiding Principle

AI agents will write most of the code. Human review does not scale to match AI output volume. Therefore: every quality gate must be automated, every convention must be enforceable, and tests must be verified to actually test something.

## Quick Reference

| Resource | Location |
|----------|----------|
| Contributing guide | `docs/contributing.md` |
| CI/CD documentation | `context/shared/documentation/ci.md` |
| Distill MCP tools | `context/shared/documentation/distill-mcp.md` |
| ADRs | `context/shared/adr/` |
| Plans & research | `context/shared/` |

**Target Python version:** 3.10+ (matches geoparquet-io dependency)

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

Pre-commit blocks on ALL checks. Install with `uv run pre-commit install`.

Hooks run: trailing-whitespace, end-of-file-fixer, check-yaml, check-toml, check-merge-conflict, mixed-line-ending, check-added-large-files, ruff (fix + format), vulture, xenon, mypy, fast unit tests, commitizen (commit-msg).

If a hook fails, fix the issue before committing. No `--no-verify`.

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

Releases are automated via commitizen on push to main. See `.github/workflows/release.yml`.

1. Commits analyzed for conventional commit types
2. Version bumped (major/minor/patch based on commits)
3. CHANGELOG.md updated
4. Git tag created
5. Published to PyPI

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

### Why This Matters

- **AI agents start fresh each session** — They don't remember past conversations
- **Context files are their memory** — ADRs, known-issues, and CLAUDE.md persist knowledge
- **Documentation compounds** — Each documented decision helps all future sessions
- **Undocumented knowledge is lost** — If it's not written down, it doesn't exist for agents

### ADR Guidelines

Create an ADR (`context/shared/adr/NNNN-title.md`) when:

- Choosing between multiple valid approaches
- Adopting a new dependency
- Establishing a pattern that others should follow
- Making a trade-off that isn't obvious

Use the template at `context/shared/adr/0000-template.md`.

### Two Documentation Audiences

| Audience | Location | Optimized For |
|----------|----------|---------------|
| **Humans** | `docs/` (mkdocs) | Readability, navigation, tutorials, visual presentation |
| **AI agents** | Docstrings, CLAUDE.md, ADRs, inline comments | Context windows, searchability, co-location with code |

**Human docs (`docs/`):**
- Rendered website via mkdocs
- Prose-heavy with examples and screenshots
- Organized by user journey (getting started → advanced topics)
- Can be verbose — humans skim and navigate

**AI docs (in-repo):**
- Docstrings: Complete API contracts (args, returns, raises, examples)
- CLAUDE.md: Development patterns, commands, gotchas
- ADRs: Decision rationale with alternatives considered
- Inline comments: Non-obvious code behavior
- Dense and structured (tables, bullet lists) — agents parse linearly

**Key difference:** Human docs explain *how to use* the tool. AI docs explain *how to modify* the codebase.

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

When building Portolan, follow these principles:

- **Don't duplicate.** Portolan orchestrates; geoparquet-io, rio-cogeo, and other libraries do the actual work. Never reimplement functionality that exists in a dependency.
- **YAGNI.** Minimal dependencies, minimal architecture. Don't build plugin discovery until someone writes a plugin. Don't add commands until someone needs them. Code is cheap to write; complexity is expensive to maintain.
- **Interactive by default, automatable always.** Every interactive prompt has a `--auto` fallback with smart defaults.
- **versions.json is the source of truth.** It drives sync, validation, and version history. See ADR-0005.
- **Plugin interface defined early, plugin system built later.** Internal format handlers follow a consistent interface so external plugins can register when needed. See ADR-0003.
- **Spec versioning from the start.** Catalogs declare which spec version they target; validators respect this.
- **CLI wraps the API.** All logic lives in the Python library. The CLI is a thin layer of Click decorators. See ADR-0007.

## Tool Usage

| Tool | Purpose | Documentation |
|------|---------|---------------|
| context7 | Up-to-date library docs | — |
| distill | Token-efficient operations | `context/shared/documentation/distill-mcp.md` |
| worktrunk | Worktree management | — |
