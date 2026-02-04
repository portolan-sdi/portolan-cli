# Portolan CLI - Development Guide

## Guiding Principle

AI agents will write most of the code. Human review does not scale to match AI output volume. Therefore: every quality gate must be automated, every convention must be enforceable, and tests must be verified to actually test something.

## Quick Reference

| Resource | Location |
|----------|----------|
| Contributing guide | `docs/contributing.md` |
| CI/CD documentation | `context/shared/documentation/ci.md` |
| ADRs | `context/adr/` |
| Plans & research | `context/shared/` |

## Project Structure

```
portolan-cli/
├── portolan_cli/          # Source code
├── tests/                 # Test suite
│   ├── fixtures/          # Test data files
│   └── specs/             # Human-written test specifications
├── docs/                  # Documentation (mkdocs)
├── context/               # AI development context
│   ├── adr/               # Architectural decisions
│   └── shared/            # Plans, research, reports
└── .github/workflows/     # CI/CD pipelines
```

## Before Writing Code

Always research before implementing:

1. **Understand the request** — Ask clarifying questions if ambiguous
2. **Search for patterns** — Check if similar functionality exists
3. **Check utilities** — Review `portolan_cli/` first
4. **Review existing tests** — Look at tests for the area you're modifying
5. **Check ADRs** — Read `context/adr/` to understand past decisions

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

Three layers of defense (see `docs/ci.md` for details):

1. **Mutation testing** — Nightly `mutmut` runs verify tests catch real bugs
2. **Property-based testing** — Use `hypothesis` for invariant verification
3. **Human test specs** — `tests/specs/` defines what matters; AI implements

### Test Fixtures

Store small, representative data files in `tests/fixtures/`. Each subdirectory gets a README.md explaining what each file is and why it exists. Fixtures should be:

- **Small** — a few rows/pixels, enough to test behavior
- **Committed to git** — they're small enough, and reproducibility matters
- **Paired with invalid variants** — every valid fixture should have a corresponding invalid one for testing error handling
- **Documented** — without docs, future contributors won't know what `test_3857.parquet` is for

## CI Pipeline

**Source of truth:** `context/shared/documentation/ci.md`

| Tier | When | What |
|------|------|------|
| Tier 1 | Pre-commit | ruff, vulture, xenon |
| Tier 2 | Every PR | lint, mypy, security, tests, docs build |
| Tier 3 | Nightly | mutation testing, benchmarks, live network tests |

**All checks are strict** — no `continue-on-error`. Fix issues or they block.

## Code Quality

### Tools

- **ruff** — Linting and formatting
- **mypy** — Type checking (strict)
- **vulture** — Dead code detection
- **xenon** — Complexity monitoring
- **bandit** — Security scanning
- **pip-audit** — Dependency vulnerabilities

### Complexity Thresholds

- No function exceeds complexity level C
- No module average exceeds B
- Codebase average must be A

## Git Workflow

### Conventional Commits

Use commitizen format:

```
feat(scope): add new feature
fix(scope): fix bug
docs(scope): update documentation
refactor(scope): restructure code
test(scope): add tests
```

### Release Automation

Releases are automated via commitizen on push to main (see `.github/workflows/release.yml`):

1. Analyze commits since last release
2. Bump version based on commit types
3. Generate changelog entry
4. Create git tag
5. Publish to PyPI

## Development Rules

- **ALL** code must have type annotations
- **ALL** new features require tests FIRST (TDD)
- **ALL** non-obvious decisions require an ADR in `context/adr/`
- **NO** new dependencies without discussion (document in ADR)

## Standardized Terminal Output

Define this once in `portolan_cli/output.py` and use it everywhere. No raw `print()` or `click.echo()` calls scattered through the codebase.

```python
from portolan_cli.output import success, info, warn, error, detail

success("Wrote output.parquet (1.2 MB)")  # ✓ Green checkmark
info("Reading data.shp (4,231 features)")  # → Blue arrow
warn("Missing thumbnail (recommended)")    # ⚠ Yellow warning
error("No geometry column (required)")     # ✗ Red X
detail("Processing chunk 3/10...")         # Dimmed text
```

## Tool Usage

- **context7** — For up-to-date library documentation
- **distill** — To keep token usage down
- **worktrunk** — For worktree management

## Distill MCP Tool Guidelines

Use Distill MCP tools for token-efficient operations:

### Rule 1: Smart File Reading

When reading source files for **exploration or understanding**:

```
mcp__distill__smart_file_read filePath="path/to/file.py"
```

**When to use native Read instead:**
- Before editing a file (Edit requires Read first)
- Configuration files: `.json`, `.yaml`, `.toml`, `.md`, `.env`

### Rule 2: Compress Verbose Output

After Bash commands that produce verbose output (>500 characters):

```
mcp__distill__auto_optimize content="<paste verbose output>"
```

### Rule 3: Code Execute SDK for Complex Operations

For multi-step operations, use `code_execute` instead of multiple tool calls (**98% token savings**):

```
mcp__distill__code_execute code="<typescript code>"
```

**SDK API (`ctx`):**

| Category | Methods |
|----------|---------|
| Compress | `auto(content, hint?)`, `logs(logs)`, `diff(diff)`, `semantic(content, ratio?)` |
| Code | `parse(content, lang)`, `extract(content, lang, {type, name})`, `skeleton(content, lang)` |
| Files | `read(path)`, `exists(path)`, `glob(pattern)` |
| Git | `diff(ref?)`, `log(limit?)`, `status()`, `branch()`, `blame(file, line?)` |
| Search | `grep(pattern, glob?)`, `symbols(query, glob?)`, `files(pattern)`, `references(symbol, glob?)` |
| Analyze | `dependencies(file)`, `callGraph(fn, file, depth?)`, `exports(file)`, `structure(dir?, depth?)` |
| Utils | `countTokens(text)`, `detectType(content)`, `detectLanguage(path)` |

### Quick Reference

| Action | Tool |
|--------|------|
| Read code for exploration | `mcp__distill__smart_file_read filePath="file.py"` |
| Get a function/class | `smart_file_read` with `target={"type":"function","name":"myFunc"}` |
| Compress build errors | `mcp__distill__auto_optimize content="..."` |
| Multi-step operations | `mcp__distill__code_execute code="return ctx.files.glob('src/**/*.py')"` |
| Before editing | Use native `Read` tool |
