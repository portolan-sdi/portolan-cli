# CI/CD Pipeline

This document is the **source of truth** for portolan-cli's CI/CD strategy. The actual workflow files in `.github/workflows/` implement this specification.

## Philosophy

AI agents write most of the code. Human review doesn't scale to match AI output volume. Therefore:

- Every quality gate is automated
- Every convention is enforceable
- Tests are verified to actually test something (mutation testing)
- Starting from zero means **strict from day one** — no `continue-on-error`

## Pipeline Tiers

| Tier | Trigger | Duration | Purpose |
|------|---------|----------|---------|
| **Tier 1** | Pre-commit hook | < 30s | Fast feedback loop for developers |
| **Tier 2** | PR / push to main | 2-5 min | Comprehensive quality gates |
| **Tier 3** | Nightly schedule | 10-30 min | Expensive checks, trend tracking |

---

## Tier 1: Pre-commit

Configured in `.pre-commit-config.yml`. Runs locally before every commit.

**Checks:**

- `ruff` — Linting with auto-fix
- `ruff format` — Code formatting
- `vulture` — Dead code detection
- `xenon` — Complexity monitoring
- Standard hooks: trailing whitespace, YAML validation, large file detection

**Not included in pre-commit** (too slow):

- Type checking (mypy)
- Tests
- Security scanning

---

## Tier 2: CI on Every PR

Workflow: `.github/workflows/ci.yml`

### Jobs

#### `lint` — Lint & Format

- `ruff format --check` — Formatting verification
- `ruff check` — Linting
- `mypy` — Type checking (strict)
- `codespell` — Spell checking

#### `security` — Security Scan

- `bandit` — Static security analysis
- `pip-audit` — Dependency vulnerability scanning

#### `test` — Test Matrix

- Python versions: 3.10, 3.11, 3.12, 3.13
- Operating systems: Ubuntu, macOS, Windows
- Excludes network, slow, and benchmark tests
- Coverage reporting to Codecov

#### `dead-code` — Dead Code & Complexity

- `vulture` — Unused code detection (min confidence 80%)
- `xenon` — Complexity thresholds (max C absolute, B modules, A average)

#### `docs` — Documentation Build

- `mkdocs build --strict` — Fails on warnings

#### `build` — Package Build

- `uv build` — Verify package builds correctly

---

## Tier 3: Nightly

Workflow: `.github/workflows/nightly.yml`

Runs at 4 AM UTC daily. Can be triggered manually.

### Jobs

#### `mutation` — Mutation Testing

Uses `mutmut` to verify tests actually catch bugs.

**Threshold:** Kill rate must be ≥ 60% (increase as codebase matures)

Why this matters: AI-generated tests can be tautological — they may pass but not actually verify behavior. Mutation testing injects bugs and checks if tests catch them.

#### `benchmark` — Performance Benchmarks

- Runs tests marked with `@pytest.mark.benchmark`
- Compares against baseline
- **Fails on >20% regression**

#### `network-live` — Live Network Tests

- Runs tests marked with `@pytest.mark.network`
- Tests against real external services
- 120-second timeout per test

#### `dependency-check` — Dependency Audit

- `pip-audit --strict` — Full security audit
- Outdated dependency reporting (informational)

---

## Release Automation

Workflow: `.github/workflows/release.yml`

Triggered on push to `main` (after PR merge).

**Process:**

1. Check if conventional commits warrant a release
2. `cz bump --changelog` — Bump version, update CHANGELOG.md
3. Push version tag
4. `uv build` — Build package
5. Publish to PyPI (trusted publishing)
6. Create GitHub Release

**Skips if:**

- No conventional commits since last release
- Commit message starts with `bump:` (avoids infinite loop)

---

## Test Markers

Define in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
markers = [
    "unit: Fast, isolated, no I/O (< 100ms each)",
    "integration: Multi-component, may touch filesystem",
    "network: Requires network access (mocked locally, real in CI)",
    "snapshot: Compares output against golden files",
    "benchmark: Performance measurement, tracked over time",
    "slow: Takes > 5 seconds",
]
```

**What runs where:**

| Gate | Tests |
|------|-------|
| Pre-commit | None (too slow) |
| CI (PR) | unit, integration, snapshot (no network, slow, benchmark) |
| Nightly | All markers including network and benchmark |

---

## Complexity Thresholds

Using `xenon` (based on radon cyclomatic complexity):

| Level | Score | Meaning |
|-------|-------|---------|
| A | 1-5 | Simple, low risk |
| B | 6-10 | Slightly complex |
| C | 11-20 | Moderately complex |
| D | 21-30 | Complex, high risk |
| E | 31-40 | Untestable, very high risk |
| F | 41+ | Error-prone, extremely high risk |

**Current thresholds:**

- `--max-absolute=C` — No function exceeds C
- `--max-modules=B` — No module average exceeds B
- `--max-average=A` — Codebase average must be A

---

## Adding New Checks

1. Add the tool to `[project.optional-dependencies.dev]` in `pyproject.toml`
2. Add to appropriate workflow tier
3. Update this document
4. **Do not add `continue-on-error: true`** — fix issues or don't add the check

---

## Troubleshooting

### "Mutation kill rate below threshold"

Your tests aren't catching enough injected bugs. Review the mutation report artifact and add tests for survived mutants.

### "Complexity exceeds threshold"

Refactor the flagged function/module. Consider extracting helper functions or simplifying logic.

### "pip-audit found vulnerabilities"

Update the affected dependency or add a temporary exception with justification in an ADR.
