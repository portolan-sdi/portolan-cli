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
| **Tier 1** | prek hook | < 30s | Fast feedback loop for developers |
| **Tier 2** | PR / push to main | 2-5 min | Comprehensive quality gates |
| **Tier 3** | Nightly schedule | 10-30 min | Expensive checks, trend tracking |

---

## Tier 1: prek

Configured in `prek.toml`. Runs locally before every commit.

Install with: `uv tool install prek && prek install`

**Checks (all blocking):**

- `ruff` / `ruff format` — Linting with auto-fix + formatting
- `mypy` — Type checking (strict)
- `import-linter` — Architecture contracts (ADR-0025)
- `codespell` — Spell checking
- `vulture` / `xenon` / `pylint` — Dead code, complexity, duplicate code (R0801)
- `bandit` — Static security analysis
- `deptry` — Dependency hygiene
- `actionlint` / `zizmor` — GitHub Actions workflow linting + supply-chain audit
- `menard check` / `check-protected` — Documentation freshness + protected content
- `validate-claude-md` — ADR index / reference validation
- `pytest -m unit` — Fast unit tests (pre-push stage)
- `commitizen` — Commit message validation (commit-msg stage)
- Builtin hooks: trailing whitespace, YAML/TOML validation, large file detection

**This is the single rule source.** Tier 2 CI runs the exact same hooks via
`prek run --all-files` (see below), so a green local `prek` run previews CI.

**Philosophy:** All hooks block. No `--no-verify`. Fix issues before committing.

---

## Tier 2: CI on Every PR

Workflow: `.github/workflows/ci.yml`

### Jobs

#### `quality` — Quality Gates (single rule source)

Runs `prek run --all-files` — the *same* hooks developers run locally (see Tier 1),
so CI and local hooks can't drift. Covers ruff, mypy, import-linter, codespell,
vulture, xenon, pylint-duplicate, bandit, deptry, menard, actionlint, zizmor, and
the builtin file hooks. Replaces the old separate `lint` and `dead-code` jobs.

CI skips three hooks: `no-commit-to-branch` (fails on push-to-main), `fast-tests`
(the `test` job covers them), and `update-freshness` (stamps today's date, so it's
non-deterministic in CI — drift is still caught by the non-mutating `menard-check`
and `validate-claude-md`).

#### `security` — Dependency Audit

- `pip-audit` — dependency vulnerability scanning. Ignores come from the
  single-source `.pip-audit-ignores` file (each entry has an expiry + reason;
  expired entries drop automatically). The same file feeds `nightly.yml` and
  `security-audit.yml`. (`bandit` moved into the `quality` job.)

#### `test` — Test Matrix

- Python versions: 3.10, 3.11, 3.12, 3.13
- Operating systems: Ubuntu, macOS, Windows
- Excludes network, slow, and benchmark tests
- Coverage reporting to Codecov (the `codecov/patch` changed-line gate is a
  required check — see [Branch protection](#branch-protection))

#### `docs` — Documentation Build

- `mkdocs build --strict` — Fails on warnings

#### `build` — Package Build

- `uv build` — Verify package builds correctly

#### `iceberg-test` — Iceberg Unit & Integration Tests

- Python versions: 3.11, 3.12, 3.13
- Operating systems: Ubuntu, macOS
- Runs only when iceberg-related paths change (or on push to `main`)
- Excludes `e2e`, `e2e_slow`, `network`, `slow`
- Coverage reporting to Codecov (`iceberg` flag)

#### `iceberg-e2e` — Iceberg E2E Tests (fast tier)

- Python versions: 3.11, 3.12, 3.13 (Ubuntu only — Docker on Linux)
- Runs only when iceberg-related paths change (or on push to `main`)
- Spins up `docker-compose` (REST Iceberg catalog + MinIO), runs `-m "e2e and not e2e_slow"` with a 120s per-test timeout
- Dumps Docker logs on failure and always tears down

---

## Tier 3: Nightly

Workflow: `.github/workflows/nightly.yml`

Runs at 4 AM UTC daily. Can be triggered manually.

### Jobs

#### Mutation Testing (two scopes)

Uses `mutmut` to verify tests actually catch bugs. The full codebase generates
~45k mutants — far more than any single run can test in a nightly window — so
mutation testing runs at two scopes that share one scorer:

- **PR-scoped** (`mutation-pr` in `ci.yml`, PR-only, advisory): mutates only the
  `portolan_cli` files the PR changed, so feedback lands on new code when the
  author can act on it. Skipped when a PR touches no source. A PR that changes
  only comments/docstrings produces no mutants and passes (`--allow-empty`).
  Advisory today — not in the `ci-success` gate — until the floor is validated
  against a real full run; promote it to required by adding it to `ci-success`
  needs.
- **Nightly sweep** (`mutation` in `nightly.yml`, hard gate): mutates a
  deterministic `1/NUM_SHARDS` slice of the source files, round-robin by
  day-of-year, so the whole tree is covered every `NUM_SHARDS` nights (currently
  25). Lower `NUM_SHARDS` to cover more per night, but only if a run still
  finishes within `timeout-minutes`.

**Threshold:** the floor lives in `.mutation-baseline` (a single integer). Both
scopes read it via `scripts/mutation_score.py` rather than hardcoding it, so it
ratchets up in a one-line, reviewable diff. The score counts `killed + timeout +
suspicious` as killed over `killed_total + survived` testable (`no_tests`
excluded). **Lowering the floor requires a justification in the PR that does so.**

**Fails loud, never silent.** On the nightly sweep, zero testable mutants means
mutation testing is broken, not passing — the scorer hard-fails (it used to
`exit 0` and report a green nightly, hiding a broken setup). `[tool.mutmut]` in
`pyproject.toml` copies the `scripts/` package, `spec/` schemas, and data files
into the mutants sandbox and scopes the stats run to the fast, offline suite with
`--no-cov`.

**Sandbox stability.** mutmut runs the suite from a copied `mutants/` sandbox and
instruments code with a trampoline that reads `os.environ["MUTANT_UNDER_TEST"]`.
Two consequences the tests must respect: a cleared environment must preserve that
var (use `cleared_environ()` from `tests/conftest.py`, not
`patch.dict(..., clear=True)`), and files read by repo-root path (e.g.
`spec/schema/`) must be listed in `[tool.mutmut] also_copy`. Parallel conversion
falls back to serial when a process pool can't start in the sandbox
(`convert.py`). (The geoparquet-io #565 CWD guard was evaluated and is **not**
needed: `isolated_filesystem`/`chdir` tests do not crash the stats phase.)

Why this matters: AI-generated tests can be tautological — they may pass but not actually verify behavior. Mutation testing injects bugs and checks if tests catch them.

#### `benchmark` — Performance Benchmarks

- Runs tests marked with `@pytest.mark.benchmark`
- Compares against baseline
- **Fails on >20% regression**

#### `network-live` — Live Network Tests

- Runs tests marked with `@pytest.mark.network`
- Tests against real external services
- 120-second timeout per test
- **Non-blocking:** live third-party flakiness isn't a contributor's to fix, so a
  failure does not fail the workflow — it opens/updates a single self-closing
  tracking issue instead (mirrors the `security-audit` pattern)

#### `dependency-check` — Dependency Audit

- `pip-audit --strict` — Full security audit
- Outdated dependency reporting (informational)

#### `iceberg-e2e-full` — Iceberg E2E Tests (full suite)

- Python 3.11 on Ubuntu
- Spins up `docker-compose` (REST Iceberg catalog + MinIO), runs `-m e2e` (includes `e2e_slow`: concurrency stress and large datasets)
- 120s per-test timeout; Docker logs on failure; always tears down

---

## Branch Protection

`main` protection is defined **as code** in `scripts/apply_branch_protection.sh`
(idempotent, admin one-shot). It creates two rulesets:

- **PR + green checks** (no bypass — binds admins too): every push goes through a
  PR and the required status checks must pass.
- **Review required** (repo admins may bypass): 1 approving review.

**Required checks** are three stable contexts: `CI Success` (the `ci.yml`
aggregation job that gates on quality/security/test/iceberg/docs/build — requiring
this one context means adding a Python/OS never drops a required check),
`codecov/patch`, and `codecov/project`.

## Self-Healing Automation

- **Supply-chain hardening is lint-enforced.** All workflows are SHA-pinned,
  least-privilege (`permissions: contents: read` widened per job), and
  `persist-credentials: false`; `actionlint` + `zizmor` (Tier 1 + the `quality`
  job) keep it that way.
- **Dependabot auto-merge** (`dependabot-automerge.yml`): patch/minor bumps are
  approved and auto-merged once the full green check set passes; majors stay human.
  A 7-day cooldown ages fresh releases before they reach a PR.
- **Security issue automation** (`security-audit.yml`): opens/updates/closes a
  single dependency-vulnerability tracking issue as CVEs appear and resolve.

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
    "realdata: Uses real-world fixtures from tests/fixtures/realdata/ (tests orchestration, not geometry)",
    "snapshot: Compares output against golden files",
    "benchmark: Performance measurement, tracked over time",
    "slow: Takes > 5 seconds",
    "e2e: End-to-end tests requiring Docker (REST catalog + MinIO)",
    "e2e_slow: Extended E2E tests (concurrency stress, large datasets) — nightly only",
]
```

**What runs where:**

| Gate | Tests |
|------|-------|
| Pre-commit | unit only (fast, < 30s total) |
| CI (PR) | unit, integration, snapshot, **realdata**; iceberg `e2e` (not `e2e_slow`) when iceberg paths change |
| Nightly | All markers including network, benchmark, and the full iceberg `e2e` suite (with `e2e_slow`) |

### Real-World Fixtures

The `realdata` marker uses fixtures committed to `tests/fixtures/realdata/` (~4MB total).

These are production data samples that test Portolan's orchestration with real-world edge cases (antimeridian, complex polygons, LineStrings, COGs). No network access needed.

See `context/shared/documentation/test-fixtures.md` for details.

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

Your tests aren't catching enough injected bugs. Review the mutation report artifact and add tests for survived mutants. The floor lives in `.mutation-baseline`.

### "No testable mutants were generated"

On the nightly sweep this is broken, not passing — the scorer hard-fails. Usual
causes: the mutmut sandbox baseline failed (a test that passes normally but not in
`mutants/`), or the `[tool.mutmut] also_copy` sandbox is missing a repo-root file
the suite reads. Reproduce locally with `uv run mutmut run` and read the
"Running stats" output for the first failing test. See
[#612](https://github.com/portolan-sdi/portolan-cli/issues/612).

The PR-scoped job treats "no mutants" as a pass (`--allow-empty`), since a PR may
change only non-mutable lines. If the PR job reports it while the nightly is green,
that is expected, not a failure.

### "Complexity exceeds threshold"

Refactor the flagged function/module. Consider extracting helper functions or simplifying logic.

### "pip-audit found vulnerabilities"

Update the affected dependency or add a temporary exception with justification in an ADR.

### "menard: stale documentation detected"

Code was modified but linked documentation wasn't updated. Options:
1. Update the documentation to reflect the code changes
2. Run `menard fix` for interactive resolution
3. Run `menard fix-mark-reviewed <code-file> <doc-file>` if the doc doesn't need changes
4. Run `menard fix-ignore <code-file> <doc-file>` to permanently ignore the relationship

To see what changed: `menard list-stale --show-diff`
