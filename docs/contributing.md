# Contributing to portolan-cli

Thank you for your interest in contributing to portolan-cli!

Most of the code here is written by AI agents, so the bar is set by **automation,
not by reviewer attention**: every quality gate is enforced in CI, and a PR is
trustable exactly to the degree it turns those gates green. AI-assisted
contributions are welcome and encouraged — the strict CI is what makes them safe
to merge.

## What a finished PR looks like

Before you ask for review, a PR should clear this bar:

- [ ] **Tests first, and they exercise real behavior.** New/changed product code
      ships with tests written before the implementation (TDD is required, see
      [Testing](#testing)). Prefer a reproducible failing test as the starting
      point.
- [ ] **Integration coverage across boundaries.** If the change spans layers
      (CLI → API → format handlers → backend), there is a test that crosses them,
      not only unit tests.
- [ ] **All CI is green.** Every required check passes — see
      [What CI checks](#what-ci-checks). "Green means green": nothing merges red.
- [ ] **Changed lines are covered.** The `codecov/patch` gate (changed-line
      coverage) is satisfied by the fast test suite.
- [ ] **At least one adversarial review.** A human or agent has actively tried to
      break the change (edge cases, failure modes), not just skimmed it.
- [ ] **CodeRabbit comments addressed.** The automated reviewer's findings are
      resolved or explicitly answered.
- [ ] **Docs and ADRs updated.** User-facing behavior is documented; non-obvious
      decisions have an ADR (`context/shared/adr/`); `menard` doc-freshness passes.

If you run `prek run --all-files` locally and it is green, you have cleared most of
this bar before pushing.

## Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/portolan-sdi/portolan-cli.git
   cd portolan-cli
   ```

2. **Install uv** (if not already installed)
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

3. **Install dependencies**
   ```bash
   uv sync --all-extras
   ```

4. **Install prek** (git hook manager)
   ```bash
   uv tool install prek
   prek install
   ```

5. **Verify setup**
   ```bash
   uv run pytest
   uv run portolan --help
   ```

## Making Changes

### Branch Naming

- Feature: `feature/description`
- Bug fix: `fix/description`
- Documentation: `docs/description`
- Refactor: `refactor/description`

### Commit Messages

We use [Conventional Commits](https://www.conventionalcommits.org/) enforced by commitizen:

```
feat(scope): add new feature
fix(scope): fix bug
docs(scope): update documentation
refactor(scope): restructure code
test(scope): add tests
```

Use `uv run cz commit` for interactive commit creation. PRs are **squash-merged**,
so the PR title becomes the commit message — write it in conventional form.

### Pull Request Process

1. Create a branch from `main`
2. Write tests first, then implement (TDD is required — see [Testing](#testing))
3. Run `prek run --all-files` to check everything locally
4. Push and open a PR — CI runs automatically
5. Fill in the PR template and link related issues

## What CI checks

There is **one rule source**: CI runs the *same* hooks you run locally via
`prek run --all-files` (see `prek.toml`), rather than re-listing each tool with its
own arguments. So a green local `prek` run is a faithful preview of the CI `quality`
job. That single job covers:

- **ruff** — lint + format
- **mypy** — type checking (strict)
- **import-linter** — architecture contracts ([ADR-0025](https://github.com/portolan-sdi/portolan-cli/blob/main/context/shared/adr/0025-architecture-as-code.md))
- **codespell** — spelling
- **vulture** / **xenon** / **pylint** — dead code, complexity, duplication
- **bandit** — security scanning
- **deptry** — dependency hygiene
- **menard** — code↔doc freshness
- **actionlint** / **zizmor** — GitHub Actions workflow linting + supply-chain audit

Alongside `quality`, CI runs the **test matrix** (Python 3.10–3.13 × Linux/macOS/
Windows), **pip-audit** (dependency CVEs, ignores tracked in `.pip-audit-ignores`
with expiry dates), the **docs build**, and the **package build**.

**Required checks** (enforced by branch-protection rulesets, applied via
`scripts/apply_branch_protection.sh`): `CI Success` (a single job that aggregates
every gate above — so adding a Python/OS never drops a required check),
`codecov/patch`, and `codecov/project`. All checks are strict; none allow failures.

### Heavier gates run nightly, not per-PR

To keep PR feedback fast, the expensive gates run on a schedule and don't block
your PR:

- **Mutation testing** (`mutmut`) — verifies tests actually catch injected bugs
  (currently being repaired, see
  [#612](https://github.com/portolan-sdi/portolan-cli/issues/612)).
- **Benchmark regression** — flags performance regressions.
- **Live-network tests** — hit real third-party services; because those can be
  flaky, this job is **non-blocking** and a failure opens a single self-closing
  tracking issue instead of turning the nightly red.

### Self-healing automation

- **Dependency vulnerabilities** — the security-audit workflow opens/updates/closes
  a single tracking issue automatically as CVEs appear and get resolved.
- **Dependabot auto-merge** — patch/minor dependency bumps are approved and
  auto-merged once the full green check set passes; majors stay for a human.

## Testing

Tests use pytest with markers to categorize test types:

| Marker | Description |
|--------|-------------|
| `@pytest.mark.unit` | Fast, isolated, no I/O (< 100ms each) |
| `@pytest.mark.integration` | Multi-component, may touch filesystem |
| `@pytest.mark.network` | Requires network (mocked locally, real in CI nightly) |
| `@pytest.mark.realdata` | Uses real-world fixtures from `tests/fixtures/realdata/` |
| `@pytest.mark.snapshot` | Compares output against golden files |
| `@pytest.mark.benchmark` | Performance measurement, tracked over time |
| `@pytest.mark.slow` | Takes > 5 seconds |

```bash
# All tests
uv run pytest

# Only unit tests
uv run pytest -m unit

# With coverage report
uv run pytest --cov=portolan_cli --cov-report=html
```

**Test-driven development is required.** Write tests before implementation. Tests must fail before the implementation exists and pass after. This is not optional.

## Release Process

Releases are **bump-commit-triggered**, not tag-triggered: pushing a `bump:` commit
to `main` drives the release workflow, which creates the tag itself. Version bumps
follow conventional commits:

| Commit type | Version bump |
|-------------|--------------|
| `feat:` | Minor (0.x.0) |
| `fix:` | Patch (0.0.x) |
| `BREAKING CHANGE:` | Major (x.0.0) |
| `docs:`, `refactor:`, `test:`, `chore:` | No release |

To cut a release, open a PR that runs:
```bash
uv run cz bump --changelog
```

When that PR merges, the release workflow detects the `bump:` commit, creates the
git tag, builds the package, publishes to PyPI (trusted publishing), and creates a
GitHub Release.

## Code Standards

- All code requires type annotations (`mypy --strict`)
- Use `portolan_cli/output.py` for all user-facing terminal messages
- Non-obvious design decisions require an ADR in `context/shared/adr/`

## Spec Changes

The Portolan specification lives in `spec/` within this repository. The CLI repo
is the **source of truth** for the spec; the separate
[portolan-spec](https://github.com/portolan-sdi/portolan-spec) repository is a
read-only mirror synced via CI.

To propose spec changes:

1. Open a PR in this repository that modifies files in `spec/`
2. The PR itself is the proposal — discuss in the PR comments
3. On merge, CI automatically syncs changes to portolan-spec

See [ADR-0048](https://github.com/portolan-sdi/portolan-cli/blob/main/context/shared/adr/0048-cli-as-spec-source.md) for rationale.

## Questions?

- **Bug reports / feature requests:** Open an issue
- **Questions:** Use GitHub Discussions

## Code of Conduct

Be respectful and constructive. Help create a welcoming environment.

## License

By contributing, you agree your contributions will be licensed under Apache 2.0.
