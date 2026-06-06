---
paths:
  - ".github/**"
  - "prek.toml"
---

# CI, hooks, and release rules

## CI Pipeline

**Source of truth:** `context/shared/documentation/ci.md`

| Tier | When | What |
|------|------|------|
| Tier 1 | Local hooks | ruff, vulture, xenon, mypy at pre-commit, fast tests at pre-push (`prek.toml`) |
| Tier 2 | Every PR | lint, mypy, security, full tests, docs build |
| Tier 3 | Nightly | mutation testing, benchmarks, live network tests |

**All checks are strict**, no `continue-on-error`. Fix issues or they block.

### prek Hooks

Install: `uv tool install prek && prek install`. All hooks block, no `--no-verify`. See `prek.toml` for full list.

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

Portolan uses a **bump-commit-triggered release workflow**, it runs on a `bump:`
commit pushed to `main` and creates the tag itself, it is not tag-triggered. See
`.github/workflows/release.yml`.

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
