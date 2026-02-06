# CI Workflow Fixes Design

**Date:** 2025-02-06
**Status:** Approved
**Author:** Claude + Nissim

## Problem Statement

Four CI workflow issues are blocking development:

1. **Release workflow fails** - `GITHUB_TOKEN` cannot push to protected `main` branch
2. **Mutation testing fails** - `--paths-to-mutate` CLI flag removed in mutmut 3.x
3. **Benchmark job fails** - No tests marked `@pytest.mark.benchmark` (exit code 5)
4. **Network job fails** - No tests marked `@pytest.mark.network` (exit code 5)

## Design Decisions

### 1. Tag-Based Release Strategy

**Decision:** Release workflow creates tags from bump commits; it does not push commits.

**Rationale:**
- Respects GitHub branch protection (no PAT/GitHub App needed)
- Matches geoparquet-io's release pattern
- Version bump commits go through normal PR review

**Developer workflow:**
1. Accumulate changes via normal PR merges
2. When ready to release, create a PR running `uv run cz bump --changelog`
3. Merge bump PR → workflow detects bump commit → creates tag → publishes

### 2. Mutmut Configuration via pyproject.toml

**Decision:** Configure mutmut in `pyproject.toml` instead of CLI flags.

**Rationale:**
- `--paths-to-mutate` removed in mutmut 3.x
- Configuration belongs with the project, not buried in CI scripts

### 3. Placeholder Tests for Benchmark and Network

**Decision:** Add minimal but meaningful placeholder tests.

**Rationale:**
- Proves infrastructure works
- Workflows pass immediately
- Easy to expand as features mature
- Aligns with "all quality gates in place from day one" philosophy

## Changes

### release.yml

```yaml
# New logic:
# 1. Trigger on push to main
# 2. Check if HEAD commit message starts with "bump:"
# 3. If yes: extract version from pyproject.toml, create tag, build, publish, release
# 4. If no: exit successfully (normal PR merge, no release needed)
```

Key changes:
- Remove `cz bump` execution (happens in PR now)
- Remove `git push` (no longer needed)
- Add version extraction from pyproject.toml
- Add tag creation step

### nightly.yml

```yaml
# Before:
uv run mutmut run \
  --paths-to-mutate=portolan_cli/ \
  --tests-dir=tests/ \
  --no-progress

# After:
uv run mutmut run --no-progress
```

### pyproject.toml

```toml
[tool.mutmut]
paths_to_mutate = ["portolan_cli/"]
tests_dir = "tests/"
```

### tests/benchmark/test_catalog_benchmark.py (new)

Benchmark `Catalog.init()` to establish performance baseline.

### tests/network/test_network_placeholder.py (new)

HEAD request to PyPI to verify network test infrastructure.

### Documentation

- Update `docs/contributing.md` with new release process
- Update `CLAUDE.md` Git Workflow section

## Testing

After implementation:
1. Run `uv run pytest -m benchmark` locally - should pass
2. Run `uv run pytest -m network` locally - should pass
3. Run `uv run mutmut run --no-progress` locally - should run without CLI errors
4. Trigger nightly workflow manually - all jobs should pass
5. Create a test bump PR to verify release workflow

## Rollout

1. Implement all changes in a single PR
2. Merge to main
3. Manually trigger nightly workflow to verify
4. Create a real release using new process to verify end-to-end
