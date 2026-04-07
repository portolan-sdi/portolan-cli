# PR #321 Async Migration Fixes Plan

**PR:** [#321 - feat(async): Complete async migration for push/pull operations](https://github.com/portolan-sdi/portolan-cli/pull/321)
**Related Issues:** #309 (Async expansion), #318 (Push/pull performance)
**Created:** 2026-04-07
**Status:** In Progress

## Executive Summary

Adversarial review of PR #321 identified 18 failing CI tests and multiple code quality issues. This plan documents all required fixes organized into parallelizable waves.

---

## Phase 1: CI Blocker Fixes (CRITICAL)

These must be fixed first - PR is unmergeable without them.

### 1.1 Mock Async Functions in Tests

**Problem:** Tests patch sync functions but code now calls async variants. Tests hit real AWS IMDS.

**Files to fix:**
- [ ] `tests/integration/test_push_integration.py` - Patch `_fetch_remote_versions_async` not `_fetch_remote_versions`
- [ ] `tests/integration/test_push_parallel_integration.py` - Mock `push_async` or mock at obstore level
- [ ] `tests/integration/test_pull_parallel_integration.py` - Update mocks for async paths

**Pattern:**
```python
# BEFORE (broken):
@patch("portolan_cli.push._fetch_remote_versions")
@patch("portolan_cli.push._setup_store")

# AFTER (correct):
@patch("portolan_cli.push._fetch_remote_versions_async", new_callable=AsyncMock)
@patch("portolan_cli.push._setup_store")  # This one is still sync
```

### 1.2 Fix Output String Contract Change

**Problem:** Tests expect "Using 2 parallel worker(s)" but code outputs "Using concurrency: 2"

**Files to fix:**
- [ ] `tests/integration/test_pull_parallel_integration.py::test_pull_workers_flag`
  - Update assertion from `"Using 2 parallel worker(s)"` to `"Using concurrency: 2"`

**Decision needed:** Should we restore the old output format for backward compatibility, or update tests?
- **Recommendation:** Update tests - "concurrency" is the correct term for async patterns

### 1.3 Fix Dry-Run TypeError

**Problem:** `TypeError: 'NoneType' object is not subscriptable` in `test_push_dry_run_flag`

**Investigation needed:**
- [ ] Trace `push_async()` dry_run path
- [ ] Check `_handle_push_dry_run()` return value
- [ ] Verify `local_data` is not None when accessed

**Likely location:** `push.py` around line 1440-1446

### 1.4 Disable IMDS in Test Environment

**Problem:** obstore tries AWS IMDS auto-discovery in CI, gets 411 errors

**Options:**
- [ ] Set `AWS_EC2_METADATA_DISABLED=true` in CI workflow
- [ ] Mock obstore at lower level (store creation)
- [ ] Add `@pytest.mark.network` to tests that need real S3

**Recommended:** Add environment variable to `.github/workflows/ci.yml`:
```yaml
env:
  AWS_EC2_METADATA_DISABLED: true
```

---

## Phase 2: Code Quality Fixes (HIGH)

### 2.1 Remove Duplicate CircuitBreaker

**Problem:** `pull.py` lines 564-594 duplicates `async_utils.py` CircuitBreaker

**Files to fix:**
- [ ] `portolan_cli/pull.py` - Remove local `CircuitBreaker` class
- [ ] Import from `async_utils` instead:
  ```python
  from portolan_cli.async_utils import CircuitBreaker
  ```

**Note:** The two implementations have different defaults (10 vs 5) and APIs. Choose one:
- **Recommendation:** Use `async_utils.CircuitBreaker` (has thread-safety via `_lock`)

### 2.2 Remove Duplicate DEFAULT_CONCURRENCY

**Problem:** `pull.py` line 62 defines `DEFAULT_CONCURRENCY = 50` instead of importing

**Files to fix:**
- [ ] `portolan_cli/pull.py` - Replace with:
  ```python
  from portolan_cli.async_utils import get_default_concurrency
  DEFAULT_CONCURRENCY = get_default_concurrency()
  ```

### 2.3 Fix run_async() Dangerous Fallback

**Problem:** `async_utils.py` lines 413-422 uses `run_until_complete()` which can crash

**Files to fix:**
- [ ] `portolan_cli/async_utils.py` - Remove fallback branch:
  ```python
  def run_async(coro: Coroutine[None, None, T]) -> T:
      """Run an async coroutine from sync code."""
      return asyncio.run(coro)  # Simple, always works
  ```

**Note:** If nested event loop is needed, use `nest_asyncio` library instead

---

## Phase 3: Performance Completeness (MEDIUM)

Per Issue #318, these were supposed to be addressed but remain incomplete.

### 3.1 Parallelize STAC File Uploads

**Problem:** `_upload_stac_files_async()` still uses sequential `for` loop

**Files to fix:**
- [ ] `portolan_cli/push.py` lines 1108-1174

**Pattern:**
```python
# Use AsyncIOExecutor for STAC files
# Maintain manifest-last ordering in phases:
# Phase 1: All item STAC files (parallel)
# Phase 2: collection.json (single)
# Phase 3: catalog.json (single)
```

### 3.2 Parallelize README Uploads

**Problem:** `_upload_readmes_async()` still uses sequential loop

**Files to fix:**
- [ ] `portolan_cli/push.py` lines 1214-1255

### 3.3 Connection Pooling

**Problem:** Each collection pull creates new store connection

**Files to fix:**
- [ ] `portolan_cli/pull.py` - Pass store instance to `_download_assets_async`

---

## Phase 4: Consolidation (LOW)

Per Issue #318 recommendations.

### 4.1 Use upload.py Store Setup

**Problem:** `push.py` reimplements `_setup_store()` instead of using `upload.py`

**Files to fix:**
- [ ] `portolan_cli/push.py` - Import `_setup_store_and_kwargs` from `upload.py`

### 4.2 Restore verbose Parameter

**Problem:** `push()` accepts `verbose` but it's documented as "ignored in async"

**Options:**
- [ ] Implement verbose in async paths
- [ ] Remove parameter and deprecate
- **Recommendation:** Implement - per ADR-0040 verbose mode is desired

---

## Parallelization Strategy

### Wave 1 (Blocking - Sequential)
Must complete before other waves:
- **1.4** Disable IMDS - affects all other test fixes

### Wave 2 (CI Fixes - Parallel)
Can run in parallel after Wave 1:
- **1.1** Mock async functions (Agent A)
- **1.2** Fix output string tests (Agent B)
- **1.3** Fix dry-run TypeError (Agent C)

### Wave 3 (Code Quality - Parallel)
Can run after CI is green:
- **2.1** Remove duplicate CircuitBreaker (Agent D)
- **2.2** Remove duplicate constant (Agent D - same file)
- **2.3** Fix run_async (Agent E)

### Wave 4 (Performance - Parallel)
Final polish:
- **3.1** Parallelize STAC uploads (Agent F)
- **3.2** Parallelize README uploads (Agent F - same file)
- **3.3** Connection pooling (Agent G)

### Wave 5 (Consolidation - Optional)
Nice to have:
- **4.1** Store setup consolidation
- **4.2** Verbose parameter

---

## Test Matrix Failures (18 total)

| Test | Failure Type | Fix Phase |
|------|--------------|-----------|
| `test_push_dry_run_flag` | TypeError | 1.3 |
| `test_push_force_flag` | IMDS 411 | 1.1, 1.4 |
| `test_push_profile_flag` | IMDS 411 | 1.1, 1.4 |
| `test_push_reads_aws_profile_from_config` | IMDS 411 | 1.1, 1.4 |
| `test_push_cli_profile_overrides_config` | IMDS 411 | 1.1, 1.4 |
| `test_push_conflict_shows_error` | IMDS 411 | 1.1, 1.4 |
| `test_push_json_success` | IMDS 411 | 1.1, 1.4 |
| `test_push_json_error` | JSONDecodeError | 1.1, 1.3 |
| `test_cli_normal_push_still_shows_nothing_to_push` | IMDS 411 | 1.1, 1.4 |
| `test_push_cli_conflict_human_advice` | IMDS 411 | 1.1, 1.4 |
| `test_non_dry_run_zero_versions_shows_nothing_to_push` | IMDS 411 | 1.1, 1.4 |
| `test_successful_push_with_versions_shows_pushed_message` | IMDS 411 | 1.1, 1.4 |
| `test_non_dry_run_nothing_to_push_no_prefix` | IMDS 411 | 1.1, 1.4 |
| `test_parallel_execution_observes_worker_count` | Mock type error | 1.1 |
| `test_sequential_execution_with_workers_1` | Mock type error | 1.1 |
| `test_parallel_continues_on_individual_failure` | IMDS 411 | 1.1, 1.4 |
| `test_pull_workers_flag` | String mismatch | 1.2 |
| `test_add_root_shows_all_collections_in_output` | Unrelated | N/A |

---

## Verification Checklist

Before marking complete:

- [ ] All 18 failing tests pass locally
- [ ] CI passes on all 10 matrix combinations
- [ ] No new mypy errors
- [ ] No new ruff warnings
- [ ] `uv run pytest tests/unit/test_push_async.py tests/unit/test_pull_async.py` passes
- [ ] `uv run pytest tests/integration/test_push_integration.py` passes
- [ ] `uv run pytest tests/integration/test_push_parallel_integration.py` passes

---

## Notes

- Issue #318 identified ~30-40% performance improvement potential from STAC parallelization
- Issue #309 established async-first pattern; this PR is the implementation
- Some tests may need `@pytest.mark.asyncio` decorator if not already present
