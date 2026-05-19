# Versioning Stress Tests Plan

**Date:** 2026-04-21
**Issue:** [#339](https://github.com/portolan-sdi/portolan-cli/issues/339) - versions.json not populated after portolan add
**Status:** ✅ FIXED

## Problem Statement

Issue #339 reports that after `portolan add` completes successfully ("Added 1900 files"), `versions.json` shows `"collections": {}` and `portolan push` reports "0 files".

### Root Cause Analysis

**Finding:** There are TWO separate `versions.json` files with different schemas:

| File | Schema | Purpose | Updated by `add`? |
|------|--------|---------|-------------------|
| `<catalog>/versions.json` | `{"catalog_id": "...", "collections": {...}}` | Catalog-level aggregate view | **YES** (after fix) |
| `<catalog>/<collection>/versions.json` | `{"spec_version": "...", "versions": [...]}` | Collection versioning (ADR-0005) | **YES** |

### The Bug (Now Fixed)

Per ADR-0005, catalog-level versions.json should track aggregate collection state:

```
catalog-root/
├── versions.json                          # Catalog-level versioning ← WAS NOT UPDATED
├── demographics/
│   ├── versions.json                      # Collection-level versioning
```

The bug was that `catalog.py:init_catalog()` created catalog-level versions.json with empty `"collections": {}`, but **nothing ever updated it**. Users checking `<catalog>/versions.json` (as documented in ADR-0005) saw empty collections.

### The Fix

Added `update_catalog_versions()` in `catalog.py` which is called from `finalize_datasets()` in `dataset.py` after each successful collection update. Now catalog-level versions.json shows:

```json
{
  "schema_version": "1.0.0",
  "catalog_id": "my-catalog",
  "created": "2026-01-15T10:00:00Z",
  "updated": "2026-04-21T10:30:00Z",
  "collections": {
    "demographics": {
      "current_version": "1.2.0",
      "updated": "2026-04-21T10:30:00Z",
      "asset_count": 5,
      "total_size_bytes": 1048576
    }
  }
}
```

### Code Changes

1. **catalog.py**: Added `update_catalog_versions()` function
2. **dataset.py**: Modified `_batch_update_versions()` to return version info, call `update_catalog_versions()` from `finalize_datasets()`

---

## Implementation Plan

### Phase 1: Human Test Specification

**File:** `tests/specs/versioning_stress.md`

Create human-readable test specification defining:
1. What behaviors MUST be tested
2. Success criteria for each scenario
3. Edge cases that matter

This follows the project's TDD approach (ADR-0001) where specs drive implementation.

### Phase 2: End-to-End Integration Tests

**File:** `tests/integration/test_versioning_stress.py`

#### Test Class: `TestAddPopulatesVersions`

Verify `portolan add` creates properly-structured collection-level `versions.json`.

| Test | Verifies | Code Reference |
|------|----------|----------------|
| `test_add_single_file_creates_versions_json` | versions.json exists after add | `dataset.py:1174` |
| `test_add_populates_versions_array` | `versions` array is non-empty | `dataset.py:1216` |
| `test_add_sets_current_version` | `current_version` field is set | `dataset.py:1187-1192` |
| `test_add_includes_asset_metadata` | Assets have sha256, size_bytes, href | `dataset.py:1208-1213` |
| `test_add_1000_files_accumulates` | Scale test: many files in one add | Issue #339 (1900 files) |

#### Test Class: `TestAddThenPushSeesFiles`

Verify the full pipeline from `add` to `push`.

| Test | Verifies | Code Reference |
|------|----------|----------------|
| `test_push_after_add_reports_nonzero_files` | Push sees files to upload | `push.py:1508-1510` |
| `test_push_dry_run_lists_assets` | Dry-run shows asset paths | `push.py:851-854` |
| `test_push_reads_collection_level_versions` | Push reads correct file | `push.py:317` |

#### Test Class: `TestSnapshotModelAccumulation`

Verify each version is a complete snapshot (ADR-0005, plan `2026-03-05-fix-versions-snapshot-model.md`).

| Test | Verifies | Code Reference |
|------|----------|----------------|
| `test_second_add_preserves_first_assets` | v2 contains v1's assets | `versions.py:357-368` |
| `test_third_add_preserves_all_prior` | v3 contains v1+v2 assets | `versions.py:357-368` |
| `test_changes_field_only_shows_delta` | changes[] has new files only | `versions.py:443-472` |
| `test_unchanged_file_readd_is_noop` | Idempotent re-add | `versions.py:376-379` |

#### Test Class: `TestPushPullDivergence`

Verify conflict detection and handling.

| Test | Verifies | Code Reference |
|------|----------|----------------|
| `test_remote_ahead_pull_downloads` | Pull gets new remote versions | `pull.py:313` |
| `test_local_ahead_pull_warns` | Pull refuses without --force | `pull.py:515-536` |
| `test_diverged_state_requires_force` | Both ahead → conflict | `pull.py:545-558` |
| `test_push_conflict_on_etag_mismatch` | Concurrent push detected | `push.py:807-813` |

#### Test Class: `TestCorruptionRecovery`

Verify handling of malformed data.

| Test | Verifies | Code Reference |
|------|----------|----------------|
| `test_truncated_versions_json_rejected` | Invalid JSON fails cleanly | `versions.py:144-146` |
| `test_missing_versions_field_rejected` | Schema validation works | `versions.py:164-168` |
| `test_missing_asset_fields_rejected` | Asset validation works | `versions.py:173-184` |
| `test_unknown_fields_ignored` | Forward compatibility | `versions.py:151-213` |

### Phase 3: Unit Test Extensions

**File:** `tests/unit/test_versions.py` (extend existing)

Add to `TestSnapshotModel` class (line 1342):

| Test | Verifies |
|------|----------|
| `test_add_version_with_100_assets` | Scale: many assets per version |
| `test_add_version_with_50_versions` | Scale: many versions in history |
| `test_add_version_remove_then_readd_same_file` | Remove + re-add cycle |
| `test_snapshot_asset_count_equals_cumulative` | Property: |assets| = sum of all added |

### Phase 4: Fixtures

**File:** `tests/conftest.py` (extend existing)

Add new fixtures after line 301 (`fresh_catalog_no_versions`):

```python
@pytest.fixture
def catalog_with_multiple_versions(tmp_path: Path) -> Path:
    """Catalog with 3 versions for divergence testing."""
    # See existing pattern at line 238: catalog_with_versions_for_dry_run

@pytest.fixture
def catalog_with_100_assets(tmp_path: Path) -> Path:
    """Catalog with many assets for scale testing."""
```

**File:** `tests/fixtures/metadata/versions/` (extend existing)

| Fixture File | Purpose |
|--------------|---------|
| `versions_diverged_local.json` | Local has v1.1.0, missing v1.0.1 |
| `versions_diverged_remote.json` | Remote has v1.0.1, missing v1.1.0 |
| `versions_100_assets.json` | Single version with 100 assets |
| `versions_empty_array.json` | Valid schema but `versions: []` |

---

## Exact File References

### Files to Create

| File | Purpose |
|------|---------|
| `tests/specs/versioning_stress.md` | Human test specification |
| `tests/integration/test_versioning_stress.py` | Integration tests |
| `tests/fixtures/metadata/versions/versions_diverged_local.json` | Fixture |
| `tests/fixtures/metadata/versions/versions_diverged_remote.json` | Fixture |
| `tests/fixtures/metadata/versions/versions_100_assets.json` | Fixture |
| `tests/fixtures/metadata/versions/versions_empty_array.json` | Fixture |

### Files to Modify

| File | Line | Change |
|------|------|--------|
| `tests/conftest.py` | after 301 | Add `catalog_with_multiple_versions` fixture |
| `tests/unit/test_versions.py` | after 1474 | Add scale/edge case tests to `TestSnapshotModel` |

### Key Code References (for assertions)

| What | File | Line | Code |
|------|------|------|------|
| versions.json path (add) | `dataset.py` | 1174 | `versions_path = collection_dir / "versions.json"` |
| versions.json path (push) | `push.py` | 317 | `versions_path = catalog_root / collection / "versions.json"` |
| Snapshot accumulation | `versions.py` | 357-368 | `merged_assets = dict(versions_file.versions[-1].assets)` |
| Idempotent skip | `versions.py` | 376-379 | `if not changes and not removed...return versions_file` |
| Push conflict detection | `push.py` | 807-813 | `mode={"e_tag": etag}` |
| Pull divergence check | `pull.py` | 311-313 | `is_diverged = bool(local_only) and bool(remote_only)` |

---

## Test Patterns to Follow

### CLI Invocation (from `tests/integration/test_push_integration.py`)

```python
from click.testing import CliRunner
from portolan_cli.cli import cli

runner = CliRunner()
result = runner.invoke(
    cli,
    ["add", str(data_path), "--catalog", str(catalog_root)],
    catch_exceptions=False,
)
assert result.exit_code == 0
```

### S3 Mocking (from `tests/integration/test_s3_moto.py:26-74`)

```python
from moto.server import ThreadedMotoServer

@pytest.fixture(scope="module")
def moto_server() -> Generator[str, None, None]:
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    endpoint_url = f"http://127.0.0.1:{server._server.server_port}"
    yield endpoint_url
    server.stop()

@pytest.fixture
def s3_bucket(moto_server: str) -> Generator[tuple[str, str], None, None]:
    client = boto3.client("s3", endpoint_url=moto_server, ...)
    client.create_bucket(Bucket=bucket_name)
    yield bucket_name, moto_server
```

### Catalog Fixture (from `tests/conftest.py:238-297`)

```python
@pytest.fixture
def catalog_with_versions_for_dry_run(tmp_path: Path) -> Path:
    catalog_root = tmp_path / "catalog"
    collection_dir = catalog_root / "test_collection"
    collection_dir.mkdir(parents=True)

    # .portolan/config.yaml (sentinel per ADR-0029)
    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("catalog_id: test-catalog\n")

    # Collection-level versions.json
    versions_data = {
        "spec_version": "1.0.0",
        "current_version": "1.0.0",
        "versions": [...]
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions_data))

    return catalog_root
```

---

## Success Criteria

1. **All new tests pass** with `pytest tests/integration/test_versioning_stress.py -v`
2. **Existing tests still pass** with `pytest tests/unit/test_versions.py -v`
3. **#339 scenario is covered**: Test that `add` of 1000+ files results in non-empty `versions[]`
4. **Divergence scenarios covered**: Tests for local-ahead, remote-ahead, and diverged states
5. **No new dependencies**: Use existing moto, pytest, click.testing

---

## Implementation Order

1. Create `tests/specs/versioning_stress.md` (TDD anchor)
2. Create fixture files in `tests/fixtures/metadata/versions/`
3. Add fixtures to `tests/conftest.py`
4. Create `tests/integration/test_versioning_stress.py` with test stubs (all failing)
5. Run tests to verify they fail as expected
6. Implement tests one class at a time
7. Extend `tests/unit/test_versions.py` with scale tests
8. Final pass: run full test suite

---

## Out of Scope

- Fixing catalog-root `versions.json` to aggregate collections (separate issue)
- Concurrent write stress tests (requires process-level isolation)
- Network failure injection (future work)
- Mutation testing integration (handled by nightly CI)
