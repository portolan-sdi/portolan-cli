# Fix versions.json Snapshot Model

**Date:** 2026-03-05
**Issues:** #141 (duplicate add), #147 (clone downloads 1 file)
**Status:** Ready for implementation

## Problem

`add_version()` creates versions with only the newly-added assets, but the design (ADR-0005) specifies each version should be a complete snapshot of all assets.

**Current behavior:**
- Add file A → version 1.0.0 with `{A}`
- Add file B → version 1.0.1 with `{B}` only

**Expected behavior:**
- Add file A → version 1.0.0 with `{A}`
- Add file B → version 1.0.1 with `{A, B}`

This causes:
1. **#141:** `is_current()` only checks latest version, misses previously-added files, creates duplicates
2. **#147:** `diff_versions()` only reads latest version's assets, downloads only 1 file

## Solution

Fix `add_version()` in `versions.py` to merge new assets with previous version's assets.

```python
# Before (buggy)
new_version = Version(assets=assets, ...)

# After (fixed)
if versions_file.versions:
    merged = versions_file.versions[-1].assets.copy()
    merged.update(assets)
else:
    merged = assets
new_version = Version(assets=merged, ...)
```

The `changes` field continues to track what was added/modified in each version.

## Removal Handling

When files are removed via `portolan rm`, the next version should omit them from assets. Add optional `removed: set[str]` parameter to `add_version()`.

## Cascading Fixes

Once `add_version()` is fixed:
- `is_current()` works correctly (latest version has all assets)
- `diff_versions()` works correctly (latest version has all assets)
- No changes needed to these functions

## Tests Required

1. `test_add_same_file_twice_no_duplicate` — Adding unchanged file skips correctly
2. `test_add_multiple_files_accumulates` — Each version contains all assets
3. `test_clone_downloads_all_assets` — Clone gets complete set
4. `test_remove_file_omits_from_next_version` — Removed files excluded

## Files to Modify

| File | Change |
|------|--------|
| `portolan_cli/versions.py` | Fix `add_version()` to merge assets |
| `tests/unit/test_versions.py` | Add 4 tests |

## No Migration Needed

No existing users with corrupted data, so no migration logic required.
