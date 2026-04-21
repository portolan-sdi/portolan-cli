# Feature: Versioning Stress Tests

**Issue:** [#339](https://github.com/portolan-sdi/portolan-cli/issues/339) - versions.json not populated after portolan add
**Related:** ADR-0005 (versions.json as single source of truth)
**Status:** ✅ FIXED

## Context

Two separate `versions.json` files exist per ADR-0005:
- **Catalog-level:** `<catalog>/versions.json` — aggregate view of all collections (updated by add)
- **Collection-level:** `<catalog>/<collection>/versions.json` — detailed version history (updated by add)

Issue #339 was a real bug: catalog-level versions.json was created empty and never updated.
The fix adds `update_catalog_versions()` to populate collection state after each add.

---

## Add Populates Versions

### Happy Path
- [ ] `portolan add` of single file creates collection-level `versions.json`
- [ ] `versions` array is non-empty after add
- [ ] `current_version` field is set to the new version
- [ ] Assets have required fields: `sha256`, `size_bytes`, `href`

### Scale
- [ ] Adding 1000 files in one command populates all in `versions[]`
- [ ] Adding 100 files across 10 invocations accumulates correctly

---

## Add Then Push Pipeline

### Happy Path
- [ ] `push` after `add` reports non-zero files to upload
- [ ] `push --dry-run` lists all asset paths from versions.json
- [ ] `push` reads collection-level versions.json (not catalog-level)

### Edge Cases
- [ ] `push` with no prior `add` reports 0 files
- [ ] `push` after `add` then `remove` reports correct count

---

## Snapshot Model Accumulation

Per ADR-0005, each version is a complete snapshot containing all assets.

### Happy Path
- [ ] Second `add` preserves first version's assets in snapshot
- [ ] Third `add` preserves all prior assets (v1 + v2)
- [ ] `changes[]` field contains only delta (newly added files)

### Idempotency
- [ ] Re-adding unchanged file is no-op (no new version created)
- [ ] Re-adding modified file creates new version with updated hash

### Edge Cases
- [ ] Remove then re-add same file: file appears in new version
- [ ] Add, remove, add different file: correct asset count at each version

---

## Push/Pull Divergence

### Remote Ahead
- [ ] `pull` downloads new remote versions not present locally
- [ ] Local versions preserved after pull

### Local Ahead
- [ ] `pull` without `--force` warns when local is ahead
- [ ] `push` succeeds when local is ahead of remote

### Diverged State
- [ ] Both local and remote ahead → conflict detected
- [ ] `--force` flag required to resolve divergence
- [ ] ETag mismatch on concurrent push detected

---

## Corruption Recovery

### Invalid JSON
- [ ] Truncated versions.json rejected with clear error
- [ ] Empty file rejected
- [ ] Valid JSON but invalid schema rejected

### Missing Required Fields
- [ ] Missing `versions` field → validation error
- [ ] Missing asset `sha256` → validation error
- [ ] Missing asset `size_bytes` → validation error
- [ ] Missing asset `href` → validation error

### Forward Compatibility
- [ ] Unknown fields in versions.json are ignored (not errors)
- [ ] Extra fields in assets are preserved through read/write cycle

---

## Invariants

These properties must ALWAYS hold:

1. **Asset count consistency:** `len(version.assets) >= len(previous_version.assets)` unless removals
2. **Hash stability:** Same file content → same sha256 (deterministic)
3. **Version ordering:** Versions are monotonically increasing (semver or timestamp)
4. **Snapshot completeness:** Any version can be restored independently
5. **Idempotency:** `add` of unchanged file produces no new version

---

## Code References

| Behavior | File | Line | Code Pattern |
|----------|------|------|--------------|
| versions.json path (add) | `dataset.py` | 1174 | `versions_path = collection_dir / "versions.json"` |
| versions.json path (push) | `push.py` | 317 | `versions_path = catalog_root / collection / "versions.json"` |
| Snapshot accumulation | `versions.py` | 357-368 | `merged_assets = dict(versions_file.versions[-1].assets)` |
| Idempotent skip | `versions.py` | 376-379 | `if not changes and not removed...return versions_file` |
| Push conflict detection | `push.py` | 807-813 | `mode={"e_tag": etag}` |
| Pull divergence check | `pull.py` | 311-313 | `is_diverged = bool(local_only) and bool(remote_only)` |
