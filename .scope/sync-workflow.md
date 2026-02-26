# Sync Workflow: Dependency-Ordered To-Do List

## TIER 1: Foundational (blocks everything else)

### 1. ADR: catalog.json location

**WHY FIRST:** Issue #94 blocks dataset operations. Every other component needs to know where catalog.json lives.

**DECISION NEEDED:**
- Option A: Root level (`./catalog.json`) — STAC standard, user-visible
- Option B: Inside `.portolan/` — keeps metadata contained

**SCOPE:**
- Write ADR documenting decision
- Update `init_catalog()` to match
- Update all code paths that read catalog.json
- Close issue #94

---

### 2. ADR: Git-style implicit tracking

**WHY SECOND:** Determines entire registration model. Blocks #3.

**DECISIONS NEEDED:**
- Confirm: subdirectory = collection (no explicit add)
- Confirm: files in catalog dir = automatically tracked
- Confirm: remove = delete file

**SCOPE:**
- Write ADR documenting git-style model
- Deprecate/remove `dataset add/remove` commands
- Update architecture.md

---

## TIER 2: Core Workflow (blocks sync working end-to-end)

### 3. Implement collection registration in check

**WHY:** Currently scan discovers files, check converts them, but NOTHING creates `<collection>/versions.json`. Push fails without this.

**WHERE:** In check command (after conversion, before push)

**SCOPE:** (per ADR-0023: STAC at root, internals in .portolan/)
- check reads scan results
- For each collection (subdirectory with geo files):
  - Create `<collection>/` at root level
  - Create `<collection>/collection.json` (STAC metadata)
  - Create `<collection>/versions.json` with file checksums
- Wire into sync workflow

---

## TIER 3: Configuration (enables better UX)

### 4. ADR: Remote URL handling

**DECISION:** Hybrid approach
- CLI arg always works: `portolan sync s3://bucket/path`
- Config stores default: `portolan config set remote s3://bucket/path`
- Then just: `portolan sync`

**SCOPE:**
- Write ADR
- Design config file format (`.portolan/config.yaml` or similar)

---

### 5. Implement config system

**DEPENDS ON:** #4

**SCOPE:**
- `portolan config set <key> <value>`
- `portolan config get <key>`
- `portolan config list`
- Read/write `.portolan/config.yaml`
- Support: remote URL, AWS profile, default collection

---

## TIER 4: Documentation (parallel after Tier 2)

### 6. Update README and docs

**SCOPE:**
- Remove fake commands (`remote add`, `dataset add` in Quick Start)
- Document actual workflow: `init` → (copy files) → `sync`
- Document bucket specification (CLI arg for now)

---

### 7. Update ROADMAP

**SCOPE:**
- Remove "drift check" (= `pull --dry-run`)
- Remove "refresh" (= `sync`)
- Keep "prune"
- Mark `dataset add/remove` as removed
- Update milestone checkmarks

---

## TIER 5: New Commands (nice to have, parallel)

### 8. Implement list command

**PURPOSE:** Tree view of catalog structure

**SCOPE:**
- `portolan list [--depth N] [--collection NAME]`
- Show: collections, files, sizes, formats
- Like `tree` but with geo metadata

---

### 9. Implement info command

**PURPOSE:** Metadata display for catalog/collection/asset

**SCOPE:**
- `portolan info` — catalog level
- `portolan info --collection NAME` — collection level
- `portolan info FILE` — asset level
- Show: bbox, features, format, versions, etc.

---

## TIER 6: Testing

### 10. Add integration tests

**SCOPE:**
- End-to-end: init → copy files → sync → verify remote
- Push/pull with real or mocked S3
- Currently 0% coverage on sync.py, push.py, pull.py

---

## Summary

| Tier | Items | Blocks |
|------|-------|--------|
| 1 | catalog.json location, git-style tracking | Everything |
| 2 | Collection registration in check | Sync working |
| 3 | Remote URL ADR, config system | Better UX |
| 4 | README, ROADMAP updates | User clarity |
| 5 | list, info commands | Discoverability |
| 6 | Integration tests | Confidence |
