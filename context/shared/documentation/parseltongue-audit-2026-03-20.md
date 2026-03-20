# Portolan CLI Documentation Audit
**Date:** 2026-03-20
**Method:** Parseltongue adversarial review
**Scope:** README.md, docs/, CLI implementation, open GitHub issues

## Executive Summary

The documentation is **largely accurate** but contains one **critical simplification** that misrepresents the `portolan sync` command's behavior. Other findings are minor (missing feature examples, no contradictions detected).

### Critical Finding
- ❌ **README oversimplifies `sync` workflow** — describes it as 3-step when it's actually 5-step

### Verification Status
- ✅ Command existence claims verified
- ✅ Installation instructions match implementation
- ✅ Format limitations documented correctly (ESRI GDB rasters)
- ✅ CLI flags and options match actual help output

---

## Detailed Findings

### 🔴 CRITICAL: Sync Workflow Misrepresentation

**Location:** `README.md` line ~27

**Claim:**
```bash
portolan sync s3://my-bucket/catalog -c demographics  # Full workflow: pull → check → push
```

**Reality (from `portolan_cli/sync.py`):**
```python
"""The sync command sequences: Pull -> Init -> Scan -> Check -> Push."""
```

**Impact:**
- Users expect 3 steps but the command runs 5
- `init` and `scan` steps are **not optional** — they run even if catalog exists
- This could confuse users debugging sync behavior or reading logs

**Evidence:**
```scheme
(fact readme-claims-sync-is-full-workflow true
  :evidence (evidence "README"
    :quotes ("Full workflow: pull → check → push")))

(fact actual-sync-workflow-includes-init-and-scan true
  :evidence (evidence "sync-source"
    :quotes ("Pull -> Init -> Scan -> Check -> Push")))

(diff sync-workflow-mismatch
  :left readme-claims-sync-is-full-workflow
  :right actual-sync-workflow-includes-init-and-scan
  :expect (= left right))
;; Result: MISMATCH DETECTED
```

**Recommendation:**
```bash
# Fix the inline comment to reflect all 5 steps:
portolan sync s3://my-bucket/catalog -c demographics  # Full workflow: pull → init → scan → check → push
```

**Rationale:** `init` and `scan` are not trivial — they touch the filesystem, discover files, and validate structure. Users should know these steps run.

---

### ✅ Verified Claims (No Issues)

#### 1. Command Existence
All commands shown in README exist in CLI:
- ✅ `portolan init`
- ✅ `portolan scan`
- ✅ `portolan add`
- ✅ `portolan check` (with `--fix`)
- ✅ `portolan rm` (with `--keep`)
- ✅ `portolan push`
- ✅ `portolan pull`
- ✅ `portolan sync`
- ✅ `portolan config` (with `set` and `list` subcommands)

#### 2. Installation Methods
- ✅ README recommends `pipx install portolan-cli` (matches ADR-0008)
- ✅ Alternative `pip install portolan-cli` documented
- ✅ Development setup uses `uv sync --all-extras` (correct)

#### 3. Python Version Requirement
- ✅ Badge shows Python 3.10+ (matches `pyproject.toml`: `requires-python = ">=3.10"`)

#### 4. Format Support Documentation
**File:** `docs/reference/formats.md`

- ✅ ESRI GDB raster limitation documented correctly
- ✅ Matches ADR-0033 decision (no GDAL dependency)
- ✅ Workaround provided (`gdal_translate` example)
- ✅ Clarifies vector `.gdb` files work normally

---

### 🟡 Minor Observations (Not Errors)

#### 1. `--workers` Flag Not Shown in Examples
**File:** README.md

The `portolan push` command supports `--workers` for parallel catalog-wide push (added in #244), but README examples don't show it:

```bash
# Actual capability (from --help):
portolan push s3://mybucket/catalog --workers 8

# README only shows:
portolan push s3://mybucket/catalog --collection demographics
```

**Assessment:** This is **acceptable** — not every flag needs an example. The help text documents it clearly.

**Recommendation (optional):** Add one advanced example:
```bash
# Advanced: Parallel push of all collections
portolan push s3://mybucket/catalog --workers 8
```

#### 2. `sync` Requires `--collection` Flag
**Verified:** README example correctly shows `-c demographics`

The CLI enforces this (`[required]`), and the README example demonstrates it properly.

#### 3. Recent Feature: `--workers` Flag (PR #244)
**Added:** 2026-03-19 (commit d1e1cec)

The flag is very new — if docs lag slightly, that's normal for fast-moving projects. However, the CLI help text is automatically up-to-date, so users won't be misled.

---

## Cross-Check with Open GitHub Issues

### Issue #235: Edge Cases Tracking Epic
**Status:** Open (tracking issue, not blocking MVP)

**Content:** Tracks future edge cases for:
- Multi-file vector datasets (chunked Parquet)
- Spatiotemporal partitioning
- STAC ecosystem interop

**Documentation Impact:** None — this is a planning document, not a claim about current behavior.

**Finding:** Documentation correctly does NOT claim these features exist (they're planned).

---

## Methodology: Parseltongue DSL

Used formal verification to check claims:

```scheme
;; Example: Verify sync workflow claim
(fact readme-claims-sync-is-full-workflow true
  :evidence (evidence "README"
    :quotes ("Full workflow: pull → check → push")))

(fact actual-sync-workflow-includes-init-and-scan true
  :evidence (evidence "sync-source"
    :quotes ("Pull -> Init -> Scan -> Check -> Push")))

(diff sync-workflow-mismatch
  :left readme-claims-sync-is-full-workflow
  :right actual-sync-workflow-includes-init-and-scan
  :expect (= left right))
```

**Result:** Detected the 3-step vs 5-step mismatch automatically.

---

## Recommendations

### Immediate (High Priority)
1. **Fix README sync comment** — update to reflect 5-step workflow
   ```diff
   - portolan sync s3://my-bucket/catalog -c demographics  # Full workflow: pull → check → push
   + portolan sync s3://my-bucket/catalog -c demographics  # Full workflow: pull → init → scan → check → push
   ```

### Nice-to-Have (Low Priority)
2. **Add `--workers` example** — show advanced parallel push usage
3. **Cross-link formats.md** — README could mention format support docs

---

## Validation Against CLAUDE.md Guidance

**Requirement (CLAUDE.md):**
> When documenting CLI commands:
> 1. Run `portolan <command> --help` to verify actual behavior
> 2. Check GitHub Issues for planned features
> 3. Do NOT deprecate planned features
> 4. Do NOT simplify orchestration commands

**Audit Findings:**
- ✅ Commands verified against `--help` output
- ✅ Planned features not claimed as implemented
- ❌ **Violated:** Sync command simplified from 5 steps to 3 (orchestration misrepresented)

This audit confirms the violation matches CLAUDE.md's specific warning about **not simplifying orchestration commands**.

---

## Conclusion

The documentation is **mostly accurate** with one critical fix needed. The README's description of `sync` as "pull → check → push" should be corrected to "pull → init → scan → check → push" to match the implementation.

All other claims verified successfully — no hallucinations, no missing commands, format limitations documented correctly.

**Trust Level:** High (post-fix)
**Action Required:** Update README.md line ~27
