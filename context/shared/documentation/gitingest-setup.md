# Gitingest Integration Setup

**Status**: Ready for use (manual workflow—no hooks required for MVP)

## What We've Configured

### 1. Project-Level (CLAUDE.md)
- Added "Dependency Research Workflow" section
- Documented Context7 + Gitingest + Distill combination
- Provided step-by-step examples for geoparquet-io, rio-cogeo, gpio-pmtiles

### 2. Global Level (/home/nissim/.claude/CLAUDE.md)
- Added Gitingest overview
- Documented when to use Gitingest vs Context7
- Explained tool ordering (Context7 first, then Gitingest, then Distill)

### 3. Reference Guide (context/shared/documentation/context-guide.md)
- Quick links to all three dependencies
- Workflow examples by question type
- Copy-paste gitingest URLs
- Token optimization examples

## How Claude Will Use This (Current Workflow)

1. **API question** → Claude automatically uses Context7
   ```
   resolve-library-id("geoparquet-io")
   query-docs(libraryId, "question")
   ```

2. **Implementation question** → Claude can now add Gitingest step
   ```
   # Already in context:
   gitingest https://github.com/geoparquet/geoparquet-io
   ```

3. **Large output** → Claude can compress with Distill
   ```
   mcp__distill__auto_optimize(output, hint="code")
   ```

## Optional: Automated Hooks (Future)

If you want to automate dependency context generation:

### Pre-commit Hook
Could validate that dependency context is up-to-date:
```bash
#!/bin/bash
# .git/hooks/pre-commit
# Check if context/shared/documentation/context-guide.md exists and is recent
if [ -f context/shared/documentation/context-guide.md ]; then
  age=$(($(date +%s) - $(stat -f%m context/shared/documentation/context-guide.md)))
  if [ $age -gt 2592000 ]; then  # 30 days
    echo "⚠️  CONTEXT_GUIDE.md is stale—consider updating dependency links"
  fi
fi
```

### Workflow Automation (CI)
Could generate fresh gitingest digests on a schedule:
```yaml
name: Update Dependency Context
on:
  schedule:
    - cron: '0 0 * * 0'  # Weekly
jobs:
  update-context:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - run: |
          gitingest https://github.com/geoparquet/geoparquet-io > /tmp/geoparquet-io.txt
          # Could commit or store as artifact
```

**Decision**: Skipping automated hooks for now. The manual workflow (CLAUDE.md + CONTEXT_GUIDE.md) is sufficient and gives Claude explicit control over when/how to fetch context.

## Files Modified/Created

| File | Change |
|------|--------|
| `CLAUDE.md` (project) | Added "Tool Usage" and "Dependency Research Workflow" |
| `CLAUDE.md` (global) | Added Gitingest guidelines + tool ordering |
| `context/shared/documentation/context-guide.md` | NEW: Quick reference for Context7 + Gitingest + Distill |
| `context/shared/documentation/gitingest-setup.md` | NEW: This file |

## Next Steps

1. ✅ Claude can now automatically use Context7 for API docs
2. ✅ Claude knows to use Gitingest for implementation exploration
3. ✅ Claude knows to use Distill for token efficiency
4. 📌 (Optional) Add automated hook if you want scheduled context updates

## Testing the Setup

**Test 1: API Question** (Should use Context7 automatically)
```
"How do I convert a GeoJSON to GeoParquet?"
→ Claude should invoke resolve-library-id + query-docs
```

**Test 2: Implementation Question** (Should offer Gitingest)
```
"How does geoparquet-io handle invalid geometries internally?"
→ Claude could suggest gitingest step
```

**Test 3: Large Output** (Should use Distill)
```
"Paste [large gitingest output here] and find error handling code"
→ Claude should use mcp__distill__auto_optimize to compress
```

---

**Summary**: Gitingest is configured for manual on-demand use. No hooks required. Claude has clear instructions in CLAUDE.md for when/how to use it alongside Context7 and Distill.
