# Getting Up-to-Speed Context on Dependencies

This guide helps Claude quickly get context on the core Portolan dependencies.

> **Note**: The tools referenced here (Context7, Gitingest, Distill) are optional but recommended. See the "Fallback Options" section if they're not available.

## ⚠️ Important: geoparquet-io and rio-cogeo

These are **foundation libraries** that handle core data transformations. Subtle bugs here can silently corrupt geospatial data, so checking source code implementation (not just API docs) is valuable when debugging or working with edge cases.

**Recommended**: When working with these libraries, pair Context7 (API docs) with Gitingest (source code exploration) when available.

## Quick Links

### Gitingest URLs (Copy-Paste to Get Full Source)

```bash
# Vector format conversion
https://github.com/geoparquet/geoparquet-io

# Raster conversion to COG
https://github.com/cogeotiff/rio-cogeo

# PMTiles generation
https://github.com/geoparquet-io/gpio-pmtiles
```

### Context7 Queries (For Official API Docs)

```
resolve-library-id("geoparquet-io") → query-docs(libraryId, "your question")
resolve-library-id("rio-cogeo") → query-docs(libraryId, "your question")
```

## Workflow by Question Type

### "How do I use [library API]?"
→ **Context7 ONLY**

```
resolve-library-id("geoparquet-io")
query-docs(libraryId, "How do I convert GeoJSON to GeoParquet?")
```

### "How does [library] actually implement [feature]?"
→ **Context7 + Gitingest**

```
# 1. Get the official answer
resolve-library-id("geoparquet-io")
query-docs(libraryId, "How does it handle missing geometries?")

# 2. If you want implementation details:
gitingest https://github.com/geoparquet/geoparquet-io
# Then search the output for "geometry" handling
```

### "What's the actual error happening with [library]?"
→ **Gitingest + Search**

```
gitingest https://github.com/geoparquet/geoparquet-io
# Search for error message or problem area
# Use mcp__distill__auto_optimize to compress if large
```

## Token Efficiency

Large gitingest outputs can be compressed:

```python
# Before: 8,000 tokens
# After: ~2,400 tokens (70% savings)
mcp__distill__auto_optimize(gitingest_output, hint="code")
```

## Tips

- **Start with Context7**: It's faster and up-to-date
- **Add Gitingest if needed**: When you need to see the actual code
- **Use Distill always**: When pasting large code outputs
- **Reference specific files**: Instead of pasting entire repos, ask for specific modules/functions

## Examples

### Example 1: Understanding GeoParquet Schema

```
# Question: How does geoparquet-io handle invalid geometries?

# Step 1: Check official docs
resolve-library-id("geoparquet-io")
query-docs(libraryId, "How does it validate geometries?")

# Step 2: If docs aren't clear, explore source
gitingest https://github.com/geoparquet/geoparquet-io
# Search for "validate", "geometry", "invalid"

# Step 3: Compress before deep analysis
mcp__distill__auto_optimize(gitingest_output, hint="code")
```

### Example 2: Debugging COG Creation

```
# Question: Why is my COG creation failing with specific error?

# Step 1: Check rio-cogeo docs
resolve-library-id("rio-cogeo")
query-docs(libraryId, "error message here")

# Step 2: If Context7 doesn't help, explore rio-cogeo source
gitingest https://github.com/cogeotiff/rio-cogeo

# Step 3: Search for error handling + compress
mcp__distill__auto_optimize(gitingest_output, hint="code")
```

## Fallback Options

If the recommended tools aren't available:

| Tool Missing | Alternative |
|--------------|-------------|
| **Context7** | Use official documentation websites directly |
| **Gitingest** | Browse GitHub web UI, use `gh api`, or clone repos locally |
| **Distill** | Work with smaller code sections; ask for specific files rather than entire repos |

**Without gitingest**, you can still explore source code:
```bash
# Clone locally
git clone --depth 1 https://github.com/geoparquet/geoparquet-io /tmp/geoparquet-io

# Or fetch a specific file via gh
gh api repos/geoparquet/geoparquet-io/contents/src/geoparquet_io/core.py --jq '.content' | base64 -d
```

## Files Referenced

- Project CLAUDE.md: Tool usage and dependency workflow
- Global CLAUDE.md: Context7 and Gitingest guidelines
- This file: Practical examples and quick reference (`context/shared/documentation/context-guide.md`)
