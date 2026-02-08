# Auto-fetch Dependency Docs via Gitingest

**Date:** 2025-02-08
**Status:** Approved
**Author:** Claude + Nissim

## Overview

A `PostToolUse` hook that auto-fetches current API documentation for core dependencies (geoparquet-io, rio-cogeo, obstore) when Claude reads Python files that import them.

## Problem

Core dependencies have evolving APIs. AI agents trained on older data may generate incorrect code. We need agents to always have current documentation.

## Solution

### Architecture

```
.claude/hooks/
├── post-read-inject-docs.sh    # Bash wrapper for Claude hook system
scripts/
├── fetch_lib_docs.py           # Python logic (parsing, fetching, caching)
```

### Hook Logic Flow

1. Hook receives JSON with `tool_input.file_path` and `tool_result.content`
2. Skip if not a `.py` file
3. Parse content for imports:
   - `from geoparquet_io` or `import geoparquet_io`
   - `from rio_cogeo` or `import rio_cogeo`
   - `from obstore` or `import obstore`
4. For each detected library (if not already fetched this session):
   - Run: `gitingest https://github.com/{org}/{repo}`
   - If Distill MCP available: compress output
   - Cache result to avoid re-fetching
5. Return `systemMessage` with the docs

### Repo Mapping

```python
REPOS = {
    "geoparquet_io": "geoparquet/geoparquet-io",
    "rio_cogeo": "cogeotiff/rio-cogeo",
    "obstore": "developmentseed/obstore",
}
```

### Session Caching

Store fetched libs in `/tmp/claude-libdocs-fetched-{session}` to avoid re-fetching within a session.

### Error Handling

All failures are soft (non-blocking):
- gitingest not installed → Log warning, skip
- Network failure → Log warning, skip
- Distill not available → Use uncompressed output
- File parse error → Skip silently

## Testing Strategy

**Unit tests:**
- Import detection for all three libraries
- Various import styles (from X, import X, from X.Y)
- Session cache prevents refetch

**Integration tests:**
- Real gitingest fetch (marked `@pytest.mark.network`)
- End-to-end hook simulation

## Dependencies

Add to `pyproject.toml`:
```toml
dev = [
    "gitingest>=0.1.0",
]
```

## Related

- ADR-0013: Auto-fetch dependency docs via gitingest
