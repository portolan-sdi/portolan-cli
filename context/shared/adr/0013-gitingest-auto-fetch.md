# ADR-0013: Auto-fetch dependency docs via gitingest

## Status
Accepted

## Context
Portolan-cli orchestrates core libraries (geoparquet-io, rio-cogeo, obstore) that have evolving APIs. AI agents trained on older data may generate incorrect code based on stale training data. CLAUDE.md already instructs agents to use gitingest for these libraries, but this relies on the agent remembering to do so.

We need a mechanism that automatically provides current API documentation when agents work with code that uses these libraries.

## Decision
Add a `PostToolUse` hook on the `Read` tool that:
1. Detects imports of `geoparquet_io`, `rio_cogeo`, or `obstore` in Python files
2. Runs `gitingest` to fetch current source/docs from GitHub
3. Compresses output via Distill MCP (if available)
4. Injects the docs as a `systemMessage` for the agent to use
5. Caches fetched libs per-session to avoid redundant fetches

The hook is implemented as:
- `.claude/hooks/post-read-inject-docs.sh` — Bash wrapper for Claude hook system
- `scripts/fetch_lib_docs.py` — Python logic for parsing, fetching, caching

Add `gitingest` as a dev dependency.

## Consequences

**Positive:**
- Agents always have current API documentation for core dependencies
- No reliance on agent memory or CLAUDE.md instructions
- Compressed output reduces token usage
- Session caching prevents redundant network calls

**Negative:**
- Adds ~2-5 seconds latency on first detection per library per session
- Requires `gitingest` as dev dependency
- Hook complexity: two files (bash wrapper + Python script)

**Neutral:**
- All failures are soft (non-blocking) — hook never prevents Claude from working

## Alternatives considered

1. **Session-start hook** — Fetch all docs at session start regardless of need. Rejected: wasteful if not working on relevant code.

2. **Reminder-only hook** — Just remind agent to use gitingest manually. Rejected: still relies on agent to take action.

3. **Keyword detection in prompts** — Trigger on user mentioning library names. Rejected: less precise than import detection.
