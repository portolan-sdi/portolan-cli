# ADR-0040: Unified Progress Output Model

## Status
Accepted

## Context

Long-running operations (`add`, `push`) produce inconsistent output:
- `add` prints per-file "Adding: X" lines (27k lines for large catalogs)
- `push` has silent setup phases and mixes Rich progress bars with line spam

Users need liveness signals (not hung), but verbose output overwhelms terminals and is useless for agents.

## Decision

Adopt a **"progress + summary"** model for all long-running commands:

1. **Setup phase:** Brief status lines for multi-second operations (e.g., "Collecting 27,000 files...")
2. **Main operation:** Rich progress bar with `transient=True` (disappears on completion)
3. **Completion:** Summary line with counts (e.g., "✓ Added 27,000 files to 10 collections")
4. **Errors:** Print immediately (fail-fast for fatal errors)
5. **Warnings:** Batch at end (max 100, then "...and N more")

Verbosity levels:
- Default: progress bar + errors + summary
- `--verbose/-v`: per-file output (current behavior, opt-in)
- `--json`: structured output (no Rich, full machine-readable results)

## Consequences

**Positive:**
- Consistent UX across commands
- Clean terminal output by default
- Liveness signals prevent "is it hung?" confusion
- Agent-friendly (JSON mode unchanged)

**Negative:**
- Verbose mode required for debugging per-file issues
- Progress reporters need total counts upfront (may add latency for pre-counting)

## Alternatives Considered

1. **Failure-only (print nothing during operation):** Rejected—silent operations appear hung.
2. **Log file for verbose output:** Adds complexity; `--verbose` is simpler.
