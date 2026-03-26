# ADR-0036: Collection Summaries Strategy

## Status
Accepted

## Context
STAC collections support `summaries` for discovery. Need to decide: which fields, auto-detect vs explicit, numeric aggregation?

## Decision
1. **Hybrid field detection:**
   - Explicit list for core fields with known strategies
   - Auto-detect extension-prefixed fields (`proj:`, `raster:`, `vector:`) not in explicit list
2. **Strategies:**
   - `DISTINCT` for categorical: `proj:code`, `vector:geometry_types`
   - `RANGE` for numeric: `gsd`
   - Default to `DISTINCT` for auto-detected fields
3. **No numeric aggregation across items** — band statistics stay at item level only
4. **Use PySTAC Summarizer** with configured field dict

## Consequences
- Predictable core summaries users can rely on
- Extensible for new extensions without code changes
- Collection summaries are for filtering/discovery, not scientific analysis
- Item-level statistics remain the source of truth for per-asset metrics

## Alternatives considered
- **Pure auto-detection:** Rejected — unpredictable output, hard to test
- **Aggregate band stats to collection:** Rejected — semantically questionable (mean of means?), rarely useful
