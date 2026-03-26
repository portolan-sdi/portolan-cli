# ADR-0034: Statistics Computation Defaults

## Status
Accepted

## Context
Wave 2 adds statistics extraction for raster bands and parquet columns. Need to decide: computed by default? Which mode? Configurable?

## Decision
1. **Compute by default** — users expect metadata to be complete
2. **Raster:** Use `approx` mode (~100ms) via GDAL overviews, not `exact` (~10s full scan)
3. **Parquet:** Use PyArrow metadata only (min/max/null_count from footer, instant)
4. **No DuckDB** — users can do post-hoc analysis on their files
5. **Config setting:** `.portolan/config.yaml` allows override:
   ```yaml
   statistics:
     enabled: true      # default
     raster_mode: approx  # approx | exact | disabled
   ```

## Consequences
- Fast by default (sub-second for most files)
- Users wanting exact stats must opt-in via config
- No new dependencies (DuckDB deferred indefinitely)

## Alternatives considered
- **Opt-in stats:** Rejected — incomplete metadata is worse UX than slightly slower adds
- **DuckDB for extended stats:** Rejected — PyArrow covers STAC spec; post-hoc analysis is user's job
