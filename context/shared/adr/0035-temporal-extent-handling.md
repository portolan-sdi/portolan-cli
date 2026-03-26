# ADR-0035: Temporal Extent Handling

## Status
Accepted (Updated)

## Context
Current implementation defaults `datetime` to "now" when unspecified. This conflates "when added to catalog" with "when data was collected" — often years apart for geospatial data.

**STAC Constraint:** STAC 1.0.0+ requires items to have temporal extent — either `datetime` or both `start_datetime` AND `end_datetime`. Items cannot have truly "null" temporal extent.

## Decision
1. **Default to current time BUT mark as provisional** — STAC requires temporal extent, so we default to `_now_utc()` and add `portolan:datetime_provisional: true` to properties
2. **Explicit datetime clears flag:** When user provides `--datetime`, we use their value and don't set the provisional flag
3. **Prompt in interactive mode:** "Enter acquisition datetime (ISO 8601, or press Enter to skip):"
4. **Accept flexible formats:** ISO 8601, `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS` — normalize to ISO
5. **Flag incomplete:** Items with `portolan:datetime_provisional: true` are flagged in `portolan check` as "missing temporal metadata"
6. **CLI flag:** `--datetime 2024-01-15` for non-interactive use
7. **Per-command scope:** `--datetime` applies to ALL items in a single `portolan add` command. For items with different acquisition dates, users must run separate commands:
   ```bash
   portolan add census/2020/ --datetime 2020-04-01
   portolan add census/2023/ --datetime 2023-04-01
   ```

## Consequences
- All items are STAC-valid (always have temporal extent)
- Provisional marker distinguishes "user-provided" from "auto-generated placeholder"
- `portolan check` can report incomplete metadata without blocking
- Existing catalogs may have incorrect "now" datetimes — no migration, users can fix manually

## Alternatives considered
- **Keep "now" default without marker:** Rejected — no way to distinguish placeholder from real datetime
- **Use `datetime: null`:** Rejected — violates STAC spec (requires start_datetime + end_datetime)
- **Require datetime (fail without it):** Rejected — too strict; some data genuinely has unknown dates
