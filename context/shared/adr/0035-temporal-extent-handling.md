# ADR-0035: Temporal Extent Handling

## Status
Accepted

## Context
STAC requires items to have temporal extent — either `datetime` or both `start_datetime` AND `end_datetime`. However, geospatial data often has unknown acquisition dates, especially legacy datasets.

The previous approach of defaulting to "now" conflated "when added to catalog" with "when data was collected" — often years apart.

## Decision
1. **Default to null temporal extent** — When `--datetime` not provided, set `datetime: null` with `start_datetime: null, end_datetime: null` (open interval)
2. **Mark as provisional** — Items without explicit datetime get `portolan:datetime_provisional: true` in properties
3. **Accept flexible formats** — ISO 8601, `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS` — normalize to ISO
4. **CLI flag** — `--datetime 2024-01-15` for explicit temporal extent
5. **Per-command scope** — `--datetime` applies to ALL items in a single `portolan add` command:
   ```bash
   portolan add census/2020/ --datetime 2020-04-01
   portolan add census/2023/ --datetime 2023-04-01
   ```
6. **Flag in check** — `portolan check` warns about items with `portolan:datetime_provisional: true`

## Consequences
- Items without `--datetime` are STAC-valid (open temporal interval)
- No lies in metadata — null means unknown, not "when I added it"
- `portolan check` surfaces incomplete metadata
- Users must provide datetime if they want temporal search to work

## Alternatives considered
- **Default to now()**: Rejected — lies about acquisition time
- **Default to now() with provisional marker**: Rejected — still puts fake data in datetime field
- **Require datetime (fail without it)**: Rejected — too strict for legacy data with unknown dates
