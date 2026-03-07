# ADR-0030: Vendor ISO 3166-1 Alpha-3 Codes for Scan Validation

## Status

Accepted

## Context

`portolan scan` warns on uppercase directory names because STAC recommends lowercase IDs.
However, ISO 3166-1 alpha-3 country codes (USA, GBR, CHN, etc.) are uppercase BY CONVENTION.

When scanning hive-partitioned datasets like GAUL admin boundaries:
```
by_country/
├── USA/USA.parquet
├── GBR/GBR.parquet
├── CHN/CHN.parquet
... (265 countries)
```

This generates 265 warnings that are pure noise. Users should not be told to rename
`USA/` to `usa/` - that loses semantic meaning and contradicts ISO standards.

Additionally, geospatial datasets often include disputed territory codes following
the `x[A-Z]{2}` pattern (e.g., `xAB`, `xJK`) which are conventions in FAO/GAUL data.

## Decision

1. **Vendor ISO 3166-1 alpha-3 codes** as a frozenset in `constants.py`
2. **Source:** FAO GAUL 2024 dataset (authoritative, used globally)
3. **Accept `x[A-Z]{2}` pattern** for disputed territory codes
4. **Skip uppercase warning** for directory names matching these patterns

## Implementation

```python
# In portolan_cli/constants.py
ISO_ALPHA3_CODES: frozenset[str] = frozenset({
    "ABW", "AFG", ..., "ZWE"  # 249 codes from FAO GAUL
})

# In scan validation
def is_valid_uppercase_id(name: str) -> bool:
    """Check if uppercase name is a known ISO code or disputed territory."""
    if name in ISO_ALPHA3_CODES:
        return True
    # Disputed territory pattern: x + 2 uppercase letters
    if len(name) == 3 and name[0] == 'x' and name[1:].isupper():
        return True
    return False
```

## Consequences

### Positive
- No external runtime dependency
- Authoritative source (FAO)
- Covers 99%+ of real geospatial data naming conventions
- Reduces warning noise from 265 to 0 for GAUL-style data

### Negative
- ~249 codes to maintain (but changes are rare - new countries are infrequent)
- Non-ISO uppercase like `FOO` still generates warning (correct behavior)

### Maintenance

ISO 3166-1 changes rarely (~1 new country per decade). When it does:
1. Update `ISO_ALPHA3_CODES` in `constants.py`
2. Source: https://www.iso.org/iso-3166-country-codes.html

## References

- [ISO 3166-1 alpha-3](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3)
- [FAO GAUL dataset](https://data.apps.fao.org/catalog/dataset/iso-3-code-list-global-region-country)
- GitHub Issue #180
