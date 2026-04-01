# Metadata Defaults

When source data files lack certain metadata (like nodata values or temporal information), you can specify defaults in `metadata.yaml` that Portolan will use to fill the gaps.

## Use Cases

- **Aerial imagery without nodata**: Source COGs exported from Global Mapper or ArcGIS often lack nodata values, even when black (0) pixels represent no data
- **Historical datasets without dates**: Legacy data may not have acquisition dates embedded in file metadata
- **Bulk imports**: When adding many files from the same source, set collection-level defaults instead of per-file flags

## Setting Defaults

Add a `defaults` section to your `.portolan/metadata.yaml`:

```yaml
# .portolan/metadata.yaml

contact:
  name: "Data Team"
  email: "data@example.org"

license: "CC-BY-4.0"

# Data defaults - applied when auto-extraction fails
defaults:
  temporal:
    year: 2025              # All items default to 2025-01-01

  raster:
    nodata: 0               # Black pixels (0) are nodata
```

## Temporal Defaults

For datasets where all items share an acquisition period:

### Year Range (Recommended for Annual Datasets)

```yaml
defaults:
  temporal:
    year: 2025    # Produces datetime: 2025-01-01T00:00:00Z
```

### Explicit Date Range

```yaml
defaults:
  temporal:
    start: "2025-04-15"    # ISO format: YYYY-MM-DD
    end: "2025-05-30"
```

!!! note "Year vs Start/End"
    If both `year` and `start` are specified, `year` takes precedence. Use one or the other, not both.

### CLI Override

The `--datetime` flag always overrides metadata.yaml defaults:

```bash
# Uses metadata.yaml default
portolan add data/

# Overrides default for this specific add
portolan add data/ --datetime 2024-06-15
```

## Raster Nodata Defaults

For COG files where nodata wasn't set in the source:

### Uniform Nodata (All Bands)

```yaml
defaults:
  raster:
    nodata: 0    # Applied to all bands
```

### Per-Band Nodata

```yaml
defaults:
  raster:
    nodata: [0, 0, 255]    # R=0, G=0, B=255
```

!!! tip "When to Use Per-Band"
    Per-band nodata is useful when different bands use different sentinel values. For RGB imagery, uniform nodata (typically 0) is usually sufficient.

## Hierarchy and Inheritance

Defaults follow Portolan's hierarchical config pattern:

```
catalog/.portolan/metadata.yaml          # Catalog-level defaults
  └── collection/.portolan/metadata.yaml # Collection overrides
        └── subcatalog/.portolan/metadata.yaml  # Most specific wins
```

**Example**: Set nodata at catalog level, override temporal at collection level:

```yaml
# catalog/.portolan/metadata.yaml
defaults:
  raster:
    nodata: 0

# catalog/aerial-2025/.portolan/metadata.yaml
defaults:
  temporal:
    year: 2025
  # Inherits raster.nodata: 0 from parent
```

## Behavior Rules

| Scenario | Behavior |
|----------|----------|
| Source file has value | File value used (defaults don't override) |
| Source file lacks value | Default applied |
| CLI flag provided | CLI flag overrides default |
| No default, no source value | Field left null/empty |

**Key principle**: Defaults fill gaps, they don't override extracted data.

## Validation

Portolan validates defaults when loading metadata.yaml:

```yaml
# Valid
defaults:
  temporal:
    year: 2025              # Integer
    start: "2025-04-15"     # ISO date string

# Invalid - will error
defaults:
  temporal:
    year: "2025"            # String instead of integer
    start: "04-15-2025"     # Wrong date format
```

## Example: Philadelphia Aerial Imagery

Real-world example from a catalog with 947 COG tiles:

```yaml
# aerial-imagery/2025/.portolan/metadata.yaml

contact:
  name: "Nissim Lebovits"
  email: "nlebovits@pm.me"

license: "LicenseRef-CityOfPhiladelphia"
attribution: "City of Philadelphia / PASDA"

defaults:
  temporal:
    year: 2025    # All 2025 imagery defaults to 2025-01-01

  raster:
    nodata: 0     # Black collar pixels are nodata
```

This sets consistent metadata across all 947 items without requiring per-file flags.

## Related

- [ADR-0038: Metadata YAML Enrichment](https://github.com/portolan-sdi/portolan-cli/blob/main/context/shared/adr/0038-metadata-yaml-enrichment.md) - Design decision
- [ADR-0035: Temporal Extent Handling](https://github.com/portolan-sdi/portolan-cli/blob/main/context/shared/adr/0035-temporal-extent-handling.md) - Why null datetime is allowed
