# Configuration

Portolan stores configuration in `.portolan/config.yaml` within your catalog directory.

## Quick Start

```yaml
# .portolan/config.yaml
remote: s3://my-bucket/catalog
aws_profile: production
```

## Setting Configuration

```bash
# Set remote storage URL
portolan config set remote s3://my-bucket/catalog

# Set AWS profile
portolan config set aws_profile production

# View current settings
portolan config list
```

## Configuration Precedence

Settings are resolved in this order (highest to lowest):

1. **CLI argument** (`--remote s3://...`)
2. **Environment variable** (`PORTOLAN_REMOTE=s3://...`)
3. **Collection config** (in `collections:` section)
4. **Catalog config** (top-level in config.yaml)
5. **Built-in default**

## Conversion Configuration

Control how Portolan handles different file formats during `check` and `convert` operations.

### Use Cases

| Scenario | Configuration |
|----------|---------------|
| Force-convert FlatGeobuf to GeoParquet | `extensions.convert: [fgb]` |
| Keep Shapefiles as-is | `extensions.preserve: [shp]` |
| Preserve everything in archive/ | `paths.preserve: ["archive/**"]` |

### Full Example

```yaml
# .portolan/config.yaml
remote: s3://my-bucket/catalog

conversion:
  extensions:
    # Force-convert these cloud-native formats to GeoParquet
    convert:
      - fgb      # FlatGeobuf

    # Keep these formats as-is (don't convert)
    preserve:
      - shp      # Shapefiles
      - gpkg     # GeoPackage

  paths:
    # Glob patterns for files to preserve regardless of format
    preserve:
      - "archive/**"           # Everything in archive/
      - "regulatory/*.shp"     # Regulatory shapefiles
      - "legacy/**"            # Legacy data directory
```

### Extension Overrides

#### `extensions.convert`

Force-convert cloud-native formats to GeoParquet. Use when:

- You want consistent columnar format for analytics
- Your tooling prefers GeoParquet over FlatGeobuf

```yaml
conversion:
  extensions:
    convert:
      - fgb       # FlatGeobuf -> GeoParquet
      - pmtiles   # PMTiles -> GeoParquet
```

#### `extensions.preserve`

Keep convertible formats as-is. Use when:

- Regulatory requirements mandate original format
- Downstream tools require specific formats
- You're preserving archival data

```yaml
conversion:
  extensions:
    preserve:
      - shp       # Keep Shapefiles
      - gpkg      # Keep GeoPackage
      - geojson   # Keep GeoJSON
```

### Path Patterns

Use glob patterns to override behavior for specific directories or files.

```yaml
conversion:
  paths:
    preserve:
      - "archive/**"           # All files in archive/ and subdirectories
      - "regulatory/*.shp"     # Only .shp files in regulatory/
      - "**/*.backup.geojson"  # Any .backup.geojson file
```

**Pattern syntax:**

- `*` matches any characters except `/`
- `**` matches any characters including `/`
- `?` matches any single character

**Precedence:** Path patterns override extension rules. A FlatGeobuf file in `archive/` will be preserved even if `extensions.convert: [fgb]` is set.

## Collection-Level Configuration

Override settings for specific collections:

```yaml
# .portolan/config.yaml
remote: s3://default-bucket/catalog

collections:
  public-data:
    remote: s3://public-bucket/data

  analytics:
    conversion:
      extensions:
        convert: [fgb]  # Force GeoParquet for analytics queries

  archive:
    conversion:
      extensions:
        preserve: [shp, gpkg, geojson]  # Preserve all original formats
```

## Environment Variables

All settings can be set via environment variables with the `PORTOLAN_` prefix:

| Setting | Environment Variable |
|---------|---------------------|
| `remote` | `PORTOLAN_REMOTE` |
| `aws_profile` | `PORTOLAN_AWS_PROFILE` |

Environment variables override config file settings but are overridden by CLI arguments.
