# Configuration

Portolan stores configuration in `.portolan/config.yaml` within your catalog directory.

## Quick Start

```yaml
# .portolan/config.yaml
remote: s3://my-bucket/catalog
profile: production    # AWS profile (alias: aws_profile)
region: us-west-2      # AWS region for S3
```

## Backend (Enterprise)

By default, Portolan uses a file-based backend (`versions.json`) for version tracking. For enterprise deployments requiring ACID transactions, distributed locking, and advanced versioning features, install the [portolake](https://github.com/portolan-sdi/portolake) plugin:

```bash
uv add portolake
# or: pip install portolake
```

Then configure the backend:

```yaml
# .portolan/config.yaml
backend: iceberg
```

Or initialize a new catalog with the Iceberg backend:

```bash
portolan init --backend iceberg
```

### Version Management Commands

With the Iceberg backend, additional commands become available:

```bash
# Show current version of a collection
portolan version current boundaries

# List all versions
portolan version list boundaries

# Rollback to a previous version (instant, uses Iceberg snapshots)
portolan version rollback boundaries 1.0.0

# Remove old versions, keeping N most recent
portolan version prune boundaries --keep 5
```

!!! note "Backend-specific commands"
    The `portolan version` subcommands require the `iceberg` backend. Running them with the default `file` backend will display an error message.

See the [portolake documentation](https://github.com/portolan-sdi/portolake) for full setup instructions and enterprise features.

## Setting Configuration

```bash
# Set remote storage URL
portolan config set remote s3://my-bucket/catalog

# Set AWS profile (either name works)
portolan config set profile production
# portolan config set aws_profile production  # Also valid

# Set AWS region
portolan config set region us-west-2

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

### COG Settings

Configure Cloud-Optimized GeoTIFF conversion parameters. By default, Portolan uses ADR-0019 defaults (DEFLATE compression, predictor=2, 512×512 tiles, nearest resampling).

```yaml
conversion:
  cog:
    compression: JPEG      # DEFLATE (default), JPEG, LZW, ZSTD, WEBP
    quality: 95            # Quality 1-100 (applies to JPEG and WEBP)
    tile_size: 512         # Internal tile size in pixels
    predictor: 2           # 1=none, 2=horizontal (default), 3=floating point
    resampling: nearest    # Overview resampling: nearest, bilinear, cubic, etc.
```

!!! note "Validation"
    Invalid settings produce warnings but don't block conversion. Quality is clamped to 1-100, and unknown compression/resampling values are passed through to let rio-cogeo handle errors.

#### Use Cases

| Scenario | Configuration |
|----------|---------------|
| RGB imagery (smaller files) | `compression: JPEG`, `quality: 95` |
| Elevation data (lossless) | `compression: DEFLATE`, `predictor: 3` |
| Analytics (fast reads) | `compression: LZW`, `tile_size: 256` |

#### Available Compression Methods

| Method | Best For | Notes |
|--------|----------|-------|
| `DEFLATE` | General use (default) | Lossless, universal compatibility |
| `LZW` | Fast compression/decompression | Lossless, slightly larger files |
| `ZSTD` | High compression ratio | Lossless, requires GDAL 2.3+ |
| `JPEG` | RGB imagery | Lossy, smallest files for photos |
| `WEBP` | Web display | Lossy, modern browsers only |

## STAC GeoParquet Settings

Generate `items.parquet` for collections with many items, enabling efficient spatial/temporal queries without N HTTP requests.

```yaml
# .portolan/config.yaml
parquet.enabled: true     # Auto-generate during add (default: false)
parquet.threshold: 100    # Hint when items exceed threshold (default: 100)
```

!!! note "Flat key syntax"
    Config keys use dot notation as literal keys (e.g., `parquet.enabled`), not nested YAML mappings.

### Commands

```bash
# Generate items.parquet for a collection
portolan stac-geoparquet -c eurosat

# Preview without creating files
portolan stac-geoparquet -c eurosat --dry-run

# Auto-generate during add
portolan add imagery/ --stac-geoparquet
```

### How It Works

- Uses [stac-geoparquet](https://github.com/stac-utils/stac-geoparquet) library
- Adds `items.parquet` as a collection-level asset (per [ADR-0031](../contributing.md)) and link with `rel: items`
- Enables spatial filtering with a single HTTP request (vs N requests for items)

| Setting | Default | Description |
|---------|---------|-------------|
| `parquet.enabled` | `false` | Auto-generate during `add` command |
| `parquet.threshold` | `100` | Show hint when items exceed threshold |

### When to Use

- Collections with >100 items (e.g., satellite imagery time series)
- Raster collections with many scenes
- Partitioned vector datasets

## Collection-Level Configuration

Override settings for specific collections using the `collections:` section:

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

This approach works well for most catalogs. For large catalogs with many collections, see [Hierarchical Configuration](#hierarchical-configuration-optional) below.

## Hierarchical Configuration (Optional)

For large catalogs or when different maintainers manage different collections, you can optionally create `.portolan/` folders at collection or subcatalog levels:

```
catalog/
  .portolan/
    config.yaml           # Catalog defaults
  demographics/
    .portolan/
      config.yaml         # Collection-specific overrides (optional)
    collection.json
  historical/             # Subcatalog
    .portolan/
      config.yaml         # Subcatalog defaults (optional)
    census-1990/
      collection.json
```

**This is entirely optional.** Benefits include:

- **Scalability**: Avoids one giant config file with 100+ collection entries
- **Ownership**: Collection maintainers edit their own folder without touching root
- **Git-friendly**: Changes to one collection don't create merge conflicts in root

### Inheritance Rules

Settings are inherited from parent levels. Child values override parent values:

```yaml
# catalog/.portolan/config.yaml
aws_profile: default
remote: s3://catalog/

# catalog/demographics/.portolan/config.yaml
remote: s3://demographics/  # Overrides parent
# aws_profile inherited from catalog
```

### Precedence

When both approaches are used, folder config takes precedence over `collections:` section:

```
CLI > Env var > Collection folder config > Subcatalog folder config >
  Root collections: section > Catalog config > Default
```

### When to Use Each Approach

| Approach | Best For |
|----------|----------|
| `collections:` section | Small catalogs, simple overrides |
| Hierarchical folders | Large catalogs, multiple maintainers, verbose metadata |

Most users should start with `collections:` and only add per-collection `.portolan/` folders when needed

## Environment Variables

All settings can be set via environment variables with the `PORTOLAN_` prefix:

| Setting | Environment Variable | Notes |
|---------|---------------------|-------|
| `remote` | `PORTOLAN_REMOTE` | |
| `aws_profile` | `PORTOLAN_AWS_PROFILE` | |
| `profile` | `PORTOLAN_PROFILE` | Alias for `aws_profile` |
| `region` | `PORTOLAN_REGION` | AWS region for S3 |

Environment variables override config file settings but are overridden by CLI arguments.

### Setting Aliases

Some settings have aliases for convenience:

| Canonical Name | Alias |
|----------------|-------|
| `aws_profile` | `profile` |

Both names work interchangeably in config files and environment variables.

## Metadata Enrichment

In addition to `config.yaml`, Portolan supports `.portolan/metadata.yaml` for human-enrichable metadata that supplements STAC.

### Purpose

STAC provides machine-extractable metadata (title, description, extent, columns). `metadata.yaml` adds **human-only fields** that can't be derived automatically:

| Field | Purpose |
|-------|---------|
| `contact` | Accountability (name, email) |
| `license` | SPDX identifier (e.g., CC-BY-4.0, MIT) |
| `citation` | Academic citation text |
| `doi` | Zenodo/DataCite DOI |
| `known_issues` | Data quality caveats |
| `source_url` | Link to original data source |
| `processing_notes` | Documentation of transformations applied |
| `keywords` | Tags for search/discovery (rendered as badges) |
| `attribution` | Credit to data provider or organization |
| `authors` | List of authors with name, optional ORCID and email |
| `related_dois` | List of related DOIs for linked publications |
| `citations` | List of citation strings for referencing |
| `upstream_version` | Version string of upstream data source |

### Quick Start

```bash
# Generate template
portolan metadata init

# Validate required fields
portolan metadata validate

# Generate README from STAC + metadata
portolan readme
```

### Example

```yaml
# .portolan/metadata.yaml
contact:
  name: Data Team
  email: data@example.org

license: CC-BY-4.0

# Optional enrichment fields
license_url: https://creativecommons.org/licenses/by/4.0/
citation: "Census Bureau (2024). Demographics Dataset. DOI: 10.5281/zenodo.1234567"
doi: 10.5281/zenodo.1234567
known_issues: "Coverage gaps in rural areas for 2020 data."

# Provenance and discovery
source_url: https://data.census.gov/demographics
processing_notes: |
  - Reprojected from NAD83 to EPSG:4326
  - Simplified geometries for web display
  - Joined with income data from ACS 2020
keywords:
  - census
  - demographics
  - population
attribution: "U.S. Census Bureau"

# Author and citation metadata
authors:
  - name: Jane Doe
    orcid: 0000-0001-2345-6789
    email: jane.doe@university.edu
  - name: John Smith
related_dois:
  - 10.5281/zenodo.1234567
  - 10.1000/related-paper
citations:
  - "Doe, J. (2024). Census Analysis Methods. J. Demographics, 1(1), 1-10."
upstream_version: "2024.1"
```

### Required Fields

Only two fields are required in `metadata.yaml`:

- **`contact.name`** and **`contact.email`** - Who maintains this data
- **`license`** - SPDX identifier (validated against common licenses)

Title and description come from STAC metadata (set during `portolan init`).

### Hierarchical Inheritance

Like `config.yaml`, `metadata.yaml` supports hierarchical resolution:

```
catalog/
  .portolan/
    metadata.yaml         # Default contact and license
  demographics/
    .portolan/
      metadata.yaml       # Override or add collection-specific fields
```

Child values override parent values. Use this to set catalog-wide defaults (license, contact) while adding collection-specific fields (known_issues, citation).

### README Generation

The `portolan readme` command generates `README.md` by combining:

**From STAC (automatic):**
- Title, description
- Spatial/temporal coverage
- Schema columns (from `table:columns`)
- Bands (from `eo:bands`, `raster:bands`)
- Files with checksums
- Code examples based on format

**From metadata.yaml (human):**
- License, contact
- Citation, DOI
- Known issues
- Source URL, processing notes
- Keywords (as badges), attribution

```bash
# Generate README.md
portolan readme

# Preview without writing
portolan readme --stdout

# Check if README is up-to-date (for CI)
portolan readme --check

# Generate for catalog and all collections
portolan readme --recursive
```

**Catalog-level README:** When run at catalog root, generates an index README with:
- Aggregated spatial extent (envelope of all collections)
- Aggregated temporal extent (earliest to latest)
- List of collections with links

### Data Defaults

When source files lack certain metadata (nodata values, temporal info), you can specify defaults in `metadata.yaml`:

```yaml
# .portolan/metadata.yaml
defaults:
  temporal:
    year: 2025              # Items default to 2025-01-01
    # Or explicit bounds:
    # start: "2025-04-15"
    # end: "2025-05-30"

  raster:
    nodata: 0               # Uniform nodata for all bands
    # Or per-band:
    # nodata: [0, 0, 255]
```

**Behavior:**

| Scenario | Result |
|----------|--------|
| Source file has value | File value used (defaults don't override) |
| Source file lacks value | Default applied |
| CLI flag provided | CLI flag overrides default |
| No default, no source value | Field left null |

**Validation:**

- `temporal.year` must be an integer between 1800 and 2100
- `temporal.start`/`temporal.end` must be valid ISO dates (YYYY-MM-DD)
- Specifying both `year` and `start` is an error (use one or the other)
- `raster.nodata` must be a finite number (no NaN or Infinity)
- Per-band nodata lists must match the raster's band count exactly

See the [Metadata Defaults Guide](../guides/metadata-defaults.md) for detailed usage.
