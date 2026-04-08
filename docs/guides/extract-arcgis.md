# Extracting Data from ArcGIS Services

Portolan can extract data directly from ArcGIS REST services:

- **FeatureServer/MapServer**: Vector data → GeoParquet files
- **ImageServer**: Raster imagery → Cloud-Optimized GeoTIFF (COG) tiles

## Quick Start

```bash
# Extract all layers from a FeatureServer
portolan extract arcgis https://services.arcgis.com/.../FeatureServer ./output

# Extract tiles from an ImageServer (uses bbox to limit area)
portolan extract arcgis https://example.com/.../ImageServer ./output --bbox "minx,miny,maxx,maxy"

# Preview what would be extracted (dry run)
portolan extract arcgis URL --dry-run
```

## Service Types

Portolan auto-detects the service type from the URL:

| URL Pattern | Service Type | Output Format |
|-------------|--------------|---------------|
| `.../FeatureServer` | Vector features | GeoParquet |
| `.../MapServer` | Vector features | GeoParquet |
| `.../ImageServer` | Raster imagery | COG tiles |

!!! warning "MapServer Raster Limitation"
    **MapServer endpoints only support vector extraction.** Even if a MapServer hosts imagery, Portolan extracts the vector tile indexes and boundaries—not the imagery itself.

    This is a fundamental ArcGIS limitation: MapServer's `/export` endpoint returns **rendered 8-bit visualization**, not source raster data. For source imagery extraction, you need an **ImageServer** endpoint.

    If you're working with a data provider that only offers MapServer (e.g., PASDA), contact them to request ImageServer access, or use their download links for the source data.

---

## FeatureServer / MapServer Extraction

### Basic Usage

Point Portolan at any ArcGIS FeatureServer or MapServer URL:

```bash
portolan extract arcgis \
  https://services.arcgis.com/fLeGjb7u4uXqeF9q/ArcGIS/rest/services/Census_2020/FeatureServer \
  ./census_2020
```

This will:

1. Discover all layers in the service
2. Extract each layer to GeoParquet format
3. Apply Hilbert spatial sorting for efficient queries
4. Initialize a Portolan catalog with STAC metadata
5. Seed `.portolan/metadata.yaml` with values from the service
6. Generate an extraction report in `.portolan/extraction-report.json`

### Filtering Layers

Use glob patterns to extract specific layers:

```bash
# Include only layers matching patterns
portolan extract arcgis URL --layers "Census*,Transport*"

# Exclude layers matching patterns
portolan extract arcgis URL --exclude-layers "*_Archive,*_Backup"

# Combine include and exclude
portolan extract arcgis URL --layers "Census*" --exclude-layers "*_2010"
```

**Pattern syntax** uses fnmatch:

- `*` matches any characters
- `?` matches a single character
- Examples: `sdn_*`, `*_2024`, `cod_ab_*`

### Output Structure

Each layer becomes a collection with the parquet file as a collection-level asset:

```
output/
├── .portolan/
│   ├── extraction-report.json    # Extraction metadata
│   └── metadata.yaml             # Pre-seeded with service metadata
├── catalog.json                  # STAC catalog
├── census_block_groups/
│   ├── collection.json
│   └── census_block_groups.parquet
└── census_tracts/
    ├── collection.json
    └── census_tracts.parquet
```

### Auto-Seeded Metadata

The extraction process automatically seeds `.portolan/metadata.yaml` with values from the ArcGIS service metadata:

| ArcGIS Field | metadata.yaml Field |
|-------------|---------------------|
| `copyrightText` | `attribution` |
| `documentInfo.Author` | `contact.name` |
| `documentInfo.Keywords` | `keywords` |
| `serviceDescription` | `processing_notes` |
| `accessInformation` | `known_issues` |
| Service URL | `source_url` |

Fields that require human input (like `contact.email` and `license`) are marked with `TODO` placeholders:

```yaml
contact:
  name: "Philadelphia GIS Team"  # Auto-filled from service
  email: "TODO: Add value"       # Needs human input
license: "TODO: Add value"       # Needs SPDX identifier
source_url: "https://services.arcgis.com/..."
attribution: "City of Philadelphia"
```

!!! tip "Won't Overwrite Existing Files"
    If `.portolan/metadata.yaml` already exists, extraction will **not** overwrite it. This preserves any manual edits you've made.

---

## ImageServer Extraction

### Basic Usage

Extract raster imagery from an ArcGIS ImageServer:

```bash
portolan extract arcgis \
  https://sampleserver6.arcgisonline.com/arcgis/rest/services/Toronto/ImageServer \
  ./toronto-imagery
```

This will:

1. Query service metadata (extent, CRS, pixel size, bands)
2. Compute a tile grid covering the service extent
3. Download tiles via `exportImage` API
4. Convert each tile to Cloud-Optimized GeoTIFF (COG)
5. Create STAC items for each tile with spatial metadata
6. Generate an extraction report
7. Seed `metadata.yaml` with service metadata (source URL, attribution, keywords)

### Limiting Extraction Area

For large ImageServers, use `--bbox` to extract a subset:

```bash
# WGS84 coordinates (latitude/longitude) - auto-converted to service CRS
portolan extract arcgis URL --bbox "-75.17,39.95,-75.15,39.97"

# Or explicit service CRS coordinates (Web Mercator in this example)
portolan extract arcgis URL --bbox "-8367886,4858679,-8365659,4861583"

# Override CRS auto-detection with --bbox-crs
portolan extract arcgis URL --bbox "100,200,300,400" --bbox-crs "EPSG:2269"
```

**Automatic CRS conversion**: Portolan detects if your bbox coordinates appear to be in WGS84 (values in -180/180, -90/90 range) and automatically reprojects them to the service's native CRS. This means you can use familiar lat/lon coordinates without manual conversion.

!!! tip "Overriding CRS Detection"
    If you're working with a local CRS (like State Plane) where coordinates happen to fall in the WGS84 range, use `--bbox-crs` to specify the exact CRS and skip auto-detection.

### ImageServer Options

```bash
# Tile size in pixels (default: 4096, auto-adjusted if exceeds service limit)
portolan extract arcgis URL --tile-size 2048

# Maximum concurrent downloads (default: 4)
portolan extract arcgis URL --max-concurrent 8

# COG compression (default: deflate)
portolan extract arcgis URL --compression jpeg  # Good for RGB imagery

# Custom collection name (default: 'tiles')
portolan extract arcgis URL --collection-name "naip-philly-2024"
```

!!! tip "Tile Size Validation"
    Portolan automatically fetches the service's maximum allowed tile dimensions during discovery. If your `--tile-size` exceeds this limit, it's auto-adjusted down with a warning—no more cryptic "bad magic bytes" errors.

### Output Structure

Raster data uses item-level assets — each tile becomes a STAC item:

```
output/
├── .portolan/
│   ├── config.yaml
│   ├── extraction-report.json
│   ├── imageserver-resume.json     # For resuming interrupted extractions
│   └── metadata.yaml               # Seeded from service metadata
├── catalog.json
└── tiles/                          # Collection name (customizable via --collection-name)
    ├── collection.json
    ├── versions.json
    ├── tile_0_0/
    │   ├── tile_0_0.json           # STAC item with tile bbox
    │   └── tile_0_0.tif            # COG asset
    ├── tile_0_1/
    │   ├── tile_0_1.json
    │   └── tile_0_1.tif
    └── ...
```

Use `--collection-name` to give the collection a meaningful name instead of the generic "tiles".

### Metadata After Extraction

Extraction automatically seeds `.portolan/metadata.yaml` with values from the ArcGIS service (source URL, description, attribution, keywords). Fields that require human input are marked with `TODO: Add value`:

```yaml
# Auto-seeded .portolan/metadata.yaml (example)
contact:
  name: "TODO: Add value"      # Required - add your name
  email: "TODO: Add value"     # Required - add your email
license: "TODO: Add value"     # Required - add SPDX identifier (e.g., CC-BY-4.0)
source_url: https://example.com/.../ImageServer  # Auto-populated
attribution: "Copyright © 2024 Example Org"      # Auto-populated from copyrightText
```

Complete the `TODO` fields, then generate the README:

```bash
# Generate README from STAC + metadata.yaml
portolan readme tiles
```

!!! info "metadata.yaml Supplements STAC"
    The `metadata.yaml` file **supplements** STAC metadata—it doesn't replace or modify it. This separation keeps machine-extracted metadata (STAC) distinct from human-enriched metadata (metadata.yaml).

    There is no `portolan metadata generate` command because `metadata.yaml` is for human enrichment only. The README command (`portolan readme`) reads from both STAC (machine-extracted) and metadata.yaml (human-enriched) to generate documentation.

!!! tip "Overriding auto-seeded metadata"
    To replace auto-seeded values, edit the generated `metadata.yaml` directly. The file is never overwritten on subsequent extractions.

---

## Common Options

These options work for both FeatureServer and ImageServer:

### Controlling Extraction

```bash
# Request timeout in seconds (default: 60 for vectors, 120 for rasters)
portolan extract arcgis URL --timeout 120

# Retry failed requests (default: 3 attempts)
portolan extract arcgis URL --retries 5
```

### Resume Failed Extractions

If an extraction fails partway through, resume from where you left off:

```bash
# Initial extraction (fails partway)
portolan extract arcgis URL ./output

# Resume - skips already-extracted layers/tiles
portolan extract arcgis URL ./output --resume
```

### Dry Run Mode

Preview what would be extracted without downloading any data:

```bash
portolan extract arcgis URL --dry-run
```

### JSON Output

For automation and scripts, use JSON output:

```bash
portolan extract arcgis URL --json
```

### Non-Interactive Mode

Skip confirmation prompts (useful in scripts):

```bash
portolan extract arcgis URL --auto
```

---

## Extraction Report

The extraction report (`.portolan/extraction-report.json`) contains:

- **Source URL** and extraction timestamp
- **Metadata** extracted from the ArcGIS service
- **Per-layer/tile results**: status, count, file size, duration, any errors
- **Summary**: totals for succeeded, failed, skipped

Example (FeatureServer):

```json
{
  "extraction_date": "2024-03-15T10:30:00Z",
  "source_url": "https://services.arcgis.com/.../FeatureServer",
  "summary": {
    "total_layers": 5,
    "succeeded": 4,
    "failed": 1,
    "total_features": 125000,
    "total_size_bytes": 45000000
  }
}
```

---

## Tips

### Finding ArcGIS Services

ArcGIS services are typically found at URLs like:

- `https://services.arcgis.com/{org_id}/ArcGIS/rest/services/{service_name}/FeatureServer`
- `https://gis.example.com/arcgis/rest/services/{folder}/{service_name}/ImageServer`

You can browse available services at the root:

- `https://services.arcgis.com/{org_id}/ArcGIS/rest/services`

### Large Services

For services with many layers or large datasets:

1. Use `--dry-run` first to see what will be extracted
2. Filter with `--layers` (vectors) or `--bbox` (rasters)
3. Use `--resume` if extraction is interrupted
4. Increase parallelism with `--workers` (vectors) or `--max-concurrent` (rasters)

### Error Handling

If a layer/tile fails to extract:

- The extraction continues with remaining items
- Failed items are recorded in the report with error details
- Use `--resume` to retry only failed items

---

## Requirements

- [geoparquet-io](https://github.com/geoparquet/geoparquet-io) — Vector extraction (automatically installed)
- [rio-cogeo](https://github.com/cogeotiff/rio-cogeo) — COG conversion (automatically installed)
- Network access to the ArcGIS service
