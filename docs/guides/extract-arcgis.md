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

### Limiting Extraction Area

For large ImageServers, use `--bbox` to extract a subset:

```bash
# Extract only tiles within bounding box (in service CRS coordinates)
portolan extract arcgis URL --bbox "-8841000,5405000,-8840000,5406000"
```

**Important**: The bbox coordinates must be in the service's native CRS (check the service metadata for `spatialReference.wkid`).

### ImageServer Options

```bash
# Tile size in pixels (default: 4096)
portolan extract arcgis URL --tile-size 2048

# Maximum concurrent downloads (default: 4)
portolan extract arcgis URL --max-concurrent 8

# COG compression (default: deflate)
portolan extract arcgis URL --compression jpeg  # Good for RGB imagery
```

### Output Structure

Raster data uses item-level assets — each tile becomes a STAC item:

```
output/
├── .portolan/
│   ├── config.yaml
│   ├── extraction-report.json
│   └── imageserver-resume.json     # For resuming interrupted extractions
├── catalog.json
└── tiles/                          # Collection (one per ImageServer)
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

### Adding Metadata After Extraction

Extraction creates STAC metadata but **not** `metadata.yaml`. Per [ADR-0038](https://github.com/portolan-sdi/portolan-cli/blob/main/context/shared/adr/0038-metadata-yaml-enrichment.md), contact and license info must be added manually:

```bash
# Create metadata.yaml in the collection's .portolan directory
mkdir -p tiles/.portolan
cat > tiles/.portolan/metadata.yaml << 'EOF'
contact:
  name: Your Name
  email: your.email@example.com
license: CC-BY-4.0
source_url: https://example.com/.../ImageServer
EOF

# Generate README from STAC + metadata.yaml
portolan readme tiles
```

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
