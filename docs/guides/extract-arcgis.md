# Extracting Data from ArcGIS Services

Portolan can extract vector data directly from ArcGIS FeatureServer and MapServer endpoints into a well-structured catalog with GeoParquet files and STAC metadata.

## Quick Start

```bash
# Extract all layers from a FeatureServer
portolan extract arcgis https://services.arcgis.com/.../FeatureServer ./output

# Preview what would be extracted (dry run)
portolan extract arcgis URL --dry-run
```

## Basic Usage

### Extracting a Single Service

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
4. Generate an extraction report in `.portolan/extraction-report.json`

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

## Advanced Options

### Controlling Extraction

```bash
# Parallel page requests per layer (default: 3)
portolan extract arcgis URL --workers 5

# Retry failed layers (default: 3 attempts)
portolan extract arcgis URL --retries 5

# Request timeout in seconds (default: 60)
portolan extract arcgis URL --timeout 120
```

### Resume Failed Extractions

If an extraction fails partway through, resume from where you left off:

```bash
# Initial extraction (fails on layer 5)
portolan extract arcgis URL ./output

# Resume - skips already-extracted layers
portolan extract arcgis URL ./output --resume
```

The resume feature uses the extraction report in `.portolan/extraction-report.json` to determine which layers have already succeeded.

### Dry Run Mode

Preview what would be extracted without downloading any data:

```bash
portolan extract arcgis URL --dry-run
```

Output shows all layers that would be extracted.

### JSON Output

For automation and scripts, use JSON output:

```bash
portolan extract arcgis URL --json
```

Returns a structured JSON envelope with:

- `source_url`: The ArcGIS service URL
- `summary`: Counts of succeeded/failed/skipped layers
- `layers`: Details for each layer including status and output path

### Non-Interactive Mode

Skip confirmation prompts (useful in scripts):

```bash
portolan extract arcgis URL --auto
```

## Output Structure

Extracted data follows the Portolan catalog structure with collection-level assets:

```
output/
├── .portolan/
│   └── extraction-report.json    # Extraction metadata
├── census_block_groups/
│   ├── collection.json
│   └── census_block_groups.parquet
├── census_tracts/
│   ├── collection.json
│   └── census_tracts.parquet
└── boundaries/
    ├── collection.json
    └── boundaries.parquet
```

Each layer becomes a collection with the parquet file as a collection-level asset (per [ADR-0031](https://github.com/portolan-sdi/portolan-cli/blob/main/context/shared/adr/0031-collection-level-assets-for-vector-data.md)).

## Extraction Report

The extraction report (`extraction-report.json`) contains:

- **Source URL** and extraction timestamp
- **Metadata** extracted from the ArcGIS service (attribution, keywords, etc.)
- **Per-layer results**: status, feature count, file size, duration, any errors
- **Summary**: total layers, succeeded, failed, skipped

Example:

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
  },
  "layers": [
    {
      "id": 0,
      "name": "Census_Block_Groups",
      "status": "success",
      "features": 50000,
      "output_path": "census_block_groups/census_block_groups.parquet"
    }
  ]
}
```

## Tips

### Finding ArcGIS Services

ArcGIS services are typically found at URLs like:

- `https://services.arcgis.com/{org_id}/ArcGIS/rest/services/{service_name}/FeatureServer`
- `https://gis.example.com/arcgis/rest/services/{folder}/{service_name}/MapServer`

You can browse available services at the root:

- `https://services.arcgis.com/{org_id}/ArcGIS/rest/services`

### Large Services

For services with many layers or large datasets:

1. Use `--dry-run` first to see what will be extracted
2. Filter to specific layers with `--layers`
3. Use `--resume` if extraction is interrupted
4. Increase `--workers` for faster extraction (if server allows)

### Error Handling

If a layer fails to extract:

- The extraction continues with remaining layers
- Failed layers are recorded in the report with error details
- Use `--resume` to retry only failed layers

## Requirements

- [geoparquet-io](https://github.com/geoparquet/geoparquet-io) (automatically installed with Portolan)
- Network access to the ArcGIS service
