# Design: `portolan extract arcgis` Command

**Status:** Draft
**Issue:** [#6 - Full ArcGIS Server → Portolan conversion](https://github.com/portolan-sdi/portolan-cli/issues/6)
**Date:** 2026-03-30

## Overview

A new command to extract vector data from ArcGIS FeatureServer/MapServer endpoints and convert them into a well-structured Portolan catalog. This enables users with data in ArcGIS Server/Portal to create fully cloud-native geospatial catalogs.

## Scope

**In scope:**
- Static, one-time extraction (no incremental sync)
- Vector data only (FeatureServer/MapServer)
- Metadata harvesting from ArcGIS REST API
- Automatic STAC catalog generation

**Out of scope (future work):**
- Incremental sync / change detection (#6 comment by @maxmalynowsky)
- Raster extraction (depends on #5)
- LLM-assisted metadata enhancement
- Portal/Hub crawling (user provides URL directly)

## Architecture

### Input Types

The command accepts two types of URLs, with behavior determined implicitly:

| Input URL Pattern | Structure |
|-------------------|-----------|
| `*/FeatureServer` or `*/MapServer` | FeatureServer = Catalog, Layers = Collections |
| `*/rest/services` (root) | Root = Catalog, Services = Subcatalogs, Layers = Collections |

### Output Structure

**Single Service (FeatureServer URL):**
```
my-catalog/
├── catalog.json
├── .portolan/
│   ├── config.yaml
│   ├── metadata.yaml          # source_url filled, required fields empty
│   └── extraction-report.json # full provenance
├── layer_name_0/
│   ├── collection.json
│   ├── layer_name_0/
│   │   ├── item.json
│   │   └── layer_name_0.parquet
│   └── versions.json
└── layer_name_1/
    └── ...
```

**Services Root (rest/services URL):**
```
my-catalog/
├── catalog.json
├── .portolan/
│   ├── config.yaml
│   ├── metadata.yaml
│   └── extraction-report.json
├── Service_A/                   # FeatureServer → Subcatalog
│   ├── catalog.json
│   ├── layer_0/                 # Layer → Collection
│   │   └── ...
│   └── layer_1/
└── Service_B/
    └── ...
```

### Parallelism Model

- **Sequential by layer** — one layer extracted at a time
- **Parallel within layer** — page requests parallelized (via gpio's `max_workers`)
- **Rationale:** Avoids complexity of multiple parallel pagination streams; keeps memory predictable

## Implementation

### Dependencies

- **gpio (geoparquet-io)** — handles single-layer extraction with pagination
  - `gpio.extract_arcgis(url, output_file, ...)` for data
  - `get_layer_info()` for layer metadata
- **httpx** — for service discovery (listing layers/services)
- **Existing Portolan internals** — STAC generation, metadata.yaml, catalog structure

### Extraction Flow

```
1. Parse URL → determine if FeatureServer or services root
2. Discover services/layers (fetch ?f=json)
3. Apply filters (--layers, --services, --exclude-layers)
4. If --dry-run: display list and exit
5. For each service (if root URL):
   a. Create subcatalog directory
6. For each layer:
   a. Fetch layer metadata (get_layer_info)
   b. Extract to parquet via gpio (with retries)
   c. Generate STAC item/collection
   d. Record result in extraction report
7. Generate root catalog.json
8. Write metadata.yaml with available metadata
9. Write extraction-report.json
10. Display summary
```

### Retry Strategy

- **Default retries:** 3 attempts per layer
- **Backoff:** Exponential (1s, 2s, 4s)
- **On persistent failure:** Log error, continue to next layer
- **Final report:** Lists all failures with error details

### Metadata Strategy

#### What We Extract (Reliably Available)

From ArcGIS REST API → `metadata.yaml`:

| ArcGIS Field | metadata.yaml Field | Notes |
|--------------|---------------------|-------|
| Service URL | `source_url` | Always available |
| `copyrightText` | `attribution` | Often empty, but extract if present |
| `description` | (catalog description) | Rarely populated |
| Field aliases | `columns.*.description` | Human-readable field names |

#### What We DON'T Extract (Auto-extracted from Parquet)

These are handled by STAC auto-generation, not ArcGIS scraping:
- bbox, extent, geometry
- CRS/projection
- Column names and types (schema)
- Feature count
- Statistics

#### What Remains Empty (Human/LLM Enrichment)

Required metadata.yaml fields that will be empty:
- `contact.name` — almost never in ArcGIS
- `contact.email` — almost never in ArcGIS
- `license` — `copyrightText` is free-form, not SPDX

The `portolan check` command will flag these as incomplete before push.

### Extraction Report

Written to `.portolan/extraction-report.json`:

```json
{
  "extraction_date": "2026-03-30T14:30:00Z",
  "source_url": "https://services.arcgis.com/.../FeatureServer",
  "portolan_version": "0.4.0",
  "gpio_version": "0.2.0",
  "layers": [
    {
      "id": 0,
      "name": "Census_Block_Groups",
      "status": "success",
      "features": 1336,
      "size_bytes": 1949696,
      "duration_seconds": 12.4,
      "output_path": "census_block_groups/census_block_groups/census_block_groups.parquet",
      "warnings": []
    },
    {
      "id": 1,
      "name": "Problematic_Layer",
      "status": "failed",
      "error": "Timeout after 3 retries",
      "attempts": 3
    }
  ],
  "summary": {
    "total_layers": 10,
    "succeeded": 9,
    "failed": 1,
    "total_features": 45000,
    "total_size_bytes": 52428800,
    "total_duration_seconds": 180
  }
}
```

## CLI Interface

```bash
portolan extract arcgis <URL> [OUTPUT_DIR] [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `URL` | ArcGIS FeatureServer, MapServer, or REST services root URL |
| `OUTPUT_DIR` | Output directory (default: inferred from service name) |

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--layers` | all | Include specific layers (by ID or name, comma-separated) |
| `--exclude-layers` | none | Exclude specific layers |
| `--services` | all | Filter services for root URLs (glob patterns) |
| `--workers` | 3 | Parallel page requests per layer |
| `--retries` | 3 | Retry attempts per failed layer |
| `--timeout` | 60 | Per-request timeout in seconds |
| `--dry-run` | false | List layers without extracting |
| `--json` | false | Output extraction report to stdout |
| `--auto` | false | Skip confirmation prompts |

### Examples

```bash
# Extract all layers from a FeatureServer
portolan extract arcgis https://services.arcgis.com/.../FeatureServer ./philly-census

# Extract specific layers by name
portolan extract arcgis https://services.arcgis.com/.../FeatureServer ./output \
  --layers "Census_Block_Groups,Census_Tracts"

# Extract from services root with filtering
portolan extract arcgis https://services.arcgis.com/.../rest/services ./output \
  --services "Census*,Transportation*"

# Dry run to see what would be extracted
portolan extract arcgis https://services.arcgis.com/.../FeatureServer --dry-run

# JSON output for agent consumption
portolan extract arcgis https://services.arcgis.com/.../FeatureServer ./output --json
```

## Error Handling

### Large Service Warning

For services root URLs with >50 services:
```
⚠ Found 1,042 services. This may take a long time.
  Use --services to filter, or --dry-run to preview.
  Continue? [y/N]
```

With `--auto`, proceeds with warning in output.

### Layer Failures

Individual layer failures don't stop extraction:
```
✗ Layer 5 (Problematic_Data): Timeout after 3 retries
→ Continuing with remaining layers...

...

Summary:
  ✓ 9/10 layers extracted successfully
  ✗ 1 layer failed (see extraction-report.json)
```

### Empty Layers

Layers with 0 features are noted but still create empty parquet files:
```
⚠ Layer 3 (Empty_Layer): 0 features (creating empty collection)
```

## Testing Strategy

### Unit Tests
- URL parsing (FeatureServer vs root detection)
- Layer filtering logic (by ID, by name, exclude)
- Service filtering (glob patterns)
- Extraction report generation

### Integration Tests
- Mock ArcGIS server responses
- End-to-end extraction with fixture data
- Retry behavior on simulated failures

### Real-World Tests (Manual)
- Philadelphia services: `https://services.arcgis.com/fLeGjb7u4uXqeF9q/ArcGIS/rest/services`
- Den Haag services (existing test data)

## Open Questions

1. **Authentication:** Should we support `--token`, `--username/--password` like gpio does? (Probably yes, pass through to gpio)

2. **Rate Limiting:** Should we add configurable delay between layers to be polite to servers?

3. **Resume Capability:** For large extractions that fail partway, should we support `--resume` using the extraction report?

## References

- [Issue #6: Full ArcGIS Server → Portolan conversion](https://github.com/portolan-sdi/portolan-cli/issues/6)
- [gpio extract arcgis documentation](https://geoparquet.io/cli/extract/?h=arcgis#extract-arcgis)
- [Den Haag roundtrip workflow](../../../portolan-test-data/den-haag-roundtrip/ROUNDTRIP-WORKFLOW.md)
- [ADR-0038: metadata.yaml enrichment](../adr/0038-metadata-yaml-enrichment.md)
- [ADR-0030: Agent-native CLI design](../adr/0030-agent-native-cli-design.md)
