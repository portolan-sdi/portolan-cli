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
- Authentication (private/protected services) — see [geoparquet-io #310](https://github.com/geoparquet/geoparquet-io/issues/310)

## Architecture

### Input Types

The command accepts two types of URLs, with behavior determined implicitly:

| Input URL Pattern | Structure |
|-------------------|-----------|
| `*/FeatureServer` or `*/MapServer` | FeatureServer = Catalog, Layers = Collections |
| `*/rest/services` (root) | Root = Catalog, Services = Subcatalogs, Layers = Collections |

### ArcGIS ↔ STAC Mapping

| ArcGIS Concept | STAC Concept | Notes |
|----------------|--------------|-------|
| Services root (`/rest/services`) | Root Catalog | Entry point |
| Folder (e.g., "Demographics") | Sub-catalog | Preserves organizational hierarchy |
| FeatureServer/MapServer | Sub-catalog (if multi-layer) or Collection (if single-layer) | One service = one logical grouping |
| Layer | Collection with collection-level asset | Per [ADR-0031](../adr/0031-collection-level-assets-for-vector-data.md) |
| Features → GeoParquet | Collection-level asset | No nested items for single vector files |

### Output Structure

**Single Service (FeatureServer URL):**

Each layer becomes a collection with a **collection-level asset** (no nested items per [ADR-0031](../adr/0031-collection-level-assets-for-vector-data.md)):

```
my-catalog/
├── catalog.json
├── .portolan/
│   ├── config.yaml
│   ├── metadata.yaml          # source_url filled, required fields empty
│   └── extraction-report.json # full provenance
├── census_tracts/
│   ├── collection.json
│   ├── census_tracts.parquet  # Collection-level asset (ADR-0031)
│   └── .portolan/
│       └── versions.json
└── block_groups/
    ├── collection.json
    ├── block_groups.parquet
    └── .portolan/
        └── versions.json
```

**Services Root (rest/services URL):**

ArcGIS folders become sub-catalogs, services become sub-catalogs (if multi-layer) or collections, layers become collections with collection-level assets:

```
my-catalog/
├── catalog.json
├── .portolan/
│   ├── config.yaml
│   ├── metadata.yaml
│   └── extraction-report.json
├── Demographics/                    # ArcGIS folder → Sub-catalog
│   ├── catalog.json
│   ├── census_tracts/               # Layer → Collection
│   │   ├── collection.json
│   │   ├── census_tracts.parquet    # Collection-level asset
│   │   └── .portolan/
│   │       └── versions.json
│   └── income_data/
│       ├── collection.json
│       ├── income_data.parquet
│       └── .portolan/
│           └── versions.json
└── Transportation/                  # Another ArcGIS folder → Sub-catalog
    ├── catalog.json
    └── roads/
        ├── collection.json
        ├── roads.parquet
        └── .portolan/
            └── versions.json
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
3. Apply filters (--layers, --exclude-layers, --services, --exclude-services)
4. If --resume: load existing extraction-report.json, skip succeeded layers
5. If --dry-run: display list and exit
6. For each service (if root URL):
   a. Create subcatalog directory
7. For each layer:
   a. Fetch layer metadata (get_layer_info)
   b. Extract to parquet via gpio (with retries)
   c. Generate STAC collection with collection-level asset (ADR-0031)
   d. Record result in extraction report
8. Generate root catalog.json
9. Write metadata.yaml with all extracted metadata
10. Write extraction-report.json
11. Display summary
```

### Retry Strategy

- **Default retries:** 3 attempts per layer
- **Backoff:** Exponential (1s, 2s, 4s)
- **On persistent failure:** Log error, continue to next layer
- **Final report:** Lists all failures with error details

### Metadata Strategy

**Principle:** Extract as much metadata as possible. Just because a field is often empty doesn't mean we shouldn't try—build extractors for everything, populate what exists.

#### ArcGIS REST API → metadata.yaml Mapping

| ArcGIS Field | metadata.yaml Field | Reliability | Notes |
|--------------|---------------------|-------------|-------|
| Service URL | `source_url` | ✓ Always | The extraction source |
| `copyrightText` | `attribution` | Sometimes | Often empty, but extract if present |
| `description` | (STAC description) | Sometimes | Service-level description |
| `serviceDescription` | `processing_notes` | Sometimes | Additional context |
| `documentInfo.Author` | `contact.name` | Rare | Worth trying |
| `documentInfo.Keywords` | `keywords` | Rare | Comma-separated → list |
| `accessInformation` | `known_issues` | Sometimes | Access restrictions, caveats |
| `licenseInfo` | (logged, not mapped) | Sometimes | Free-form, not SPDX—log for human review |
| Field aliases | (STAC table:columns) | Common | Human-readable field names |

#### What We DON'T Extract (Auto-extracted from Parquet)

These are handled by STAC auto-generation, not ArcGIS scraping:
- bbox, extent, geometry
- CRS/projection
- Column names and types (schema)
- Feature count
- Statistics

#### What Typically Remains Empty (Human/LLM Enrichment)

Required metadata.yaml fields that usually need manual enrichment:
- `contact.email` — almost never in ArcGIS metadata
- `license` — `licenseInfo` is free-form, not SPDX (logged for human review)

Optional fields that may need enrichment:
- `citation`, `doi` — academic attribution (never in ArcGIS)
- `license_url` — link to full license text

The `portolan check` command will flag required fields as incomplete before push.

### Extraction Report

Written to `.portolan/extraction-report.json`:

```json
{
  "extraction_date": "2026-03-30T14:30:00Z",
  "source_url": "https://services.arcgis.com/.../FeatureServer",
  "portolan_version": "0.4.0",
  "gpio_version": "0.2.0",
  "metadata_extracted": {
    "source_url": "https://services.arcgis.com/.../FeatureServer",
    "attribution": "City of Philadelphia",
    "keywords": ["census", "demographics"],
    "contact_name": null,
    "processing_notes": null,
    "known_issues": null,
    "license_info_raw": "Public domain - no restrictions"
  },
  "layers": [
    {
      "id": 0,
      "name": "Census_Block_Groups",
      "status": "success",
      "features": 1336,
      "size_bytes": 1949696,
      "duration_seconds": 12.4,
      "output_path": "census_block_groups/census_block_groups.parquet",
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
    "skipped": 0,
    "total_features": 45000,
    "total_size_bytes": 52428800,
    "total_duration_seconds": 180
  }
}
```

The `metadata_extracted` block documents exactly what was found vs. what was empty, enabling human/LLM enrichment to focus on gaps. The `license_info_raw` field captures the original free-form text for manual SPDX mapping.

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
| `--exclude-layers` | none | Exclude specific layers (by ID or name, comma-separated) |
| `--services` | all | Include specific services for root URLs (glob patterns) |
| `--exclude-services` | none | Exclude specific services (glob patterns) |
| `--workers` | 3 | Parallel page requests per layer |
| `--retries` | 3 | Retry attempts per failed layer |
| `--timeout` | 60 | Per-request timeout in seconds |
| `--resume` | false | Resume from existing extraction-report.json (skip succeeded layers) |
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

# Extract from services root, include only Census and Transportation
portolan extract arcgis https://services.arcgis.com/.../rest/services ./output \
  --services "Census*,Transportation*"

# Extract from services root, exclude a few problematic services
portolan extract arcgis https://services.arcgis.com/.../rest/services ./output \
  --exclude-services "Legacy*,Test*,Archive*"

# Dry run to see what would be extracted
portolan extract arcgis https://services.arcgis.com/.../FeatureServer --dry-run

# Resume a failed extraction (skip already-succeeded layers)
portolan extract arcgis https://services.arcgis.com/.../FeatureServer ./output --resume

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

### Resume Behavior

When `--resume` is specified:

1. Load existing `.portolan/extraction-report.json`
2. For each layer in the current service:
   - If `status: "success"` in report → skip (already extracted)
   - If `status: "failed"` in report → retry
   - If not in report (new layer) → extract
3. Merge new results into existing report
4. Update summary counts

```
→ Resuming extraction (found extraction-report.json)
  ✓ Skipping 8 already-succeeded layers
  → Retrying 2 failed layers...

✓ Layer 5 (Previously_Failed): 1,234 features
✗ Layer 7 (Still_Failing): Timeout after 3 retries

Summary:
  ✓ 9/10 layers extracted successfully
  ✗ 1 layer failed
```

If no extraction report exists, `--resume` is a no-op (proceeds normally).

## Testing Strategy

### Unit Tests
- URL parsing (FeatureServer vs root detection)
- Layer filtering logic (by ID, by name, include/exclude)
- Service filtering (glob patterns, include/exclude)
- Extraction report generation
- Resume logic (skip succeeded, retry failed)
- Metadata extraction mapping

### Integration Tests
- Mock ArcGIS server responses
- End-to-end extraction with fixture data
- Retry behavior on simulated failures

### Real-World Tests (Manual)
- Philadelphia services: `https://services.arcgis.com/fLeGjb7u4uXqeF9q/ArcGIS/rest/services`
- Den Haag services (existing test data)

## Resolved Questions

1. **Authentication:** Out of scope for MVP. gpio already supports auth (`ArcGISAuth`), but this command targets public data only. Future work tracked in [geoparquet-io #318](https://github.com/geoparquet/geoparquet-io/issues/318) (unified auth for downstream tools) and related [#310](https://github.com/geoparquet/geoparquet-io/issues/310) (WFS auth).

2. **Rate Limiting:** Handled by gpio via `max_workers` and built-in throttling. No additional rate limiting needed at Portolan layer.

3. **Resume Capability:** Yes—`--resume` flag uses existing `extraction-report.json` to skip succeeded layers and retry failed ones.

## References

- [Issue #6: Full ArcGIS Server → Portolan conversion](https://github.com/portolan-sdi/portolan-cli/issues/6)
- [gpio extract arcgis documentation](https://geoparquet.io/cli/extract/?h=arcgis#extract-arcgis)
- [Den Haag roundtrip workflow](../../../portolan-test-data/den-haag-roundtrip/ROUNDTRIP-WORKFLOW.md)
- [ADR-0031: Collection-level assets for vector data](../adr/0031-collection-level-assets-for-vector-data.md)
- [ADR-0032: Nested catalogs with flat collections](../adr/0032-nested-catalogs-with-flat-collections.md)
- [ADR-0038: metadata.yaml enrichment](../adr/0038-metadata-yaml-enrichment.md)
- [ADR-0030: Agent-native CLI design](../adr/0030-agent-native-cli-design.md)
- [geoparquet-io #318: Unified auth for downstream tools](https://github.com/geoparquet/geoparquet-io/issues/318)
- [geoparquet-io #310: WFS authentication support](https://github.com/geoparquet/geoparquet-io/issues/310)
