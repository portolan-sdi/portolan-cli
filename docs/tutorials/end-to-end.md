# End-to-End Tutorial: Belgium Buildings

This tutorial walks through the complete Portolan workflow using real-world data: Belgium INSPIRE building footprints from a WFS service.

**You'll learn:**

- Extracting data from WFS services
- Initializing and managing a Portolan catalog
- Generating PMTiles with automatic styles and thumbnails
- Validating and fixing metadata
- Generating documentation
- Publishing to cloud storage

## Prerequisites

- Portolan CLI installed (`pipx install portolan-cli`)
- AWS credentials configured (for S3 push)

## Step 1: Create a Workspace

```bash
mkdir belgium-buildings && cd belgium-buildings
```

## Step 2: Extract from WFS

Belgium's Wallonia region publishes INSPIRE-compliant building footprints via WFS. First, explore what's available:

```bash
portolan extract wfs \
  "https://geoservices.wallonie.be/geoserver/inspire_bu/ows" \
  --dry-run
```

Output shows two layers:

- `inspire_bu:BU.Building_building_emprise` — building footprints
- `inspire_bu:BU.Building_building_lod1` — 3D building models

Extract the building footprints:

```bash
portolan extract wfs \
  "https://geoservices.wallonie.be/geoserver/inspire_bu/ows" \
  buildings \
  --layers "inspire_bu:BU.Building_building_emprise" \
  --auto
```

This creates a catalog structure in `buildings/` with:

- GeoParquet data file (cloud-optimized, Hilbert-ordered)
- STAC catalog and collection metadata
- Metadata seeded from the WFS service's ISO 19139 records

!!! note "Full extract vs. limited"
    Remove `--limit` for production use. The full Belgium buildings dataset is ~750MB with 3.8M features.

## Step 3: Explore the Catalog

Check what was created:

```bash
portolan list
```

View collection details:

```bash
portolan info buildings/inspire_bu_bu_building_building_emprise
```

## Step 4: Generate PMTiles

PMTiles are cloud-optimized vector tiles that enable web mapping without a tile server. Generate them with automatic style and thumbnail:

```bash
portolan add buildings/**/*.parquet --pmtiles
```

This generates three assets per GeoParquet file:

| Asset | Purpose |
|-------|---------|
| `.pmtiles` | Vector tiles for web maps |
| `style.json` | Mapbox GL style (auto-generated colors) |
| `thumbnail.png` | Preview image with basemap |

## Step 5: Scan and Validate

Scan for metadata extraction:

```bash
portolan scan
```

Validate the catalog structure and auto-fix issues:

```bash
portolan check --fix
```

Common auto-fixes include:

- Adding missing bbox from geometry
- Setting temporal extent to null (unknown dates)
- Generating STAC item IDs

## Step 6: Generate Documentation

Create README files from STAC metadata:

```bash
portolan readme --recursive
```

This generates `README.md` at both catalog and collection levels, combining:

- STAC metadata (machine-extracted)
- Human enrichment from `.portolan/metadata.yaml`

!!! tip "Edit metadata.yaml, not README.md"
    The README is always regenerated. Add descriptions, licenses, and attribution to `metadata.yaml`.

## Step 7: Configure Remote

Create a `.env` file with your remote storage:

```bash
cat > .env << 'EOF'
PORTOLAN_REMOTE=s3://your-bucket/belgium-buildings/
PORTOLAN_PROFILE=your-aws-profile
EOF
```

## Step 8: Push to Cloud

Push the catalog:

```bash
portolan push
```

Portolan uploads:

- STAC catalog and collection JSON
- GeoParquet data files
- PMTiles, styles, and thumbnails
- READMEs

## Verify Publication

Your data is now available at the S3 URL. The PMTiles can be loaded directly in web mapping libraries:

```javascript
import maplibregl from "maplibre-gl";
import { Protocol } from "pmtiles";

const protocol = new Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

const map = new maplibregl.Map({
  container: "map",
  style: "https://your-bucket.s3.amazonaws.com/belgium-buildings/.../style.json",
});
```

## Summary

| Step | Command | Purpose |
|------|---------|---------|
| 1 | `extract wfs` | Pull data from WFS service |
| 2 | `list`, `info` | Explore catalog contents |
| 3 | `add --pmtiles` | Generate vector tiles + style + thumbnail |
| 4 | `scan` | Extract metadata |
| 5 | `check --fix` | Validate and auto-fix |
| 6 | `readme --recursive` | Generate documentation |
| 7 | `.env` config | Configure remote storage |
| 8 | `push` | Publish to cloud |

## Agent Usage

For AI agents working with Portolan, use `--json` for machine-parseable output:

```bash
portolan quickstart  # Dense agent reference
portolan list --json
portolan check --fix --json
```

See `portolan quickstart` for the complete agent reference.
