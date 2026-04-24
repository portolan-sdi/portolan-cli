# Extracting from WFS Services

This guide walks through extracting building footprints from a WFS (Web Feature Service), enriching metadata, generating PMTiles for web visualization, and publishing to Source Cooperative.

!!! info "Live Example"
    The result of this tutorial is published at:

    - **Source Cooperative:** [source.coop/nlebovits/belgium-buildings](https://source.coop/nlebovits/belgium-buildings/)
    - **STAC Browser:** [Browse the catalog](https://radiantearth.github.io/stac-browser/#/external/us-west-2.opendata.source.coop/nlebovits/belgium-buildings/catalog.json)
    - **PMTiles Viewer:** [View on map](https://protomaps.github.io/PMTiles/?url=https://us-west-2.opendata.source.coop/nlebovits/belgium-buildings/inspire_bu_bu_building_building_emprise_865a73/inspire_bu_bu_building_building_emprise_865a73.pmtiles)

## Prerequisites

- Portolan CLI installed (`pipx install portolan-cli`)
- [tippecanoe](https://github.com/felt/tippecanoe) for PMTiles generation
- AWS credentials configured for your target bucket

## Step 1: Explore the WFS Service

First, discover available layers without extracting:

```bash
portolan extract wfs \
  "https://geoservices.wallonie.be/geoserver/inspire_bu/ows" \
  --dry-run
```

```
Dry run - would extract 2 layers:
  • inspire_bu:BU.Building_building_emprise (ID: 0)
  • inspire_bu:BU.Building_building_lod1 (ID: 1)
```

Two layers available: building footprints (`building_emprise`) and 3D buildings (`building_lod1`).

## Step 2: Extract Building Footprints

Extract 100,000 features from the footprints layer:

```bash
portolan extract wfs \
  "https://geoservices.wallonie.be/geoserver/inspire_bu/ows" \
  buildings \
  --layers "inspire_bu:BU.Building_building_emprise" \
  --limit 100000 \
  --auto
```

Portolan automatically:

1. **Downloads features** via WFS with pagination
2. **Converts to GeoParquet** with Hilbert spatial ordering
3. **Generates STAC catalog** with proper metadata
4. **Seeds metadata.yaml** from ISO 19139 records (if available)

!!! tip "Metadata Auto-Extraction"
    Since v0.6.0, `portolan extract wfs` automatically fetches ISO 19139 metadata records
    from the WFS service, populating license, description, keywords, and contact info.

## Step 3: Enrich Metadata

The extraction creates two metadata files:

- `buildings/.portolan/metadata.yaml` — Catalog level
- `buildings/inspire_bu_.../.portolan/metadata.yaml` — Collection level

Review and complete these files:

```yaml title="buildings/.portolan/metadata.yaml"
contact:
  name: "Your Name"
  email: "your@email.com"

license: "CC-BY-4.0"
license_url: "https://creativecommons.org/licenses/by/4.0/"

title: "Belgium Building Footprints - Wallonia"
description: |
  INSPIRE-compliant building footprints for the Wallonia region.

keywords:
  - buildings
  - footprints
  - belgium
  - inspire
  - geoparquet

source_url: "https://geoservices.wallonie.be/geoserver/inspire_bu/ows"
attribution: "Service Public de Wallonie (SPW)"
```

!!! warning "License Verification"
    Always verify the license from the source. Check `_license_info_from_source` in
    the auto-generated metadata.yaml for hints from the WFS service.

## Step 4: Generate PMTiles

PMTiles enables efficient web-based map rendering. Generate from GeoParquet:

```bash
cd buildings/inspire_bu_bu_building_building_emprise_865a73

ogr2ogr -f GeoJSONSeq \
  -s_srs EPSG:3035 \
  -t_srs EPSG:4326 \
  /vsistdout/ \
  inspire_bu_bu_building_building_emprise_865a73.parquet | \
tippecanoe \
  -o inspire_bu_bu_building_building_emprise_865a73.pmtiles \
  -z14 -Z4 \
  --layer=buildings \
  --force \
  --read-parallel \
  --drop-densest-as-needed
```

!!! note "CRS Reprojection"
    Some WFS services return coordinates in projected CRS (like EPSG:3035) even when
    the GeoParquet metadata says WGS84. The `-s_srs` flag overrides the source CRS
    for correct reprojection.

Then add the PMTiles to your catalog:

```bash
cd buildings
portolan add inspire_bu_.../*.pmtiles --stac-geoparquet
```

## Step 5: Generate READMEs

Create human-readable documentation from STAC metadata:

```bash
portolan readme --recursive
```

This generates README.md files at catalog and collection levels.

## Step 6: Push to Source Cooperative

Configure your remote in `.env`:

```bash title="buildings/.env"
PORTOLAN_REMOTE=s3://us-west-2.opendata.source.coop/username/dataset/
PORTOLAN_PROFILE=source-coop
```

Push to the bucket:

```bash
portolan push --verbose
```

## Summary

| Step | Command |
|------|---------|
| Explore | `portolan extract wfs URL --dry-run` |
| Extract | `portolan extract wfs URL OUTPUT --layers PATTERN --limit N` |
| Metadata | Edit `.portolan/metadata.yaml` files |
| PMTiles | `ogr2ogr \| tippecanoe` pipeline |
| Add tiles | `portolan add *.pmtiles --stac-geoparquet` |
| READMEs | `portolan readme --recursive` |
| Push | `portolan push --verbose` |

## See Also

- [Extracting from ArcGIS](extract-arcgis.md) — Similar workflow for ArcGIS REST services
- [Metadata Defaults](metadata-defaults.md) — Configure default metadata values
- [CLI Reference](../reference/cli.md) — Full command documentation
