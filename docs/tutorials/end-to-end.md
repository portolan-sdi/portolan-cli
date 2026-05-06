# End-to-End: Belgium Buildings

Published catalog: [source.coop/nlebovits/belgium-buildings](https://source.coop/nlebovits/belgium-buildings)
Browse in STAC Browser: [radiantearth.github.io/stac-browser](https://radiantearth.github.io/stac-browser/#/external/us-west-2.opendata.source.coop/nlebovits/belgium-buildings/catalog.json)

## Extract from WFS

Wallonia publishes INSPIRE building footprints via WFS at `https://geoservices.wallonie.be/geoserver/inspire_bu/ows`. The `--dry-run` flag shows available layers without fetching data.

```bash
portolan extract wfs \
  "https://geoservices.wallonie.be/geoserver/inspire_bu/ows" \
  buildings \
  --layers "inspire_bu:BU.Building_building_emprise" \
  --auto
```

This creates `buildings/` with GeoParquet (Hilbert-ordered for spatial queries), STAC catalog/collection JSON, and metadata seeded from the service's ISO 19139 records. The full dataset is 3.8M features (~750MB).

## Generate PMTiles

```bash
portolan add buildings/**/*.parquet \
  --pmtiles \
  --workers 4
```

The `--pmtiles` flag generates vector tiles, a Mapbox GL style, and a PNG thumbnail for each GeoParquet. The `--workers` flag parallelizes metadata extraction across files.

## Validate and Fix

```bash
portolan scan --tree
portolan check --fix
```

The `--tree` flag shows directory structure with status markers. The `--fix` flag converts non-cloud-native formats, updates stale STAC metadata, and generates missing bbox/temporal extents. Add `--dry-run` to preview changes.

## Generate READMEs

```bash
portolan readme --recursive
```

READMEs are generated from STAC metadata plus `.portolan/metadata.yaml` (human enrichment layer). Edit `metadata.yaml` to add descriptions, licenses, and attribution—never edit README.md directly.

## Configure Remote

```bash
cat > .env << 'EOF'
PORTOLAN_REMOTE=s3://us-west-2.opendata.source.coop/nlebovits/belgium-buildings/
PORTOLAN_PROFILE=source-coop
EOF
```

## Push

```bash
portolan push --workers 4 --concurrency 16
```

The `--workers` flag parallelizes across collections (each gets its own upload thread). The `--concurrency` flag controls parallel file uploads within each collection. With 4 workers and 16 concurrency, you get up to 64 simultaneous uploads.

## Result

The published catalog includes GeoParquet, PMTiles, style.json, thumbnail.png, and READMEs—all accessible via HTTP range requests without a tile server.
