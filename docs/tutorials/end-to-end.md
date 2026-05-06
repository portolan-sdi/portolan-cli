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

Creates `buildings/` with GeoParquet (Hilbert-ordered), STAC catalog/collection JSON, and metadata seeded from ISO 19139 records. The full dataset is 3.8M features (~750MB).

## Generate PMTiles

```bash
portolan add buildings/**/*.parquet --pmtiles --workers 4
```

Generates vector tiles (`.pmtiles`), a Mapbox GL style (embedded in STAC as `pmtiles:style`), and a thumbnail (`.thumb.jpg`). The `--workers` flag parallelizes metadata extraction.

## Validate and Fix

```bash
portolan scan --tree
portolan check --fix
```

Scan shows directory structure with status markers. Check validates STAC and converts non-cloud-native formats. The `--fix` flag updates stale metadata and generates missing bbox/temporal extents. Add `--dry-run` to preview.

## Generate READMEs

```bash
portolan readme --recursive
```

Generates from STAC metadata plus `.portolan/metadata.yaml`. Edit `metadata.yaml` for descriptions, licenses, attribution—never edit README.md directly.

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

Parallelizes across collections (`--workers`) and files within each collection (`--concurrency`). With 4 workers and 16 concurrency: up to 64 simultaneous uploads.

## Result

Published catalog includes GeoParquet, PMTiles with inline style, thumbnails, and READMEs—all accessible via HTTP range requests.
