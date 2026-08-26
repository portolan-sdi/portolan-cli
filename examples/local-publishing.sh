#!/bin/sh
# Publish a local catalog from a small committed GeoJSON fixture.
set -eu

: "$CATALOG_DIR"
: "$PORTOLAN_EXAMPLE_SOURCE"

portolan init "$CATALOG_DIR" --auto --license CC-BY-4.0
mkdir -p "$CATALOG_DIR/places"
cp "$PORTOLAN_EXAMPLE_SOURCE" "$CATALOG_DIR/places/points.geojson"
portolan add --portolan-dir "$CATALOG_DIR" "$CATALOG_DIR/places"
portolan check "$CATALOG_DIR" --geo-assets
