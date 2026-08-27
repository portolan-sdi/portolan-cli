#!/bin/sh
# Build, validate, and optionally publish the Philadelphia housing Catalog.
set -eu

: "${CATALOG_DIR:?Set CATALOG_DIR to a new catalog directory}"

arcgis_url=${PORTOLAN_PHL_ARCGIS_URL:-https://services.arcgis.com/fLeGjb7u4uXqeF9q/ArcGIS/rest/services}
example_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

portolan extract arcgis "$arcgis_url" "$CATALOG_DIR" \
    --services "AffordableHousingProduction,Council_Districts_2024" \
    --workers 2 \
    --retries 3 \
    --license other \
    --license-url "https://metadata.phila.gov/#help/help-faqs/what-are-the-terms-of-use/" \
    --auto

cp "$example_dir/metadata/catalog.yaml" "$CATALOG_DIR/.portolan/metadata.yaml"
mkdir -p "$CATALOG_DIR/affordablehousingproduction/.portolan"
mkdir -p "$CATALOG_DIR/council_districts_2024/.portolan"
cp \
    "$example_dir/metadata/affordable_housing.yaml" \
    "$CATALOG_DIR/affordablehousingproduction/.portolan/metadata.yaml"
cp \
    "$example_dir/metadata/council_districts_2024.yaml" \
    "$CATALOG_DIR/council_districts_2024/.portolan/metadata.yaml"

portolan add \
    --portolan-dir "$CATALOG_DIR" \
    --force \
    --thumbnails \
    "$CATALOG_DIR/affordablehousingproduction" \
    "$CATALOG_DIR/council_districts_2024"

(cd "$CATALOG_DIR" && portolan readme)
# Refresh the generated documentation Asset metadata.
portolan check "$CATALOG_DIR" --fix --workers 1
portolan check "$CATALOG_DIR" --strict

echo "Catalog passes the Portolan check."

if [ -n "${PORTOLAN_PHL_REMOTE:-}" ]; then
    portolan push "$PORTOLAN_PHL_REMOTE" --catalog "$CATALOG_DIR"
    echo "Published Catalog to $PORTOLAN_PHL_REMOTE."
fi
