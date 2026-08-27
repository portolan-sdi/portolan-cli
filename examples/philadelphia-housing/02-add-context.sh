#!/bin/sh
# Add useful descriptions, previews, and documentation to the local catalog.
set -eu

: "${CATALOG_DIR:?Set CATALOG_DIR to the catalog directory}"

example_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

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
# Refresh the generated documentation asset metadata.
portolan check "$CATALOG_DIR" --fix --workers 1
portolan check "$CATALOG_DIR" --strict

echo "Catalog passes the Portolan check."
