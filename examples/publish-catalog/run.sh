#!/bin/sh
# Build, validate, publish, and clone one Portolan Catalog.
set -eu

: "${CATALOG_DIR:?Set CATALOG_DIR to a new catalog directory}"
: "${PORTOLAN_EXAMPLE_SOURCE:?Set PORTOLAN_EXAMPLE_SOURCE to a GeoParquet file}"

example_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

portolan init "$CATALOG_DIR" --auto --license CC-BY-4.0
cp "$example_dir/metadata.yaml" "$CATALOG_DIR/.portolan/metadata.yaml"

mkdir -p "$CATALOG_DIR/places"
cp "$PORTOLAN_EXAMPLE_SOURCE" "$CATALOG_DIR/places/points.parquet"

portolan add --portolan-dir "$CATALOG_DIR" --thumbnails "$CATALOG_DIR/places"
(cd "$CATALOG_DIR" && portolan readme)
portolan check "$CATALOG_DIR" --strict
portolan info "$CATALOG_DIR/places" --catalog "$CATALOG_DIR"
portolan version current places --catalog "$CATALOG_DIR"

echo "Catalog passes the Portolan check."

if [ -n "${PORTOLAN_EXAMPLE_REMOTE:-}" ]; then
    : "${CLONED_CATALOG_DIR:?Set CLONED_CATALOG_DIR when publishing}"
    portolan push "$PORTOLAN_EXAMPLE_REMOTE" --catalog "$CATALOG_DIR"
    portolan clone "$PORTOLAN_EXAMPLE_REMOTE" "$CLONED_CATALOG_DIR"
    portolan check "$CLONED_CATALOG_DIR" --no-data --strict
    echo "Published catalog and verified its clone."
fi
