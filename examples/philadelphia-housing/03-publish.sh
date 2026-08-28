#!/bin/sh
# Publish the finished catalog to object storage.
set -eu

: "${CATALOG_DIR:?Set CATALOG_DIR to the catalog directory}"
: "${PORTOLAN_PHL_REMOTE:?Set PORTOLAN_PHL_REMOTE to an object-storage URL}"

portolan push "$PORTOLAN_PHL_REMOTE" --catalog "$CATALOG_DIR"
echo "Published catalog to $PORTOLAN_PHL_REMOTE."
