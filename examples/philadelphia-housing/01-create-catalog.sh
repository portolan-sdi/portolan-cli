#!/bin/sh
# Create a local Portolan catalog from two Philadelphia ArcGIS services.
set -eu

: "${CATALOG_DIR:?Set CATALOG_DIR to a new catalog directory}"

arcgis_url=${PORTOLAN_PHL_ARCGIS_URL:-https://services.arcgis.com/fLeGjb7u4uXqeF9q/ArcGIS/rest/services}

portolan extract arcgis "$arcgis_url" "$CATALOG_DIR" \
    --services "AffordableHousingProduction,Council_Districts_2024" \
    --workers 2 \
    --retries 3 \
    --license other \
    --license-url "https://metadata.phila.gov/#help/help-faqs/what-are-the-terms-of-use/" \
    --auto
