# Portolan Examples

This directory contains reference implementations of Portolan catalogs.

## Available Examples

### Tabular Collection (Non-Geospatial)

A reference implementation of a tabular (non-geospatial) collection.

- **File**: [tabular-collection.json](tabular-collection.json)
- **Demonstrates**:
  - `portolan:geospatial: false` flag for non-spatial data
  - STAC Table extension for schema documentation
  - Temporal extent without spatial extent
  - Collection-level assets for single-file tabular data
  - Provenance link to source (Eurostat)

### Argentina 2022 Census

A complete implementation of Argentina's 2022 census data as a Portolan catalog.

- **Status**: In progress
- **Publication**: Will be available on [Source.Cooperative](https://source.coop/) once complete
- **Formats**: GeoParquet (tabular data + geometries), PMTiles (visualization)
- **Demonstrates**:
  - Multi-level geographic hierarchy (nation → province → department → census tract)
  - Versioning and temporal data management
  - Machine-readable metadata for coded variables
  - Default styling and visualization

## Using Examples

Examples serve as canonical references for what valid Portolan catalogs look like. When in doubt about how to structure your catalog or implement a specific feature, refer to these examples.
