# Sparse ArcGIS layer IDs (#1003)

A MapServer whose layer IDs have gaps. Its first three layers have the IDs 2,
9, and 10. `--layers 2` must extract layer ID 2, not the layer at list
position 2.

- Source: `https://eh-gis.ehamptonny.gov/arcgis/rest/services/MapServiceForOpenGov/MapServer`,
  recorded on 2026-10-23.
- The service document keeps only the layers 2, 9, and 10 of 59.
- Each feature page keeps 2 features, with each geometry reduced to its
  largest ring of at most 40 vertices. Each count response says 2.
