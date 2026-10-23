# ArcGIS layer URL (#652)

The URL names one layer: `.../MapServer/0`. The extraction must extract that
layer or fail. It must not report success with 0 layers.

- Source: `https://gisportal.boston.gov/arcgis/rest/services/BaseServices/Open_Space_Public/MapServer`,
  recorded on 2026-10-23.
- The feature page keeps 2 of 559 features, with each geometry reduced to its
  largest ring of at most 40 vertices. The count response says 2. The page
  skips one beach feature, because codespell reads its place name as a typo.
