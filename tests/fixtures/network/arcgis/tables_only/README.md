# Service with tables and no layers (#812)

The service document lists one table in `tables` and no layer in `layers`.
The command must exit non-zero, and its message must state the tables (#1034).

## Source

- #812 shows the shape of
  `https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/zoning_descriptions/FeatureServer`.
- On 2026-10-23 that service sends `400 Invalid URL`.
- The service document is hand-written.
