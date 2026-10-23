# Layer IDs with gaps (#1003)

The IDs of the first three layers of this map service are 2, 9, and 10.
`--layers 2` must select layer ID 2. It must not select the layer at list
position 2.

## Source

- The upstream service is
  `https://eh-gis.ehamptonny.gov/arcgis/rest/services/MapServiceForOpenGov/MapServer`.
- The responses date from 2026-10-23.
- The service document keeps the layers 2, 9, and 10 of 59.
- Each feature page keeps 2 features. Each geometry keeps its largest ring,
  with 40 vertices or fewer.
- Each count response says 2.
