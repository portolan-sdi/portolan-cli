# Layer address (#652)

The address ends in `/MapServer/0`, so it points to one layer. The command
must get that layer, or it must fail. It must not report success with 0
layers.

## Source

- The upstream service is
  `https://gisportal.boston.gov/arcgis/rest/services/BaseServices/Open_Space_Public/MapServer`.
- The responses date from 2026-10-23.
- The feature page keeps 2 of 559 features. Each geometry keeps its largest
  ring, with 40 vertices or fewer.
- The count response says 2.
- The page skips one beach feature, because codespell reads its place name as
  a typo.
