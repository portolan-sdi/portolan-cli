# Layer names with a workspace prefix (#768)

The feature type names are `topp:states` and `topp:tasmania_roads`.
`--exclude-layers states` must remove `topp:states` from the run.

## Source

- The upstream service is `https://gs-stable.geo-solutions.it/geoserver/wfs`,
  version 2.0.0.
- The responses date from 2026-10-23.
- The capabilities document keeps these two feature types. It drops the
  `fes:Functions` list.
- Each link has `{{base_url}}` in place of the upstream origin.
- Each GetFeature response keeps 2 features. Each geometry has 30 vertices or
  fewer.
- Each count says 2.
