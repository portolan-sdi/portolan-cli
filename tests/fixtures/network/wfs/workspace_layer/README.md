# WFS names with a workspace prefix (#768)

The FeatureType names are `topp:states` and `topp:tasmania_roads`.
`--exclude-layers states` must exclude `topp:states`.

- Source: `https://gs-stable.geo-solutions.it/geoserver/wfs`, WFS 2.0.0,
  recorded on 2026-10-23.
- The capabilities keep only these two FeatureTypes and drop the
  `fes:Functions` list. The upstream origin in each link is `{{base_url}}`.
- Each GetFeature response keeps 2 features, with each geometry reduced to at
  most 30 vertices. Each count says 2.
