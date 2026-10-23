# Empty WFS FeatureCollection (#963)

The service answers a well-formed FeatureCollection with 0 features for a
layer that `--layers` names. The run must exit non-zero (#1034).

- Source: `https://geodata.naturvardsverket.se/n2000/wfs`, layer
  `N2000_WFS:N2000`, WFS 2.0.0, recorded on 2026-10-23.
- The capabilities, DescribeFeatureType, GetStyles, and GetLegendGraphic
  responses are real. The layer holds 4136 features. The `hits` response says
  `numberMatched="0"`, and the GetFeature response holds no feature. This is
  the answer of the degraded server in #963.
