# Empty response for a named layer (#963)

The GetFeature response is a valid feature collection with 0 features. The
run selects the layer with `--layers`, so the command must exit non-zero
(#1034).

## Source

- The upstream service is `https://geodata.naturvardsverket.se/n2000/wfs`,
  layer `N2000_WFS:N2000`, version 2.0.0.
- The responses date from 2026-10-23. The layer has 4136 features.
- The capabilities, DescribeFeatureType, GetStyles, and GetLegendGraphic
  responses are the real responses.
- The `hits` response says `numberMatched="0"`, and the GetFeature response
  contains no feature. #963 reports this response from the degraded server.
