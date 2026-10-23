# Philadelphia housing services

Two one-layer FeatureServers and a services root, for the tutorial test in
`tests/docs/test_v1_examples.py`.

- Source: `https://services.arcgis.com/fLeGjb7u4uXqeF9q/ArcGIS/rest/services`,
  services `AffordableHousingProduction` and `Council_Districts_2024`.
- The feature pages come from the pinned Assets in
  `tests/fixtures/realdata/philadelphia-housing/` through `pages_from`. That
  directory names the query that produced them.
- The service and layer documents are authored. They hold the subset of the
  ArcGIS REST schema that `portolan extract arcgis` reads.
- `Unrelated_Parks` in `services.json` has no routes. The test asserts that
  `--services` does not request it.
- The first request for `resultOffset=200` of the affordable housing layer
  answers 503 once. The test asserts that the extractor retries the page.

No credential appears in any file.
