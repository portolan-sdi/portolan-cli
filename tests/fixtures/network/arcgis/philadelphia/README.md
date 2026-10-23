# Philadelphia housing services

The tutorial test in `tests/docs/test_v1_examples.py` uses this scenario. It
contains two feature services and a services root.

## Source

- The upstream root is
  `https://services.arcgis.com/fLeGjb7u4uXqeF9q/ArcGIS/rest/services`.
- The services are `AffordableHousingProduction` and `Council_Districts_2024`.
- The feature pages come from the Assets in
  `tests/fixtures/realdata/philadelphia-housing/` through `pages_from`. The
  README in that directory gives the query for each Asset.
- The service and layer documents are hand-written. They contain the fields
  that `portolan extract arcgis` reads.
- `Unrelated_Parks` in `services.json` has no routes. The test asserts that
  `--services` does not request it.
- The first request for `resultOffset=200` of the housing layer gets a 503.
  The test asserts that the extractor requests the page again.
- The files contain no credential.
