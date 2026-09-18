# Issue: ArcGIS ImageServer exportImage returns HTTP 5xx for large tiles

## Symptom

`portolan extract arcgis <ImageServer URL>` downloads some tiles and then
reports other tiles as failed with an HTTP 500 response from `exportImage`.
Issue #870 shows this against the USGS NAIP service with `--tile-size 3000`
and the default four concurrent requests. Five of twelve tiles failed after
three attempts each.

Since September 2026 the failure line gives the reason. It quotes the ArcGIS
`error.message` and `error.details` when the body is JSON, an excerpt of the
body otherwise, and the full `exportImage` URL in every case. The run then
prints a recovery hint.

## Root cause

The server builds each tile on demand. A 3000 by 3000 pixel, four band export
over a mosaic dataset takes the server several seconds. Under parallel load
the server times out or runs out of memory on some requests and returns HTTP
500 with the message "Unable to complete operation." The client cannot make
the server succeed. The failure is not a bug in the tile request itself, so a
retry of the same request often succeeds later.

The client already retries each tile three times with exponential backoff.
`--retries` raises that count for the raster path since September 2026.
Before that the flag only applied to FeatureServer layers.

## Workaround

Run the same command again with `--resume`. The resume state keeps the failed
tiles and skips the tiles that succeeded, so the second run retries the
failures alone.

If the second run fails on the same tiles, lower the load per request:

```bash
portolan extract arcgis <ImageServer URL> . --auto --resume \
  --tile-size 1024 --max-concurrent 1 --retries 5
```

A smaller `--tile-size` produces more tiles, and each one costs the server
less. Lower `--max-concurrent` stops parallel requests from competing for the
same server resources.

## References

- Issue #870, "Unable to extract data from an ESRI Image Service"
- ArcGIS REST API, Export Image (Image Service):
  https://developers.arcgis.com/rest/services-reference/enterprise/export-image/

## Regression test

- `tests/unit/extract/arcgis/imageserver/test_extractor.py::TestHttpErrorMessages`
- `tests/unit/extract/arcgis/imageserver/test_orchestrator.py::TestRetriesAndFailureHint`
