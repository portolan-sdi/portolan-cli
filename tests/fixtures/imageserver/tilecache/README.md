# ImageServer tile cache fixtures

Real tiles from a hosted tiled imagery layer on ArcGIS Online. The service is
`Atlantic_Marine_Mammals__Southeast_Blueprint_Indicator_2023_` on
`tiledimageservices.arcgis.com`. It reports `capabilities: "Image,TilesOnly"`,
so it rejects `exportImage` and serves only the cache (issue #870).

| File | Source | Content |
|------|--------|---------|
| `lerc2d_level0_0_0.bin` | `/ImageServer/tile/0/0/0` | LERC2D v6, 256x256, `uint8`, 2851 valid pixels with class values 1 to 10 |
| `lerc2d_empty.bin` | `/ImageServer/tile/9/0/0` | LERC2D v6, 256x256, 0 valid pixels |

GDAL 3.12 fails to decode both with `MRF: Error decoding Lerc`, because its MRF
driver reads an older LERC2 version. The `lerc` package decodes both.
