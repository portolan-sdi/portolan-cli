# ImageServer tile cache fixtures

Real tiles from a hosted tiled imagery layer on ArcGIS Online. The service is
`Atlantic_Marine_Mammals__Southeast_Blueprint_Indicator_2023_` on
`tiledimageservices.arcgis.com`. It reports `capabilities: "Image,TilesOnly"`,
so it rejects `exportImage` and serves only the cache (issue #870).

| File | Source | Content |
|------|--------|---------|
| `lerc2d_level0_0_0.bin` | `/ImageServer/tile/0/0/0` | LERC2D v6, 256x256, `uint8`, 2851 valid pixels with class values 1 to 10 |
| `lerc2d_empty.bin` | `/ImageServer/tile/9/0/0` | LERC2D v6, 256x256, 0 valid pixels |
| `lerc2d_float32.bin` | `make_lerc2d_float32.py` | LERC2D v6, 256x256, `float32`, 43,887 valid pixels that all hold 741.4 |

GDAL 3.12 fails to decode the two service tiles with `MRF: Error decoding Lerc`,
because its MRF driver reads an older LERC2 version. The `lerc` package decodes
them.

`lerc2d_float32.bin` is synthetic, because no float service publishes a tile
small enough to commit. `make_lerc2d_float32.py` builds it with the same `lerc`
encoder, and writes the same LERC2 version 6 that a live service stores. It
holds a ragged valid footprint, so no reader can infer the mask from the pixel
values. The live float service `FRR1_100yr_WSEL` (ESRI:102697) shows the same
shape of data.
