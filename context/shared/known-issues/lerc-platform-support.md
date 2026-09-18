# Issue: the lerc package includes no Linux aarch64 binary

## Symptom

On Linux aarch64, every tile fails when the service is a hosted tiled imagery
layer whose cache stores LERC:

```
✗ Tile tile_0_1: failed [1/2]: This service stores LERC tiles. GDAL cannot decode them, because its MRF driver reads an older LERC2 version, so Portolan needs the 'lerc' package. The package is installed, and its binary did not load on this platform: libLerc.so.4: cannot open shared object file: No such file or directory. The lerc wheel carries binaries for Windows x64, macOS, and Linux x86-64 only. Run the extraction on one of those. A cache that stores PNG or JPEG needs no LERC decoder, and reads on every platform.
```

Every other Portolan command works. Only a LERC cache needs the package.

## Root cause

A service that reports `capabilities: "Image,TilesOnly"` rejects `exportImage`
and serves its data from a tile cache. Most such services store LERC.

GDAL cannot decode those tiles. GDAL 3.12 opens a LERC2 version 6 tile with
its MRF driver and then fails:

```
MRF: Error decoding Lerc
```

GDAL decodes LERC inside a GeoTIFF, because that path uses the Lerc 4.0 C API.
Its MRF driver carries an older LERC2 reader, which stops at version 5. A
hand-written `.mrf` header does not change the result.

So `portolan_cli/extract/arcgis/imageserver/tilecache.py` uses the `lerc`
package from Esri. Version 4.0.1 is a `py3-none-any` wheel that carries three
prebuilt binaries: `Lerc.dll` for Windows x64, a universal `libLerc.dylib` for
macOS, and `libLerc.so.4` for Linux x86-64. There is no Linux aarch64 binary,
and the last release is from March 2023.

`pip install lerc` succeeds on every platform, because the wheel declares
none. The failure happens later, when ctypes loads the shared library.

## Workaround

Run the extraction on x86-64, macOS, or Windows. There is no pure-Python LERC
decoder.

## Constraint accepted

`tilecache.py` calls `_load_lerc` from the decode path, not at module import.
This placement has these effects:

- An install on Linux aarch64 works, and every other Portolan command works.
- The message specifies the service that needed the package. It separates the two
  failures: an absent package says to install it, and an unloadable binary
  says which platforms include one.
- A cache that stores PNG or JPEG reads on every platform, because rasterio
  decodes those tiles.
