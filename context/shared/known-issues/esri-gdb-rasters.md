# Issue: ESRI File Geodatabase rasters are unreadable and undetected

## Symptom

A raster stored inside an ESRI File Geodatabase (`.gdb`) cannot be cataloged.
`.gdb` paths route to the vector pipeline unconditionally
(`portolan_cli/formats.py:479`), so a raster geodatabase fails during conversion
rather than being reported up front.

Vector layers in `.gdb` are unaffected and work through pyogrio.

## Root cause

The GDB raster format was reverse-engineered for GDAL's OpenFileGDB driver
alone. No pure Python implementation exists, so reading these rasters requires
either a system GDAL installation or conda-forge rasterio, which bundles GDAL.

## Why Portolan does not add GDAL

Both paths were rejected, and this remains the standing decision:

- **System GDAL** creates installation friction and platform-specific breakage.
- **conda-forge** breaks the `uv tool install portolan` path that the README
  documents.

Adding GDAL as an optional dependency was also rejected. GDAL is difficult to
package, and an optional dependency still generates support burden while
muddying the installation story. Revisit if demand for GDB rasters materializes;
a separate plugin remains possible.

## Workaround

Convert to COG with GDAL before cataloging:

```bash
gdalinfo input.gdb
gdal_translate input.gdb/raster_name output.tif -of COG
```

`docs/reference/formats.md` documents this for users.

## Outstanding gap

Detection is unimplemented. The original decision called for `scan` to identify
GDB rasters and report them as requiring manual conversion. No such branch
exists, so users encounter a conversion failure instead of a clear message.

## Regression test

None. The limitation is a missing dependency rather than a code defect.
