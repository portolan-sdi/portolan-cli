# ADR-0033: ESRI GDB Rasters Require External GDAL

## Status
Accepted

## Context
ESRI File Geodatabase (.gdb) rasters cannot be read by any pure Python library. The format was reverse-engineered exclusively for GDAL's OpenFileGDB driver—no alternative implementations exist.

Supporting GDB rasters in Portolan would require:
- System GDAL installation, or
- conda-forge rasterio (which bundles GDAL)

Neither option is acceptable:
- **System GDAL** creates installation friction and platform-specific issues
- **conda-forge** breaks our `pipx install portolan` story (per ADR-0008)

This affects a narrow use case: raster data stored in ESRI geodatabases. Vector GDB support is unaffected (pyogrio handles it).

## Decision
**Do not support ESRI GDB rasters.** Users must pre-convert to COG using GDAL before cataloging:

```bash
gdal_translate input.gdb/raster_name output.tif -of COG
```

Portolan will:
1. Detect GDB rasters during `scan`
2. Report them as requiring manual conversion
3. Document the workaround in user-facing docs

## Consequences
- ✅ No GDAL dependency—keeps installation simple
- ✅ Core remains pip-installable via pipx
- ⚠️ GDB raster users need extra step before import
- ⚠️ Cannot auto-convert GDB rasters with `--convert`

## Alternatives Considered

### Add GDAL as optional dependency
Rejected: GDAL is notoriously difficult to package. Even as optional, it creates support burden and confuses the installation story.

### Create portolan-gdb plugin
Rejected for MVP: Adds maintenance burden for a niche format. Can revisit if demand materializes.

### Bundle GDAL via conda
Rejected: Breaks pipx installation model (ADR-0008). Users expect `pipx install portolan` to work.
