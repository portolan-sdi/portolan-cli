# ADR-0057: Adopt STAC raster extension v2.0.0

## Status
Accepted

## Context
Portolan already emits the STAC 1.1 unified, top-level `bands` array for raster
assets (the COG pipeline builds `{"bands": [...]}` with `name`, `data_type`,
`nodata`, plus `raster:spatial_resolution`). But it declared the raster
extension at **v1.1.0**, whose schema still models bands as `raster:bands` and
uses an unprefixed `spatial_resolution`. The declared version therefore did not
match the fields we write.

The raster extension **v2.0.0** (released 2024-09-09, stable) is the release that
drops `raster:bands` in favor of the STAC 1.1 common-metadata `bands` array, and
renames the remaining raster fields with a `raster:` prefix (e.g.
`raster:spatial_resolution`). That is exactly the model Portolan produces.

Separately, the ArcGIS ImageServer harvest path was the last emitter of the
legacy `raster:bands` field, on both the item data asset and the collection
summaries, so it diverged from the COG pipeline.

## Decision
1. **Declare the raster extension at v2.0.0** everywhere it is referenced
   (`stac.py` `EXTENSION_URLS["raster"]`, and the ArcGIS ImageServer collection
   and item builders).
2. **Emit the unified `bands` array from the ArcGIS path**, replacing
   `raster:bands` on the data asset and in collection summaries, so it matches
   the COG pipeline and the v2.0.0 model.
3. **Keep reading legacy `raster:bands`** in the README generator so previously
   generated catalogs still render, while preferring `eo:bands` then `bands`.

This follows ADR-0037 (use current extensions, accept migration cost, no
fallback prefixes).

## Consequences
- The declared schema version now matches the band model Portolan writes, so the
  output is internally consistent (and would validate against the v2.0.0 schema).
- ArcGIS-harvested catalogs align with COG catalogs on the band model.
- Clients that only read `raster:bands` (v1.1.0) will no longer find bands on
  Portolan output, they must read the unified `bands` array. The COG pipeline
  already stopped emitting `raster:bands`, so this only newly affects the ArcGIS
  path.
- Compliance validation is unaffected. Per ADR-0056 only the STAC core 1.1.0
  schemas are vendored, and core 1.1.0 already defines `bands`. The raster
  extension schema itself is not vendored.

## Alternatives considered
- **Stay on v1.1.0 and downgrade emission to `raster:bands`:** Rejected. It moves
  away from the STAC 1.1 unified band model that core and the rest of Portolan
  already use.
- **Bump the URL only, leave ArcGIS on `raster:bands`:** Rejected. That would
  declare v2.0.0 while emitting a field v2.0.0 removed, keeping the very
  inconsistency this ADR resolves.

## Notes
- `raster:spatial_resolution` is currently written on `item.properties`, whereas
  in both extension versions it is a band-object field. That placement is a
  pre-existing question independent of this version bump and is out of scope
  here.
