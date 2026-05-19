# ADR-0043: Style and Thumbnail Architecture

## Status
Accepted (style storage section superseded by ADR-0045; thumbnail and basemap decisions remain active)

## Context

Issue #13 required defining how to store styling information for geospatial assets and how to generate preview thumbnails. Key questions:

1. **Style storage:** Separate `style.json` files vs inline in STAC asset properties?
2. **Style format:** Custom Portolan format vs Mapbox GL spec?
3. **Thumbnail generation:** When and how to generate thumbnails for vector vs raster data?
4. **Basemaps:** Should thumbnails include basemap context?
5. **Coordinate systems:** PMTiles tiles use tile-space coordinates (0-4096), not geographic.

## Decision

### Style Storage: Inline in STAC Assets

Store styles directly in STAC asset properties rather than separate files:

**Vector (PMTiles):**
```json
{
  "href": "./data.pmtiles",
  "type": "application/vnd.pmtiles",
  "pmtiles:style": {
    "version": 8,
    "layers": [{ "id": "default", "type": "fill", ... }]
  }
}
```

**Raster (COG):**
```json
{
  "href": "./data.tif",
  "type": "image/tiff; application=geotiff; profile=cloud-optimized",
  "render:colormap_name": "viridis",
  "render:rescale": [[0, 255]]
}
```

**Rationale:**
- Single source of truth (no sync issues between style.json and STAC)
- STAC render extension already defines raster styling this way
- `pmtiles:style` mirrors this pattern for vectors (proposed upstream to web-map-links extension)
- Styles are small enough to inline without bloating STAC JSON

### Style Format: Mapbox GL Spec v8 Subset

Use standard Mapbox GL style spec for vector styling rather than inventing a custom format.

**Rationale:**
- Industry standard, supported by MapLibre GL, Mapbox GL, deck.gl
- Well-documented, validated by tooling
- No translation layer needed for rendering

### Basemaps: Vector Only

Add basemap support for vector thumbnails but NOT for raster thumbnails.

**Vector thumbnails need basemaps:**
- Points, lines, and sparse polygons lack geographic context
- Without a basemap, users see shapes on white background
- Basemap shows "where" the data is located

**Raster thumbnails don't need basemaps:**
- Raster data fills the entire extent — it IS the visual content
- A basemap underneath would be completely hidden by the raster
- Exception: rasters with large nodata areas, but these are rare

**Implementation:**
- Vector: contextily + CartoDB.Positron default, configurable
- Raster: basemap_provider parameter exists but is intentionally ignored

### Coordinate Transformation for PMTiles

MVT (Mapbox Vector Tile) coordinates are in tile-extent space (typically 0-4096), not geographic coordinates. Thumbnails must transform these to lat/lon for:
1. Correct aspect ratio in rendered output
2. Basemap alignment (basemap tiles are fetched by geographic bounds)

**Implementation:**
- `_tile_bounds(z, x, y)` calculates geographic bounds from tile coordinates
- `_transform_coords()` recursively transforms coordinate arrays
- Bounds returned alongside geometries for basemap fetching

### Thumbnail Generation Timing

Generate thumbnails during conversion/PMTiles generation, NOT during scan.

**Rationale:**
- `scan` is read-only per ADR-0016 (scan-before-import pattern)
- Thumbnails are derived artifacts that belong with conversion
- Conversion already generates COG thumbnails; vector thumbnails follow same pattern

**Workflow:**
1. `convert_file()` → GeoParquet → thumbnail from GeoParquet
2. `generate_pmtiles_for_collection()` → PMTiles → thumbnail from PMTiles (preferred)

PMTiles thumbnails are preferred because PMTiles already has simplified/generalized geometry appropriate for overview rendering.

## Consequences

**Easier:**
- Consumers get styling info directly from STAC without fetching additional files
- Standard Mapbox GL styles work with existing map rendering libraries
- Vector thumbnails have useful geographic context via basemaps

**Harder:**
- Inline styles increase STAC JSON size slightly
- `pmtiles:style` is non-standard until upstreamed to web-map-links extension
- PMTiles coordinate transformation adds complexity to thumbnail code

## Alternatives Considered

### Separate style.json files
Rejected: Creates sync issues, requires additional file management, no clear benefit.

### Custom Portolan style format
Rejected: Reinventing the wheel. Mapbox GL spec is mature and widely supported.

### Basemaps for raster thumbnails
Rejected: Rasters fill their extent — basemap would be invisible. Adds complexity (CRS reprojection, alpha compositing) for no visual benefit.

### Thumbnail generation during scan
Rejected: Violates ADR-0016. Scan is read-only analysis; thumbnails are derived artifacts.
