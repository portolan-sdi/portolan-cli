# Issue #13: Style and Thumbnail Implementation Plan

**Issue:** https://github.com/portolan-sdi/portolan-cli/issues/13
**Date:** 2026-05-06
**Status:** Ready for implementation

## Summary

Implement auto-generated thumbnails with configurable basemaps and inline style storage for both vector (PMTiles) and raster (COG) assets.

## Research Findings

### Existing STAC Extensions

| Extension | Covers | Gap |
|-----------|--------|-----|
| **render** | Raster styling (`rescale`, `colormap`, `expression`) | Vector not supported |
| **web-map-links** | PMTiles links with `pmtiles:layers` | No `pmtiles:style` field |
| **vector** | Geometry types, scale metadata | No styling |

**Conclusion:** No vector styling extension exists. We'll use asset properties and propose upstream contribution to web-map-links.

### Thumbnail Generation Approaches (Validated via Spike)

| Source | Method | Status |
|--------|--------|--------|
| **PMTiles** | `pmtiles` + `mapbox-vector-tile` + matplotlib | Spike validated |
| **GeoParquet** | geopandas + matplotlib (fallback) | Standard approach |
| **COG** | rasterio overviews | Already implemented (#372) |

### Basemap Support

`contextily` + `xyzservices` provides:
- `CartoDB.Positron` (light, default)
- `CartoDB.DarkMatter` (dark)
- `CartoDB.Voyager` (colored)
- OSM, Stamen, and 100+ other providers

Pure Python, pip-installable.

---

## Architecture

### Workflow Integration

```
portolan add / scan
    │
    ├─► Raster detected
    │       │
    │       ├─► convert to COG
    │       ├─► generate_cog_thumbnail() [exists]
    │       └─► write render extension props to COG asset
    │
    └─► Vector detected
            │
            ├─► convert to GeoParquet
            ├─► generate PMTiles (if enabled)
            ├─► generate_vector_thumbnail() [NEW]
            │       └─► prefer PMTiles, fallback to GeoParquet
            └─► write pmtiles:style to PMTiles asset
```

### Style Storage

**Vector (PMTiles asset):**
```json
{
  "href": "./data.pmtiles",
  "type": "application/vnd.pmtiles",
  "roles": ["data"],
  "pmtiles:style": {
    "version": 8,
    "layers": [{
      "id": "default",
      "type": "fill",
      "source-layer": "data",
      "paint": {
        "fill-color": "#3388ff",
        "fill-opacity": 0.6,
        "fill-outline-color": "#2266cc"
      }
    }]
  }
}
```

**Raster (COG asset):**
```json
{
  "href": "./data.tif",
  "type": "image/tiff; application=geotiff; profile=cloud-optimized",
  "roles": ["data"],
  "render:rescale": [[0, 255]],
  "render:colormap_name": "viridis"
}
```

### Configuration Schema

Add to `config.yaml`:

```yaml
thumbnails:
  enabled: true
  max_size: 512
  quality: 75
  basemap:
    provider: CartoDB.Positron  # CartoDB.DarkMatter, CartoDB.Voyager, none
    opacity: 1.0
    zoom_adjust: 0  # +/- zoom level adjustment

styles:
  vector:
    point:
      circle-radius: 4
      circle-color: "#3388ff"
      circle-opacity: 0.8
    line:
      line-color: "#3388ff"
      line-width: 2
      line-opacity: 0.8
    polygon:
      fill-color: "#3388ff"
      fill-opacity: 0.6
      fill-outline-color: "#2266cc"
  raster:
    colormap: viridis
    rescale: auto  # or [min, max]
```

---

## Implementation Tasks

### Phase 1: Vector Thumbnail Generation

**Files to create/modify:**

1. **`portolan_cli/thumbnail.py`** (NEW)
   - `generate_vector_thumbnail(source: Path, output: Path, config: ThumbnailConfig) -> Path | None`
   - `generate_thumbnail_from_pmtiles(pmtiles_path: Path, ...) -> Path | None`
   - `generate_thumbnail_from_geoparquet(gpq_path: Path, ...) -> Path | None`
   - `add_basemap(ax: Axes, bounds: tuple, provider: str) -> None`

2. **`portolan_cli/conversion_config.py`**
   - Add `ThumbnailConfig` dataclass
   - Add `VectorStyleConfig` dataclass
   - Wire into existing config loading

3. **`portolan_cli/convert.py`**
   - Call `generate_vector_thumbnail()` after vector conversion
   - Match existing COG thumbnail pattern

4. **`portolan_cli/scan.py`**
   - Generate thumbnails for existing PMTiles/GeoParquet during scan

### Phase 2: Style Storage

**Files to modify:**

1. **`portolan_cli/dataset.py`**
   - `_build_pmtiles_style(geometry_type: str, config: VectorStyleConfig) -> dict`
   - `_build_raster_style(cog_path: Path, config: RasterStyleConfig) -> dict`
   - Add style props when building STAC assets

2. **`portolan_cli/models/item.py`**
   - Ensure asset properties support arbitrary fields (already should)

### Phase 3: Test Fixtures

**Files to create:**

1. **`tests/fixtures/metadata/style/valid/`**
   - `style_point.json` — Circle layer for points
   - `style_polygon.json` — Fill layer for polygons
   - `style_line.json` — Line layer
   - `style_categorical.json` — Data-driven color by category
   - `style_graduated.json` — Graduated color ramp

2. **`tests/fixtures/metadata/style/invalid/`**
   - `style_bad_syntax.json` — Invalid JSON
   - `style_missing_layers.json` — Missing required field

3. **`tests/fixtures/README.md`** — Update with style fixture docs

### Phase 4: Tests

1. **`tests/unit/test_thumbnail.py`**
   - Test PMTiles → thumbnail
   - Test GeoParquet → thumbnail
   - Test basemap integration
   - Test config variations

2. **`tests/unit/test_style.py`**
   - Test style generation for each geometry type
   - Test style config parsing
   - Test render extension props for raster

3. **`tests/integration/test_thumbnail_workflow.py`**
   - End-to-end: add vector → PMTiles + thumbnail generated
   - End-to-end: add raster → COG + thumbnail generated
   - Verify STAC assets have correct style properties

---

## Dependencies

Add to `pyproject.toml`:

```toml
[project.dependencies]
# Existing deps...
contextily = ">=1.5.0"
xyzservices = ">=2024.1.0"
pmtiles = ">=3.2.0"
mapbox-vector-tile = ">=2.0.0"
# matplotlib already a dep via geopandas
```

---

## Migration / Breaking Changes

None. This is additive functionality.

- Existing catalogs without thumbnails continue to work
- Thumbnails are opt-out via `thumbnails.enabled: false`
- Styles are auto-generated but can be overridden

---

## STAC Extension Contribution (Parallel Track)

Propose `pmtiles:style` field to [stac-extensions/web-map-links](https://github.com/stac-extensions/web-map-links):

```json
{
  "rel": "pmtiles",
  "href": "./data.pmtiles",
  "type": "application/vnd.pmtiles",
  "pmtiles:layers": ["default"],
  "pmtiles:style": {
    "version": 8,
    "layers": [...]
  }
}
```

This is non-blocking — we use asset properties now, migrate to extension when accepted.

---

## Acceptance Criteria

- [x] Vector thumbnails auto-generated with configurable basemap
- [x] Style properties written to PMTiles assets
- [x] Render extension properties written to COG assets
- [x] Config schema supports thumbnail and style customization
- [x] All test fixtures created
- [x] Unit and integration tests passing
- [ ] Documentation updated

**Dropped:** COG thumbnails with basemap. Rasters fill their entire extent — basemaps would be invisible underneath. Vector data needs basemaps because points/lines are sparse and benefit from geographic context. See ADR-0042.

---

## Open Questions (Resolved)

| Question | Resolution |
|----------|------------|
| Separate style.json or inline? | **Inline** in asset properties |
| Render extension for vectors? | **No** — it's raster-only, use `pmtiles:style` |
| Basemaps for vectors? | **Yes** — Carto Positron default, configurable |
| Basemaps for rasters? | **No** — rasters fill extent, basemap would be hidden (ADR-0042) |
| When generate? | **During convert/PMTiles generation** — not scan (scan is read-only per ADR-0016) |

---

## References

- [STAC render extension](https://github.com/stac-extensions/render)
- [STAC web-map-links extension](https://github.com/stac-extensions/web-map-links)
- [contextily docs](https://contextily.readthedocs.io/)
- [PMTiles Python library](https://pypi.org/project/pmtiles/)
- [mapbox-vector-tile](https://pypi.org/project/mapbox-vector-tile/)
- Spike script: `spike_thumbnail.py` (validated approach)
