# Implementation Plan: ArcGIS ImageServer Support

**Issue:** [portolan-sdi/portolan-cli#5](https://github.com/portolan-sdi/portolan-cli/issues/5)
**Milestone:** v0.7.0
**Status:** Research complete, ready for implementation

---

## Sources

| Source | URL | Purpose |
|--------|-----|---------|
| GitHub Issue #5 | https://github.com/portolan-sdi/portolan-cli/issues/5 | Feature request from @cholmes |
| geoparquet-io PR #162 | https://github.com/geoparquet/geoparquet-io/pull/162 | Javier's ImageServer/Raquet implementation (rejected for scope) |
| geoparquet-io arcgis.py | https://github.com/geoparquet/geoparquet-io/blob/main/geoparquet_io/core/arcgis.py | Existing auth system to reuse |
| PHL aerial imagery scripts | `../portolan-test-data/phl-aerial-imagery/scripts/` | Manual COG pipeline reference |
| ADR-0003 | `context/shared/adr/0003-plugin-architecture.md` | Plugin architecture guidance |
| ADR-0010 | `context/shared/adr/0010-delegate-conversion-validation.md` | Delegate to upstream libraries |
| ADR-0019 | `context/shared/adr/0019-cog-optimization-defaults.md` | COG defaults (DEFLATE, 512x512) |
| ADR-0033 | `context/shared/adr/0033-esri-gdb-raster-gdal-requirement.md` | ESRI raster GDAL requirements |

---

## Research Summary

### Current State: Portolan-cli ArcGIS Support

**Existing vector extraction** (production-ready):
- Complete FeatureServer/MapServer extraction in `portolan_cli/extract/arcgis/`
- Modules: `orchestrator.py`, `discovery.py`, `filters.py`, `metadata.py`, `url_parser.py`, `report.py`, `resume.py`, `retry.py`
- User guide: `docs/guides/extract-arcgis.md`

**Key finding:** rio-cogeo does NOT have an ESRI extractor. It only handles local GeoTIFF → COG conversion. ImageServer extraction must be built fresh.

### Reference: geoparquet-io PR #162

Javier's PR added two major components:
1. `arcgis.py` (961 lines) - ArcGIS Feature Service integration with auth
2. `raquet.py` (1668 lines) - GeoTIFF → QUADBIN-indexed Parquet

**Why rejected for geoparquet-io:**
- Scope creep: adds service-specific auth, CLI commands, orchestration
- geoparquet-io is format-focused; ImageServer is cloud-service-specific
- Heavy deps: rasterio, mercantile, quadbin, httpx

**Patterns to reuse:**
- `ArcGISAuth` dataclass (token, token_file, username/password, portal_url)
- `generate_token()` - 60-min tokens from ArcGIS Online or enterprise
- `resolve_token()` - Priority cascade for auth sources
- HTTP client with connection pooling, retries, exponential backoff
- Two-pass streaming for memory efficiency
- Error classification (401/403, 4xx, 429)

### Reference: geoparquet-io arcgis.py (main branch)

**Source:** https://github.com/geoparquet/geoparquet-io/blob/main/geoparquet_io/core/arcgis.py

**Critical finding:** geoparquet-io explicitly rejects ImageServer URLs with error:
> "ImageServer (raster) services are not supported"

This confirms ImageServer support belongs in portolan-cli, not geoparquet-io.

### Reference: PHL Aerial Imagery Pipeline

**Location:** `../portolan-test-data/phl-aerial-imagery/scripts/`

**Note:** This used PASDA directory listings (IIS-style HTML), NOT ArcGIS ImageServer REST API. Different architecture, but useful for:
- COG conversion settings: GDAL with JPEG Q95, 512×512 tiles
- Compression ratios: 5.7x average (250GB raw → 53GB COG)
- Gotchas: PySTAC `normalize_hrefs` breaks expected structure

---

## Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Output format | **COG only** | Simpler, broader ecosystem support; Raquet later |
| Auth scope | **Reuse geoparquet-io patterns** | Already battle-tested in `geoparquet_io/core/arcgis.py` |
| CLI structure | **Extend `portolan extract arcgis`** | Auto-detect ImageServer vs FeatureServer URLs |

---

## Implementation Plan

### Phase 1: Module Structure

```
portolan_cli/extract/arcgis/
├── __init__.py              # Update exports
├── orchestrator.py          # Modify to detect ImageServer vs FeatureServer
├── imageserver/             # NEW
│   ├── __init__.py
│   ├── discovery.py         # Query /ImageServer?f=json for service metadata
│   ├── extractor.py         # Tile iteration + exportImage calls
│   └── metadata.py          # Raster metadata → STAC
```

### Phase 2: URL Detection

Modify `orchestrator.py` to route based on URL:
```python
if "/ImageServer" in url:
    return extract_imageserver(url, output_dir, **kwargs)
else:
    return extract_featureserver(url, output_dir, **kwargs)
```

### Phase 3: Service Discovery

Query ImageServer REST API (`imageserver/discovery.py`):
```
GET {base_url}/ImageServer?f=json
```

Extract:
- `extent` (full service bbox)
- `spatialReference` (WKID → EPSG)
- `bandCount`, `pixelType`, `pixelSizeX/Y`
- `maxImageHeight`, `maxImageWidth` (server limits)

### Phase 4: Tile Iteration

Export tiles via (`imageserver/extractor.py`):
```
GET {base_url}/ImageServer/exportImage
  ?bbox={minx},{miny},{maxx},{maxy}
  &size={width},{height}
  &format=tiff
  &f=image
```

Strategy options:
- **Option A:** Use server `maxImageWidth/Height` to subdivide extent
- **Option B:** Standard tile grid (4096×4096 chunks) - more predictable, easier to resume

### Phase 5: COG Conversion

Reuse existing infrastructure:
- Downloaded tiles → temporary GeoTIFF
- `rio_cogeo.cog_translate()` → final COG
- Follows ADR-0019 defaults (DEFLATE, 512×512 tiles)

### Phase 6: CLI Integration

New options for ImageServer:
- `--tile-size` (default: 4096)
- `--compression` (default: DEFLATE, option: JPEG for RGB imagery)

---

## Files to Modify

| File | Change |
|------|--------|
| `portolan_cli/extract/arcgis/__init__.py` | Export new ImageServer module |
| `portolan_cli/extract/arcgis/orchestrator.py` | Add URL type detection, route to ImageServer |
| `portolan_cli/extract/arcgis/url_parser.py` | Add ImageServer URL validation |
| `docs/guides/extract-arcgis.md` | Document ImageServer support |

## Files to Create

| File | Purpose |
|------|---------|
| `portolan_cli/extract/arcgis/imageserver/__init__.py` | Module exports |
| `portolan_cli/extract/arcgis/imageserver/discovery.py` | Service metadata query |
| `portolan_cli/extract/arcgis/imageserver/extractor.py` | Tile iteration + download |
| `portolan_cli/extract/arcgis/imageserver/metadata.py` | STAC metadata generation |
| `tests/unit/extract/arcgis/imageserver/` | Unit tests |
| `tests/integration/extract/arcgis/test_imageserver.py` | Integration tests |

---

## Verification Checklist

- [ ] Unit tests pass: `uv run pytest tests/unit/extract/arcgis/imageserver/ -v`
- [ ] Integration test: Extract from public ImageServer → COG
- [ ] Catalog valid: `portolan check` passes on output
- [ ] STAC valid: `stac-validator` on generated items
- [ ] COG valid: `rio cogeo validate` on output files

---

## Open Questions

1. **Mosaic vs. individual tiles:** Should output be one merged COG or multiple tile COGs?
2. **Resume support:** How to handle interrupted downloads (checkpointing)?
3. **Rate limiting:** Does the existing retry logic from geoparquet-io suffice?
