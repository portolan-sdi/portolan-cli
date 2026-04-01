# Implementation Plan: ArcGIS ImageServer Support

**Issue:** [portolan-sdi/portolan-cli#5](https://github.com/portolan-sdi/portolan-cli/issues/5)
**Milestone:** v0.7.0
**Status:** Design complete, ready for implementation
**Last updated:** 2026-04-01

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
| Mosaic vs tiles | **Individual tiles as items** | Per ADR-0031: raster → item-level assets; enables bbox-based discovery |
| Resume support | **Adapt existing `resume.py`** | Track by tile coordinates `(x, y)` instead of layer ID |
| Rate limiting | **Reuse existing `retry.py`** | Exponential backoff (1s→2s→4s), max 60s delay, 3 attempts default |

### Output Structure (per ADR-0031)

Raster data uses **item-level assets** — each tile becomes a STAC item:

```
output/
├── .portolan/
│   └── extraction-report.json    # Tracks tile status for resume
├── catalog.json                  # Auto-init after extraction
└── {service-name}/               # Collection (one per ImageServer)
    ├── collection.json           # Service metadata, extent, CRS
    ├── tile_0_0/
    │   ├── item.json             # STAC item with tile bbox
    │   └── tile_0_0.tif          # COG asset
    ├── tile_0_1/
    │   ├── item.json
    │   └── tile_0_1.tif
    └── ...
```

### Resume Implementation

Adapt `ResumeState` to track tile coordinates:

```python
@dataclass
class ImageServerResumeState:
    succeeded_tiles: set[tuple[int, int]]  # (x, y) coordinates
    failed_tiles: set[tuple[int, int]]

def should_process_tile(x: int, y: int, state: ImageServerResumeState | None) -> bool:
    if state is None:
        return True
    if (x, y) in state.succeeded_tiles:
        return False
    return True  # Failed or new → process
```

### Retry Configuration

Reuse existing `RetryConfig` from `retry.py`:

```python
config = RetryConfig(
    max_attempts=3,      # CLI: --retries
    initial_delay=1.0,
    backoff_factor=2.0,
    max_delay=60.0,
)
```

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

**Strategy: Standard tile grid** (Option B) — more predictable, easier to resume.

1. Query service for `fullExtent` and `pixelSizeX/Y`
2. Compute grid: `tile_size` pixels × `pixelSizeX` = tile width in map units
3. Generate tile coordinates: `(x, y)` pairs covering extent
4. For each tile:
   - Check resume state → skip if succeeded
   - Call `exportImage` with tile bbox
   - Convert to COG via `rio_cogeo.cog_translate()`
   - Create STAC item with tile bbox
   - Update extraction report

**Default tile size:** 4096×4096 pixels (configurable via `--tile-size`)

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
| `portolan_cli/extract/arcgis/imageserver/tiling.py` | Tile grid calculation |
| `portolan_cli/extract/arcgis/imageserver/resume.py` | Tile-based resume state |
| `tests/unit/extract/arcgis/imageserver/` | Unit tests |
| `tests/integration/extract/arcgis/test_imageserver.py` | Integration tests |
| `tests/network/extract/arcgis/test_imageserver_live.py` | Live network tests (nightly) |
| `tests/fixtures/imageserver/charlotte_las_metadata.json` | Mock service metadata |
| `tests/fixtures/imageserver/README.md` | Fixture documentation |

---

## Verification Checklist

- [ ] Unit tests pass: `uv run pytest tests/unit/extract/arcgis/imageserver/ -v`
- [ ] Integration tests pass: `uv run pytest tests/integration/extract/arcgis/test_imageserver.py -v`
- [ ] Network test (Charlotte LAS): `uv run pytest tests/network/extract/arcgis/test_imageserver_live.py -v`
- [ ] Catalog valid: `portolan check` passes on output
- [ ] STAC valid: `stac-validator` on generated items
- [ ] COG valid: `rio cogeo validate` on output files
- [ ] Resume works: Interrupt extraction, resume with `--resume`, verify no duplicate downloads
- [ ] Dry run works: `--dry-run` shows tile count without downloading

---

## Test Fixtures

### Public ImageServers for Testing

| Service | Size | Bands | Resolution | Capabilities | Use Case |
|---------|------|-------|------------|--------------|----------|
| **Charlotte LAS** | ~9 MB | 1 (F32) | 10m | Image ✓ | Unit/integration tests |
| **Toronto** | ~1.2 GB | 4 (U16) | 1m | Image ✓ | Larger integration tests |
| **Ogunquit 2022** | ~18 GB | 4 (U8) | 7.5cm | Image ✓ | Real-world (bbox subset) |

**URLs:**
```
# Charlotte LAS (ESRI Sample) - RECOMMENDED for CI
https://sampleserver6.arcgisonline.com/arcgis/rest/services/CharlotteLAS/ImageServer

# Toronto (ESRI Sample) - 4-band imagery
https://sampleserver6.arcgisonline.com/arcgis/rest/services/Toronto/ImageServer

# Ogunquit 2022 (Maine) - high-res aerial (use small bbox only!)
https://gis.maine.gov/image/rest/services/Municipal/orthoMunicipalOgunquit2022/ImageServer
```

### Test Strategy

1. **Unit tests** (`@pytest.mark.unit`):
   - Mock HTTP responses based on Charlotte LAS metadata
   - Test tile grid calculation, resume logic, STAC generation
   - No network calls

2. **Integration tests** (`@pytest.mark.integration`):
   - Use `responses` or `respx` to mock `exportImage` calls
   - Test full extraction pipeline with mocked responses
   - Verify COG output, STAC validity

3. **Network tests** (`@pytest.mark.network`):
   - Extract from Charlotte LAS (9 MB total, fast)
   - Run in CI nightly, not on every PR
   - Validate against `rio cogeo validate` and `stac-validator`

4. **Real-world subset** (`@pytest.mark.realdata`):
   - Extract small bbox from Ogunquit (~100m × 100m ≈ 1.8 MB)
   - Tests high-resolution imagery handling
   - Optional, for manual validation

### Sample Mock Response

```python
# tests/fixtures/imageserver/charlotte_las_metadata.json
{
    "name": "CharlotteLAS",
    "bandCount": 1,
    "pixelType": "F32",
    "pixelSizeX": 10,
    "pixelSizeY": 10,
    "fullExtent": {
        "xmin": 1420000, "ymin": 460000,
        "xmax": 1435000, "ymax": 475000,
        "spatialReference": {"wkid": 102719}
    },
    "maxImageWidth": 15000,
    "maxImageHeight": 4100,
    "capabilities": "Image,Metadata,Catalog,Mensuration"
}
```

---

## Open Questions (Resolved)

| Question | Resolution |
|----------|------------|
| ~~Mosaic vs. individual tiles~~ | **Individual tiles as items** — per ADR-0031 |
| ~~Resume support~~ | **Adapt `resume.py`** — track `(x, y)` coordinates |
| ~~Rate limiting~~ | **Reuse `retry.py`** — exponential backoff, configurable |
