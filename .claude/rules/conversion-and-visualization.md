---
paths:
  - "portolan_cli/convert.py"
  - "portolan_cli/conversion_config.py"
  - "portolan_cli/formats.py"
  - "portolan_cli/crs.py"
  - "portolan_cli/pmtiles.py"
  - "portolan_cli/partitioning.py"
  - "portolan_cli/thumbnail.py"
  - "portolan_cli/style.py"
  - "portolan_cli/metadata/cog.py"
  - "portolan_cli/metadata/geoparquet.py"
  - "portolan_cli/metadata/pmtiles.py"
  - "portolan_cli/metadata/flatgeobuf.py"
---

# Conversion, CRS, PMTiles, thumbnails, styles

This subsystem turns source files into cloud-native formats and renders
visualizations. Two themes cause almost every bug here: **non-3857 / non-4326
data breaks downstream tools when reprojection is skipped or done wrong**, and
**a fix lands in one render path but not its twin**. We orchestrate upstream
libraries (`geoparquet-io`, `rio-cogeo`, `contextily`, `tippecanoe`), we do not
reimplement geometry or raster math (ADR-0010).

## Delegate, never reimplement, and guard the known crashes

- Conversion goes through `geoparquet-io` (vector) and `rio-cogeo` (raster).
- `geoparquet-io` crashes on edge inputs: multilayer conversion **aborts on
  macOS**, malformed input **segfaults on Windows**. The tests that hit those
  paths are `skipif`-guarded by platform, do not remove the guards. See
  `context/shared/known-issues/geoparquet-io-macos-abort.md` and
  `geoparquet-io-windows-segfault.md`.
- PyArrow is pinned `<22.0.0` (abseil ABI break on Ubuntu 22.04). Do not bump it
  without reading `context/shared/known-issues/pyarrow-abseil-abi.md`.
- Some "bugs" here are upstream `geoparquet-io` issues (FileGDB CRS, projected-CRS
  metadata). When you suspect that, pin/verify the upstream version and document
  in `context/shared/known-issues/`, do not patch around it in our layer.

## Skip conversion for ALL cloud-native formats, not just .parquet

The public entry point is `convert_file()` in `convert.py`, which calls
`get_cloud_native_status()` and SKIPS anything already cloud-native. Gate on the
`CLOUD_NATIVE_EXTENSIONS` set in `formats.py`, never a hardcoded `.parquet`
check, an earlier `.parquet`-only gate let `.pmtiles` and `.fgb` fall through to
`gpio.convert()` and fail with "No CRS found". Note the legacy `convert_vector()`
helper in `dataset.py` still has a literal `.parquet` skip, do not copy that
pattern, route new format checks through `get_cloud_native_status()`. Multi-layer
`.gpkg`/`.gdb` must convert **all** layers or warn explicitly, never silently
drop layers (that is data loss).

## CRS: reproject explicitly, and the mismatch heuristic is projected-only

- The ArcGIS-style "coords look like lat/lon but the declared CRS is not WGS84"
  mismatch heuristic applies **only to projected CRSes**. Skip it entirely when
  `parsed_crs.is_geographic`, otherwise it false-positives on legitimate ETRS89
  (EPSG:4258) and NAD83 (EPSG:4269) data and blocks every EU INSPIRE dataset.
- Re-derive `proj:epsg` from the actual file on **every** add, it goes stale
  after an external reprojection.
- A CRS change is a **breaking** version bump (major), per `spec/versions.md`.

## Thumbnails: plot data first, basemap LAST, and fix BOTH render paths

This is the most-reverted logic in the repo. `contextily` derives its tile zoom
from the **current axes extent**, so order matters and there are two separate
render paths.

- Order is strict: plot the geometry, then `ax.set_xlim/set_ylim` from the data
  bounds, then `add_basemap` **last**. Adding the basemap first makes contextily
  read an empty/default extent and pick an absurd zoom (blank thumbnail).
- Compute the extent from **actual geometry bounds**, not tile bounds. PMTiles
  tile bounds at z=0 are the whole world. Read the GeoParquet bbox from file
  metadata (O(1)), for very large files sample features, never scan all rows.
- Let `contextily` reproject the few basemap tiles via its `crs=` param. Do not
  `.to_crs()` millions of geometry vertices for alignment. PMTiles coordinates
  are always EPSG:4326.
- **There are two render paths** (GeoParquet and PMTiles). The basemap-ordering
  fix shipped for the GeoParquet path and a later regression fix had to apply it
  to the PMTiles path (commit `c41da07`, #468). When you change rendering or CRS
  logic here, grep for every `add_basemap(` call and apply the change to **both**
  paths, then assert parity. Basemaps are for **vector** thumbnails only, raster
  thumbnails get no basemap (ADR-0043).
- `contextily` is an optional lazy import, guard for its absence.
- **The matplotlib floor is a punchy data-aware preset, NOT the extracted style**
  (#518, Track 1). The WFS/Mapbox style is pale by design (`fill-opacity 0.2`,
  hairline outline) and washes out at 512 px, so do **not** read `fill_opacity`
  / `fill_outline_color` / the style's default fill into the thumbnail. Use
  `THUMB_FILL_COLOR` / `THUMB_EDGE_COLOR` and `_compute_render_params()` (scales
  marker size / stroke / opacity to geometry type + feature count, opacity always
  ≥ 0.5). Only the style's **categorical** color map is reused, via
  `resolve_color_for_properties` / `resolve_colors_for_gdf` with an explicit
  punchy `fallback`. The *real* style is rendered by the opt-in MapLibre-native
  skill (Track 2), never here. Do not re-open fixed-preset tuning or
  vision-in-the-loop (both rejected in #518).
- **Frame the bbox before setting limits** with `_frame_bounds()` (margin +
  aspect-cap). Its real jobs are (1) adding a margin so geometry isn't flush to
  the edge and (2) giving degenerate extents — a single point, or a perfectly
  vertical/horizontal line — a finite box so `set_xlim`/`set_ylim` don't collapse
  and contextily can still derive a zoom. It is O(1) on the bbox — do not reach
  for per-vertex percentile cropping. Note what it does **not** do: under
  `set_aspect('equal')` it cannot de-elongate the *geometry* — a hemisphere-
  spanning sliver still renders as a thin strip (~11px vs ~14px wide at 512px,
  measured), because the longer axis fixes the scale and widening the short
  axis's limits only adds whitespace. If elongated geometry must read wider,
  that's a Track-2 (MapLibre) concern, not something `max_aspect` delivers here.
- **Mixed-geometry layers**: one `RenderParams` is applied to every feature
  (the GeoParquet path issues a single `gdf.plot`), so `_compute_render_params`
  floors the off-axis dimensions (`_MIN_VISIBLE_MARKER` / `_MIN_VISIBLE_STROKE`)
  rather than zeroing them — otherwise points vanish in a polygon-dominant layer
  and lines/edges vanish in a point-dominant one. Keep the floors non-zero.
- Pass `aspect="equal"` to `gdf.plot()`. Without it geopandas derives a
  latitude-corrected aspect and raises "aspect must be finite and positive" when
  a layer declares a geographic CRS but holds projected-magnitude coords (#516
  family) — that would leave the collection with no thumbnail at all.
- The render presets and helpers (`_compute_render_params`, `_frame_bounds`,
  `_geom_category`, `_profile_*`) are **shared by both paths** — that *is* the
  parity mechanism. Change them once, both paths follow.

## PMTiles: thread src_crs through, register at collection level

- PMTiles is required for vector datasets > 100 MB and recommended > 10 MB
  (ADR-0050). Generate alongside the GeoParquet.
- `pmtiles.src_crs` from `.portolan/config.yaml` must be threaded
  `_get_pmtiles_settings()` (in `cli.py`) -> `generate_pmtiles_for_collection()`
  -> `gpio-pmtiles`, which reprojects to WGS84 before tippecanoe. Dropping it
  breaks PMTiles for any projected source.
- A generated `.pmtiles` MUST be a **collection-level** asset AND have a
  `rel: "pmtiles"` link (web-map-links extension). Item-level-only is
  non-conformant (RULE-0060 / RULE-0061, both errors). PMTiles discovery
  (`_find_geoparquet_assets`) only scans collection-level assets.

## Styles are standalone STAC assets (ADR-0045, supersedes 0043)

- A style is a complete **Mapbox GL v8** JSON file in `{collection}/styles/`,
  not inline in the STAC. Asset key `styles/{stem}`, `type: "application/json"`,
  `roles` containing `"style"` (RULE-0066).
- The style JSON MUST have `version == 8`, `sources`, `layers` (RULE-0068).
  `sources.data.url` is a **relative** path to the PMTiles (`../file.pmtiles`),
  `layers[].source` is always `"data"`.
- `portolan:styles` is an ordered array of asset keys (first = default), and
  every entry MUST reference an existing asset (RULE-0067). Vary default colors
  across a catalog so it is not monotone.

## Partitioning: let geoparquet-io name things, detect Hive by pattern

- Detect Hive partitions by the `key=value/` (`*=*`) pattern using the existing
  `is_hive_partition_dir()` regex, **not** a column-name allowlist. Arbitrary
  keys like `gms_feature_id=<uuid>/` are valid and must not leak into
  collection-id inference (strip the `key=value/` segment first).
- `geoparquet-io` names the partition dirs and files (e.g. `kdtree_cell=0000/`,
  `{cell_id}.parquet`). Derive the partition `item_id` from
  `partition_path.parent.name`, do not construct `{stem}_{cell}`. Glob partition
  files with `*.parquet`, never a hardcoded `data.parquet`. After integrating
  gpio output, verify the `versions.json` item paths against the **actual**
  on-disk Hive structure (an integration test reads it back via DuckDB).
- Each partition is a STAC item with its own bbox. The collection adds the
  partition extension URL to `stac_extensions` and sets `partition:scheme` and
  `partition:keys` (ADR-0042). `partitioning.py` is the reference implementation
  named by the `stac-partition-extension` repo, so its output must match that
  schema exactly.

## COG and output-location defaults (ADR-0019, ADR-0020)

COG defaults are DEFLATE, predictor=2, 512x512 tiles, nearest resampling.
Conversion output goes **side-by-side for vectors, in-place for rasters**.
Accept non-cloud-native formats with a warning, do not hard-fail (ADR-0014).
Raster band metadata lands on the data asset, see `.claude/rules/stac-assets.md`.

## Where to investigate further

- ADRs 0010, 0014, 0019, 0020, 0026, 0042, 0043, 0045, 0049, 0050.
- `spec/formats/vector.md`, `spec/formats/raster.md`, `spec/best-practices.md`,
  `spec/schema/rules.yaml` (RULE-0030..0032 classification, 0060..0068 PMTiles
  and styles).
- The three `context/shared/known-issues/` files named above.
- The `stac-partition-extension` repo schema for partition field shapes.
- Tests: `tests/integration/test_gpio_integration.py`,
  `test_add_multilayer_integration.py`, the thumbnail render tests.
