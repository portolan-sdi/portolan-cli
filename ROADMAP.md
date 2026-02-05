# Portolan Ecosystem Roadmap

## Vision

Portolan makes it easy to publish and consume cloud-native geospatial data. The ecosystem includes a spec, CLI, format plugins, a QGIS plugin, and a global data bootstrapper—each designed to work standalone or together.

Development is **spec-driven but implementation-informed**: the [Portolan Spec](https://github.com/portolan-sdi/portolan-spec) evolves alongside the CLI.

---

## Phase 1: Core CLI + Spec

The foundation. A complete, working CLI with Python API underneath.

### Epic: Dataset Lifecycle

Convert files to cloud-native formats, manage metadata, organize into a local catalog. Vector conversion uses [geoparquet-io](https://github.com/geoparquet/geoparquet-io).

| Capability | Description |
|------------|-------------|
| `portolan init` | Create `.portolan/` catalog structure |
| `portolan dataset add` | Detect format → convert (GeoParquet/COG) → extract metadata → stage |
| `portolan dataset remove` | Remove datasets from catalog |
| `portolan dataset list/info` | Catalog exploration |
| Interactive + `--auto` | Works for humans and agents |

### Epic: Cloud Sync

Push catalogs to object storage. Portolan owns the bucket contents.

| Capability | Description |
|------------|-------------|
| `portolan remote add/list` | Configure S3, GCS, Azure backends |
| `portolan sync` | Push `.portolan/` to remote |
| `versions.json` | Version history, checksums, sync state |

### Epic: Validation & Repair

Ensure catalogs meet the Portolan spec. Detect and fix drift.

| Capability | Description |
|------------|-------------|
| `portolan check` | Validate local catalog against spec |
| `portolan check --remote` | Detect drift (external edits to bucket) |
| `portolan repair` | Re-sync remote from local truth |
| `portolan prune` | Clean up old versions |
| Actionable output | Specific guidance, not just pass/fail |

### Epic: Styling & Thumbnails

Make datasets visually browsable.

| Capability | Description |
|------------|-------------|
| `style.json` | MapLibre-compatible style definitions |
| Thumbnail generation | Auto-render preview images |
| Smart defaults | Infer styles from data characteristics |

### Epic: PMTiles Generation

Generate vector tile overviews from GeoParquet datasets using [gpio-pmtiles](https://github.com/geoparquet-io/gpio-pmtiles).

| Capability | Description |
|------------|-------------|
| PMTiles as derivative | Generated from GeoParquet for web display |
| Automatic on `dataset add` | Optional; controlled by flag or config |
| Stored alongside source | Part of the dataset, not a separate dataset |

**Note:** PMTiles are a *view* of the data for rendering, not the source of truth. GeoParquet remains the canonical format. (PMTiles *could* be added as standalone datasets, but the primary use case is as overviews.)

### Epic: COPC Support

Cloud-optimized point clouds for LiDAR and similar data.

| Capability | Description |
|------------|-------------|
| COPC conversion | Convert point cloud formats to COPC |
| Metadata extraction | Bounds, point count, CRS |
| Styling conventions | Point cloud visualization defaults |

### Epic: Python API

All functionality is implemented as a Python library; CLI wraps it.

| Capability | Description |
|------------|-------------|
| `Catalog` class | `init()`, `add()`, `sync()`, `check()` |
| Built simultaneously | API *is* the implementation; CLI is the interface |
| Agent-friendly | Clear errors, predictable outputs |
| `SKILLS.md` | LLM-optimized documentation |

### Spec Evolution (Phase 1)

The [Portolan Spec](https://github.com/portolan-sdi/portolan-spec) develops in lockstep:

- Required metadata fields
- Catalog structure and naming
- Validation rules
- Remote structure and versioning
- PMTiles and COPC conventions

---

## Parallel: Iceberg Plugin

Tabular analytics on geospatial data. Developed by Javier alongside Phase 1.

| Capability | Description |
|------------|-------------|
| `portolan-iceberg` | Apache Iceberg tables alongside STAC |
| Query integration | SQL/DataFrame access to versioned data |

**Note:** Separate package, separate maintainer, but expected to land around the same time as Phase 1. STAC remains the catalog layer; Iceberg is the analytics layer.

---

## Phase 2: QGIS Plugin

Bring Portolan catalogs into desktop GIS workflows.

| Capability | Description |
|------------|-------------|
| Browse catalogs | Connect to Portolan remotes, explore datasets |
| Pull data | Load GeoParquet/COG into QGIS layers |
| Edit metadata | Update titles, descriptions, licenses |
| Spec validation | Check datasets from within QGIS |

**Dependency:** Phase 1 complete.

---

## Phase 3: Global Data Bootstrapper

Subset global datasets to bootstrap local catalogs.

| Capability | Description |
|------------|-------------|
| Source registry | Curated global datasets (Overture, ESA, etc.) |
| Region extraction | Clip to bounding box or admin boundary |
| One-command bootstrap | `portolan bootstrap --region "Nairobi"` |

**Dependency:** Phase 1 complete.

---

## Out of Scope for v1.0

| Item | Reason |
|------|--------|
| 3D Tiles | Niche; can be community-contributed later |
| Browser/Map UI | May be unnecessary with agentic workflows; revisit post-v1 |
| Multi-user collaboration | Different problem; Portolan owns the bucket |

---

## Summary

| Phase | Scope | Timing |
|-------|-------|--------|
| **Phase 1** | Core CLI, Python API, Spec, PMTiles, COPC | Now |
| **Parallel** | Iceberg Plugin (Javier) | Alongside Phase 1 |
| **Phase 2** | QGIS Plugin | After Phase 1 |
| **Phase 3** | Global Bootstrapper | After Phase 1 |

---

*Portolan is an open source project under [Radiant Earth](https://radiant.earth).*
