# Portolan CLI Architecture

## Overview

Portolan CLI is the command-line tool for creating and managing Portolan catalogs — collections of cloud-native geospatial data with rich metadata, styling, and versioning. It orchestrates format conversion, STAC catalog generation, metadata enrichment, and cloud sync.

## System Boundaries

Portolan CLI is one component in a broader ecosystem. Each component has a clear responsibility:

| Component | Responsibility | Status |
|---|---|---|
| **[geoparquet-io](https://github.com/geoparquet/geoparquet-io)** | Vector format conversion, inspection, partitioning, sorting | Existing |
| **[gpio-pmtiles](https://github.com/geoparquet-io/gpio-pmtiles)** | PMTiles generation from GeoParquet | Existing |
| **[rio-cogeo](https://github.com/cogeotiff/rio-cogeo)** | Raster conversion to Cloud-Optimized GeoTIFF | Existing |
| **portolan-cli** | Catalog orchestration, metadata, versioning, validation, sync | This project |
| **QGIS plugin** | Browse and pull data from Portolan catalogs into local GIS | Later |
| **Browser/Map UI** | Web-based catalog browsing and visualization | Later |
| **Global Data Bootstrapper** | Subset global datasets to bootstrap local catalogs | Later |

Portolan CLI does not implement format conversion itself. It calls geoparquet-io and rio-cogeo as library dependencies and wires their output into the catalog structure.

**Analogy:** geoparquet-io is to portolan what a compiler is to a build system. Most users interact with portolan; power users who need fine-grained file-level control use geoparquet-io directly.

## Dependencies

### Core (always installed)

- **click** — CLI framework
- **pystac** — STAC catalog generation
- **obstore** — Cloud storage abstraction (S3, GCS, Azure, local)
- **[geoparquet-io](https://github.com/geoparquet/geoparquet-io)** — Vector format conversion and inspection
- **[gpio-pmtiles](https://github.com/geoparquet-io/gpio-pmtiles)** — PMTiles generation from GeoParquet
- **[rio-cogeo](https://github.com/cogeotiff/rio-cogeo)** — Raster conversion to COG

### Plugins (separate packages, entry point registration)

Future format support is added via plugins (PMTiles, COPC, 3D Tiles, Iceberg). See **ADR-0003** for the plugin architecture and **ADR-0004** for why Iceberg is a plugin rather than core.

## CLI Commands

Twelve commands covering the full lifecycle from files to live catalog:

```
portolan init                          # Create .portolan/ catalog structure
portolan dataset add <file_or_dir>     # Convert, enrich, stage (interactive or --auto)
portolan dataset remove <name_or_dir>  # Remove dataset(s) from catalog
portolan dataset list                  # Show catalog contents
portolan dataset info <name>           # Metadata, extent, schema summary

portolan remote add <name> <url>       # Configure storage backend
portolan remote list                   # Show configured remotes
portolan sync                          # Push .portolan/ to remote

portolan check                         # Validate local catalog against spec
portolan check --remote                # Detect drift between local and remote
portolan repair                        # Re-sync remote from local truth
portolan prune                         # Delete old version files from remote
```

### `dataset add` Workflow

This is the primary command. It accepts a single file or a directory and runs an interactive workflow:

1. Detect input format, dispatch to appropriate converter (geoparquet-io or rio-cogeo)
2. Convert to cloud-native format if needed
3. Extract metadata (spatial extent, schema, CRS)
4. Interactive style definition — show columns, suggest smart defaults, user confirms/adjusts → write `style.json`
5. Generate thumbnail from the style definition
6. Generate STAC collection/item with metadata
7. Update `versions.json` with checksums
8. Generate README from metadata
9. Stage everything in `.portolan/`

Flags:
- `--auto` / `--non-interactive` — skip interactive prompts, use smart defaults (also the agentic path)
- `--no-convert` — stage a pre-converted file without running conversion
- `--title`, `--description`, `--license` — metadata overrides

### Directory Handling

`dataset add` and `dataset remove` both accept directories for batch operations. Open question: when adding a directory, the CLI needs to distinguish between a directory of files that form a single dataset (e.g., `radios.parquet` + `census-data.parquet` + `metadata.parquet` as one collection) versus a directory of independent files that should each become separate datasets.

## Catalog Structure

A Portolan catalog on disk (`.portolan/`) contains:

```
.portolan/
├── catalog.json              # STAC root catalog
├── README.md                 # Root README
├── <dataset>/
│   ├── collection.json       # STAC collection
│   ├── versions.json         # Version manifest with checksums
│   ├── style.json            # MapLibre style definition
│   ├── thumbnail.png         # Auto-generated preview
│   ├── README.md             # Dataset README
│   └── <data files>          # Cloud-native formats
```

## Remote Ownership

Portolan owns the bucket contents. Users configure access; Portolan manages everything inside. Manual edits are unsupported and flagged as drift. See **ADR-0006** for the full ownership model.

- `portolan check --remote` — detect drift (files added, deleted, or modified outside Portolan)
- `portolan repair` — re-sync remote from local truth

**Future:** Multi-tenant access control and visibility are planned but not yet scoped. See [Roadmap: Access Control & Visibility](../ROADMAP.md#tbd-access-control--visibility).

## Versioning

`versions.json` is the single source of truth for version history, sync state, and integrity checksums. See **ADR-0005** for the full design.

- Current version files live at dataset root
- Old versions archived to `/v{version}/` paths
- `portolan prune` cleans up old versions (with safety mechanisms)

## Validation

`portolan check` validates the catalog against the Portolan spec with a clear distinction between must-have (errors) and should-have (warnings):

- Every dataset has valid STAC metadata
- Checksums in `versions.json` match actual files
- Thumbnails exist
- READMEs exist
- Descriptions are non-empty
- Style files are valid MapLibre JSON
- CRS is set
- Spec version is declared

Output is actionable: not just "invalid" but specific guidance on what to fix.

## Dual Interface: CLI + Python API

Every Portolan operation is available as both a CLI command and a Python function. The CLI is a thin wrapper around the Python API — all logic lives in the library layer. See **ADR-0007** for the architecture.

```python
# Python API
from portolan import Catalog

catalog = Catalog.init("./my-catalog")
catalog.add("census.parquet", title="Census 2022", auto=True)
catalog.sync()
```

```bash
# CLI equivalent
portolan init
portolan dataset add census.parquet --title "Census 2022" --auto
portolan sync
```

## Data Consumption

Portolan publishes catalogs; consuming them is up to the user's toolchain. GeoParquet and COG are designed for direct access from analytics engines.

**Supported patterns:**
- **SQL engines** — DuckDB, Snowflake, BigQuery, Databricks, Trino query GeoParquet directly via S3/GCS
- **Python** — Pandas, GeoPandas, or any library with fsspec/obstore support
- **GIS tools** — QGIS, ArcGIS load COG and GeoParquet natively

**Future:** Consumption guides and connection generators are planned. See [Roadmap: Data Consumption & SQL Engines](../ROADMAP.md#tbd-data-consumption--sql-engines).

## AI Integration

Portolan is AI-native but does not assume AI access. Many target users face cost, capacity, or security constraints that preclude AI use.

- **Without AI:** Full functionality via CLI and Python API. Interactive prompts with smart defaults handle styling, metadata, and validation.
- **With AI:** A `SKILLS.md` file ships with portolan, designed for users who want to pair the tool with an LLM. It documents the catalog structure, common workflows, and the Python API in a format optimized for LLM context windows.

AI is an accelerator, not a dependency.

## Design Principles

See `CLAUDE.md` for development principles. Key ADRs:

| ADR | Decision |
|-----|----------|
| [ADR-0003](shared/adr/0003-plugin-architecture.md) | Plugin architecture for format support |
| [ADR-0004](shared/adr/0004-iceberg-as-plugin.md) | Iceberg as plugin, not core |
| [ADR-0005](shared/adr/0005-versions-json-source-of-truth.md) | versions.json as single source of truth |
| [ADR-0006](shared/adr/0006-remote-ownership-model.md) | Remote ownership model |
| [ADR-0007](shared/adr/0007-cli-wraps-api.md) | CLI wraps Python API |
