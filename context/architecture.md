# Portolan CLI Architecture

## Overview

Portolan CLI is the command-line tool for creating and managing Portolan catalogs — collections of cloud-native geospatial data with rich metadata, styling, and versioning. It orchestrates format conversion, STAC catalog generation, metadata enrichment, and cloud sync.

## System Boundaries

Portolan CLI is one component in a broader ecosystem. Each component has a clear responsibility:

| Component | Responsibility |
|---|---|
| **[geoparquet-io](https://github.com/geoparquet/geoparquet-io)** | Vector format conversion, inspection, partitioning, sorting |
| **[gpio-pmtiles](https://github.com/geoparquet-io/gpio-pmtiles)** | PMTiles generation from GeoParquet |
| **[rio-cogeo](https://github.com/cogeotiff/rio-cogeo)** | Raster conversion to Cloud-Optimized GeoTIFF |
| **portolan-cli** | Catalog orchestration, metadata, versioning, validation, sync |
| **QGIS plugin** | Browse and pull data from Portolan catalogs into local GIS |
| **Browser/Map UI** | Web-based catalog browsing and visualization |
| **Global Data Bootstrapper** | Subset global datasets to bootstrap local catalogs |

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

Commands covering the full lifecycle from files to live catalog:

```
# Catalog setup
portolan init [PATH]                   # Create catalog.json + .portolan/ structure
portolan init --auto                   # Non-interactive, use defaults

# Discovery & validation
portolan scan <path>                   # Discover geospatial files, detect issues
portolan scan --fix --dry-run          # Preview safe renames (invalid chars, reserved names)
portolan check                         # Validate local catalog (metadata + geo-assets)
portolan check --fix --dry-run         # Preview cloud-native conversions
portolan check --metadata              # Validate STAC metadata only
portolan check --geo-assets            # Check cloud-native compliance only

# File tracking (git-style top-level commands)
portolan add <path>                    # Track files (collection inferred from directory)
portolan rm <path>                     # Untrack and delete (requires --force)
portolan rm --keep <path>              # Untrack without deleting
portolan dataset list                  # List collections and items
portolan dataset info <name>           # Collection/item metadata summary

# Remote sync (all require --collection/-c flag)
portolan push <url> -c <collection>    # Upload collection to remote
portolan pull <url> -c <collection>    # Download collection from remote
portolan sync <url> -c <collection>    # Orchestrate: pull → check --fix → push
portolan clone <url> <path> -c <coll>  # Clone remote collection to new directory

# Configuration
portolan config set <key> <value>      # Set config (e.g., remote URL)
portolan config get <key>              # Get config value
portolan config list                   # List all settings
portolan config unset <key>            # Remove setting
```

### `portolan add` Workflow

The `add` command tracks files in the catalog with automatic collection inference:

1. Detect input format
2. Extract metadata (spatial extent, schema, CRS)
3. Infer collection from directory structure (first path component)
4. Generate STAC collection/item with metadata
5. Update `versions.json` with checksums
6. Stage files to the catalog structure (STAC at root per ADR-0023)

The command is designed for simplicity — it just tracks files. Metadata enrichment (titles, descriptions, style generation) is a separate workflow (see issue #108).

Flags:
- `--verbose` — show detailed output including skipped files
- `--portolan-dir` — override catalog root detection

### Directory Handling

`portolan add` and `portolan rm` both accept directories for batch operations. Files are processed individually, with each becoming a separate item in the inferred collection.

## Catalog Structure

Per **ADR-0023**: STAC files and `versions.json` live at root level. Only internal tooling state goes in `.portolan/`.

```
./                            # Catalog root
├── catalog.json              # STAC root catalog
├── versions.json             # Catalog-level versioning (discoverable)
├── .portolan/                # Internal state only
│   ├── config.json           # Managed state sentinel (empty)
│   ├── config.yaml           # User configuration (remote URL, etc.)
│   └── state.json            # Local sync state
├── <collection>/             # Collection at root level
│   ├── collection.json       # STAC collection
│   ├── versions.json         # Collection-level versioning (discoverable)
│   └── <item>/               # Item directory
│       ├── item.json         # STAC item
│       └── <data files>      # Cloud-native formats
```

**Note:** `versions.json` is user-visible metadata (version history, checksums), not internal state. It lives alongside STAC files so consumers can discover it.

**Planned additions:** `style.json` (MapLibre styles), `thumbnail.png` (previews), `README.md` (auto-generated docs) — see issue #108.

## Remote Ownership

Portolan owns the bucket contents. Users configure access; Portolan manages everything inside. Manual edits are unsupported and flagged as drift. See **ADR-0006** for the full ownership model.

Currently, `push --force` can overwrite remote state. Future commands for drift detection and repair are planned.

**Future:** Multi-tenant access control and visibility are planned but not yet scoped. See [Roadmap: Access Control & Visibility](../ROADMAP.md#tbd-access-control--visibility).

## Versioning

`versions.json` is the single source of truth for version history, sync state, and integrity checksums. See **ADR-0005** for the full design.

- Current version files live at collection/item root
- Old versions archived to `/v{version}/` paths
- Version pruning is planned (see ROADMAP v0.8)

## Discovery & Validation

### `portolan scan`

Discovers geospatial files and detects issues before import:

- File discovery by extension (.parquet, .shp, .tif, .gpkg, etc.)
- Shapefile completeness validation (.shp/.shx/.dbf/.prj)
- Filename issues (invalid characters, Windows reserved names, long paths)
- Multiple primary assets in same directory (manual grouping decisions)
- Collection structure suggestions based on filename patterns

With `--fix`: auto-renames files with safe transformations (spaces → underscores, etc.)

### `portolan check`

Validates the catalog against the Portolan spec with a clear distinction between must-have (errors) and should-have (warnings):

- Catalog structure (.portolan/ exists, catalog.json valid)
- STAC fields present and valid
- Cloud-native format compliance

Flags:
- `--metadata` — validate STAC metadata only (links, schema, required fields)
- `--geo-assets` — check geospatial assets only (cloud-native status)
- `--fix` — convert non-cloud-native files to GeoParquet (vectors) or COG (rasters)
- `--dry-run` — preview what would be converted

Output is actionable: not just "invalid" but specific guidance on what to fix.

## Dual Interface: CLI + Python API

Every Portolan operation is available as both a CLI command and a Python function. The CLI is a thin wrapper around the Python API — all logic lives in the library layer. See **ADR-0007** for the architecture.

```python
# Python API (module-level functions)
from portolan_cli.catalog import init_catalog
from portolan_cli.dataset import add_dataset  # Legacy name, tracks files to collection
from portolan_cli.push import push

init_catalog(Path("./my-catalog"))
add_dataset(Path("./my-catalog"), Path("demographics/"))  # Add directory to collection
push(Path("./my-catalog"), "s3://bucket/catalog", collection="demographics")
```

```bash
# CLI equivalent
portolan init
portolan add demographics/
portolan push s3://bucket/catalog --collection demographics
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

- **Without AI:** Full functionality via CLI and Python API. Smart defaults handle validation and format conversion.
- **With AI:** A `SKILLS.md` file is planned (see issue #109) to document the catalog structure, common workflows, and the Python API in a format optimized for LLM context windows.

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
