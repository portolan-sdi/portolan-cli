# portolan-cli SKILL.md

This file helps AI agents assist users with Portolan CLI tasks.

<!-- BEGIN GENERATED: overview -->
## What is Portolan?

Portolan CLI - Publish and manage cloud-native geospatial data catalogs.

Portolan is a CLI for publishing and managing **cloud-native geospatial data catalogs**. It orchestrates format conversion (GeoParquet, COG), versioning, and sync to object storage (S3, GCS, Azure)—no running servers, just static files.

**Key concepts:**
- **STAC** (SpatioTemporal Asset Catalog) — The catalog metadata spec
- **GeoParquet** — Cloud-optimized vector data (columnar, spatial indexing)
- **COG** (Cloud-Optimized GeoTIFF) — Cloud-optimized raster data (HTTP range requests)
- **versions.json** — Single source of truth for version history, sync state, and checksums
<!-- END GENERATED: overview -->

<!-- BEGIN GENERATED: cli-commands -->
## CLI Commands

### `portolan init`
Initialize a new Portolan catalog.

Creates a catalog.json at the root level and a .portolan directory with management files (config.json, state.json, versions.json).

```bash
portolan init                       # Initialize in current directory
portolan init --auto                # Skip prompts, use defaults
portolan init --title "My Catalog"  # Set title
portolan init /path/to/data --auto  # Initialize in specific directory
```

### `portolan scan`
Scan a directory for geospatial files and potential issues.

Discovers files by extension, validates shapefile completeness, and reports issues that may cause problems during import.

```bash
portolan scan /data/geospatial      # Scan directory
portolan scan . --json              # JSON output
portolan scan /large/tree --max-depth=2  # Limit depth
portolan scan /data --fix --dry-run # Preview fixes
portolan scan /data --fix           # Apply auto-fixes
```

### `portolan check`
Validate a Portolan catalog or check files for cloud-native status.

Runs validation rules against the catalog and reports any issues. With --fix, converts non-cloud-native files to GeoParquet (vectors) or COG (rasters).

```bash
portolan check                        # Validate all (metadata + geo-assets)
portolan check --metadata             # Validate metadata only
portolan check --geo-assets           # Check geo-assets only
portolan check /data --fix            # Convert files to cloud-native
portolan check /data --fix --dry-run  # Preview conversions
```

### `portolan add`
Track files in the catalog.

Adds files to the Portolan catalog with automatic collection inference. The collection ID is determined from the first directory component of the path relative to the catalog root.

```bash
cd my-catalog && portolan add demographics/census.parquet
portolan add imagery/                 # Add all files in directory
portolan add .                        # Add all files in catalog
```

### `portolan rm`
Remove files from tracking.

By default, removes the file from disk AND untracks it from the catalog. Requires --force for destructive operations.

```bash
portolan rm --keep imagery/old.tif    # Untrack only (safe)
portolan rm --dry-run vectors/        # Preview removal
portolan rm -f demographics/census.parquet  # Force delete
```

### `portolan push`
Push local catalog changes to cloud object storage.

Syncs a collection's versions to a remote destination (S3, GCS, Azure). Uses optimistic locking to detect concurrent modifications.

```bash
portolan push s3://mybucket/catalog --collection demographics
portolan push gs://mybucket/catalog -c imagery --dry-run
portolan push s3://mybucket/catalog -c data --force --profile prod
```

### `portolan pull`
Pull updates from a remote catalog.

Fetches changes from a remote catalog and downloads updated files. Checks for uncommitted local changes before overwriting.

```bash
portolan pull s3://mybucket/catalog --collection demographics
portolan pull s3://mybucket/catalog -c imagery --dry-run
portolan pull s3://bucket/catalog -c data --force
```

### `portolan sync`
Sync local catalog with remote storage (pull + push).

Orchestrates a full sync workflow: Pull → Init → Scan → Check → Push. This is the recommended way to keep a local catalog in sync with remote.

```bash
portolan sync s3://mybucket/catalog --collection demographics
portolan sync s3://mybucket/catalog -c imagery --dry-run
portolan sync s3://mybucket/catalog -c data --fix --force
```

### `portolan config`
Manage catalog configuration.

Configuration is stored in .portolan/config.yaml and follows precedence: CLI argument > environment variable > collection-level > catalog-level > default.

```bash
portolan config set remote s3://my-bucket/catalog/
portolan config get remote
portolan config list
portolan config unset remote
```
<!-- END GENERATED: cli-commands -->

<!-- BEGIN GENERATED: python-api -->
## Python API

Portolan exposes a Python API for programmatic access:

```python
from portolan_cli import Catalog, FormatType, detect_format

# Initialize a catalog
catalog = Catalog("/path/to/data")

# Detect file format
format_type = detect_format("data.parquet")  # Returns FormatType.GEOPARQUET
```

**Public exports:**
- `Catalog` - Main catalog class for programmatic operations
- `CatalogExistsError` - Raised when catalog already exists
- `FormatType` - Enum of supported geospatial formats
- `detect_format` - Detect format type from file path
- `cli` - Click CLI entry point
<!-- END GENERATED: python-api -->

<!-- freshness: last-verified: 2026-02-27 -->
## Common Workflows

### Publishing a New Catalog

1. **Initialize the catalog structure:**
   ```bash
   portolan init --title "My Geospatial Data"
   ```

2. **Scan directory for files and issues:**
   ```bash
   portolan scan /data/geospatial
   # Fix any issues found
   portolan scan /data/geospatial --fix
   ```

3. **Check cloud-native compliance and convert:**
   ```bash
   portolan check --geo-assets --fix --dry-run  # Preview
   portolan check --geo-assets --fix            # Convert
   ```

4. **Track files in the catalog:**
   ```bash
   portolan add demographics/
   portolan add imagery/
   ```

5. **Push to cloud storage:**
   ```bash
   portolan push s3://mybucket/my-catalog --collection demographics
   ```

### Updating an Existing Catalog

1. **Pull latest from remote:**
   ```bash
   portolan pull s3://mybucket/my-catalog --collection demographics
   ```

2. **Make local changes** (add/modify files)

3. **Scan and check:**
   ```bash
   portolan scan .
   portolan check
   ```

4. **Push changes:**
   ```bash
   portolan push s3://mybucket/my-catalog --collection demographics
   ```

### Full Sync Workflow (Recommended)

For ongoing synchronization, use `sync` which orchestrates the full workflow:

```bash
# Single command: pull → init → scan → check → push
portolan sync s3://mybucket/my-catalog --collection demographics

# With auto-fix for cloud-native conversion
portolan sync s3://mybucket/my-catalog -c demographics --fix
```
<!-- /freshness -->

## Troubleshooting

### Common Errors

#### "Not inside a Portolan catalog"
**Error:** `Not inside a Portolan catalog (no catalog.json found)`

**Solution:** Either:
- Run `portolan init` to create a catalog
- Navigate into an existing catalog directory
- Use `--portolan-dir` to specify the catalog path

#### "Catalog already exists"
**Error:** `Already a Portolan catalog at /path`

**Solution:** The directory already has a catalog. If you want to reinitialize, remove `catalog.json` and `.portolan/` first.

#### "Push conflict"
**Error:** `Push conflict: remote has newer version`

**Solution:** Either:
- Run `portolan pull` first to get remote changes
- Use `--force` to overwrite (careful: loses remote changes)

#### "Uncommitted changes"
**Error:** `Pull blocked by uncommitted changes`

**Solution:** Either:
- Commit or push your local changes first
- Use `--force` to discard local changes and pull anyway

### File Format Issues

#### Shapefile Missing Components
**Warning:** Shapefiles require .shp, .shx, and .dbf files together.

**Solution:** Ensure all required sidecar files are present. `portolan scan` will detect incomplete shapefiles.

#### Non-Cloud-Native Files
**Warning:** Files like GeoJSON or Shapefiles aren't cloud-optimized.

**Solution:** Use `portolan check --fix` to convert:
- Vectors → GeoParquet
- Rasters → COG (Cloud-Optimized GeoTIFF)

### Getting JSON Output

All commands support `--json` or `--format json` for machine-readable output:

```bash
portolan scan . --json
portolan check --format json
portolan --format json init --auto
```

JSON output follows a consistent envelope format:
```json
{
  "success": true,
  "command": "scan",
  "data": { ... },
  "errors": []
}
```
