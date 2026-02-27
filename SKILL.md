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

### `portolan add`
Track files in the catalog.

```bash
portolan add imagery/                      # Add all files in directory
portolan add .                             # Add all files in catalog
```

### `portolan check`
Validate a Portolan catalog or check files for cloud-native status.

```bash
portolan check                        # Validate all (metadata + geo-assets)
portolan check --metadata             # Validate metadata only
portolan check --geo-assets           # Check geo-assets only
portolan check /data --fix            # Convert files to cloud-native
```

### `portolan clone`
Clone a remote catalog to a local directory.

```bash
portolan clone s3://mybucket/catalog ./local --collection demographics
portolan clone s3://mybucket/catalog ./data -c imagery --profile prod
```

### `portolan config`
Manage catalog configuration.

```bash
portolan config set remote s3://my-bucket/catalog/
portolan config get remote
portolan config list
portolan config unset remote
```

### `portolan dataset`
Manage datasets in the catalog.

### `portolan init`
Initialize a new Portolan catalog.

```bash
portolan init                       # Initialize in current directory
portolan init --auto                # Skip prompts, use defaults
portolan init --title "My Catalog"  # Set title
portolan init /path/to/data --auto  # Initialize in specific directory
```

### `portolan pull`
Pull updates from a remote catalog.

```bash
portolan pull s3://mybucket/my-catalog --collection demographics
portolan pull s3://mybucket/catalog -c imagery --dry-run
portolan pull s3://bucket/catalog -c data --force
portolan pull s3://bucket/catalog -c data --profile myprofile
```

### `portolan push`
Push local catalog changes to cloud object storage.

```bash
portolan push s3://mybucket/catalog --collection demographics
portolan push gs://mybucket/catalog -c imagery --dry-run
portolan push s3://mybucket/catalog -c data --force --profile prod
```

### `portolan rm`
Remove files from tracking.

```bash
portolan rm --keep imagery/old_data.tif     # Safe: untrack only
portolan rm --dry-run vectors/              # Preview what would be removed
portolan rm -f demographics/census.parquet  # Force delete and untrack
portolan rm -f vectors/                     # Force remove entire directory
```

### `portolan scan`
Scan a directory for geospatial files and potential issues.

```bash
portolan scan /data/geospatial
portolan scan . --json
portolan scan /large/tree --max-depth=2
portolan scan /data --no-recursive
```

### `portolan sync`
Sync local catalog with remote storage (pull + push).

```bash
portolan sync s3://mybucket/catalog --collection demographics
portolan sync s3://mybucket/catalog -c imagery --dry-run
portolan sync s3://mybucket/catalog -c data --fix --force
portolan sync s3://mybucket/catalog -c data --profile prod
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
- `Catalog` - A Portolan catalog backed by a .portolan directory.
- `CatalogExistsError` - Raised when attempting to initialize a catalog that already exists.
- `FormatType` - Detected format type for routing to conversion library.
- `cli` - Portolan - Publish and manage cloud-native geospatial data catalogs.
- `detect_format` - Detect whether a file is vector, raster, or unknown.
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
