# Creating Nested Catalogs

Portolan supports hierarchical catalog structures where directories automatically become subcatalogs. This is useful for organizing large datasets by theme, region, or time period.

## Quick Start

```bash
# Organize your data into themed directories
mkdir -p my-catalog/{climate,environment,housing}
cp climate-data/*.parquet my-catalog/climate/
cp env-data/*.parquet my-catalog/environment/

# Initialize and add everything
cd my-catalog
portolan init --auto --title "My Regional Data"
portolan add . --workers 4

# Add metadata and generate documentation
portolan metadata init
# Edit .portolan/metadata.yaml with your info
portolan readme --recursive
```

## How Directory Structure Maps to STAC

Portolan infers the catalog hierarchy from your directory layout:

```
my-catalog/                    # Root catalog (catalog.json)
├── climate/                   # Subcatalog (climate/catalog.json)
│   ├── temperature/          # Collection (climate/temperature/collection.json)
│   │   └── temperature.parquet
│   └── precipitation/        # Collection
│       └── precipitation.parquet
└── demographics/              # Subcatalog
    └── census-2020/          # Collection
        └── census.parquet
```

When you run `portolan add .`, Portolan:

1. Creates `catalog.json` at the root with links to subcatalogs
2. Creates `catalog.json` in each intermediate directory (subcatalogs)
3. Creates `collection.json` + item metadata in leaf directories (collections)
4. Generates `versions.json` for tracking at each level

## Bulk Adding Files

Process many files efficiently with parallel workers:

```bash
portolan add . --workers 4 --verbose
```

The `--verbose` flag shows progress for each file. Without it, only changed/added files appear.

## Metadata and READMEs

### Setting Up Metadata

```bash
portolan metadata init
```

This creates `.portolan/metadata.yaml` with required fields (contact, license) and optional fields (citation, keywords, source URL, known issues).

Example:

```yaml
contact:
  name: "Data Team"
  email: "data@example.org"

license: "CC-BY-4.0"
license_url: "https://creativecommons.org/licenses/by/4.0/"

keywords:
  - climate
  - regional data
  - open data

source_url: "https://data.example.org/"
processing_notes: "Converted from Shapefile to GeoParquet with Hilbert sorting."
known_issues: "Temporal extent not specified for most datasets."
```

### Generating READMEs

```bash
portolan readme --recursive
```

This generates README.md files at every level — root catalog, subcatalogs, and collections. Metadata from the root cascades down, so you only need to edit one `metadata.yaml` for consistent attribution across all READMEs.

To preview without writing:

```bash
portolan readme --stdout
```

## Validation

Check the catalog structure and data formats:

```bash
portolan check --verbose
```

This validates:

- STAC metadata completeness
- Cloud-native format compliance (GeoParquet, COG)
- Provisional datetime warnings (items without explicit dates)

## Example: The Hague Open Data

A real-world example with 6 thematic subcatalogs and 23 collections:

```
den-haag/
├── catalog.json
├── climate/           # 3 collections: heat maps, climate scores
├── environment/       # 7 collections: air quality, noise, soil
├── housing/           # 1 collection: energy labels
├── infrastructure/    # 3 collections: waste, zones, storage
├── nature/            # 7 collections: species, habitats, trees
└── water/             # 2 collections: gauges, water bodies
```

Created with:

```bash
portolan init --auto --title "The Hague Open Data" \
  --description "Municipal open data from Den Haag, Netherlands"
portolan add . --workers 4
portolan metadata init
# Edit .portolan/metadata.yaml
portolan readme --recursive
portolan check
```

## Cloning Remote Nested Catalogs

Clone nested catalogs from object storage:

```bash
# Clone recursively discovers all subcatalogs and collections
portolan clone s3://bucket/nested-catalog ./local-copy --profile my-profile
```

Portolan automatically traverses subcatalog `catalog.json` files to find actual collections. For the structure above, clone would find:

- `climate/temperature` (collection)
- `climate/precipitation` (collection)
- `demographics/census-2020` (collection)

Not the intermediate subcatalogs (`climate/catalog.json`, `demographics/catalog.json`).

## Restoring Missing Files

If you accidentally delete local data files, use `--restore` to re-download them:

```bash
# Normal pull - won't download if versions match
portolan pull s3://bucket/my-catalog

# Restore pull - re-downloads missing files even when versions match
portolan pull s3://bucket/my-catalog --restore
```

The `--restore` flag checks file existence locally and downloads any missing assets, regardless of version metadata. Useful for recovering from accidental deletions.

## Tips

**Start flat, restructure later.** You can reorganize directories and re-run `portolan add .` — Portolan regenerates the STAC hierarchy from the current structure.

**One metadata.yaml for consistency.** Root-level metadata cascades to all READMEs. Only create collection-level `metadata.yaml` files when you need overrides.

**Use `--workers` for large catalogs.** Parallel processing significantly speeds up metadata extraction for catalogs with many files.
