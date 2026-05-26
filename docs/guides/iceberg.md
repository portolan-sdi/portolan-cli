# Iceberg Backend

The Iceberg backend provides lakehouse-grade versioning for Portolan catalogs using [Apache Iceberg](https://iceberg.apache.org/). It adds ACID transactions, rollback, and snapshot pruning on top of Portolan's standard versioning.

## Installation

The Iceberg backend is an optional extra:

```bash
pip install portolan-cli[iceberg]
```

Or with pipx:

```bash
pipx install portolan-cli[iceberg]
```

!!! note "Python 3.11+"
    The Iceberg backend requires Python 3.11 or later (due to PyIceberg).

## Quick Start

```bash
# Initialize a catalog with the Iceberg backend
portolan init my-catalog --backend iceberg

# Add data (collection is inferred from the parent directory per ADR-0022;
# there is no --collection flag — place files under the collection dir first)
portolan add boundaries/data.parquet

# Check version history
portolan version list boundaries

# Rollback to a previous version
portolan version rollback boundaries 1.0.0

# Prune old versions
portolan version prune boundaries --keep 5
```

## What It Provides

| Feature | File Backend (default) | Iceberg Backend |
|---------|----------------------|-----------------|
| Version tracking | `versions.json` | Iceberg snapshot properties |
| Concurrent writes | Single-writer only | ACID transactions |
| Rollback | Not supported | Instant (snapshot pointer reset) |
| Prune | Not supported | Expire old snapshots |
| Schema evolution | Manual detection | Automatic via Iceberg |
| Catalog backend | Local files only | SQLite, REST, Glue, DynamoDB, etc. |

## Configuration

### Selecting the Backend

```bash
# Set during init (persists in config)
portolan init my-catalog --backend iceberg

# Or set in existing catalog
portolan config set backend iceberg
```

### Iceberg Catalog Configuration

The backend uses [PyIceberg's configuration](https://py.iceberg.apache.org/configuration/) via environment variables:

```
PYICEBERG_CATALOG__PORTOLAKE__<PROPERTY>=<value>
```

### Defaults (SQLite)

With no configuration, a local SQLite catalog is created:

| Setting | Default |
|---------|---------|
| Catalog type | `sql` (SQLite) |
| Catalog URI | `sqlite:///<cwd>/.portolan/iceberg.db` |
| Warehouse | `file:///<cwd>/.portolan/warehouse` |

### REST Catalog

Connect to any Iceberg REST catalog (Tabular, Polaris, Unity Catalog, Nessie):

```bash
export PYICEBERG_CATALOG__PORTOLAKE__TYPE=rest
export PYICEBERG_CATALOG__PORTOLAKE__URI=https://my-catalog.example.com
export PYICEBERG_CATALOG__PORTOLAKE__WAREHOUSE=s3://my-bucket/warehouse
```

### AWS Glue

```bash
export PYICEBERG_CATALOG__PORTOLAKE__TYPE=glue
export PYICEBERG_CATALOG__PORTOLAKE__WAREHOUSE=s3://my-bucket/warehouse
```

### Configuration Precedence

1. **External PyIceberg config** — `PYICEBERG_CATALOG__PORTOLAKE__*` environment
   variables and/or `~/.pyiceberg.yaml`, resolved by PyIceberg itself
   (see [PyIceberg configuration](https://py.iceberg.apache.org/configuration/)
   for the env-vs-YAML ordering)
2. **Defaults** (SQLite in `.portolan/`)

Portolan delegates external-config resolution to PyIceberg; it does not
separately layer env vars over the YAML file.

## How It Works

### Versioning: Semver on Iceberg Snapshots

Each Iceberg snapshot stores version metadata in its summary properties:

```python
{
    "portolake.version": "1.1.0",
    "portolake.breaking": "false",
    "portolake.message": "Updated population data",
    "portolake.assets": '{"data.parquet": {"sha256": "...", ...}}',
    "portolake.schema": '{"type": "geoparquet", ...}',
    "portolake.changes": '["data.parquet"]'
}
```

No external `versions.json` — version info travels with the Iceberg table.

### Collection-to-Table Mapping

Each collection maps to an Iceberg table under the `portolake` namespace: `portolake.<collection_name>`.

### Spatial Optimization

For datasets with geometry columns, the backend automatically:

- Adds a **geohash column** to every table with geometry. Precision is
  auto-selected by row count: `geohash_3` (~150 km cells) for 100K–10M rows,
  `geohash_4` (~20 km cells) otherwise.
- Adds **bbox columns** (`bbox_xmin`, `bbox_ymin`, `bbox_xmax`, `bbox_ymax`)
  for manifest statistics.
- Configures an **Iceberg partition spec** on the geohash column when the
  table has ≥100K rows. Smaller tables get the geohash column but no
  partition spec.

These derived columns are excluded from the STAC `table:columns` field and
from the static GeoParquet export.

## Programmatic Usage

```python
from portolan_cli.backends import get_backend

# Load the backend
backend = get_backend("iceberg")

# Or with a custom catalog
from pyiceberg.catalog import load_catalog
from portolan_cli.backends.iceberg import IcebergBackend

catalog = load_catalog("portolake", type="rest", uri="https://...")
backend = IcebergBackend(catalog=catalog)
```

See [Iceberg API Reference](../reference/iceberg-api.md) for full method documentation.
