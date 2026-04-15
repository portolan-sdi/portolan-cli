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

# Add data (automatically versioned in Iceberg)
portolan add data.parquet --collection boundaries

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

1. **Environment variables** (`PYICEBERG_CATALOG__PORTOLAKE__*`)
2. **PyIceberg config file** (`~/.pyiceberg.yaml`)
3. **Defaults** (SQLite in `.portolan/`)

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

- Adds **geohash columns** for Iceberg partition specs (datasets >= 100K rows)
- Adds **bbox columns** (xmin, ymin, xmax, ymax) for manifest statistics

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
