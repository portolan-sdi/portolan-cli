# Partitioning Large GeoParquet Files

Portolan automatically partitions large GeoParquet files into smaller, spatially-organized chunks. This improves query performance and enables efficient cloud access patterns.

## When to Partition

Partition GeoParquet files when:

- **File size exceeds 2GB** (OGC best practices threshold)
- **Row count exceeds millions** of features
- **Spatial queries are common** (partitioning enables spatial pruning)

Portolan uses the 2GB threshold by default, matching [OGC GeoParquet best practices](https://geoparquet.org/).

## How It Works

When you add a large GeoParquet file, Portolan:

1. **Detects** the file exceeds the threshold
2. **Prompts** for confirmation (in interactive mode)
3. **Partitions** using KD-tree spatial indexing
4. **Stores** as Hive-style directories: `kdtree_cell=0/`, `kdtree_cell=1/`, etc.
5. **Emits** `partition:*` STAC extension metadata

```
$ portolan add large-dataset.parquet

Found 1 file(s) exceeding 2.0 GB threshold:
  large-dataset.parquet (4.23 GB)

Partition large files into spatial chunks? [Y/n] y

Partitioning: large-dataset.parquet
  Strategy: kdtree (data-driven spatial)
  Target: ~120,000 rows per partition
  ████████████████████████████████████████ 100%

✓ Created 98 partitions in large-dataset/
✓ Added to collection: default
```

## Configuration

Configure partitioning in `.portolan/config.yaml`:

```yaml
partitioning:
  enabled: true           # Enable auto-partitioning (default: true)
  prompt: true            # Ask before partitioning in interactive mode (default: true)
  threshold_gb: 2.0       # Size threshold in GB (default: 2.0)
  strategy: kdtree        # Partitioning strategy (default: kdtree)
  target_rows: 120000     # Target rows per partition (default: 120,000)
```

### Configuration Options

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `partitioning.enabled` | bool | `true` | Enable automatic partitioning during `portolan add` |
| `partitioning.prompt` | bool | `true` | Prompt user before partitioning (interactive mode) |
| `partitioning.threshold_gb` | float | `2.0` | File size threshold in GB |
| `partitioning.strategy` | string | `kdtree` | Partitioning strategy |
| `partitioning.target_rows` | int | `120000` | Target rows per partition |

### Strategies

| Strategy | Description |
|----------|-------------|
| `kdtree` | KD-tree spatial partitioning (default, data-driven) |
| `h3` | H3 hexagonal grid partitioning (planned) |
| `s2` | S2 cell partitioning (planned) |
| `quadkey` | Quadkey partitioning (planned) |

Currently only `kdtree` is implemented. Other strategies are planned for future releases.

## STAC Metadata

Partitioned collections include `partition:*` extension fields:

```json
{
  "stac_extensions": [
    "https://portolan-sdi.github.io/stac-partition-extension/v1.0.0/schema.json"
  ],
  "partition:scheme": "hive",
  "partition:strategy": "kdtree",
  "partition:keys": [
    {"name": "kdtree_cell", "type": "string"}
  ],
  "partition:file_count": 98,
  "assets": {
    "data": {
      "href": "./kdtree_cell=*/data.parquet",
      "partition:glob": "s3://bucket/collection/kdtree_cell=*/data.parquet"
    }
  }
}
```

## Consuming Partitioned Data

### DuckDB

DuckDB can query partitioned data directly using the glob pattern:

```sql
SELECT *
FROM read_parquet('s3://bucket/collection/kdtree_cell=*/data.parquet')
WHERE ST_Intersects(geometry, ST_GeomFromText('POLYGON(...)'))
```

DuckDB automatically prunes partitions based on the Hive directory structure.

### PyArrow

```python
import pyarrow.parquet as pq
import pyarrow.dataset as ds

# Read as dataset with partition pruning
dataset = ds.dataset(
    "s3://bucket/collection/",
    partitioning="hive"
)

# Query with partition filtering
table = dataset.to_table(
    filter=ds.field("kdtree_cell").isin(["0", "1", "2"])
)
```

### GDAL/OGR

```bash
ogrinfo -al "/vsicurl/s3://bucket/collection/kdtree_cell=0/data.parquet"
```

## Validation

Use `portolan check --thorough` to validate partition consistency:

```bash
$ portolan check --thorough

Checking partition structure...
✓ All partitions use consistent key: kdtree_cell
✓ All partition files have consistent schema
✓ No orphan files outside partition structure
```

This checks:
- All partition directories use the same key pattern
- All parquet files have identical schemas
- No files exist outside the partition structure

## Manual Partitioning

For more control, use the standalone `portolan partition` command:

```bash
portolan partition large.parquet \
  --output-dir ./partitioned/ \
  --strategy kdtree \
  --target-rows 100000
```

See `portolan partition --help` for all options.
