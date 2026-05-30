# AI Skills

Portolan includes **skills** — markdown guides that help AI assistants guide you through complex workflows. Think of them as recipes that Claude, GPT, or other AI agents can follow to help you accomplish tasks.

## Available Skills

| Skill | Description |
|-------|-------------|
| `bootstrap` | End-to-end catalog creation from any data source |
| `sourcecoop` | Upload data to [Source Cooperative](https://source.coop) |
| `consume` | Query and explore Portolan catalogs with DuckDB/Python |

## Using Skills

### With Claude Code

If you're using [Claude Code](https://docs.anthropic.com/en/docs/claude-code), simply ask:

> "Help me upload this data to Source Cooperative"

Claude will automatically use the `sourcecoop` skill to guide you through the process.

### Viewing Skills Directly

You can view any skill's content:

```bash
# List available skills
portolan skills list

# View a specific skill
portolan skills show sourcecoop
```

---

## Bootstrap Skill

End-to-end catalog creation from any data source. Checkpoint-based — pauses at key decisions and asks rather than assumes.

Supports remote services (WFS, ArcGIS) and local files (Shapefile, GeoPackage, etc.).

```bash
portolan skills show bootstrap
```

---

## Source Cooperative Skill

The `sourcecoop` skill helps you publish geospatial data to [Source Cooperative](https://source.coop), an open data commons for geospatial data.

### What It Does

1. **Checks credentials** — Verifies you have Source Co-op access configured
2. **Configures remote** — Sets up the S3 destination for your org/product
3. **Creates metadata** — Ensures required fields (title, description, license, contact)
4. **Generates READMEs** — Creates documentation from your metadata
5. **Uploads data** — Pushes to Source Co-op with parallel uploads

### Prerequisites

You need **automated access** to Source Cooperative. If you don't have it yet, contact [hello@source.coop](mailto:hello@source.coop) to request access.

### Quick Example

```bash
# Navigate to your data
cd ~/data/my-dataset

# Initialize catalog
portolan init --title "My Dataset" --auto

# Configure Source Co-op
portolan config set remote "s3://us-west-2.opendata.source.coop/myorg/my-dataset/"
portolan config set profile source-coop

# Add files and create metadata
portolan add .
portolan metadata init

# Edit .portolan/metadata.yaml with:
#   title, description, license, contact.email

# Generate READMEs and push
portolan readme
portolan push --workers 8
```

### Required Metadata

Source Co-op emphasizes good documentation. The skill ensures you provide:

| Field | Example |
|-------|---------|
| `title` | Philadelphia 2023 Aerial Orthoimagery |
| `description` | High-resolution aerial imagery covering Philadelphia County |
| `license` | CC-BY-4.0 |
| `contact.email` | data@example.org |

### Troubleshooting

**"Access Denied"** — Check your AWS credentials in `~/.aws/credentials` under `[source-coop]`. Credentials may have expired.

**Slow uploads** — Use `--workers 8` for parallel uploads. More than 8 workers doesn't usually help.

**Missing metadata** — Run `portolan metadata validate` to see which required fields are missing.

---

## Consume Skill

The `consume` skill helps you query and explore data from Portolan catalogs. It detects your environment, reads STAC metadata, and generates optimized queries.

### What It Does

1. **Detects your environment** — Checks for DuckDB, GeoPandas, rioxarray
2. **Reads STAC metadata** — Understands schema, assets, spatial extent
3. **Generates queries** — DuckDB SQL or Python code with full URLs
4. **Explains optimizations** — How to leverage Portolan's Hilbert ordering and bbox structs
5. **Guides exploration** — Dry runs, size checks, spatial filters

### Portolan GeoParquet Optimizations

Portolan produces optimized GeoParquet files that enable fast cloud-native queries:

| Optimization | Benefit |
|--------------|---------|
| **Hilbert spatial ordering** | Spatial queries read data sequentially |
| **Row groups (~100K rows)** | Predicate pushdown skips irrelevant data |
| **ZSTD compression** | Smaller files, fast decompression |
| **bbox struct column** | Fast spatial filter without geometry parsing |

### Quick Example (DuckDB)

```sql
-- Install spatial extension (once)
INSTALL spatial; LOAD spatial;

-- Query directly from Source Cooperative
SELECT * FROM read_parquet(
  'https://data.source.coop/nlebovits/censo-argentino/2022/radios.parquet'
) LIMIT 10;

-- Fast spatial filter using bbox struct
SELECT * FROM read_parquet(
  'https://data.source.coop/nlebovits/censo-argentino/2022/radios.parquet'
) WHERE bbox.xmin > -58.6 AND bbox.xmax < -58.2
    AND bbox.ymin > -34.8 AND bbox.ymax < -34.4;
```

### Quick Example (Python)

```python
import geopandas as gpd

gdf = gpd.read_parquet(
    "https://data.source.coop/nlebovits/censo-argentino/2022/radios.parquet"
)
print(gdf.head())
```

### Custom Examples in metadata.yaml

For datasets with unusual structure (required joins, multiple files), add custom examples to `.portolan/metadata.yaml`:

```yaml
examples:
  - engine: duckdb
    description: "Join census data with geographic boundaries"
    code: |
      SELECT r.*, c.population
      FROM read_parquet('https://.../radios.parquet') r
      JOIN read_parquet('https://.../census-data.parquet') c
        ON r.cod_2022 = c.id_geo
  - engine: python
    description: "Load and merge with GeoPandas"
    code: |
      radios = gpd.read_parquet('https://.../radios.parquet')
      census = pd.read_parquet('https://.../census-data.parquet')
      merged = radios.merge(census, left_on='cod_2022', right_on='id_geo')
```

### Troubleshooting

**403 Forbidden** — Source Cooperative uses HTTPS URLs, not S3. Use `https://data.source.coop/...` not `s3://...`.

**Slow queries** — Always `LIMIT` during exploration. Use `bbox` struct for spatial pre-filtering.

**Memory issues** — Use DuckDB (streams data) instead of loading everything into GeoPandas.
