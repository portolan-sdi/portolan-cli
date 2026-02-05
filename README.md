<div align="center">
  <img src="docs/assets/images/logo.svg" alt="Portolan Logo" width="200"/>
  <h1>Portolan CLI</h1>
  <p><strong>Cloud-native geospatial data catalogs, simplified</strong></p>
</div>

---

A CLI for publishing and managing **cloud-native geospatial data catalogs**. Portolan orchestrates format conversion (GeoParquet, COG), versioning, and sync to object storage—no running servers, just static files.

## Why Portolan?

| Benefit | How |
|---------|-----|
| **Scalable** | Cloud object storage that scales to petabytes |
| **Open** | 100% open source, open formats (GeoParquet, COG, STAC, Iceberg) |
| **AI-Ready** | STAC metadata enables semantic search and LLM integration |
| **Cheap** | Pay only for storage + egress — no servers to run |
| **Sovereign** | Host anywhere (AWS, GCS, Azure, MinIO, Cloudflare R2) |
| **Breaks the GIS silo** | Query with DuckDB, Snowflake, BigQuery, Databricks, Pandas — not just GIS tools |

## What It Does

- **Convert** vector/raster data to cloud-native formats (GeoParquet, COG)
- **Generate** STAC catalogs with rich metadata, thumbnails, and MapLibre styles
- **Version** datasets with checksums and history tracking
- **Sync** to S3, GCS, Azure, or any S3-compatible storage

## Quick Example

```bash
portolan init
portolan dataset add census.parquet --title "Census 2022" --auto
portolan remote add prod s3://my-bucket/catalog
portolan sync
```

## Installation

### Recommended: pipx (for global use)

```bash
pipx install portolan-cli
```

This installs `portolan` in an isolated environment while making the command globally available.

If you don't have pipx installed:
```bash
python3 -m pip install --user pipx
python3 -m pipx ensurepath
```

### Alternative: pip

```bash
pip install portolan-cli
```

**Note:** This installs into your global or user site-packages and may conflict with other packages.

### For Development

Use [uv](https://github.com/astral-sh/uv) for local development:

```bash
git clone https://github.com/portolan-sdi/portolan-cli.git
cd portolan-cli
uv sync --all-extras
uv run portolan --help
```

See [Contributing Guide](docs/contributing.md) for full development setup.

## Documentation

- [Contributing Guide](docs/contributing.md)
- [Architecture](context/architecture.md)
- [Roadmap](ROADMAP.md)

## License

Apache 2.0 — see [LICENSE](LICENSE)
