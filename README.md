# Portolan CLI

A CLI for publishing and managing **cloud-native geospatial data catalogs**. Portolan orchestrates format conversion (GeoParquet, COG), versioning, and sync to object storage—no running servers, just static files.

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

```bash
pip install portolan-cli
```

Or with uv:

```bash
uv add portolan-cli
```

## Documentation

- [Contributing Guide](docs/contributing.md)
- [Architecture](context/architecture.md)
- [Roadmap](ROADMAP.md)

## License

Apache 2.0 — see [LICENSE](LICENSE)
