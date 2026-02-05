# Portolan CLI

A CLI for publishing and managing **cloud-native geospatial data catalogs**. Portolan orchestrates format conversion (GeoParquet, COG), versioning, and sync to object storage—no running servers, just static files.

## What It Does

- **Convert** vector/raster data to cloud-native formats (GeoParquet, COG)
- **Generate** STAC catalogs with rich metadata, thumbnails, and MapLibre styles
- **Version** datasets with checksums and history tracking
- **Sync** to S3, GCS, Azure, or any S3-compatible storage

## Installation

### Recommended: pipx (for global use)

We recommend installing Portolan CLI with [pipx](https://pipx.pypa.io/), which installs the tool in an isolated environment while making the `portolan` command globally available:

```bash
pipx install portolan-cli
```

!!! tip "Installing pipx"
    If you don't have pipx installed:
    ```bash
    python3 -m pip install --user pipx
    python3 -m pipx ensurepath
    ```

    After installation, restart your terminal or run the path command shown by pipx.

### Alternative: pip

You can also install with pip, though this may conflict with other packages:

```bash
pip install portolan-cli
```

### For Development

If you're contributing to Portolan CLI, use [uv](https://github.com/astral-sh/uv) for local development:

```bash
git clone https://github.com/portolan-sdi/portolan-cli.git
cd portolan-cli
uv sync --all-extras
uv run portolan --help
```

See the [Contributing Guide](contributing.md) for full development setup.

## Quick Start

Initialize a new catalog:

```bash
portolan init
```

Add a dataset:

```bash
portolan dataset add census.parquet --title "Census 2022" --auto
```

Configure a remote:

```bash
portolan remote add prod s3://my-bucket/catalog
```

Sync to remote:

```bash
portolan sync
```

## Next Steps

- Read the [Roadmap](roadmap.md) to see planned features
- Check out the [Contributing Guide](contributing.md) to get involved
- View the [Changelog](changelog.md) for release history

## License

Apache 2.0 — see [LICENSE](https://github.com/portolan-sdi/portolan-cli/blob/main/LICENSE)
