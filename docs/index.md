---
hide:
  - navigation
  - toc
---

# Portolan CLI

**Cloud-native geospatial data catalogs, simplified**

A CLI for publishing and managing **cloud-native geospatial data catalogs**. Portolan orchestrates format conversion (GeoParquet, COG), versioning, and sync to object storage—no running servers, just static files.

[Get Started](#installation){ .md-button .md-button--primary }
[View on GitHub](https://github.com/portolan-sdi/portolan-cli){ .md-button }

---

## What It Does

<div class="grid cards" markdown>

- :material-file-replace-outline:{ .lg .middle } **Convert**

    ---

    Transform vector and raster data to cloud-native formats (GeoParquet, COG)

- :material-map-outline:{ .lg .middle } **Catalog**

    ---

    Generate STAC catalogs with rich metadata, thumbnails, and MapLibre styles

- :material-history:{ .lg .middle } **Version**

    ---

    Track datasets with checksums and full history

- :material-cloud-sync-outline:{ .lg .middle } **Sync**

    ---

    Push to S3, GCS, Azure, or any S3-compatible storage

</div>

## Installation

### Recommended: pipx

Install with [pipx](https://pipx.pypa.io/) for an isolated global install:

```bash
pipx install portolan-cli
```

!!! tip "Installing pipx"
    ```bash
    python3 -m pip install --user pipx
    python3 -m pipx ensurepath
    ```
    Restart your terminal after installation.

### Alternative: pip

```bash
pip install portolan-cli
```

### For Development

```bash
git clone https://github.com/portolan-sdi/portolan-cli.git
cd portolan-cli
uv sync --all-extras
uv run portolan --help
```

See the [Contributing Guide](contributing.md) for full setup.

## Quick Start

```bash
# Initialize a catalog
portolan init

# Add a dataset
portolan dataset add census.parquet --title "Census 2022" --auto

# Configure remote storage
portolan remote add prod s3://my-bucket/catalog

# Sync to remote
portolan sync
```

## Next Steps

<div class="grid cards" markdown>

- :material-road-variant:{ .lg .middle } **[Roadmap](roadmap.md)**

    ---

    See planned features and the development timeline

- :material-account-group:{ .lg .middle } **[Contributing](contributing.md)**

    ---

    Learn how to get involved in development

- :material-text-box-outline:{ .lg .middle } **[Changelog](changelog.md)**

    ---

    View the release history

</div>

---

<small>
**License**: Apache 2.0 — [View on GitHub](https://github.com/portolan-sdi/portolan-cli/blob/main/LICENSE)
</small>
