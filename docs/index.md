---
hide:
  - navigation
  - toc
---

# Portolan CLI

**Cloud-native geospatial data catalogs, simplified**

Portolan enables organizations to share geospatial data in a low-cost, accessible, sovereign, and reliable way. Built on [cloud-native geospatial](https://cloudnativegeo.org) formats, a Portolan catalog is as interactive as any geospatial portal—but faster, more scalable, and much cheaper to run.

This CLI converts data to cloud-native formats (GeoParquet, COG), generates rich STAC metadata, and syncs to any object storage—no servers required.

[Get Started](#installation){ .md-button .md-button--primary }
[View on GitHub](https://github.com/portolan-sdi/portolan-cli){ .md-button }

---

## Why Portolan?

<div class="grid cards" markdown>

- :material-arrow-expand-all:{ .lg .middle } **Scalable**

    ---

    Cloud object storage that scales to petabytes

- :material-open-source-initiative:{ .lg .middle } **Open**

    ---

    100% open source, open formats (GeoParquet, COG)

- :material-robot:{ .lg .middle } **AI-Ready**

    ---

    STAC metadata enables semantic search and LLM integration

- :material-currency-usd-off:{ .lg .middle } **Cheap**

    ---

    Pay only for storage + egress — no servers to run

- :material-flag:{ .lg .middle } **Sovereign**

    ---

    Host anywhere (AWS, GCS, Azure, MinIO, Cloudflare R2)

- :material-table-large:{ .lg .middle } **Breaks the GIS silo**

    ---

    Query with DuckDB, Snowflake, BigQuery, Databricks, Pandas — not just GIS tools

</div>

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

# Add files to a collection (collection name = directory name)
portolan add demographics/

# Push the collection to remote storage
portolan push s3://my-bucket/catalog --collection demographics
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
