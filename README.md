# Portolan CLI

[![PyPI version](https://badge.fury.io/py/portolan-cli.svg)](https://badge.fury.io/py/portolan-cli)
[![CI](https://github.com/portolan-sdi/portolan-cli/actions/workflows/ci.yml/badge.svg)](https://github.com/portolan-sdi/portolan-cli/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/portolan-sdi/portolan-cli/branch/main/graph/badge.svg)](https://codecov.io/gh/portolan-sdi/portolan-cli)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/portolan-sdi/portolan-cli/blob/main/LICENSE)

Portolan is an opinionated specification for serverless spatial data infrastructures.
It defines how publishers store, organize, document, and serve geospatial data as static files in their own storage.
Every Portolan catalog should offer a predictable, high-quality experience for publishers, people, and agents.

Portolan builds on existing standards instead of replacing them.
GeoParquet and PMTiles serve vector data, while COG serves raster data.
STAC provides a consistent catalog structure and index.
Required README and AGENTS.md files explain each catalog to people and agents.
The specification also covers operational details such as spatial ordering, bounding boxes, and CORS.

This repository contains the Portolan CLI, an implementation of the specification.
It helps publishers:

- Build [STAC](https://stacspec.org/en/) catalogs with [GeoParquet](https://geoparquet.org/) and [COG](https://cogeo.org/) assets.
- Extract ArcGIS, WFS, and Carto sources.
- Inspect and fetch published catalogs from the Portolan registry.
- Plan and publish catalog resources to GeoServer.
- Generate thumbnails, [PMTiles](https://docs.protomaps.com/pmtiles/), MapLibre styles, and STAC GeoParquet indexes.
- Track collection versions and checksums without a database.
- Validate catalog metadata, structure, and assets against the [Portolan specification](https://github.com/portolan-sdi/portolan-spec).
- Push, pull, sync, and clone catalogs through S3, GCS, Azure, or S3-compatible storage.
- Use structured JSON output, a typed Python API, or backend plugins.

## Command Areas

Portolan separates catalog publication from server publication.
The core commands build and publish a static Portolan catalog:

```sh
portolan init ./catalog
portolan add ./source-files --portolan-dir ./catalog
portolan check ./catalog --strict
portolan push s3://my-bucket/catalog --catalog ./catalog
```

The extraction commands turn service data into the same catalog shape:

```sh
portolan extract arcgis URL ./catalog --auto
portolan extract wfs URL ./catalog --auto
portolan extract carto URL ./catalog --auto
```

The registry commands read the published Portolan registry.
They can list registered catalogs and fetch local snapshots for follow-on work:

```sh
portolan registry list
portolan registry fetch CATALOG_ID --output ./catalogs
```

The server commands publish a Portolan catalog into a geospatial serving stack.
GeoServer is the first provider:

```sh
portolan server geoserver plan ./catalog
portolan server geoserver publish ./catalog
portolan server geoserver sync ./catalog
```

`plan` is read-only.
`publish` creates or updates GeoParquet and COG resources in GeoServer.
`sync` currently matches idempotent publish behavior and does not prune server resources.

## Extensibility Design

The CLI is a thin Click layer over library modules.
Command handlers resolve paths, credentials, and output mode, then delegate to
typed orchestration code.
This keeps the command surface stable while provider code evolves.

Server publication uses a provider boundary under `portolan_cli/server/`.
`server/model.py` defines provider-independent resource specs, plans, and
results.
`server/planner.py` reads a STAC catalog and selects publishable assets.
`server/providers/base.py` defines the `ServerProvider` protocol with `plan`,
`publish`, and `sync`.

The GeoServer implementation lives under `server/providers/geoserver/`.
It converts generic Portolan resources into GeoServer REST operations and
stores `portolan.*` provenance metadata on created resources.
That metadata lets a later plan detect whether a collection already exists,
needs an update, or must be skipped.

This boundary is intentionally provider-neutral.
A MapServer, Esri, or other server provider can reuse the same catalog loader
and planning model if it can map Portolan assets to that server's publishable
resources.
Provider-specific code should stay below `server/providers/<provider>/`, while
shared catalog discovery stays in `server/planner.py`.
The Portolan specification remains the source of truth for catalog conformance.

## Installation

*Portolan CLI is pre-1.0 software, as breaking changes are expected for the time being.*

```sh
uv tool install portolan-cli
```

Or with pip:

```sh
pip install portolan-cli
```

## Documentation

Start with the [end-to-end publishing example](https://portolan-sdi.github.io/portolan-cli/examples/).
We plan to add more tutorials over time.

See the [full documentation](https://portolan-sdi.github.io/portolan-cli/).
It includes an auto-generated [CLI reference](https://portolan-sdi.github.io/portolan-cli/reference/cli/)
and [Python API reference](https://portolan-sdi.github.io/portolan-cli/reference/python/).

## Development

```sh
git clone https://github.com/portolan-sdi/portolan-cli.git
cd portolan-cli
uv sync --all-extras
uv run pytest
```

See the [contributing guide](https://portolan-sdi.github.io/portolan-cli/contributing/) for details.

## License

[Apache 2.0](https://github.com/portolan-sdi/portolan-cli/blob/main/LICENSE)
