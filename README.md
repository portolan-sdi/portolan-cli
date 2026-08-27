# Portolan CLI

[![PyPI version](https://badge.fury.io/py/portolan-cli.svg)](https://badge.fury.io/py/portolan-cli)
[![CI](https://github.com/portolan-sdi/portolan-cli/actions/workflows/ci.yml/badge.svg)](https://github.com/portolan-sdi/portolan-cli/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/portolan-sdi/portolan-cli/branch/main/graph/badge.svg)](https://codecov.io/gh/portolan-sdi/portolan-cli)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/portolan-sdi/portolan-cli/blob/main/LICENSE)

Publish geospatial data as plain files in your own storage, connected into a searchable network.

- Build [STAC](https://stacspec.org/en/) catalogs with [GeoParquet](https://geoparquet.org/) and [COG](https://cogeo.org/) assets.
- Extract ArcGIS, WFS, and Carto sources.
- Generate thumbnails, [PMTiles](https://docs.protomaps.com/pmtiles/), MapLibre styles, and STAC GeoParquet indexes.
- Track collection versions and checksums without a database.
- Validate catalog metadata, structure, and assets against the [Portolan standard](https://github.com/portolan-sdi/portolan-spec).
- Push, pull, sync, and clone catalogs through S3, GCS, Azure, or S3-compatible storage.
- Use structured JSON output, a typed Python API, or backend plugins.

## Installation

```sh
uv tool install portolan-cli
```

Or with pip:

```sh
pip install portolan-cli
```

## Documentation

[Full documentation](https://portolan-sdi.github.io/portolan-cli/) is available on the website.

Start with the [tested publishing example](https://portolan-sdi.github.io/portolan-cli/examples/).
Use the generated [CLI reference](https://portolan-sdi.github.io/portolan-cli/reference/cli/) for every command and option.

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
