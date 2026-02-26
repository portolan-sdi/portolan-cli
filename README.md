<div align="center">
  <img src="docs/assets/images/cover.png" alt="Portolan" width="600"/>
</div>

<div align="center">

[![CI](https://github.com/portolan-sdi/portolan-cli/actions/workflows/ci.yml/badge.svg)](https://github.com/portolan-sdi/portolan-cli/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/portolan-sdi/portolan-cli/branch/main/graph/badge.svg)](https://codecov.io/gh/portolan-sdi/portolan-cli)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![PyPI version](https://badge.fury.io/py/portolan-cli.svg)](https://badge.fury.io/py/portolan-cli)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

</div>

---

Portolan enables organizations to share geospatial data in a low-cost, accessible, sovereign, and reliable way. Built on [cloud-native geospatial](https://cloudnativegeo.org) formats, a Portolan catalog is as interactive as any geospatial portal—but faster, more scalable, and much cheaper to run.

This CLI converts data to cloud-native formats (GeoParquet, COG), generates rich STAC metadata, and syncs to any object storage—no servers required.

## Quick Start

```bash
# Initialize a catalog in the current directory
portolan init

# Scan files before adding (optional but recommended)
portolan scan demographics/

# Add a dataset (collection is inferred from the directory name)
portolan add demographics/census.parquet

# Push collection to remote storage
portolan push s3://my-bucket/catalog --collection demographics
```

Other common commands:

```bash
portolan check                              # Validate catalog
portolan check --fix                        # Convert to cloud-native formats
portolan rm --keep demographics/old.parquet # Untrack without deleting
portolan pull s3://my-bucket/catalog -c demographics  # Pull from remote
portolan sync s3://my-bucket/catalog -c demographics  # Pull + push
portolan config set remote s3://my-bucket/catalog     # Save remote URL
portolan config list                        # List all config settings
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
