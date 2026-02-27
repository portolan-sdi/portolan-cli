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

<!-- --8<-- [start:intro] -->
Portolan enables organizations to share geospatial data in a low-cost, accessible, sovereign, and reliable way. Built on [cloud-native geospatial](https://cloudnativegeo.org) formats, a Portolan catalog is as interactive as any geospatial portal—but faster, more scalable, and much cheaper to run.

This CLI converts data to cloud-native formats (GeoParquet, COG), generates rich STAC metadata, and syncs to any object storage—no servers required.
<!-- --8<-- [end:intro] -->

<!-- --8<-- [start:quickstart] -->
## Quick Start

```bash
# Initialize a catalog
portolan init

# Scan a directory for issues (optional but recommended)
portolan scan demographics/

# Add all files in a directory (creates a "demographics" collection)
portolan add demographics/

# notest - requires S3 credentials
# Push the collection to remote storage
portolan push s3://my-bucket/catalog --collection demographics
```

Other common commands:

```bash
portolan check                              # Validate catalog
portolan check --fix                        # Convert to cloud-native formats
portolan rm --keep demographics/old.parquet # Untrack without deleting
# notest - requires S3 credentials
portolan pull s3://my-bucket/catalog -c demographics  # Pull from remote
# notest - requires S3 credentials
portolan sync s3://my-bucket/catalog -c demographics  # Full workflow: pull → check → push
portolan config set remote s3://my-bucket/catalog     # Save remote URL
portolan config list                        # List all config settings
```
<!-- --8<-- [end:quickstart] -->

<!-- --8<-- [start:installation] -->
## Installation

### Recommended: pipx (for global use)

```bash
# notest - installation command
pipx install portolan-cli
```

This installs `portolan` in an isolated environment while making the command globally available.

If you don't have pipx installed:
```bash
# notest - installation commands
python3 -m pip install --user pipx
python3 -m pipx ensurepath
```

### Alternative: pip

```bash
# notest - installation command
pip install portolan-cli
```

**Note:** This installs into your global or user site-packages and may conflict with other packages.

### For Development

Use [uv](https://github.com/astral-sh/uv) for local development:

```bash
# notest - development setup
git clone https://github.com/portolan-sdi/portolan-cli.git
cd portolan-cli
uv sync --all-extras
uv run portolan --help
```
<!-- --8<-- [end:installation] -->

See [Contributing Guide](docs/contributing.md) for full development setup.

## Documentation

- [Contributing Guide](docs/contributing.md)

## License

Apache 2.0 — see [LICENSE](LICENSE)
