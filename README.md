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

### Bootstrap & Roundtrip Example

```bash
# Initialize a catalog
portolan init

# Add files (creates collections from directories)
portolan add demographics/

# Validate and convert to cloud-native formats
portolan check --fix

# notest - requires S3 credentials
# Push to remote storage
portolan push s3://my-bucket/catalog --collection demographics

# Later: pull updates from remote
portolan pull s3://my-bucket/catalog -c demographics

# Or sync everything (pull → init → scan → check → push)
portolan sync s3://my-bucket/catalog -c demographics
```

### All Commands

```bash
portolan init                               # Initialize catalog
portolan scan <path>                        # Scan for issues (--fix for filenames)
portolan add <path>                         # Track files
portolan rm <path>                          # Untrack files (--keep to preserve data)
portolan check                              # Validate catalog (metadata + geo-assets)
portolan check --fix                        # Convert to cloud-native + update metadata
portolan list                               # List tracked files
portolan info <path>                        # Show file/collection info
portolan push <remote>                      # Push to remote storage
portolan pull <remote>                      # Pull from remote storage
portolan sync <remote>                      # Full roundtrip sync
portolan clone <remote> <path>              # Clone remote catalog
portolan config <set|get|list|unset>        # Manage configuration
portolan clean                              # Remove Portolan metadata
```
<!-- --8<-- [end:quickstart] -->

<!-- --8<-- [start:installation] -->
## Installation

```bash
# Recommended: uv (fast, isolated, project-aware)
uv tool install portolan-cli

# Alternative: pipx (isolated environment)
# pipx install portolan-cli

# Alternative: pip (global/user site-packages, may conflict)
# pip install portolan-cli
```

### For Development

```bash
git clone https://github.com/portolan-sdi/portolan-cli.git
cd portolan-cli
uv sync --all-extras
uv run portolan --help
```
<!-- --8<-- [end:installation] -->

See [Contributing Guide](docs/contributing.md) for full development setup.

## License

Apache 2.0 — see [LICENSE](LICENSE)
