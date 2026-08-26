# Portolan CLI

Portolan CLI helps GIS publishers build geospatial catalogs as plain files in their own storage. It creates structured STAC metadata, tracks catalog versions, and publishes files to object storage.

The [Portolan standard](https://github.com/portolan-sdi/portolan-spec) defines catalog requirements. This CLI builds catalogs that you can check with the Portolan validator.

## Install

Install the CLI with uv:

```sh
uv tool install portolan-cli
```

See [the project README](https://github.com/portolan-sdi/portolan-cli) for development setup.

## Quick Start

Create a catalog with an SPDX license identifier:

```sh
portolan init my-catalog --auto --license CC-BY-4.0
```

The [local publishing example](https://github.com/portolan-sdi/portolan-cli/blob/main/docs/examples.md#local-publishing) creates a catalog from a committed GeoJSON file. It checks the generated catalog artifacts.

## Executable Workflows

The checked-in scripts are the command source for both workflows.

- [Local publishing](https://github.com/portolan-sdi/portolan-cli/blob/main/docs/examples.md#local-publishing) creates a catalog on local storage.
- [MinIO storage round trip](https://github.com/portolan-sdi/portolan-cli/blob/main/docs/examples.md#minio-storage-round-trip) pushes a catalog to S3-compatible storage, then clones it.

Use the following sensitive settings only in the process environment or a catalog `.env` file. Do not add them to `.portolan/config.yaml`.

- `PORTOLAN_S3_ENDPOINT`: S3-compatible endpoint, such as a MinIO host and port.
- `PORTOLAN_S3_USE_SSL`: `true` for HTTPS or `false` for HTTP. The default is `true`.
- `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`: storage credentials.

## Reference

- [CLI reference](https://github.com/portolan-sdi/portolan-cli/blob/main/docs/reference/cli.md) is generated from the shipped Click command tree.
- [Python API reference](https://github.com/portolan-sdi/portolan-cli/blob/main/docs/reference/python.md) is generated from supported package exports.
- [Contributing guide](https://github.com/portolan-sdi/portolan-cli/blob/main/docs/contributing.md) explains development and review requirements.
- [Changelog](https://github.com/portolan-sdi/portolan-cli/blob/main/docs/changelog.md) records released changes.

## License

Portolan CLI is licensed under Apache-2.0.
