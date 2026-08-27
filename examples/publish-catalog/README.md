# Publish and Clone a Catalog

This example builds one complete Portolan Catalog from a committed GeoParquet Asset.
The same script can publish the Catalog and verify a clean clone.

## What This Example Proves

The script uses the shipped CLI to:

1. Create a Catalog with an SPDX license.
2. Apply human-authored title, provider, contact, and provenance metadata.
3. Add a GeoParquet Asset to the `places` Collection.
4. Create a thumbnail and Collection version history.
5. Generate Catalog and Collection READMEs.
6. Run the strict Portolan conformance check.
7. Show Collection details and the current version.
8. Optionally push the Catalog, clone it, and check the clone.

The test suite inspects the generated STAC metadata, Asset checksums, thumbnail, providers, and provenance.

## Run It Locally

Run these commands from the repository root:

```sh
uv sync --all-extras
export CATALOG_DIR=/tmp/portolan-example
export PORTOLAN_EXAMPLE_SOURCE=$PWD/examples/publish-catalog/points.parquet
uv run ./examples/publish-catalog/run.sh
```

`CATALOG_DIR` must name a new directory.
The script exits when any command or conformance assertion fails.

## Publish and Clone

Set the remote and clone paths before the first run:

```sh
export CATALOG_DIR=/tmp/portolan-publish-example
export CLONED_CATALOG_DIR=/tmp/portolan-publish-example-clone
export PORTOLAN_EXAMPLE_SOURCE=$PWD/examples/publish-catalog/points.parquet
export PORTOLAN_EXAMPLE_REMOTE=s3://my-bucket/portolan-example
uv run ./examples/publish-catalog/run.sh
```

Use the credentials required by your storage provider.
Portolan supports S3, GCS, Azure, and S3-compatible storage.

For S3-compatible storage, set `PORTOLAN_S3_ENDPOINT` and `PORTOLAN_S3_USE_SSL` in the process environment.
Do not put credentials in `.portolan/config.yaml`.

## How CI Uses MinIO

CI supplies a temporary MinIO service for the S3-compatible path.
MinIO does not appear in the Catalog or the public workflow.
The test compares the source and cloned Asset checksums, then checks the clone in strict mode.

Every pull request also provides the complete built documentation as a workflow artifact.

## Workflow Source

```sh
--8<-- "examples/publish-catalog/run.sh"
```

The workflow reads this metadata before it creates the Collection:

```yaml
--8<-- "examples/publish-catalog/metadata.yaml"
```

## Add Another Example

Create a sibling directory under `examples/` with its script, metadata, and small fixture.
Add a test that executes the exact public script and inspects its output.
The documentation CI job provides the shared object-storage service and site build.
