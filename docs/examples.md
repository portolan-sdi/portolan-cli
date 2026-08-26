# Executable Examples

These scripts are the command source for Portolan workflows. The documentation test suite runs the local script with a committed GeoJSON fixture. Documentation CI also runs the storage script against MinIO.

## Local Publishing

Set `CATALOG_DIR` to a new directory and `PORTOLAN_EXAMPLE_SOURCE` to a GeoJSON file. Run the script from the repository root.

```sh
--8<-- "examples/local-publishing.sh"
```

The script creates a `places` collection and checks its geospatial assets.

## MinIO Storage Round Trip

Set the two catalog paths, source path, MinIO endpoint, and MinIO credentials in the environment. The script creates the source catalog, pushes it to `s3://portolan-docs/catalog`, and clones it into `CLONED_CATALOG_DIR`.

```sh
--8<-- "examples/minio-round-trip.sh"
```

The endpoint and credentials stay outside the catalog files. This also applies to other S3-compatible storage.
