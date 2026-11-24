# Iceberg

Portolan publishes cloud-native files under STAC. The Iceberg storage layer adds an
Apache Iceberg table per collection, in an Iceberg REST catalog you already
run, for teams that want ACID commits, concurrent writers, and version
history as table snapshots.

DuckDB writes the tables. Portolan talks to the catalog over its REST API for
the parts DuckDB has no statement for: the version record, rollback, and
snapshot expiry. You install no extra package.

## When to use it

Use the Iceberg storage layer when all of these are true.

- You have an Iceberg REST catalog with an S3-compatible object store. The
  catalog is hosted or your own.
- You want ACID commits and concurrent writers on a table.
- You want version history as Iceberg snapshots, with rollback and expiry.
- You accept a second copy of the rows, inside the warehouse.

Use the default file storage otherwise. It writes `versions.json`, needs no
server, and every other part of Portolan assumes it.

## Point the CLI at the catalog

The CLI reads the catalog settings from environment variables, so a secret never
lands in a file the catalog publishes.

| Variable | Meaning | Default |
|----------|---------|---------|
| `PORTOLAN_ICEBERG_URI` | The REST catalog, such as `https://catalog.example.org`. Required. | |
| `PORTOLAN_ICEBERG_WAREHOUSE` | The warehouse name the catalog serves. The catalog's documentation gives it. | unset |
| `PORTOLAN_ICEBERG_NAMESPACE` | The namespace of the tables. Each collection gets one table. | `portolake` |
| `PORTOLAN_ICEBERG_AUTH` | `oauth2`, `bearer`, or `none`. | inferred from the credential |
| `PORTOLAN_ICEBERG_CLIENT_ID`, `PORTOLAN_ICEBERG_CLIENT_SECRET` | The OAuth2 client credentials. | |
| `PORTOLAN_ICEBERG_OAUTH2_SERVER_URI` | The token endpoint. | `<uri>/v1/oauth/tokens` |
| `PORTOLAN_ICEBERG_TOKEN` | A bearer token an identity provider minted. | |
| `PORTOLAN_ICEBERG_STORAGE` | `s3`, `vended` (the catalog hands out the storage credential), or `gcs`. | `s3` |

The S3 settings are the ones `portolan push` uses: `PORTOLAN_S3_ENDPOINT`,
`PORTOLAN_S3_USE_SSL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_REGION`, or an AWS profile. A hosted catalog that issues one key pair for
the catalog and the store takes it twice: as the client pair and as the AWS
pair.

A catalog with OAuth2 and a path-style S3 endpoint:

```bash
export PORTOLAN_ICEBERG_URI=https://catalog.example.org
export PORTOLAN_ICEBERG_WAREHOUSE=my-warehouse
export PORTOLAN_ICEBERG_CLIENT_ID=...
export PORTOLAN_ICEBERG_CLIENT_SECRET=...
export PORTOLAN_S3_ENDPOINT=s3.example.org
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
```

The compose stack under `tests/iceberg/e2e/` is a local catalog for
development: an Apache REST fixture on port 8181 and RustFS on port 9000.

```bash
docker compose -f tests/iceberg/e2e/docker-compose.yml up -d --wait
export PORTOLAN_ICEBERG_URI=http://localhost:8181
export PORTOLAN_S3_ENDPOINT=localhost:9000 PORTOLAN_S3_USE_SSL=false
export AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin AWS_REGION=us-east-1
```

## Create a catalog that uses it

```bash
portolan init my-catalog --backend iceberg --license CC0-1.0
```

`.portolan/config.yaml` then records `backend: iceberg`. The command reads the
environment and connects to nothing. A missing `PORTOLAN_ICEBERG_URI` stops it
with the variable named.

## Add data

Put the file in a subdirectory named after the collection. `add` reads the
collection from that directory, and rejects a file at the catalog root.

```bash
mkdir buildings
cp ~/data/boston-open-space.parquet buildings/
portolan add buildings/boston-open-space.parquet
```

The rows go into the table `portolake.buildings`, created at Iceberg format
version 3. The geometry column keeps the CRS of the GeoParquet file, as
`geometry(EPSG:25830)` for a projected file. A CRS84 file gets the default
`geometry`.

Each `add` replaces the rows of the table with the rows of every Parquet data
asset the collection holds, so a second `add` of the same file keeps one row
per feature. A companion such as `items.parquet` or a thumbnail is tracked as
an asset and never becomes rows.

A collection with `partition:keys` in its `collection.json` gets a table
partitioned by identity on those columns, and the rows read from the
`key=value` directories.

The GeoParquet file stays where it is, as its own asset on the collection. The
table is a second copy of the rows, inside the warehouse.

## Version record

Version `1.2.0` of a collection has two parts:

- The table property `portolan.version.1.2.0`. Its value records the assets,
  the schema, and the message.
- The tag `v1.2.0`. It points at the snapshot with the rows of that version.

Portolan writes the property and the tag in one commit after the rows. A hosted catalog's maintenance expires untagged snapshots after a
window, so the tag is what keeps a version alive.

## Read the table

Any Iceberg client reads the table. `collection.json` names the current
metadata file in `iceberg:metadata_location`, and a serverless reader opens it
with DuckDB. The metadata file lists its data files as `s3://` paths, so the
reader tells DuckDB which endpoint serves that bucket. A public catalog needs
no key, and an unsigned request is enough:

```sql
INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
CREATE SECRET store (TYPE s3, ENDPOINT 's3.example.org', URL_STYLE 'path', REGION 'us-east-1');
SELECT count(*) FROM iceberg_scan('https://s3.example.org/my-warehouse/portolake/buildings/metadata/00003-....metadata.json');
```

A catalog's maintenance rewrites the metadata file after a while, so the URL
written at `add` time names an older file. That file still answers, and it
reads the version `add` published. A public catalog also answers reads by
name through its public endpoint, with no credential:

```sql
ATTACH 'handle/my-catalog' AS lake (TYPE iceberg, ENDPOINT 'https://public-catalog.example.org', AUTHORIZATION_TYPE 'none');
SELECT count(*) FROM lake.portolake.buildings;
```

Through the catalog, with the same variables the CLI uses:

```sql
CREATE SECRET cat (TYPE iceberg, CLIENT_ID '...', CLIENT_SECRET '...', OAUTH2_SERVER_URI 'https://catalog.example.org/v1/oauth/tokens');
ATTACH 'my-warehouse' AS lake (TYPE iceberg, ENDPOINT 'https://catalog.example.org', SECRET cat);
SELECT count(*) FROM lake.portolake.buildings;
```

## Version commands

`portolan version current` and `portolan version list` work with every storage
layer. The commands below need the Iceberg table, so they work only here:

```bash
portolan version rollback buildings 1.2.0
portolan version prune buildings --keep 3
```

Rollback moves the table's `main` branch to the tagged snapshot. No row is
copied. The next `add` continues from the newest version number.

Prune removes the records, the tags, and the snapshots of the older versions,
keeps the `--keep` newest and the current one. Then it deletes each data file
that the remaining snapshots do not use.

The maintenance of a hosted catalog writes a new metadata file. Refresh
writes the current `iceberg:metadata_location` and the other Iceberg fields
into `collection.json`. With a remote configured, it also uploads
`collection.json`:

```bash
portolan version refresh buildings
```

## Iceberg fields on the collection

`add` writes these onto `collection.json`:

```json
"iceberg:catalog_type": "rest",
"iceberg:catalog_uri": "https://catalog.example.org",
"iceberg:rest_prefix": "my-warehouse",
"iceberg:authorization_type": "oauth2",
"iceberg:table_id": "portolake.buildings",
"iceberg:format_version": 3,
"iceberg:metadata_location": "https://s3.example.org/my-warehouse/portolake/buildings/metadata/00003-....metadata.json",
"iceberg:current_snapshot_id": "3024701191045327257"
```

They follow the
[STAC Iceberg extension](https://github.com/portolan-sdi/stac-iceberg-extension).
When the object store serves the metadata file over HTTPS, the collection also
carries an `iceberg` asset with that URL, so a reader with no catalog
credential opens the table from the file.

## Limits

`portolan push` stops here. `add` has already written to the warehouse, so no
local tree remains to upload:

```
Push is not supported with the 'iceberg' backend. The iceberg backend manages versions through its catalog.
```

A browser and a plain HTTP client read the GeoParquet file beside the
collection. Neither reads the warehouse.

DuckDB writes merge-on-read: a replaced data file stays in the warehouse,
hidden by a deletion vector, and the current snapshot still lists it. Prune
removes versions from the catalog. It deletes a data file only when no
remaining snapshot uses it. Prune frees no space until the maintenance
of the catalog compacts the table.

A table that the earlier PyIceberg-based Portolan wrote (format version 2, records
in the snapshot summaries) is read-only. `version list` and `version current`
read it; `add`, `rollback`, and `prune` refuse it. Publish the collection again
to get a format version 3 table.

`add` refuses a column whose type changes between two adds, because the
table keeps its history. `add` appends a new column to the table.

`portolan check` reports no drift between the collection and the table. No
check compares them yet.
