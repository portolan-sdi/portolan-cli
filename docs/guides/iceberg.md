# Iceberg

Portolan publishes cloud-native files under STAC. The optional `[iceberg]` extra
adds an Apache Iceberg lakehouse, for teams that already run an Iceberg catalog
server and want Portolan to write into it.

This page covers that lakehouse, the Iceberg mode the CLI provides today.
A second mode is planned. See [Modes](#modes).

## When to use it

Reach for the lakehouse when all of these hold.

- You already run an Iceberg catalog server: REST, Glue, Hive, SQL, or BigLake.
- You want ACID commits and concurrent writers on a table.
- You want version history as Iceberg snapshots, with rollback and expiry.
- You accept a second copy of the rows, inside the warehouse.

Use the default file storage otherwise. It writes `versions.json`, needs no
server, and every other part of Portolan assumes it.

## Install

```bash
pip install portolan-cli[iceberg]
```

The extra brings PyIceberg and a SQLite catalog driver. Without it, every
Iceberg command stops with an install hint.

## Create a catalog that uses it

```bash
portolan init my-catalog --backend iceberg --license CC0-1.0
```

`.portolan/config.yaml` then records `backend: iceberg`. The default catalog is
local SQLite at `.portolan/iceberg.db`, with a warehouse under
`.portolan/warehouse`, so no data leaves the machine.

To use a catalog server, set the standard PyIceberg variables before you run the
CLI. The CLI loads the catalog named `portolake`:

```bash
export PYICEBERG_CATALOG__PORTOLAKE__TYPE=rest
export PYICEBERG_CATALOG__PORTOLAKE__URI=https://catalog.example.com
export PYICEBERG_CATALOG__PORTOLAKE__WAREHOUSE=s3://my-bucket/warehouse
```

A `~/.pyiceberg.yaml` file works too. PyIceberg reads the first one it finds in
`PYICEBERG_HOME`, then your home directory, then the working directory. See the
[PyIceberg configuration reference](https://py.iceberg.apache.org/configuration/).

## Add data

Put the file in a subdirectory named after the collection. `add` reads the
collection from that directory, and rejects a file at the catalog root.

```bash
mkdir buildings
cp ~/data/boston-open-space.parquet buildings/
portolan add buildings/boston-open-space.parquet
```

The rows go into the Iceberg table `portolake.buildings`, as one commit. The
commit records the Portolan version string in its snapshot summary, so version
history and table history are one history.

`add` also derives spatial helper columns and, above 100,000 rows, partitions
the table by identity on a geohash column. Those columns are an internal storage
layout, and `table:columns` in the STAC output leaves them out.

The GeoParquet file stays where it is, as its own asset on the collection. The
table is a second copy of the rows, inside the warehouse.

## Read the table

Any Iceberg client reads the table. The local warehouse writes one numbered
metadata file per commit, under
`.portolan/warehouse/portolake/<collection>/metadata/`. The highest number is
the current one, and `iceberg:metadata_location` on the collection names it.

```sql
INSTALL iceberg; LOAD iceberg;
SELECT count(*) FROM iceberg_scan(
  '.portolan/warehouse/portolake/buildings/metadata/00001-d1c5ba5b-a0fd-4053-98fc-ee20adf2dfd9.metadata.json'
);
-- 1012
```

PyIceberg reads it through the catalog:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("portolake")
table = catalog.load_table("portolake.buildings")
print(table.scan().to_arrow().num_rows)
```

## Version commands

`portolan version current` and `portolan version list` work with every storage
layer. These two need Iceberg snapshots, so they work only here:

```bash
portolan version rollback buildings 1.2.0
portolan version prune buildings --keep 3
```

## Iceberg fields on the collection

`add` writes these onto `collection.json`:

```json
"iceberg:catalog_type": "sql",
"iceberg:table_id": "portolake.buildings",
"iceberg:catalog_uri": "sqlite:///.../.portolan/iceberg.db",
"iceberg:format_version": 2,
"iceberg:current_snapshot_id": "3024701191045327257"
```

They follow the
[STAC Iceberg extension](https://github.com/portolan-sdi/stac-iceberg-extension).
A reader opens the table through them, because a catalog server resolves the
current metadata at read time and publishes no fixed path to put in an asset.

## Limits

`portolan push` stops here. `add` has already written to the warehouse, so no
local tree remains to upload:

```
Push is not supported with the 'iceberg' backend.
The iceberg backend manages versions through its catalog.
```

A browser and a plain HTTP client read the GeoParquet file beside the
collection. Neither reads the warehouse.

Engine support for format version 3 geometry still varies. DuckDB reads
`geometry` and answers "Geography support: not implemented" for `geography`.
Spark and Trino need a catalog server.

PyIceberg 0.12 writes a quoted CRS in a geometry type string and parses only the
quoted form, so a table it writes and a table DuckDB writes are not
interchangeable.

`check_drift` is a stub and reports nothing.

## Modes

The lakehouse above stores the rows itself, and you select it at `init`.

A second mode is planned and not built: a static Iceberg table over the
GeoParquet a catalog already publishes. It copies no rows, needs no server, and
leaves the STAC `data` asset alone. The Portolan specification describes that
mode, under
[Iceberg](https://github.com/portolan-sdi/portolan-spec/blob/main/specs/incubating/iceberg.md).

The reasoning behind both is recorded in
[`context/shared/documentation/iceberg-as-optional-extra.md`](https://github.com/portolan-sdi/portolan-cli/blob/main/context/shared/documentation/iceberg-as-optional-extra.md).
