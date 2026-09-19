# Why Iceberg is an optional extra

Portolan's catalog layer is STAC. Apache Iceberg support is packaged as the
optional `[iceberg]` extra and never becomes the catalog layer. It gives Iceberg-capable
engines a second way to read the same published files. This note records the
reasoning, which the packaging decision alone does not convey.

## Portolan needs Iceberg's read side, not its write side

Iceberg's write side handles concurrent writers, ACID commits, and schema
evolution on petabyte tables. Portolan users publish static datasets and need
none of that, so they run no catalog server.

Its read side solves problems a glob over partitioned GeoParquet has. A manifest
names every file, so a public catalog works over plain HTTPS where a glob needs
a bucket listing. Per-file bounds, including geometry bounds in format version
3, let an engine skip files without opening their footers. One `ATTACH` exposes
every collection as a table in one SQL namespace, joinable with each other and
with other catalogs. Spark, Trino, BigQuery and Snowflake read that; none reads
a STAC record that points at a glob.

The gain is smallest for a single-file collection, where `read_parquet` on the
asset is already one request, and absent in a browser, where PMTiles and direct
GeoParquet remain the path. STAC stays the catalog: it says what the data is,
who published it, and how to draw it. Iceberg says how to query it.

## An open specification

Apache Iceberg is an open specification, an Apache Software Foundation project
under the Apache-2.0 licence, like Portolan. STAC remains the catalog layer
for the technical reasons above.

## A reader needs no server

An Iceberg table is readable from its `metadata.json` alone. DuckDB
`iceberg_scan` and pyiceberg `StaticTable` open it with no catalog service, and
the STAC Iceberg extension names this pattern `catalog_type: static`. A static
Iceberg REST surface, a tree of JSON files under `v1/`, lets any Iceberg client
`ATTACH` the whole catalog the same way. Hosting remains static files on
object storage.

## Two modes

The extra provides a lakehouse, `portolan init --backend iceberg`, for teams
that already run a catalog server such as REST, Glue, Hive, or BigLake.
It copies data into catalog-managed tables and keeps version history in
snapshots. That mode is outside the spec convention.

The static form is the spec convention and the simpler alternative. One Iceberg
table per collection sits over the published GeoParquet, with no data rewrite.
The collection gains a `metadata` asset, and publish time emits the REST
surface. STAC JSON remains the source of truth. The static form is a committed
decision and is not yet built; the Iceberg alignment plan tracks it.

## Geospatial support has arrived, unevenly

Iceberg format version 3 carries native geometry and geography types with a
CRS. DuckDB 1.5 reads and writes them. pyiceberg 0.12 defines and reads them but
cannot write a v3 table yet, so the static writer patches the format version and
the geometry type into the metadata it emits until that release. Spark and Trino
read v3 geometry from Iceberg Java 1.12.

## Interoperability does not require coupling

GeoParquet and COG files are identical regardless of catalog layer. A GeoParquet
2.0 file already satisfies Iceberg's Parquet mapping, so an Iceberg table can
reference the published file in place. That is an integration use case, which
the extra serves, not an argument for replacing STAC.

## What this costs

Users running Iceberg-native stacks install the extra rather than getting
integration by default. A collection that opts in grows by a metadata directory
of a few kilobytes per snapshot. See `portolan_cli/backends/` for the protocol
the lakehouse backend implements, and `docs/guides/iceberg.md` for the user
guide to it. The guide gains the static mode when that writer exists.
