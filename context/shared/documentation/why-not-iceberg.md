# Why Iceberg is an optional extra

Portolan's catalog layer is STAC. Apache Iceberg support ships as the optional
`[iceberg]` extra and never becomes the catalog layer. This note records the
reasoning, which the packaging decision alone does not convey.

## The problems Iceberg solves are not the problems Portolan has

Iceberg handles concurrent writes, ACID transactions, and schema evolution on
petabyte tables. Portolan users publish static datasets and need none of that.

Iceberg also requires a catalog server, a REST API at minimum. Portolan's value
is static files on object storage with no running services, which puts hosting
around $5 a month. Adding a required server inverts that.

STAC already provides the manifest Portolan needs: here are the files, here is
what they contain, here is their spatial extent.

## Governance exposure

Databricks acquired Tabular, whose founders created Iceberg. The specification
is shaped by warehouse vendors, and the catalog layer is an active commercial
battleground. Tying the catalog layer to that spec means inheriting its
politics.

## Geospatial support was not ready

Iceberg V3 geometry types exist in the specification but not in the tooling.
Neither DuckDB's Iceberg extension nor PyIceberg supported them at the time of
the decision, and the specification deferred edge cases to V4.

Revisit this if the tooling matures and a catalog-free mode appears. The extra
can be promoted; the decision does not foreclose that.

## Interoperability does not require coupling

GeoParquet and COG files are identical regardless of catalog layer. Registering
Portolan data in an existing Iceberg catalog works today. That is an integration
use case, which the extra serves, not an argument for replacing STAC.

## What this costs

Users running Iceberg-native stacks install the extra rather than getting
integration by default. See `docs/guides/iceberg.md` for that path, and
`portolan_cli/backends/` for the protocol the extra implements.
