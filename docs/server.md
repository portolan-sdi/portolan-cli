# Registry And Server Commands

Portolan catalogs publish as static files first.
That remains the canonical output.
The registry and server commands add two follow-on workflows:

- Find or fetch a published catalog from the Portolan registry.
- Publish catalog resources into a geospatial server when an existing stack needs them.

These commands do not change the Portolan catalog format.
The Portolan specification remains the source of truth for catalog conformance.

## Registry

The registry commands read the public Portolan registry export.
By default, they include only entries whose registry status is `valid`.

List published catalog entries:

```sh
portolan registry list
```

Limit the list when you only need a small sample:

```sh
portolan registry list --limit 10
```

Fetch one catalog snapshot:

```sh
portolan registry fetch CATALOG_ID --output ./catalogs
```

Fetch all valid catalog snapshots:

```sh
portolan registry fetch --all --output ./catalogs
```

`registry fetch` writes a local copy of the STAC catalog documents.
For collection assets, it rewrites relative asset `href` values to absolute URLs.
That makes the snapshot useful for server commands without copying the data assets.

Use `--registry-url` to read a different registry export.
Use `--include-stale` when a workflow must inspect stale entries.
Use `--json` for structured output.

## GeoServer

The `server geoserver` commands publish a Portolan catalog into GeoServer.
They currently support GeoParquet vector resources and COG raster resources.
PMTiles resources are detected by the generic planner, but the GeoServer provider skips them.

Show the planned changes:

```sh
portolan server geoserver plan ./catalog
```

Publish supported resources:

```sh
portolan server geoserver publish ./catalog
```

Reconcile GeoServer with the current catalog:

```sh
portolan server geoserver sync ./catalog
```

`sync` currently matches idempotent publish behavior.
It does not remove GeoServer resources that no longer appear in the catalog.

### Credentials

Pass GeoServer connection settings as flags:

```sh
portolan server geoserver plan ./catalog \
  --url https://geoserver.example.org/geoserver/cloud \
  --user admin \
  --password "$GEOSERVER_PASSWORD" \
  --workspace city-catalog
```

The same values can come from environment variables:

```sh
export PORTOLAN_GEOSERVER_URL=https://geoserver.example.org/geoserver/cloud
export PORTOLAN_GEOSERVER_USER=admin
export PORTOLAN_GEOSERVER_PASSWORD="$GEOSERVER_PASSWORD"
export PORTOLAN_GEOSERVER_WORKSPACE=city-catalog

portolan server geoserver plan ./catalog
```

Do not store credentials in `.portolan/config.yaml`.
Use CLI flags, environment variables, or a catalog-local `.env` file.

## Provider Design

Server publication has a provider boundary.
`portolan_cli/server/planner.py` reads the STAC catalog and emits generic
resource candidates.
Those candidates describe the desired state with a resource type, format,
asset URL, metadata, and collection provenance.

`portolan_cli/server/providers/base.py` defines the provider protocol.
Each provider implements `plan`, `publish`, and `sync`.
The GeoServer provider maps generic resources to GeoServer REST calls.
It also writes `portolan.*` metadata to each created GeoServer resource.
Later plans use that metadata to decide whether a resource already exists or
needs an update.

This structure lets another provider reuse the catalog loader and plan model.
A MapServer or Esri provider would add code under `server/providers/<provider>/`
and translate the same generic resources into that server's API or files.
That provider can support only the resource formats that its target server can serve.
