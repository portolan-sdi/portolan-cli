# Publish and analyze Philadelphia housing data

Use this tutorial to publish two Philadelphia ArcGIS layers as a Portolan Catalog.
Then run a spatial join directly against the GeoParquet Assets.

The workflow uses these Collections:

- Affordable Housing Production contains 501 projects in the pinned August 27, 2026 snapshot.
- City Council Districts contains the ten districts drawn after the 2020 census.

For the complete ten-Collection example, see the [published Philadelphia housing Catalog](https://source.coop/nlebovits/phl-housing-demo).

## What the workflow does

The script runs the shipped Portolan CLI from source to published files:

1. Search Philadelphia's ArcGIS services root and select two named services.
2. Download each FeatureServer layer in parallel, with pagination and retries.
3. Convert the features to spatially ordered GeoParquet with bbox statistics.
4. Preserve each ArcGIS renderer as a MapLibre style.
5. Add publisher, license, contact, and source metadata.
6. Generate thumbnails, `README.md` files, and `AGENTS.md` files.
7. Run the Portolan validator in strict mode.
8. Optionally push the static Catalog to S3-compatible storage.

The Catalog does not need a database, API, or running Portolan service after publication.

## Before you begin

Clone this repository and install its dependencies:

```sh
uv sync --all-extras
```

The default workflow reads public Philadelphia ArcGIS endpoints.
The City of Philadelphia's [metadata terms](https://metadata.phila.gov/#help/help-faqs/what-are-the-terms-of-use/) apply.

## Build the Catalog

Run these commands from the repository root:

```sh
export CATALOG_DIR=/tmp/philadelphia-housing
uv run ./examples/philadelphia-housing/run.sh
```

`CATALOG_DIR` must identify a new directory.
The command creates two Collections and stops if extraction or validation fails.

The Affordable Housing Production service name becomes the Collection ID `affordablehousingproduction`.
The City Council Collection ID is `council_districts_2024`.

## Query the GeoParquet Assets

Run the included DuckDB analysis against the local Catalog:

```sh
uv run python examples/philadelphia-housing/query.py "$CATALOG_DIR"
```

The pinned August 27, 2026 snapshot returns:

```text
district  projects  units
       1        37   2310
       2        62   1886
       3        97   3724
       4        36   1172
       5       119   4774
       6         9    264
       7        44   1915
       8        53   1796
       9         9    310
      10        10    433

Located projects: 476
Located units: 18,584
Projects without geometry: 25
Units without geometry: 665
```

The join places 476 projects and 18,584 units in City Council districts.
It also identifies 25 projects that lack geometry.
Those projects account for 665 units and cannot be assigned by this spatial join.

Philadelphia can update the live ArcGIS layers.
Your live result can differ from the pinned CI result.

## Publish and query over HTTP

Set an object-storage destination before you run the workflow:

```sh
export CATALOG_DIR=/tmp/philadelphia-housing-published
export PORTOLAN_PHL_REMOTE=s3://my-bucket/philadelphia-housing
uv run ./examples/philadelphia-housing/run.sh
```

Use the credentials required by your storage provider.
For S3-compatible storage, set `PORTOLAN_S3_ENDPOINT` and `PORTOLAN_S3_USE_SSL`.
Do not store credentials in `.portolan/config.yaml`.

After you make the files public, pass their HTTP base URL to the same analysis:

```sh
uv run python examples/philadelphia-housing/query.py \
  https://data.example.org/philadelphia-housing
```

DuckDB reads the required Parquet ranges from object storage.
It does not download or clone the complete Catalog first.

## How CI verifies the tutorial

CI runs this exact script against a deterministic replay of the two ArcGIS services.
The replay uses the two GeoParquet Assets from the published Philadelphia Catalog.
It advertises a 100-feature page limit and returns one temporary HTTP 503 response.

The test verifies all of these results:

- The service filter excludes an unrelated service.
- Affordable Housing Production requires six pages.
- The extractor retries the temporary failure.
- Both Collections include real row counts, styles, thumbnails, documentation, source links, and bbox statistics.
- Strict Portolan validation passes.
- The Catalog publishes to MinIO.
- The ArcGIS replay stops before DuckDB queries the published Assets over HTTP.
- The district totals match the table in this tutorial.

This test keeps extraction, publication, and analysis in one CI contract.

## Workflow source

The test suite executes this script without rewriting it:

```sh
--8<-- "examples/philadelphia-housing/run.sh"
```

The test suite also executes this analysis:

```python
--8<-- "examples/philadelphia-housing/query.py"
```
