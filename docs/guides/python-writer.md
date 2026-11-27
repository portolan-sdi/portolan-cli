# Write a catalog from Python

The `portolan-core` package publishes a Portolan catalog from your own Python
code. Install it with `pip install portolan-core`, which leaves out the CLI and
the geospatial stack. The writer saves the STAC documents and Markdown files
that you supply. To build those documents from your data files, see
[Build STAC documents from data](#build-stac-documents-from-data), which needs
`portolan-cli`. To also convert the data, use `portolan add`.

For each name and parameter, see the
[catalog writer reference](../reference/python.md).

## Plan the catalog

Pass `plan_catalog` your STAC documents and the text of each `README.md` and
`AGENTS.md`, keyed by path from the catalog root:

```python
--8<-- "examples/python-writer/write_catalog.py:plan"
```

The planner copies your documents and adds the links, titles, and profile URI
that the spec requires. The writes come later, when you call `apply_plan`.

## Inspect the plan

A plan is plain data, a list of put and delete operations. You can print it
before a write or assert on it in a test:

```python
--8<-- "examples/python-writer/write_catalog.py:inspect"
```

## Write to a folder

`LocalSink` writes the plan into a folder:

```python
--8<-- "examples/python-writer/write_catalog.py:local"
```

The writer saves only the catalog files. Copy each asset to its `href`
yourself, which for this example means `city-catalog/parks/parks.parquet` and
`city-catalog/parks/thumbnail.png`.

## Write to an object store

`ObstoreSink` accepts any [obstore](https://developmentseed.org/obstore/)
store, such as `S3Store`, `GCSStore`, or `AzureStore`. This example uses an
in-memory store:

```python
--8<-- "examples/python-writer/write_catalog.py:store"
```

For other storage, write a class with the `put` and `delete` methods of the
`Sink` protocol.

## Remove files from an earlier publish

The writer never lists your storage. To delete the files that a catalog no
longer needs, pass the keys of your last plan as `prior_managed_keys`. In this
example, the second plan drops the parks collection:

```python
--8<-- "examples/python-writer/write_catalog.py:update"
```

Each prior key that the new plan leaves out becomes a delete. Asset files stay
in place, so remove those yourself.

On Source Cooperative, use the repository as the bucket. Put the account in
the endpoint, such as `https://data.source.coop/my-account`. With the account as
the bucket, puts succeed but deletes fail with `NoSuchBucket`. `ObstoreSink`
raises that error and does not ignore it.

## Build STAC documents from data

The STAC builders in `portolan_cli.api` make the Items and Collections for
`plan_catalog` from your data files. They need no catalog folder, so a job
that writes straight to object storage can use them. Install them with
`pip install portolan-cli`.

### Build the Items

This example reads each GeoParquet file in a folder and makes one Item for
each file:

```python
--8<-- "examples/python-writer/build_item.py:items"
```

STAC requires a WGS84 `bbox`. The `bbox` that
`extract_geoparquet_metadata` reads is in the CRS of the file. `wgs84_bbox`
measures the geometry in the file and returns its WGS84 extent.
`create_item` raises `portolan_core.errors.InvalidBboxError` for a `bbox`
outside WGS84.
`add_projection_extension` keeps the native CRS and `bbox` as `proj:code` and
`proj:bbox`.

### Build the Collection

```python
--8<-- "examples/python-writer/build_item.py:collection"
```

Each collection needs at least one provider with the `producer` role and a
thumbnail asset.

### Write the catalog

Convert each object with
`to_dict(include_self_link=False, transform_hrefs=False)` and give the dicts to
`plan_catalog`:

```python
--8<-- "examples/python-writer/build_item.py:write"
```

`transform_hrefs=False` keeps each asset `href` as you set it. A relative
`href` resolves from the folder of its Item, and an absolute URL stays as it
is. The writer sets the `root`, `parent`, `child`, `item`, and `collection`
links, so leave those out. Copy each asset to its `href` yourself, for example
`wetlands-catalog/wetlands/dc-2023/dc-2023.parquet` and
`wetlands-catalog/wetlands/thumbnail.png`.
