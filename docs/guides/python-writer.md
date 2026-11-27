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

## What the planner does

The names in `portolan_core.writer.__all__` are the supported interface.
`PutOperation`, `DeleteOperation`, and `WritePlan` are frozen dataclasses of
plain values, so `dataclasses.asdict` gives plain data. Any object with the
`put(key, content, media_type)` and `delete(key)` methods is a `Sink`.

The planner changes your documents as follows:

- It adds the `root`, `parent`, `collection`, `child`, and `item` links. Each
  `child` and `item` link gets the title of its target, or the `id` of an item
  that has no `properties.title`. It removes each relative structural link that
  you supply. An absolute structural link, such as a `child` link to another
  host, stays.
- It sets the `collection` field of each item, as STAC 1.1.0 requires.
- It adds an `agents` link to `./AGENTS.md` and a `describedby` link to
  `./README.md` on each catalog and collection. If your link has the same rel
  and file, the planner keeps its other fields, such as `title` and `hreflang`.
  All other links stay after these links.
- It adds the newest Portolan profile URI to a catalog or collection that
  declares no profile.
- It writes no `self` link. STAC requires an absolute `self` link, and the
  planner does not know the URL where you publish.

The planner does not do these things:

- It does not read storage or asset files. It writes only the catalog JSON and
  the Markdown files. Each asset stays as you supply it, so a directory asset,
  such as a Zarr store, keeps your `file:size` and gets no `file:checksum`.
- It deletes only keys from `prior_managed_keys`. A prior key must name a
  `.json` file, a `README.md`, or an `AGENTS.md`. It must not be a
  `versions.json` or the `href` of an asset in the new plan. The planner cannot
  tell an item from a data file that has no links, so give only keys that an
  earlier plan put.

Keys are POSIX paths from the catalog root. The planner puts `key_prefix` in
front of each put key and each delete key. Give `prior_managed_keys` without
the prefix, in the same form as the keys of your documents.

The puts come first. Each put comes after the puts of all objects that it
links down to, so a reader that starts at the root never finds a link to a
missing object. The deletes come last.

The same input gives the same operations, byte for byte. The JSON keeps the
key order of your documents.

The planner refuses two keys, or two directories of keys, that differ only by
case. On a case-insensitive store, such as Windows or the default macOS file
system, these keys name the same file. The planner applies this rule on all
hosts, so a plan does not change with the host that makes it.

A translated root is a language root only when the root catalog links it with
a relative `alternate` link of type `application/json`. Rashid does not follow
an absolute `alternate` link, so the planner gives that tree the links of an
ordinary subdirectory.

The planner checks all input before it makes an operation. A failure raises a
subclass of `CatalogWriteError`:

- `InvalidKeyError`: a key or the prefix is absolute, holds `..`, a backslash,
  a hidden directory, or an empty segment, or starts with a drive such as `C:`
  on any host. Two keys or two directories differ only by case. A prior key is
  not a catalog file, is a `versions.json`, or is an asset.
- `InvalidDocumentError`: a document is not the STAC object that its file name
  shows, has links or assets of the wrong shape, has no title or description,
  has no `README.md` or `AGENTS.md`, or is outside the tree. A collection holds
  another collection, which the spec forbids (PORTO-CORE-004, PORTO-CORE-018).
- `MissingOwnerError`: a `README.md` or `AGENTS.md` has no catalog or
  collection in its folder.

## Build STAC documents from data

The STAC builders in `portolan_cli.api` make the Items and Collections for
`plan_catalog` from your data files. They need no catalog folder, so a job
that writes straight to object storage can use them. Install them with
`pip install portolan-cli`.

### Build the items

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

### Build the collection

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
links, so leave those out. Copy each asset to its `href` yourself. In this example, the asset files are
`wetlands-catalog/wetlands/dc-2023/dc-2023.parquet` and
`wetlands-catalog/wetlands/thumbnail.png`.
