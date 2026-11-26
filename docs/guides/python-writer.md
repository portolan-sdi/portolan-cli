# Write a catalog from Python

An application that holds STAC documents in memory can publish a Portolan
catalog with the `portolan-core` package. It does not need the CLI or the
geospatial stack. `portolan-cli` depends on `portolan-core`, so an install
of the CLI includes it.

The writer makes a plan first and writes second. The plan is plain data, so
you can inspect it, test it, and write it through your own storage layer.

## Make a plan

Give `plan_catalog` your STAC documents and the text of each `README.md` and
`AGENTS.md`. Key each one by its path from the catalog root.

```python
--8<-- "examples/python-writer/write_catalog.py:plan"
```

`plan_catalog` copies the documents and does not change your dictionaries.
On each copy it:

- adds the links that make the tree navigable: `root`, `parent`,
  `collection`, `child`, and `item`. Each `child` and `item` link gets the
  title of its target. An item without `properties.title` gives its `id`.
- sets the `collection` field of each item.
- adds an `agents` link to `./AGENTS.md` and a `describedby` link to
  `./README.md` on each catalog and collection.
- adds the Portolan profile URI to a catalog or collection that declares
  none.
- replaces the relative structural links that you gave, and keeps your other
  links, including an absolute link to another host.

It writes no `self` link, because it does not know where you publish the
catalog. It never reads asset files. Your assets pass through unchanged, so
give `file:size` and `file:checksum` yourself when you have them. For a
directory asset such as a Zarr store, give `file:size` and leave out
`file:checksum`.

The writer checks the input before it makes any operation. A key that is
absolute or holds `..`, a catalog or collection without a `title`, a missing
`README.md` or `AGENTS.md`, or documentation with no catalog or collection
beside it raises a `CatalogWriteError` subclass.

## Write the plan

`apply_plan` sends each operation to a sink in plan order. `LocalSink` writes
into a folder:

```python
--8<-- "examples/python-writer/write_catalog.py:apply"
```

The writer writes only the catalog JSON and the documentation. Put each asset
file at its href yourself. For this example, copy your data to
`my-catalog/parks/parks.parquet` and your preview image to
`my-catalog/parks/thumbnail.png`. Without them, the asset links do not
resolve. The `--no-data` check below does not report that.

`ObstoreSink` writes to any [obstore](https://developmentseed.org/obstore/)
store, such as `S3Store`, `GCSStore`, `AzureStore`, `LocalStore`, or
`MemoryStore`:

```python
from obstore.store import S3Store
from portolan_core.writer import ObstoreSink

apply_plan(plan, ObstoreSink(S3Store("my-bucket", prefix="catalogs/city")))
```

A sink is any object with these two methods:

```python
def put(self, key: str, content: bytes, media_type: str) -> None: ...
def delete(self, key: str) -> None: ...
```

You can also loop over the plan yourself. Each operation is a `PutOperation`
(`key`, `content`, `media_type`) or a `DeleteOperation` (`key`):

```python
--8<-- "examples/python-writer/write_catalog.py:loop"
```

Keep the plan order. Each object is written after the objects it links to, so
a reader that starts at the root never meets a missing file. The deletes come
last.

## Update a catalog

The writer never lists your storage. To remove files that a collection no
longer needs, pass the keys you wrote last time as `prior_managed_keys`. Each
prior key that the new plan does not write becomes a delete:

```python
plan = plan_catalog(documents, documentation, prior_managed_keys=previous_keys)
```

`previous_keys` is the set of document and documentation keys that you passed
last time, without a prefix. A prior key must be a `.json` file, a
`README.md`, or an `AGENTS.md`. The writer refuses a `versions.json` and a key
that an asset of the new catalog points at. It cannot tell an item from a
data file that nothing links, so list only keys that a previous plan wrote.

The writer also refuses a plan in which two keys differ only by case, because
such a plan is unsafe on a case-insensitive store.

To write under a prefix, pass `key_prefix="catalogs/city"`. The prefix goes in
front of every put and every delete key.

## Check the result

`portolan-core` installs the validator core, `rashid-core`. Run it on the
folder:

```bash
python -m rashid_core check --no-data ./my-catalog
```
