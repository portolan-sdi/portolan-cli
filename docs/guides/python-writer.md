# Write a catalog from Python

The `portolan-core` package publishes a Portolan catalog from your own Python
code. Install it with `pip install portolan-core`, which leaves out the CLI and
the geospatial stack. The writer saves the STAC documents and Markdown files
that you supply. To convert data or extract metadata, use `portolan add`
instead.

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
