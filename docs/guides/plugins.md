# Write a plugin

A plugin is a Python package that adds behavior to the Portolan CLI. It can
add a command, enrich STAC metadata, store versions, convert a new file format,
or pull layers from a remote service. Portolan finds a plugin through the
entry points in its `pyproject.toml`, so the package needs no change to
Portolan itself.

Every example on this page comes from the
[example plugin](https://github.com/portolan-sdi/portolan-cli/tree/main/tests/plugins/portolan_example),
which uses each extension point. CI installs that package and calls each of
its hooks. A change to the CLI that breaks this code fails CI.

## The public surface

A plugin imports from three modules and from nothing else in `portolan_cli`:

| Module | Holds |
|---|---|
| `portolan_cli.api` | Protocols, data classes, helpers, and `hookimpl` |
| `portolan_cli.hookspecs` | The hook signatures |
| `portolan_cli.testing` | Conformance checks for plugin objects |

These modules follow semantic versioning. A change that removes a name or
breaks a signature needs a major release. A deprecated hook or hook argument
stays for one minor release, and pluggy warns each plugin that still uses it.
The [Python API reference](../reference/python.md#plugin-api) documents each
name.

A published plugin declares `portolan-cli` as a dependency. Keep module-level
imports light, because Portolan imports a hook module on the first hook call.
Import a heavy library inside the function that needs it.

## Entry points

The plugin declares its entry points in `pyproject.toml`:

```toml
--8<-- "tests/plugins/portolan_example/pyproject.toml:entry-points"
```

The `"portolan"` group names a module of hook implementations. The
`"portolan.commands"` group maps a command name to a Click command. Portolan
imports the command module only when you run the command, and
`portolan --help` lists it with the name of its distribution. A dotted name
attaches the command to a built-in group: `version.rollback` becomes
`portolan version rollback`.

Portolan still reads the deprecated `portolan.backends` group. It warns each
plugin that registers there to move to the `portolan_backends` hook. A future
minor release stops reading the group.

## Add a command

The entry point loads a Click group or command. A command uses the terminal
helpers from `portolan_cli.api` so its output matches the built-in commands:

```python
--8<-- "tests/plugins/portolan_example/src/portolan_example/cli.py:command"
```

A command that prints JSON builds it with `success_envelope` or
`error_envelope` and writes `envelope.to_json()` to stdout.

## Enrich items and collections

The `portolan_enrich_item` and `portolan_enrich_collection` hooks receive the
STAC JSON as a dict and change it in place. `EnrichContext` gives the catalog
root, the collection id, the document path, and the item id.

```python
--8<-- "tests/plugins/portolan_example/src/portolan_example/hooks.py:enrich"
```

The hooks run each time Portolan writes the document, during `portolan add`
or `portolan extract`. A second run must give the same result. Portolan restores `id`, `type`, `stac_version`, and
`links` after each hook. Portolan skips a hook wrapper with a warning.

## Store versions

The `portolan_backends` hook maps a name to a factory. Portolan calls
`factory(catalog_root=path)` and expects a
[`VersioningBackend`](../reference/python.md#versioning-backends).
A class satisfies this.

```python
--8<-- "tests/plugins/portolan_example/src/portolan_example/hooks.py:backends"
```

A catalog selects it with `portolan init --backend example` or with
`backend: example` in `.portolan/config.yaml`.

## Convert a file format

The `portolan_formats` hook returns
[`FormatHandler`](../reference/python.md#format-handlers)
objects. A handler declares its name, family, extensions, media type, and
asset level, then claims, converts, and describes files.

```python
--8<-- "tests/plugins/portolan_example/src/portolan_example/hooks.py:formats"
```

```python
--8<-- "tests/plugins/portolan_example/src/portolan_example/formats.py:handler"
```

The family is any string. The built-in families are `vector` and `raster`. The
helpers in `portolan_cli.api`, such as `convert_vector` and
`describe_vector_item`, let a handler reuse the built-in conversion.

Portolan picks the handler for one file in this order:

1. A pin in `.portolan/config.yaml` under `conversion.handlers`, such as
   `.xyz: xyz`.
2. A plugin handler that claims the file. When two plugin handlers claim it,
   Portolan raises `AmbiguousFormatError` until a pin decides.
3. A built-in handler that claims the file.

## Add a data source

The `portolan_extractors` hook returns
[`Extractor`](../reference/python.md#extractors) objects. Each
one becomes `portolan extract <name>`.

A service that serves named layers can implement
[`LayerSource`](../reference/python.md#layer-sources) instead.
`extractor_from` wraps it and gives it the layer filters, resume, retry, the
extraction report, and catalog setup that the WFS, ArcGIS, and Carto sources
use.

```python
--8<-- "tests/plugins/portolan_example/src/portolan_example/hooks.py:extractors"
```

```python
--8<-- "tests/plugins/portolan_example/src/portolan_example/geojson_source.py:source"
```

The class also implements `layer_metadata`, `styles`, and `source_link`.
`portolan extract --help` imports the hook module, so import a heavy library
inside the hook function.

## Test a plugin

`portolan_cli.testing` checks a plugin object against its protocol. Each check
raises `ConformanceError` with one line for each problem it finds:

- `check_backend(factory, catalog_root)` publishes two versions of one
  collection and reads them back.
- `check_format_handler(handler, sample, output_dir)` converts a sample file
  and reads the metadata of the result.
- `check_extractor(extractor, source, output_dir)` builds the command. With a
  source and an output directory, it also runs the extraction.

```python
--8<-- "tests/plugins/test_example_plugin.py:check-backend"
```

Install the plugin into the environment before you run its tests. With uv, use
`uv pip install ./path/to/plugin`. `uv run --with` can reuse a cached build of
a local package and run old code.

## Select plugins

`PORTOLAN_PLUGINS` controls which outside plugins load. The built-in plugins
always load.

| Value | Effect |
|---|---|
| unset | Every installed plugin loads. |
| `none` | No outside plugin loads. |
| `example,other` | Only the plugins with those entry point names load. |

A name in the list that matches no installed plugin produces a warning. A
plugin that fails to load also produces a warning, and the CLI keeps running.

A built-in name wins a collision. A plugin command with the name of a built-in
command does not run, and `portolan --help` warns about it. For backends and
extractors, the first registration of a name stays and Portolan warns about
each later one. The built-in plugins register first, so a plugin cannot replace
`file` or `iceberg`.
