# Write a plugin

Use a plugin to extend the Portolan CLI without changing Portolan itself. A
plugin is a Python package that can add a command, enrich STAC metadata, store
versions, convert another file format, or retrieve layers from a remote
service. Portolan discovers plugins from entry points in `pyproject.toml`.

The examples on this page come from the
[example plugin](https://github.com/portolan-sdi/portolan-cli/tree/main/tests/plugins/portolan_example),
which uses each extension point. CI installs that package and calls each of
its hooks. A change to the CLI that breaks this code fails CI.

## Use the public API

Import plugin interfaces from these three modules only:

| Module | Holds |
|---|---|
| `portolan_cli.api` | Protocols, data classes, helpers, and `hookimpl` |
| `portolan_cli.hookspecs` | The hook signatures |
| `portolan_cli.testing` | Conformance checks for plugin objects |

These modules follow semantic versioning. Removing a name or breaking a
signature requires a major release. A deprecated hook or hook argument remains
available for one minor release. Pluggy warns you when your plugin uses one.
See the [Python API reference](../reference/python.md) for each name.

Before you publish a plugin, declare `portolan-cli` as a dependency. Keep
module-level imports light because Portolan imports a hook module on the first
hook call. Import a heavy library inside the function that uses it.

## Entry points

Declare your plugin entry points in `pyproject.toml`:

```toml
--8<-- "tests/plugins/portolan_example/pyproject.toml:entry-points"
```

Use the `"portolan"` group for a module of hook implementations. Use
`"portolan.commands"` to map a command name to a Click command. Portolan
imports the command module when you run the command. The output of
`portolan --help` includes the command and its distribution name. To attach a
command to a built-in group, use a dotted name such as `version.rollback`.
This example becomes `portolan version rollback`.

If your plugin uses the deprecated `portolan.backends` group, move it to the
`portolan_backends` hook. Portolan supports the old group for one minor
release and displays a warning when a plugin uses it.

## Add a command

Point the entry point to a Click group or command. Use `info` and `success`
from `portolan_cli.api` to match the output of built-in commands:

```python
--8<-- "tests/plugins/portolan_example/src/portolan_example/cli.py:command"
```

For JSON output, build an envelope with `success_envelope` or
`error_envelope`. Write `envelope.to_json()` to stdout.

## Enrich items and collections

Implement `portolan_enrich_item` or `portolan_enrich_collection` to change
STAC JSON in place. Each hook receives the JSON as a dict. `EnrichContext`
provides the catalog root, collection ID, document path, and item ID.

```python
--8<-- "tests/plugins/portolan_example/src/portolan_example/hooks.py:enrich"
```

Portolan runs the hooks each time `portolan add` or `portolan extract` writes a
document. Make each hook produce the same document when it runs more than
once. After each hook, Portolan restores `id`, `type`, `stac_version`, and
`links`. Portolan skips a hook wrapper and displays a warning.

## Store versions

Use the `portolan_backends` hook to map a name to a factory. Portolan calls
`factory(catalog_root=path)`. Return a class that implements
[`VersioningBackend`](../reference/python.md).

```python
--8<-- "tests/plugins/portolan_example/src/portolan_example/hooks.py:backends"
```

Configure the version store with `portolan init --backend example` or set
`backend: example` in `.portolan/config.yaml`.

## Convert a file format

Return [`FormatHandler`](../reference/python.md) objects from the
`portolan_formats` hook. A handler declares its name, family, extensions,
media type, and asset level. It also claims, converts, and describes files.

```python
--8<-- "tests/plugins/portolan_example/src/portolan_example/hooks.py:formats"
```

```python
--8<-- "tests/plugins/portolan_example/src/portolan_example/formats.py:handler"
```

Use any string for the family. Portolan provides the built-in `vector` and
`raster` families. To reuse built-in conversion, call helpers such as
`convert_vector` and `describe_vector_item` from `portolan_cli.api`.

Portolan picks the handler for one file in this order:

1. A pin in `.portolan/config.yaml` under `conversion.handlers`, such as
   `.xyz: xyz`.
2. A plugin handler that claims the file. If two plugin handlers claim it,
   Portolan raises `AmbiguousFormatError` until you configure a pin.
3. A built-in handler that claims the file.

## Add a data source

Return [`Extractor`](../reference/python.md) objects from the
`portolan_extractors` hook. Portolan adds each extractor as
`portolan extract <name>`.

If a service provides named layers, you can implement
[`LayerSource`](../reference/python.md) instead. The `extractor_from` helper
adds layer filters, resume, retry, an extraction report, and catalog setup.
The WFS, ArcGIS, and Carto sources use the same helper.

```python
--8<-- "tests/plugins/portolan_example/src/portolan_example/hooks.py:extractors"
```

```python
--8<-- "tests/plugins/portolan_example/src/portolan_example/geojson_source.py:source"
```

Implement `layer_metadata`, `styles`, and `source_link` on the class too.
Because `portolan extract --help` imports the hook module, import heavy
libraries inside the hook function.

## Test a plugin

Use `portolan_cli.testing` to check a plugin object against its protocol. Each
check raises `ConformanceError` with one line for each problem:

- `check_backend(factory, catalog_root)` publishes two versions of one
  collection and reads them back.
- `check_format_handler(handler, sample, output_dir)` converts a sample file
  and reads the metadata of the result.
- `check_extractor(extractor, source, output_dir)` builds the command. With a
  source and an output directory, it also runs the extraction.

```python
--8<-- "tests/plugins/test_example_plugin.py:check-backend"
```

Before you run the tests, install the plugin into the environment. With uv,
run `uv pip install ./path/to/plugin`. The `uv run --with` command can reuse a
cached build of a local package and run old code.

## Select plugins

Set `PORTOLAN_PLUGINS` to control which third-party plugins load. Built-in
plugins always load.

| Value | Effect |
|---|---|
| unset | Every installed plugin loads. |
| `none` | No non-built-in plugin loads. |
| `example,other` | Only the plugins with those entry point names load. |

If a name does not match an installed plugin, Portolan displays a warning. If
a plugin fails to load, Portolan displays a warning and continues to run.

If names conflict, the built-in name takes precedence. Portolan does not run a
plugin command that has the same name as a built-in command.
`portolan --help` displays a warning about the conflict. For backends and
extractors, Portolan keeps the first registration and warns about each later
one. Built-in plugins register first, so a plugin cannot replace `file` or
`iceberg`.
