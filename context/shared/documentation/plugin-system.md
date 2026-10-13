# Plugin system

This note records how the plugin system works and why (#947). The hookspecs
and their docstrings are in `portolan_cli/hookspecs.py`. The public names are
in `portolan_cli/api.py`.

## Entry point groups

| Group | Holds | Read by |
|---|---|---|
| `portolan` | Hook implementation modules | `plugins/manager.py`, through pluggy |
| `portolan.commands` | Click commands | `plugins/commands.py`, without pluggy |

Commands have their own group because `pluggy.PluginManager` imports every
plugin module when it loads them. A command that came from a hook would make
`portolan --help` import every installed plugin. The lazy group reads command
names from entry point metadata and imports a command module only when the
user runs that command. Help shows `(plugin: <distribution>)` in place of the
help text, which lives in the module. A command that `portolan-cli` itself
declares is the exception. Help loads it and shows its help text, because its
module imports only Click and the public API. Shell completion shows the same
rows. It imports no outside plugin either.

The group caches a loaded command by its entry point. After
`reset_plugin_manager()`, a command that `PORTOLAN_PLUGINS` disables is not
found, so it does not run.

A dotted command name attaches to a built-in group. `version.rollback`
becomes `portolan version rollback`.

## The manager

`get_plugin_manager()` builds one `pluggy.PluginManager` on the first hook
call and caches it. Importing the CLI never builds it. The manager registers
the built-in hook modules in `manager._BUILTIN_MODULES` first, as
`builtin:<name>`, then each outside plugin. Built-ins register directly and
not through entry points, so they come first whatever order the entry points
have. Pluggy calls the
plugin registered last first, so `PluginManager.hook.<name>()` returns plugin
results before built-in results.

The manager does not use `load_setuptools_entrypoints`. That method stops at
the first import error and has no filter. The manager loop catches each
failure and emits a `PluginLoadWarning`. `pm.check_pending()` catches a hook
name that no hookspec declares. The manager unregisters that plugin and warns.
`pm.register` adds hooks one by one and keeps the ones it added before an
invalid hook. The manager unregisters the plugin in that case too, so a plugin
that fails validation adds no hook. An outside entry point name cannot start
with `builtin:`, which is reserved for the built-in modules.

`call_each` and `hook_calls` call a hook plugin by plugin. They keep the
plugin name of each result. A hook that raises produces a `PluginWarning`, and
the next plugin still runs. The CLI prints a `PluginWarning` as an output
warning, without the source path that Python adds.

`hook_calls` lists the built-ins first, in registration order. The outside
plugins follow in pluggy call order: `tryfirst`, then the plugin registered
last, then `trylast`. Pluggy itself calls a normal outside plugin before the
built-ins, because it calls the plugin registered last first. The manager puts
the built-ins first and uses pluggy order only inside the
outside plugins. The deprecated
`legacy_backends` module comes after every outside plugin. A hook wrapper
(`wrapper=True` or `hookwrapper=True`) produces a `PluginWarning` and is
skipped, because a plain call of a wrapper only creates a generator.

## Selecting plugins

`PORTOLAN_PLUGINS` unset loads every installed plugin. `none` loads none. A
comma list loads those names. An unknown name in the list produces a warning.

An entry point that `portolan-cli` itself declares always loads. A command is
selected by the names of the `portolan` entries that its distribution
declares. One name enables or disables everything a package adds. A distribution with commands
and no hooks is selected by the first segment of the command name.

## Name collisions

A built-in name always wins:

- A plugin command with the name of a built-in command does not run. Help
  warns about it.
- A plugin command with the name of a command that `portolan-cli` declares
  as an entry point, such as `version.rollback`, does not run. Discovery
  order does not change this. Help warns about it.
- A `portolan_backends` result named `file` or `iceberg` is ignored with a
  `PluginWarning`. `trylast` and `tryfirst` do not change this.
- A `portolan_backends` result that is not a mapping is ignored with a
  `PluginWarning`. `get_backend("file")` still resolves.

## Format handlers

Format handlers follow a different rule. A plugin handler comes before the
built-in handler for the same extension, so a plugin can replace the COG
conversion for `.tif`. `plugins/formats.py` `select_handler` applies this
order:

1. A pin in `.portolan/config.yaml`, such as `conversion.handlers: {.tif: cog}`.
   A pin to a missing name raises `UnknownFormatHandlerError`.
2. A plugin handler whose `extensions` hold the extension and whose `claims`
   returns True. When more than one plugin handler claims the file,
   `AmbiguousFormatError` names the pin to add.
3. A built-in handler that claims the file.

`FormatType` is gone. A format family is a plain string, because a plugin
brings its own. The built-in families are `vector` and `raster`.

A file that a plugin handler claims takes the plugin path. `prepare_item`
calls the handler's `convert` and `read_metadata`, and the rest of `add` runs
as for a built-in format of the same family and `asset_level`.
`convert_file` returns the handler's output. `add`, `check --geo-assets`,
`scan`, and `get_cloud_native_status` accept the extensions that plugin
handlers register.

`add` and `check --fix` convert through the same handler code. The built-in
handlers live in `portolan_cli/formats_builtin`, one package per format, and
import only `portolan_cli.api`. `preparation.convert_vector` and
`preparation.convert_raster` are the one vector and one raster converter. The
`add` safety features apply to both commands:

- the fidelity gate
- the rewrite temp marker
- the schema metadata restore
- the transient retry

Partitioning follows the size rule and the index rule.

- The size rule is in `partitioning.partition_strategy`, and only `add`
  applies it, as before the merge. A file at or above `partitioning.threshold_gb` partitions into
  Hive directories next to it. The file is removed only when the step wrote a
  partition. A directory that already holds partitions for the strategy raises
  `PartitionCollisionError`, because geoparquet-io does not write over a cell.
  `add` then removes a converted file that the run wrote, so no row appears
  twice. A file that existed before the run stays.
  `check --fix` converts a source again on every run, so the size rule would
  collide with its own earlier partitions there.
- The index rule (`conversion.vector.partition`) applies only to
  `check --fix`, as before the merge. `convert._convert_partitioned_by_index`
  writes the partitions to `<output_dir>/<stem>/`. `add` writes one file with
  the index column.

The index rule keeps a directory per source. geoparquet-io names a Hive file
after its cell and does not write over a cell that exists. In one shared
directory, the second source lost every row in a cell that the first source
wrote. Small files also failed in `add`, because geoparquet-io refuses
partitions under 100 rows.

The handler owns the per-format STAC logic. `describe_item` adds statistics,
render fields, nodata defaults, and the item extensions. `describe_collection`
adds the table extension. `inspect` answers `portolan info`. Core code uses
the family only to group files in `scan` and to label them.

The COG handler writes the raster thumbnail when it converts, because each
raster is an item. It keeps a thumbnail that exists, so a re-add leaves a
hand-made thumbnail in place. An old thumbnail also stays after the imagery
changes. Delete it to get a new one. The GeoParquet handler writes no thumbnail. A collection
can hold thousands of vector files, and `add` writes one thumbnail for the
collection. `check --fix` no longer writes a thumbnail next to a converted
vector file. A reference catalog built before and after the merge
differs only in timestamps and in those `check --fix` thumbnails.

`--force` without `--reconvert` keeps an existing output for every handler. The
handler converts it again only when it reads that format. `--reconvert` on a
file that is its own output calls the handler's `reconvert`. `check --fix
--force` calls `reoptimize`, which only the COG handler has (issue #530).

## Extractors

`portolan extract` gets its subcommands from the `portolan_extractors` hook.
`ExtractGroup` calls the hook on first use, so `portolan --help` builds no
plugin manager. `portolan extract --help` imports the hook modules.

`plugins/builtin/extractors.py` registers ArcGIS, Carto, and WFS through that
hook. Each `BuiltinExtractor` wraps the source's catalog function and its
Click command in `extract/<source>/cli.py`. The commands share the helpers
in `extract/common/cli_helpers.py`. The help output of each command matches
the output from before the move byte for byte.

A layer-based source implements `LayerSource` and wraps it with
`extractor_from`. `run_layer_extraction` then gives it the shared steps:
filters, the license check, resume, retry, the report, catalog setup,
styles, provenance links, and collection metadata. `layer_cli.build_command`
makes its command from the same helpers as the built-in commands.

The built-in sources use the same driver. `WFSLayerSource`,
`ArcGISLayerSource`, and `CartoLayerSource` live in each source's
`orchestrator.py`. `extract_<source>_catalog` keeps its signature and maps
the source's `ExtractionOptions` to `LayerSourceOptions`. ArcGIS ImageServer
stays outside the driver, because it tiles one raster and has no layers.
A plugin and a built-in source get the same lifecycle, so one fix in
`source.py` reaches both.

The source-specific steps fit the protocol with no extra method:

| Step | Where it goes |
|------|---------------|
| WFS version negotiation | `discover` negotiates once and puts the version in each `LayerRef.data` |
| ArcGIS services-root walk | `discover` walks the folders and gives each `LayerRef` a `path` below its service |
| Per-layer service URL | `LayerRef.data` holds it, and `fetch`, `layer_metadata`, and `source_link` read it |
| Parallel downloads | `LayerSourceOptions.workers` |
| WFS per-layer deadline | `LayerSourceOptions.layer_timeout` |
| Carto geometry probe | `discover` filters first, then probes only the kept tables |
| ArcGIS folder coverage | `Discovery.folder_coverage` |
| Root title on a new catalog | `Discovery.root_metadata` |

`discover` receives `LayerSourceOptions`, so a source can filter before an
expensive probe. The driver filters again, which costs nothing.

Moving the built-ins changed some results. All sources now share these rules:

- A failed layer records None for features, size, duration, and output path.
- A resumed layer keeps its earlier result. It emits "starting" and then
  "skipped". The text output then shows the layer name above the skip line.
- The progress index is the position of the layer in the run.
- In an ArcGIS services root, the via link and the layer details use the URL
  of the layer's own service.

## Enrichment runs on the files

`portolan_enrich_item` and `portolan_enrich_collection` receive the STAC JSON
as a dict. They do not receive pystac objects, which keeps pystac out of the
public API.

The hooks run on the JSON file after Portolan writes it. An in-memory change
before the write would be lost: `pystac.Collection.save` in
`finalization._save_collection_with_links` serializes every item again from
memory. pystac keeps unknown fields on a round trip, so a later save keeps the
fields a hook added.

`finalization._finalize_collection` runs the hooks after the collection save
and before `_publish_collection_version`. A `VersioningBackend` can upload the
STAC JSON when it publishes. `IcebergBackend.on_post_add` does this. It merges
its `table:*` and `iceberg:*` fields into the `collection.json` on disk. A
pystac save from the in-memory collection would drop the hook fields.

`add` attaches companion assets (non-geospatial files) after the publish. It
runs the hooks again on the documents that this phase changes. The local
files then hold the enriched companion assets. The published version does
not hold them. `add` used this order before the hooks existed.

`add --external` runs the collection hook on the collection it writes.
`metadata fix` runs the item hook on an item it creates. A hook must give the
same result when it runs twice.

After each hook, `stac_enrich` restores `id`, `type`, `stac_version`, and
`links` and warns. A hook that raises leaves no partial change.

## Deprecated `portolan.backends` group

v0.8.0 and v1.0.0b0 found plugin factories in the `portolan.backends` entry
point group. The `portolan_backends` hook replaces it.
`plugins/builtin/legacy_backends.py` reads the group for one more minor
release. It ranks after the outside plugins, so a `portolan_backends` hook
wins a shared name. Each entry point it finds produces a `PluginWarning`. Its
class loads only when `get_backend` asks for it. `PORTOLAN_PLUGINS` selects
these entry points like command entry points. A later minor release deletes
the module.

## Layers

Import-linter contracts in `pyproject.toml` hold the layers:

| Contract | Rule |
|---|---|
| `layer-read` | Click, rasterio, and the write modules are forbidden in the read modules |
| `layer-manage` | No module outside the CLI layer imports Click |
| `layer-cli` | Only the CLI layer imports a CLI module (a `protected` contract) |

The CLI layer is `cli`, `cli_types`, `emit`, `add_progress`, `scan.progress`,
`plugins.commands`, and `backends.iceberg.cli`. `output.py` has a writer that uses no Click. `cli.py`
installs `click_writer` at import time, so CLI output does not change.

`tests/unit/plugins/test_boundary.py` parses each plugin package and fails on
a `portolan_cli` import outside `api`, `hookspecs`, and `testing`. If a
built-in plugin needs a private module, widen the API.

## Iceberg is a built-in plugin

`backends/iceberg/hooks.py` registers the `iceberg` backend as
`builtin:iceberg`. Its factory imports pyiceberg on the first request. On
Python 3.10 it raises a `ValueError` that names the Python requirement. With
no extra installed, the `ValueError` names the extra. `get_backend` passes a
factory's `ValueError` through unchanged.

`backends/iceberg/cli.py` holds `version rollback` and `version prune`.
`pyproject.toml` declares them as `version.rollback` and `version.prune` in
`portolan.commands`. They load with `PORTOLAN_PLUGINS=none`, because
`portolan-cli` declares them.

The package root loads `IcebergBackend` on first access, so the `hooks` and
`cli` modules import no pyiceberg or pyarrow. The package imports only the
public modules, and `test_boundary.py` checks it. That rule replaced the
`iceberg-backend-isolation` import-linter contract.

## Public surface

`portolan_cli.api` reads each name from its source module on every access and
keeps no copy. A patch on the source module also changes the name that a
plugin imports from `api`.

`tests/unit/plugins/public_api.txt` records the names in `portolan_cli.api`
and `portolan_cli.testing`, and every hookspec signature. A change to it
shows up in review. Removing or changing a name breaks plugins and needs a
major release. The deprecation policy is in the `hookspecs` docstring.

## Startup cost

Measured in October 2026 on Linux with CPython 3.12.13, 230 distributions
installed:

| Measurement | Result |
|---|---|
| `portolan --help`, median of 15 runs | 694 ms on the branch, 687 ms on `main` |
| Help rendering alone, first call | 8.5 ms on the branch, 1.2 ms on `main` |
| One `entry_points(group=...)` scan | 7 to 9 ms |
| First scan with 300 extra distributions (528 in total) | 24.5 ms |

The manager reads each group once per process (`manager._scan`). Before that
cache, one `--help` scanned the command group several times.

Compare timings with the same interpreter on both sides. A pyenv build of
CPython 3.12.2 imported `portolan_cli.cli` about 110 ms slower than the
uv-managed 3.12.13 build. That gap looked like a plugin cost at first.

## Example plugin

`tests/plugins/portolan_example` is a separate package. It adds a command, item
and collection enrichment, and a versioning backend. The `plugin-test` CI job
installs it with `uv pip install` and runs `pytest tests/plugins -m plugin`.
Its `pyproject.toml` leaves out the `portolan-cli` dependency, so CI tests the
checkout and not a release.

Do not install it with `uv run --with`. With uv 0.6.14 on CI, that overlay used
the runner's system Python, and pytest skipped every test. Locally, uv keeps a
cached build of the package and reuses it after a source change, because the
cache key ignores the source files.
