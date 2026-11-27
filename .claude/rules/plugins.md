---
paths:
  - "portolan_cli/hookspecs.py"
  - "portolan_cli/api.py"
  - "portolan_cli/testing.py"
  - "portolan_cli/plugins/**"
  - "portolan_cli/stac_enrich.py"
  - "portolan_cli/lazy_exports.py"
  - "tests/plugins/**"
  - "tests/unit/plugins/**"
---

# Plugin system

`docs/guides/plugins.md` is the author guide.

## How it works

- `PORTOLAN_PLUGINS` controls discovery of outside plugins: unset loads all,
  `none` loads none, and `a,b` loads only those entry point names. Built-in
  plugins always load. A plugin that fails to load gives a
  `PluginLoadWarning`, and the CLI keeps running.
- Call hooks through `hook_calls` or `call_each`, which put the built-ins
  first. Do not call `PluginManager.hook.<name>()`: pluggy calls the plugin
  registered last first. The manager reads entry points itself, because
  `load_setuptools_entrypoints` stops at the first import error and has no filter.
- A dotted command name attaches to a built-in group: `version.rollback` is
  `portolan version rollback`. A plugin command never replaces a built-in
  command, and help warns about the collision.
- Format handler precedence, first match wins: a `conversion.handlers` pin in
  `.portolan/config.yaml`, then a plugin handler that claims the file, then a
  built-in handler. Two plugin handlers that claim one file raise
  `AmbiguousFormatError` until a pin decides.
- An enrichment hook may add fields. Portolan restores `id`, `type`,
  `stac_version`, and `links` after each hook and warns. A hook that raises
  leaves no partial change.
- A `LayerSource` serves named layers. `discover` does the once-per-run work
  and returns a `Discovery`. Each `LayerRef` carries the collection path and
  the source's own `data`. `run_layer_extraction` runs the lifecycle, and
  `extractor_from` wraps the source as `portolan extract <name>`.

## The public surface is a contract

- `portolan_cli.api`, `portolan_cli.hookspecs`, and `portolan_cli.testing`
  follow semantic versioning. A plugin imports nothing else from
  `portolan_cli`.
- `tests/unit/plugins/public_api.txt` records the surface. When you change it
  on purpose, update the file in the same commit. Removing or changing a name
  needs a major release. Deprecate a hook with `warn_on_impl` or
  `warn_on_impl_args` for one minor release first.
- Add a name to `api.py` in three places: the `TYPE_CHECKING` imports,
  `__all__`, and `_EXPORTS`. `import portolan_cli.api` must not load Click or
  a geospatial library. `tests/unit/test_startup_budget.py` checks it.

## Startup cost

- Never build the plugin manager at import time. `get_plugin_manager()` builds
  it on the first hook call.
- `portolan --help` imports no plugin module. `PluginGroup.format_commands`
  reads names from entry point metadata. Do not call `get_command` for a plugin
  name in a help path.
- Read entry points through `manager._scan`, which caches each group once per
  process.

## Built-in plugins use the same path

- List each built-in hook module in `manager._BUILTIN_MODULES`, under the
  name it registers as (`builtin:<name>`). A small shim goes in
  `portolan_cli/plugins/builtin/`. A built-in plugin package, such as
  `backends/iceberg/`, keeps its `hooks.py` in the package.
- A command that portolan-cli provides through the plugin path goes in the
  `portolan.commands` entry points in `pyproject.toml`. Help loads it for its
  help text, so its module imports only Click and the public modules.
- A plugin package imports only the public modules.
  `tests/unit/plugins/test_boundary.py` enforces it for each package in
  `PLUGIN_PACKAGES`. When a built-in plugin needs a private module, widen the
  API. Do not import past it.

## Enrichment

- The enrichment hooks run on the JSON files after Portolan writes them, never
  on pystac objects. `Collection.save` re-serializes items from memory.
- Run them before `_publish_collection_version`. A new code path that creates or
  rebuilds a STAC document calls `stac_enrich.enrich_item_file` or
  `enrich_collection_file`.
- A backend that changes STAC JSON after the hooks run merges into the file
  on disk. Never call `pystac.Collection.save` there, because it drops the
  hook fields. `IcebergBackend.on_post_add` shows the pattern.
- Backend precedence comes from `hook_calls`. It lists the built-ins first in
  registration order. The outside plugins follow in pluggy call order.
  `builtin:legacy_backends` comes last. `tryfirst` and `trylast` cannot move
  an outside plugin before a built-in.

## Format handlers

- A format family is a string. Never add a format enum, and never treat
  `vector` and `raster` as the only families.
- Pick a handler with `plugins.formats.handler_for`. Do not route on a file
  extension in new code.
- Per-format logic belongs in the handler package under
  `portolan_cli/formats_builtin/`. A shared helper the package needs goes into
  `portolan_cli.api`, and `public_api.txt` records it.
- A read module uses `format_families`, never the plugin registry.

## Example plugin

- `tests/plugins/portolan_example` uses each extension point. When you add a
  hook, add it to the example and to `tests/plugins/test_example_plugin.py`.
- Run it with `uv pip install ./tests/plugins/portolan_example`, then
  `uv run --no-sync pytest tests/plugins -m plugin`. Do not use
  `uv run --with` for it: uv keeps a stale build when only the source changes.
