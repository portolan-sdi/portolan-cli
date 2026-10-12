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

# Plugin system (#947)

The design record is `context/shared/documentation/plugin-system.md`. Read it
before you change the plugin code.

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
- Backend precedence comes from `hook_calls`, which lists built-ins first.
  Do not depend on pluggy call order.

## Example plugin

- `tests/plugins/portolan_example` uses each extension point. When you add a
  hook, add it to the example and to `tests/plugins/test_example_plugin.py`.
- Run it with `uv pip install ./tests/plugins/portolan_example`, then
  `uv run --no-sync pytest tests/plugins -m plugin`. Do not use
  `uv run --with` for it: uv keeps a stale build when only the source changes.
