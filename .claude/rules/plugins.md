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

- Define a built-in hook module in `portolan_cli/plugins/builtin/` and list it
  in `manager._BUILTIN_MODULES`.
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

## Example plugin

- `tests/plugins/portolan_example` uses each extension point. When you add a
  hook, add it to the example and to `tests/plugins/test_example_plugin.py`.
- Run it with
  `uv run --with ./tests/plugins/portolan_example pytest tests/plugins -m plugin`.
