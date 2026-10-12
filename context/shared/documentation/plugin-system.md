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
help text, which lives in the module.

A dotted command name attaches to a built-in group. `version.rollback`
becomes `portolan version rollback`.

## The manager

`get_plugin_manager()` builds one `pluggy.PluginManager` on the first hook
call and caches it. Importing the CLI never builds it. The manager registers
the built-in hook modules first, then each outside plugin. Pluggy calls the
plugin registered last first, so `PluginManager.hook.<name>()` returns plugin
results before built-in results.

The manager does not use `load_setuptools_entrypoints`. That method stops at
the first import error and has no filter. The manager loop catches each
failure and emits a `PluginLoadWarning`. `pm.check_pending()` catches a hook
name that no hookspec declares. The manager unregisters that plugin and warns.

`call_each` and `hook_calls` call a hook plugin by plugin. They keep the
plugin name of each result. A hook that raises produces a `PluginWarning`, and
the next plugin still runs. The CLI prints a `PluginWarning` as an output
warning, without the source path that Python adds.

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
- A `portolan_backends` result named `file` or `iceberg` is ignored with a
  `PluginWarning`.

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
STAC JSON when it publishes. `IcebergBackend.on_post_add` does this.

`add` runs the hooks again on documents that the companion-asset phase
changes. `metadata fix` runs the item hook on an item it creates. A hook must
give the same result when it runs twice.

After each hook, `stac_enrich` restores `id`, `type`, `stac_version`, and
`links` and warns. A hook that raises leaves no partial change.

## Layers

Import-linter contracts in `pyproject.toml` hold the layers:

| Contract | Rule |
|---|---|
| `layer-read` | Click, rasterio, and the write modules are forbidden in the read modules |
| `layer-manage` | No module outside the CLI layer imports Click |
| `layer-cli` | Only the CLI layer imports a CLI module (a `protected` contract) |

The CLI layer is `cli`, `cli_types`, `emit`, `add_progress`, `scan.progress`,
and `plugins.commands`. `output.py` has a writer that uses no Click. `cli.py`
installs `click_writer` at import time, so CLI output does not change.

`tests/unit/plugins/test_boundary.py` parses each plugin package and fails on
a `portolan_cli` import outside `api`, `hookspecs`, and `testing`. If a
built-in plugin needs a private module, widen the API.

## Public surface

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
