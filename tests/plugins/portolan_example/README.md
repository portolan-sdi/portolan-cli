# Example Portolan plugin

This package shows each extension point of the Portolan plugin system (#947).
CI installs it and calls every hook, so a change to `portolan_cli.api` or
`portolan_cli.hookspecs` that breaks a plugin fails the build.

| Extension | Module | Result |
|---|---|---|
| Command | `cli.py` | `portolan example versions` |
| Item and collection metadata | `hooks.py` | Adds the STAC `sci` extension |
| Version storage | `backend.py` | `ExampleBackend` under the name `example`, one JSON file per collection |

The package imports only `portolan_cli.api`, `portolan_cli.hookspecs`, and
`portolan_cli.testing`.

Run its tests from the repository root:

```bash
uv pip install ./tests/plugins/portolan_example
uv run --no-sync pytest tests/plugins -m plugin
```

Install it again after you change its code. `uv run --with` keeps a cached
build of a local package, and the cache key ignores the source files, so it
can run the old code.
