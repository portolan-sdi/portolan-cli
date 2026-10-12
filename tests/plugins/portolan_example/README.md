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
uv run --with ./tests/plugins/portolan_example \
  pytest tests/plugins -m plugin
```
