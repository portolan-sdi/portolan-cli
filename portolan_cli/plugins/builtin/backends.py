"""The built-in ``file`` versioning backend."""

from __future__ import annotations

from typing import TYPE_CHECKING

from portolan_cli.hookspecs import hookimpl

if TYPE_CHECKING:
    from portolan_cli.hookspecs import BackendFactory


@hookimpl
def portolan_backends() -> dict[str, BackendFactory]:
    """Register ``file``, the versions.json backend."""
    from portolan_cli.backends.json_file import JsonFileBackend

    return {"file": JsonFileBackend}
