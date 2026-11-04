"""Hook implementations of the built-in Iceberg plugin (#947).

The module imports neither DuckDB nor httpx. The factory imports the
backend when a catalog asks for it, so a catalog on the ``file`` backend
does not pay for them, and ``portolan --help`` loads neither.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from portolan_cli.hookspecs import hookimpl

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


def iceberg_backend(catalog_root: Path | None = None) -> Any:
    """Create an ``IcebergBackend`` from the ``PORTOLAN_ICEBERG_*`` environment.

    Args:
        catalog_root: Catalog root the backend reads and writes.

    Returns:
        The backend.

    Raises:
        ValueError: A required variable is missing. The message names it.
            ``IcebergSettingsError`` is a ``ValueError``, so the command
            reports it the way it reports any backend setup error.
    """
    from portolan_cli.backends.iceberg.backend import IcebergBackend

    return IcebergBackend(catalog_root=catalog_root)


@hookimpl
def portolan_backends() -> dict[str, Callable[..., Any]]:
    """Register the ``iceberg`` versioning backend."""
    return {"iceberg": iceberg_backend}
