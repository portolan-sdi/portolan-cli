"""Hook implementations of the built-in Iceberg plugin (#947).

The module imports no pyiceberg. The factory imports the backend when a
catalog asks for it, so a catalog on the ``file`` backend does not pay for it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from portolan_cli.hookspecs import hookimpl

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


def iceberg_backend(catalog_root: Path | None = None) -> Any:
    """Create an ``IcebergBackend``.

    Args:
        catalog_root: Catalog root the backend reads and writes.

    Returns:
        The backend.

    Raises:
        ValueError: Without the iceberg extra.
    """
    try:
        from portolan_cli.backends.iceberg.backend import IcebergBackend
    except ImportError as e:
        raise ValueError(
            "The 'iceberg' backend requires the iceberg extra. "
            "Install it with: pip install portolan-cli[iceberg]"
        ) from e
    return IcebergBackend(catalog_root=catalog_root)


@hookimpl
def portolan_backends() -> dict[str, Callable[..., Any]]:
    """Register the ``iceberg`` versioning backend."""
    return {"iceberg": iceberg_backend}
