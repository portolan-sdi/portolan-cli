"""Hook implementations of the built-in Iceberg plugin."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from portolan_cli.hookspecs import hookimpl

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


def iceberg_backend(catalog_root: Path | None = None) -> Any:
    """Create an ``IcebergBackend``."""
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
