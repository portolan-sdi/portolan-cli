"""Hook implementations of the built-in Iceberg plugin (#947).

The module imports no pyiceberg. The factory imports the backend when a
catalog asks for it, so a catalog on the ``file`` backend does not pay for it.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

from portolan_cli.hookspecs import hookimpl

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

# A name, not a literal, so mypy does not decide the check for the Python it
# targets and report the rest of the function as unreachable (warn_unreachable).
_MINIMUM_PYTHON = (3, 11)


def iceberg_backend(catalog_root: Path | None = None) -> Any:
    """Create an ``IcebergBackend``.

    Args:
        catalog_root: Catalog root the backend reads and writes.

    Returns:
        The backend.

    Raises:
        ValueError: On Python before 3.11, or without the iceberg extra.
    """
    if sys.version_info[:2] < _MINIMUM_PYTHON:
        raise ValueError(
            "The Iceberg backend requires Python 3.11+. "
            f"You are running Python {sys.version_info[0]}.{sys.version_info[1]}."
        )
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
