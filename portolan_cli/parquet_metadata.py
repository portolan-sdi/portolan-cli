"""Parquet key-value footer metadata, read through geoparquet-io.

pyarrow rebuilds the Arrow schema metadata from the ``ARROW:schema`` footer blob
when a file carries one, and it drops every other footer key while it does so. A
writer that appends ``geo`` at close therefore produces a valid GeoParquet whose
``geo`` key ``schema_arrow.metadata``, ``pq.read_schema()`` and
``FileMetaData.schema.to_arrow_schema()`` never show. Appending at close is the
normal pattern, because bbox and geometry_types are only known then (issue #864).

geoparquet-io reads the raw footer and gets these files right, so Portolan reads
the footer through it rather than through the reconstructed Arrow schema. The
functions here are the only supported way to read ``geo`` or any other footer
key in this codebase.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

__all__ = ["read_geo_metadata", "read_kv_metadata"]


def read_kv_metadata(path: Path) -> dict[bytes, bytes]:
    """Read every key-value pair in a Parquet file's footer.

    Args:
        path: Path to a Parquet file.

    Returns:
        The footer key-value metadata. Empty for a file that declares none, and
        empty for an unreadable file. The keys include ``ARROW:schema`` when
        pyarrow wrote the file.
    """
    from geoparquet_io.core.duckdb_metadata import (  # type: ignore[import-untyped]
        get_kv_metadata,
    )

    try:
        return dict(get_kv_metadata(str(path)) or {})
    except Exception:  # noqa: BLE001 - an unreadable file carries no metadata
        logger.debug("Failed to read Parquet footer metadata from %s", path, exc_info=True)
        return {}


def read_geo_metadata(path: Path) -> dict[str, Any] | None:
    """Read and parse the ``geo`` key of a GeoParquet file.

    Args:
        path: Path to a Parquet file.

    Returns:
        The parsed GeoParquet metadata object. None for a plain Parquet file,
        for a file whose ``geo`` value is not a JSON object, and for an
        unreadable file.
    """
    from geoparquet_io.core.duckdb_metadata import get_geo_metadata

    try:
        geo = get_geo_metadata(str(path))
    except Exception:  # noqa: BLE001 - an unreadable file is not a GeoParquet
        logger.debug("Failed to read GeoParquet metadata from %s", path, exc_info=True)
        return None
    return geo if isinstance(geo, dict) else None
