"""Markers for upstream bugs that more than one test module hits.

Each marker is a strict xfail, so CI fails when upstream fixes the bug.
"""

from __future__ import annotations

import pytest


def _gpio_connection_cap() -> int | None:
    """Return the memory_limit in bytes that geoparquet-io sets on a new connection."""
    from geoparquet_io.core.memory_limits import (
        default_memory_limit,
        parse_size,
    )

    limit = default_memory_limit()
    return int(parse_size(limit)) if limit else None


# geoparquet-io 1.7 caps each connection at 50% of the RAM; the WFS parse needs about 4.2 GiB.
# The 7 GB macOS runner gets a 3.2 GiB cap; the 16 GB runners pass.
_cap = _gpio_connection_cap()
GPIO_WFS_OOM = pytest.mark.xfail(
    _cap is not None and _cap <= 4 * 1024**3,
    reason="https://github.com/geoparquet/geoparquet-io/issues/1344 "
    "WFS GeoJSON parse runs out of memory under the 1.7 connection cap",
    strict=True,
)
