"""Register the built-in format handlers (#947).

The handlers live in ``portolan_cli.formats_builtin``, one package per format.
Those packages use the public API only, the same as an outside plugin.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from portolan_cli.formats_builtin.cloud_native import make_handlers as _cloud_native
from portolan_cli.formats_builtin.cog import make_handler as _cog
from portolan_cli.formats_builtin.geoparquet import make_handler as _geoparquet
from portolan_cli.hookspecs import hookimpl

if TYPE_CHECKING:
    from portolan_cli.plugins.formats import FormatHandler

GEOPARQUET = _geoparquet()
COG = _cog()
PMTILES, FLATGEOBUF = _cloud_native()

BUILTIN_HANDLERS = (GEOPARQUET, COG, PMTILES, FLATGEOBUF)


@hookimpl
def portolan_formats() -> list[FormatHandler]:
    """Register the built-in format handlers."""
    return list(BUILTIN_HANDLERS)
