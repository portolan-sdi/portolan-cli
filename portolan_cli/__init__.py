"""Portolan CLI - Publish and manage cloud-native geospatial data catalogs."""

from __future__ import annotations

import importlib
import logging
import os
import sys
import types
from typing import TYPE_CHECKING, Any

from portolan_cli.lazy_exports import lazy_exports

# PyArrow 26 mimalloc commits huge pages eagerly; set before pyarrow loads (tests/unit/test_arrow_allocator.py).
os.environ.setdefault("MIMALLOC_ARENA_EAGER_COMMIT", "0")

logging.getLogger(__name__).addHandler(logging.NullHandler())
# geoparquet-io installs its own INFO stream handler when no logger has one.
logging.getLogger("geoparquet_io").addHandler(logging.NullHandler())

if TYPE_CHECKING:
    from portolan_cli.catalog import Catalog, CatalogExistsError
    from portolan_cli.cli import cli
    from portolan_cli.formats import detect_format

__all__ = [
    "Catalog",
    "CatalogExistsError",
    "cli",
    "detect_format",
]

# Lazy: an eager import loads cli.py, Click and the geospatial stack for every submodule.
_EXPORTS = {
    "Catalog": "portolan_cli.catalog",
    "CatalogExistsError": "portolan_cli.catalog",
    "cli": "portolan_cli.cli",
    "detect_format": "portolan_cli.formats",
}


__getattr__, __dir__ = lazy_exports(__name__, _EXPORTS, __all__, globals())


class _Package(types.ModuleType):
    """The package module, with ``cli`` bound to the Click group."""

    @property
    def cli(self) -> Any:
        """The ``portolan`` Click group."""
        return importlib.import_module("portolan_cli.cli").cli

    @cli.setter
    def cli(self, value: object) -> None:
        if not isinstance(value, types.ModuleType):
            raise AttributeError("portolan_cli.cli is read-only")


sys.modules[__name__].__class__ = _Package
