"""Portolan CLI - Publish and manage cloud-native geospatial data catalogs."""

from __future__ import annotations

import importlib
import sys
import types
from typing import TYPE_CHECKING, Any

from portolan_cli.lazy_exports import lazy_exports

if TYPE_CHECKING:
    from portolan_cli.catalog import Catalog, CatalogExistsError
    from portolan_cli.cli import cli
    from portolan_cli.formats import FormatType, detect_format

__all__ = [
    "Catalog",
    "CatalogExistsError",
    "FormatType",
    "cli",
    "detect_format",
]

# Python runs this file before any submodule. An eager import here makes
# `import portolan_cli.stac` load cli.py, Click, and the geospatial stack
# (issue #944). Each export loads from its module on first access instead.
_EXPORTS = {
    "Catalog": "portolan_cli.catalog",
    "CatalogExistsError": "portolan_cli.catalog",
    "FormatType": "portolan_cli.formats",
    "cli": "portolan_cli.cli",
    "detect_format": "portolan_cli.formats",
}


__getattr__, __dir__ = lazy_exports(__name__, _EXPORTS, __all__, globals())


class _Package(types.ModuleType):
    """The package module, with ``cli`` bound to the Click group.

    The export ``cli`` and the submodule ``portolan_cli.cli`` share one name.
    When the submodule loads, the import system binds the module object to
    that name, and ``__getattr__`` does not run again. ``from portolan_cli
    import cli`` then returns the module. This property always returns the
    group, and the setter drops the module object the import system assigns.
    """

    @property
    def cli(self) -> Any:
        """The ``portolan`` Click group."""
        return importlib.import_module("portolan_cli.cli").cli

    @cli.setter
    def cli(self, value: object) -> None:
        if not isinstance(value, types.ModuleType):
            raise AttributeError("portolan_cli.cli is read-only")


sys.modules[__name__].__class__ = _Package
