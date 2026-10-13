"""Module-level ``__getattr__`` and ``__dir__`` that load names on first access.

The package root and ``portolan_cli.api`` re-export names from heavier
modules. Loading each name on first access keeps their import light (#944,
#947). This module imports only the standard library.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


def lazy_exports(
    module_name: str,
    exports: dict[str, str],
    public: list[str],
    namespace: dict[str, Any],
    *,
    cache: bool = True,
) -> tuple[Callable[[str], Any], Callable[[], list[str]]]:
    """Build ``__getattr__`` and ``__dir__`` for a module with lazy exports.

    Args:
        module_name: ``__name__`` of the module, for the error message.
        exports: Name to the module that defines it.
        public: The module's ``__all__``.
        namespace: The module's ``globals()``. A loaded name is cached here
            when ``cache`` is True.
        cache: Keep a loaded name in ``namespace``. With False, every access
            reads the source module again, as a function-level import does.
            A test that patches the source module then reaches the caller.

    Returns:
        The ``__getattr__`` and ``__dir__`` functions for the module.
    """

    def _getattr(name: str) -> Any:
        source = exports.get(name)
        if source is None:
            raise AttributeError(f"module {module_name!r} has no attribute {name!r}")
        value = getattr(importlib.import_module(source), name)
        if cache:
            namespace[name] = value
        return value

    def _dir() -> list[str]:
        return sorted([*namespace, *public])

    return _getattr, _dir
