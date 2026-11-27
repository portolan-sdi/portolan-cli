"""Module-level ``__getattr__`` and ``__dir__`` that load names on first access."""

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
    """Build ``__getattr__`` and ``__dir__`` for a module with lazy exports."""

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
