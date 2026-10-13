"""A plugin that fails on import."""

from __future__ import annotations

raise ImportError("fixture plugin cannot import its dependency")
