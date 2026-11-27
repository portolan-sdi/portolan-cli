"""Shared progress plumbing for extract orchestrators."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeAlias

logger = logging.getLogger(__name__)

MessageCallback: TypeAlias = Callable[[str, str], None]
"""Called as ``on_message(level, text)`` with a message for the user. See ``emit_message``."""

_LOG_LEVELS = {"warn": logging.WARNING, "error": logging.ERROR}


@dataclass
class ExtractionProgress:
    """Progress callback data for extraction."""

    layer_index: int
    total_layers: int
    layer_name: str
    status: str
    error: str | None = None


LayerProgressCallback: TypeAlias = Callable[[ExtractionProgress], None]
"""Called with the progress of each layer. See ``emit_progress``."""


def emit_progress(
    on_progress: LayerProgressCallback | None,
    layer_index: int,
    total_layers: int,
    layer_name: str,
    status: str,
    error: str | None = None,
) -> None:
    """Emit a progress event if a callback is provided."""
    if on_progress:
        on_progress(
            ExtractionProgress(
                layer_index=layer_index,
                total_layers=total_layers,
                layer_name=layer_name,
                status=status,
                error=error,
            )
        )


def emit_message(on_message: MessageCallback | None, level: str, text: str) -> None:
    """Hand a message for the user to ``on_message(level, text)``."""
    if on_message:
        on_message(level, text)
    else:
        logger.log(_LOG_LEVELS.get(level, logging.INFO), text)
