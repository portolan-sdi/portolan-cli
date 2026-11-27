"""JSON output envelope module for consistent CLI output formatting."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from portolan_core import trace


@dataclass
class ErrorDetail:
    """Structure for individual error entries in the errors array.

    Attributes:
        type: Error class name (e.g., "CatalogExistsError", "FileNotFoundError")
        message: Human-readable error description
        code: Optional structured error code (e.g., "PRTLN-CAT001")
    """

    type: str
    message: str
    code: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result: dict[str, Any] = {"type": self.type, "message": self.message}
        if self.code is not None:
            result["code"] = self.code
        return result


@dataclass
class OutputEnvelope:
    """The consistent wrapper structure for all JSON command output."""

    success: bool
    command: str
    data: dict[str, Any] | None
    errors: list[ErrorDetail] | None = field(default=None)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result: dict[str, Any] = {
            "success": self.success,
            "command": self.command,
            "data": self.data,
        }

        if self.errors is not None:
            result["errors"] = [e.to_dict() for e in self.errors]

        meta = trace.meta()
        if meta is not None:
            result["meta"] = meta

        return result

    def to_json(self, *, indent: int | None = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)


def success_envelope(command: str, data: dict[str, Any]) -> OutputEnvelope:
    """Create a success envelope with the given command and data.

    Args:
        command: Name of the command (e.g., "scan", "init", "check")
        data: Command-specific payload

    Returns:
        OutputEnvelope with success=True and errors=None
    """
    return OutputEnvelope(success=True, command=command, data=data)


def error_envelope(
    command: str,
    errors: list[ErrorDetail],
    *,
    data: dict[str, Any] | None = None,
) -> OutputEnvelope:
    """Create an error envelope with the given command and errors.

    Args:
        command: Name of the command (e.g., "scan", "init", "check")
        errors: List of ErrorDetail objects describing the errors
        data: Optional partial data to include (default: empty dict)

    Returns:
        OutputEnvelope with success=False and the provided errors
    """
    return OutputEnvelope(
        success=False,
        command=command,
        data=data if data is not None else {},
        errors=errors,
    )
