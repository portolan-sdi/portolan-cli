"""JSON output envelope module for consistent CLI output formatting.

This module provides the OutputEnvelope dataclass and factory functions
for generating consistent JSON output across all CLI commands. The envelope
structure supports agent-native workflows where LLMs parse command output.

Envelope Structure:
    {
        "success": true|false,
        "command": "command_name",
        "data": { ... },
        "errors": [ ... ]  # Only present when success=false
    }

Usage:
    from portolan_cli.json_output import success_envelope, error_envelope, ErrorDetail

    # For successful operations
    envelope = success_envelope("scan", {"files": [...], "count": 5})
    print(envelope.to_json())

    # For errors
    errors = [ErrorDetail(type="FileNotFoundError", message="File not found")]
    envelope = error_envelope("scan", errors)
    print(envelope.to_json())
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ErrorDetail:
    """Structure for individual error entries in the errors array.

    Attributes:
        type: Error class name (e.g., "CatalogExistsError", "FileNotFoundError")
        message: Human-readable error description
    """

    type: str
    message: str

    def to_dict(self) -> dict[str, str]:
        """Convert to dictionary for JSON serialization."""
        return {"type": self.type, "message": self.message}


@dataclass
class OutputEnvelope:
    """The consistent wrapper structure for all JSON command output.

    Attributes:
        success: True if command completed without errors, False otherwise
        command: Name of the command that produced this output (e.g., "scan", "init")
        data: Command-specific payload; structure varies by command
        errors: Array of error objects; present only when success=False
    """

    success: bool
    command: str
    data: dict[str, Any] | None
    errors: list[ErrorDetail] | None = field(default=None)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization.

        The errors field is excluded when None (for success cases).
        """
        result: dict[str, Any] = {
            "success": self.success,
            "command": self.command,
            "data": self.data,
        }

        if self.errors is not None:
            result["errors"] = [e.to_dict() for e in self.errors]

        return result

    def to_json(self, *, indent: int | None = 2) -> str:
        """Convert to JSON string.

        Args:
            indent: Indentation level for pretty printing. Default is 2.
                   Use None for compact output.

        Returns:
            JSON string representation of the envelope.
        """
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
