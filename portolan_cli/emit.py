"""Dual-mode output helpers: JSON envelope or human-readable message."""

from __future__ import annotations

from typing import Any

import click

from portolan_cli.json_output import ErrorDetail, error_envelope, success_envelope
from portolan_cli.output import error as _error


def emit_error(
    command: str,
    error_type: str,
    message: str,
    *,
    use_json: bool,
    code: str | None = None,
) -> None:
    """Emit an error as a JSON envelope (JSON mode) or a styled message (text)."""
    if use_json:
        envelope = error_envelope(
            command,
            [ErrorDetail(type=error_type, message=message, code=code)],
        )
        click.echo(envelope.to_json())
    else:
        _error(message)


def emit_success(
    command: str,
    data: dict[str, Any],
    *,
    use_json: bool,
) -> bool:
    """Emit a success envelope in JSON mode; signal whether it was emitted."""
    if use_json:
        envelope = success_envelope(command, data)
        click.echo(envelope.to_json())
        return True
    return False
