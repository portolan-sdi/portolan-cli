"""Dual-mode output helpers: JSON envelope or human-readable message.

`cli.py` repeatedly branched on ``use_json`` to either build an
``OutputEnvelope`` and echo it, or print a styled message via ``output.py``.
This module collapses that boilerplate into two helpers so command handlers
stay focused on control flow.

It lives in its own module (rather than in ``json_output`` or ``output``)
because it must import *both*: the import-linter "utilities-are-foundational"
independence contract forbids ``json_output`` and ``output`` from importing
each other, so the code that bridges them cannot live in either one.

The JSON emission is byte-identical to the previous inline blocks: it echoes
``envelope.to_json()`` exactly as ``cli.output_json_envelope`` did.
"""

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
    """Emit an error as a JSON envelope (JSON mode) or a styled message (text).

    This does not exit. Callers keep their own ``raise SystemExit(...)`` so the
    exit code and exception chaining (``from err``) stay explicit at the call
    site.

    Args:
        command: Name of the command producing the error (envelope ``command``).
        error_type: Error class name reported in ``ErrorDetail.type``.
        message: Human-readable error description (used for both channels).
        use_json: If True, echo a JSON error envelope; otherwise print via
            ``output.error``.
        code: Optional structured error code for ``ErrorDetail.code``.
    """
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
    """Emit a success envelope in JSON mode; signal whether it was emitted.

    In text mode nothing is printed, letting the caller render its own
    human-readable output. The typical call site is::

        if not emit_success("status", data, use_json=use_json):
            ...  # human-readable output

    Args:
        command: Name of the command producing the output (envelope ``command``).
        data: Command-specific payload for the envelope ``data`` field.
        use_json: If True, echo a JSON success envelope and return True.

    Returns:
        True if a JSON envelope was emitted (text output should be skipped),
        False if the caller should produce human-readable output.
    """
    if use_json:
        envelope = success_envelope(command, data)
        click.echo(envelope.to_json())
        return True
    return False
