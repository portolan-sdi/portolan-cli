#!/usr/bin/env python3
"""Single source of truth for pip-audit vulnerability ignores.

The ``.pip-audit-ignores`` file at the repo root lists vulnerabilities we
knowingly accept, each with an **expiry date** and a **reason**. This module
turns that file into ``pip-audit --ignore-vuln`` arguments, and is consumed by
every workflow that runs pip-audit::

    uv run pip-audit ... $(uv run python scripts/pip_audit_ignores.py)

File format (whitespace-separated; ``#`` starts a comment line)::

    VULN-ID  EXPIRES(YYYY-MM-DD)  REASON...

Two properties make this safe:

* **Expiry drops entries automatically.** Past the expiry date an entry is no
  longer emitted, so a still-present vulnerability starts failing CI again and
  forces a conscious decision (extend the date with a reason, or pin/replace
  the dependency) instead of being ignored forever.
* **Malformed lines fail loud.** A missing field or bad date raises rather than
  silently dropping the line — a broken ignore file must never quietly disable
  auditing.

``--json`` emits the parsed entries (id, expiry, reason) so tooling — e.g. the
security-audit workflow's issue body — can render the reasons from this one file
instead of re-hardcoding them.
"""

from __future__ import annotations

import argparse
import datetime
import json
import sys
from dataclasses import dataclass
from pathlib import Path

DEFAULT_PATH = Path(__file__).resolve().parent.parent / ".pip-audit-ignores"


class IgnoreFileError(ValueError):
    """Raised when .pip-audit-ignores contains a malformed line."""


@dataclass(frozen=True)
class IgnoreEntry:
    """One accepted vulnerability: its id, expiry date, and human reason."""

    vuln_id: str
    expires: datetime.date
    reason: str


def parse_entries(path: Path) -> list[IgnoreEntry]:
    """Parse every non-comment, non-blank line into an :class:`IgnoreEntry`.

    Raises :class:`IgnoreFileError` on any malformed line (missing fields or an
    unparseable date), naming the line number so the fix is obvious.
    """
    entries: list[IgnoreEntry] = []
    for lineno, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(maxsplit=2)
        if len(parts) < 3:
            raise IgnoreFileError(
                f"{path.name} line {lineno}: expected 'VULN-ID EXPIRES REASON', got: {raw!r}"
            )
        vuln_id, expires_str, reason = parts
        try:
            expires = datetime.date.fromisoformat(expires_str)
        except ValueError as exc:
            raise IgnoreFileError(
                f"{path.name} line {lineno}: expiry must be YYYY-MM-DD, got: {expires_str!r}"
            ) from exc
        entries.append(IgnoreEntry(vuln_id=vuln_id, expires=expires, reason=reason))
    return entries


def active_ignores(path: Path, today: datetime.date) -> list[str]:
    """Return the vulnerability IDs that are still within their expiry window.

    Expiry is inclusive: an entry remains active on its expiry date.
    """
    return [e.vuln_id for e in parse_entries(path) if today <= e.expires]


def format_args(ids: list[str]) -> str:
    """Format vulnerability IDs as ``pip-audit --ignore-vuln`` arguments."""
    return " ".join(f"--ignore-vuln {vuln_id}" for vuln_id in ids)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--path",
        type=Path,
        default=DEFAULT_PATH,
        help="Path to the ignore file (default: repo-root .pip-audit-ignores).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit active entries as JSON (id, expires, reason) instead of pip-audit args.",
    )
    args = parser.parse_args(argv)

    try:
        today = datetime.date.today()
        if args.json:
            active = [e for e in parse_entries(args.path) if today <= e.expires]
            payload = [
                {"id": e.vuln_id, "expires": e.expires.isoformat(), "reason": e.reason}
                for e in active
            ]
            print(json.dumps(payload))
        else:
            output = format_args(active_ignores(args.path, today=today))
            if output:
                print(output)
    except (OSError, IgnoreFileError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
