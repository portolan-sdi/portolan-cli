#!/usr/bin/env python3
"""Turn ``.pip-audit-ignores`` into ``pip-audit --ignore-vuln`` arguments, or JSON with ``--json``.

Security: an expired entry is dropped and a malformed line raises, so the audit never goes quiet.
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
    """Parse every non-comment, non-blank line into an :class:`IgnoreEntry`."""
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
    """Run the ignore-list check from the command line."""
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
        today = datetime.datetime.now(datetime.UTC).date()
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
