"""Temporal extent handling for STAC items.

Per ADR-0035:
- Default to null (not current time)
- Accept flexible datetime formats
- Prompt in interactive mode
- Flag incomplete items in portolan check
"""

from __future__ import annotations

from datetime import datetime, timezone

import click


def parse_flexible_datetime(value: str | None) -> datetime | None:
    """Parse a datetime string with flexible format support.

    Accepts:
    - ISO 8601: 2024-01-15T10:30:00Z
    - ISO without Z: 2024-01-15T10:30:00
    - Date only: 2024-01-15
    - Space-separated: 2024-01-15 10:30:00

    Args:
        value: Datetime string or None.

    Returns:
        Parsed datetime or None if input is empty/None.

    Raises:
        ValueError: If format is invalid.
    """
    if value is None or value.strip() == "":
        return None

    value = value.strip()

    # Try formats in order of specificity
    formats = [
        "%Y-%m-%dT%H:%M:%SZ",  # ISO with Z
        "%Y-%m-%dT%H:%M:%S%z",  # ISO with timezone
        "%Y-%m-%dT%H:%M:%S",  # ISO without TZ
        "%Y-%m-%d %H:%M:%S",  # Space-separated
        "%Y-%m-%d",  # Date only
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(value, fmt)
            # Add UTC timezone if missing and format had Z
            if fmt.endswith("Z") and dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue

    raise ValueError(f"Invalid datetime format: {value!r}. Use ISO 8601 (e.g., 2024-01-15)")


class FlexibleDateTime(click.ParamType):
    """Click parameter type for flexible datetime parsing.

    Accepts multiple formats and returns None for empty input.
    """

    name = "datetime"

    def convert(
        self,
        value: str | datetime | None,
        param: click.Parameter | None,
        ctx: click.Context | None,
    ) -> datetime | None:
        """Convert string to datetime."""
        if isinstance(value, datetime):
            return value

        if value is None or (isinstance(value, str) and value.strip() == ""):
            return None

        try:
            return parse_flexible_datetime(value)
        except ValueError as e:
            self.fail(str(e), param, ctx)


# Singleton instance for use in Click options
FLEXIBLE_DATETIME = FlexibleDateTime()
