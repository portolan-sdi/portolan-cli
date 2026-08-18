"""Temporal extent handling for STAC items.

- Default to null (open temporal interval) when --datetime not provided
- Publish the sentinel start/end range for a null datetime, which is what says
  the temporal extent is unknown (no marker field travels with it: the spec
  defines no portolan: property, issue #654)
- Accept flexible datetime formats (ISO 8601, YYYY-MM-DD, space-separated)
"""

from __future__ import annotations

from datetime import datetime, timezone

import click


def ensure_utc_aware(dt: datetime | None) -> datetime | None:
    """Ensure a datetime is timezone-aware (UTC).

    Converts naive datetimes to UTC-aware. Required for STAC compliance and
    to avoid comparison errors between naive and aware datetimes.

    Args:
        dt: Datetime to normalize, or None.

    Returns:
        UTC-aware datetime, or None if input is None.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        # Naive datetime - assume UTC
        return dt.replace(tzinfo=timezone.utc)
    return dt


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


class FlexibleDateTime(click.ParamType["datetime | None"]):
    """Click parameter type for flexible datetime parsing.

    Accepts multiple formats and returns None for empty input.

    ``ParamType`` is generic in the converted value from click 8.4 on; the
    parameter is a string annotation because the class is not subscriptable at
    runtime in that release's stubs-only form.
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
