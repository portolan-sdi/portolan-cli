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
        A timezone-aware datetime, or None if the input is empty or None. An
        input without an offset reads as UTC.

    Raises:
        ValueError: If format is invalid.
    """
    if value is None or value.strip() == "":
        return None

    value = value.strip()

    # An explicit offset wins, because the caller stated the zone. "%z" also
    # accepts a "Z" suffix from Python 3.7 on, so it covers both ISO spellings.
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z")
    except ValueError:
        pass

    # The remaining formats carry no zone. STAC requires an RFC 3339 timestamp
    # with an offset, so a naive result reads as UTC. Without this the CLI
    # published a naive datetime for "--datetime 2024-01-15", and a later
    # comparison against an aware datetime raised TypeError.
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",  # ISO without a zone
        "%Y-%m-%d %H:%M:%S",  # Space-separated
        "%Y-%m-%d",  # Date only
    ):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
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
