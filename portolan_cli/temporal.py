"""Temporal extent handling for STAC items."""

from __future__ import annotations

from datetime import UTC, datetime


def ensure_utc_aware(dt: datetime | None) -> datetime | None:
    """Ensure a datetime is timezone-aware (UTC)."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        # Naive datetime - assume UTC
        return dt.replace(tzinfo=UTC)
    return dt


def parse_flexible_datetime(value: str | None) -> datetime | None:
    """Parse a datetime string with flexible format support."""
    if value is None or value.strip() == "":
        return None

    value = value.strip()

    # An explicit offset wins, because the caller stated the zone. "%z" also
    # accepts a "Z" suffix from Python 3.7 on, so it covers both ISO spellings.
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z")
    except ValueError:
        pass

    for fmt in (
        "%Y-%m-%dT%H:%M:%S",  # ISO without a zone
        "%Y-%m-%d %H:%M:%S",  # Space-separated
        "%Y-%m-%d",  # Date only
    ):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue

    raise ValueError(f"Invalid datetime format: {value!r}. Use ISO 8601 (e.g., 2024-01-15)")
