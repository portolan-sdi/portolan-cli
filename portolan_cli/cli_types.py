"""Click parameter types for the CLI layer."""

from __future__ import annotations

from datetime import datetime

import click

from portolan_cli.temporal import parse_flexible_datetime


class FlexibleDateTime(click.ParamType["datetime | None"]):
    """Click parameter type for flexible datetime parsing."""

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

        if value is None or value.strip() == "":
            return None

        try:
            return parse_flexible_datetime(value)
        except ValueError as e:
            self.fail(str(e), param, ctx)


# Singleton instance for use in Click options
FLEXIBLE_DATETIME = FlexibleDateTime()
