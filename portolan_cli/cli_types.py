"""Click parameter types for the CLI layer.

These types import Click, so they live outside the library modules they wrap.
``temporal.parse_flexible_datetime`` does the parsing (#947).
"""

from __future__ import annotations

from datetime import datetime

import click

from portolan_cli.temporal import parse_flexible_datetime


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
