"""Print the lines of a push or an upload, as the CLI does.
Pass the ``print_report`` fixture as ``report`` and read the lines with ``capsys``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from portolan_cli.sync.report import Report

__all__ = ["print_report_fixture"]


@pytest.fixture(name="print_report")
def print_report_fixture() -> Report:
    """Return the callback that prints each line through ``portolan_cli.output``."""
    from portolan_cli.cli import _report

    return _report
