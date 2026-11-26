"""Print the lines of a push or an upload, as the CLI does.

Push and upload print nothing. They send each line to the ``report`` callback
that the caller passes (``portolan_cli.sync.report``). A test that calls them
directly and reads the lines with ``capsys`` passes the ``print_report``
fixture as ``report``.
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
