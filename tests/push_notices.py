"""Print the notices of a push or an upload, as the CLI does.

Push and upload print nothing. They send each message to the notify callback
of the run (``portolan_cli.sync.notices``). A test that calls them directly and
reads the messages with ``capsys`` takes the ``_print_notices`` fixture.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = ["print_notices_fixture"]


@pytest.fixture(name="_print_notices")
def print_notices_fixture() -> Iterator[None]:
    """Print each notice through ``portolan_cli.output``, as ``portolan push`` does."""
    from portolan_cli.cli import _print_push_notice
    from portolan_cli.sync.notices import notifying

    with notifying(lambda level, message: _print_push_notice(level, message, verbose=False)):
        yield
