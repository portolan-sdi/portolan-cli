"""`format_size` lives in utils, so the library can name a file size.

It used to live in `cli.py`. The in-place GeoParquet rewrite reports the size
of the file it is about to read and write, and a library module must not import
the CLI layer (issue #805). Moving it keeps the import contract intact.
"""

from __future__ import annotations

import pytest

from portolan_cli.utils import format_size

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("size_bytes", "expected"),
    [
        (0, "0B"),
        (100, "100B"),
        (1023, "1023B"),
        (1024, "1.0KB"),
        (1536, "1.5KB"),
        (1024 * 1024, "1.0MB"),
        (4 * 1024 * 1024 + 209715, "4.2MB"),
        (1024 * 1024 * 1024, "1.0GB"),
        (3 * 1024 * 1024 * 1024, "3.0GB"),
    ],
)
def test_format_size_covers_every_unit(size_bytes: int, expected: str) -> None:
    """Each threshold reports in the unit above it."""
    assert format_size(size_bytes) == expected


def test_cli_still_exposes_it() -> None:
    """`cli.format_size` keeps working, so callers do not have to move."""
    from portolan_cli.cli import format_size as cli_format_size

    assert cli_format_size is format_size
