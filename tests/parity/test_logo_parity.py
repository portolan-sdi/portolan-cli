"""Command parity for `logo`.

Only the failure path holds parity today. On success, `cli.py` builds the
envelope data from `LogoResult` field by field. It renames `media_type` to
`type`, makes `path` relative, and drops `changed`. So `logo` stays in
`PARITY_PENDING` until `LogoResult.to_dict()` returns that data.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from portolan_cli.errors import LogoSourceNotFoundError
from portolan_cli.logo import set_catalog_logo

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration


def test_logo_missing_source(parity: Parity, build_conformant_catalog: Callable[..., Path]) -> None:
    """A missing source fails both sides with the same error type, message, and code."""
    parity.error(
        "logo",
        ["missing.png"],
        catalog=build_conformant_catalog,
        call=lambda root: set_catalog_logo(root, "missing.png", title=None),
        raises=LogoSourceNotFoundError,
    )
