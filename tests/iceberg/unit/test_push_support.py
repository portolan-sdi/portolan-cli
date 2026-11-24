"""``push`` is blocked on the Iceberg backend: ``add`` already wrote the rows."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from tests.iceberg.fake_lake import Lake

pytestmark = pytest.mark.unit


def test_supports_push_is_false(lake: Lake) -> None:
    result = lake.backend.supports_push()

    assert isinstance(result, bool)
    assert result is False


def test_the_message_with_a_remote_says_add_already_uploads(lake: Lake) -> None:
    message = lake.backend.push_blocked_message(remote="gs://bucket/catalog")

    assert message == (
        "Push is not needed with the 'iceberg' backend. "
        "The `add` command already uploads data to the configured remote."
    )


def test_the_message_without_a_remote_says_push_is_not_supported(lake: Lake) -> None:
    message = lake.backend.push_blocked_message(remote=None)

    assert message == (
        "Push is not supported with the 'iceberg' backend. "
        "The iceberg backend manages versions through its catalog."
    )
