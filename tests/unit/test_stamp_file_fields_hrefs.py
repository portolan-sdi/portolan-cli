"""``stamp_file_fields`` leaves an absolute href untouched, encoded or not."""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import quote

import pytest

from portolan_cli.stac_parquet import _is_relative_href, stamp_file_fields

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.unit

_FIELDS = {"file:size": 5, "file:checksum": "1220" + "00" * 32}


@pytest.mark.parametrize("exists", [False, True])
def test_an_encoded_absolute_href_is_left_untouched(tmp_path: Path, exists: bool) -> None:
    """The bytes are outside the collection, so the fields cannot be checked."""
    outside = tmp_path / "outside.parquet"
    if exists:
        outside.write_bytes(b"PAR1 outside")
    collection_dir = tmp_path / "catalog" / "roads"
    collection_dir.mkdir(parents=True)
    asset = {"href": quote(outside.as_posix(), safe=""), **_FIELDS}

    assert stamp_file_fields(asset, collection_dir) is False
    assert asset == {"href": quote(outside.as_posix(), safe=""), **_FIELDS}


@pytest.mark.parametrize(
    "href", ["report%3A2024.parquet", "./report%3A2024.parquet", "a/b%3Ac.tif"]
)
def test_an_encoded_colon_in_a_relative_file_name_is_relative(href: str) -> None:
    """A decoded colon is a file name character, not a URI scheme."""
    assert _is_relative_href(href)


@pytest.mark.parametrize(
    "href", ["%2Ftmp%2Fx.parquet", "C%3A/x.parquet", "%5Cx.parquet", "s3://b/x"]
)
def test_an_absolute_href_is_not_relative(href: str) -> None:
    """An encoded absolute path, an encoded drive, and a URL are all absolute."""
    assert not _is_relative_href(href)
