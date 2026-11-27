"""Download_file sends its transfer lines to a report callback, or to the log."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest
from obstore.store import MemoryStore

from portolan_cli.sync.download import download_file

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

pytestmark = pytest.mark.unit


@pytest.fixture
def store() -> Iterator[MemoryStore]:
    """Route the store setup of download.py to one MemoryStore that holds one object."""
    memory = MemoryStore()
    memory.put("catalog/data.parquet", b"x" * 2048)
    with patch(
        "portolan_cli.sync.download._setup_store_and_kwargs", return_value=(memory, {})
    ) as setup:
        yield memory
    assert setup.called, "download_file did not reach the patched store setup"


@pytest.mark.usefixtures("store")
def test_a_report_receives_the_download_lines(tmp_path: Path) -> None:
    """With a report, the start and the end of the download go to it."""
    lines: list[tuple[str, str]] = []

    def record(level: Any, message: str) -> None:
        lines.append((level, message))

    result = download_file(
        "s3://bucket/catalog/data.parquet", tmp_path / "data.parquet", report=record
    )

    assert result.files_downloaded == 1
    assert (tmp_path / "data.parquet").read_bytes() == b"x" * 2048
    assert [level for level, _ in lines] == ["info", "success"]
    assert lines[0][1] == "Downloading data.parquet (0.00 MB) <- catalog/data.parquet"
    assert re.fullmatch(r"Download complete \(\d+\.\d\d MB/s\)", lines[1][1])


@pytest.mark.usefixtures("store")
def test_no_report_prints_nothing(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Without a report, download_file writes nothing to stdout or stderr."""
    result = download_file("s3://bucket/catalog/data.parquet", tmp_path / "data.parquet")

    assert result.files_downloaded == 1
    assert capsys.readouterr() == ("", "")
