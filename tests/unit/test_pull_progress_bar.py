"""The pull library draws a progress bar only when the caller asks for it.

A ``report`` callback carries messages only. ``show_progress`` alone turns the
bar on. Both streams look like a terminal here, so the bar would draw if the
library chose to draw it.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest

from portolan_cli.sync.pull import _download_assets_async

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.unit

ASSETS: dict[str, dict[str, str | int]] = {
    f"file_{i}.parquet": {"sha256": "x", "size_bytes": 1000, "href": f"c/file_{i}.parquet"}
    for i in range(3)
}


async def _download_on_a_terminal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, **kwargs: Any
) -> tuple[int, int]:
    """Download ``ASSETS`` with a fake transfer while stdout and stderr report a terminal.

    The patch runs inside the test call. pytest swaps the capture streams
    between the setup and the call, so a fixture would patch the wrong object.
    """
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True, raising=False)
    monkeypatch.setattr(sys.stderr, "isatty", lambda: True, raising=False)

    async def fake_download(store: Any, key: str, path: Path) -> tuple[bool, int]:
        return True, 1000

    with patch("portolan_cli.sync.pull._download_file_async", fake_download):
        return await _download_assets_async(
            store=object(),  # type: ignore[arg-type]
            prefix="catalog",
            local_root=tmp_path,
            files_to_download=list(ASSETS),
            remote_assets=ASSETS,
            **kwargs,
        )


async def test_a_report_callback_alone_draws_no_bar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A caller that passes only ``report`` gets no terminal output."""
    lines: list[tuple[str, str]] = []

    counts = await _download_on_a_terminal(
        tmp_path, monkeypatch, report=lambda level, msg: lines.append((level, msg))
    )

    assert counts == (3, 0)
    assert capsys.readouterr() == ("", "")


async def test_show_progress_draws_the_bar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """``show_progress=True`` draws the bar on stderr, so the terminal patch works."""
    counts = await _download_on_a_terminal(tmp_path, monkeypatch, show_progress=True)

    assert counts == (3, 0)
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "Downloading" in captured.err
