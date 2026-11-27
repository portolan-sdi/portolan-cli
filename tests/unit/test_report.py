"""Tests for the report callback of the library."""

from __future__ import annotations

import logging

import pytest

from portolan_cli.report import emit, reporting

pytestmark = pytest.mark.unit


def test_emit_goes_to_the_callback_of_the_block() -> None:
    lines: list[tuple[str, str]] = []
    with reporting(lambda level, text: lines.append((level, text))):
        emit("warn", "one")
        with reporting(None):
            emit("info", "two")
    emit("info", "three")

    assert lines == [("warn", "one"), ("info", "two")]


def test_emit_logs_without_a_callback(
    caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    with caplog.at_level(logging.DEBUG, logger="portolan_cli.extract.common.progress"):
        emit("warn", "kept")
        emit("detail", "rendered")

    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.WARNING, "kept"),
        (logging.INFO, "rendered"),
    ]
    assert capsys.readouterr() == ("", "")
