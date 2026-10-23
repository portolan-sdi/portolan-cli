"""Unit tests for the logging setup (issue #991)."""

from __future__ import annotations

import io
import json
import logging
import os
import re
import subprocess
import sys
import warnings
from typing import TYPE_CHECKING

import pytest

from portolan_cli import logging_setup, trace

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

pytestmark = pytest.mark.unit

PKG = logging.getLogger("portolan_cli")


@pytest.fixture(autouse=True)
def default_root_level() -> Iterator[None]:
    """Start from Python's default root level, and restore the level after.

    Another test in the same worker can leave the root logger at a different
    level. These tests assert levels relative to the default.
    """
    root = logging.getLogger()
    saved = root.level
    root.setLevel(logging.WARNING)
    yield
    root.setLevel(saved)


def _our_handlers() -> list[logging.Handler]:
    return [h for h in logging.getLogger().handlers if getattr(h, "_portolan", False)]


class TestLevel:
    @pytest.mark.parametrize(
        ("verbosity", "quiet", "env", "expected"),
        [
            (0, False, None, logging.WARNING),
            (1, False, None, logging.INFO),
            (2, False, None, logging.DEBUG),
            (3, False, None, logging.DEBUG),
            (0, True, None, logging.ERROR),
            (0, False, "debug", logging.DEBUG),
            (0, False, "INFO", logging.INFO),
            (0, False, "warning", logging.WARNING),
            (0, False, "nonsense", logging.WARNING),
            # A flag wins over the environment variable.
            (1, False, "debug", logging.INFO),
            (0, True, "debug", logging.ERROR),
        ],
    )
    def test_level_mapping(
        self,
        monkeypatch: pytest.MonkeyPatch,
        verbosity: int,
        quiet: bool,
        env: str | None,
        expected: int,
    ) -> None:
        if env is not None:
            monkeypatch.setenv("PORTOLAN_LOG", env)
        logging_setup.configure_logging(verbosity=verbosity, quiet=quiet)
        assert PKG.getEffectiveLevel() == expected

    def test_default_leaves_third_party_at_warning(self) -> None:
        logging_setup.configure_logging(verbosity=1, quiet=False)
        assert logging.getLogger().level == logging.WARNING
        assert not logging.getLogger("urllib3").isEnabledFor(logging.DEBUG)

    def test_vv_sets_third_party_debug_and_captures_warnings(self) -> None:
        logging_setup.configure_logging(verbosity=2, quiet=False)
        for name in ("urllib3", "httpx", "obstore"):
            assert logging.getLogger(name).level == logging.DEBUG
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        logging.getLogger("py.warnings").addHandler(handler)
        try:
            warnings.showwarning("captured-warning", UserWarning, "f.py", 1)
        finally:
            logging.getLogger("py.warnings").removeHandler(handler)
        assert "captured-warning" in stream.getvalue()


class TestHandlers:
    def test_writes_to_stderr_only(self, capsys: pytest.CaptureFixture[str]) -> None:
        logging_setup.configure_logging(verbosity=2, quiet=False)
        logging.getLogger("portolan_cli.add").debug("hello %s", "world")
        captured = capsys.readouterr()
        assert captured.out == ""
        assert re.search(
            r"^\+\d+\.\d{3}s DEBUG portolan_cli\.add: hello world$", captured.err, re.MULTILINE
        )

    def test_handler_follows_swapped_stderr(self, monkeypatch: pytest.MonkeyPatch) -> None:
        logging_setup.configure_logging(verbosity=1, quiet=False)
        swapped = io.StringIO()
        monkeypatch.setattr(sys, "stderr", swapped)
        logging.getLogger("portolan_cli.x").info("late stream")
        assert "late stream" in swapped.getvalue()

    def test_repeat_calls_are_idempotent(self) -> None:
        logging_setup.configure_logging(verbosity=1, quiet=False)
        logging_setup.configure_logging(verbosity=2, quiet=False)
        assert len(_our_handlers()) == 1

    def test_reset_removes_handlers(self) -> None:
        logging_setup.configure_logging(verbosity=2, quiet=False)
        logging_setup.reset_logging()
        assert _our_handlers() == []
        assert PKG.level == logging.NOTSET

    def test_log_file_writes_json_lines_with_run_id(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = tmp_path / "log.jsonl"
        monkeypatch.setenv("PORTOLAN_LOG_FILE", str(path))
        logging_setup.configure_logging(verbosity=2, quiet=False)
        logging.getLogger("portolan_cli.sync").debug("one")
        try:
            raise ValueError("boom")
        except ValueError:
            logging.getLogger("portolan_cli.sync").exception("two")
        logging_setup.reset_logging()
        records = [json.loads(line) for line in path.read_text().splitlines()]
        assert [r["msg"] for r in records] == ["one", "two"]
        assert {r["run_id"] for r in records} == {trace.run_id()}
        assert records[0]["level"] == "DEBUG"
        assert records[0]["logger"] == "portolan_cli.sync"
        assert "ValueError: boom" in records[1]["exc"]
        assert "rel" in records[0] and "ts" in records[0]


class TestUserFormat:
    """At the default level a portolan_cli record reads as an output line."""

    def test_a_warning_reads_as_a_warn_line(self, capsys: pytest.CaptureFixture[str]) -> None:
        logging_setup.configure_logging(verbosity=0, quiet=False)
        logging.getLogger("portolan_cli.config").warning("unknown sort '%s'", "bogus")
        assert capsys.readouterr() == ("", "\u26a0 unknown sort 'bogus'\n")

    def test_an_error_reads_as_an_error_line(self, capsys: pytest.CaptureFixture[str]) -> None:
        logging_setup.configure_logging(verbosity=0, quiet=True)
        logging.getLogger("portolan_cli.push").error("upload failed")
        assert capsys.readouterr() == ("", "\u2717 upload failed\n")

    def test_the_traceback_waits_for_verbose(self, capsys: pytest.CaptureFixture[str]) -> None:
        logging_setup.configure_logging(verbosity=0, quiet=False)
        try:
            raise ValueError("boom")
        except ValueError:
            logging.getLogger("portolan_cli.add").warning("kept the file", exc_info=True)
        assert capsys.readouterr().err == "\u26a0 kept the file\n"

    def test_verbose_keeps_the_record_format(self, capsys: pytest.CaptureFixture[str]) -> None:
        logging_setup.configure_logging(verbosity=1, quiet=False)
        logging.getLogger("portolan_cli.config").warning("unknown sort")
        assert re.fullmatch(
            r"\+\d+\.\d{3}s WARNING portolan_cli\.config: unknown sort\n", capsys.readouterr().err
        )

    def test_a_third_party_warning_keeps_the_record_format(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        logging_setup.configure_logging(verbosity=0, quiet=False)
        logging.getLogger("urllib3").warning("retrying")
        assert re.fullmatch(r"\+\d+\.\d{3}s WARNING urllib3: retrying\n", capsys.readouterr().err)


class TestGdalDebug:
    def test_sets_cpl_debug(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CPL_DEBUG", raising=False)
        monkeypatch.setenv("PORTOLAN_GDAL_DEBUG", "1")
        logging_setup.configure_logging(verbosity=0, quiet=False)
        assert os.environ["CPL_DEBUG"] == "ON"

    def test_unset_leaves_cpl_debug(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CPL_DEBUG", raising=False)
        logging_setup.configure_logging(verbosity=0, quiet=False)
        assert "CPL_DEBUG" not in os.environ

    def test_gdal_debug_records_reach_stderr(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Rasterio logs GDAL debug messages at DEBUG on ``rasterio._env``."""
        monkeypatch.delenv("CPL_DEBUG", raising=False)
        monkeypatch.setenv("PORTOLAN_GDAL_DEBUG", "1")
        logging_setup.configure_logging(verbosity=0, quiet=False)
        logging.getLogger("rasterio._env").debug("CPLE_None in GDAL: GDALOpen(x.tif) succeeds")
        assert re.search(
            r"^\+\d+\.\d{3}s DEBUG rasterio\._env: CPLE_None in GDAL: GDALOpen\(x\.tif\) succeeds$",
            capsys.readouterr().err,
            re.MULTILINE,
        )

    def test_gdal_records_stay_hidden_without_gdal_debug(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.delenv("CPL_DEBUG", raising=False)
        logging_setup.configure_logging(verbosity=2, quiet=False)
        logging.getLogger("rasterio._env").debug("hidden gdal record")
        assert "hidden gdal record" not in capsys.readouterr().err

    def test_reset_restores_gdal_logger_level(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CPL_DEBUG", raising=False)
        monkeypatch.setenv("PORTOLAN_GDAL_DEBUG", "1")
        logging_setup.configure_logging(verbosity=0, quiet=False)
        logging_setup.reset_logging()
        assert logging.getLogger("rasterio._env").level == logging.NOTSET


def test_package_logger_has_null_handler() -> None:
    """A fresh interpreter sees the NullHandler that the package adds on import."""
    code = (
        "import logging, portolan_cli; "
        "print(any(isinstance(h, logging.NullHandler) "
        "for h in logging.getLogger('portolan_cli').handlers))"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert result.stdout.strip() == "True"
