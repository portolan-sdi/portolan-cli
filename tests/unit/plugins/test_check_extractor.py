"""Tests for `portolan_cli.testing.check_extractor` (#947)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import click
import pytest

from portolan_cli.testing import ConformanceError, check_extractor

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

pytestmark = pytest.mark.unit


@click.command("other")
def _other() -> None:
    """A command with the wrong name."""


class _Mismatched:
    name = "mine"
    summary = "A source whose command has another name."

    def extract(self, source: str, output_dir: Path, **options: Any) -> Any:
        return None

    def cli_command(self) -> click.Command:
        return _other


def test_builtin_extractors_conform(register_plugin: Callable[..., None]) -> None:
    from portolan_cli.plugins.extractors import registered_extractors

    for extractor in registered_extractors().values():
        check_extractor(extractor)


def test_command_name_must_match(tmp_path: Path) -> None:
    with pytest.raises(
        ConformanceError, match=r"cli_command\(\) is named 'other', expected 'mine'"
    ):
        check_extractor(_Mismatched())


def test_extract_must_return_a_report(tmp_path: Path) -> None:
    extractor = _Mismatched()
    extractor.cli_command = lambda: click.Command("mine")  # type: ignore[method-assign]

    with pytest.raises(
        ConformanceError, match=r"extract\(\) returned NoneType, expected ExtractionReport"
    ):
        check_extractor(extractor, "https://example.org", tmp_path)


def test_object_must_be_an_extractor() -> None:
    with pytest.raises(ConformanceError, match=r"object does not implement Extractor"):
        check_extractor(object())
