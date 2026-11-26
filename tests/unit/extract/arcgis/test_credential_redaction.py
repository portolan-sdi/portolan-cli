"""A token in an ArcGIS URL never reaches a result, a repr, an error, or a message.

The Codex review of #1133 found the raw ImageServer URL in the ``repr()`` of
``ImageServerRunResult``. A repr reaches tracebacks and logs.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.extract.arcgis.imageserver.discovery import (
    ImageServerDiscoveryError,
    _validate_required_fields,
)
from portolan_cli.extract.arcgis.imageserver.extractor import ImageServerExtractionError
from portolan_cli.extract.arcgis.imageserver.orchestrator import (
    ImageServerCLIOptions,
    run_imageserver_extraction,
)
from portolan_cli.extract.arcgis.orchestrator import ExtractionOptions as ArcGISOptions
from portolan_cli.extract.arcgis.orchestrator import run_extract_arcgis
from portolan_cli.extract.carto.orchestrator import ExtractionOptions as CartoOptions

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.unit

TOKEN_URL = "https://example.com/arcgis/rest/services/x/ImageServer?token=SECRET"
RUN = "portolan_cli.extract.arcgis.imageserver.orchestrator.run_imageserver"


async def _no_tiles(*_args: object, **_kwargs: object) -> tuple[int, None]:
    return 0, None


async def _leaky_error(*_args: object, **_kwargs: object) -> tuple[int, None]:
    """Stand in for a lower layer that quotes the raw URL in its error."""
    raise ImageServerExtractionError(f"Failed to read {TOKEN_URL}")


def test_imageserver_result_holds_no_token(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(RUN, _no_tiles)

    result = run_extract_arcgis(
        TOKEN_URL, tmp_path, imageserver=ImageServerCLIOptions(dry_run=True)
    )

    assert "SECRET" not in repr(result)
    assert "SECRET" not in str(result)
    assert "SECRET" not in json.dumps(result.to_dict())


@pytest.mark.parametrize("output_format", ["text", "json"])
def test_cli_error_path_prints_no_token(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, output_format: str
) -> None:
    monkeypatch.setattr(RUN, _leaky_error)

    result = CliRunner().invoke(
        cli,
        [
            "--format",
            output_format,
            "extract",
            "arcgis",
            TOKEN_URL,
            str(tmp_path / "out"),
            "--auto",
        ],
    )

    assert result.exit_code == 1
    assert "Failed to read" in result.stderr
    assert "SECRET" not in result.stdout
    assert "SECRET" not in result.stderr


@pytest.mark.asyncio
async def test_compatibility_wrapper_message_holds_no_token(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(RUN, _leaky_error)

    result = await run_imageserver_extraction(TOKEN_URL, tmp_path)

    assert result == (1, None)
    assert [r.levelname for r in caplog.records] == ["ERROR"]
    assert "Failed to read" in caplog.text
    assert "SECRET" not in caplog.text


def test_options_repr_holds_no_credential() -> None:
    assert "SECRET" not in repr(ArcGISOptions(token="SECRET"))
    assert "SECRET" not in repr(CartoOptions(api_key="SECRET"))
    assert "SECRET" not in repr(ArcGISOptions(token="SECRET").client())


def test_missing_field_error_holds_no_token() -> None:
    with pytest.raises(ImageServerDiscoveryError) as caught:
        _validate_required_fields({}, TOKEN_URL)

    assert "Missing required field 'name'" in str(caught.value)
    assert "SECRET" not in str(caught.value)
