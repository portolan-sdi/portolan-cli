"""Tests for the emit helpers (dual-mode JSON/human output).

These pin the helpers to behavior byte-identical to the inline
``if use_json: ... else: ...`` blocks they replace in ``cli.py``:
- JSON mode echoes ``envelope.to_json()`` exactly.
- Text mode routes through ``output.error`` / renders nothing for success.
"""

from __future__ import annotations

import json

import pytest

from portolan_cli import output
from portolan_cli.emit import emit_error, emit_success
from portolan_cli.json_output import ErrorDetail, error_envelope, success_envelope

pytestmark = pytest.mark.unit


class TestEmitError:
    def test_json_mode_is_byte_identical_to_envelope(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """JSON mode output equals the prior inline error_envelope block."""
        emit_error("scan", "PathNotFoundError", "Directory does not exist: /x", use_json=True)
        captured = capsys.readouterr().out

        expected = (
            error_envelope(
                "scan",
                [ErrorDetail(type="PathNotFoundError", message="Directory does not exist: /x")],
            ).to_json()
            + "\n"
        )
        assert captured == expected

    def test_json_mode_parses_to_expected_structure(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """The emitted JSON has the expected envelope fields."""
        emit_error("scan", "NotADirectoryError", "Path is not a directory: /x", use_json=True)
        payload = json.loads(capsys.readouterr().out)

        assert payload["success"] is False
        assert payload["command"] == "scan"
        assert payload["errors"] == [
            {"type": "NotADirectoryError", "message": "Path is not a directory: /x"}
        ]

    def test_json_mode_includes_code_when_provided(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """An explicit code propagates into ErrorDetail.code."""
        emit_error("clone", "CloneError", "bad url", use_json=True, code="INVALID_URL")
        payload = json.loads(capsys.readouterr().out)

        assert payload["errors"][0]["code"] == "INVALID_URL"

    def test_json_mode_omits_code_by_default(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Without a code, the key is absent (matches ErrorDetail.to_dict)."""
        emit_error("scan", "FileNotFoundError", "missing", use_json=True)
        payload = json.loads(capsys.readouterr().out)

        assert "code" not in payload["errors"][0]

    def test_text_mode_routes_through_output_error(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Text mode output equals a direct output.error call, and emits no JSON."""
        emit_error("scan", "PathNotFoundError", "Directory does not exist: /x", use_json=False)
        captured = capsys.readouterr().out

        output.error("Directory does not exist: /x")
        expected = capsys.readouterr().out

        assert captured == expected
        assert "{" not in captured  # no JSON envelope leaked


class TestEmitSuccess:
    def test_json_mode_is_byte_identical_to_envelope(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """JSON mode output equals the prior inline success_envelope block."""
        data = {"collections": []}
        result = emit_success("status", data, use_json=True)
        captured = capsys.readouterr().out

        assert result is True
        assert captured == success_envelope("status", data).to_json() + "\n"

    def test_json_mode_returns_true(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Returning True signals the caller to skip human output."""
        assert emit_success("status", {"collections": []}, use_json=True) is True
        capsys.readouterr()

    def test_text_mode_returns_false_and_emits_nothing(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Text mode prints nothing and returns False so the caller renders."""
        result = emit_success("status", {"collections": []}, use_json=False)
        captured = capsys.readouterr().out

        assert result is False
        assert captured == ""
