"""Tests for JSON output envelope module.

These tests verify the OutputEnvelope dataclass and factory functions
for consistent JSON output across all CLI commands.
"""

from __future__ import annotations

import json

import pytest

# These imports will fail until we implement the module
from portolan_cli.json_output import (
    ErrorDetail,
    OutputEnvelope,
    error_envelope,
    success_envelope,
)


class TestOutputEnvelope:
    """Tests for OutputEnvelope dataclass."""

    @pytest.mark.unit
    def test_envelope_has_required_fields(self) -> None:
        """OutputEnvelope should have success, command, data fields."""
        envelope = OutputEnvelope(success=True, command="test", data={"key": "value"})

        assert envelope.success is True
        assert envelope.command == "test"
        assert envelope.data == {"key": "value"}

    @pytest.mark.unit
    def test_envelope_errors_optional(self) -> None:
        """OutputEnvelope errors field should be optional (defaults to None)."""
        envelope = OutputEnvelope(success=True, command="test", data={})

        assert envelope.errors is None

    @pytest.mark.unit
    def test_envelope_errors_can_be_set(self) -> None:
        """OutputEnvelope errors field can contain ErrorDetail list."""
        errors = [ErrorDetail(type="TestError", message="Test message")]
        envelope = OutputEnvelope(success=False, command="test", data={}, errors=errors)

        assert envelope.errors is not None
        assert len(envelope.errors) == 1
        assert envelope.errors[0].type == "TestError"

    @pytest.mark.unit
    def test_to_dict_returns_dict(self) -> None:
        """to_dict() should return a dictionary with all fields."""
        envelope = OutputEnvelope(success=True, command="scan", data={"count": 5})

        result = envelope.to_dict()

        assert isinstance(result, dict)
        assert result["success"] is True
        assert result["command"] == "scan"
        assert result["data"] == {"count": 5}

    @pytest.mark.unit
    def test_to_dict_excludes_none_errors(self) -> None:
        """to_dict() should exclude errors field when None."""
        envelope = OutputEnvelope(success=True, command="init", data={})

        result = envelope.to_dict()

        assert "errors" not in result

    @pytest.mark.unit
    def test_to_dict_includes_errors_when_present(self) -> None:
        """to_dict() should include errors field when present."""
        errors = [ErrorDetail(type="FileNotFoundError", message="File not found")]
        envelope = OutputEnvelope(success=False, command="scan", data={}, errors=errors)

        result = envelope.to_dict()

        assert "errors" in result
        assert len(result["errors"]) == 1
        assert result["errors"][0]["type"] == "FileNotFoundError"
        assert result["errors"][0]["message"] == "File not found"

    @pytest.mark.unit
    def test_to_json_returns_valid_json(self) -> None:
        """to_json() should return a valid JSON string."""
        envelope = OutputEnvelope(success=True, command="check", data={"passed": True})

        json_str = envelope.to_json()

        # Should be parseable
        parsed = json.loads(json_str)
        assert parsed["success"] is True
        assert parsed["command"] == "check"

    @pytest.mark.unit
    def test_to_json_with_indent(self) -> None:
        """to_json() should support custom indentation."""
        envelope = OutputEnvelope(success=True, command="test", data={})

        # With indent=2
        json_str = envelope.to_json(indent=2)

        # Should have newlines (indented)
        assert "\n" in json_str

        # Without indent
        json_str_compact = envelope.to_json(indent=None)

        # Should be compact (no newlines for simple objects)
        # Note: Still valid JSON
        parsed = json.loads(json_str_compact)
        assert parsed["success"] is True


class TestErrorDetail:
    """Tests for ErrorDetail dataclass."""

    @pytest.mark.unit
    def test_error_detail_has_required_fields(self) -> None:
        """ErrorDetail should have type and message fields."""
        error = ErrorDetail(type="ValueError", message="Invalid value")

        assert error.type == "ValueError"
        assert error.message == "Invalid value"

    @pytest.mark.unit
    def test_error_detail_to_dict(self) -> None:
        """ErrorDetail should convert to dict correctly."""
        error = ErrorDetail(type="KeyError", message="Key not found")

        result = error.to_dict()

        assert result == {"type": "KeyError", "message": "Key not found"}


class TestSuccessEnvelope:
    """Tests for success_envelope factory function."""

    @pytest.mark.unit
    def test_success_envelope_sets_success_true(self) -> None:
        """success_envelope() should set success=True."""
        envelope = success_envelope("test", {"result": "ok"})

        assert envelope.success is True

    @pytest.mark.unit
    def test_success_envelope_sets_command(self) -> None:
        """success_envelope() should set command name."""
        envelope = success_envelope("scan", {})

        assert envelope.command == "scan"

    @pytest.mark.unit
    def test_success_envelope_sets_data(self) -> None:
        """success_envelope() should set data payload."""
        data = {"files": ["a.parquet", "b.parquet"]}
        envelope = success_envelope("scan", data)

        assert envelope.data == data

    @pytest.mark.unit
    def test_success_envelope_errors_none(self) -> None:
        """success_envelope() should have errors=None."""
        envelope = success_envelope("init", {})

        assert envelope.errors is None


class TestErrorEnvelope:
    """Tests for error_envelope factory function."""

    @pytest.mark.unit
    def test_error_envelope_sets_success_false(self) -> None:
        """error_envelope() should set success=False."""
        errors = [ErrorDetail(type="Error", message="Failed")]
        envelope = error_envelope("test", errors)

        assert envelope.success is False

    @pytest.mark.unit
    def test_error_envelope_sets_command(self) -> None:
        """error_envelope() should set command name."""
        errors = [ErrorDetail(type="Error", message="Failed")]
        envelope = error_envelope("scan", errors)

        assert envelope.command == "scan"

    @pytest.mark.unit
    def test_error_envelope_sets_errors(self) -> None:
        """error_envelope() should set errors list."""
        errors = [
            ErrorDetail(type="FileNotFoundError", message="File not found"),
            ErrorDetail(type="ValueError", message="Invalid format"),
        ]
        envelope = error_envelope("scan", errors)

        assert envelope.errors is not None
        assert len(envelope.errors) == 2

    @pytest.mark.unit
    def test_error_envelope_data_default_empty(self) -> None:
        """error_envelope() should default data to empty dict."""
        errors = [ErrorDetail(type="Error", message="Failed")]
        envelope = error_envelope("test", errors)

        assert envelope.data == {}

    @pytest.mark.unit
    def test_error_envelope_data_can_be_provided(self) -> None:
        """error_envelope() should accept optional data parameter."""
        errors = [ErrorDetail(type="Error", message="Failed")]
        envelope = error_envelope("scan", errors, data={"partial": True})

        assert envelope.data == {"partial": True}


class TestEnvelopeEdgeCases:
    """Tests for edge cases in envelope handling."""

    @pytest.mark.unit
    def test_envelope_with_empty_data(self) -> None:
        """Envelope with empty data dict should serialize correctly."""
        envelope = OutputEnvelope(success=True, command="init", data={})

        result = envelope.to_dict()

        assert result["data"] == {}

    @pytest.mark.unit
    def test_envelope_with_none_data(self) -> None:
        """Envelope with None data should serialize correctly."""
        envelope = OutputEnvelope(success=True, command="init", data=None)  # type: ignore

        result = envelope.to_dict()

        assert result["data"] is None

    @pytest.mark.unit
    def test_envelope_with_nested_data(self) -> None:
        """Envelope with deeply nested data should serialize correctly."""
        data = {
            "files": [
                {"path": "/data/a.parquet", "format": "geoparquet"},
                {"path": "/data/b.geojson", "format": "geojson"},
            ],
            "summary": {"total": 2, "errors": 0},
        }
        envelope = OutputEnvelope(success=True, command="scan", data=data)

        json_str = envelope.to_json()
        parsed = json.loads(json_str)

        assert parsed["data"]["files"][0]["path"] == "/data/a.parquet"
        assert parsed["data"]["summary"]["total"] == 2

    @pytest.mark.unit
    def test_envelope_with_multiple_errors(self) -> None:
        """Envelope with multiple errors should serialize all of them."""
        errors = [
            ErrorDetail(type="Error1", message="First error"),
            ErrorDetail(type="Error2", message="Second error"),
            ErrorDetail(type="Error3", message="Third error"),
        ]
        envelope = OutputEnvelope(success=False, command="check", data={}, errors=errors)

        result = envelope.to_dict()

        assert len(result["errors"]) == 3
        assert result["errors"][0]["type"] == "Error1"
        assert result["errors"][2]["type"] == "Error3"
