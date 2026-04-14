"""Tests for IcebergBackend push support methods.

IcebergBackend.supports_push() should return False, and
push_blocked_message() should return appropriate messages.
"""

from __future__ import annotations

import pytest

from portolan_cli.backends.iceberg.backend import IcebergBackend


@pytest.mark.integration
def test_supports_push_returns_false(iceberg_backend):
    """IcebergBackend should not support push."""
    assert iceberg_backend.supports_push() is False


@pytest.mark.integration
def test_push_blocked_message_with_remote(iceberg_backend):
    """Message with remote should explain that add already uploads."""
    msg = iceberg_backend.push_blocked_message(remote="gs://bucket/catalog")
    assert "add" in msg.lower()
    assert "upload" in msg.lower() or "already" in msg.lower()


@pytest.mark.integration
def test_push_blocked_message_without_remote(iceberg_backend):
    """Message without remote should explain push is not supported."""
    msg = iceberg_backend.push_blocked_message(remote=None)
    assert "not supported" in msg.lower() or "not needed" in msg.lower()


@pytest.mark.unit
def test_supports_push_is_bool():
    """supports_push() should return a bool, not a truthy value."""
    backend = IcebergBackend.__new__(IcebergBackend)
    result = backend.supports_push()
    assert isinstance(result, bool)
    assert result is False
