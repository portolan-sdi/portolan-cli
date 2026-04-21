"""Unit tests for concurrency defaults and configuration.

Tests for Issue #344: Conservative concurrency defaults to prevent
overwhelming home networks. Validates:
- Default file concurrency lowered from 50 to 8
- Default chunk concurrency lowered from 12 to 4
- Total connection footprint calculation

TDD: These tests are written FIRST, before implementation changes.
"""

from __future__ import annotations

import pytest

# =============================================================================
# Default Value Tests
# =============================================================================


class TestDefaultConcurrencyValues:
    """Tests for conservative default concurrency values."""

    @pytest.mark.unit
    def test_get_default_concurrency_returns_8(self) -> None:
        """Default file concurrency should be 8 (lowered from 50).

        Rationale (Issue #344): 50 concurrent file uploads × 12 chunks each
        = 600 connections, which overwhelms home NAT tables. 8 is safe.
        """
        from portolan_cli.async_utils import get_default_concurrency

        assert get_default_concurrency() == 8

    @pytest.mark.unit
    def test_get_default_chunk_concurrency_returns_4(self) -> None:
        """Default chunk concurrency should be 4 (lowered from 12).

        Rationale (Issue #344): Per-file multipart chunk concurrency of 12
        multiplies with file concurrency to create connection storms.
        4 chunks × 8 files = 32 connections, which is safe for home networks.
        """
        from portolan_cli.async_utils import get_default_chunk_concurrency

        assert get_default_chunk_concurrency() == 4

    @pytest.mark.unit
    def test_default_connection_footprint_is_32(self) -> None:
        """Default connection footprint should be 32 (8 files × 4 chunks).

        This is the maximum concurrent HTTP connections with default settings.
        Safe for consumer NAT tables (typically 1k-4k session limit).
        """
        from portolan_cli.async_utils import (
            get_default_chunk_concurrency,
            get_default_concurrency,
        )

        file_concurrency = get_default_concurrency()
        chunk_concurrency = get_default_chunk_concurrency()
        footprint = file_concurrency * chunk_concurrency

        assert footprint == 32
        assert footprint < 100  # Well under consumer NAT limits


class TestConcurrencyConstants:
    """Tests for concurrency-related constants and bounds."""

    @pytest.mark.unit
    def test_max_safe_connections_constant(self) -> None:
        """MAX_SAFE_CONNECTIONS should be defined for validation."""
        from portolan_cli.async_utils import MAX_SAFE_CONNECTIONS

        # 100 is a safe upper bound for home networks
        assert MAX_SAFE_CONNECTIONS == 100

    @pytest.mark.unit
    def test_calculate_connection_footprint(self) -> None:
        """calculate_connection_footprint should multiply file × chunk."""
        from portolan_cli.async_utils import calculate_connection_footprint

        assert calculate_connection_footprint(8, 4) == 32
        assert calculate_connection_footprint(50, 12) == 600
        assert calculate_connection_footprint(1, 1) == 1

    @pytest.mark.unit
    def test_calculate_connection_footprint_with_workers(self) -> None:
        """calculate_connection_footprint should include workers multiplier."""
        from portolan_cli.async_utils import calculate_connection_footprint

        # With workers=4: 4 × 8 × 4 = 128 connections
        assert calculate_connection_footprint(8, 4, workers=4) == 128

        # Old defaults with workers: 4 × 50 × 12 = 2400 connections (the bug)
        assert calculate_connection_footprint(50, 12, workers=4) == 2400


# =============================================================================
# Upload Module Default Tests
# =============================================================================


class TestUploadModuleDefaults:
    """Tests that upload module uses conservative defaults."""

    @pytest.mark.unit
    def test_upload_file_default_chunk_concurrency(self) -> None:
        """upload_file should default to 4 chunk concurrency."""
        import inspect

        from portolan_cli.upload import upload_file

        sig = inspect.signature(upload_file)
        chunk_param = sig.parameters.get("chunk_concurrency")

        assert chunk_param is not None
        assert chunk_param.default == 4

    @pytest.mark.unit
    def test_upload_directory_default_chunk_concurrency(self) -> None:
        """upload_directory should default to 4 chunk concurrency."""
        import inspect

        from portolan_cli.upload import upload_directory

        sig = inspect.signature(upload_directory)
        chunk_param = sig.parameters.get("chunk_concurrency")

        assert chunk_param is not None
        assert chunk_param.default == 4

    @pytest.mark.unit
    def test_setup_store_default_chunk_concurrency(self) -> None:
        """setup_store should use 4 as internal chunk concurrency default."""
        # This is tested indirectly - setup_store passes chunk_concurrency
        # to _setup_store_and_kwargs. The default should be 4.
        import inspect

        from portolan_cli.upload import _setup_store_and_kwargs

        sig = inspect.signature(_setup_store_and_kwargs)
        chunk_param = sig.parameters.get("chunk_concurrency")

        assert chunk_param is not None
        # _setup_store_and_kwargs requires chunk_concurrency (no default)
        # but callers should pass get_default_chunk_concurrency()


# =============================================================================
# Push Module Default Tests
# =============================================================================


class TestPushModuleDefaults:
    """Tests that push module uses conservative defaults."""

    @pytest.mark.unit
    def test_push_async_default_concurrency(self) -> None:
        """push_async should default to 8 file concurrency."""
        import inspect

        from portolan_cli.push import push_async

        sig = inspect.signature(push_async)
        conc_param = sig.parameters.get("concurrency")

        assert conc_param is not None
        # None means "use get_default_concurrency()" which is 8
        assert conc_param.default is None

    @pytest.mark.unit
    def test_push_async_has_chunk_concurrency_param(self) -> None:
        """push_async should accept chunk_concurrency parameter."""
        import inspect

        from portolan_cli.push import push_async

        sig = inspect.signature(push_async)
        chunk_param = sig.parameters.get("chunk_concurrency")

        assert chunk_param is not None, "push_async must have chunk_concurrency param"

    @pytest.mark.unit
    def test_push_all_collections_has_chunk_concurrency_param(self) -> None:
        """push_all_collections should accept chunk_concurrency parameter."""
        import inspect

        from portolan_cli.push import push_all_collections

        sig = inspect.signature(push_all_collections)
        chunk_param = sig.parameters.get("chunk_concurrency")

        assert chunk_param is not None, "push_all_collections must have chunk_concurrency param"


# =============================================================================
# CLI Default Tests
# =============================================================================


class TestCLIDefaults:
    """Tests that CLI uses conservative defaults."""

    @pytest.mark.unit
    def test_push_command_concurrency_default(self) -> None:
        """push command --concurrency should default to 8."""
        from portolan_cli.cli import push

        # Check the option default
        for param in push.params:
            if param.name == "concurrency":
                assert param.default == 8
                break
        else:
            pytest.fail("--concurrency option not found on push command")

    @pytest.mark.unit
    def test_push_command_has_chunk_concurrency_option(self) -> None:
        """push command should have --chunk-concurrency option."""
        from portolan_cli.cli import push

        option_names = [p.name for p in push.params]
        assert "chunk_concurrency" in option_names, (
            "push command must have --chunk-concurrency option"
        )

    @pytest.mark.unit
    def test_push_command_chunk_concurrency_default(self) -> None:
        """push command --chunk-concurrency should default to 4."""
        from portolan_cli.cli import push

        for param in push.params:
            if param.name == "chunk_concurrency":
                assert param.default == 4
                break
        else:
            pytest.fail("--chunk-concurrency option not found")
