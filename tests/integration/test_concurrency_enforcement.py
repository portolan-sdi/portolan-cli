"""Integration tests for concurrency enforcement (Issue #344).

These tests verify that concurrency limits are ACTUALLY ENFORCED at runtime,
not just passed through the API. They measure real in-flight operations.

Unlike unit tests that verify parameters are passed correctly, these tests
verify the runtime behavior:
- Adaptive concurrency starts low and ramps up
- max_connections cap is respected
- Worker multiplication is accounted for
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

# =============================================================================
# Adaptive Concurrency Runtime Tests
# =============================================================================


class TestAdaptiveConcurrencyRuntime:
    """Tests that adaptive concurrency ACTUALLY gates admission at runtime."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_adaptive_starts_low_and_ramps_up(self) -> None:
        """Verify adaptive mode starts with low concurrency and increases.

        This test measures actual in-flight operations over time to verify
        that the slow-start ramp-up is actually happening.
        """
        from portolan_cli.async_utils import (
            AdaptiveConcurrencyManager,
            AsyncIOExecutor,
        )

        # Track actual in-flight operations over time
        in_flight_samples: list[int] = []
        in_flight = 0
        in_flight_lock = asyncio.Lock()

        async def track_concurrency(item: str) -> int:
            """Operation that tracks actual concurrency."""
            nonlocal in_flight
            async with in_flight_lock:
                in_flight += 1
                in_flight_samples.append(in_flight)

            # Simulate work
            await asyncio.sleep(0.02)

            async with in_flight_lock:
                in_flight -= 1

            return int(item)

        manager = AdaptiveConcurrencyManager(
            max_concurrency=20,
            initial_concurrency=2,
            success_window=3,  # Ramp up after 3 successes
        )

        executor = AsyncIOExecutor[int](
            concurrency=manager.current_concurrency,
            adaptive_manager=manager,
        )

        # Run enough items to see ramp-up
        items = [str(i) for i in range(50)]
        results = await executor.execute(items=items, operation=track_concurrency)

        # All should succeed
        assert len(results) == 50
        assert all(r.error is None for r in results)

        # Key assertion: early samples should be low (slow-start)
        early_samples = in_flight_samples[:10]
        late_samples = in_flight_samples[-10:]

        # Early concurrency should be <= initial (2) plus small buffer
        assert max(early_samples) <= 4, f"Early samples too high: {early_samples}"

        # Late concurrency should have ramped up
        assert max(late_samples) > max(early_samples), (
            f"No ramp-up detected: early={max(early_samples)}, late={max(late_samples)}"
        )

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_adaptive_backs_off_on_errors(self) -> None:
        """Verify adaptive mode reduces concurrency after errors."""
        from portolan_cli.async_utils import (
            AdaptiveConcurrencyManager,
            AsyncIOExecutor,
        )

        in_flight_at_error: list[int] = []
        in_flight = 0
        in_flight_lock = asyncio.Lock()
        error_count = 0

        async def fail_sometimes(item: str) -> int:
            """Fail every 5th item to trigger backoff."""
            nonlocal in_flight, error_count
            async with in_flight_lock:
                in_flight += 1
                current = in_flight

            await asyncio.sleep(0.01)

            async with in_flight_lock:
                in_flight -= 1

            if int(item) % 5 == 0 and int(item) > 0:
                in_flight_at_error.append(current)
                error_count += 1
                raise RuntimeError("Simulated error")

            return int(item)

        manager = AdaptiveConcurrencyManager(
            max_concurrency=20,
            initial_concurrency=10,  # Start higher to see backoff
            backoff_factor=0.5,
        )

        executor = AsyncIOExecutor[int](
            concurrency=manager.current_concurrency,
            adaptive_manager=manager,
        )

        items = [str(i) for i in range(30)]
        await executor.execute(items=items, operation=fail_sometimes)

        # Should have some errors
        assert error_count > 0

        # Concurrency should have decreased after errors
        assert manager.current_concurrency < 10, (
            f"Concurrency should have backed off: {manager.current_concurrency}"
        )


class TestMaxConnectionsEnforcement:
    """Tests that max_connections cap is actually enforced."""

    @pytest.mark.integration
    def test_adjust_concurrency_respects_max_globally(self) -> None:
        """Verify adjustment math is correct for global limits."""
        from portolan_cli.async_utils import adjust_concurrency_for_max_connections

        # Scenario: 4 workers, each wants 8 files x 4 chunks = 32 per worker
        # Total would be 4 x 32 = 128 connections
        # With max_connections=64, each worker gets budget of 16

        workers = 4
        max_connections = 64
        per_worker_budget = max_connections // workers  # 16

        file_conc, chunk_conc = adjust_concurrency_for_max_connections(
            file_concurrency=8,
            chunk_concurrency=4,
            max_connections=per_worker_budget,  # 16
        )

        # Should reduce to fit in per-worker budget
        assert file_conc * chunk_conc <= per_worker_budget
        # Total across all workers respects global cap
        assert file_conc * chunk_conc * workers <= max_connections

    @pytest.mark.integration
    def test_extreme_reduction_maintains_minimum(self) -> None:
        """Verify extreme max_connections still allows work."""
        from portolan_cli.async_utils import adjust_concurrency_for_max_connections

        file_conc, chunk_conc = adjust_concurrency_for_max_connections(
            file_concurrency=100,
            chunk_concurrency=100,
            max_connections=1,
        )

        # Must be at least 1 x 1
        assert file_conc >= 1
        assert chunk_conc >= 1
        assert file_conc * chunk_conc == 1


class TestConnectionFootprintAccuracy:
    """Tests that connection footprint calculations are accurate."""

    @pytest.mark.integration
    def test_footprint_includes_workers(self) -> None:
        """Verify footprint calculation includes worker multiplication."""
        from portolan_cli.async_utils import calculate_connection_footprint

        # Single worker
        assert calculate_connection_footprint(8, 4, workers=1) == 32

        # Multiple workers should multiply
        assert calculate_connection_footprint(8, 4, workers=4) == 128

        # Old dangerous defaults
        assert calculate_connection_footprint(50, 12, workers=4) == 2400

    @pytest.mark.integration
    def test_warning_threshold(self) -> None:
        """Verify warning triggers at correct threshold."""
        from portolan_cli.async_utils import (
            MAX_SAFE_CONNECTIONS,
            calculate_connection_footprint,
        )

        # Under threshold - no warning needed
        assert calculate_connection_footprint(8, 4, workers=1) < MAX_SAFE_CONNECTIONS

        # At threshold - borderline
        assert calculate_connection_footprint(10, 10, workers=1) == 100
        assert 100 <= MAX_SAFE_CONNECTIONS

        # Over threshold - warning needed
        assert calculate_connection_footprint(50, 4, workers=1) > MAX_SAFE_CONNECTIONS


class TestChunkConcurrencyEnforcement:
    """Tests that chunk_concurrency actually limits per-file multipart."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_large_file_uses_chunk_concurrency(self, tmp_path: Path) -> None:
        """Verify large files use sync path with chunk_concurrency.

        This test creates a mock that tracks whether obs.put() is called
        with the correct max_concurrency parameter for large files.
        """
        from unittest.mock import MagicMock, patch

        # Create a large file (>5MB threshold)
        large_file = tmp_path / "large.bin"
        large_file.write_bytes(b"x" * (6 * 1024 * 1024))  # 6MB

        # Track calls to obs.put (sync) and obs.put_async (async)
        put_calls: list[dict[str, Any]] = []
        put_async_calls: list[dict[str, Any]] = []

        def mock_put(store: Any, key: str, data: Any, **kwargs: Any) -> None:
            put_calls.append({"key": key, "max_concurrency": kwargs.get("max_concurrency")})

        async def mock_put_async(store: Any, key: str, data: bytes) -> None:
            put_async_calls.append({"key": key, "size": len(data)})

        with (
            patch("portolan_cli.push.obs.put", side_effect=mock_put),
            patch("portolan_cli.push.obs.put_async", side_effect=mock_put_async),
        ):
            from portolan_cli.push import _upload_assets_async

            mock_store = MagicMock()
            await _upload_assets_async(
                store=mock_store,
                catalog_root=tmp_path,
                prefix="test",
                assets=[large_file],
                concurrency=8,
                chunk_concurrency=6,  # Custom chunk concurrency
                adaptive=False,
            )

        # Large file should use sync put() with max_concurrency
        assert len(put_calls) == 1, "Large file should use sync obs.put()"
        assert put_calls[0]["max_concurrency"] == 6, f"chunk_concurrency not passed: {put_calls[0]}"

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_small_file_uses_async_path(self, tmp_path: Path) -> None:
        """Verify small files use efficient async path."""
        from unittest.mock import MagicMock, patch

        # Create a small file (<5MB threshold)
        small_file = tmp_path / "small.txt"
        small_file.write_bytes(b"small content")

        put_calls: list[dict[str, Any]] = []
        put_async_calls: list[dict[str, Any]] = []

        def mock_put(store: Any, key: str, data: Any, **kwargs: Any) -> None:
            put_calls.append({"key": key})

        async def mock_put_async(store: Any, key: str, data: bytes) -> None:
            put_async_calls.append({"key": key, "size": len(data)})

        with (
            patch("portolan_cli.push.obs.put", side_effect=mock_put),
            patch("portolan_cli.push.obs.put_async", side_effect=mock_put_async),
        ):
            from portolan_cli.push import _upload_assets_async

            mock_store = MagicMock()
            await _upload_assets_async(
                store=mock_store,
                catalog_root=tmp_path,
                prefix="test",
                assets=[small_file],
                concurrency=8,
                chunk_concurrency=4,
                adaptive=False,
            )

        # Small file should use async put_async()
        assert len(put_async_calls) == 1, "Small file should use async obs.put_async()"
        assert len(put_calls) == 0, "Small file should NOT use sync obs.put()"


class TestCLIIntegration:
    """Integration tests for CLI concurrency options."""

    @pytest.mark.integration
    def test_max_connections_adjusts_effective_concurrency(self) -> None:
        """Verify CLI applies max_connections before calling push."""
        from unittest.mock import patch

        from click.testing import CliRunner

        from portolan_cli.cli import cli

        runner = CliRunner()

        with runner.isolated_filesystem():
            # Setup minimal catalog
            Path(".portolan").mkdir()
            Path(".portolan/config.yaml").write_text("version: '1.0'\nremote: s3://test/\n")
            Path("col1").mkdir()
            Path("col1/versions.json").write_text('{"versions": []}')

            with patch("portolan_cli.push.push_all_collections") as mock_push:
                from portolan_cli.push import PushAllResult

                mock_push.return_value = PushAllResult(
                    success=True,
                    total_collections=1,
                    successful_collections=1,
                    failed_collections=0,
                    total_files_uploaded=0,
                    total_versions_pushed=0,
                )

                # Request high concurrency but limit with max_connections
                result = runner.invoke(
                    cli,
                    [
                        "push",
                        "--catalog",
                        ".",
                        "--concurrency",
                        "50",
                        "--chunk-concurrency",
                        "12",
                        "--max-connections",
                        "32",
                    ],
                )

                assert result.exit_code == 0, f"Failed: {result.output}"
                mock_push.assert_called_once()

                kwargs = mock_push.call_args.kwargs
                # Effective concurrency should be reduced to respect max_connections
                effective = kwargs["file_concurrency"] * kwargs["chunk_concurrency"]
                assert effective <= 32, f"Effective concurrency {effective} exceeds max 32"
