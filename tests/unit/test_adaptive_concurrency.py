"""Unit tests for adaptive concurrency (slow-start) feature.

Tests for Issue #344 Item 4: Adaptive slow-start for network-safe uploads.
The adaptive concurrency manager:
- Starts with a small concurrency window
- Ramps up on success
- Backs off on errors/timeouts

TDD: These tests are written FIRST, before implementation.
"""

from __future__ import annotations

import asyncio

import pytest

# =============================================================================
# AdaptiveConcurrencyManager Tests
# =============================================================================


class TestAdaptiveConcurrencyManager:
    """Tests for AdaptiveConcurrencyManager class."""

    @pytest.mark.unit
    def test_manager_exists(self) -> None:
        """AdaptiveConcurrencyManager class should exist."""
        from portolan_cli.async_utils import AdaptiveConcurrencyManager

        assert AdaptiveConcurrencyManager is not None

    @pytest.mark.unit
    def test_manager_initial_concurrency_is_low(self) -> None:
        """Manager should start with low concurrency (slow-start)."""
        from portolan_cli.async_utils import AdaptiveConcurrencyManager

        manager = AdaptiveConcurrencyManager(max_concurrency=50)

        # Initial concurrency should be much lower than max
        assert manager.current_concurrency <= 4
        assert manager.current_concurrency >= 1

    @pytest.mark.unit
    def test_manager_ramps_up_on_success(self) -> None:
        """Manager should increase concurrency after successful operations."""
        from portolan_cli.async_utils import AdaptiveConcurrencyManager

        manager = AdaptiveConcurrencyManager(max_concurrency=50)
        initial = manager.current_concurrency

        # Record several successes
        for _ in range(10):
            manager.record_success()

        # Concurrency should have increased
        assert manager.current_concurrency > initial

    @pytest.mark.unit
    def test_manager_backs_off_on_error(self) -> None:
        """Manager should decrease concurrency after errors."""
        from portolan_cli.async_utils import AdaptiveConcurrencyManager

        manager = AdaptiveConcurrencyManager(max_concurrency=50)

        # First ramp up
        for _ in range(20):
            manager.record_success()

        current = manager.current_concurrency

        # Record an error
        manager.record_error()

        # Concurrency should decrease
        assert manager.current_concurrency < current

    @pytest.mark.unit
    def test_manager_backs_off_on_timeout(self) -> None:
        """Manager should decrease concurrency after timeouts."""
        from portolan_cli.async_utils import AdaptiveConcurrencyManager

        manager = AdaptiveConcurrencyManager(max_concurrency=50)

        # First ramp up
        for _ in range(20):
            manager.record_success()

        current = manager.current_concurrency

        # Record a timeout
        manager.record_timeout()

        # Concurrency should decrease
        assert manager.current_concurrency < current

    @pytest.mark.unit
    def test_manager_respects_max_concurrency(self) -> None:
        """Manager should never exceed max_concurrency."""
        from portolan_cli.async_utils import AdaptiveConcurrencyManager

        manager = AdaptiveConcurrencyManager(max_concurrency=10)

        # Record many successes
        for _ in range(100):
            manager.record_success()

        assert manager.current_concurrency <= 10

    @pytest.mark.unit
    def test_manager_respects_min_concurrency(self) -> None:
        """Manager should never go below 1."""
        from portolan_cli.async_utils import AdaptiveConcurrencyManager

        manager = AdaptiveConcurrencyManager(max_concurrency=50)

        # Record many errors
        for _ in range(100):
            manager.record_error()

        assert manager.current_concurrency >= 1

    @pytest.mark.unit
    def test_manager_aggressive_backoff_on_consecutive_errors(self) -> None:
        """Manager should back off more aggressively on consecutive errors."""
        from portolan_cli.async_utils import AdaptiveConcurrencyManager

        manager = AdaptiveConcurrencyManager(max_concurrency=50)

        # Ramp up first
        for _ in range(30):
            manager.record_success()

        # First error - moderate backoff
        manager.record_error()
        after_first_error = manager.current_concurrency

        # Reset and try again
        manager = AdaptiveConcurrencyManager(max_concurrency=50)
        for _ in range(30):
            manager.record_success()

        # Multiple consecutive errors - should back off more
        for _ in range(3):
            manager.record_error()

        after_multiple_errors = manager.current_concurrency

        # Should be lower after multiple consecutive errors
        assert after_multiple_errors < after_first_error or after_multiple_errors == 1


class TestAdaptiveConcurrencyManagerConfig:
    """Tests for AdaptiveConcurrencyManager configuration options."""

    @pytest.mark.unit
    def test_manager_accepts_initial_concurrency(self) -> None:
        """Manager should accept custom initial concurrency."""
        from portolan_cli.async_utils import AdaptiveConcurrencyManager

        manager = AdaptiveConcurrencyManager(
            max_concurrency=50,
            initial_concurrency=8,
        )

        assert manager.current_concurrency == 8

    @pytest.mark.unit
    def test_manager_accepts_ramp_up_factor(self) -> None:
        """Manager should accept custom ramp-up factor."""
        from portolan_cli.async_utils import AdaptiveConcurrencyManager

        # Aggressive ramp-up
        manager = AdaptiveConcurrencyManager(
            max_concurrency=50,
            initial_concurrency=2,
            ramp_up_factor=2.0,  # Double on each success window
        )

        initial = manager.current_concurrency

        # Trigger ramp-up (after success window)
        for _ in range(10):
            manager.record_success()

        # With 2.0 factor, should roughly double
        assert manager.current_concurrency >= initial * 1.5

    @pytest.mark.unit
    def test_manager_accepts_backoff_factor(self) -> None:
        """Manager should accept custom backoff factor."""
        from portolan_cli.async_utils import AdaptiveConcurrencyManager

        manager = AdaptiveConcurrencyManager(
            max_concurrency=50,
            initial_concurrency=20,
            backoff_factor=0.5,  # Halve on error
        )

        manager.record_error()

        # Should be roughly half (10)
        assert manager.current_concurrency <= 12
        assert manager.current_concurrency >= 8


class TestAdaptiveConcurrencyIntegration:
    """Integration tests for adaptive concurrency with AsyncIOExecutor."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_executor_with_adaptive_concurrency(self) -> None:
        """AsyncIOExecutor should work with AdaptiveConcurrencyManager."""
        from portolan_cli.async_utils import (
            AdaptiveConcurrencyManager,
            AsyncIOExecutor,
        )

        manager = AdaptiveConcurrencyManager(max_concurrency=10)
        executor = AsyncIOExecutor[int](
            concurrency=manager.current_concurrency,
            adaptive_manager=manager,
        )

        # Simple operation that always succeeds
        async def succeed(item: str) -> int:
            await asyncio.sleep(0.001)
            return int(item)

        items = [str(i) for i in range(20)]
        results = await executor.execute(items=items, operation=succeed)

        # All should succeed
        assert len(results) == 20
        assert all(r.error is None for r in results)

        # Concurrency should have increased from initial
        assert manager.current_concurrency > 2

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_executor_backs_off_on_failures(self) -> None:
        """AsyncIOExecutor should reduce concurrency on failures."""
        from portolan_cli.async_utils import (
            AdaptiveConcurrencyManager,
            AsyncIOExecutor,
        )

        manager = AdaptiveConcurrencyManager(
            max_concurrency=20,
            initial_concurrency=10,
        )

        fail_count = 0

        async def fail_sometimes(item: str) -> int:
            nonlocal fail_count
            await asyncio.sleep(0.001)
            if int(item) % 3 == 0:  # Fail every 3rd item
                fail_count += 1
                raise Exception("Simulated failure")
            return int(item)

        executor = AsyncIOExecutor[int](
            concurrency=manager.current_concurrency,
            adaptive_manager=manager,
        )

        items = [str(i) for i in range(30)]
        results = await executor.execute(items=items, operation=fail_sometimes)

        # Some should have failed
        assert fail_count > 0
        errors = [r for r in results if r.error is not None]
        assert len(errors) > 0

        # Concurrency should have decreased due to failures
        assert manager.current_concurrency < 10


class TestAdaptiveConcurrencyCLI:
    """Tests for CLI integration with adaptive concurrency."""

    @pytest.mark.unit
    def test_push_has_adaptive_flag(self) -> None:
        """push command should have --adaptive flag."""
        from portolan_cli.cli import push

        option_names = [p.name for p in push.params]
        assert "adaptive" in option_names, "push command must have --adaptive option"

    @pytest.mark.unit
    def test_adaptive_flag_default_is_true(self) -> None:
        """--adaptive should default to True (safer for home networks)."""
        from portolan_cli.cli import push

        for param in push.params:
            if param.name == "adaptive":
                assert param.default is True
                break
        else:
            pytest.fail("--adaptive option not found")

    @pytest.mark.unit
    def test_no_adaptive_flag_disables(self) -> None:
        """--no-adaptive should disable adaptive concurrency."""
        from click.testing import CliRunner

        from portolan_cli.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["push", "--help"])

        assert result.exit_code == 0
        # Should show --no-adaptive or --adaptive / --no-adaptive
        assert "adaptive" in result.output.lower()
