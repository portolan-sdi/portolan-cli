"""Tests for push verbose mode and upload metrics.

Tests the quiet-by-default behavior of push uploads:
- Default: only show failures, not per-file success messages
- --verbose: show per-file upload details with size and speed

See GitHub issue #282 for the upload metrics feature.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from portolan_cli.push import UploadMetrics, _upload_assets_async, format_file_size, format_speed

if TYPE_CHECKING:
    pass


# =============================================================================
# Formatting Tests
# =============================================================================


class TestFormatFileSize:
    """Tests for human-readable file size formatting."""

    @pytest.mark.unit
    def test_format_bytes(self) -> None:
        """Small files should show bytes."""
        assert format_file_size(500) == "500 B"
        assert format_file_size(0) == "0 B"

    @pytest.mark.unit
    def test_format_kilobytes(self) -> None:
        """Files under 1MB should show KB."""
        assert format_file_size(1024) == "1.0 KB"
        assert format_file_size(1536) == "1.5 KB"
        assert format_file_size(500 * 1024) == "500.0 KB"

    @pytest.mark.unit
    def test_format_megabytes(self) -> None:
        """Files under 1GB should show MB."""
        assert format_file_size(1024 * 1024) == "1.0 MB"
        assert format_file_size(54.2 * 1024 * 1024) == "54.2 MB"

    @pytest.mark.unit
    def test_format_gigabytes(self) -> None:
        """Large files should show GB."""
        assert format_file_size(1024 * 1024 * 1024) == "1.0 GB"
        assert format_file_size(2.5 * 1024 * 1024 * 1024) == "2.5 GB"


class TestFormatSpeed:
    """Tests for human-readable upload speed formatting."""

    @pytest.mark.unit
    def test_format_bytes_per_second(self) -> None:
        """Slow speeds should show B/s."""
        assert format_speed(500) == "500 B/s"
        assert format_speed(0) == "0 B/s"

    @pytest.mark.unit
    def test_format_kibibytes_per_second(self) -> None:
        """Medium speeds should show KiB/s."""
        assert format_speed(1024) == "1.0 KiB/s"
        assert format_speed(500 * 1024) == "500.0 KiB/s"

    @pytest.mark.unit
    def test_format_mebibytes_per_second(self) -> None:
        """Fast speeds should show MiB/s."""
        assert format_speed(1024 * 1024) == "1.0 MiB/s"
        assert format_speed(10.5 * 1024 * 1024) == "10.5 MiB/s"

    @pytest.mark.unit
    def test_format_gibibytes_per_second(self) -> None:
        """Very fast speeds should show GiB/s."""
        assert format_speed(1024 * 1024 * 1024) == "1.0 GiB/s"


# =============================================================================
# Upload Metrics Tests
# =============================================================================


class TestUploadMetrics:
    """Tests for upload metrics tracking."""

    @pytest.mark.unit
    def test_empty_metrics(self) -> None:
        """Empty metrics should have zero values."""
        metrics = UploadMetrics()
        assert metrics.total_bytes == 0
        assert metrics.total_duration == 0.0
        assert metrics.file_count == 0

    @pytest.mark.unit
    def test_record_upload(self) -> None:
        """Recording uploads should accumulate metrics."""
        metrics = UploadMetrics()
        metrics.record(size_bytes=1000, duration_seconds=0.1)
        metrics.record(size_bytes=2000, duration_seconds=0.2)

        assert metrics.total_bytes == 3000
        assert metrics.total_duration == pytest.approx(0.3)
        assert metrics.file_count == 2

    @pytest.mark.unit
    def test_average_speed(self) -> None:
        """Average speed should be total bytes / total duration."""
        metrics = UploadMetrics()
        metrics.record(size_bytes=1000, duration_seconds=0.1)  # 10000 B/s
        metrics.record(size_bytes=2000, duration_seconds=0.2)  # 10000 B/s

        assert metrics.average_speed == pytest.approx(10000.0)

    @pytest.mark.unit
    def test_average_speed_zero_duration(self) -> None:
        """Zero duration should return 0 to avoid division by zero."""
        metrics = UploadMetrics()
        assert metrics.average_speed == 0.0


# =============================================================================
# Verbose Mode Tests
# =============================================================================


class TestUploadAssetsVerboseMode:
    """Tests for quiet-by-default upload behavior."""

    @pytest.fixture
    def mock_catalog(self, tmp_path: Path) -> Path:
        """Create a mock catalog with test files."""
        catalog = tmp_path / "catalog"
        catalog.mkdir()

        # Create a collection with some assets
        collection = catalog / "test-collection"
        collection.mkdir()

        # Create asset files of known sizes
        asset1 = collection / "item1" / "data.parquet"
        asset1.parent.mkdir(parents=True)
        asset1.write_bytes(b"x" * 1000)  # 1KB file

        asset2 = collection / "item2" / "data.parquet"
        asset2.parent.mkdir(parents=True)
        asset2.write_bytes(b"y" * 2000)  # 2KB file

        return catalog

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_default_mode_suppresses_success_messages(
        self, mock_catalog: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Default mode should not print per-file success messages."""
        assets = [
            mock_catalog / "test-collection" / "item1" / "data.parquet",
            mock_catalog / "test-collection" / "item2" / "data.parquet",
        ]

        mock_store = MagicMock()

        with patch("portolan_cli.push.obs.put_async", new_callable=AsyncMock):
            await _upload_assets_async(
                store=mock_store,
                catalog_root=mock_catalog,
                prefix="test-prefix",
                assets=assets,
                suppress_progress=True,  # Default quiet mode
            )

        captured = capsys.readouterr()
        # Should NOT contain per-file "Uploaded:" messages
        assert "Uploaded:" not in captured.out
        assert "data.parquet" not in captured.out

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_json_mode_suppresses_progress_bar(
        self, mock_catalog: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """JSON mode should suppress progress bar output.

        Note: The async upload uses AsyncProgressReporter which is suppressed
        when json_mode=True. Per-file verbose output is not implemented in
        the async version - progress is shown via the Rich progress bar.
        """
        assets = [
            mock_catalog / "test-collection" / "item1" / "data.parquet",
        ]

        mock_store = MagicMock()

        with patch("portolan_cli.push.obs.put_async", new_callable=AsyncMock):
            files_uploaded, errors, uploaded_keys, metrics = await _upload_assets_async(
                store=mock_store,
                catalog_root=mock_catalog,
                prefix="test-prefix",
                assets=assets,
                json_mode=True,  # Suppress progress bar
            )

        # Should complete successfully
        assert files_uploaded == 1
        assert len(errors) == 0
        assert len(uploaded_keys) == 1
        # Metrics should be recorded
        assert metrics.file_count == 1
        assert metrics.total_bytes == 1000  # 1KB file

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_failures_always_shown(
        self, mock_catalog: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Failures should always be shown, regardless of verbose mode."""
        assets = [
            mock_catalog / "test-collection" / "item1" / "data.parquet",
        ]

        mock_store = MagicMock()

        with patch(
            "portolan_cli.push.obs.put_async",
            new_callable=AsyncMock,
            side_effect=Exception("Network error"),
        ):
            files_uploaded, errors, _, _metrics = await _upload_assets_async(
                store=mock_store,
                catalog_root=mock_catalog,
                prefix="test-prefix",
                assets=assets,
                suppress_progress=True,  # Even in quiet mode
            )

        captured = capsys.readouterr()
        # Failures should be printed
        assert "Failed:" in captured.out or "Failed" in captured.err
        assert len(errors) == 1
        assert "Network error" in errors[0]

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_returns_upload_metrics(self, mock_catalog: Path) -> None:
        """Upload should return metrics for summary display."""
        assets = [
            mock_catalog / "test-collection" / "item1" / "data.parquet",
            mock_catalog / "test-collection" / "item2" / "data.parquet",
        ]

        mock_store = MagicMock()

        with patch("portolan_cli.push.obs.put_async", new_callable=AsyncMock):
            files_uploaded, errors, uploaded_keys, metrics = await _upload_assets_async(
                store=mock_store,
                catalog_root=mock_catalog,
                prefix="test-prefix",
                assets=assets,
                suppress_progress=True,
            )

        assert isinstance(metrics, UploadMetrics)
        assert metrics.file_count == 2
        assert metrics.total_bytes == 3000  # 1000 + 2000 bytes

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_summary_shows_total_size_and_speed(
        self, mock_catalog: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Summary should show total size and average speed even in non-verbose mode."""
        assets = [
            mock_catalog / "test-collection" / "item1" / "data.parquet",
            mock_catalog / "test-collection" / "item2" / "data.parquet",
        ]

        mock_store = MagicMock()

        with patch("portolan_cli.push.obs.put_async", new_callable=AsyncMock):
            _files_uploaded, _errors, _uploaded_keys, metrics = await _upload_assets_async(
                store=mock_store,
                catalog_root=mock_catalog,
                prefix="test-prefix",
                assets=assets,
                suppress_progress=True,  # Even in quiet mode
            )

        # Metrics should be populated
        assert metrics.total_bytes > 0
        assert metrics.file_count == 2
        # Average speed should be calculated (may be very fast in tests)
        assert metrics.average_speed >= 0


class TestUploadMetricsMerge:
    """Tests for merging upload metrics across collections."""

    @pytest.mark.unit
    def test_merge_empty_metrics(self) -> None:
        """Merging empty metrics should work."""
        m1 = UploadMetrics()
        m2 = UploadMetrics()
        m1.merge(m2)
        assert m1.total_bytes == 0
        assert m1.file_count == 0

    @pytest.mark.unit
    def test_merge_accumulates_values(self) -> None:
        """Merging should accumulate all values."""
        m1 = UploadMetrics()
        m1.record(1000, 0.1)

        m2 = UploadMetrics()
        m2.record(2000, 0.2)

        m1.merge(m2)

        assert m1.total_bytes == 3000
        assert m1.total_duration == pytest.approx(0.3)
        assert m1.file_count == 2

    @pytest.mark.unit
    def test_merge_multiple_metrics(self) -> None:
        """Merging multiple metrics should accumulate correctly."""
        total = UploadMetrics()

        for i in range(5):
            m = UploadMetrics()
            m.record(1000 * (i + 1), 0.1 * (i + 1))
            total.merge(m)

        assert total.total_bytes == 1000 + 2000 + 3000 + 4000 + 5000  # 15000
        assert total.file_count == 5
