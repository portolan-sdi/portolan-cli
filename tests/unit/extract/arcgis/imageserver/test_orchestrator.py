"""Tests for ImageServer CLI orchestrator.

TDD tests for Wave 3: CLI-facing orchestrator that wraps extract_imageserver().
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from portolan_cli.extract.arcgis.imageserver.orchestrator import (
    ImageServerCLIOptions,
    run_imageserver_extraction,
)

pytestmark = pytest.mark.unit


class TestImageServerCLIOptions:
    """Tests for ImageServerCLIOptions dataclass."""

    def test_defaults(self) -> None:
        """Default options should be sensible."""
        options = ImageServerCLIOptions()

        assert options.tile_size == 4096
        assert options.max_concurrent == 4
        assert options.dry_run is False
        assert options.resume is False
        assert options.bbox is None
        assert options.timeout == 120.0

    def test_custom_options(self) -> None:
        """Custom options should be respected."""
        options = ImageServerCLIOptions(
            tile_size=2048,
            max_concurrent=8,
            dry_run=True,
            resume=True,
            bbox=(0.0, 1.0, 2.0, 3.0),
            timeout=60.0,
        )

        assert options.tile_size == 2048
        assert options.max_concurrent == 8
        assert options.dry_run is True
        assert options.resume is True
        assert options.bbox == (0.0, 1.0, 2.0, 3.0)
        assert options.timeout == 60.0


class TestRunImageServerExtraction:
    """Tests for run_imageserver_extraction function."""

    @pytest.mark.asyncio
    async def test_successful_extraction(self, tmp_path: Path) -> None:
        """Successful extraction should return exit code 0."""
        mock_result = MagicMock()
        mock_result.tiles_downloaded = 10
        mock_result.tiles_failed = 0
        mock_result.tiles_skipped = 0
        mock_result.total_bytes = 1000000
        mock_result.collection_path = tmp_path / "collection.json"

        with patch(
            "portolan_cli.extract.arcgis.imageserver.orchestrator.extract_imageserver",
            new_callable=AsyncMock,
            return_value=mock_result,
        ):
            exit_code = await run_imageserver_extraction(
                url="https://example.com/rest/services/Test/ImageServer",
                output_dir=tmp_path,
                options=ImageServerCLIOptions(),
            )

        assert exit_code == 0

    @pytest.mark.asyncio
    async def test_extraction_with_failures(self, tmp_path: Path) -> None:
        """Extraction with some failures should return exit code 0 (partial success)."""
        mock_result = MagicMock()
        mock_result.tiles_downloaded = 8
        mock_result.tiles_failed = 2
        mock_result.tiles_skipped = 0
        mock_result.total_bytes = 800000
        mock_result.collection_path = tmp_path / "collection.json"

        with patch(
            "portolan_cli.extract.arcgis.imageserver.orchestrator.extract_imageserver",
            new_callable=AsyncMock,
            return_value=mock_result,
        ):
            exit_code = await run_imageserver_extraction(
                url="https://example.com/rest/services/Test/ImageServer",
                output_dir=tmp_path,
                options=ImageServerCLIOptions(),
            )

        # Partial success is still exit code 0
        assert exit_code == 0

    @pytest.mark.asyncio
    async def test_extraction_all_failed(self, tmp_path: Path) -> None:
        """Extraction with all tiles failed should return exit code 1."""
        mock_result = MagicMock()
        mock_result.tiles_downloaded = 0
        mock_result.tiles_failed = 10
        mock_result.tiles_skipped = 0
        mock_result.total_bytes = 0
        mock_result.collection_path = tmp_path / "collection.json"

        with patch(
            "portolan_cli.extract.arcgis.imageserver.orchestrator.extract_imageserver",
            new_callable=AsyncMock,
            return_value=mock_result,
        ):
            exit_code = await run_imageserver_extraction(
                url="https://example.com/rest/services/Test/ImageServer",
                output_dir=tmp_path,
                options=ImageServerCLIOptions(),
            )

        assert exit_code == 1

    @pytest.mark.asyncio
    async def test_extraction_error_returns_exit_code_1(self, tmp_path: Path) -> None:
        """Extraction error should return exit code 1."""
        with patch(
            "portolan_cli.extract.arcgis.imageserver.orchestrator.extract_imageserver",
            new_callable=AsyncMock,
            side_effect=Exception("Network error"),
        ):
            exit_code = await run_imageserver_extraction(
                url="https://example.com/rest/services/Test/ImageServer",
                output_dir=tmp_path,
                options=ImageServerCLIOptions(),
            )

        assert exit_code == 1

    @pytest.mark.asyncio
    async def test_dry_run_returns_exit_code_0(self, tmp_path: Path) -> None:
        """Dry run should return exit code 0."""
        mock_result = MagicMock()
        mock_result.tiles_downloaded = 0
        mock_result.tiles_failed = 0
        mock_result.tiles_skipped = 0
        mock_result.total_bytes = 0
        mock_result.collection_path = tmp_path / "collection.json"

        with patch(
            "portolan_cli.extract.arcgis.imageserver.orchestrator.extract_imageserver",
            new_callable=AsyncMock,
            return_value=mock_result,
        ):
            exit_code = await run_imageserver_extraction(
                url="https://example.com/rest/services/Test/ImageServer",
                output_dir=tmp_path,
                options=ImageServerCLIOptions(dry_run=True),
            )

        assert exit_code == 0

    @pytest.mark.asyncio
    async def test_bbox_option_passed_to_extractor(self, tmp_path: Path) -> None:
        """bbox option should be passed to extract_imageserver."""
        mock_result = MagicMock()
        mock_result.tiles_downloaded = 5
        mock_result.tiles_failed = 0
        mock_result.tiles_skipped = 0
        mock_result.total_bytes = 500000
        mock_result.collection_path = tmp_path / "collection.json"

        mock_extract = AsyncMock(return_value=mock_result)

        with patch(
            "portolan_cli.extract.arcgis.imageserver.orchestrator.extract_imageserver",
            mock_extract,
        ):
            await run_imageserver_extraction(
                url="https://example.com/rest/services/Test/ImageServer",
                output_dir=tmp_path,
                options=ImageServerCLIOptions(bbox=(-122.0, 37.0, -121.0, 38.0)),
            )

        # Verify bbox was passed
        call_kwargs = mock_extract.call_args.kwargs
        assert call_kwargs["bbox"] == (-122.0, 37.0, -121.0, 38.0)
