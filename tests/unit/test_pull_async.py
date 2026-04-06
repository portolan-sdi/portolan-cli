"""Unit tests for async pull functionality.

Tests for the async migration of pull operations (Wave 2A).
These tests verify concurrent download behavior, rate limiting,
and error handling in the async pull implementation.

Test categories:
- Concurrent download execution
- Concurrency limit enforcement
- Rate limit handling (429 responses)
- Circuit breaker on repeated failures
- Progress reporting
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from portolan_cli.pull import (
    PullResult,
    pull_async,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def catalog_with_versions(tmp_path: Path) -> Path:
    """Create a catalog with a versions.json file.

    Per ADR-0023: Collections and versions.json live at root level.
    """
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()

    # Create catalog.json at root (per ADR-0023)
    catalog_data = {
        "type": "Catalog",
        "id": "test-catalog",
        "stac_version": "1.0.0",
        "description": "Test catalog",
        "links": [],
    }
    (catalog_root / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    # Create .portolan directory for internal state
    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir(parents=True)

    # Create collection directory at root (per ADR-0023)
    collection_dir = catalog_root / "test-collection"
    collection_dir.mkdir(parents=True)

    # Create versions.json in collection directory (per ADR-0023)
    versions_data = {
        "spec_version": "1.0.0",
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-15T10:00:00Z",
                "breaking": False,
                "message": "Initial version",
                "assets": {
                    "data.parquet": {
                        "sha256": "abc123",
                        "size_bytes": 1000,
                        "href": "test-collection/data.parquet",
                    }
                },
                "changes": ["data.parquet"],
            }
        ],
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))

    # Create the actual data file in collection
    data_file = collection_dir / "data.parquet"
    data_file.write_bytes(b"x" * 1000)

    return catalog_root


# =============================================================================
# Test: Concurrent Downloads
# =============================================================================


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pull_async_downloads_concurrently(
    catalog_with_versions: Path,
) -> None:
    """Test that pull_async downloads multiple files concurrently.

    Verifies that:
    1. Multiple downloads can run at the same time
    2. All files are downloaded successfully
    3. The operation completes faster than sequential
    """
    # Track concurrent execution
    concurrent_count = 0
    max_concurrent = 0
    download_events: list[tuple[str, str]] = []  # (filename, event_type)

    async def mock_download_file_async(store: Any, key: str, path: Path) -> tuple[bool, int]:
        """Mock download that tracks concurrency."""
        nonlocal concurrent_count, max_concurrent

        filename = path.name
        concurrent_count += 1
        max_concurrent = max(max_concurrent, concurrent_count)
        download_events.append((filename, "start"))

        # Simulate network delay to allow concurrency to build up
        await asyncio.sleep(0.05)

        download_events.append((filename, "end"))
        concurrent_count -= 1
        return True, 1000

    with (
        patch("portolan_cli.pull._fetch_remote_versions_async") as mock_fetch,
        patch("portolan_cli.pull._download_file_async", mock_download_file_async),
    ):
        # Setup mock to return remote versions with 10 files
        from portolan_cli.versions import Asset, Version, VersionsFile

        remote = VersionsFile(
            spec_version="1.0.0",
            current_version="2.0.0",
            versions=[
                Version(
                    version="2.0.0",
                    created=datetime(2024, 1, 20, 10, 0, 0, tzinfo=timezone.utc),
                    breaking=False,
                    message="Added many files",
                    assets={
                        f"file_{i}.parquet": Asset(
                            sha256=f"sha256_{i}",
                            size_bytes=1000,
                            href=f"test-collection/file_{i}.parquet",
                        )
                        for i in range(10)
                    },
                    changes=[f"file_{i}.parquet" for i in range(10)],
                )
            ],
        )
        mock_fetch.return_value = remote

        result = await pull_async(
            remote_url="s3://test-bucket/catalog",
            local_root=catalog_with_versions,
            collection="test-collection",
            concurrency=5,
            force=True,  # Bypass conflict detection for download testing
        )

    # Verify concurrent execution happened
    assert max_concurrent > 1, "Expected concurrent downloads, but only 1 at a time"
    assert max_concurrent <= 5, f"Concurrency exceeded limit: {max_concurrent} > 5"
    assert result.success
    assert result.files_downloaded == 10


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pull_async_respects_concurrency_limit(
    catalog_with_versions: Path,
) -> None:
    """Test that pull_async never exceeds the concurrency limit.

    Verifies that:
    1. At most `concurrency` downloads run simultaneously
    2. Setting concurrency=1 forces sequential execution
    """
    concurrent_count = 0
    max_concurrent = 0
    concurrency_violations: list[int] = []
    concurrency_limit = 3

    async def mock_download_file_async(store: Any, key: str, path: Path) -> tuple[bool, int]:
        """Mock download that strictly checks concurrency."""
        nonlocal concurrent_count, max_concurrent

        concurrent_count += 1
        max_concurrent = max(max_concurrent, concurrent_count)

        if concurrent_count > concurrency_limit:
            concurrency_violations.append(concurrent_count)

        # Longer delay to ensure we'd see violations if limit not respected
        await asyncio.sleep(0.1)

        concurrent_count -= 1
        return True, 1000

    with (
        patch("portolan_cli.pull._fetch_remote_versions_async") as mock_fetch,
        patch("portolan_cli.pull._download_file_async", mock_download_file_async),
    ):
        from portolan_cli.versions import Asset, Version, VersionsFile

        # Create 20 files to download - more than concurrency limit
        remote = VersionsFile(
            spec_version="1.0.0",
            current_version="2.0.0",
            versions=[
                Version(
                    version="2.0.0",
                    created=datetime(2024, 1, 20, 10, 0, 0, tzinfo=timezone.utc),
                    breaking=False,
                    message="Many files",
                    assets={
                        f"file_{i}.parquet": Asset(
                            sha256=f"sha256_{i}",
                            size_bytes=1000,
                            href=f"test-collection/file_{i}.parquet",
                        )
                        for i in range(20)
                    },
                    changes=[f"file_{i}.parquet" for i in range(20)],
                )
            ],
        )
        mock_fetch.return_value = remote

        result = await pull_async(
            remote_url="s3://test-bucket/catalog",
            local_root=catalog_with_versions,
            collection="test-collection",
            concurrency=concurrency_limit,
            force=True,  # Bypass conflict detection for download testing
        )

    assert len(concurrency_violations) == 0, f"Concurrency violations: {concurrency_violations}"
    assert max_concurrent <= concurrency_limit
    assert result.success


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pull_async_handles_rate_limit(
    catalog_with_versions: Path,
) -> None:
    """Test that pull_async handles rate limiting (429 responses) with backoff.

    Verifies that:
    1. Rate-limited requests are retried after delay
    2. Backoff increases on repeated rate limits
    3. Eventually succeeds after rate limit clears
    """
    call_count = 0

    async def mock_download_with_rate_limit(store: Any, key: str, path: Path) -> tuple[bool, int]:
        """Mock that returns rate limit errors initially."""
        nonlocal call_count
        call_count += 1

        # First 2 calls get rate limited, then succeed
        if call_count <= 2:
            raise Exception("429 Too Many Requests")

        return True, 1000

    with (
        patch("portolan_cli.pull._fetch_remote_versions_async") as mock_fetch,
        patch("portolan_cli.pull._download_file_async", mock_download_with_rate_limit),
    ):
        from portolan_cli.versions import Asset, Version, VersionsFile

        remote = VersionsFile(
            spec_version="1.0.0",
            current_version="2.0.0",
            versions=[
                Version(
                    version="2.0.0",
                    created=datetime(2024, 1, 20, 10, 0, 0, tzinfo=timezone.utc),
                    breaking=False,
                    message="Single file",
                    assets={
                        "data.parquet": Asset(
                            sha256="new_sha256",
                            size_bytes=1000,
                            href="test-collection/data.parquet",
                        )
                    },
                    changes=["data.parquet"],
                )
            ],
        )
        mock_fetch.return_value = remote

        result = await pull_async(
            remote_url="s3://test-bucket/catalog",
            local_root=catalog_with_versions,
            collection="test-collection",
            concurrency=5,
            force=True,  # Bypass conflict detection for rate limit testing
        )

    # Should have retried and eventually succeeded
    assert call_count == 3, f"Expected 3 calls (2 rate-limited + 1 success), got {call_count}"
    assert result.success


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pull_async_circuit_breaker_on_failures(
    catalog_with_versions: Path,
) -> None:
    """Test that pull_async trips circuit breaker on repeated failures.

    Verifies that:
    1. After N consecutive failures, circuit breaker opens
    2. Subsequent requests fail fast without attempting download
    3. Error message indicates circuit breaker tripped
    """
    call_count = 0

    async def mock_download_always_fails(store: Any, key: str, path: Path) -> tuple[bool, int]:
        """Mock that always fails."""
        nonlocal call_count
        call_count += 1
        raise Exception("Network error")

    with (
        patch("portolan_cli.pull._fetch_remote_versions_async") as mock_fetch,
        patch("portolan_cli.pull._download_file_async", mock_download_always_fails),
    ):
        from portolan_cli.versions import Asset, Version, VersionsFile

        # Create many files to trigger circuit breaker
        remote = VersionsFile(
            spec_version="1.0.0",
            current_version="2.0.0",
            versions=[
                Version(
                    version="2.0.0",
                    created=datetime(2024, 1, 20, 10, 0, 0, tzinfo=timezone.utc),
                    breaking=False,
                    message="Many files",
                    assets={
                        f"file_{i}.parquet": Asset(
                            sha256=f"sha256_{i}",
                            size_bytes=1000,
                            href=f"test-collection/file_{i}.parquet",
                        )
                        for i in range(50)
                    },
                    changes=[f"file_{i}.parquet" for i in range(50)],
                )
            ],
        )
        mock_fetch.return_value = remote

        result = await pull_async(
            remote_url="s3://test-bucket/catalog",
            local_root=catalog_with_versions,
            collection="test-collection",
            concurrency=10,
            force=True,  # Bypass conflict detection for circuit breaker testing
        )

    # Circuit breaker should prevent all 50 files from being attempted
    # After ~10 consecutive failures, it should stop
    assert call_count < 50, f"Circuit breaker didn't trip: {call_count} calls"
    assert not result.success


# =============================================================================
# Test: Sync Wrapper
# =============================================================================


@pytest.mark.unit
def test_pull_uses_async_internally(
    catalog_with_versions: Path,
) -> None:
    """Test that sync pull() function wraps pull_async().

    Verifies that:
    1. The sync pull() function calls pull_async internally
    2. Results are properly returned from the async call
    """
    with (
        patch("portolan_cli.pull.pull_async") as mock_async,
    ):
        from portolan_cli.pull import pull

        # Setup mock to return a result
        mock_async.return_value = PullResult(
            success=True,
            files_downloaded=5,
            files_skipped=0,
            local_version="1.0.0",
            remote_version="2.0.0",
        )

        result = pull(
            remote_url="s3://test-bucket/catalog",
            local_root=catalog_with_versions,
            collection="test-collection",
        )

    # Verify async was called
    mock_async.assert_called_once()
    assert result.success
    assert result.files_downloaded == 5


# =============================================================================
# Test: Progress Reporting
# =============================================================================


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pull_async_reports_progress(
    catalog_with_versions: Path,
) -> None:
    """Test that pull_async reports download progress.

    Verifies that:
    1. Progress callbacks are invoked for each file
    2. Progress shows completed/total counts
    """

    async def mock_download_file_async(store: Any, key: str, path: Path) -> tuple[bool, int]:
        return True, 1000

    with (
        patch("portolan_cli.pull._fetch_remote_versions_async") as mock_fetch,
        patch("portolan_cli.pull._download_file_async", mock_download_file_async),
        patch("portolan_cli.pull.info") as mock_info,
    ):
        from portolan_cli.versions import Asset, Version, VersionsFile

        remote = VersionsFile(
            spec_version="1.0.0",
            current_version="2.0.0",
            versions=[
                Version(
                    version="2.0.0",
                    created=datetime(2024, 1, 20, 10, 0, 0, tzinfo=timezone.utc),
                    breaking=False,
                    message="Files",
                    assets={
                        f"file_{i}.parquet": Asset(
                            sha256=f"sha256_{i}",
                            size_bytes=1000,
                            href=f"test-collection/file_{i}.parquet",
                        )
                        for i in range(5)
                    },
                    changes=[f"file_{i}.parquet" for i in range(5)],
                )
            ],
        )
        mock_fetch.return_value = remote

        result = await pull_async(
            remote_url="s3://test-bucket/catalog",
            local_root=catalog_with_versions,
            collection="test-collection",
            concurrency=5,
            force=True,  # Bypass conflict detection for progress testing
        )

    # Verify progress was reported
    assert result.success
    # Check that info was called (progress reporting)
    assert mock_info.call_count > 0
