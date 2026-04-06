"""Tests for add progress reporting (ADR-0040).

Tests verify:
1. File pre-counting is accurate
2. Progress reporter tracks state correctly
3. JSON mode suppresses output
4. Thread-safety for parallel workers
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from portolan_cli.add_progress import AddProgressReporter, count_files


@pytest.mark.unit
class TestFilePreCount:
    """Tests for the count_files() function."""

    def test_count_single_file(self, tmp_path: Path) -> None:
        """Single file path returns 1."""
        file = tmp_path / "data.txt"
        file.write_text("content")

        count = count_files([file])

        assert count == 1

    def test_count_multiple_files(self, tmp_path: Path) -> None:
        """Multiple file paths returns correct count."""
        files = [tmp_path / f"data{i}.txt" for i in range(5)]
        for f in files:
            f.write_text("content")

        count = count_files(files)

        assert count == 5

    def test_count_directory(self, tmp_path: Path) -> None:
        """Directory path recursively counts all files."""
        (tmp_path / "a.txt").write_text("a")
        (tmp_path / "b.txt").write_text("b")
        subdir = tmp_path / "sub"
        subdir.mkdir()
        (subdir / "c.txt").write_text("c")

        count = count_files([tmp_path])

        assert count == 3

    def test_count_mixed_paths(self, tmp_path: Path) -> None:
        """Mix of files and directories works correctly."""
        file1 = tmp_path / "single.txt"
        file1.write_text("single")

        dir1 = tmp_path / "dir1"
        dir1.mkdir()
        (dir1 / "a.txt").write_text("a")
        (dir1 / "b.txt").write_text("b")

        count = count_files([file1, dir1])

        assert count == 3  # single + a + b

    def test_count_excludes_hidden_in_directories(self, tmp_path: Path) -> None:
        """Hidden files are excluded when recursing directories."""
        (tmp_path / "visible.txt").write_text("v")
        (tmp_path / ".hidden.txt").write_text("h")

        count = count_files([tmp_path])

        assert count == 1

    def test_count_explicit_hidden_file_is_counted(self, tmp_path: Path) -> None:
        """Explicitly passed hidden file paths are always counted."""
        hidden = tmp_path / ".hidden.txt"
        hidden.write_text("h")

        # Explicitly passing a hidden file should count it (user intent)
        count = count_files([hidden])

        assert count == 1

    def test_count_includes_hidden_when_requested(self, tmp_path: Path) -> None:
        """Hidden files included when include_hidden=True."""
        (tmp_path / "visible.txt").write_text("v")
        (tmp_path / ".hidden.txt").write_text("h")

        count = count_files([tmp_path], include_hidden=True)

        assert count == 2

    def test_count_empty_directory(self, tmp_path: Path) -> None:
        """Empty directory returns 0."""
        count = count_files([tmp_path])

        assert count == 0

    def test_count_nonexistent_file(self, tmp_path: Path) -> None:
        """Nonexistent file is not counted."""
        fake = tmp_path / "does_not_exist.txt"

        count = count_files([fake])

        assert count == 0


@pytest.mark.unit
class TestAddProgressReporter:
    """Tests for the AddProgressReporter context manager."""

    def test_reporter_suppresses_in_json_mode(self) -> None:
        """JSON mode suppresses progress display."""
        reporter = AddProgressReporter(total_files=100, json_mode=True)

        with reporter:
            reporter.advance()

        # In JSON mode, no Rich progress should be created
        assert reporter._progress is None

    def test_reporter_tracks_progress(self) -> None:
        """Reporter correctly tracks files processed."""
        reporter = AddProgressReporter(total_files=10, json_mode=True)

        with reporter:
            for _ in range(5):
                reporter.advance()

        assert reporter.files_processed == 5

    def test_reporter_measures_elapsed_time(self) -> None:
        """Reporter measures elapsed time correctly."""
        reporter = AddProgressReporter(total_files=1, json_mode=True)

        with reporter:
            time.sleep(0.1)

        # Should have measured at least 100ms
        assert reporter.elapsed_seconds >= 0.1

    def test_reporter_handles_empty_total(self) -> None:
        """Reporter handles zero total files gracefully."""
        reporter = AddProgressReporter(total_files=0, json_mode=True)

        with reporter:
            pass

        assert reporter.files_processed == 0
        assert reporter.elapsed_seconds >= 0


@pytest.mark.unit
class TestAddProgressIntegration:
    """Integration tests for add progress with actual callbacks."""

    def test_progress_callback_advances_reporter(self, tmp_path: Path) -> None:
        """Progress callback correctly advances the reporter."""
        files = [tmp_path / f"f{i}.txt" for i in range(3)]
        for f in files:
            f.write_text("x")

        reporter = AddProgressReporter(total_files=3, json_mode=True)
        processed = []

        def callback(path: Path) -> None:
            processed.append(path)
            reporter.advance()

        with reporter:
            for f in files:
                callback(f)

        assert len(processed) == 3
        assert reporter.files_processed == 3

    def test_reporter_thread_safety(self) -> None:
        """Reporter is thread-safe with concurrent advance() calls."""
        from concurrent.futures import ThreadPoolExecutor

        total = 1000
        reporter = AddProgressReporter(total_files=total, json_mode=True)

        def advance_many(count: int) -> None:
            for _ in range(count):
                reporter.advance()

        # Use 10 threads, each advancing 100 times
        with reporter:
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(advance_many, 100) for _ in range(10)]
                for f in futures:
                    f.result()

        # All 1000 advances should be counted correctly
        assert reporter.files_processed == total
