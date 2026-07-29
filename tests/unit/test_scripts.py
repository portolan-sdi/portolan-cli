"""Tests for documentation maintenance scripts.

These tests verify that the scripts in scripts/ work correctly:
- update_freshness.py: Auto-updates freshness markers
- generate_claude_md_sections.py: Generates the known-issues table and command surface.
- generate_skill_md.py: Generates CLI commands, Python API sections
- validate_claude_md.py: Validates CLAUDE.md references
- validate_skill_md.py: Validates SKILL.md structure
"""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest

# mutmut refuses to import on Windows — ``mutmut/__main__.py`` prints "please use
# the WSL" and calls ``sys.exit(1)`` at module scope. The contract tests below
# compare against its real mutant names, so they can only run where mutation
# testing itself runs. See boxed/mutmut#397.
requires_mutmut_import = pytest.mark.skipif(
    sys.platform == "win32",
    reason="mutmut exits at import on Windows (boxed/mutmut#397)",
)


def real_mutant_name(path: str, mangled: str) -> str:
    """Return ``mutmut``'s own mutant name for ``path`` and ``mangled``.

    Computed in a subprocess because importing ``mutmut.__main__`` calls
    ``multiprocessing.set_start_method('fork')`` at module scope, which raises
    ``RuntimeError: context has already been set`` if any earlier test in the
    session started a process pool (``test_convert.py`` does). In-process the
    contract tests below therefore passed or failed on file ordering alone —
    green under xdist's distribution, red in a single-process run. A fresh
    interpreter has no start method set, so the answer is order-independent.

    Callers are integration tests, not unit tests: each call spawns an
    interpreter and imports mutmut, which costs about half a second against the
    100ms unit budget.
    """
    source = (
        "from pathlib import Path;"
        "from mutmut.__main__ import get_mutant_name;"
        f"print(get_mutant_name(Path({path!r}), {mangled!r}))"
    )
    result = subprocess.run(  # noqa: S603 - fixed argv, no shell, no user input
        [sys.executable, "-c", source],
        capture_output=True,
        text=True,
        check=True,
        # mutmut's import runs module-scope setup; a wedged one would otherwise
        # hang the job until the whole workflow times out.
        timeout=120,
    )
    return result.stdout.strip()


# Add scripts directory to path for imports
@pytest.fixture(autouse=True)
def _add_scripts_to_path() -> None:
    """Add scripts directory to sys.path for imports."""
    scripts_dir = Path(__file__).parent.parent.parent / "scripts"
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))


class TestUpdateFreshness:
    """Tests for update_freshness.py."""

    @pytest.mark.unit
    def test_file_to_section_mapping_exists(self) -> None:
        """FILE_TO_SECTION_MAP should have entries."""
        from update_freshness import FILE_TO_SECTION_MAP

        assert len(FILE_TO_SECTION_MAP) > 0
        assert "portolan_cli/output.py" in FILE_TO_SECTION_MAP

    @pytest.mark.unit
    def test_get_sections_for_files_finds_mapped_section(self) -> None:
        """Should return sections for mapped files."""
        from update_freshness import get_sections_for_files

        sections = get_sections_for_files(["portolan_cli/output.py"])
        assert "Standardized Terminal Output" in sections

    @pytest.mark.unit
    def test_get_sections_for_files_returns_empty_for_unmapped(self) -> None:
        """Should return empty set for unmapped files."""
        from update_freshness import get_sections_for_files

        sections = get_sections_for_files(["portolan_cli/some_random_file.py"])
        assert sections == set()

    @pytest.mark.unit
    def test_update_freshness_marker_updates_date(self) -> None:
        """Should update the date in freshness marker."""
        from update_freshness import update_freshness_marker

        content = """<!-- freshness: last-verified: 2020-01-01 -->
## Standardized Terminal Output

Some content here.
<!-- /freshness -->"""

        today = datetime.now().strftime("%Y-%m-%d")
        updated = update_freshness_marker(content, "Standardized Terminal Output", today)

        assert f"last-verified: {today}" in updated
        assert "last-verified: 2020-01-01" not in updated


class TestValidateClaudeMd:
    """Tests for validate_claude_md.py."""

    @pytest.mark.unit
    def test_validation_result_aggregates_errors(self) -> None:
        """ValidationResult should aggregate errors and warnings."""
        from validate_claude_md import ValidationResult

        result = ValidationResult(validator="test")
        result.errors.append("Error 1")
        result.errors.append("Error 2")
        result.warnings.append("Warning 1")

        assert len(result.errors) == 2
        assert len(result.warnings) == 1


class TestMutantGlobs:
    """Tests for mutant_globs.py (source path -> mutmut mutant-name pattern)."""

    @pytest.mark.unit
    def test_module_path_becomes_dotted_prefix(self) -> None:
        """A source path maps to its dotted module name plus the mangle prefix."""
        from mutant_globs import mutant_glob

        assert (
            mutant_glob("portolan_cli/backends/iceberg/backend.py")
            == "portolan_cli.backends.iceberg.backend.x*"
        )

    @pytest.mark.unit
    def test_package_init_collapses_into_package(self) -> None:
        """mutmut strips ``.__init__.`` from mutant names, so the glob must too."""
        from mutant_globs import mutant_glob

        assert mutant_glob("portolan_cli/extract/__init__.py") == "portolan_cli.extract.x*"

    @pytest.mark.unit
    def test_top_level_init_collapses_to_bare_package(self) -> None:
        """The root ``__init__.py`` mutates under the bare package name."""
        from mutant_globs import mutant_glob

        assert mutant_glob("portolan_cli/__init__.py") == "portolan_cli.x*"

    @pytest.mark.unit
    def test_src_prefix_is_stripped(self) -> None:
        """mutmut strips a ``src.`` layout prefix from module names."""
        from mutant_globs import mutant_glob

        assert mutant_glob("src/portolan_cli/readme.py") == "portolan_cli.readme.x*"

    @pytest.mark.unit
    def test_non_python_path_is_rejected(self) -> None:
        """Only ``.py`` sources have mutants; anything else is a caller bug."""
        from mutant_globs import mutant_glob

        with pytest.raises(ValueError):
            mutant_glob("portolan_cli/data.json")

    @pytest.mark.integration  # Spawns an interpreter per call to import mutmut
    @requires_mutmut_import
    @pytest.mark.parametrize(
        "path",
        [
            "portolan_cli/readme.py",
            "portolan_cli/extract/__init__.py",
            "portolan_cli/__init__.py",
            "portolan_cli/backends/iceberg/backend.py",
        ],
    )
    @pytest.mark.parametrize(
        "mangled",
        ["x_convert__mutmut_1", "xǁBackendǁpush__mutmut_12"],
    )
    def test_glob_matches_real_mutmut_names(self, path: str, mangled: str) -> None:
        """Contract test against mutmut itself: the glob matches names it emits.

        This is the assertion that would have caught #612's shard bug — the
        workflows globbed file paths (``portolan_cli/readme.py*``) while mutmut
        names mutants by dotted module (``portolan_cli.readme.x_...``), so the
        filter matched nothing and the run tested zero mutants.
        """
        import fnmatch

        from mutant_globs import mutant_glob

        name = real_mutant_name(path, mangled)
        assert fnmatch.fnmatch(name, mutant_glob(path))

    @pytest.mark.integration  # Spawns an interpreter to import mutmut
    @requires_mutmut_import
    def test_glob_excludes_sibling_modules_of_a_package(self) -> None:
        """A package glob must not swallow its submodules' mutants."""
        import fnmatch

        from mutant_globs import mutant_glob

        package = mutant_glob("portolan_cli/extract/__init__.py")
        submodule = real_mutant_name(
            "portolan_cli/extract/common/converters/base.py", "x_run__mutmut_1"
        )
        assert not fnmatch.fnmatch(submodule, package)

    @pytest.mark.unit
    def test_main_prints_one_glob_per_path(self, capsys: pytest.CaptureFixture[str]) -> None:
        """The CLI shim emits one glob per argument for the workflow to consume."""
        from mutant_globs import main

        code = main(["portolan_cli/readme.py", "portolan_cli/extract/__init__.py"])
        assert code == 0
        assert capsys.readouterr().out == "portolan_cli.readme.x*\nportolan_cli.extract.x*\n"

    @pytest.mark.unit
    def test_main_rejects_empty_input(self, capsys: pytest.CaptureFixture[str]) -> None:
        """No paths means no globs, which would silently mutate nothing."""
        from mutant_globs import main

        assert main([]) == 1
        assert "no source paths" in capsys.readouterr().err


class TestMutationScore:
    """Tests for mutation_score.py (shared mutmut floor enforcement)."""

    @pytest.mark.unit
    def test_read_floor_skips_comments_and_blanks(self) -> None:
        """First non-comment, non-blank line is parsed as the integer floor."""
        from mutation_score import read_floor

        text = "# a comment\n\n   # indented comment\n60\n70\n"
        assert read_floor(text) == 60

    @pytest.mark.unit
    def test_read_floor_rejects_non_integer(self) -> None:
        """A non-integer floor is a hard error, not a silent default."""
        from mutation_score import read_floor

        with pytest.raises(ValueError):
            read_floor("# header\nninety\n")

    @pytest.mark.unit
    def test_evaluate_zero_testable_is_broken(self) -> None:
        """Zero testable mutants means mutmut produced nothing — never a pass."""
        from mutation_score import evaluate

        score = evaluate(
            {"killed": 0, "survived": 0, "no_tests": 5, "timeout": 0, "suspicious": 0},
            floor=60,
        )
        assert score.testable == 0
        assert score.ok is False
        assert score.kill_rate is None

    @pytest.mark.unit
    def test_evaluate_below_floor_fails(self) -> None:
        """Kill rate under the floor fails the run."""
        from mutation_score import evaluate

        score = evaluate(
            {"killed": 1, "survived": 9, "no_tests": 0, "timeout": 0, "suspicious": 0},
            floor=60,
        )
        assert score.testable == 10
        assert score.kill_rate == 10.0
        assert score.ok is False

    @pytest.mark.unit
    def test_evaluate_at_floor_passes(self) -> None:
        """Kill rate exactly at the floor passes (>=, not >)."""
        from mutation_score import evaluate

        score = evaluate(
            {"killed": 6, "survived": 4, "no_tests": 0, "timeout": 0, "suspicious": 0},
            floor=60,
        )
        assert score.kill_rate == 60.0
        assert score.ok is True

    @pytest.mark.unit
    def test_evaluate_counts_timeout_and_suspicious_as_killed(self) -> None:
        """Timeout and suspicious mutants are killed (the test suite reacted)."""
        from mutation_score import evaluate

        # 5 clean-killed + 3 timeout + 2 suspicious = 10 killed, 0 survived.
        score = evaluate(
            {"killed": 5, "survived": 0, "no_tests": 0, "timeout": 3, "suspicious": 2},
            floor=60,
        )
        assert score.killed_total == 10
        assert score.testable == 10
        assert score.kill_rate == 100.0
        assert score.ok is True

    @pytest.mark.unit
    def test_main_exits_nonzero_below_floor(self, tmp_path: Path) -> None:
        """End-to-end: main() reads files and returns exit 1 below the floor."""
        import json

        from mutation_score import main

        stats = tmp_path / "stats.json"
        stats.write_text(
            json.dumps(
                {
                    "killed": 1,
                    "survived": 9,
                    "no_tests": 0,
                    "timeout": 0,
                    "suspicious": 0,
                }
            )
        )
        baseline = tmp_path / ".mutation-baseline"
        baseline.write_text("# floor\n60\n")

        code = main(["--stats", str(stats), "--baseline", str(baseline)])
        assert code == 1

    @pytest.mark.unit
    def test_main_zero_testable_fails_by_default(self, tmp_path: Path) -> None:
        """Nightly semantics: zero testable mutants is a broken run (exit 1)."""
        import json

        from mutation_score import main

        stats = tmp_path / "stats.json"
        stats.write_text(json.dumps({"killed": 0, "survived": 0, "no_tests": 3}))
        baseline = tmp_path / ".mutation-baseline"
        baseline.write_text("60\n")

        assert main(["--stats", str(stats), "--baseline", str(baseline)]) == 1

    @pytest.mark.unit
    def test_main_zero_testable_allowed_with_flag(self, tmp_path: Path) -> None:
        """PR semantics: --allow-empty treats no mutants in scope as a pass (exit 0)."""
        import json

        from mutation_score import main

        stats = tmp_path / "stats.json"
        stats.write_text(json.dumps({"killed": 0, "survived": 0, "no_tests": 3}))
        baseline = tmp_path / ".mutation-baseline"
        baseline.write_text("60\n")

        code = main(["--stats", str(stats), "--baseline", str(baseline), "--allow-empty"])
        assert code == 0

    @pytest.mark.unit
    def test_main_exits_zero_at_or_above_floor(self, tmp_path: Path) -> None:
        """End-to-end: a passing run returns exit 0 and writes a summary table."""
        import json

        from mutation_score import main

        stats = tmp_path / "stats.json"
        stats.write_text(
            json.dumps(
                {
                    "killed": 8,
                    "survived": 2,
                    "no_tests": 1,
                    "timeout": 0,
                    "suspicious": 0,
                }
            )
        )
        baseline = tmp_path / ".mutation-baseline"
        baseline.write_text("60\n")
        summary = tmp_path / "summary.md"

        code = main(
            [
                "--stats",
                str(stats),
                "--baseline",
                str(baseline),
                "--summary",
                str(summary),
                "--label",
                "changed files",
            ]
        )
        assert code == 0
        written = summary.read_text()
        assert "changed files" in written
        assert "80" in written  # kill rate


class TestShardSelect:
    """Tests for shard_select.py (which files the nightly sweep mutates)."""

    @pytest.mark.unit
    def test_assignment_is_stable_across_calls(self) -> None:
        """The same path always lands in the same shard — no per-process salt."""
        from shard_select import shard_of

        first = shard_of("portolan_cli/sync/upload_progress.py", 25)
        assert first == shard_of("portolan_cli/sync/upload_progress.py", 25)

    @pytest.mark.unit
    def test_windows_and_posix_paths_agree(self) -> None:
        """Separator style must not change a file's shard."""
        from shard_select import shard_of

        assert shard_of("portolan_cli\\sync\\upload.py", 25) == shard_of(
            "portolan_cli/sync/upload.py", 25
        )

    @pytest.mark.unit
    def test_shard_is_within_range(self) -> None:
        """Every path lands in 0..num_shards-1."""
        from shard_select import shard_of

        paths = [f"portolan_cli/mod_{i}.py" for i in range(200)]
        assert all(0 <= shard_of(p, 25) < 25 for p in paths)

    @pytest.mark.unit
    def test_adding_a_file_leaves_other_assignments_alone(self) -> None:
        """The regression that per-shard baselines depend on.

        Index-based round-robin reshuffled every file's shard whenever a file was
        added, so a recorded per-shard kill rate went stale on any commit adding
        a module. Hash assignment moves only the new file.
        """
        from shard_select import select

        before = [f"portolan_cli/mod_{i}.py" for i in range(50)]
        after = sorted([*before, "portolan_cli/aaa_new_module.py"])

        for shard in range(25):
            was = set(select(before, 25, shard))
            now = set(select(after, 25, shard))
            assert was <= now
            assert now - was <= {"portolan_cli/aaa_new_module.py"}

    @pytest.mark.unit
    def test_shards_partition_the_file_list(self) -> None:
        """Every file lands in exactly one shard: no gaps, no double-mutating."""
        from shard_select import select

        paths = [f"portolan_cli/mod_{i}.py" for i in range(200)]
        selected = [p for shard in range(25) for p in select(paths, 25, shard)]
        assert sorted(selected) == sorted(paths)
        assert len(selected) == len(set(selected))

    @pytest.mark.unit
    def test_select_rejects_out_of_range_shard(self) -> None:
        """A shard index outside the count is a caller bug, not an empty result."""
        from shard_select import select

        with pytest.raises(ValueError):
            select(["portolan_cli/a.py"], 25, 25)

    @pytest.mark.unit
    def test_rejects_non_positive_shard_count(self) -> None:
        """Zero shards would divide by zero; reject it at the edge."""
        from shard_select import shard_of

        with pytest.raises(ValueError):
            shard_of("portolan_cli/a.py", 0)

    @pytest.mark.integration  # Walks a tmp_path tree
    def test_main_emits_only_the_requested_shard(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """End-to-end: the shards partition the tree, printed root-relative."""
        from shard_select import main, shard_of

        root = tmp_path / "pkg"
        root.mkdir()
        expected = sorted(f"pkg/mod_{i}.py" for i in range(30))
        for name in expected:
            (root / Path(name).name).write_text("x = 1\n")

        emitted: list[str] = []
        for shard in range(5):
            assert main(["--root", str(root), "--num-shards", "5", "--shard", str(shard)]) == 0
            printed = [line for line in capsys.readouterr().out.splitlines() if line]
            # Paths come out relative to the package, never carrying the tmp_path
            # prefix: mutant_globs.py dots them into module names.
            assert all(shard_of(p, 5) == shard for p in printed)
            emitted.extend(printed)

        assert sorted(emitted) == expected  # no file skipped, none mutated twice

    @pytest.mark.integration  # Walks a tmp_path tree
    def test_assignment_ignores_where_the_package_lives(self, tmp_path: Path) -> None:
        """The regression macOS CI caught: an absolute root reshuffled the shards.

        Hashing the path as walked made membership depend on the working
        directory, so the same file landed in a different shard when the sweep
        ran from a tmp dir — voiding every recorded per-shard baseline.
        """
        from shard_select import shard_key

        root = tmp_path / "portolan_cli"
        (root / "sync").mkdir(parents=True)
        absolute = root / "sync" / "upload.py"
        absolute.write_text("x = 1\n")

        assert shard_key(root, absolute) == "portolan_cli/sync/upload.py"
        assert shard_key(Path("portolan_cli"), Path("portolan_cli/sync/upload.py")) == (
            "portolan_cli/sync/upload.py"
        )

    @pytest.mark.integration  # Walks a tmp_path tree
    def test_main_fails_when_root_has_no_sources(self, tmp_path: Path) -> None:
        """An empty package means a broken checkout, never a covered sweep (#612)."""
        from shard_select import main

        empty = tmp_path / "pkg"
        empty.mkdir()

        assert main(["--root", str(empty), "--num-shards", "5", "--shard", "0"]) == 1

    @pytest.mark.integration  # Walks a tmp_path tree
    def test_main_fails_when_a_shard_draws_no_files(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A populated tree can still leave a shard empty; that is not a sweep.

        Exiting 0 with no output would hand the workflow an empty glob list, and
        a night that mutates nothing must not read as a night that found nothing
        wrong (#612).
        """
        from shard_select import main, shard_of

        root = tmp_path / "pkg"
        root.mkdir()
        names = [f"mod_{i}.py" for i in range(3)]
        for name in names:
            (root / name).write_text("x = 1\n")

        occupied = {shard_of(f"pkg/{name}", 8) for name in names}
        starved = next(s for s in range(8) if s not in occupied)

        assert main(["--root", str(root), "--num-shards", "8", "--shard", str(starved)]) == 1
        captured = capsys.readouterr()
        assert captured.out == ""  # nothing for the workflow to mistake for work
        assert "selected none of the 3 files" in captured.err


class TestShardBaselines:
    """Tests for the per-shard floors in .mutation-shards.json."""

    @staticmethod
    def _shards_doc(rates: dict[str, float], *, num_shards: int = 25) -> str:
        import json

        return json.dumps(
            {
                "num_shards": num_shards,
                "tolerance": 3.0,
                "shards": {k: {"kill_rate": v} for k, v in rates.items()},
            }
        )

    @pytest.mark.unit
    def test_recorded_rate_raises_the_floor(self) -> None:
        """A shard is gated on its own measurement, not the repo-wide floor."""
        from mutation_score import read_shard_baselines

        baselines = read_shard_baselines(self._shards_doc({"8": 44.63}))
        floor, source = baselines.floor_for(8, repo_floor=30)

        assert floor == 41.63  # 44.63 less the 3pp tolerance
        assert "44.63" in source

    @pytest.mark.unit
    def test_unrecorded_shard_falls_back_to_repo_floor(self) -> None:
        """Shards not yet measured are still gated, just more loosely."""
        from mutation_score import read_shard_baselines

        baselines = read_shard_baselines(self._shards_doc({}))
        floor, source = baselines.floor_for(3, repo_floor=30)

        assert floor == 30
        assert "no recorded rate" in source

    @pytest.mark.unit
    def test_repo_floor_wins_when_recorded_rate_is_lower(self) -> None:
        """A stale low record must not weaken the repo-wide floor."""
        from mutation_score import read_shard_baselines

        baselines = read_shard_baselines(self._shards_doc({"4": 20.0}))
        floor, _ = baselines.floor_for(4, repo_floor=30)

        assert floor == 30

    @pytest.mark.unit
    def test_malformed_documents_are_rejected(self) -> None:
        """Bad baselines fail loudly rather than defaulting to no gate."""
        import json

        from mutation_score import read_shard_baselines

        with pytest.raises(ValueError):
            read_shard_baselines("{not json")
        with pytest.raises(ValueError):
            read_shard_baselines(json.dumps({"num_shards": 25}))
        with pytest.raises(ValueError):
            read_shard_baselines(
                json.dumps({"num_shards": 25, "tolerance": 3.0, "shards": {"8": 44.63}})
            )

    @pytest.mark.unit
    def test_repo_baseline_file_parses(self) -> None:
        """The committed .mutation-shards.json is valid and matches nightly.yml."""
        from mutation_score import read_shard_baselines

        repo_root = Path(__file__).parent.parent.parent
        baselines = read_shard_baselines((repo_root / ".mutation-shards.json").read_text())

        assert baselines.num_shards == 25  # keep in step with NUM_SHARDS in nightly.yml
        assert baselines.tolerance > 0

    @pytest.mark.unit
    def test_main_fails_below_shard_floor(self, tmp_path: Path) -> None:
        """A shard regressing under its recorded rate fails the sweep."""
        import json

        from mutation_score import main

        stats = tmp_path / "stats.json"
        stats.write_text(json.dumps({"killed": 35, "survived": 65}))  # 35%
        baseline = tmp_path / ".mutation-baseline"
        baseline.write_text("30\n")
        shards = tmp_path / ".mutation-shards.json"
        shards.write_text(self._shards_doc({"8": 44.63}))

        code = main(
            [
                "--stats",
                str(stats),
                "--baseline",
                str(baseline),
                "--shards",
                str(shards),
                "--shard",
                "8",
                "--num-shards",
                "25",
            ]
        )
        assert code == 1  # 35% clears the repo floor but not the shard's 41.63%

    @pytest.mark.unit
    def test_main_rejects_stale_shard_count(self, tmp_path: Path) -> None:
        """Changing NUM_SHARDS re-partitions the tree; recorded rates no longer apply."""
        import json

        from mutation_score import main

        stats = tmp_path / "stats.json"
        stats.write_text(json.dumps({"killed": 90, "survived": 10}))
        baseline = tmp_path / ".mutation-baseline"
        baseline.write_text("30\n")
        shards = tmp_path / ".mutation-shards.json"
        shards.write_text(self._shards_doc({"8": 44.63}, num_shards=25))

        code = main(
            [
                "--stats",
                str(stats),
                "--baseline",
                str(baseline),
                "--shards",
                str(shards),
                "--shard",
                "8",
                "--num-shards",
                "10",
            ]
        )
        assert code == 1

    @pytest.mark.unit
    def test_main_requires_shards_file_with_shard(self, tmp_path: Path) -> None:
        """--shard without its baselines file is a wiring bug, not a silent pass."""
        import json

        from mutation_score import main

        stats = tmp_path / "stats.json"
        stats.write_text(json.dumps({"killed": 90, "survived": 10}))
        baseline = tmp_path / ".mutation-baseline"
        baseline.write_text("30\n")

        code = main(["--stats", str(stats), "--baseline", str(baseline), "--shard", "8"])
        assert code == 1

    @pytest.mark.unit
    def test_main_prompts_to_record_an_unmeasured_shard(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A passing unmeasured shard prints the line that gates it next time."""
        import json

        from mutation_score import main

        stats = tmp_path / "stats.json"
        stats.write_text(json.dumps({"killed": 60, "survived": 40}))
        baseline = tmp_path / ".mutation-baseline"
        baseline.write_text("30\n")
        shards = tmp_path / ".mutation-shards.json"
        shards.write_text(self._shards_doc({}))

        code = main(
            [
                "--stats",
                str(stats),
                "--baseline",
                str(baseline),
                "--shards",
                str(shards),
                "--shard",
                "7",
                "--num-shards",
                "25",
            ]
        )
        out = capsys.readouterr().out
        assert code == 0
        assert "::notice::" in out
        assert "60.0" in out  # the measured rate, ready to paste
