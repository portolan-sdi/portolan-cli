"""Tests for documentation maintenance scripts.

These tests verify that the scripts in scripts/ work correctly:
- update_freshness.py: Auto-updates freshness markers
- generate_claude_md_sections.py: Generates ADR index, known issues, etc.
- generate_skill_md.py: Generates CLI commands, Python API sections
- validate_claude_md.py: Validates CLAUDE.md references
- validate_skill_md.py: Validates SKILL.md structure
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pytest

# mutmut refuses to import on Windows — ``mutmut/__main__.py`` prints "please use
# the WSL" and calls ``sys.exit(1)`` at module scope. The contract tests below
# import it to compare against its real mutant names, so they can only run where
# mutation testing itself runs. See boxed/mutmut#397.
requires_mutmut_import = pytest.mark.skipif(
    sys.platform == "win32",
    reason="mutmut exits at import on Windows (boxed/mutmut#397)",
)


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


class TestGenerateClaudeMdSections:
    """Tests for generate_claude_md_sections.py."""

    @pytest.mark.integration  # Uses tmp_path filesystem I/O
    def test_extract_adr_title_from_file(self, tmp_path: Path) -> None:
        """Should extract title from ADR file."""
        from generate_claude_md_sections import extract_adr_title

        adr = tmp_path / "0001-test-decision.md"
        adr.write_text("# ADR-0001: My Test Decision\n\n## Status\nAccepted")

        title = extract_adr_title(adr)
        assert title == "My Test Decision"

    @pytest.mark.integration  # Uses tmp_path filesystem I/O
    def test_extract_adr_title_fallback_to_filename(self, tmp_path: Path) -> None:
        """Should fall back to filename if no heading found."""
        from generate_claude_md_sections import extract_adr_title

        adr = tmp_path / "0002-another-decision.md"
        adr.write_text("No heading here, just content.")

        title = extract_adr_title(adr)
        assert "another" in title.lower() or "decision" in title.lower()

    @pytest.mark.integration  # Uses tmp_path filesystem I/O
    def test_generate_adr_index_produces_table(self, tmp_path: Path) -> None:
        """Should generate a markdown table for ADRs."""
        from generate_claude_md_sections import generate_adr_index

        # Create ADR directory structure
        adr_dir = tmp_path / "context" / "shared" / "adr"
        adr_dir.mkdir(parents=True)

        (adr_dir / "0001-first.md").write_text("# ADR-0001: First Decision\n")
        (adr_dir / "0002-second.md").write_text("# ADR-0002: Second Decision\n")

        result = generate_adr_index(tmp_path)

        assert "| ADR | Decision |" in result
        assert "[0001]" in result
        assert "[0002]" in result


class TestValidateClaudeMd:
    """Tests for validate_claude_md.py."""

    @pytest.mark.unit
    def test_extract_adr_links_finds_links(self) -> None:
        """Should extract ADR links from markdown."""
        from validate_claude_md import extract_adr_links

        content = """
| ADR | Decision |
|-----|----------|
| [0001](context/shared/adr/0001-first.md) | First |
| [0002](context/shared/adr/0002-second.md) | Second |
"""
        links = extract_adr_links(content)

        assert "context/shared/adr/0001-first.md" in links
        assert "context/shared/adr/0002-second.md" in links

    @pytest.mark.unit
    def test_extract_adr_links_ignores_non_adr_links(self) -> None:
        """Should not extract non-ADR links."""
        from validate_claude_md import extract_adr_links

        content = """
See [this guide](docs/contributing.md) for details.
"""
        links = extract_adr_links(content)
        assert len(links) == 0

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

    @pytest.mark.unit
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
        from mutmut.__main__ import get_mutant_name

        name = get_mutant_name(Path(path), mangled)
        assert fnmatch.fnmatch(name, mutant_glob(path))

    @pytest.mark.unit
    @requires_mutmut_import
    def test_glob_excludes_sibling_modules_of_a_package(self) -> None:
        """A package glob must not swallow its submodules' mutants."""
        import fnmatch

        from mutant_globs import mutant_glob
        from mutmut.__main__ import get_mutant_name

        package = mutant_glob("portolan_cli/extract/__init__.py")
        submodule = get_mutant_name(
            Path("portolan_cli/extract/common/converters/base.py"), "x_run__mutmut_1"
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
