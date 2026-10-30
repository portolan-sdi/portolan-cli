"""Check that every CI gate can block a merge or fail a nightly run (#1035).

A gate that cannot go red is not a gate. On ``ff0c61b`` the ``plugin-test``
job ran on every pull request but ``CI Success`` did not wait for it, the
nightly benchmark compared each run with itself, and nothing ran diff-cover.
These tests read the workflow files and the pytest configuration and fail
when a job drops out of the aggregate check or a nightly gate stops failing.

The last tests run the cheap faults of ``scripts/gate_selftest.py`` on a copy
of the tree, so a gate that stops catching its fault fails a pull request and
not only the nightly run.
"""

from __future__ import annotations

import fnmatch
import importlib.util
import re
import sys
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import yaml

if TYPE_CHECKING:
    from types import ModuleType

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
SELFTEST_SCRIPT = REPO_ROOT / "scripts" / "gate_selftest.py"

# Jobs that cannot feed CI Success: the change filter it does not gate on, and
# the aggregate itself.
NOT_AGGREGATED = {"changes", "ci-success"}


def _jobs(workflow: str) -> dict[str, Any]:
    data = yaml.safe_load((WORKFLOWS / workflow).read_text(encoding="utf-8"))
    jobs: dict[str, Any] = data["jobs"]
    return jobs


def _run_text(job: dict[str, Any]) -> str:
    return "\n".join(str(step.get("run", "")) for step in job.get("steps", []))


def _load_selftest() -> ModuleType:
    spec = importlib.util.spec_from_file_location("gate_selftest", SELFTEST_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["gate_selftest"] = module
    spec.loader.exec_module(module)
    return module


def missing_from_ci_success(jobs: dict[str, Any]) -> set[str]:
    """Return each job that ``ci-success`` does not wait for."""
    return set(jobs) - NOT_AGGREGATED - set(jobs["ci-success"]["needs"])


def test_every_job_feeds_ci_success() -> None:
    """Branch protection requires CI Success alone, so a job outside its needs never blocks."""
    jobs = _jobs("ci.yml")

    assert missing_from_ci_success(jobs) == set()
    assert set(jobs["ci-success"]["needs"]) == set(jobs) - NOT_AGGREGATED
    assert jobs["ci-success"]["if"] == "always()"


def test_iceberg_tests_run_when_core_python_changes() -> None:
    """#977 merged because a core change skipped the Iceberg tests."""
    jobs = _jobs("ci.yml")

    assert jobs["iceberg-test"]["if"] == (
        "needs.changes.outputs.iceberg == 'true' || needs.changes.outputs.python == 'true'"
        " || github.event_name == 'push'"
    )
    # The Docker e2e job keeps the narrow filter. Nightly runs the full e2e suite.
    assert jobs["iceberg-e2e"]["if"] == (
        "needs.changes.outputs.iceberg == 'true' || github.event_name == 'push'"
    )


def test_pytest_rejects_unknown_markers(pytestconfig: pytest.Config) -> None:
    """A misspelled marker must fail collection, not move the test out of its lane.

    The ini key and not ``--strict-markers`` in addopts: pytest 9.0.3 ignores
    the flag there (pytest-dev/pytest#14442). The unknown-marker fault below
    proves the behavior. This test pins the key.
    """
    config = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert config["tool"]["pytest"]["ini_options"]["strict_markers"] is True
    assert pytestconfig.getini("strict_markers") is True


COVERAGE_PATTERN = "coverage-*"


def _cells(job: dict[str, Any]) -> list[dict[str, str]]:
    """Return the matrix cells of ``job`` as ``os`` and ``python-version`` pairs."""
    matrix = job["strategy"]["matrix"]
    if "include" in matrix:
        return [{"os": c["os"], "python-version": c["python-version"]} for c in matrix["include"]]
    return [
        {"os": os_name, "python-version": python}
        for os_name in matrix["os"]
        for python in matrix["python-version"]
    ]


def coverage_artifacts(job: dict[str, Any]) -> list[str]:
    """Return the coverage artifact name that each matrix cell of ``job`` uploads.

    A conditional upload step returns no names, so the list is shorter than
    the matrix.
    """
    uploads = [
        step
        for step in job["steps"]
        if str(step.get("uses", "")).startswith("actions/upload-artifact@")
    ]
    assert len(uploads) == 1
    (upload,) = uploads
    assert upload["with"]["path"] == "coverage.xml"
    assert upload["with"]["if-no-files-found"] == "error"
    if "if" in upload:
        return []
    template = str(upload["with"]["name"])
    return [
        template.replace("${{ matrix.os }}", cell["os"]).replace(
            "${{ matrix.python-version }}", cell["python-version"]
        )
        for cell in _cells(job)
    ]


def test_every_test_cell_measures_coverage() -> None:
    """A line that only Windows or macOS runs is covered when that cell runs it."""
    step = next(
        step
        for step in _jobs("ci.yml")["test"]["steps"]
        if str(step.get("name", "")).startswith("Run tests")
    )
    command = re.search(r"uv run pytest .*", step["run"], re.DOTALL)

    assert command is not None
    assert "--cov=portolan_cli --cov-report=xml" in command.group(0)
    assert "cov_args" not in step["run"]


@pytest.mark.parametrize("job", ["test", "iceberg-test"])
def test_every_cell_uploads_its_coverage_report(job: str) -> None:
    jobs = _jobs("ci.yml")
    prefix = {"test": "coverage-", "iceberg-test": "coverage-iceberg-"}[job]
    expected = [f"{prefix}{c['os']}-{c['python-version']}" for c in _cells(jobs[job])]

    assert coverage_artifacts(jobs[job]) == expected


def test_diff_cover_reads_the_report_of_every_cell() -> None:
    jobs = _jobs("ci.yml")
    names = coverage_artifacts(jobs["test"]) + coverage_artifacts(jobs["iceberg-test"])
    downloads = [
        step
        for step in jobs["diff-cover"]["steps"]
        if str(step.get("uses", "")).startswith("actions/download-artifact@")
    ]
    run = _run_text(jobs["diff-cover"])

    assert len(names) == 15
    assert len(set(names)) == len(names)
    assert [step["with"] for step in downloads] == [
        {"pattern": COVERAGE_PATTERN, "path": "coverage"}
    ]
    assert [name for name in names if not fnmatch.fnmatchcase(name, COVERAGE_PATTERN)] == []
    assert "reports=(coverage/*/coverage.xml)" in run
    assert '"${reports[@]}"' in run


def test_coverage_reports_name_files_relative_to_the_checkout() -> None:
    """diff-cover on Linux cannot match an absolute Windows or macOS source path.

    Without the key, the Windows report names the source
    ``D:\\a\\portolan-cli\\portolan-cli\\portolan_cli`` and the macOS report names
    ``/Users/runner/work/...``. diff-cover joins that source with each file name
    and finds no file of the Linux checkout.
    """
    config = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert config["tool"]["coverage"]["run"]["relative_files"] is True


def test_diff_cover_gates_the_changed_lines() -> None:
    """diff-cover measures the diff of the pull request against its own base branch.

    A stacked pull request into an integration or release branch is measured
    on its own lines, not on every line its base branch adds to main.
    """
    jobs = _jobs("ci.yml")
    job = jobs["diff-cover"]
    run = _run_text(job)

    assert job["needs"] == ["test", "iceberg-test"]
    assert job["if"] == (
        "always() && github.event_name == 'pull_request' && needs.test.result == 'success'"
    )
    checkout = job["steps"][0]
    assert checkout["with"] == {"fetch-depth": 0, "persist-credentials": False}
    assert 'git fetch --no-tags origin "$BASE_REF"' in run
    assert re.search(r'uvx "diff-cover==\$\{DIFF_COVER_VERSION\}"', run)
    assert '--compare-branch="origin/$BASE_REF"' in run
    assert "origin/main" not in run
    assert '--fail-under="$FLOOR"' in run
    assert 'cat diff-cover.md >> "$GITHUB_STEP_SUMMARY"' in run
    env = job["env"]
    assert env["BASE_REF"] == "${{ github.base_ref }}"
    assert re.fullmatch(r"\d+\.\d+\.\d+", env["DIFF_COVER_VERSION"])
    assert int(env["FLOOR"]) == 90


def test_only_the_live_network_step_continues_on_error() -> None:
    """A soft-fail step gates nothing. Live third-party servers are the one exception."""
    jobs = _jobs("nightly.yml")

    soft = [
        (name, step.get("name"))
        for name, job in jobs.items()
        for step in job.get("steps", [])
        if step.get("continue-on-error")
    ]
    soft_jobs = [name for name, job in jobs.items() if job.get("continue-on-error")]

    assert soft == [("network-live", "Run network tests against live services")]
    assert soft_jobs == []
    assert "dependency-check" not in jobs


@pytest.mark.parametrize("workflow", ["ci.yml", "nightly.yml"])
def test_every_step_that_pipes_into_tee_sets_pipefail(workflow: str) -> None:
    """A step with no ``shell:`` runs ``bash -e``, so ``cmd | tee`` returns tee's status.

    The nightly live-network step piped pytest into tee without pipefail. Its
    outcome was success on every run, so the tracking issue never opened.
    """
    unsafe = [
        (name, step.get("name"))
        for name, job in _jobs(workflow).items()
        for step in job.get("steps", [])
        if "| tee" in str(step.get("run", ""))
        and "set -o pipefail" not in str(step["run"])
        and step.get("shell") != "bash"
    ]

    assert unsafe == []


def test_nightly_runs_every_property_test_unseeded() -> None:
    job = _jobs("nightly.yml")["property-tests"]
    run = _run_text(job)

    assert job["env"] == {"HYPOTHESIS_PROFILE": "nightly"}
    assert """grep -rlE '^\\s*@given\\(' tests --include="test_*.py\"""" in run
    assert "set -o pipefail" in run
    assert "reproduce_failure" in run


def test_the_property_selector_picks_decorated_tests_only() -> None:
    """A file that names the decorator in a docstring is not a property test.

    The first nightly run on this branch selected this module, because its
    text holds the selector, and ran the whole module under the nightly profile.
    """
    decorated = re.compile(r"^\s*@given\(", re.MULTILINE)
    selected = {
        path.relative_to(REPO_ROOT).as_posix()
        for path in (REPO_ROOT / "tests").rglob("test_*.py")
        if decorated.search(path.read_text(encoding="utf-8"))
    }

    assert "tests/docs/test_ci_wiring.py" not in selected
    assert "tests/unit/test_add_rm_hypothesis.py" in selected


def test_no_hypothesis_profile_keeps_a_deadline() -> None:
    """A deadline measures machine load under xdist, not a property.

    Five property tests failed in one of four full runs on a loaded machine
    with DeadlineExceeded, and passed alone.
    """
    from hypothesis import settings
    from hypothesis.errors import InvalidArgument

    import tests.conftest  # noqa: F401  (registers the profiles)

    assert settings.default.deadline is None
    for name in ("ci", "nightly", "local"):
        try:
            profile = settings.get_profile(name)
        except InvalidArgument:
            continue
        assert profile.deadline is None, name


def test_nightly_runs_the_gate_selftest() -> None:
    job = _jobs("nightly.yml")["gate-selftest"]

    assert job["timeout-minutes"] == 30
    assert "uv run python scripts/gate_selftest.py" in _run_text(job)


def test_scenario_regressions_fail_the_nightly_run() -> None:
    """The nightly job compares wall time and peak RSS with the committed baseline (#992)."""
    jobs = _jobs("nightly.yml")
    run = _run_text(jobs["scenarios"])

    assert "benchmark" not in jobs
    assert "compare=(--compare .benchmarks/scenarios.json)" in run
    assert 'uv run python -m tests.perf.bench --out scenario-results.json "${compare[@]}"' in run
    assert 'if [ "$RECORD_BASELINE" = "true" ]; then\n  compare=()' in run


def _scenario_results(wall_s: float, peak_rss_mb: float, n: int = 100) -> dict[str, Any]:
    scenario = {"n": n, "wall_s": wall_s, "peak_rss_mb": peak_rss_mb}
    return {
        "machine": "x86_64",
        "calibration_s": 1.0,
        "scenarios": {"add-many-collections": scenario},
    }


@pytest.mark.parametrize(
    ("wall_s", "peak_rss_mb", "n", "failed"),
    [
        (20.0, 115.0, 100, False),  # +100% wall and +15% RSS are the limits
        (20.1, 100.0, 100, True),
        (10.0, 115.1, 100, True),
        (10.0, 100.0, 50, True),  # a run at another size has no baseline
    ],
)
def test_the_scenario_compare_fails_past_its_thresholds(
    wall_s: float, peak_rss_mb: float, n: int, failed: bool
) -> None:
    from tests.perf import bench

    failures = bench.compare(_scenario_results(wall_s, peak_rss_mb, n), _scenario_results(10, 100))

    assert bool(failures) is failed


def test_the_scenario_compare_refuses_another_machine() -> None:
    from tests.perf import bench

    baseline = {**_scenario_results(10, 100), "machine": "arm64"}

    assert bench.compare(_scenario_results(10, 100), baseline) == [
        "baseline machine arm64 is not x86_64"
    ]


def test_the_scenario_job_fails_without_a_baseline(tmp_path: Path) -> None:
    from tests.perf import bench

    missing = tmp_path / "scenarios.json"

    assert bench.main(["--out", str(tmp_path / "out.json"), "--compare", str(missing)]) == 1


def test_the_counter_gates_run_once_per_pull_request() -> None:
    """tests/perf runs in its own job, and the test matrix skips it."""
    jobs = _jobs("ci.yml")
    test_run = _run_text(jobs["test"])

    assert "--ignore=tests/perf/" in test_run
    assert 'uv run pytest tests/perf -m "not slow" -n 4' in _run_text(jobs["perf-gates"])
    assert jobs["perf-gates"]["runs-on"] == "ubuntu-latest"


def test_codspeed_measures_the_benchmarks() -> None:
    """CodSpeed replaces the wall-time compare of pytest-benchmark (#992)."""
    job = _jobs("ci.yml")["codspeed"]
    (step,) = [s for s in job["steps"] if str(s.get("uses", "")).startswith("CodSpeedHQ/action@")]

    assert re.fullmatch(r"CodSpeedHQ/action@[0-9a-f]{40}", step["uses"])
    assert step["with"] == {
        "mode": "simulation",
        "run": "uv run pytest tests/benchmark -m benchmark --codspeed",
    }
    assert job["permissions"] == {"contents": "read", "id-token": "write"}
    assert "continue-on-error" not in job
    assert not [s for s in job["steps"] if s.get("continue-on-error")]


# ---------------------------------------------------------------------------
# Cheap gate faults, run on every pull request
# ---------------------------------------------------------------------------

selftest = _load_selftest()
CHEAP_FAULTS = ("library-print", "dead-function", "ci-success-wiring", "unknown-marker")


def test_the_ci_success_check_fails_without_plugin_test() -> None:
    """The in-process half of the ci-success-wiring fault."""
    jobs = _jobs("ci.yml")
    jobs["ci-success"]["needs"].remove("plugin-test")

    assert missing_from_ci_success(jobs) == {"plugin-test"}


@pytest.mark.integration
@pytest.mark.parametrize("name", CHEAP_FAULTS)
def test_the_gate_catches_its_fault(name: str, tmp_path: Path) -> None:
    """The gate passes on a clean copy of the tree and fails once the fault is in."""
    fault = selftest.FAULTS_BY_NAME[name]

    result = selftest.run_fault(fault, REPO_ROOT, tmp_path)

    assert (result.status, result.detail) == ("CAUGHT", "")
