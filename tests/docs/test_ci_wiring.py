"""Check that every CI gate can block a merge or fail a nightly run (#1035).

A gate that cannot go red is not a gate. On ``ff0c61b`` the ``plugin-test``
job ran on every pull request but ``CI Success`` did not wait for it, the
nightly benchmark compared each run with itself, and nothing ran diff-cover.
These tests read the workflow files and the pytest configuration and fail
when a job drops out of the aggregate check or a nightly gate stops failing.

The last tests run the cheap faults of ``scripts/gate_selftest.py`` on a copy
of the tree, so a gate that stops catching its fault fails a pull request and
not only the nightly run.

The module lives in ``tests/docs/`` because it reads repository files that the
mutmut sandbox does not copy (#997).
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import yaml

if sys.version_info >= (3, 11):
    import tomllib
else:  # pytest itself depends on tomli before 3.11
    import tomli as tomllib

if TYPE_CHECKING:
    from types import ModuleType

pytestmark = [pytest.mark.unit, pytest.mark.source_scan]

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
BENCHMARK_BASELINE = REPO_ROOT / ".benchmarks" / "baseline.json"
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


def test_the_coverage_cells_upload_their_reports() -> None:
    jobs = _jobs("ci.yml")
    cell = "matrix.os == 'ubuntu-latest' && matrix.python-version == '3.11'"

    for job, artifact in (("test", "coverage-main"), ("iceberg-test", "coverage-iceberg")):
        uploads = [
            step
            for step in jobs[job]["steps"]
            if str(step.get("uses", "")).startswith("actions/upload-artifact@")
        ]
        assert len(uploads) == 1, job
        assert uploads[0]["if"] == cell
        assert uploads[0]["with"]["name"] == artifact
        assert uploads[0]["with"]["path"] == "coverage.xml"
    iceberg_upload = next(
        step for step in jobs["iceberg-test"]["steps"] if step.get("name") == "Upload coverage XML"
    )
    assert iceberg_upload["with"]["if-no-files-found"] == "ignore"


def test_diff_cover_gates_the_changed_lines() -> None:
    jobs = _jobs("ci.yml")
    job = jobs["diff-cover"]
    run = _run_text(job)

    assert job["needs"] == ["test", "iceberg-test"]
    assert job["if"] == (
        "always() && github.event_name == 'pull_request' && needs.test.result == 'success'"
    )
    checkout = job["steps"][0]
    assert checkout["with"] == {"fetch-depth": 0, "persist-credentials": False}
    assert "git fetch --no-tags origin main" in run
    assert re.search(r'uvx "diff-cover==\$\{DIFF_COVER_VERSION\}"', run)
    assert "--compare-branch=origin/main" in run
    assert '--fail-under="$FLOOR"' in run
    assert 'cat diff-cover.md >> "$GITHUB_STEP_SUMMARY"' in run
    env = job["env"]
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
    assert "tests/integration/test_mutmut_hypothesis_guard.py" not in selected
    assert "tests/unit/test_add_rm_hypothesis.py" in selected


def test_nightly_runs_the_gate_selftest() -> None:
    job = _jobs("nightly.yml")["gate-selftest"]

    assert job["timeout-minutes"] == 30
    assert "uv run python scripts/gate_selftest.py" in _run_text(job)


def test_benchmark_regressions_fail_the_nightly_run() -> None:
    """The old step compared each run with a baseline that git never held."""
    run = _run_text(_jobs("nightly.yml")["benchmark"])

    assert "--benchmark-compare=.benchmarks/baseline.json" in run
    assert "--benchmark-compare-fail=min:20%" in run
    assert "pytest-benchmark compare" not in run
    assert "cp benchmark-results.json" not in run


def test_benchmark_baseline_exists_and_parses() -> None:
    """The baseline comes from a nightly run on an x86_64 runner, never a dev machine."""
    baseline = json.loads(BENCHMARK_BASELINE.read_text(encoding="utf-8"))

    assert baseline["benchmarks"]
    assert baseline["machine_info"]["machine"] == "x86_64"
    assert baseline["commit_info"]["id"]


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

    if result.status == "PENDING":
        # The gate's configuration is not in the tree yet. The nightly run
        # turns a PENDING row red after selftest.PENDING_DEADLINE.
        assert result.detail == fault.requires(REPO_ROOT)
        assert result.detail
    else:
        assert (result.status, result.detail) == ("CAUGHT", "")
