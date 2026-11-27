"""Check that every CI gate can block a merge or fail a nightly run."""

from __future__ import annotations

import fnmatch
import importlib.util
import os
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
    assert spec is not None
    assert spec.loader is not None
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
    """A core Python change runs the Iceberg tests too."""
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
    """pytest 9.0.3 ignores --strict-markers in addopts: https://github.com/pytest-dev/pytest/issues/14442"""
    config = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert config["tool"]["pytest"]["ini_options"]["strict_markers"] is True
    assert pytestconfig.getini("strict_markers") is True


COVERAGE_PATTERN = "coverage-*"
COVERAGE_OS = "ubuntu-latest"
COVERAGE_PYTHON = "3.11"
COVERAGE_CONDITION = f"matrix.os == '{COVERAGE_OS}' && matrix.python-version == '{COVERAGE_PYTHON}'"


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
    """Return the coverage artifact names that ``job`` uploads, from the ubuntu 3.11 cell only."""
    uploads = [
        step
        for step in job["steps"]
        if str(step.get("uses", "")).startswith("actions/upload-artifact@")
    ]
    assert len(uploads) == 1
    (upload,) = uploads
    assert upload["with"]["path"] == "coverage.xml"
    assert upload["with"]["if-no-files-found"] == "error"
    assert upload["if"] == COVERAGE_CONDITION
    template = str(upload["with"]["name"])
    return [
        template.replace("${{ matrix.os }}", cell["os"]).replace(
            "${{ matrix.python-version }}", cell["python-version"]
        )
        for cell in _cells(job)
        if (cell["os"], cell["python-version"]) == (COVERAGE_OS, COVERAGE_PYTHON)
    ]


def _pytest_step(job: dict[str, Any], prefix: str) -> dict[str, Any]:
    return next(step for step in job["steps"] if str(step.get("name", "")).startswith(prefix))


@pytest.mark.parametrize(
    ("job", "prefix", "flag"),
    [
        ("test", "Run tests", "--cov=portolan_cli --cov=portolan_core --cov-report=xml"),
        ("iceberg-test", "Run unit and integration tests", "--cov=portolan_cli/backends/iceberg"),
    ],
)
def test_only_the_linux_cell_measures_coverage(job: str, prefix: str, flag: str) -> None:
    """Only the ubuntu 3.11 condition fills the ``coverage`` array, and pytest reads the array."""
    step = _pytest_step(_jobs("ci.yml")[job], prefix)
    run = str(step["run"])
    lines = [line.strip() for line in run.splitlines() if line.strip()]
    command = " ".join(run[run.index("uv run pytest ") :].replace("\\\n", " ").split())

    assert lines[:2] == [
        "coverage=()",
        f'if [ "$MATRIX_OS" = "{COVERAGE_OS}" ] && [ "$MATRIX_PYTHON" = "{COVERAGE_PYTHON}" ]; then',
    ]
    assert lines[2].startswith("coverage=(")
    assert lines[2].endswith(")")
    assert flag in lines[2]
    assert lines[3] == "fi"
    assert [line for line in lines if "coverage=" in line] == lines[0:1] + lines[2:3]
    assert command.endswith(' "${coverage[@]}"')
    assert run.count("${coverage[@]}") == 1
    assert "--cov" not in command
    assert step["env"]["MATRIX_OS"] == "${{ matrix.os }}"
    assert step["env"]["MATRIX_PYTHON"] == "${{ matrix.python-version }}"


@pytest.mark.parametrize(
    ("job", "name"),
    [
        ("test", "coverage-ubuntu-latest-3.11"),
        ("iceberg-test", "coverage-iceberg-ubuntu-latest-3.11"),
    ],
)
def test_only_the_linux_cell_uploads_its_coverage_report(job: str, name: str) -> None:
    jobs = _jobs("ci.yml")

    assert coverage_artifacts(jobs[job]) == [name]


def test_diff_cover_reads_the_two_linux_reports() -> None:
    jobs = _jobs("ci.yml")
    names = coverage_artifacts(jobs["test"]) + coverage_artifacts(jobs["iceberg-test"])
    downloads = [
        step
        for step in jobs["diff-cover"]["steps"]
        if str(step.get("uses", "")).startswith("actions/download-artifact@")
    ]
    run = _run_text(jobs["diff-cover"])

    assert len(names) == 2
    assert len(set(names)) == len(names)
    assert [step["with"] for step in downloads] == [
        {"pattern": COVERAGE_PATTERN, "path": "coverage"}
    ]
    assert [name for name in names if not fnmatch.fnmatchcase(name, COVERAGE_PATTERN)] == []
    assert "reports=(coverage/*/coverage.xml)" in run
    assert '"${reports[@]}"' in run


def test_coverage_reports_name_files_relative_to_the_checkout() -> None:
    config = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert config["tool"]["coverage"]["run"]["relative_files"] is True


def test_diff_cover_gates_the_changed_lines() -> None:
    """A stacked pull request is measured on its own lines, not on the lines its base adds."""
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
    soft: list[tuple[str, str, str | None]] = []
    soft_jobs: list[tuple[str, str]] = []
    for workflow in sorted(WORKFLOWS.glob("*.yml")):
        for name, job in _jobs(workflow.name).items():
            if job.get("continue-on-error"):
                soft_jobs.append((workflow.name, name))
            soft.extend(
                (workflow.name, name, step.get("name"))
                for step in job.get("steps", [])
                if step.get("continue-on-error")
            )

    assert soft == [("nightly.yml", "network-live", "Run network tests against live services")]
    assert soft_jobs == []
    assert "dependency-check" not in _jobs("nightly.yml")


def test_security_audit_fails_when_pip_audit_cannot_run() -> None:
    """The capture keeps stderr out of the JSON: a clean run writes to stderr."""
    (step,) = [s for s in _jobs("security-audit.yml")["audit"]["steps"] if s.get("id") == "audit"]
    capture = step["run"].split("AUDIT_OUTPUT=$(uv run pip-audit", 1)[1].split(")", 1)[0]
    error_branch = step["run"].split("::error::pip-audit failed", 1)[1]

    assert capture.rstrip().endswith("--format json")
    assert "exit 1" in error_branch


@pytest.mark.parametrize("workflow", ["ci.yml", "nightly.yml"])
def test_every_step_that_pipes_into_tee_sets_pipefail(workflow: str) -> None:
    """A step with no ``shell:`` runs ``bash -e``, so ``cmd | tee`` returns the status of tee."""
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
    """A file that names the decorator in a docstring is not a property test."""
    decorated = re.compile(r"^\s*@given\(", re.MULTILINE)
    selected = {
        path.relative_to(REPO_ROOT).as_posix()
        for path in (REPO_ROOT / "tests").rglob("test_*.py")
        if decorated.search(path.read_text(encoding="utf-8"))
    }

    assert "tests/docs/test_ci_wiring.py" not in selected
    assert "tests/unit/test_add_rm_hypothesis.py" in selected


def test_no_hypothesis_profile_keeps_a_deadline() -> None:
    """A deadline measures machine load under xdist, not a property."""
    from hypothesis import settings
    from hypothesis.errors import InvalidArgument

    import tests.conftest  # noqa: F401 - registers the profiles

    default = settings.default
    assert default is not None
    assert default.deadline is None
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
    """CodSpeed replaces the wall-time compare of pytest-benchmark."""
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


BUILD_PACKAGE = re.compile(r"uv build --package (\S+) --out-dir (\S+)")


def test_release_publishes_portolan_core_before_portolan_cli() -> None:
    """portolan-cli pins portolan-core exactly, so PyPI must hold portolan-core first."""
    steps = _jobs("release.yml")["release"]["steps"]
    out_dirs = {
        package: out_dir
        for step in steps
        for package, out_dir in BUILD_PACKAGE.findall(str(step.get("run", "")))
    }
    published = [
        step.get("with", {}).get("packages-dir", "dist/")
        for step in steps
        if str(step.get("uses", "")).startswith("pypa/gh-action-pypi-publish@")
    ]

    assert {"portolan-core", "portolan-cli"} <= out_dirs.keys()
    assert out_dirs["portolan-core"] in published
    assert out_dirs["portolan-cli"] in published
    assert published.index(out_dirs["portolan-core"]) < published.index(out_dirs["portolan-cli"])


def test_release_checks_the_versions_before_the_tag() -> None:
    """A version mismatch must stop the release before it pushes a tag or publishes."""
    steps = _jobs("release.yml")["release"]["steps"]
    runs = [str(step.get("run", "")) for step in steps]
    guard = next(
        i for i, run in enumerate(runs) if "test_core_package.py::test_the_versions_agree" in run
    )
    tag = next(i for i, run in enumerate(runs) if 'git tag -a "$TAG"' in run)

    assert guard < tag
    assert "if" not in steps[guard]


def test_ci_runs_the_wheel_smoke() -> None:
    job = _jobs("ci.yml")["wheel-smoke"]

    assert "python scripts/wheel_smoke.py" in _run_text(job)
    assert job["steps"][0]["with"]["persist-credentials"] is False


def _builds(job: dict[str, Any]) -> list[tuple[str, str]]:
    return [
        pair for step in job["steps"] for pair in BUILD_PACKAGE.findall(str(step.get("run", "")))
    ]


def test_release_builds_into_the_ignored_dist_folder() -> None:
    """A dist-core/ folder at the root goes into the portolan-cli sdist; .gitignore excludes dist/."""
    out_dirs = [out_dir for _, out_dir in _builds(_jobs("release.yml")["release"])]
    gitignore = (REPO_ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()

    assert "dist/" in gitignore
    assert out_dirs == ["dist/core", "dist/cli"]


def test_the_build_job_builds_as_the_release_does() -> None:
    """The build job checks the sdists of the same commands that the release runs."""
    build = _jobs("ci.yml")["build"]

    assert _builds(build) == _builds(_jobs("release.yml")["release"])
    assert "dist/*/*.tar.gz" in _run_text(build)


def _step_index(steps: list[dict[str, Any]], text: str) -> int:
    (index,) = [i for i, step in enumerate(steps) if text in str(step.get("run", ""))]
    return index


def test_a_recovery_run_builds_the_tagged_commit() -> None:
    """main can move on after a partial release, so the build checks out the tag first."""
    steps = _jobs("release.yml")["release"]["steps"]
    tag = _step_index(steps, 'git tag -a "$TAG"')
    checkout = _step_index(steps, 'git checkout --quiet "$TAG"')
    build = _step_index(steps, "uv build --package portolan-core")

    assert tag < checkout < build
    assert steps[checkout]["if"] == steps[build]["if"]


def test_the_release_compares_the_build_with_pypi_before_it_publishes() -> None:
    """skip-existing keeps a file that PyPI holds, so a different local build must fail first."""
    steps = _jobs("release.yml")["release"]["steps"]
    build = _step_index(steps, "uv build --package portolan-core")
    compare = _step_index(steps, 'check_pypi_digests.py" portolan-core')
    first_publish = next(
        i
        for i, step in enumerate(steps)
        if str(step.get("uses", "")).startswith("pypa/gh-action-pypi-publish@")
    )
    run = str(steps[compare]["run"])

    assert build < compare < first_publish
    assert 'portolan-core "$VERSION" dist/core' in run
    assert 'portolan-cli "$VERSION" dist/cli' in run


def _load_digests() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "check_pypi_digests", REPO_ROOT / "scripts" / "check_pypi_digests.py"
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("published", "expected"),
    [
        ({}, []),  # nothing of the release is on PyPI yet
        ({"a.whl": "1", "a.tar.gz": "2"}, []),
        ({"a.whl": "1"}, []),  # the sdist is not on PyPI yet
        ({"a.whl": "1", "a.tar.gz": "9"}, ["a.tar.gz"]),
        ({"a.whl": "9", "a.tar.gz": "9", "b.whl": "3"}, ["a.tar.gz", "a.whl"]),
    ],
)
def test_the_digest_check_names_each_file_with_other_bytes(
    published: dict[str, str], expected: list[str]
) -> None:
    local = {"a.whl": "1", "a.tar.gz": "2"}

    assert _load_digests().mismatches(local, published) == expected


# Cheap gate faults, run on every pull request

selftest = _load_selftest()
CHEAP_FAULTS = ("library-print", "dead-function", "ci-success-wiring", "unknown-marker")


def test_the_gate_env_puts_both_copied_packages_first(tmp_path: Path) -> None:
    """Without the core folder a gate imports portolan_core from the editable install."""
    path = selftest.gate_env(tmp_path)["PYTHONPATH"].split(os.pathsep)

    assert path == [str(tmp_path), str(tmp_path / "packages" / "portolan-core" / "src")]


def test_the_duplicate_gate_runs_the_prek_jscpd_command() -> None:
    """The self-test proves the hook that CI runs, so both read portolan-core."""
    prek = (REPO_ROOT / "prek.toml").read_text(encoding="utf-8")
    (entry,) = re.findall(r'entry = "(uvx jscpd@[^"]+)"', prek)
    (gate,) = selftest.FAULTS_BY_NAME["duplicate-block"].gates

    assert gate.command == tuple(entry.split())
    assert selftest.DUPLICATE_MODULE.startswith("packages/portolan-core/src/")


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
