---
paths:
  - "tests/**"
  - "**/test_*.py"
---

# Testing rules

## Test-Driven Development (MANDATORY)

**YOU MUST USE TDD. NO EXCEPTIONS.** Unless the user explicitly says "skip tests":

1. **WRITE TESTS FIRST**, Before ANY implementation code
2. **RUN TESTS**, Verify they fail with `uv run pytest`
3. **IMPLEMENT**, Minimal code to pass tests
4. **RUN TESTS AGAIN**, Verify they pass
5. **ADD EDGE CASES**, Test error conditions

## Run the affected tests while you work

Run only the tests your change can reach while you work:

```bash
uv run python scripts/affected_tests.py --run -n 4
```

The script reads the import graph and the changed files against
`origin/main`. A change to `pyproject.toml`, `uv.lock`, `tests/conftest.py`,
or `tests/fixtures/` selects the full suite. Run the full suite once before you
push, or let CI run it:

```bash
HYPOTHESIS_PROFILE=ci uv run pytest -n 4 -q \
  -m "not network and not slow and not benchmark and not e2e and not e2e_slow"
```

The affected set is a guide, not a proof. A test that reaches code through a
subprocess or a file path, and not through an import, can be missing from it.

Test markers are documented in the root `AGENTS.md` (kept there because
`scripts/validate_agents_md.py` and `scripts/generate_agents_md_sections.py`
keep them in sync with `pyproject.toml`).

**Real-world fixtures:** `tests/fixtures/realdata/README.md` records the source of each one.
These test Portolan's **orchestration** with production data, they do NOT test geometry conversion (that's upstream's job).

## Defending Against Tautological Tests

Two defenses:

1. **The per-assertion checklist below**, Each regression test must go red on
   the pre-fix code. Revert the fix, or inject the wrong result, and confirm
   that the test fails before you commit it.
2. **Ruff assertion rules**, The `B` and `F` rule sets in `[tool.ruff.lint]`
   flag `assert False` (B011), a comparison with no effect (B015), a test that
   expects a bare `Exception` (B017), and an assert on a tuple (F631). The
   `PT` (flake8-pytest-style) rules are not selected.

Property tests (`hypothesis`) check invariants, and `tests/specs/` holds the
human specs that the tests implement.

### Per-assertion checklist (this is the most-flagged review issue)

Reviewers repeatedly catch tests that cannot fail. Before you commit a test, run
it against the assertions below.

- **Never** assert something that is always true: `len(x) >= 0`, `count > 0`
  where 0 is impossible, "accept exit code 0 or 1", `"name" not in dir(obj)`, or
  a body that is just `pass`.
- A **regression test** must assert the specific expected value or output, and
  you must be able to say out loud which concrete pre-fix behavior turns it red.
  If you cannot, the test does not cover the bug.
- For mtime / tolerance regressions, **pin** the mtime to the exact tolerance
  window, do not leave it to wall-clock timing.
- Do not weaken an assertion to encode a known bug (e.g. relaxing `== 2` to
  `>= 1` so a double-count passes). Fix the code or mark the test `xfail` with a
  reason, never launder the bug into a green test.
- Assert exact strings/counts, not substrings of a substring. `"0"` and
  `"0 assets"` are different assertions.

### Test docstrings are optional

The test name carries the description. A test docstring is optional and has
one line.

### Every new test needs a marker


Marker-based CI selection (`uv run pytest -m unit`) silently skips unmarked
tests, so an unmarked test never runs in the fast tier. Add `pytestmark` (or
per-test decorators) to every new test file, and confirm it is collected:
`uv run pytest -m unit --collect-only` should list your new tests.

### Do not over-suppress

- Put optional-dependency imports (`moto`, `boto3`) inside the specific
  network fixtures/tests, not a module-level `importorskip` that drops unrelated
  coverage.
- Do not add `# type: ignore` to test helpers, annotate them, `mypy --strict`
  applies to tests too.

## Command parity

Each CLI command delegates to one library function. The function takes the same
options and returns a result without printing. A parity test in
`tests/parity/` proves it for one command.

**Add a parity test:**

1. Put it in `tests/parity/test_<family>_parity.py` with
   `pytestmark = pytest.mark.integration`.
2. Call the `parity` fixture with the command path as a string literal, the
   arguments without `--json`, a catalog factory, and the library call:

   ```python
   parity(
       "info",
       ["points"],
       catalog=points_catalog,
       call=lambda root: inspect_collection(Path("points")),
   )
   ```

   `call` receives the catalog root, and the working directory is that root.
   `tests/parity/test_info_parity.py` is a full example.
3. Remove the command from `PARITY_PENDING` in
   `tests/parity/test_parity_harness.py`. That list only shrinks. The test
   fails while a covered command stays in it, and for a command that is
   neither covered nor pending.

**What one call does:** the harness builds the catalog at
`tmp_path / "parity-catalog"` and runs `portolan <command> <args> --json`
through `run_cli`. It checks the catalog invariants, builds a fresh catalog at
the same path, and runs the library call there. The library call and its
`to_dict()` must not write to stdout or stderr.

**Conversion rule:** the command passes `result.to_dict()` to `emit_success`
as the envelope `data` and changes nothing. The harness compares the two after
one `json.dumps` and `json.loads`. Put each field in `to_dict()`, not in `cli.py`.

**Ignore rule:** a field that differs between runs, such as a timestamp, goes
in `ignore` as a dotted key path: `"created"`, `"summary.elapsed"`, or
`"collections.*.updated"`. A number selects one list element and `*` selects
each. The path must exist. Do not ignore a field to hide a real difference.

**Failure path:** `parity.error(command, args, catalog=..., call=...,
raises=SomeError)` runs both sides on a failing case. The library call must
raise `raises`. The command must exit non-zero with `success: false` and
exactly one error whose `type`, `message`, and `code` equal the `ErrorDetail`
built from that exception. A failure case does not move a command out of
`PARITY_PENDING`.

## Test Fixtures

Store small, representative data files in `tests/fixtures/`. Fixtures should be:

- **Small**, a few rows/pixels, enough to test behavior
- **Committed to git**, they're small enough, and reproducibility matters
- **Paired with invalid variants**, every valid fixture should have a corresponding invalid one
- **Documented**, each subdirectory gets a README.md
