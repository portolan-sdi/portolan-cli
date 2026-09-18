---
paths:
  - "portolan_cli/**/*.py"
---

# Python source rules

## Standardized Terminal Output

Use `portolan_cli/output.py` for all user-facing messages:

```python
from portolan_cli.output import success, info, warn, error, detail

success("Wrote output.parquet (1.2 MB)")  # ✓ Green checkmark
info("Reading data.shp (4,231 features)")  # → Blue arrow
warn("Missing thumbnail (recommended)")    # ⚠ Yellow warning
error("No geometry column (required)")     # ✗ Red X
detail("Processing chunk 3/10...")         # Dimmed text
```

**Progress UI:** The `add` and `scan` commands have excellent progress printing with real-time updates. Use this pattern (Rich progress bars + batched output) for any long-running operations.

## Code Quality

- **ruff**, Linting, formatting, complexity (`C901`), security (`S`), and
  docstrings (`D`). Ruff replaced xenon, radon, and bandit in September 2026.
- **mypy**, Type checking (`strict = true`)
- **vulture**, Dead code detection
- **jscpd**, Duplicate code detection, gated against `.jscpd-baseline.json`.
  The gate fails only when a commit adds a clone that the baseline does not hold.
- **pip-audit**, Dependency vulnerabilities

All code must have type annotations (`mypy --strict`). The CLI is a thin Click layer, all logic lives in the library.

## Coding conventions (enforced or de-facto in this repo)

- **Paths use `pathlib.Path`, never `os.path`.** The codebase is pathlib-first
  (the large majority of modules import `pathlib`, only a handful of legacy
  `os.path` calls remain).
  Build paths with `path / "sub"`, read with `.read_text()`, test with
  `.exists()`. Do not introduce `os.path.join`, `os.getcwd`, or `open(str_path)`.
  When comparing a filesystem path to a STAC href, normalize with
  `PurePath(...).as_posix()` so Windows backslashes do not break matching.
- **Modern typing, Python 3.10+.** Nearly every module begins with
  `from __future__ import annotations` (the large majority do). Use `X | None`,
  `list[str]`, `dict[str, Any]`. Never import `Optional`, `List`, `Dict` from
  `typing` (ruff `UP` rejects them, and there are zero `Optional[` in the tree).
- **Raise typed errors from `errors.py`.** There is a hierarchy rooted at
  `PortolanError` (`CatalogNotFoundError`, `CollectionAlreadyExistsError`,
  `UnsupportedFormatError`, `CRSMismatchError`, `ConfigParseError`, and more).
  Raise the specific subclass, never a bare `Exception` / `ValueError` /
  `RuntimeError`. The JSON error envelope reports the error **class name**
  (`json_output.ErrorDetail`), so the type is part of the agent contract. Add a
  new subclass to `errors.py` rather than reusing a generic one.
- **Three output channels, do not mix them.** User-facing styled messages go
  through `output.py` (`success`/`info`/`warn`/`error`/`detail`). Internal
  diagnostics use the stdlib `logging` module (already used across ~22 modules).
  Raw `print()` appears only inside progress rendering and JSON emission, do not
  use it for normal messaging.
- **Architecture is enforced by import-linter** (`uv run lint-imports`).
  Three contracts: `portolan_cli.cli` must not import `portolan_cli.backends`
  (only `backends.protocol` under `TYPE_CHECKING`), `backends.iceberg` must not
  import `cli`, and utility/leaf modules stay independent. Check
  `[tool.importlinter]` before adding any cross-module import.
- **Ruff rule sets**: see `select` in `[tool.ruff.lint]`. It holds 37 groups.
  Read the comment on each one before you suppress it. Line length 100, double
  quotes (ruff format applies both).
- **Complexity ceiling is `C901` at 15.** Ruff counts every decision point in a
  function, and a nested function adds to the function that holds it. When the
  rule fires, extract the reporting code first. That is the largest win in this
  repo, because a command interleaves the work with the report of the work.
- **Fix a rule category in the config, not at each site.** `ignore`,
  `per-file-ignores`, `builtins-ignorelist`, and `extend-ignore-names` are the
  knobs. Each entry carries the reason it exists. Add a `# noqa` only when the
  finding is a false positive that no config knob covers.
- **Security rules come from `S` (flake8-bandit).** Write `# noqa: Sxxx`, not
  `# nosec Bxxx`. The old `# nosec` comments do nothing now.
- **`PLC0415` (import-outside-top-level) stays off, and this was measured.**
  The tree holds 440 function-level imports. 88 of them break a real import
  cycle. About 10 more defer a heavy third-party import that costs 0.16s to
  0.33s each against a 1.0s `portolan --help`. The rule cannot tell those apart
  from the 274 that buy nothing. Enabling it buys tidier imports for about 98
  permanent suppressions. Move an import to the top of the module when you
  touch it and nothing needs it lazy. Do not select the rule.
