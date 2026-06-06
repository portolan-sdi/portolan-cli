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

- **ruff**, Linting and formatting
- **mypy**, Type checking (`strict = true`)
- **vulture**, Dead code detection
- **xenon**, Complexity monitoring (max C function, B module, A average)
- **pylint**, Duplicate code detection (R0801 only, `--fail-under=9.5`)
- **bandit**, Security scanning
- **pip-audit**, Dependency vulnerabilities

All code must have type annotations (`mypy --strict`). The CLI is a thin Click layer, all logic lives in the library (ADR-0007).

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
- **Architecture is enforced by import-linter** (`uv run lint-imports`, ADR-0025).
  Three contracts: `portolan_cli.cli` must not import `portolan_cli.backends`
  (only `backends.protocol` under `TYPE_CHECKING`), `backends.iceberg` must not
  import `cli`, and utility/leaf modules stay independent. Check
  `[tool.importlinter]` before adding any cross-module import.
- **Ruff rule sets**: E/W (pycodestyle), F (pyflakes), I (isort), B (bugbear),
  C4 (comprehensions), UP (pyupgrade). Line length 100, double quotes (ruff
  format applies both).
