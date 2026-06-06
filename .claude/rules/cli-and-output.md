---
paths:
  - "portolan_cli/cli.py"
  - "portolan_cli/output.py"
  - "portolan_cli/json_output.py"
  - "portolan_cli/errors.py"
  - "portolan_cli/validation/input_hardening.py"
  - "portolan_cli/add_progress.py"
  - "portolan_cli/scan_progress.py"
  - "portolan_cli/upload_progress.py"
---

# CLI surface, output, and the agent-native contract

This is the user-and-agent-facing boundary. Portolan is designed to be driven by
AI agents (ADR-0030), so the JSON envelope and input hardening are a contract,
not a convenience. `cli.py` is a thin Click layer, all logic stays in the library
(ADR-0007). Several real bugs came from breaking the rules below.

## Resolve the catalog root FIRST, then load .env and config

Every command must resolve the final catalog root (honoring `--catalog` /
`--portolan-dir`) **before** calling `load_dotenv_and_warn_sensitive(root)` or any
config-backed gate. Loading `.env` or a backend-support check against the wrong
root leaks catalog A's credentials into a `--catalog B` run. The order in every
handler is: resolve root, load `.env` for that root, resolve sensitive settings,
then do the work. Sensitive settings (`remote`, `profile`/`aws_profile`,
`region`) come from env or `.env`, never `config.yaml` (which gets pushed).

## The JSON envelope is stable and side-effect-honest (ADR-0030)

- Every command supports `--json` / `--format json` and returns the
  `OutputEnvelope` from `json_output.py`: `{success, command, data, errors}`.
  Use `success_envelope(command, data)` and `error_envelope(...)`. Do not
  hand-build the dict, and do not print Rich or colored output in JSON mode.
- **Never gate a mutating side-effect behind `if not use_json`.** A real bug was
  parquet generation and post-add steps being skipped in JSON mode, so agents got
  a success envelope for work that never ran. Run the side-effect, then format
  the output.
- **An explicitly requested step that fails must fail the command.** If the user
  passed `--stac-geoparquet` (or any explicit flag) and that step errors, return
  a non-zero exit and an error envelope, do not warn-and-continue with
  `success: true`.
- **A non-catalog path is an error, not a clean result.** When `catalog.json` is
  missing, do not return an empty/clean report and exit 0. Resolve to the catalog
  root first and surface the structural error, the same one the non-fix path
  reports, so `check <subdir> --metadata --fix` cannot silently no-op.

## Errors carry their type into the envelope (errors.py)

`json_output.ErrorDetail` reports the **error class name**. Raise the specific
`PortolanError` subclass (`CatalogNotFoundError`, `UnsupportedFormatError`,
`CRSMismatchError`, `ConfigParseError`, ...), never a bare `Exception` /
`ValueError`. Wrap stale-config `ValueError`s and `SystemExit` from deep calls
into proper CLI or JSON error envelopes, agents parse the `type`, so a generic
exception degrades the contract. Add a new subclass to `errors.py` rather than
reusing a generic one.

## Three output channels, kept separate

- User-facing styled messages go through `output.py`
  (`success`/`info`/`warn`/`error`/`detail`), which is RLock-guarded for
  concurrent threads.
- Match severity to outcome. A file accepted with no conversion (ADR-0014) is a
  WARNING, not an `error`/`✗ failed`. A file that was actually tracked must not
  print as failed.
- Internal diagnostics use stdlib `logging`. Raw `print()` belongs only inside
  progress rendering and JSON emission.

## Progress and summary model (ADR-0040)

Long-running commands (`add`, `scan`, `push`) use a Rich progress bar
(`transient=True`), surface errors immediately, and **batch repeated warnings by
type** (cap around 100) into a summary. `--verbose` adds per-file detail, `--json`
disables Rich entirely. Dry-run summary counts MUST match the files listed, no
"Would push N" followed by "Nothing to push".

## Harden every agent-supplied input (validation/input_hardening.py)

Agents hallucinate paths, embed query params, double-encode, and inject control
chars. Validate before any filesystem, API, or URL use:
`validate_safe_path` (rejects absolute paths and `..` traversal),
`validate_collection_id`, `validate_item_id`, `validate_remote_url`,
`validate_config_key`, `validate_config_value`. Do not pass raw CLI input into a
path join, a store key, or a service URL.

## Concurrency warnings must reflect real fan-out

The connection footprint is `file_concurrency x chunk_concurrency x workers`.
Include `workers` in any warning and emit it after the worker count resolves.
Do not advertise a flag (for example `--adaptive`) in help unless it is wired to
the executor.

## Where to investigate further

- ADRs 0007 (CLI wraps API), 0009 (dry-run and verbose), 0030 (agent-native JSON
  and input hardening), 0040 (progress and summary).
- `json_output.py` (`OutputEnvelope`, `ErrorDetail`), `errors.py` (the
  `PortolanError` hierarchy), `validation/input_hardening.py` (the validators).
- The sync rules in `sync.md` for how credentials and dry-run behave end to end.
