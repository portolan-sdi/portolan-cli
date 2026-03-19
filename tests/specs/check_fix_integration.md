# Feature: check --fix Integration

Integrates the convert module with the `check` command's `--fix` flag to
automatically convert non-cloud-native files to cloud-native formats.

## Command Signature

```bash
# Check for issues (report only)
portolan check [PATH]

# Auto-fix convertible files
portolan check [PATH] --fix

# Preview what would be fixed
portolan check [PATH] --fix --dry-run
```

## Module Design

Per ADR-0007 (CLI wraps API), we create `check.py` with the logic and
wire it into `cli.py` as a thin wrapper.

```python
# portolan_cli/check.py

def check_directory(
    path: Path,
    *,
    fix: bool = False,
    dry_run: bool = False,
    on_progress: Callable[[CheckResult], None] | None = None,
) -> CheckReport
```

## Test Scenarios

### Basic Check (Task 6.2)

- [ ] check command detects CONVERTIBLE files from scan result
- [ ] check command reports count of cloud-native vs convertible files
- [ ] check command identifies unsupported formats

### Check --fix (Task 6.3)

- [ ] --fix converts CONVERTIBLE files to cloud-native
- [ ] --fix validates converted output
- [ ] --fix reports conversion summary (succeeded/failed/skipped)

### Check --fix Updates versions.json (Task 6.4)

- [ ] Converted assets have source_path set to original file
- [ ] Converted assets have source_mtime set to original mtime
- [ ] versions.json is updated with new assets

### Check --fix --dry-run (Task 6.5)

- [ ] --dry-run shows what would be converted
- [ ] --dry-run does NOT modify any files
- [ ] --dry-run does NOT update versions.json

### Partial Failure Handling (Task 6.6)

- [ ] One file fails, others still converted
- [ ] Report includes both succeeded and failed
- [ ] Exit code reflects overall status (0 = all good, 1 = some failed)

### CLI Output (Tasks 6.7, 6.8)

- [ ] Human-readable output shows progress
- [ ] --json flag outputs JSON envelope
- [ ] Progress bar shown for multiple files

## Invariants

- [ ] Original files are NEVER deleted (side-by-side conversion)
- [ ] Failed conversions do not block other files
- [ ] Existing cloud-native files are skipped
- [ ] Unsupported formats are reported but do not cause errors

## Exit Codes

- 0: All checks passed, all conversions succeeded (or nothing to do)
- 1: Some errors occurred (validation failed or conversion failed)
