# Feature: ConversionReport and convert_directory()

Batch conversion of files in a directory with progress callbacks.

## ConversionReport Dataclass

```python
@dataclass
class ConversionReport:
    results: list[ConversionResult]

    @property
    def succeeded(self) -> int: ...
    @property
    def failed(self) -> int: ...
    @property
    def skipped(self) -> int: ...
    @property
    def invalid(self) -> int: ...
    @property
    def total(self) -> int: ...

    def to_dict(self) -> dict[str, Any]: ...
```

## ConversionReport Tests

- [ ] Empty report has all counts = 0
- [ ] Report with mixed results has correct counts
- [ ] total equals len(results)
- [ ] to_dict() returns JSON-serializable dictionary
- [ ] to_dict() includes summary counts and results array

## convert_directory() Function Signature

```python
def convert_directory(
    path: Path,
    output_dir: Path | None = None,
    on_progress: Callable[[ConversionResult], None] | None = None,
    recursive: bool = True,
) -> ConversionReport
```

## Happy Path

- [ ] Convert directory with multiple files: Returns report with all results
- [ ] Callback called for each file with ConversionResult
- [ ] Callback receives results in order files were processed
- [ ] Non-recursive mode only processes top-level files

## Failure Handling

- [ ] One file fails, others still processed (not atomic)
- [ ] Failed files counted in report.failed
- [ ] Report includes all results, successful and failed
- [ ] Error messages preserved in failed results

## Idempotent Behavior

- [ ] Re-run on same directory skips already-converted files
- [ ] Skipped files counted in report.skipped
- [ ] Already cloud-native files are skipped

## Edge Cases

- [ ] Empty directory returns empty report (total=0)
- [ ] Directory with only cloud-native files: all skipped
- [ ] Directory does not exist: raises FileNotFoundError
- [ ] path is a file not directory: raises ValueError or similar

## Invariants

- [ ] Original files are NEVER deleted
- [ ] succeeded + failed + skipped + invalid == total
- [ ] Report.results contains one ConversionResult per processed file
- [ ] If callback provided, it's called exactly len(results) times
