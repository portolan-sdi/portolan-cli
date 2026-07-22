# DuckDB "Query interrupted" transient failure during bulk conversion

## Status
**Known issue** — Workaround applied (bounded retry, shared across every
conversion entry point the `add` pipeline uses).

> **Update (Issue #339 follow-up):** the original retry was added to
> `convert.py::_convert_vector`, but bulk `portolan add` converts single-layer
> vectors through `preparation.py::convert_vector` (and tabular files through
> `preparation.py::convert_tabular`) — **not** `_convert_vector` — so the nightly
> `test_add_1000_files_*` job kept flaking on the exact same `Query interrupted`.
> The retry logic was extracted into `convert.py::run_with_transient_convert_retry`
> and is now applied on every add-path conversion seam (`_convert_vector`,
> `_convert_vector_layer`, `preparation.convert_vector`, `preparation.convert_tabular`).

## Description
During large bulk `portolan add` runs, a single vector→GeoParquet conversion
occasionally fails with a DuckDB `InterruptException: Query interrupted`. It is
non-deterministic: a *different* file fails each time, roughly 1 in 1000, and
only under the nightly CI runner — it has not reproduced locally across 1000+
sequential conversions.

`geoparquet-io` runs conversions through DuckDB. The DuckDB Python client checks
for pending process signals during query execution (via `PyErr_CheckSignals`);
when a signal is pending it interrupts the running query and raises
`InterruptException`. Under the busy nightly runner a stray/transient signal
trips this check. Because `gpio.convert()` opens a **fresh** DuckDB connection
per file and `PyErr_CheckSignals` consumes the pending signal when it fires, the
condition clears immediately — so simply re-running the same conversion
succeeds.

## Symptom
The nightly *Slow Tests (Stress/Scale)* job failed intermittently:

```
FAILED tests/integration/test_versioning_stress.py::TestScaleAt1000Files::test_add_1000_files_populates_versions
  AssertionError: Add failed: ✓ Added 999 files to 1 collection
    ✗ 1 item failed:
    ✗   - .../scale-1000/large-collection/file_0435.geojson: Query interrupted
```

## Impact
- One transient interrupt out of 1000 files failed the *entire* `add` (exit 1),
  because a per-file conversion failure is aggregated into an `AddFailure`.
- Real users doing large bulk adds hit the same one-file-fails-the-batch problem.
- Does not affect correctness of the 999 files that convert normally.

## Workaround Applied
`portolan_cli/convert.py::run_with_transient_convert_retry` runs a conversion
callable and retries a transient interrupt up to `_CONVERT_MAX_ATTEMPTS` (3)
times. Detection is by exception **type name** (`InterruptException`) plus
message (`"query interrupted"`) — we deliberately do **not** `import duckdb` (a
transitive dependency we don't declare; a direct import would break the deptry
transitive-import contract). Any non-transient error still fails on the first
attempt.

Every conversion seam the `add` pipeline can reach is wrapped in this helper:
- `convert.py::_convert_vector` (single-layer, `convert_file` route)
- `convert.py::_convert_vector_layer` (multi-layer GeoPackage/FileGDB)
- `preparation.py::convert_vector` (single-layer vectors — the `add` route)
- `preparation.py::convert_tabular` (CSV/TSV/XLSX)

See `_is_transient_conversion_error`,
`tests/unit/test_convert.py::TestVectorConversionRetriesTransientInterrupt`, and
`tests/unit/test_preparation.py::TestConvertVectorRetriesTransientInterrupt`.

## If This Resurfaces
If a file fails all 3 attempts, the signal source is likely *sustained* rather
than one-shot — investigate what is delivering signals to the process during the
run (subprocess reaping, a timeout mechanism) rather than raising the attempt
count.
